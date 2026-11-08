# Copyright (c) 2026 Kenneth Stott
# Canary: 147e9aef-a10b-4b57-9f1e-755a231fe46f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-native-meta, REQ-2014: DuckDBFederationRuntime exposes the org's control plane -- PostgreSQL --
as the ``provisa_admin`` catalog so meta/ops queries (e.g. registered_tables) resolve on the native
tier, as they do on Trino, where ``provisa_admin`` is a real catalog over the same database. The
attach is DuckDB's postgres extension, read-only and live: every schema maps 1:1 under the catalog
and a row the control plane commits is read by the next statement, with no snapshot to retake."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import insert

duckdb = pytest.importorskip("duckdb")

import provisa.api.app  # noqa: E402,F401 -- the API layer says whether the deployment holds many orgs, which reading a source's secrets asks

# Imported as the module: DuckDB resolves an unknown table name to a Python variable of that name
# in the calling frame (its replacement scan), which the missing-catalog case must not find.
from provisa.core import schema_org  # noqa: E402
from provisa.core.models import SourceType  # noqa: E402
from provisa.federation import duckdb_runtime  # noqa: E402
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402
from provisa.federation.native_backend import libpq_dsn  # noqa: E402

_REGISTERED = 'SELECT table_name FROM "provisa_admin"."org_default"."registered_tables"'


def _register(plane, table_id: int, table_name: str) -> None:
    async def _go() -> None:
        async with plane.org().acquire() as conn:
            if table_id == 1:
                await conn.execute_core(
                    insert(schema_org.domains).values(id="sales", description="")
                )
                await conn.execute_core(
                    insert(schema_org.sources).values(id="s1", type="postgresql")
                )
            await conn.execute_core(
                insert(schema_org.registered_tables).values(
                    id=table_id,
                    source_id="s1",
                    domain_id="sales",
                    schema_name="public",
                    table_name=table_name,
                    description="",
                )
            )

    asyncio.run(_go())


@pytest.fixture
def runtime(control_plane):
    rt = DuckDBFederationRuntime()
    rt.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
    yield rt
    rt.close()


def test_provisa_admin_catalog_missing_without_attach_raises():
    rt = DuckDBFederationRuntime()
    try:
        with pytest.raises(duckdb.Error, match="provisa_admin"):
            rt._con.execute(_REGISTERED).fetchall()
    finally:
        rt.close()


def test_the_control_plane_is_read_under_provisa_admin(control_plane, runtime):
    _register(control_plane, 1, "orders")
    assert [r[0] for r in runtime.run_sync(_REGISTERED).rows] == ["orders"]
    # every table of the org's schema, not a hand-picked list
    assert runtime.run_sync('SELECT id FROM "provisa_admin"."org_default"."sources"').rows == [
        ("s1",)
    ]


def test_a_row_committed_after_the_attach_is_read_by_the_next_statement(control_plane, runtime):
    _register(control_plane, 1, "orders")
    assert [r[0] for r in runtime.run_sync(_REGISTERED).rows] == ["orders"]
    _register(control_plane, 2, "pets")
    assert sorted(r[0] for r in runtime.run_sync(_REGISTERED).rows) == ["orders", "pets"]


def test_attaching_again_is_a_no_op(control_plane, runtime):
    runtime.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
    assert runtime.run_sync(_REGISTERED).rows == []


def test_the_attach_is_read_only(runtime):
    with pytest.raises(duckdb.Error):
        runtime._con.execute(
            'INSERT INTO "provisa_admin"."org_default"."sources" (id, type) VALUES (\'x\', \'y\')'
        )


def test_concurrent_queries_never_steal_each_others_results(control_plane, runtime):
    """Regression: two queries running at once on the shared DuckDB connection came back with ZERO
    ROWS. A duckdb connection holds ONE pending result -- the second execute() replaces the first,
    and the first caller's fetchall() then returns an empty list rather than raising."""
    _register(control_plane, 1, "orders")
    _register(control_plane, 2, "pets")
    stop = threading.Event()
    wrong: list[str] = []
    errors: list[str] = []

    def hammer(sql: str, expected: int) -> None:
        loop = asyncio.new_event_loop()
        try:
            while not stop.is_set():
                rows = loop.run_until_complete(runtime.run(sql)).rows
                if len(rows) != expected:
                    wrong.append(f"{sql[:40]}: {len(rows)} rows")
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            loop.close()

    threads = [
        threading.Thread(target=hammer, args=(_REGISTERED, 2), daemon=True),
        threading.Thread(target=hammer, args=("SELECT * FROM range(500)", 500), daemon=True),
    ]
    try:
        for t in threads:
            t.start()
        time.sleep(3)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)
    assert errors == []
    assert wrong == [], f"{len(wrong)} wrong-length results under concurrency: {wrong[:5]}"


def _sqlite_source(tmp_path, catalog: str) -> SimpleNamespace:
    """A table of a SQLite source, as the attach walk hands it over (native_backend._attach_tbl),
    named under ``catalog``."""
    path = tmp_path / "inquiries.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS inquiries (id INTEGER)")
    return SimpleNamespace(
        id="inquiries-sqlite",
        type=SourceType.sqlite,
        catalog=catalog,
        path=str(path),
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        federation_hints={},
        mapping={},
        max_live_concurrency=None,
        schema_name="main",
        table_name="inquiries",
    )


def test_a_source_named_under_provisa_admin_is_refused(control_plane, tmp_path):
    """Regression: a SQLite source handed over under ``provisa_admin`` (the catalog Trino names a
    source it cannot read in place) took the name as an in-memory catalog before the control plane
    was attached; the control plane's attach then counted itself done, and the source's drop
    detached the catalog, so every meta read failed on a catalog that was gone.

    The refusal is caught as the ValueError it is and only then checked for its class, so against
    a runtime without the refusal the test fails on the attach that went through (DID NOT RAISE),
    not on importing a name that runtime does not have."""
    rt = DuckDBFederationRuntime()
    try:
        with pytest.raises(ValueError, match="'provisa_admin', which is reserved") as refused:
            rt.attach_source(_sqlite_source(tmp_path, "provisa_admin"))
        assert isinstance(refused.value, duckdb_runtime.ReservedCatalog)
        rt.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
        rt.drop_source("inquiries-sqlite")
        _register(control_plane, 1, "orders")
        assert [r[0] for r in rt.run_sync(_REGISTERED).rows] == ["orders"]
    finally:
        rt.close()


def test_a_detached_control_plane_is_attached_again(control_plane, runtime):
    """Whether the control plane is attached is read from the connection, so an attach after it
    went is made again rather than answered from memory."""
    runtime._con.execute('DETACH "provisa_admin"')
    runtime.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
    _register(control_plane, 1, "orders")
    assert [r[0] for r in runtime.run_sync(_REGISTERED).rows] == ["orders"]


def test_another_database_under_the_control_planes_name_is_refused(control_plane):
    rt = DuckDBFederationRuntime()
    try:
        rt._con.execute("ATTACH ':memory:' AS \"provisa_admin\"")
        with pytest.raises(RuntimeError, match="'provisa_admin' is attached as a duckdb database"):
            rt.attach_control_plane(libpq_dsn(control_plane.org().engine.url))
    finally:
        rt.close()
