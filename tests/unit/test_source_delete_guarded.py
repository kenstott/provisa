# Copyright (c) 2026 Kenneth Stott
# Canary: 0a4ae16c-17ac-45ef-a731-f002e1ed907f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source is deleted only when no table is registered against it (REQ-1918).

The model store's ``source.delete`` used to delete the source's registered tables with it —
and, through them on PostgreSQL, every relationship, row filter and tag that referred to those
tables. It now asks the dependency guard: while a table is registered against the source or a
command is bound to it, the delete is refused naming each one and nothing is removed. When it
may go, its remote-registration rows and its tag assignments go with it.
"""

# Requirements: REQ-1918, REQ-1919, REQ-014, REQ-2248

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.repositories import source as source_repo
from provisa.core.schema_org import (
    api_endpoints,
    api_sources,
    domains,
    registered_tables,
    sources,
    tag_assignments,
    tracked_functions,
)
from tests.unit.gate_identity import serve_every_column


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which dependents a caller may be told of is REQ-2152's own
    tests (test_refusal_read_rights)."""
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    """``crm`` has a registered table and a command bound to it; ``api`` has neither, and an API
    registration with an endpoint and a tag."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
        for source_id, kind in (("crm", "postgresql"), ("api", "openapi")):
            await conn.execute_core(insert(sources).values(id=source_id, type=kind))
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="crm",
                domain_id="sales",
                schema_name="public",
                table_name="customers",
            )
        )
        await conn.execute_core(
            insert(tracked_functions).values(name="refund", source_id="crm", domain_id="sales")
        )
        await conn.execute_core(
            insert(api_sources).values(id="api", type="openapi", base_url="http://x")
        )
        await conn.execute_core(
            insert(api_endpoints).values(source_id="api", path="/p", table_name="t", columns=[])
        )
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="pii",
                base_tag_id="pii",
                object_type="source",
                object_key="api",
                source_id="api",
            )
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


async def _source_ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r["id"] for r in await source_repo.list_all(conn)}


async def test_a_source_with_a_table_or_a_command_is_refused_naming_each(plane):
    async with plane.acquire() as conn:
        table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
        with pytest.raises(source_repo.SourceDeleteRefused) as err:
            await source_repo.delete(conn, "crm")
    assert err.value.reason == "dependents"
    assert [(d.ref.kind, d.ref.id, d.via) for d in err.value.dependents] == [
        ("command", "refund", ("tracked_functions.source_id",)),
        ("table", table_id, ("registered_tables.source_id",)),
    ]
    assert "crm" in await _source_ids(plane)
    assert await _count(plane, registered_tables) == 1  # its table was NOT taken with it


async def test_a_source_nothing_is_registered_against_is_retired_keeping_its_registration_rows(
    plane,
):
    """REQ-2248 (amended 2026-11-15): the delete retires the source; nothing is removed -- its
    registration rows and its tag are kept with it, under the key it is retired at."""
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "api") is True
        assert await source_repo.delete(conn, "api") is False
        [retired] = await source_repo.list_all(conn, retired=True)
    assert "api" not in await _source_ids(plane)
    assert retired["retired_id"] == "api"
    async with plane.acquire() as conn:
        assert (await conn.execute_core(select(api_sources.c.id))).scalar_one() == retired["id"]
        assert (await conn.execute_core(select(api_endpoints.c.source_id))).scalar_one() == retired[
            "id"
        ]
        assert (
            await conn.execute_core(select(tag_assignments.c.source_id))
        ).scalar_one() == retired["id"]


async def test_the_source_goes_once_its_table_and_command_have(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(registered_tables.delete())
        await conn.execute_core(tracked_functions.delete())
        assert await source_repo.delete(conn, "crm") is True


async def test_a_source_the_deployment_keeps_is_refused(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="duckdb"))
        with pytest.raises(source_repo.SourceDeleteRefused) as err:
            await source_repo.delete(conn, DERIVED_SOURCE_ID)
    assert err.value.reason == "system" and DERIVED_SOURCE_ID in await _source_ids(plane)


async def test_a_declared_set_is_removed_without_the_guard(plane):
    async with plane.acquire() as conn:
        await source_repo.remove_where(conn, sources.c.id.not_in(["api"]))
    assert await _source_ids(plane) == {"api"}


# --- the mutation --------------------------------------------------------------------------------


@pytest.fixture
def served(plane, monkeypatch) -> dict[str, list]:
    seen: dict[str, list] = {"rebuilds": [], "dropped": []}

    async def _pool():
        return plane

    async def _rebuild() -> None:
        seen["rebuilds"].append(1)

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(
        schema_mutation, "_drop_source_on_engine", lambda _state, sid: seen["dropped"].append(sid)
    )
    monkeypatch.setattr(appmod.state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(appmod.state, "source_catalogs", {}, raising=False)
    return seen


async def _delete_source(source_id: str) -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info = types.SimpleNamespace(context={"request": request})
    return await schema_mutation.Mutation().delete_source(info, source_id)  # type: ignore[arg-type]


async def test_the_mutation_refuses_with_the_list_and_leaves_the_engine_alone(plane, served):
    result = await _delete_source("crm")
    assert (result.success, result.code) == (False, "schema.source_has_dependents")
    assert {(d["kind"], d["via"][0]) for d in result.params["dependents"]} == {
        ("command", "tracked_functions.source_id"),
        ("table", "registered_tables.source_id"),
    }
    assert "crm" in await _source_ids(plane)
    assert served == {"rebuilds": [], "dropped": []}


async def test_the_mutation_deletes_drops_the_catalog_and_rebuilds(plane, served):
    result = await _delete_source("api")
    assert (result.success, result.code) == (True, "schema.source_deleted")
    # The source's row goes with its credentials in it; nothing is removed from a vault.
    assert served == {"rebuilds": [1], "dropped": ["api"]}


async def test_the_mutation_answers_not_found(plane, served):
    result = await _delete_source("nowhere")
    assert (result.success, result.code) == (False, "schema.source_not_found")


async def test_no_server_is_started_for_the_source_while_it_is_deleted(plane, served, monkeypatch):
    """REQ-1824: from before its row goes until its server is stopped, the source is marked as
    being deleted, so nothing starts it a server the deletion would never stop; afterwards the
    mark is gone -- a refused deletion included."""
    from provisa.federation import pgwire_replica as pr

    monkeypatch.setattr(pr, "_DELETING", set())
    marked: list[bool] = []

    def _drop(_state, sid: str) -> None:
        from provisa.core.org_scope import source_key

        marked.append(source_key(sid) in pr._DELETING)

    monkeypatch.setattr(schema_mutation, "_drop_source_on_engine", _drop)
    assert (await _delete_source("api")).success
    assert marked == [True]
    assert pr._DELETING == set()
    assert not (await _delete_source("crm")).success
    assert pr._DELETING == set()


async def test_the_deletion_does_not_wait_for_a_catalog_walk_inside_an_adapter_read(plane, served):
    """REQ-1824: deleteSource took 88 s and 125 s in CI, waiting for the source's catalog walk to
    end while the walk sat in an adapter read. The walk is cancelled, not waited for."""
    import asyncio
    import threading
    import time

    from provisa.core.connection_loop import spawn_background
    from provisa.discovery.catalog_cache import index_task_name

    reading, answer = threading.Event(), threading.Event()

    async def _walk() -> None:
        await asyncio.to_thread(reading.set)
        await asyncio.to_thread(answer.wait, 10)  # inline on its background loop

    walk = spawn_background(_walk(), name=index_task_name("index", "api"))
    try:
        assert await asyncio.to_thread(reading.wait, 5)
        began = time.monotonic()
        result = await _delete_source("api")
        assert time.monotonic() - began < 2
        assert (result.success, served["dropped"]) == (True, ["api"])
        assert not walk.done()
    finally:
        answer.set()
    await asyncio.wait_for(asyncio.wrap_future(walk), 5)


# --- a code source's native host (REQ-2225) ------------------------------------------------------


@pytest.fixture
def hosted(plane, served, tmp_path, monkeypatch):
    """Code sources ``spark_jobs`` (a host this server runs, running) and ``spark_ext`` (a host
    its operator runs), deleted in org ``acme``."""
    import subprocess
    import sys

    from provisa.codesource import native_connector
    from provisa.core.request_context import reset_current_org, set_current_org

    monkeypatch.setattr(native_connector, "state_dir", lambda o, s: tmp_path / o / s)
    monkeypatch.setattr(native_connector, "_hosts", {})
    work = tmp_path / "acme" / "spark_jobs"
    (work / "state").mkdir(parents=True)
    process = subprocess.Popen(  # noqa: S603 -- the test's interpreter
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    native_connector._hosts[("acme", "spark_jobs")] = native_connector._Host(process, 40123)  # pyright: ignore[reportPrivateUsage]
    token = set_current_org("acme")
    yield {"process": process, "work": work}
    reset_current_org(token)
    native_connector.stop_all()


async def _insert_code_source(plane: Database, source_id: str, remote: dict) -> None:
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(
                id=source_id, type="python", mapping={"mode": "built", "remote": remote}
            )
        )


async def test_deleting_a_native_source_stops_its_host_and_removes_its_state(plane, hosted):
    import os

    from provisa.codesource import native_connector

    await _insert_code_source(plane, "spark_jobs", {"host": "127.0.0.1", "native": "script"})
    assert (await _delete_source("spark_jobs")).success
    process = hosted["process"]
    assert process.poll() is not None
    with pytest.raises(ProcessLookupError):
        os.kill(process.pid, 0)
    assert not hosted["work"].exists()
    with pytest.raises(native_connector.NativeHostFailed, match="has no state"):
        native_connector.ensure_running("acme", "spark_jobs", "d" * 64, "script", 40123)


async def test_deleting_an_image_source_touches_no_host(plane, hosted, monkeypatch):
    from provisa.codesource import native_connector

    def refuse(*_args):
        raise AssertionError("an image source's host is its operator's")

    monkeypatch.setattr(native_connector, "stop", refuse)
    await _insert_code_source(plane, "spark_ext", {"host": "10.0.0.9", "port": 7443})
    assert (await _delete_source("spark_ext")).success
    assert hosted["process"].poll() is None
    assert hosted["work"].exists()
