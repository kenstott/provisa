# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f4a90-e1c6-4d35-8f02-6a9d3c5e7b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A worker's per-source state follows the ``sources`` rows its model reload reads (REQ-1914).

The worker that registers, changes or deletes a source updates its own pool and maps in the
mutation. Every other worker reaches the same state when its schema build reads the rows:

* the first build records the rows and touches nothing (the boot built the state from them);
* an added or changed source is (re)built — a changed one after its old pool is dropped;
* a deleted source loses its pool and its entries;
* a source the config declares is built from the config's entry, where its credentials are;
* a failure to build source state is logged and does not stop the schema build;
* a retired source (REQ-2248) is not a registered source: the rows a build reads leave it out,
  so it is given no state, and a restored one is built on the next pass."""

# Requirements: REQ-1914, REQ-2248

from __future__ import annotations

import ast
import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from sqlalchemy import insert

from provisa.api import model_reload
from provisa.api.org_runtime import OrgRuntime
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.core.repositories.retirement import RETIRED_MARK
from provisa.core.schema_org import sources


def _row(sid: str, host: str = "db1") -> dict:
    return {
        "id": sid,
        "type": "postgresql",
        "host": host,
        "port": 5432,
        "database": "d",
        "username": "u",
        "password_ref": "",
        "path": None,
        "federation_hints": {},
        "binding": "own",
        "synthetic": None,
        "description": "",
        # REQ-1919: the pool's settings are the row's.
        "pool_min": 1,
        "pool_max": 5,
        "use_pgbouncer": False,
        "pgbouncer_port": 6432,
    }


@pytest.fixture
def worker(monkeypatch):
    """A worker's state with one runtime, and the pool builder recorded instead of dialled."""
    runtime = OrgRuntime(org_id="acme")
    runtime.source_pools = SimpleNamespace(remove=AsyncMock())
    state = SimpleNamespace(
        _active_runtime=lambda: runtime,
        source_pools=runtime.source_pools,
        graphql_remote_sources={},
        config=None,
    )
    built: list[list[Source]] = []

    async def _build(config, extra_sources=None):
        assert list(config.sources) == []
        built.append(list(extra_sources or []))

    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setattr("provisa.api.app_loaders._build_source_pools_and_enums", _build)
    return SimpleNamespace(state=state, runtime=runtime, built=built)


def _reconcile(rows: list[dict]) -> None:
    asyncio.run(model_reload.reconcile_sources({r["id"]: r for r in rows}))


def test_the_first_build_records_the_rows_and_builds_nothing(worker):
    _reconcile([_row("a"), _row("b")])
    assert worker.built == []
    worker.runtime.source_pools.remove.assert_not_awaited()
    assert set(worker.runtime.source_rows) == {"a", "b"}


def test_unchanged_rows_build_nothing(worker):
    _reconcile([_row("a")])
    _reconcile([_row("a")])
    assert worker.built == []


def test_a_source_another_worker_registered_is_built_here(worker):
    _reconcile([_row("a")])
    _reconcile([_row("a"), _row("b")])
    # Only the new source: an unchanged one is not contacted, so a registered source whose
    # server is down does not hold up (or fail) the build of the one just added.
    assert [[s.id for s in batch] for batch in worker.built] == [["b"]]
    worker.runtime.source_pools.remove.assert_not_awaited()  # nothing existing is torn down
    assert set(worker.runtime.source_rows) == {"a", "b"}


def test_a_source_another_worker_changed_gets_a_new_pool(worker):
    _reconcile([_row("a", host="db1")])
    _reconcile([_row("a", host="db2")])
    worker.runtime.source_pools.remove.assert_awaited_once_with("a")
    assert [s.host for s in worker.built[0]] == ["db2"]


def test_a_change_that_is_not_to_the_connection_builds_nothing(worker):
    _reconcile([_row("a")])
    _reconcile([{**_row("a"), "description": "renamed in the catalog"}])
    assert worker.built == []


def test_a_source_another_worker_deleted_loses_its_pool_and_entries(worker):
    _reconcile([_row("a"), _row("b")])
    worker.runtime.source_types.update(a="postgresql", b="postgresql")
    worker.runtime.source_catalogs.update(a="a", b="b")
    worker.state.graphql_remote_sources["b"] = object()
    worker.runtime.pg_enums_by_source.update(a=({"mood": ["sad"]}, {}), b=({"size": ["s"]}, {}))
    _reconcile([_row("a")])
    worker.runtime.source_pools.remove.assert_awaited_once_with("b")
    assert set(worker.runtime.pg_enums_by_source) == {"a"}
    assert set(worker.runtime.pg_enum_types) == {"mood"}
    assert worker.runtime.source_types == {"a": "postgresql"}
    assert worker.runtime.source_catalogs == {"a": "a"}
    assert worker.state.graphql_remote_sources == {}
    assert worker.built == []
    assert set(worker.runtime.source_rows) == {"a"}


def test_a_source_another_worker_deleted_is_detached_from_this_workers_engine(worker, monkeypatch):
    """The deleting worker detaches the source from its engine (``_drop_source_on_engine``).
    Every other worker learns of the deletion only from its model reload, and must detach it
    from ITS engine too — an in-process engine (DuckDB's attach, a pgwire replica endpoint) is
    per worker, so a catalog left attached here outlives the source on this worker."""
    dropped: list[tuple[str, str | None]] = []
    stopped: list[str] = []
    worker.state.federation_engine = SimpleNamespace(
        drop_source=lambda sid, catalog_name=None: dropped.append((sid, catalog_name))
    )
    worker.state.source_catalogs = worker.runtime.source_catalogs
    monkeypatch.setattr("provisa.federation.pgwire_replica.stop_endpoint", stopped.append)
    _reconcile([_row("a"), _row("b")])
    worker.runtime.source_catalogs.update(a="a_cat", b="b_cat")
    _reconcile([_row("a")])
    # detached under the catalog name it was attached under, before that name is forgotten
    assert dropped == [("b", "b_cat")]
    assert stopped == ["b"]
    assert worker.runtime.source_catalogs == {"a": "a_cat"}


def test_a_built_in_source_is_never_reconciled(worker):
    _reconcile([_row("a")])
    _reconcile([_row("a"), _row("provisa-admin")])
    assert worker.built == []


def test_a_source_the_config_declares_is_built_from_its_row(worker):
    """REQ-1919: after the seed the store alone owns the model — a source the file declares is
    built from its row, so an admin's edit governs and the file's declaration is not read."""
    declared = Source(
        id="a",
        type=SourceType.postgresql,
        host="file-host",
        port=5432,
        database="d",
        username="u",
        password="${env:PG_PASSWORD}",
    )
    worker.state.config = SimpleNamespace(sources=[declared], domains=[])
    _reconcile([_row("a", host="db1")])
    _reconcile([_row("a", host="db2")])
    (built,) = worker.built[0]
    assert (built.host, built.password) == ("db2", "")


def test_a_failure_to_build_source_state_does_not_stop_the_schema_build(
    worker, monkeypatch, caplog
):
    async def _refused(config, extra_sources=None):
        raise RuntimeError("the vault could not be opened")

    monkeypatch.setattr("provisa.api.app_loaders._build_source_pools_and_enums", _refused)
    _reconcile([_row("a")])
    with caplog.at_level(logging.ERROR):
        _reconcile([_row("a"), _row("b")])  # does not raise
    assert "source state could not be brought in line" in caplog.text
    # Not recorded: the next build finds `b` still unbuilt and tries again.
    assert set(worker.runtime.source_rows) == {"a"}


# --- REQ-2248: a retired source is not a registered source ---------------------------------------


async def _register(conn, sid: str) -> None:
    await conn.execute_core(
        insert(sources).values(
            id=sid, type="postgresql", host="db1", port=5432, database="d", username="u"
        )
    )


async def _pass(db) -> dict[str, dict]:
    """One schema build's source step: the rows it reads, then this worker brought in line."""
    async with db.acquire() as conn:
        rows = await model_reload.registered_source_rows(conn)
    await model_reload.reconcile_sources(rows)
    return rows


@pytest.fixture
async def plane(control_plane):
    """A control plane holding the sources ``kept`` and ``old``."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await _register(conn, "kept")
        await _register(conn, "old")
    return db


async def test_a_retired_source_is_not_among_the_rows_a_build_reads(plane):
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "old", reason="moved")
        rows = await model_reload.registered_source_rows(conn)
    assert "kept" in rows
    assert "old" not in rows
    assert not [sid for sid in rows if RETIRED_MARK in sid]


async def test_a_source_added_after_another_was_retired_is_built_here(plane, worker, caplog):
    """Another worker retires ``old`` and registers ``new``. This worker's next build raises
    nothing, builds ``new`` and holds nothing for the retired source."""
    await _pass(plane)  # this worker's first build: both sources in service
    worker.runtime.source_types.update(kept="postgresql", old="postgresql")
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "old", reason="moved")
        await _register(conn, "new")
    with caplog.at_level(logging.ERROR):
        await _pass(plane)
    assert "source state could not be brought in line" not in caplog.text
    assert [[s.id for s in batch] for batch in worker.built] == [["new"]]
    worker.runtime.source_pools.remove.assert_awaited_once_with("old")
    assert {"kept", "new"} <= set(worker.runtime.source_rows)
    assert "old" not in worker.runtime.source_rows
    assert "old" not in worker.runtime.source_types
    assert not [sid for sid in worker.runtime.source_rows if RETIRED_MARK in sid]


async def test_a_runtime_that_never_saw_the_retired_source_builds_the_next_one(
    plane, worker, caplog
):
    """A runtime whose first build comes after the retirement: the retired source was never
    known to it, and a source registered afterwards is still built."""
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "old", reason="moved")
    await _pass(plane)  # the first build, with the retired row in the store
    assert not [sid for sid in worker.runtime.source_rows if RETIRED_MARK in sid]
    async with plane.acquire() as conn:
        await _register(conn, "new")
    with caplog.at_level(logging.ERROR):
        await _pass(plane)
    assert "source state could not be brought in line" not in caplog.text
    assert [[s.id for s in batch] for batch in worker.built] == [["new"]]
    assert "new" in worker.runtime.source_rows


async def test_a_restored_source_is_built_on_the_next_pass(plane, worker, caplog):
    await _pass(plane)
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "old", reason="moved")
    await _pass(plane)
    assert "old" not in worker.runtime.source_rows
    async with plane.acquire() as conn:
        retired = await source_repo.list_all(conn, retired=True)
        assert [r["retired_id"] for r in retired] == ["old"]
        assert await source_repo.restore(conn, retired[0]["id"])
    with caplog.at_level(logging.ERROR):
        await _pass(plane)
    assert "source state could not be brought in line" not in caplog.text
    assert [[s.id for s in batch] for batch in worker.built] == [["old"]]
    assert "old" in worker.runtime.source_rows


_SOURCES_TABLE_NAMES = {"sources", "sources_t", "_sources_t"}


def _whole_table_source_reads(code: str) -> list[int]:
    """The lines of ``code`` that select every row of the ``sources`` table: ``select(sources)``
    with no ``.where(...)`` on it, so retired rows included."""
    tree = ast.parse(code)
    filtered = {
        id(node.func.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "where"
    }
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "select"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id in _SOURCES_TABLE_NAMES
        and id(node) not in filtered
    ]


def test_a_whole_table_read_of_sources_is_recognised():
    code = (
        "async def build(conn):\n"
        "    every = await conn.execute_core(select(_sources_t))\n"
        "    live = await conn.execute_core(select(sources).where(sources.c.retired.is_(False)))\n"
    )
    assert _whole_table_source_reads(code) == [2]


@pytest.mark.parametrize(
    "module", ["provisa/api/app.py", "provisa/api/apply_dry_run.py", "provisa/api/model_reload.py"]
)
def test_what_builds_a_runtimes_source_state_reads_no_retired_row(module):
    """The schema build, the cloned environment's install and the configuration dry run each take
    their source rows from ``registered_source_rows``: none selects the whole ``sources`` table,
    which holds the retired rows under keys that are no source id (REQ-2248)."""
    code = (Path(__file__).parents[2] / module).read_text()
    assert _whole_table_source_reads(code) == []
    if module != "provisa/api/model_reload.py":
        assert "registered_source_rows(conn)" in code
