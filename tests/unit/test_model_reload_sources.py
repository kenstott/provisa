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
* a failure to build source state is logged and does not stop the schema build."""

# Requirements: REQ-1914

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api import model_reload
from provisa.api.org_runtime import OrgRuntime
from provisa.core.models import Source, SourceType


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
        "bound": True,
        "description": "",
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
    assert [[s.id for s in batch] for batch in worker.built] == [["a", "b"]]
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
    _reconcile([_row("a")])
    worker.runtime.source_pools.remove.assert_awaited_once_with("b")
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


def test_a_source_the_config_declares_is_built_from_the_config(worker):
    declared = Source(
        id="a",
        type=SourceType.postgresql,
        host="db2",
        port=5432,
        database="d",
        username="u",
        password="${env:PG_PASSWORD}",
    )
    worker.state.config = SimpleNamespace(sources=[declared], domains=[])
    _reconcile([_row("a", host="db1")])
    _reconcile([_row("a", host="db2")])
    assert worker.built[0] == [declared]


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
