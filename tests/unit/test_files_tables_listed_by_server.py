# Copyright (c) 2026 Kenneth Stott
# Canary: 98656fad-d507-4746-8136-9d7eb3addbd3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source the engine reads through its bundled connector server (files on DuckDB) has its tables
listed from that server's information_schema as soon as it listens -- not through the engine's
attach, which waits for the server's first catalog query: that counts every table's rows, and on a
cold CI runner it ran past the Register Table form's 30s wait (core lane, alpha.482 round 2). An
engine with a reader of its own (Trino's file catalog) still lists through the engine."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.api.admin import introspect
from provisa.federation import pgwire_replica

pytestmark = pytest.mark.asyncio


@pytest.fixture
def server(monkeypatch):
    asked: list[tuple[str, str]] = []

    async def _source(source_id):
        return SimpleNamespace(id=source_id, type="files")

    @asynccontextmanager
    async def _bound():
        yield

    async def _tables(source, schema):
        asked.append((source.id, schema))
        return ["categories", "customers", "orders"]

    monkeypatch.setattr(introspect, "_adapter_source", _source)
    monkeypatch.setattr(introspect, "_adapter_bound", _bound)
    monkeypatch.setattr(pgwire_replica, "adapter_tables", _tables)
    return asked


# No direct connection to the source: what is not listed natively falls to the engine.
_NO_POOL = SimpleNamespace(has=lambda _source_id: False)


def _state(engine):
    return SimpleNamespace(federation_engine=SimpleNamespace(engine=engine))


async def test_on_duckdb_a_files_sources_tables_come_from_its_server(server):
    from provisa.federation.engine import build_duckdb_engine

    listed = await introspect.native_tables(
        "e2e-northwind", "files", "E2E_Northwind", None, None, _state(build_duckdb_engine())
    )
    assert [t.name for t in listed] == ["categories", "customers", "orders"]
    assert server == [("e2e-northwind", "e2e_northwind")]


async def test_on_trino_a_files_sources_tables_come_from_the_engine(server):
    from provisa.federation.engine import build_trino_engine

    listed = await introspect.native_tables(
        "e2e-northwind", "files", "e2e_northwind", _NO_POOL, None, _state(build_trino_engine())
    )
    assert listed is None  # the engine's own file catalog lists them
    assert server == []


def _bundle_not_staged():
    from pathlib import Path

    from provisa.runtime_deps.pgwire_bundles import BundleNotStaged, bundle_spec_for

    spec = bundle_spec_for("files")
    return BundleNotStaged(spec, Path("/nowhere") / spec.version / spec.artifact_name)


@pytest.fixture
def unstaged_bundle_on_duckdb(monkeypatch):
    """A files source on DuckDB whose pinned pgwire-file bundle is not staged: every read of its
    server -- the listing on the request, and the engine's own attach -- is refused by name."""
    from provisa.api import app as app_module
    from provisa.api.admin import schema_query
    from provisa.federation.engine import build_duckdb_engine

    async def _source(source_id):
        return SimpleNamespace(id=source_id, type="files")

    @asynccontextmanager
    async def _bound():
        yield

    async def _refused(source, schema):
        raise _bundle_not_staged()

    class _Pool:
        @asynccontextmanager
        async def acquire(self):
            yield None

    async def _pool():
        return _Pool()

    def _seam_refused(src, schema):
        raise _bundle_not_staged()

    async def _no_floor(source_id, what):
        return None

    async def _src(source_id):
        return SimpleNamespace(id=source_id, type="files")

    engine = SimpleNamespace(engine=build_duckdb_engine(), introspect_tables=_seam_refused)
    monkeypatch.setattr(introspect, "_adapter_source", _source)
    monkeypatch.setattr(introspect, "_adapter_bound", _bound)
    monkeypatch.setattr(pgwire_replica, "adapter_tables", _refused)
    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_require_live_attach", _no_floor)
    monkeypatch.setattr(schema_query, "_source_for_introspection", _src)
    monkeypatch.setattr(app_module.state, "source_types", {"test3": "files"}, raising=False)
    monkeypatch.setattr(app_module.state, "source_pools", _NO_POOL, raising=False)
    monkeypatch.setattr(app_module.state, "federation_engine", engine, raising=False)


async def test_a_bundle_not_staged_is_the_register_lists_answer_not_an_empty_list(
    unstaged_bundle_on_duckdb,
):
    """The File Crawler's Register tables step said "This schema has no tables" when the pinned
    pgwire-file bundle was not staged (the pin moved to engine-v0.131.1): the refusal, which names
    the bundle and the stage command, was swallowed as a best-effort read and the list was empty."""
    from provisa.api.admin.schema_query import offered_tables
    from provisa.runtime_deps.pgwire_bundles import BundleNotStaged

    with pytest.raises(BundleNotStaged, match="pgwire-file.*not staged.*stage_assets"):
        await offered_tables("test3", "test3")
