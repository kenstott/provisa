# Copyright (c) 2026 Kenneth Stott
# Canary: ec634bdc-8223-4242-94e7-0ce5b7e1a4c9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# REQ-984: the legacy periodic CTAS refresh_loop is retired; reclamation_loop is the sole
# storage-GC path (drop removed MVs + reap orphans) and never recomputes MVs.
import inspect

import pytest

import provisa.mv.refresh as refresh
from provisa.mv.models import MVDefinition
from provisa.mv.registry import MVRegistry


class _RecordingEngine:
    def __init__(self, tables):
        self._tables = list(tables)
        self.executed = []

    def address_replicas(self, sql):
        return sql  # this stand-in's tables are all read where the statement names them

    async def execute_engine(self, sql, *, authorization=None):
        del authorization  # REQ-1760: execute_engine's caller-accountability param; this fake
        # only records the SQL, verify_execution_authorization has its own unit tests.
        self.executed.append(sql)
        if sql.startswith("SHOW TABLES"):
            return type("R", (), {"rows": [(t,) for t in self._tables]})()
        return type("R", (), {"rows": []})()


def test_refresh_loop_is_retired_reclamation_loop_is_the_path():
    # The periodic CTAS refresh loop no longer exists; reclamation_loop replaces it.
    assert not hasattr(refresh, "refresh_loop")
    assert inspect.iscoroutinefunction(refresh.reclamation_loop)


@pytest.mark.asyncio
async def test_reclaim_removed_mv_drops_table_and_unregisters_without_recompute():
    reg = MVRegistry()
    kept = MVDefinition(id="keep", source_tables=["t"], target_catalog="c", target_schema="s")
    gone = MVDefinition(id="gone", source_tables=["t"], target_catalog="c", target_schema="s")
    reg.register(kept)
    reg.register(gone)
    engine = _RecordingEngine(tables=[])

    reclaimed = await refresh.reclaim_removed_mvs(engine, reg, config_mv_ids={"keep"})

    assert reclaimed == ["gone"]
    assert reg.get("gone") is None
    assert reg.get("keep") is kept
    # Only a DROP is issued — no CREATE TABLE AS / INSERT recompute.
    assert engine.executed == ['DROP TABLE IF EXISTS "c"."s"."mv_gone"']
    assert not any("CREATE TABLE" in s or "INSERT" in s for s in engine.executed)


@pytest.mark.asyncio
async def test_detect_orphans_returns_untracked_tables():
    reg = MVRegistry()
    reg.register(MVDefinition(id="a", source_tables=["t"], target_catalog="c", target_schema="s"))
    engine = _RecordingEngine(tables=["mv_a", "mv_stray"])

    orphans = await refresh.detect_orphans(engine, reg, schema_name="s", catalog="c")

    assert orphans == ["mv_stray"]


@pytest.mark.asyncio
async def test_a_copy_of_a_view_another_region_keeps_is_an_orphan_here():
    """REQ-1921: a view naming eu is kept only in eu — on a us node its table (left when its
    region changed) is reaped like a removed view's."""
    from provisa.core import process_region

    platform = {
        "regions": [
            {"id": "eu", "address": "https://eu.example.com"},
            {"id": "us", "address": "https://us.example.com"},
        ]
    }
    reg = MVRegistry()
    for mv_id, region in (("here", None), ("ours", "us"), ("theirs", "eu")):
        reg.register(
            MVDefinition(
                id=mv_id, source_tables=["t"], target_catalog="c", target_schema="s", region=region
            )
        )
    engine = _RecordingEngine(tables=["mv_here", "mv_ours", "mv_theirs"])
    was = process_region._region
    try:
        process_region.bind_launch(platform, requested="us")
        orphans = await refresh.detect_orphans(engine, reg, schema_name="s", catalog="c")
    finally:
        process_region._region = was
    assert orphans == ["mv_theirs"]
