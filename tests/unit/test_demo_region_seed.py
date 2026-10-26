# Copyright (c) 2026 Kenneth Stott
# Canary: 4b9dca4d-a184-4892-a3b8-d75fdff670e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo in regions on the regions sandbox tier (REQ-2068): its tables are moved, a schema at a
time, between a seed and a region's store; a node waits only for what its own region keeps; each
tier reaps its own sandboxes. A region's PostgreSQL store is exercised by the integration test;
here the stores are DuckDB files, through the same mover."""

# Requirements: REQ-2068

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import duckdb
import pytest

from provisa.api import demo_region_seed
from provisa.api.demo_region_seed import copy_tables, held_rows, kept_here


def _store(path, schema: str, **tables: int) -> str:
    con = duckdb.connect(str(path))
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    for name, rows in tables.items():
        con.execute(f'CREATE TABLE "{schema}"."{name}" AS SELECT range AS id FROM range({rows})')
    con.close()
    return f"duckdb:///{path}"


def test_a_schema_is_copied_whole_or_by_table_and_replaces_what_is_there(tmp_path):
    src = _store(tmp_path / "src.duckdb", "org_tpl__region_eu_replicas", a=3, b=2)
    dst = _store(tmp_path / "dst.duckdb", "replicas", a=9)
    copied = copy_tables(src, "org_tpl__region_eu_replicas", dst, "replicas")
    assert copied == ["a", "b"]
    assert held_rows(dst, "replicas", ["a", "b", "c"]) == {"a": 3, "b": 2}
    assert copy_tables(src, "org_tpl__region_eu_replicas", dst, "only_b", ["b"]) == ["b"]
    assert held_rows(dst, "only_b", ["a", "b"]) == {"b": 2}


def test_a_schema_is_copied_within_one_store(tmp_path):
    store = _store(tmp_path / "region.duckdb", "org_tpl__region_eu_mv_cache", v=4)
    copy_tables(store, "org_tpl__region_eu_mv_cache", store, "org_sbxr1__region_eu_mv_cache")
    assert held_rows(store, "org_sbxr1__region_eu_mv_cache", ["v"]) == {"v": 4}
    assert held_rows(store, "org_tpl__region_eu_mv_cache", ["v"]) == {"v": 4}


def test_a_node_of_a_region_keeps_its_regions_tables_and_those_naming_none(monkeypatch):
    tables = [SimpleNamespace(region=r) for r in ("eu", "us", None)]
    monkeypatch.setattr(demo_region_seed, "regional", lambda: True)
    monkeypatch.setattr(demo_region_seed, "here", lambda: "us")
    assert [t.region for t in kept_here(tables)] == ["us", None]
    monkeypatch.setattr(demo_region_seed, "regional", lambda: False)
    assert kept_here(tables) == tables


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("regional", "here", "reaped"),
    [
        (True, "us", ["sbxrus00000000000a"]),
        (True, "warehouse", ["sbxrwarehouse00000000000b"]),
        (False, "us", ["sbx0a"]),
    ],
)
async def test_each_node_reaps_its_own_sandboxes(monkeypatch, regional, here, reaped):
    """The sandbox tier reaps its own; each node of the regions tier (REQ-2068), the sandboxes its
    region minted, never another node's."""
    from provisa.api import demo_sandbox

    rows = [("sbx0a",), ("sbxrus00000000000a",), ("sbxrwarehouse00000000000b",)]

    class _Conn:
        async def execute_core(self, _stmt):
            return SimpleNamespace(fetchall=lambda: rows)

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_a):
            return False

    monkeypatch.setattr(
        "provisa.api.admin.orgs_router._admin_pool",
        lambda: SimpleNamespace(acquire=lambda: _Acquire()),
    )
    monkeypatch.setattr(demo_region_seed, "regional", lambda: regional)
    monkeypatch.setattr(demo_region_seed, "here", lambda: here)
    monkeypatch.setattr(demo_sandbox.settings_registry, "value", lambda _k: 60)
    assert await demo_sandbox.expired(datetime.now(UTC) + timedelta(hours=1)) == reaped


def test_a_regions_demo_s_setup_history_is_carried_in_its_home_region_s_seed(tmp_path):
    """REQ-1162, REQ-2068: the setup builds a bitemporal view's history in its home region's store,
    as the setup's org; its seeding node then builds the view once more, as the template's. The
    history the bundle carries is the setup's: it replaces that view's table in the region's seed,
    and the seed's record names it."""
    from provisa.api.demo_region_seed import carry_setup_history, seed_dir, views_schema

    demo = tmp_path / "banking"
    seed = seed_dir(demo, "warehouse")
    seed.mkdir(parents=True)
    region_store = _store(
        tmp_path / "warehouse.duckdb", views_schema("default", "warehouse"), mv_eod_totals=30
    )
    _store(seed / "store.duckdb", "views", mv_eod_totals=1, mv_other=2)
    (seed / "record.json").write_text(json.dumps({"tables": [], "views": ["mv_other"]}))

    carry_setup_history(
        "default", demo, {"warehouse": ["mv_eod_totals"]}, {"warehouse": region_store}
    )

    assert held_rows(
        f"duckdb:///{seed / 'store.duckdb'}", "views", ["mv_eod_totals", "mv_other"]
    ) == {
        "mv_eod_totals": 30,
        "mv_other": 2,
    }
    assert json.loads((seed / "record.json").read_text())["views"] == ["mv_eod_totals", "mv_other"]


def test_a_bitemporal_view_the_setup_never_built_fails_the_build_by_name(tmp_path):
    from provisa.api.demo_region_seed import carry_setup_history, seed_dir, views_schema

    demo = tmp_path / "banking"
    seed = seed_dir(demo, "warehouse")
    seed.mkdir(parents=True)
    region_store = _store(tmp_path / "warehouse.duckdb", views_schema("default", "warehouse"), x=1)
    _store(seed / "store.duckdb", "views", mv_other=2)
    (seed / "record.json").write_text(json.dumps({"tables": [], "views": ["mv_other"]}))

    with pytest.raises(RuntimeError, match="mv_eod_totals"):
        carry_setup_history(
            "default", demo, {"warehouse": ["mv_eod_totals"]}, {"warehouse": region_store}
        )
