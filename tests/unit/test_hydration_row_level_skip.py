# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2a9c53-4d1f-4b86-8a30-c5f6e1d9b742
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL's API-table hydration leaves a row-level replica table alone (REQ-1865).

A Neo4j table is registered as an API endpoint (REQ-1668). Before the engine runs a GraphQL
query, ``_hydrate_api_tables_before_engine`` fills the whole-table API cache
(``"default"."<table>"``) of every endpoint of every source the query reads. A table replicated
row by row (``row_materialize``) has no such cache table — its rows live in the row-level replica,
filled by key — so the fill's freshness read failed: ``relation "default.bench_order_node" does
not exist`` on ``SELECT _cached_at FROM "default"."bench_order_node" WHERE _params_hash = …``,
for every GraphQL query that touched the source. The raw-SQL/compiled path already skips these
tables (``_materialize_api_to_engine_cache``); this is the same rule."""

# Requirements: REQ-1865, REQ-1668

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data import hydration

pytestmark = pytest.mark.asyncio


class _Db:
    def __init__(self) -> None:
        self.acquires = 0

    def acquire(self):
        self.acquires += 1
        raise AssertionError("a row-level replica table was read as an API cache table")


def _endpoint(source_id: str, table_name: str):
    return SimpleNamespace(
        source_id=source_id,
        table_name=table_name,
        ttl=300,
        columns=[],
        path="/",
        response_root=None,
        error_path=None,
        pk_column="order_id",
    )


def _state(tables):
    return SimpleNamespace(
        api_endpoints={
            ("neo", "bench_order_node"): _endpoint("neo", "bench_order_node"),
            ("neo", "bench_placed_edge"): _endpoint("neo", "bench_placed_edge"),
        },
        api_sources={"neo": SimpleNamespace(base_url="http://neo")},
        tenant_db=_Db(),
        tables=tables,
    )


@pytest.fixture(autouse=True)
def _fresh_expiry(monkeypatch):
    monkeypatch.setattr(hydration, "_source_hydration_expiry", {})


async def test_a_row_level_table_is_not_filled_as_an_api_cache_table(monkeypatch):
    filled: list[str] = []

    async def _collection(src, endpoint, *args):
        filled.append(endpoint.table_name)

    monkeypatch.setattr(hydration, "_hydrate_collection", _collection)
    state = _state(
        [
            {"table_name": "bench_order_node", "source_id": "neo", "row_materialize": True},
            {"table_name": "bench_placed_edge", "source_id": "neo", "row_materialize": False},
        ]
    )
    compiled = SimpleNamespace(sources={"neo"}, api_args={})
    ctx = SimpleNamespace(joins={}, tables={})
    await hydration._hydrate_api_tables_before_engine(compiled, ctx, state)
    assert filled == ["bench_placed_edge"], "the row-level table went through the API-cache fill"
    assert state.tenant_db.acquires == 0
