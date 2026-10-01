# Copyright (c) 2026 Kenneth Stott
# Canary: ee9e9f71-eee8-4c83-9587-266c00af0e0e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865: row_materialized_tables_by_name must key by the SEMANTIC table name
(apply_sql_name(alias or table_name)) -- the same name extract_pk_bounds matches against in
_resolve_pk_bounds's already-semantic AST (governed_semantic). A table registered with an alias
that differs from its physical table_name (every neo4j/query_template table in the perf-bench
demo: bench_order_node -> alias "Order") appears under that alias in semantic SQL, never its
physical name. Keying by bare table_name alone silently matched nothing for such a table --
confirmed live on the perf-bench VM: every row_materialize-enabled neo4j table fell through to
the pre-existing full-source land on every query, since extract_pk_bounds never errors on a miss.
"""

from __future__ import annotations

import pytest

from provisa.compiler.naming import apply_sql_name
from provisa.compiler.pk_bounds import extract_pk_bounds
from provisa.federation.query_residency import row_materialized_tables_by_name

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _registered_sources(monkeypatch):
    """The tables' source (row_materialize applies only when the engine can't attach its type)."""
    from types import SimpleNamespace

    async def _fake_sources(state):
        return [SimpleNamespace(id="bench-neo4j", type=SimpleNamespace(value="neo4j"))]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _fake_sources)


def _table(**kw):
    from provisa.core.models import Column, Table

    defaults = dict(
        source_id="bench-neo4j",
        domain_id="perf-bench",
        schema_name="neo4j",
        table_name="bench_order_node",
        alias="Order",
        row_materialize=True,
        cache_ttl=300,
        columns=[
            Column(
                name="order_id", visible_to=["org_admin"], data_type="integer", is_primary_key=True
            ),
            Column(name="customer_id", visible_to=["org_admin"], data_type="integer"),
        ],
    )
    defaults.update(kw)
    return Table(**defaults)


@pytest.mark.asyncio
async def test_keyed_by_both_alias_and_bare_table_name_when_alias_differs(monkeypatch):
    """Dual-keyed by design (REQ-1865 amendment, see the function's own docstring): a raw-SQL
    statement (pgwire/Flight) references the table by its bare physical name, never rewritten,
    while a GraphQL-compiled statement rewrites it to the alias -- keying by only one form silently
    matched nothing for a statement using the other."""
    table = _table()

    async def _fake_registered(state):
        return [table]

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _fake_registered)
    row_tables = await row_materialized_tables_by_name(state=None)
    assert set(row_tables) == {apply_sql_name("Order"), "bench_order_node"}


@pytest.mark.asyncio
async def test_falls_back_to_physical_table_name_when_no_alias(monkeypatch):
    table = _table(alias=None, table_name="orders")

    async def _fake_registered(state):
        return [table]

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _fake_registered)
    row_tables = await row_materialized_tables_by_name(state=None)
    assert set(row_tables) == {apply_sql_name("orders")}


@pytest.mark.asyncio
async def test_end_to_end_pk_bounds_resolves_against_the_semantic_alias_reference(monkeypatch):
    """The exact query that hung on the perf-bench VM: FROM perf_bench.order (the semantic alias),
    filtering on order_id -- must resolve a pk bound, not silently fall through to a full land."""
    import sqlglot

    table = _table()

    async def _fake_registered(state):
        return [table]

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _fake_registered)
    row_tables = await row_materialized_tables_by_name(state=None)
    ast = sqlglot.parse_one(
        "SELECT order_id, customer_id FROM perf_bench.order WHERE order_id = 3238114",
        read="postgres",
    )
    bounds = extract_pk_bounds(ast, row_tables)
    assert len(bounds) == 1
    assert bounds[0].table_name == "bench_order_node"
    assert bounds[0].values == ((3238114,),)
