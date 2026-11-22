# Copyright (c) 2026 Kenneth Stott
# Canary: 5c0e8b2d-7a41-4f36-9d15-e2b6a3f8c047
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-467 (amended 2026-11-20): SQL, Cypher and GraphQL resolve a model name through the names
table (REQ-194), so a renamed object resolves to the same target in all three, and a name the
names table does not hold resolves in none."""

from dataclasses import replace

import pytest
from graphql import parse

from provisa.compiler import names_table, naming
from provisa.compiler.names_table import NamesTable
from provisa.compiler.sql_gen import UnknownRootField, compile_query
from provisa.compiler.sql_rewrite import normalize_table_refs
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_sql_gen import _build_schema_and_ctx, _col

_DOMAINS = ["*", "meta", "ontology", "ops"]


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


def _ctx(alias: str | None = None):
    """One table of the sales domain, physical ``public.orders``, with ``alias`` as its name."""
    table = {
        "id": 1,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "alias": alias,
        "columns": [
            {"column_name": "id", "visible_to": ["admin"]},
            {"column_name": "amount", "visible_to": ["admin"]},
        ],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }
    _, ctx = _build_schema_and_ctx(
        tables=[table],
        relationships=[],
        column_types={1: [_col("id", "integer"), _col("amount", "decimal(10,2)")]},
    )
    return ctx


def _sql_target(ctx, sql: str) -> str:
    return normalize_table_refs(sql, ctx)


def _cypher_sql(ctx, cypher: str) -> str:
    lm = CypherLabelMap.from_schema(ctx, domain_access=_DOMAINS)
    return cypher_to_sql(parse_cypher(cypher), lm, {})[0].sql(dialect="postgres")


def _graphql_sql(ctx, query: str) -> str:
    return compile_query(parse(query), ctx)[0].sql


# ── a renamed object resolves to one target in every language ─────────────────────────────────


def test_an_aliased_table_resolves_in_sql_cypher_and_graphql_by_the_names_tables_names():
    ctx = _ctx(alias="purchases")
    names = names_table.context_names(ctx)
    sql_name = names.table_row(1, "sql").name
    label = names.table_row(1, "cypher_label").name
    field = names.table_row(1, "graphql_field").name
    assert (sql_name, label, field) == ("purchases", "Purchases", "purchases")

    assert '"public"."orders"' in _sql_target(ctx, f"SELECT id FROM sales.{sql_name}")
    assert '"orders"' in _cypher_sql(ctx, f"MATCH (n:{label}) RETURN n.id")
    assert '"orders"' in _graphql_sql(ctx, f"{{ {field} {{ id }} }}")


def test_a_graphql_root_variant_resolves_by_the_names_table_under_the_tables_convention():
    ctx = _ctx()
    names = names_table.context_names(ctx)
    aggregate = names.table_row(1, "graphql_aggregate").name
    assert aggregate == "ordersAggregate"
    assert names_table.root_field(ctx, aggregate) == ("graphql_aggregate", ctx.tables["orders"])
    # The other convention's spelling is no name of the table: the schema does not serve it.
    assert names_table.root_field(ctx, "orders_aggregate") is None
    with pytest.raises(UnknownRootField):
        _graphql_sql(ctx, "{ orders_aggregate { aggregate { count } } }")


# ── each language reads the names table, never a name of its own ─────────────────────────────


@pytest.fixture
def renamed(monkeypatch):
    """A context whose names table names table 1 ``bought`` on every surface, where every
    derivation would name it ``orders``: what each language resolves is the table's name."""
    ctx = _ctx()
    real = names_table.context_names(ctx)
    renames = {
        "sql": "bought",
        "cypher_label": "Bought",
        "graphql_field": "bought",
        "graphql_aggregate": "boughtAggregate",
    }
    rows = tuple(
        replace(r, name=renames[r.surface])
        if r.table_id == 1 and r.object_kind == "table" and r.surface in renames
        else r
        for r in real.rows
    )
    renamed_table = NamesTable(rows=rows)
    monkeypatch.setattr(names_table, "context_names", lambda c: renamed_table)
    return ctx


def test_sql_resolves_a_table_by_its_names_table_name(renamed):
    assert '"public"."orders"' in _sql_target(renamed, "SELECT id FROM sales.bought")


def test_cypher_resolves_a_label_by_its_names_table_name(renamed):
    assert '"orders"' in _cypher_sql(renamed, "MATCH (n:Bought) RETURN n.id")
    lm = CypherLabelMap.from_schema(renamed, domain_access=_DOMAINS)
    assert "Orders" not in {nm.table_label for nm in lm.nodes.values()}


def test_graphql_resolves_a_root_field_by_its_names_table_name(renamed):
    assert '"orders"' in _graphql_sql(renamed, "{ bought { id } }")
    assert '"orders"' in _graphql_sql(renamed, "{ boughtAggregate { aggregate { count } } }")
    with pytest.raises(UnknownRootField):
        _graphql_sql(renamed, "{ orders { id } }")
