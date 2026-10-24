# Copyright (c) 2026 Kenneth Stott
# Canary: 3c4a875b-ec85-4c56-801b-e210b61cdbe2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's row limit bounds every list a read nests (REQ-005, amended 2026-10-24): a group's
nodes and a to-many relationship's rows hold at most that many entries, the counts stay over every
row, a list of exactly that many says it may have more (REQ-1350), and an uncapped role reads
everything. Each case is governed as the pipeline governs it and run on DuckDB."""

# Requirements: REQ-005, REQ-1350

import json
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
import sqlglot
from graphql import parse

from provisa.compiler.list_cap import ListNotCappable, at_ceiling, cap_lists
from provisa.compiler.row_limit import RowLimit
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.executor.serialize import serialize_rows
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_group_by import _build_schema_and_ctx as _group_by_model
from tests.unit.test_sql_gen import _build_schema_and_ctx as _sales_model
from tests.unit.test_where_relationships import RELATIONSHIPS


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """us: three orders of customer 1; eu: one order of customer 2."""
    c = duckdb.connect()
    for schema in ("public", "sales"):
        c.execute(f"CREATE SCHEMA {schema}")
    c.execute(
        "CREATE TABLE public.orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), "
        "region VARCHAR, status VARCHAR, created_at TIMESTAMP)"
    )
    c.execute(
        "INSERT INTO public.orders VALUES (10, 1, 5, 'us', 'new', NULL), "
        "(11, 2, 7, 'eu', 'new', NULL), (12, 1, 9, 'us', 'new', NULL), "
        "(13, 1, 2, 'us', 'new', NULL)"
    )
    c.execute("CREATE TABLE public.customers (id INTEGER, name VARCHAR, email VARCHAR)")
    c.execute("INSERT INTO public.customers VALUES (1, 'Ann', 'a@x'), (2, 'Bo', 'b@x')")
    for table in ("orders", "customers"):
        c.execute(f"CREATE VIEW sales.{table} AS SELECT * FROM public.{table}")
    return c


def _governed(sql: str, params: list, ceiling: int | None) -> tuple[str, GovernanceContext]:
    gov = GovernanceContext()
    gov.limit_ceiling = ceiling
    return apply_governance(sql, gov, None, params), gov


def _answer(con, ctx, text: str, ceiling: int | None) -> tuple[list[dict], GovernanceContext]:
    [compiled] = compile_query(parse(text), ctx)
    governed, gov = _governed(make_semantic_sql(compiled.sql, ctx), compiled.params, ceiling)
    rows = con.execute(transpile_to_duckdb(governed), compiled.params).fetchall()
    return serialize_rows(rows, compiled.columns, compiled.root_field)["data"][
        compiled.root_field
    ], gov


GROUPS = "{ orders_group_by(by: [region]) { groupKey aggregate { count } nodes { id } } }"


def _by_region(groups: list[dict]) -> dict:
    return {g["groupKey"]["region"]: g for g in groups}


def test_a_capped_role_gets_at_most_its_ceiling_of_nodes_per_group_counted_over_all(con):
    _, ctx = _group_by_model()
    groups, gov = _answer(con, ctx, GROUPS, ceiling=2)
    us, eu = _by_region(groups)["us"], _by_region(groups)["eu"]
    assert us["aggregate"]["count"] == 3 and len(us["nodes"]) == 2
    assert eu["aggregate"]["count"] == 1 and len(eu["nodes"]) == 1
    assert gov.list_limit == RowLimit(2, "role")


def test_a_capped_role_s_to_many_list_holds_at_most_its_ceiling(con):
    _, ctx = _sales_model(relationships=RELATIONSHIPS)
    customers, gov = _answer(con, ctx, "{ customers { id orders { id } } }", ceiling=2)
    by_id = {c["id"]: c for c in customers}
    assert len(by_id[1]["orders"]) == 2 and len(by_id[2]["orders"]) == 1
    assert gov.list_limit == RowLimit(2, "role")


def test_an_uncapped_role_gets_every_entry(con):
    _, ctx = _group_by_model()
    groups, gov = _answer(con, ctx, GROUPS, ceiling=None)
    assert sorted(n["id"] for n in _by_region(groups)["us"]["nodes"]) == [10, 12, 13]
    assert gov.list_limit is None
    _, ctx = _sales_model(relationships=RELATIONSHIPS)
    customers, _ = _answer(con, ctx, "{ customers { id orders { id } } }", ceiling=None)
    assert len({c["id"]: c for c in customers}[1]["orders"]) == 3


async def test_a_list_of_exactly_the_ceiling_says_it_may_have_more():
    from provisa.pgwire._pipeline import _warn_if_cut

    def plan() -> Any:
        return SimpleNamespace(list_limit=RowLimit(2, "role"), row_limit=None, warnings=[])

    def answer(rows: list[tuple]) -> Any:
        return SimpleNamespace(rows=rows, redirect=None)

    full = plan()
    await _warn_if_cut(full, answer([("us", '[{"id": 1}, {"id": 2}]')]), None)
    assert [w.code for w in full.warnings] == ["statement.list_at_ceiling"]
    assert full.warnings[0].params == {"limit": 2, "kind": "role"}
    short = plan()
    await _warn_if_cut(short, answer([("eu", '[{"id": 3}]')]), None)
    assert short.warnings == []


def test_a_list_at_the_ceiling_is_found_at_any_depth():
    nested = json.dumps([{"id": 1, "orders": [{"id": 2}, {"id": 3}]}])
    assert at_ceiling([(nested,)], 2) and not at_ceiling([(nested,)], 3)


def test_a_list_in_a_shape_the_cap_has_no_form_for_is_refused_by_name():
    tree = sqlglot.parse_one(
        "SELECT json_agg(a.x), json_agg(b.y) FROM a JOIN b ON a.id = b.id", read="postgres"
    )
    with pytest.raises(ListNotCappable, match="a select of several lists"):
        cap_lists(tree, 2)


def test_a_capped_list_s_filter_is_its_aggregate_s_on_each_engine():
    from provisa.transpiler.transpile import transpile_to_duckdb, transpile_to_trino

    capped = (
        "SELECT COUNT(*), json_agg(json_object(KEY 'id' VALUE \"id\")) "
        "FILTER (WHERE t.__list_rn <= 2) FROM (SELECT *, ROW_NUMBER() OVER () AS __list_rn "
        'FROM "public"."orders") AS t'
    )
    # Trino has no json_arrayagg: its ARRAY_AGG is filtered, inside the conversion to JSON.
    assert "ARRAY_AGG(JSON_PARSE(" in transpile_to_trino(capped)
    assert "FILTER(WHERE t.__list_rn <= 2) AS JSON)" in transpile_to_trino(capped)
    # DuckDB's json_group_array is a macro: its own aggregate (list, ARRAY_AGG) is filtered.
    assert "TO_JSON(ARRAY_AGG(JSON_OBJECT('id', \"id\")) FILTER(WHERE t.__list_rn <= 2))" in (
        transpile_to_duckdb(capped)
    )
