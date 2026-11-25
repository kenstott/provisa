# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8d2e61-9f37-4c05-a2e8-61c9d3f7b0a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship field reads alike on every engine (#362): a many-to-one is an object, a
one-to-many a list, [] for none, null for no match, at any depth and under a list cap. The
compiler writes each as one grouped read of its target joined back by key -- no subquery
correlated with the outer row, which chdb refuses (NOT_IMPLEMENTED) -- and each engine's
transpile writes only its own JSON functions. Each query is run on DuckDB, PostgreSQL and
ClickHouse (embedded chdb), each through its engine's own transpile, and answers the same JSON."""

# Requirements: REQ-047, REQ-048, REQ-2236

import uuid

import psycopg
import pytest
import sqlglot
import sqlglot.expressions as exp
from psycopg import sql as pgsql

from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.transpiler.transpile import (
    rewrite_json_object_to_build_object,
    transpile,
    transpile_to_duckdb,
    transpile_to_trino,
)
from tests.unit.json_encoder.rows import serialize_rows
from tests.unit.test_nested_field_arguments import _EDGES, _TERMS, _compiled, meta  # noqa: F401

_TO_ONE = "{ on_termEdges(order_by: { id: asc }) { id targetTerm { id name } } }"
_TO_MANY = "{ on_terms(order_by: { id: asc }) { id name edgesOut { id relType } } }"
_NESTED = (
    "{ on_termEdges(order_by: { id: asc }) { id targetTerm { id edgesOut { id targetTerm "
    "{ name } } } } }"
)
_CAPPED = "{ on_terms(order_by: { id: asc }) { id edgesOut(limit: 1) { id } } }"


@pytest.fixture
def pg(embedded_postgres):
    """The terms and edges in a database of this test's own."""
    name = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
        admin.execute(pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(name)))
    con = psycopg.connect(embedded_postgres.dsn(name), autocommit=True)
    con.execute("CREATE SCHEMA ontology")
    con.execute("CREATE TABLE ontology.terms (id INTEGER, name VARCHAR)")
    con.cursor().executemany("INSERT INTO ontology.terms VALUES (%s, %s)", _TERMS)
    con.execute(
        "CREATE TABLE ontology.term_edges (id INTEGER, from_term_id INTEGER, "
        "to_term_id INTEGER, rel_type VARCHAR)"
    )
    con.cursor().executemany("INSERT INTO ontology.term_edges VALUES (%s, %s, %s, %s)", _EDGES)
    yield con
    con.close()
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as admin:
        admin.execute(pgsql.SQL("DROP DATABASE {}").format(pgsql.Identifier(name)))


@pytest.fixture(scope="module")
def clickhouse():
    """The terms and edges on embedded ClickHouse (chdb), as a mounted source holds them."""
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    runtime = ClickHouseFederationRuntime.embedded()
    runtime.run_sync("CREATE DATABASE IF NOT EXISTS ontology").iter_rows()
    for table in ("terms", "term_edges"):
        runtime.run_sync(f"DROP TABLE IF EXISTS ontology.{table}").iter_rows()
    runtime.run_sync(
        "CREATE TABLE ontology.terms (id Nullable(Int32), name Nullable(String)) ENGINE = Memory"
    ).iter_rows()
    runtime.run_sync(
        "CREATE TABLE ontology.term_edges (id Nullable(Int32), from_term_id Nullable(Int32), "
        "to_term_id Nullable(Int32), rel_type Nullable(String)) ENGINE = Memory"
    ).iter_rows()
    terms = ", ".join(f"({i}, '{n}')" for i, n in _TERMS)
    edges = ", ".join(f"({i}, {f}, {t}, '{r}')" for i, f, t, r in _EDGES)
    runtime.run_sync(f"INSERT INTO ontology.terms VALUES {terms}").iter_rows()
    runtime.run_sync(f"INSERT INTO ontology.term_edges VALUES {edges}").iter_rows()
    yield runtime
    for table in ("terms", "term_edges"):
        runtime.run_sync(f"DROP TABLE IF EXISTS ontology.{table}").iter_rows()


def _semantic(meta, query: str):  # noqa: F811
    _schema, ctx, _con = meta
    root = _compiled(meta, query)
    assert root.params == []
    return root, make_semantic_sql(root.sql, ctx)


def _answered(root, rows) -> list[dict]:
    return serialize_rows(list(rows), root.columns, root.root_field)["data"][root.root_field]


def _on_duckdb(meta, query):  # noqa: F811
    root, sql = _semantic(meta, query)
    return _answered(root, meta[2].execute(transpile_to_duckdb(sql, generated=True)).fetchall())


def _on_postgres(meta, query, con):  # noqa: F811
    root, sql = _semantic(meta, query)
    written = rewrite_json_object_to_build_object(transpile(sql, "postgres", generated=True))
    return _answered(root, con.execute(written).fetchall())


def _on_clickhouse(meta, query, runtime):  # noqa: F811
    root, sql = _semantic(meta, query)
    written = transpile(sql, "clickhouse", generated=True)
    return _answered(root, (tuple(r) for r in runtime.run_sync(written).iter_rows()))


_EXPECTED = {
    _TO_ONE: [
        {"id": 7, "targetTerm": {"id": 1, "name": "party"}},
        {"id": 8, "targetTerm": {"id": 1, "name": "party"}},
        {"id": 9, "targetTerm": {"id": 2, "name": "customer"}},
        {"id": 10, "targetTerm": {"id": 4, "name": "old"}},
        {"id": 12, "targetTerm": {"id": 2, "name": "customer"}},
        {"id": 13, "targetTerm": {"id": 4, "name": "old"}},
        {"id": 14, "targetTerm": {"id": 4, "name": "old"}},
    ],
}


def _sorted_lists(value):
    """``value`` with each list in a canonical order: json_agg's order is no engine's promise."""
    if isinstance(value, dict):
        return {k: _sorted_lists(v) for k, v in value.items()}
    if isinstance(value, list):
        return sorted((_sorted_lists(v) for v in value), key=repr)
    return value


@pytest.mark.parametrize("query", [_TO_ONE, _TO_MANY, _NESTED], ids=["to-one", "to-many", "nested"])
def test_every_engine_answers_the_same_json(meta, pg, clickhouse, query):  # noqa: F811
    duck = _sorted_lists(_on_duckdb(meta, query))
    assert _sorted_lists(_on_postgres(meta, query, pg)) == duck
    assert _sorted_lists(_on_clickhouse(meta, query, clickhouse)) == duck
    if query in _EXPECTED:
        assert duck == _sorted_lists(_EXPECTED[query])


def test_a_to_one_is_an_object_and_a_to_many_a_list_on_clickhouse(meta, clickhouse):  # noqa: F811
    edges = _on_clickhouse(meta, _TO_ONE, clickhouse)
    assert edges[0]["targetTerm"] == {"id": 1, "name": "party"}  # an object, not a list of one
    terms = {t["name"]: t for t in _on_clickhouse(meta, _TO_MANY, clickhouse)}
    # ledger's edges are 8, 9 and 12; old has none: [] not null.
    assert sorted(e["id"] for e in terms["ledger"]["edgesOut"]) == [8, 9, 12]
    assert {e["relType"] for e in terms["ledger"]["edgesOut"]} == {
        "PART_OF",
        "KIND_OF",
        "RELATED_TO",
    }
    assert terms["old"]["edgesOut"] == []


def test_a_capped_list_keeps_at_most_its_cap_of_each_rows_on_clickhouse(meta, clickhouse):  # noqa: F811
    terms = _on_clickhouse(meta, _CAPPED, clickhouse)
    # party has one edge, customer three, ledger three, old none.
    assert [len(t["edgesOut"]) for t in terms] == [1, 1, 1, 0]
    assert [len(t["edgesOut"]) for t in _on_duckdb(meta, _CAPPED)] == [1, 1, 1, 0]


@pytest.mark.parametrize("query", [_TO_ONE, _TO_MANY, _NESTED, _CAPPED])
def test_no_subquery_is_correlated_with_the_outer_row(meta, query):  # noqa: F811
    """Each relationship is a derived table joined by key: a projection holds no subquery."""
    _root, sql = _semantic(meta, query)
    for select in sqlglot.parse_one(sql, read="postgres").find_all(exp.Select):
        for projection in select.expressions:
            assert projection.find(exp.Subquery) is None, projection.sql()


@pytest.mark.parametrize("query", [_TO_ONE, _TO_MANY, _NESTED, _CAPPED])
def test_clickhouse_sql_builds_its_json_with_its_own_functions(meta, query):  # noqa: F811
    _root, sql = _semantic(meta, query)
    written = transpile(sql, "clickhouse", generated=True)
    tree = sqlglot.parse_one(written, read="clickhouse")
    assert tree.find(exp.JSONObject, exp.JSONArrayAgg) is None, written
    assert "toJSONString" in written, written


@pytest.mark.parametrize("query", [_TO_ONE, _TO_MANY, _NESTED])
def test_trino_reads_the_same_grouped_form(meta, query):  # noqa: F811
    """Trino takes the compiler's form as it is: no correlated subquery is left to lift."""
    _root, sql = _semantic(meta, query)
    trino = transpile_to_trino(sql, generated=True)
    assert "_grel_" not in trino and "_read" in trino, trino


def _capped(sql: str, limit: int) -> str:
    """``sql`` with a role's list cap (REQ-005) on each list it nests, as governance applies it."""
    from provisa.compiler.list_cap import cap_lists

    from tests.unit.test_nested_field_arguments import _COLUMNS

    def columns_of(table: exp.Expr) -> list[str] | None:  # pyright: ignore[reportPrivateImportUsage]
        # Governance knows a registered table's columns (stage2); here the meta model's.
        if not isinstance(table, exp.Table):
            return None
        return [c for c, _type in _COLUMNS[f"_{table.name}"]]

    tree = sqlglot.parse_one(sql, read="postgres")
    assert cap_lists(tree, limit, columns_of)
    return tree.sql(dialect="postgres")


@pytest.mark.parametrize("query", [_TO_MANY, _NESTED], ids=["to-many", "nested"])
def test_a_roles_list_cap_holds_on_every_engine(meta, pg, clickhouse, query):  # noqa: F811
    """A grouped read's list, numbered per key under the cap (list_cap: FILTER), reads alike:
    ledger's three edges and customer's three are held to two. Which two is no engine's promise:
    the lengths are compared."""
    root, sql = _semantic(meta, query)
    capped = _capped(sql, 2)
    duck = meta[2].execute(transpile_to_duckdb(capped, generated=True)).fetchall()
    on_pg = pg.execute(
        rewrite_json_object_to_build_object(transpile(capped, "postgres", generated=True))
    ).fetchall()
    on_ch = [
        tuple(r)
        for r in clickhouse.run_sync(transpile(capped, "clickhouse", generated=True)).iter_rows()
    ]

    def lengths(rows) -> list:
        def walk(value):
            if isinstance(value, dict):
                return {k: walk(v) for k, v in value.items()}
            if isinstance(value, list):
                return [len(value), sorted((walk(v) for v in value), key=repr)]
            return None

        return [walk(r) for r in _answered(root, rows)]

    assert lengths(on_ch) == lengths(duck) == lengths(on_pg)
    assert all(len(v) <= 2 for r in _answered(root, on_ch) for v in _lists_in(r)), _answered(
        root, on_ch
    )


def _lists_in(value):
    if isinstance(value, dict):
        for v in value.values():
            yield from _lists_in(v)
    elif isinstance(value, list):
        yield value
        for v in value:
            yield from _lists_in(v)
