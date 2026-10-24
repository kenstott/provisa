# Copyright (c) 2026 Kenneth Stott
# Canary: d0cee0c3-cdea-4732-8cc0-ace9bb12856d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A group-by root that selects its nodes is one statement (REQ-196, REQ-654): each group's
key, its aggregates and its rows -- ``json_agg(json_object(...)) AS nodes`` -- with no join back
to the groups. It is what the SQL panel and a strict-NL read show, so it runs under the join
guard (V002) as it is, answers on DuckDB what the GraphQL answers, and its Cypher prints the
rows as ``collect({...}) AS nodes`` beside the key and reads back to the same groups."""

# Requirements: REQ-196, REQ-654, REQ-2135

import json

import duckdb
import pytest
from graphql import parse, validate

from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.cypher.parser import parse_cypher
from provisa.cypher.printer import print_cypher
from provisa.executor.serialize import serialize_rows
from provisa.gql.assembler import assemble_rows
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.graphql_front.lower import lower_read
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_group_by import _build_schema_and_ctx

READ = "{ orders_group_by(by: [region]) { groupKey aggregate { count } nodes { id amount } } }"


@pytest.fixture
def model():
    schema, ctx = _build_schema_and_ctx()
    assert not validate(schema, parse(READ))
    return ctx


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("CREATE SCHEMA public")
    c.execute(
        "CREATE TABLE public.orders (id INTEGER, amount DECIMAL(10,2), region VARCHAR, "
        "status VARCHAR, created_at TIMESTAMP)"
    )
    c.execute(
        "INSERT INTO public.orders VALUES (10, 5, 'us', 'new', NULL), "
        "(11, 7, 'eu', 'new', NULL), (12, 9, 'us', 'shipped', NULL)"
    )
    # The semantic statement a Cypher read lowers to names the table by its domain.
    c.execute("CREATE SCHEMA sales")
    c.execute("CREATE VIEW sales.orders AS SELECT * FROM public.orders")
    return c


def _groups(groups: list[dict]) -> list[dict]:
    """Each group with its nodes in id order: a group's rows come in no order."""
    out = []
    for g in groups:
        nodes = sorted(
            ({k: float(v) if k == "amount" else v for k, v in n.items()} for n in g["nodes"]),
            key=lambda n: n["id"],
        )
        out.append({**g, "nodes": nodes})
    return sorted(out, key=lambda g: json.dumps(g["groupKey"], sort_keys=True))


def test_the_statement_joins_nothing_and_passes_the_join_guard(model):
    [compiled] = compile_query(parse(READ), model)
    sql = make_semantic_sql(compiled.sql, model)
    assert "JOIN" not in sql.upper() and "json_agg" in sql
    gov = GovernanceContext()
    gov.table_map = {t.table_name: t.table_id for t in model.tables.values()}
    violations = validate_sql(compiled.sql, model, gov, {"domain_access": ["*"]}, [])
    assert [v for v in violations if v.code == "V002"] == []


def test_it_answers_each_group_with_its_own_rows(model, con):
    [compiled] = compile_query(parse(READ), model)
    sql = transpile_to_duckdb(compiled.sql)
    rows = con.execute(sql).fetchall()
    groups = serialize_rows(rows, compiled.columns, compiled.root_field)["data"]["orders_group_by"]
    assert _groups(groups) == [
        {
            "groupKey": {"region": "eu"},
            "aggregate": {"count": 1},
            "nodes": [{"id": 11, "amount": 7.0}],
        },
        {
            "groupKey": {"region": "us"},
            "aggregate": {"count": 2},
            "nodes": [{"id": 10, "amount": 5.0}, {"id": 12, "amount": 9.0}],
        },
    ]


def test_its_cypher_collects_each_group_s_rows_and_reads_back_to_the_same_groups(model, con):
    lm = CypherLabelMap.from_schema(model, domain_access=["*"])
    [root] = lower_read(parse(READ), model, lm, None)
    cypher = print_cypher(root.statement)
    assert "collect({id: n0.id, amount: n0.amount}) AS nodes" in cypher
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, lm)
    sql = transpile_to_duckdb(make_semantic_sql(sql_ast.sql(dialect="postgres"), model))
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    rows = assemble_rows([dict(zip(names, r, strict=True)) for r in cur.fetchall()], graph_vars)

    def plain(value):
        """A Cypher cell as JSON holds it: a map's values are JSON text."""
        if isinstance(value, str) and value[:1] in "[{":
            return plain(json.loads(value))
        if isinstance(value, dict):
            return {k: plain(json.loads(v) if isinstance(v, str) else v) for k, v in value.items()}
        if isinstance(value, list):
            return [plain(v) for v in value]
        return value

    groups = [plain(r) for r in rows]
    assert _groups(
        [
            {"groupKey": g["groupKey"], "aggregate": g["aggregate"], "nodes": g["nodes"]}
            for g in groups
        ]
    ) == _groups(
        [
            {
                "groupKey": {"region": "eu"},
                "aggregate": {"count": 1},
                "nodes": [{"id": 11, "amount": 7}],
            },
            {
                "groupKey": {"region": "us"},
                "aggregate": {"count": 2},
                "nodes": [{"id": 10, "amount": 5}, {"id": 12, "amount": 9}],
            },
        ]
    )
