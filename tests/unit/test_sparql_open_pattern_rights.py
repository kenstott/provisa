# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``?s ?p ?o`` for a role that cannot read every column (REQ-2135, REQ-263): the open pattern
names no column, so it is no request for a hidden one. The role is answered with the triples of
what it reads, and nothing it does not. Compiled as /data/sparql compiles it (the role's schema,
label map and vocabulary), held to the validator every graph surface runs, and run on DuckDB."""

# Requirements: REQ-2135, REQ-263

from __future__ import annotations

import json

import duckdb
import pytest
import sqlglot

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import compile_sparql
from provisa.sparql_query.vocabulary import Grounding, build_vocabulary, column_types_of

BASE = "https://data.example.com"
ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}
OPEN = "SELECT * WHERE { ?s ?p ?o } LIMIT 25"
SECRET = "Secret Customer Name"


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _column(name: str, hidden: frozenset[str]) -> dict:
    return {"column_name": name, "visible_to": [] if name in hidden else ["analyst"]}


def _table(table_id: int, name: str, columns: list[str], hidden: frozenset[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "public",
        "table_name": name,
        "columns": [_column(c, hidden) for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _meta(*names: tuple[str, str]) -> list[ColumnMetadata]:
    return [ColumnMetadata(column_name=n, data_type=t, is_nullable=True) for n, t in names]


#: Each order is of one customer: customers.id is the target of a many-to-one relationship, so it
#: is the key of a customer's node.
RELATIONSHIPS = [
    {
        "id": 1,
        "source_table_id": 2,
        "target_table_id": 1,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "alias": "customer",
    },
]


class Model:
    """The role's view of customers and orders, ``hidden`` being the (table id, column) it is not
    served, compiled and run as /data/sparql does."""

    def __init__(self, hidden: set[tuple[int, str]]) -> None:
        def own(table_id: int) -> frozenset[str]:
            return frozenset(c for t, c in hidden if t == table_id)

        self.tables = [
            _table(1, "customers", ["id", "name", "tier"], own(1)),
            _table(2, "orders", ["id", "customer_id", "total"], own(2)),
        ]
        si = SchemaInput(
            tables=self.tables,
            relationships=RELATIONSHIPS,
            column_types={
                1: _meta(("id", "integer"), ("name", "varchar"), ("tier", "varchar")),
                2: _meta(("id", "integer"), ("customer_id", "integer"), ("total", "integer")),
            },
            naming_rules=[],
            role=ROLE,
            domains=[{"id": "shop", "description": "Shop"}],
        )
        generate_schema(si)
        self.ctx = build_context(si)
        self.gov = build_governance_context(
            "analyst", RLSContext(rules={}), {}, self.ctx, self.tables, ROLE, RELATIONSHIPS
        )
        self.graph = CypherLabelMap.from_schema(self.ctx, domain_access=["*"])
        self.vocab = build_vocabulary(
            self.graph, BASE, column_types_of(self.graph, self.ctx), Grounding()
        )
        self.con = duckdb.connect()
        self.con.execute("ATTACH ':memory:' AS pg")
        self.con.execute("CREATE SCHEMA pg.public")
        self.con.execute(
            "CREATE TABLE pg.public.customers (id INTEGER, name VARCHAR, tier VARCHAR)"
        )
        self.con.execute(
            "CREATE TABLE pg.public.orders (id INTEGER, customer_id INTEGER, total INTEGER)"
        )
        self.con.execute(f"INSERT INTO pg.public.customers VALUES (7001, '{SECRET}', 'gold')")
        self.con.execute("INSERT INTO pg.public.orders VALUES (8001, 7001, 9001)")

    def semantic_sql(self, query: str) -> tuple[str, dict]:
        compiled = compile_sparql(query, self.vocab, self.graph)
        assert compiled.ast is not None
        sql, _, graph_vars = cypher_to_sql(compiled.ast, self.graph, {})
        sql = apply_graph_rewrites(sql, graph_vars, self.graph)
        return make_semantic_sql(sql.sql(dialect="postgres"), self.ctx), graph_vars

    def refusals(self, sql: str) -> list[str]:
        return [
            v.message
            for v in validate_sql(
                sql, self.ctx, self.gov, ROLE, self.tables, bypass_uncovered_relationships=True
            )
        ]

    def answer(self, query: str) -> list[dict]:
        sql, graph_vars = self.semantic_sql(query)
        assert self.refusals(sql) == []
        lowered = lower_to_catalog_physical(apply_governance(sql, self.gov), self.ctx)
        statement = sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]
        cur = self.con.execute(statement)
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
        return assemble_rows(take_cut(rows, []), graph_vars)


def test_a_hidden_column_that_is_no_key_gives_no_triple():
    answer = json.dumps(Model({(1, "name"), (2, "total")}).answer(OPEN), default=str)
    assert "gold" in answer  # a column the role reads
    assert SECRET not in answer
    assert "9001" not in answer
    assert "#name" not in answer  # nor is the hidden column's predicate named
    assert "#total" not in answer


def test_a_table_whose_key_the_role_cannot_read_does_not_refuse_the_open_pattern():
    answer = json.dumps(Model({(1, "id"), (1, "name")}).answer(OPEN), default=str)
    assert "9001" in answer  # the triples of a table the role reads whole
    assert SECRET not in answer
