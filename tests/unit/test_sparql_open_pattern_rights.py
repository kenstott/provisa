# Copyright (c) 2026 Kenneth Stott
# Canary: bc4a5ded-2c74-460e-aa80-d3a11dade246
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
from types import SimpleNamespace

import duckdb
import pytest
import sqlglot

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
from provisa.api.rest.cypher_router import access_violations
from provisa.compiler.sql_validator import ValidationViolation, validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError
from provisa.pgwire._pipeline import relationship_guard_bypassed
from provisa.sparql_query.compiler import compile_sparql
from provisa.sparql_query.vocabulary import (
    Grounding,
    build_vocabulary,
    column_iri,
    column_types_of,
    table_class_iri,
)

BASE = "https://data.example.com"
ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}
OPEN = "SELECT * WHERE { ?s ?p ?o } LIMIT 25"
SECRET = "Secret Customer Name"


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _column(name: str, hidden: frozenset[str], keys: frozenset[str]) -> dict:
    return {
        "column_name": name,
        "visible_to": [] if name in hidden else ["analyst"],
        "is_primary_key": name in keys,
    }


def _table(
    table_id: int,
    name: str,
    columns: list[str],
    hidden: frozenset[str],
    keys: frozenset[str] = frozenset(),
) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "shop",
        "schema_name": "public",
        "table_name": name,
        "columns": [_column(c, hidden, keys) for c in columns],
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

    def __init__(
        self,
        hidden: set[tuple[int, str]],
        keyed: bool = False,
        role: dict = ROLE,
    ) -> None:
        def own(table_id: int) -> frozenset[str]:
            return frozenset(c for t, c in hidden if t == table_id)

        self.role = role
        declared = frozenset({"id"}) if keyed else frozenset()
        self.tables = [
            _table(1, "customers", ["id", "name", "tier"], own(1), declared),
            _table(2, "orders", ["id", "customer_id", "total"], own(2), declared),
            _table(
                3,
                "lines",
                ["order_no", "line_no", "sku"],
                own(3),
                frozenset({"order_no", "line_no"}),
            ),
        ]
        si = SchemaInput(
            tables=self.tables,
            relationships=RELATIONSHIPS,
            column_types={
                1: _meta(("id", "integer"), ("name", "varchar"), ("tier", "varchar")),
                2: _meta(("id", "integer"), ("customer_id", "integer"), ("total", "integer")),
                3: _meta(("order_no", "integer"), ("line_no", "integer"), ("sku", "varchar")),
            },
            naming_rules=[],
            role=role,
            domains=[{"id": "shop", "description": "Shop"}],
        )
        generate_schema(si)
        self.ctx = build_context(si)
        self.gov = build_governance_context(
            "analyst", RLSContext(rules={}), {}, self.ctx, self.tables, role, RELATIONSHIPS
        )
        self.graph = CypherLabelMap.from_schema(
            self.ctx, domain_access=["*"], all_tables=self.tables
        )
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
        self.con.execute(
            "CREATE TABLE pg.public.lines (order_no INTEGER, line_no INTEGER, sku VARCHAR)"
        )
        self.con.execute(
            "INSERT INTO pg.public.lines VALUES (5001, 1, 'sku-one'), (5001, 2, 'sku-two')"
        )

    def semantic_sql(self, query: str) -> tuple[str, dict]:
        compiled = compile_sparql(query, self.vocab, self.graph)
        assert compiled.ast is not None
        sql, _, graph_vars = cypher_to_sql(compiled.ast, self.graph, {})
        sql = apply_graph_rewrites(sql, graph_vars, self.graph)
        return make_semantic_sql(sql.sql(dialect="postgres"), self.ctx), graph_vars

    def violations(self, sql: str) -> list[ValidationViolation]:
        return validate_sql(
            sql,
            self.ctx,
            self.gov,
            self.role,
            self.tables,
            # As access_violations decides it, by the one bypass rule.
            bypass_relationship_guard=relationship_guard_bypassed(
                self.role, SimpleNamespace(security_high=False), statement_opts_out=False
            ),
            bypass_uncovered_relationships=True,
        )

    def refusals(self, sql: str) -> list[str]:
        return [v.message for v in self.violations(sql)]

    def answer(self, query: str) -> list[dict]:
        sql, graph_vars = self.semantic_sql(query)
        assert self.refusals(sql) == []
        lowered = lower_to_catalog_physical(apply_governance(sql, self.gov), self.ctx)
        statement = sqlglot.transpile(lowered, read="postgres", write="duckdb")[0]
        cur = self.con.execute(statement)
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
        return assemble_rows(take_cut(rows, []), graph_vars)


def _text(rows: list[dict]) -> str:
    return json.dumps(rows, default=str)


def _class(table: str) -> str:
    return table_class_iri(BASE, "shop", table)


def test_a_hidden_column_that_is_no_key_gives_no_triple():
    answer = _text(Model({(1, "name"), (2, "total")}).answer(OPEN))
    assert "gold" in answer  # a column the role reads
    assert SECRET not in answer
    assert "9001" not in answer
    assert "#name" not in answer  # nor is the hidden column's predicate named
    assert "#total" not in answer


def test_a_table_whose_key_the_role_cannot_read_is_left_out_of_the_open_pattern():
    # customer_id is hidden too: it holds the same values, and a column the role reads may say them.
    answer = _text(Model({(1, "id"), (1, "name"), (2, "customer_id")}).answer(OPEN))
    assert "9001" in answer  # the triples of a table the role reads
    assert SECRET not in answer
    assert "7001" not in answer  # as a subject, or an edge's object
    assert "gold" not in answer  # no row of the table can be named, so none is answered


def test_an_edge_into_a_table_with_a_hidden_key_is_left_out():
    rows = Model({(1, "id")}).answer(OPEN)
    predicates = {r["p"]["iri"] for r in rows}
    assert not any("/vocab/rel/" in p for p in predicates)
    assert column_iri(BASE, "shop", "orders", "total") in predicates


@pytest.mark.parametrize("keyed", [False, True], ids=["no-registered-key", "registered-key"])
def test_a_role_without_ignore_relationships_is_answered_the_open_pattern(keyed):
    """The join of a variable predicate is Provisa's lowering, no relationship a client wrote."""
    answer = _text(Model(set(), keyed=keyed).answer(OPEN.replace("25", "100")))
    assert SECRET in answer
    assert "gold" in answer
    assert "9001" in answer
    assert "/vocab/rel/" in answer  # the edge, through its registered relationship


def test_every_kind_of_predicate_reads_its_own_subject():
    rows = Model(set()).answer("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    by_predicate = {r["p"]["iri"]: r for r in rows}
    total = by_predicate[column_iri(BASE, "shop", "orders", "total")]
    assert total["o"]["literal"] == 9001
    assert total["s"]["node"]["table"] == "orders"


def test_a_hidden_key_never_falls_back_to_a_column_that_is_no_key():
    model = Model({(2, "id")})
    answer = _text(model.answer(OPEN.replace("25", "100")))
    assert '"orders"' not in answer  # no subject of orders, keyed by customer_id or otherwise
    assert "9001" not in answer
    assert "gold" in answer


def test_a_compound_key_the_role_reads_part_of_leaves_the_table_out():
    answer = _text(Model({(3, "line_no")}).answer(OPEN.replace("25", "100")))
    assert "sku-one" not in answer  # both lines would be the one subject 5001
    assert "sku-two" not in answer
    assert "gold" in answer


def test_a_whole_compound_key_addresses_each_row():
    rows = Model(set()).answer("SELECT ?s ?o WHERE { ?s ?p ?o }")
    keys = {
        json.dumps(r["s"]["node"]["key"]): r["o"]["literal"]
        for r in rows
        if "literal" in r["o"] and str(r["o"]["literal"]).startswith("sku-")
    }
    assert keys == {"[5001, 1]": "sku-one", "[5001, 2]": "sku-two"}


def test_a_count_over_a_compound_key_reads_each_rows_own_triples():
    """Each line's own triples: a join of a table to itself on the first column of its key alone
    would give each line the other's too."""
    query = f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s a <{_class('lines')}> ; ?p ?o }}"
    (row,) = Model(set()).answer(query)
    assert row["n"]["literal"] == 8  # four predicates (type, three columns) of two lines


@pytest.mark.parametrize(
    "hidden,table",
    [({(1, "id")}, "customers"), ({(2, "id")}, "orders"), ({(3, "line_no")}, "lines")],
    ids=["join-target", "fallback", "compound"],
)
def test_naming_the_class_and_projecting_the_subject_is_refused_by_name(hidden, table):
    model = Model(hidden)
    with pytest.raises(CypherTranslateError, match=f"{table}.* key is not readable"):
        model.semantic_sql(f"SELECT ?s WHERE {{ ?s a <{_class(table)}> }}")


def test_a_class_named_without_its_subject_is_answered():
    query = f"SELECT ?t WHERE {{ ?s <{column_iri(BASE, 'shop', 'customers', 'tier')}> ?t }}"
    rows = Model({(1, "id"), (1, "name")}).answer(query)
    assert [r["t"]["literal"] for r in rows] == ["gold"]


def test_a_repeated_refusal_is_said_once_and_its_detail_kept(monkeypatch):
    repeated = [ValidationViolation("V003", "Column s.id is not visible to this role")] * 3
    other = ValidationViolation("V003", "Column _s.id is not visible to this role")
    monkeypatch.setattr(
        "provisa.compiler.sql_validator.validate_sql", lambda *a, **k: [*repeated, other]
    )
    monkeypatch.setattr(
        "provisa.compiler.stage2.build_governance_context", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "provisa.pgwire._pipeline.relationship_guard_bypassed", lambda *a, **k: False
    )
    monkeypatch.setattr(
        "provisa.security.rights.effective_domain_access_role", lambda *a, **k: ROLE
    )
    surface = SimpleNamespace(rls=None, context=None)
    state = SimpleNamespace(masking_rules={}, roles={})
    refusal = access_violations("SELECT 1", surface, "analyst", state)
    assert refusal is not None
    body = json.loads(bytes(refusal.body))
    # The message, each once...
    assert body["detail"] == (
        "Column s.id is not visible to this role; Column _s.id is not visible to this role"
    )
    assert len(body["violations"]) == 4  # ...and every violation still listed
    assert body["params"]["count"] == 4
