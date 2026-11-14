# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e6c1a-7d2f-4a8e-9c31-2f6d8e4b7a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1385 (Amended 2026-11-14, THE IDENTIFIER IS THE NAME, AND WHEN IT CHANGES): SPARQL names a
table's class and a column's predicate by the model name -- the alias when one is set -- not by the
physical table or column name. A physical rename under an alias leaves the IRI as it was; a change
of the alias changes it. A query written with the model-name IRIs reads the physical table and
column the alias stands for. A term carrying an imported IRI states its equivalence to that IRI
(REQ-2141, REQ-2163)."""

# Requirements: REQ-1385, REQ-2141, REQ-2163

from __future__ import annotations

import duckdb
import pytest
from sqlalchemy import text

from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import compile_sparql
from provisa.sparql_query.export import ir_to_sparql
from provisa.sparql_query.vocabulary import (
    OWL,
    SKOS,
    Grounding,
    Vocabulary,
    build_vocabulary,
    column_iri,
    table_class_iri,
)
from provisa.transpiler.transpile import transpile_to_duckdb
from provisa.cypher.parser import parse_cypher
from tests.unit import test_sparql_axioms as axioms

BASE = "https://data.example.com"
con = axioms.con  # the ontology axioms' DuckDB fixture


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _schema_input(table_name: str, alias: str | None, column: str, column_alias: str | None):
    """One table of domain hr on source pg: an id and one more column, ``column``, published
    under ``column_alias`` when given; the table published under ``alias`` when given."""
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput

    columns = [
        {"column_name": "id", "visible_to": ["admin"]},
        {"column_name": column, "visible_to": ["admin"], "alias": column_alias},
    ]
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "hr",
                "schema_name": "public",
                "table_name": table_name,
                "alias": alias,
                "columns": columns,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name=column, data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "hr", "description": "HR"}],
        source_types={"pg": "postgresql"},
    )


def _label_map(
    table_name: str = "persons",
    alias: str | None = "people",
    column: str = "LegalName",
    column_alias: str | None = "legal_name",
) -> CypherLabelMap:
    from provisa.compiler.context import build_context

    ctx = build_context(_schema_input(table_name, alias, column, column_alias))
    return CypherLabelMap.from_schema(ctx, domain_access=["*"])


def _vocab(lm: CypherLabelMap) -> Vocabulary:
    return build_vocabulary(lm, BASE, {}, Grounding())


def _table_classes(lm: CypherLabelMap) -> set[str]:
    return set(_vocab(lm).classes)


def _predicates(lm: CypherLabelMap) -> set[str]:
    return set(_vocab(lm).columns)


# -- the class IRI of a table --


def test_an_aliased_tables_class_iri_is_named_by_its_alias():
    assert _table_classes(_label_map()) == {table_class_iri(BASE, "hr", "people")}


def test_a_physical_rename_under_an_alias_leaves_the_class_iri_unchanged():
    before = _table_classes(_label_map(table_name="persons"))
    after = _table_classes(_label_map(table_name="persons_v2"))
    assert before == after == {table_class_iri(BASE, "hr", "people")}


def test_changing_the_alias_changes_the_class_iri():
    assert _table_classes(_label_map(alias="staff")) == {table_class_iri(BASE, "hr", "staff")}


def test_a_table_without_an_alias_is_named_by_its_table_name():
    assert _table_classes(_label_map(alias=None)) == {table_class_iri(BASE, "hr", "persons")}


# -- the predicate IRI of a column --


def test_an_aliased_columns_predicate_is_named_by_its_alias():
    """The alias legal_name is the SQL spelling of LegalName, so the graph's property is
    legalName (its GraphQL spelling); the predicate is still the alias."""
    assert column_iri(BASE, "hr", "people", "legal_name") in _predicates(_label_map())


def test_a_physical_column_rename_under_an_alias_leaves_the_predicate_unchanged():
    before = _predicates(_label_map(column="LegalName"))
    after = _predicates(_label_map(column="FullName"))
    assert before == after
    assert column_iri(BASE, "hr", "people", "legal_name") in after


def test_changing_a_columns_alias_changes_its_predicate():
    predicates = _predicates(_label_map(column_alias="registered_name"))
    assert column_iri(BASE, "hr", "people", "registered_name") in predicates
    assert column_iri(BASE, "hr", "people", "legal_name") not in predicates


# -- a query written with the model-name IRIs reads the physical table and column --


@pytest.fixture
def people_db() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute('CREATE TABLE pg.public.persons_v2 (id INTEGER, "FullName" VARCHAR)')
    c.execute("INSERT INTO pg.public.persons_v2 VALUES (1, 'Ann'), (2, 'Bo')")
    return c


def test_a_query_on_the_alias_iris_reads_the_physical_table_and_column(people_db):
    lm = _label_map(table_name="persons_v2", column="FullName")
    vocab = _vocab(lm)
    people = table_class_iri(BASE, "hr", "people")
    query = (
        f"SELECT ?s ?n WHERE {{ ?s a <{people}> ; "
        f"<{column_iri(BASE, 'hr', 'people', 'legal_name')}> ?n }}"
    )
    compiled = compile_sparql(query, vocab, lm)
    assert compiled.ast is not None
    sql, _, graph_vars = cypher_to_sql(compiled.ast, lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    cur = people_db.execute(transpile_to_duckdb(sql.sql(dialect="postgres")))
    names = [d[0] for d in cur.description]
    rows = assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)
    assert sorted(r["n"]["literal"] for r in rows) == ["Ann", "Bo"]
    assert {r["s"]["node"]["key"][0] for r in rows} == {1, 2}


def test_a_query_on_the_physical_names_iris_matches_nothing():
    lm = _label_map(table_name="persons_v2", column="FullName")
    vocab = _vocab(lm)
    assert table_class_iri(BASE, "hr", "persons_v2") not in vocab.classes
    assert column_iri(BASE, "hr", "persons_v2", "FullName") not in vocab.columns


def test_a_cypher_query_exported_to_sparql_writes_the_alias_iris():
    lm = _label_map()
    label = next(iter(lm.nodes.values())).label
    sparql = ir_to_sparql(
        parse_cypher(f"MATCH (p:{label}) RETURN p.legalName AS n"), _vocab(lm), lm
    )
    assert f"<{table_class_iri(BASE, 'hr', 'people')}#>" in sparql
    assert "persons" not in sparql


# -- a term with an imported IRI states its equivalence to it --

IMPORTED_CLASS = "https://schema.org/Person"
IMPORTED_PROPERTY = "https://schema.org/name"
IMPORTED_CONCEPT = "https://example.org/concepts/party"


async def _axioms_with_imported_iris(control_plane) -> set[tuple]:
    from provisa.api._meta_views import _META_TABLE_VIEWS
    from provisa.api._ontology_views import ONTOLOGY_TABLE_VIEWS
    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.schema_org import glossary_terms
    from tests.unit.test_ontology_axioms import CUSTOMER, PARTY, SEGMENT, _seed

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            for term_id, iri in (
                (CUSTOMER, IMPORTED_CLASS),
                (SEGMENT, IMPORTED_PROPERTY),
                (PARTY, IMPORTED_CONCEPT),
            ):
                await conn.execute_core(
                    glossary_terms.update().where(glossary_terms.c.id == term_id).values(iri=iri)
                )
            await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS["table_columns"]))
            for ddl in ONTOLOGY_TABLE_VIEWS.values():
                await conn.execute(_adapt_view_ddl(ddl))
            rows = (
                await conn.execute_core(
                    text(
                        "SELECT subject, predicate, object_iri FROM _axioms_ontology "
                        "WHERE object_iri IN (:a, :b, :c)"
                    ),
                    {"a": IMPORTED_CLASS, "b": IMPORTED_PROPERTY, "c": IMPORTED_CONCEPT},
                )
            ).fetchall()
    finally:
        await db.close()
    return {tuple(r) for r in rows}


@pytest.mark.asyncio
async def test_a_term_with_an_imported_iri_states_its_equivalence_to_it(control_plane):
    assert await _axioms_with_imported_iris(control_plane) == {
        ("Customer", f"{OWL}equivalentClass", IMPORTED_CLASS),
        ("segment", f"{OWL}equivalentProperty", IMPORTED_PROPERTY),
        ("Party", f"{SKOS}exactMatch", IMPORTED_CONCEPT),
    }


def test_sparql_answers_a_terms_equivalence_to_its_imported_iri(con):
    c = con
    c.execute(
        "INSERT INTO pg.public._axioms_ontology VALUES ('x1', 'Customer', ?, NULL, ?, NULL, NULL)",
        [f"{OWL}equivalentClass", IMPORTED_CLASS],
    )
    rows = axioms._answer(c, "SELECT ?c ?x WHERE { ?c owl:equivalentClass ?x }")
    assert rows == [(axioms.t("Customer"), IMPORTED_CLASS)]
    asked = axioms._answer(c, f"SELECT ?c WHERE {{ ?c owl:equivalentClass <{IMPORTED_CLASS}> }}")
    assert asked == [(axioms.t("Customer"),)]
