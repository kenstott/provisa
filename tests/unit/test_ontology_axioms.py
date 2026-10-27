# Copyright (c) 2026 Kenneth Stott
# Canary: d18e380c-f447-454d-add1-3f5720bb1a02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2163 step 3: the ontology schema stated as RDFS/OWL/SKOS, one triple a row of
ontology._axioms, computed in SQL over the glossary: Class terms are owl:Class, kind-of edges
rdfs:subClassOf (and rdfs:subPropertyOf between properties), Property terms owl:DatatypeProperty
or owl:ObjectProperty with their rdfs:domain and rdfs:range, and each term's skos:prefLabel and
skos:definition. A retired term states nothing. The view is SQL's alone: no GraphQL field and no
Cypher label; SPARQL reaches it by lowering these patterns to it."""

from __future__ import annotations

import pytest
from sqlalchemy import insert, text

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api._ontology_views import ONTOLOGY_TABLE_VIEWS, ONTOLOGY_TABLES
from provisa.core.schema_org import (
    domains,
    glossary_term_edges,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    relationships,
    sources,
    table_columns,
)
from provisa.security.rights import GLOSSARY_SCOPED_VIEWS, SQL_ONLY_TABLES, ONTOLOGY_DOMAIN_ID

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
SKOS = "http://www.w3.org/2004/02/skos/core#"
XSD = "http://www.w3.org/2001/XMLSchema#"

CUSTOMER, ORDER, PARTY, SEGMENT, CUSTOMER_REF, ORDER_TOTAL, AMOUNT, OLD = range(1, 9)
_TERMS = {
    CUSTOMER: ("Customer", "Someone who buys", False),
    ORDER: ("Order", "A purchase", False),
    PARTY: ("Party", "A person or organisation", False),
    SEGMENT: ("segment", None, False),
    CUSTOMER_REF: ("customer", "Who placed the order", False),
    ORDER_TOTAL: ("order total", "What the order costs", False),
    AMOUNT: ("amount", "A sum of money", False),
    OLD: ("old customer", "Superseded", True),
}


async def _seed(conn) -> None:
    await conn.execute_core(insert(domains).values(id="sales", description="sales"))
    await conn.execute_core(insert(sources).values(id="erp", type="postgres"))
    for term_id, (name, definition, retired) in _TERMS.items():
        await conn.execute_core(
            insert(glossary_terms).values(
                id=term_id, name=name, definition=definition, retired=retired
            )
        )
    for table_id, name, class_term in ((21, "customers", CUSTOMER), (22, "orders", ORDER)):
        await conn.execute_core(
            insert(registered_tables).values(
                id=table_id,
                source_id="erp",
                domain_id="sales",
                schema_name="public",
                table_name=name,
                class_term_id=class_term,
            )
        )
    for table_id, column, data_type in (
        (21, "id", "integer"),
        (21, "segment", "varchar"),
        (22, "customer_id", "integer"),
        (22, "total", "decimal(12,2)"),
    ):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=table_id,
                domain_id="sales",
                column_name=column,
                data_type=data_type,
                visible_to=[],
            )
        )
    await conn.execute_core(
        insert(relationships).values(
            id="orders-customer",
            source_table_id=22,
            target_table_id=21,
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
        )
    )
    for term_id, table_id, column in (
        (SEGMENT, 21, "segment"),
        (CUSTOMER_REF, 22, "customer_id"),
        (ORDER_TOTAL, 22, "total"),
    ):
        await conn.execute_core(
            insert(glossary_term_refs).values(
                term_id=term_id, table_id=table_id, column_name=column
            )
        )
    for a, b, rel_type in (
        (CUSTOMER, PARTY, "KIND_OF"),
        (ORDER_TOTAL, AMOUNT, "KIND_OF"),
        (CUSTOMER, ORDER, "RELATED_TO"),  # not an axiom
        (OLD, CUSTOMER, "KIND_OF"),  # retired: states nothing
    ):
        await conn.execute_core(
            insert(glossary_term_edges).values(from_term_id=a, to_term_id=b, rel_type=rel_type)
        )


async def _axioms(control_plane) -> set[tuple]:
    from provisa.api.startup_seed import _adapt_view_ddl

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS["table_columns"]))
            for ddl in ONTOLOGY_TABLE_VIEWS.values():
                await conn.execute(_adapt_view_ddl(ddl))
            rows = (
                await conn.execute_core(
                    text(
                        "SELECT subject, predicate, object, object_iri, object_literal "
                        "FROM _axioms_ontology"
                    )
                )
            ).fetchall()
    finally:
        await db.close()
    return {tuple(r) for r in rows}


def _term(subject, predicate, obj):
    return (subject, predicate, obj, None, None)


def _iri(subject, predicate, iri):
    return (subject, predicate, None, iri, None)


def _lit(subject, predicate, value):
    return (subject, predicate, None, None, value)


@pytest.mark.asyncio
async def test_the_glossary_is_stated_as_rdfs_owl_and_skos(control_plane):
    axioms = await _axioms(control_plane)
    live = [t for t in _TERMS.values() if not t[2]]
    expected = {
        _iri("Customer", f"{RDF}type", f"{OWL}Class"),
        _iri("Order", f"{RDF}type", f"{OWL}Class"),
        _iri("segment", f"{RDF}type", f"{OWL}DatatypeProperty"),
        _iri("order total", f"{RDF}type", f"{OWL}DatatypeProperty"),
        # Bound to a key of a classed table: its value is an instance of that table's class.
        _iri("customer", f"{RDF}type", f"{OWL}ObjectProperty"),
        _term("Customer", f"{RDFS}subClassOf", "Party"),
        _term("order total", f"{RDFS}subPropertyOf", "amount"),
        _term("segment", f"{RDFS}domain", "Customer"),
        _term("customer", f"{RDFS}domain", "Order"),
        _term("order total", f"{RDFS}domain", "Order"),
        _iri("segment", f"{RDFS}range", f"{XSD}string"),
        _iri("order total", f"{RDFS}range", f"{XSD}decimal"),
        _term("customer", f"{RDFS}range", "Customer"),
        *(_lit(name, f"{SKOS}prefLabel", name) for name, _d, _r in live),
        *(_lit(name, f"{SKOS}definition", d) for name, d, _r in live if d),
    }
    assert axioms == expected


def test_axioms_are_the_ontology_schemas_and_sql_s_alone():
    assert "_axioms" in ONTOLOGY_TABLES
    assert (ONTOLOGY_DOMAIN_ID, "_axioms") in SQL_ONLY_TABLES
    # A triple is read only where both its terms are readable (a literal or IRI object has none).
    assert GLOSSARY_SCOPED_VIEWS["_axioms_ontology"] == ("subject_domains", "object_domains")


def _schema_input():
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput

    cols = {"_terms": ["id", "name"], "_axioms": ["id", "subject", "predicate"]}
    return SchemaInput(
        tables=[
            {
                "id": i,
                "source_id": "provisa-admin",
                "domain_id": ONTOLOGY_DOMAIN_ID,
                "schema_name": "public",
                "table_name": name,
                "columns": [{"column_name": c, "visible_to": []} for c in cs],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for i, (name, cs) in enumerate(cols.items(), 1)
        ],
        relationships=[],
        column_types={
            i: [ColumnMetadata(column_name=c, data_type="varchar", is_nullable=True) for c in cs]
            for i, (_name, cs) in enumerate(cols.items(), 1)
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": ONTOLOGY_DOMAIN_ID, "description": "Ontology", "graphql_alias": "ont"}],
        domain_prefix=True,
    )


def test_sql_reaches_axioms_and_graphql_and_cypher_do_not():
    from provisa.compiler.context import build_context
    from provisa.compiler.schema_gen import generate_schema
    from provisa.gql.label_map import CypherLabelMap

    si = _schema_input()
    fields = set(generate_schema(si).query_type.fields)
    assert "ont__terms" in fields and not any("axioms" in f.lower() for f in fields)
    ctx = build_context(si)
    # SQL reads it: the compilation context has it.
    assert any(m.table_name == "_axioms" for m in ctx.tables.values())
    lm = CypherLabelMap.from_schema(ctx, domain_access=["*"])
    assert all(nm.table_name != "_axioms" for nm in lm.nodes.values())
    # SPARQL lowers to it through the map that holds the SQL-only tables too.
    assert any(nm.table_name == "_axioms" for nm in lm.with_sql_only().nodes.values())
