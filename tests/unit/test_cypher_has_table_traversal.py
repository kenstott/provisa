# Copyright (c) 2026 Kenneth Stott
# Canary: e64fd3b5-4439-4edf-88f9-d1efc737453b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A link whose source side is a constant -- HAS_TABLE, every row of a table to its one
registration, on the (domain, name) pair -- is traversed only by a pattern naming it.

An untyped variable-length path between two data tables took Inquiries -> registered_tables ->
Employees, relating the two through a meta row and refused as a cross join (V002); an unlabeled
``()-[r]-()`` read every table's link to its registration. Both must leave it out, as they do a
link with a ``source_constant``.
"""

from __future__ import annotations

import pytest

from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.parser import parse_cypher
from provisa.gql.translator import cypher_to_sql

pytestmark = pytest.mark.unit

_TARGET_EXPR = '({alias}."domain_id", {alias}."table_query_name")'


def _node(label: str, table_id: int, schema: str, table: str) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id=schema,
        id_column="id",
        pk_columns=[],
        catalog_name=schema,
        schema_name=schema,
        table_name=table,
        properties={"id": "id"},
    )


def _has_table(label: str, domain: str, table: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type="HAS_TABLE",
        source_label=label,
        target_label="RegisteredTables",
        join_source_column="__table_id__",
        join_target_column="table_query_name",
        field_name="_meta",
        alias="HAS_TABLE",
        source_expr=f"('{domain}', '{table}')",
        target_expr=_TARGET_EXPR,
    )


def _label_map() -> CypherLabelMap:
    nodes = {
        "Inquiries": _node("Inquiries", 1, "pet_store", "inquiries"),
        "Pets": _node("Pets", 2, "pet_store", "pets"),
        "Employees": _node("Employees", 3, "shelter", "employees"),
        "RegisteredTables": _node("RegisteredTables", 4, "meta", "registered_tables"),
    }
    about = RelationshipMapping(
        rel_type="ABOUT",
        source_label="Inquiries",
        target_label="Pets",
        join_source_column="pet_id",
        join_target_column="id",
        field_name="pet",
    )
    cared = RelationshipMapping(
        rel_type="CARED_BY",
        source_label="Pets",
        target_label="Employees",
        join_source_column="carer_id",
        join_target_column="id",
        field_name="carer",
    )
    links = [
        _has_table("Inquiries", "pet-store", "inquiries"),
        _has_table("Pets", "pet-store", "pets"),
        _has_table("Employees", "shelter", "employees"),
    ]
    rels = {f"{r.rel_type}::{r.source_label}→{r.target_label}": r for r in [about, cared, *links]}
    return CypherLabelMap(
        nodes=nodes,
        relationships=rels,
        nodes_by_table={label: [label] for label in nodes},
        aliases={"ABOUT": [about], "CARED_BY": [cared], "HAS_TABLE": links},
    )


def _sql(query: str) -> str:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _label_map(), {})
    return sql_ast.sql(dialect="postgres")


def test_a_constant_source_expression_is_a_constant_source() -> None:
    link = _has_table("Pets", "pet-store", "pets")
    assert link.constant_source
    assert RelationshipMapping(
        rel_type="R",
        source_label="A",
        target_label="B",
        join_source_column="x",
        join_target_column="y",
        field_name="r",
        source_constant=7,
    ).constant_source
    assert not RelationshipMapping(
        rel_type="R",
        source_label="A",
        target_label="B",
        join_source_column="x",
        join_target_column="y",
        field_name="r",
        source_expr='LOWER({alias}."x")',
    ).constant_source


def test_an_untyped_variable_length_path_does_not_pass_through_a_registration() -> None:
    sql = _sql("MATCH (a:Inquiries)-[*..5]-(b:Employees) RETURN a, b LIMIT 5")
    assert "registered_tables" not in sql
    assert '"pets"' in sql


def test_an_unlabeled_relationship_does_not_read_a_link_to_a_registration() -> None:
    sql = _sql("MATCH ()-[r]-() RETURN r LIMIT 5")
    assert "registered_tables" not in sql
    assert '"pets"' in sql


def test_a_pattern_naming_the_link_still_reads_it() -> None:
    sql = _sql("MATCH (p:Pets)-[:HAS_TABLE]->(t:RegisteredTables) RETURN p, t")
    assert "registered_tables" in sql


def _registered(table_id: int, domain: str, table: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "provisa-admin",
        "domain_id": domain,
        "schema_name": "org_default",
        "table_name": table,
        "columns": [{"column_name": c, "visible_to": ["admin"]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def test_a_registration_links_only_to_an_ops_row_naming_its_domain_and_table() -> None:
    """The registration -> ops link matches CONCAT(domain_id, '.', table_name) on both sides; an
    ops table naming its table some other way (replica_build_log: source, schema, physical name)
    has no domain_id to read, and a link to it failed every statement that crossed it."""
    from provisa.compiler.context import build_context
    from provisa.compiler.schema_gen import SchemaInput
    from tests.unit.test_sql_gen import _col

    tables = [
        _registered(1, "meta", "registered_tables", ["id", "domain_id", "table_name"]),
        _registered(2, "ops", "query_audit_log", ["id", "domain_id", "table_name"]),
        _registered(3, "ops", "replica_build_log", ["id", "source_id", "table_name"]),
    ]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types={
            t["id"]: [_col(c["column_name"], "varchar") for c in t["columns"]] for t in tables
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "meta", "description": "Meta"}, {"id": "ops", "description": "Ops"}],
        source_catalogs={"provisa-admin": "provisa_admin"},
    )
    linked = {
        j.target.table_name
        for j in build_context(si).joins.values()
        if (j.cypher_alias or "").startswith("HAS_") and j.target.domain_id == "ops"
    }
    assert linked == {"query_audit_log"}
