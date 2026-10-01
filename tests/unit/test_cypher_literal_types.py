# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3a1f46-2b7d-4c95-a0e8-5d9c6b1f4a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher comparison keeps the type of what it compares (issue #130).

A property of one registered table is that table's own typed column, so the literal or parameter
compared to it reaches the source as written: an integer stays an integer, a parameter stays a
bare bound placeholder. Only the graph identity of a node drawn from several tables — a VARCHAR
built by the translator itself — is compared in string space."""

# Requirements: REQ-345, REQ-352

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.cypher.label_map import CypherLabelMap
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import cypher_to_sql

_COLUMNS = [
    ("id", "integer"),
    ("score", "float"),
    ("active", "boolean"),
    ("name", "varchar"),
    ("note", "varchar"),
]


def _label_map() -> CypherLabelMap:
    table = SimpleNamespace(
        type_name="Sales_Orders",
        table_id=1,
        source_id="pg",
        catalog_name="postgresql",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    ctx = SimpleNamespace(
        tables={"sales__orders": table},
        joins={},
        aggregate_columns={1: _COLUMNS},
        pk_columns={},
        native_filter_columns={},
        physical_to_sql={},
        gql_governed_object_cols=set(),
    )
    return CypherLabelMap.from_schema(ctx)


def _where(cypher: str, params: dict | None = None) -> tuple[str, list[str]]:
    ast, ordered, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), params or {})
    return ast.sql(dialect="postgres").split("WHERE", 1)[1].strip(), ordered


def _one_table(predicate: str, params: dict | None = None) -> tuple[str, list[str]]:
    return _where(f"MATCH (o:Orders) WHERE {predicate} RETURN o.name AS name", params)


@pytest.mark.parametrize(
    "predicate, sql",
    [
        ("o.id = 7", 'o."id" = 7'),
        ("7 = o.id", '7 = o."id"'),
        ("o.id <> 7", 'o."id" <> 7'),
        ("o.id = 7.5", 'o."id" = 7.5'),
        ("o.id = '7'", "o.\"id\" = '7'"),
        ("o.id = true", 'o."id" = TRUE'),
        ("o.id = null", 'o."id" = NULL'),
        ("o.score = 10.5", 'o."score" = 10.5'),
        ("o.active = true", 'o."active" = TRUE'),
        ("o.active = false", 'o."active" = FALSE'),
        ("o.name = 'x'", "o.\"name\" = 'x'"),
        ("o.note = null", 'o."note" = NULL'),
        ("o.note IS NULL", 'o."note" IS NULL'),
    ],
)
def test_a_literal_compared_to_a_table_column_keeps_its_type(predicate, sql):
    assert _one_table(predicate) == (sql, [])


@pytest.mark.parametrize(
    "predicate, sql",
    [
        ("o.id = $p", 'o."id" = $1'),
        ("$p = o.id", '$1 = o."id"'),
        ("o.id <> $p", 'o."id" <> $1'),
        ("o.score = $p", 'o."score" = $1'),
        ("o.active = $p", 'o."active" = $1'),
        ("o.name = $p", 'o."name" = $1'),
    ],
)
def test_a_parameter_compared_to_a_table_column_stays_a_bare_placeholder(predicate, sql):
    assert _one_table(predicate, {"p": 7}) == (sql, ["p"])


@pytest.mark.parametrize("match", ["(n)", "(n:Sales)"])
def test_the_identity_of_a_node_drawn_from_several_tables_is_compared_as_text(match):
    """That identity is a column the translator builds as VARCHAR (CAST(pk AS VARCHAR)) so nodes
    of differently keyed tables fit one column; a value compared to it is put in the same space."""
    cypher = f"MATCH {match} WHERE n.id = 7 RETURN n.id AS i"
    ast, _, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), {})
    assert 'CAST("id" AS VARCHAR) AS "id"' in ast.sql(dialect="postgres")
    literal, _ = _where(cypher)
    assert literal.endswith('n."id" = CAST(7 AS TEXT)')
    bound, ordered = _where(f"MATCH {match} WHERE n.id = $p RETURN n.id AS i", {"p": 7})
    assert bound.endswith('n."id" = CAST($1 AS TEXT)') and ordered == ["p"]
