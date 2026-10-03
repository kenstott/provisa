# Copyright (c) 2026 Kenneth Stott
# Canary: 3ff51a34-da80-49b4-ad25-feb8ebaf6a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A Cypher write's property values are data in the statement it becomes.

A value holding a quote, or a quote and a backslash, stays one value assigned to its one column:
it never ends its literal and adds an assignment, a statement or a predicate."""

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.write_translator import WriteTranslator, parse_cypher_write

HOSTILE = [
    "x', salary = '999",
    "x'); DELETE FROM persons; --",
    "x\\', salary = '999",
    "it's",
]


def _translator() -> WriteTranslator:
    person = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="test-pg",
        id_column="id",
        pk_columns=[],
        catalog_name="mycat",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "salary": "salary"},
        physical_properties={},
    )
    return WriteTranslator(CypherLabelMap(nodes={"Person": person}, relationships={}))


def _cypher_string(value: str) -> str:
    """``value`` as a double-quoted Cypher string (the values here hold no double quote)."""
    assert '"' not in value
    return f'"{value}"'


@pytest.mark.parametrize("value", HOSTILE)
def test_a_set_value_is_one_value_of_its_one_column(value):
    sql = _translator().translate(
        parse_cypher_write(f"MATCH (n:Person) WHERE n.id = 1 SET n.name = {_cypher_string(value)}")
    )
    (statement,) = sqlglot.parse(sql, read="postgres")
    assert isinstance(statement, exp.Update)
    assignments = statement.args["expressions"]
    assert [a.this.name for a in assignments] == ["name"]
    assert assignments[0].expression.this == value


@pytest.mark.parametrize("value", HOSTILE)
def test_a_create_value_is_one_value_of_its_one_column(value):
    sql = _translator().translate(
        parse_cypher_write(f"CREATE (n:Person {{name: {_cypher_string(value)}}})")
    )
    (statement,) = sqlglot.parse(sql, read="postgres")
    assert isinstance(statement, exp.Insert)
    values = list(statement.find_all(exp.Literal))
    assert [v.this for v in values] == [value]


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "Infinity"])
def test_a_non_finite_numeric_string_on_a_numeric_column_is_not_a_bare_identifier(value):
    # A numeric column given "nan"/"inf" must not yield `= nan` (an identifier) — it is written
    # as the dialect's non-finite literal (PostgreSQL 'NaN'::float8), still one value.
    person = NodeMapping(
        label="Person", type_name="Person", domain_label=None, table_label="Person", table_id=1,
        source_id="test-pg", id_column="id", pk_columns=[], catalog_name="mycat",
        schema_name="public", table_name="persons",
        properties={"score": "score"}, physical_properties={},
    )  # fmt: skip
    tr = WriteTranslator(CypherLabelMap(nodes={"Person": person}, relationships={}))
    sql = tr.translate(
        parse_cypher_write(f'MATCH (n:Person) WHERE n.id = 1 SET n.score = "{value}"')
    )
    (statement,) = sqlglot.parse(sql, read="postgres")
    (assignment,) = statement.args["expressions"]
    assert not isinstance(assignment.expression, exp.Column), sql
