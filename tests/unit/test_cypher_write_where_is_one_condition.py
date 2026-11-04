# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher write's WHERE is one condition on the matched rows in the statement it becomes.

The WHERE of a ``MATCH ... WHERE ... DELETE`` or ``... SET`` is a Cypher expression: it is parsed
as one and lowered to SQL from its tree, never written into the statement as its text. Text that is
not a Cypher expression -- a second statement, a SQL subquery -- is refused; a string holding
quote characters, comment markers or a semicolon is one value."""

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.expr_parser import CypherExprParseError
from provisa.cypher.write_translator import (
    CypherWriteParseError,
    WriteTranslator,
    parse_cypher_write,
)
from provisa.gql.label_map import CypherLabelMap, NodeMapping


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
        properties={"id": "id", "name": "name", "salary": "salary"},
        physical_properties={},
    )
    return WriteTranslator(CypherLabelMap(nodes={"Person": person}, relationships={}))


def _translated(cypher: str) -> str:
    return _translator().translate(parse_cypher_write(cypher))


NOT_AN_EXPRESSION = [
    "n.id = 1; DELETE FROM persons",
    "n.id = 1); DROP TABLE persons; --",
    "n.id IN (SELECT id FROM secrets)",
    "n.id = 1 UNION SELECT 1",
]


@pytest.mark.parametrize("where", NOT_AN_EXPRESSION)
@pytest.mark.parametrize("tail", ["DELETE n", "SET n.name = 'x'"])
def test_a_where_that_is_not_a_cypher_expression_is_refused(where, tail):
    with pytest.raises((CypherWriteParseError, CypherExprParseError)):
        _translated(f"MATCH (n:Person) WHERE {where} {tail}")


HOSTILE_VALUES = [
    "x' OR '1'='1",
    "x'; DELETE FROM persons; --",
    "a /* b */ c -- d",
    "q’”＇ r",
]


@pytest.mark.parametrize("value", HOSTILE_VALUES)
@pytest.mark.parametrize("tail", ["DELETE n", "SET n.salary = 1"])
def test_a_string_in_a_where_is_one_value(value, tail):
    cypher_string = '"' + value + '"'
    sql = _translated(f"MATCH (n:Person) WHERE n.name = {cypher_string} {tail}")
    (statement,) = sqlglot.parse(sql, read="postgres")
    assert isinstance(statement, (exp.Delete, exp.Update)), sql
    where = statement.args["where"].this
    assert isinstance(where, exp.EQ), sql
    assert where.this == exp.column("name", quoted=True), sql
    assert where.expression == exp.Literal.string(value), sql


def test_a_property_of_another_variable_is_refused_by_name():
    with pytest.raises(CypherWriteParseError, match="m"):
        _translated("MATCH (n:Person) WHERE m.id = 1 DELETE n")


def test_a_parameter_in_a_where_stays_a_parameter():
    sql = _translated("MATCH (n:Person) WHERE n.id = $who DELETE n")
    (statement,) = sqlglot.parse(sql, read="postgres")
    (param,) = statement.find_all(exp.Parameter)
    assert param.this.name == "who"
