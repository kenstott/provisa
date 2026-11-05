# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c2a91-8d47-4b0e-9e15-7a2c5d8b1f64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a Cypher read's lists and maps on the pg engine, as Trino and DuckDB answer them (and the
openCypher TCK where it decides): a filter of a null list is null; toInteger / toFloat and size()
of an item of a list of several types read the item's value, not its JSON text (DuckDB counted
'"ab"' as 4); a quantifier over an empty list is answered as written; a raise is PostgreSQL's own
error with its message; a map is jsonb, its key read by ->."""

# Requirements: REQ-345, REQ-347, REQ-066

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.metadata_server_forms import SERVER_FORM_RAISE
from provisa.transpiler.transpile import RANGE_STEP_ZERO, transpile, transpile_to_duckdb

pytestmark = pytest.mark.unit


def _semantic_sql(statement: str) -> str:
    """The statement's SQL as the Cypher read writes it, before an engine's transpile."""
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher(statement), CypherLabelMap(nodes={}, relationships={}), {}
    )
    return sql_ast.sql(dialect="postgres")


def _postgres(statement: str) -> exp.Expr:
    """The statement as the pg engine runs it, parsed."""
    return sqlglot.parse_one(transpile(_semantic_sql(statement), "postgres"), read="postgres")


def _duckdb(statement: str) -> list[tuple]:
    return duckdb.connect().execute(transpile_to_duckdb(_semantic_sql(statement))).fetchall()


def _calls(tree: exp.Expr, name: str) -> list[exp.Anonymous]:
    return [n for n in tree.find_all(exp.Anonymous) if n.name.upper() == name.upper()]


# -- 1. a filter of a null list ---------------------------------------------------------------

_NULL_LIST = (
    "WITH [1, 2] AS l WITH CASE WHEN false THEN l END AS n RETURN [x IN n WHERE x > 1] AS a"
)


def test_a_filter_of_a_null_list_on_postgres_is_null():
    tree = _postgres(_NULL_LIST)
    (listed,) = [a for a in tree.find_all(exp.Array) if a.find(exp.Select)]
    # The rows of a null list are none: the list is null, as Trino's filter answers, never [].
    guard = listed.parent
    assert isinstance(guard, exp.Case), tree.sql("postgres")
    (null_list,) = guard.args["ifs"]
    assert isinstance(null_list.this, exp.Is), tree.sql("postgres")
    assert isinstance(null_list.args["true"], exp.Null), tree.sql("postgres")


def test_a_filter_of_a_null_list_on_duckdb_is_null():
    assert _duckdb(_NULL_LIST) == [(None,)]


# -- 2. toInteger / toFloat of an item of a list of several types -----------------------------

_CONVERTED = (
    "WITH [1, 'a', 2.5, '3'] AS l "
    "RETURN toInteger(l[0]) AS a, toFloat(l[2]) AS b, toInteger(l[3]) AS c, toFloat(l[0]) AS d, "
    "toInteger(l[1]) AS e"
)


def test_a_conversion_of_an_item_of_several_types_reads_its_value():
    sql = _semantic_sql(_CONVERTED)
    tree = sqlglot.parse_one(sql, read="postgres")
    # Each cast reads the item's scalar ('3', not '"3"'), never its JSON.
    for cast in tree.find_all(exp.TryCast):
        if isinstance(cast.this, exp.Trunc):
            continue
        assert cast.this.find(exp.JSONExtractScalar) is not None, sql


def test_a_conversion_of_an_item_of_several_types_on_postgres_is_its_own_sql():
    tree = _postgres(_CONVERTED)
    assert not list(tree.find_all(exp.TryCast)), tree.sql("postgres")


def test_a_nested_null_on_failure_cast_on_postgres_is_lowered():
    # toInteger('3.7') is TRY_CAST(TRUNC(TRY_CAST('3.7' AS DECIMAL)) AS BIGINT): both lowered.
    tree = _postgres("RETURN toInteger('3.7') AS a")
    assert not list(tree.find_all(exp.TryCast)), tree.sql("postgres")
    tests = _calls(tree, "PG_INPUT_IS_VALID")
    assert {t.expressions[1].this for t in tests} == {"BIGINT", "DECIMAL(38, 10)"}, tree.sql()


def test_a_conversion_of_an_item_of_several_types_on_duckdb():
    assert _duckdb(_CONVERTED) == [(1, 2.5, 3, 1.0, None)]


# -- 3 and 6. size() and a comparison of an item, outside a comprehension ----------------------

_ITEM_SIZE = "WITH [1, 'ab', [1, 2, 3]] AS l RETURN size(l[1]) AS a, size(l[2]) AS b"
_ITEM_COMPARED = "WITH [1, 'ab', 2] AS l RETURN l[0] < l[2] AS c, l[1] = 'ab' AS d"


def test_the_size_of_an_item_of_several_types_is_read_from_its_json():
    sql = _semantic_sql(_ITEM_SIZE)
    assert sql.upper().count("JSON_SIZE(") == 2, sql
    assert "CARDINALITY(" not in sql.upper(), sql


def test_the_size_of_an_item_of_several_types_on_postgres_reads_its_json():
    tree = _postgres(_ITEM_SIZE)
    assert _calls(tree, "json_array_length"), tree.sql("postgres")
    assert not _calls(tree, "cardinality"), tree.sql("postgres")


def test_the_size_of_an_item_of_several_types_on_duckdb_is_its_size():
    # Its JSON's text was counted: '"ab"' is 4, '[1,2,3]' 7.
    assert _duckdb(_ITEM_SIZE) == [(2, 3)]


def test_a_comparison_of_items_of_several_types_on_duckdb():
    assert _duckdb(_ITEM_COMPARED) == [(True, True)]


# -- 4. quantifiers over an empty list --------------------------------------------------------

_EMPTY_QUANTIFIED = (
    "RETURN all(x IN [] WHERE x > 0) AS a, any(x IN [] WHERE x > 0) AS b, "
    "none(x IN [] WHERE x > 0) AS c, single(x IN [] WHERE x > 0) AS d"
)


def test_a_quantifier_over_an_empty_list_is_answered_as_written():
    sql = _semantic_sql(_EMPTY_QUANTIFIED)
    # openCypher: all and none of nothing are true, any and single false.
    assert sql == "SELECT TRUE AS a, FALSE AS b, TRUE AS c, FALSE AS d", sql


def test_a_quantifier_over_an_empty_list_on_postgres_names_no_empty_array():
    tree = _postgres(_EMPTY_QUANTIFIED)
    assert not [a for a in tree.find_all(exp.Array) if not a.expressions], tree.sql("postgres")


def test_a_quantifier_over_an_empty_list_on_duckdb():
    assert _duckdb(_EMPTY_QUANTIFIED) == [(True, False, True, False)]


# -- 5. a raise on PostgreSQL is its own error --------------------------------------------------


def test_a_computed_step_of_zero_on_postgres_raises_its_own_message():
    tree = _postgres("WITH 0 AS s UNWIND range(1, 3, s) AS i RETURN i")
    (raised,) = _calls(tree, SERVER_FORM_RAISE)
    sqlstate, message = raised.expressions
    assert message.this == RANGE_STEP_ZERO, tree.sql("postgres")
    assert sqlstate.this == "22023", tree.sql("postgres")
    # Never a cast of the message to an integer, whose error wraps it (invalid input syntax).
    assert not [c for c in tree.find_all(exp.Cast) if isinstance(c.this, exp.Subquery)], tree.sql(
        "postgres"
    )


# -- 7. maps on PostgreSQL are jsonb ---------------------------------------------------------

_MAP = "WITH {a: 1, b: 'x'} AS m RETURN m AS m"
_MAP_KEYED = "UNWIND [{a: 1}, {a: 2}] AS m RETURN m['a'] AS a"


def test_a_map_on_postgres_is_a_jsonb_object():
    tree = _postgres(_MAP)
    assert not _calls(tree, "MAP"), tree.sql("postgres")
    (built,) = _calls(tree, "JSONB_BUILD_OBJECT")
    keys = [e.this for e in built.expressions[::2]]
    assert keys == ["a", "b"], tree.sql("postgres")


def test_an_empty_map_on_postgres_is_an_empty_jsonb_object():
    tree = _postgres("WITH {} AS m RETURN m AS m")
    (built,) = _calls(tree, "JSONB_BUILD_OBJECT")
    assert not built.expressions, tree.sql("postgres")


def test_a_key_of_a_map_on_postgres_is_its_jsonb_field():
    tree = _postgres(_MAP_KEYED)
    assert not _calls(tree, "ELEMENT_AT"), tree.sql("postgres")
    (field,) = tree.find_all(exp.JSONExtract)
    assert field.sql("postgres").endswith("-> 'a'"), tree.sql("postgres")
    # Its value read as a number from jsonb: PostgreSQL casts no json to a number.
    assert "AS JSON)" not in tree.sql("postgres").upper(), tree.sql("postgres")


def test_the_keys_of_a_map_on_postgres_are_its_jsonb_keys():
    tree = _postgres("WITH {a: 1, b: 'x'} AS m, 'b' AS k RETURN keys(m) AS ks")
    assert not _calls(tree, "MAP_KEYS"), tree.sql("postgres")
    assert _calls(tree, "JSONB_OBJECT_KEYS"), tree.sql("postgres")


def test_a_key_of_a_map_on_duckdb_answers_as_it_did():
    assert _duckdb(_MAP_KEYED) == [(1,), (2,)]
