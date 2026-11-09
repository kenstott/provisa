# Copyright (c) 2026 Kenneth Stott
# Canary: 3ed4ce91-2b38-485a-a274-26bfb40ecc81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-066: the premises under which the compile path edits a sqlglot tree in place, and the
memoized trees whose copies stay."""

# Requirements: REQ-066

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from provisa.transpiler import (
    function_macros,
    pg_forms_json,
    pg_functions,
    pg_held_types,
    transpile_correlated,
)
from provisa.transpiler.transpile import transpile

#: Statements each step of the chain writes forms for, on the dialects those forms are for.
_FORMS = [
    ("trino", "SELECT json_agg(a) FROM t"),
    ("trino", "SELECT width_bucket(a, 0, 10, 2), json_agg(json_build_object('k', a)) FROM t"),
    ("duckdb", "SELECT to_hex(1), jsonb_extract_path(j, 'a') FROM t"),
    ("clickhouse", "SELECT num_nulls(a, b) FROM t"),
]

_STATEMENTS = [
    "SELECT a, b FROM s.t AS t WHERE a = 1 AND b LIKE 'x%' ORDER BY a NULLS FIRST LIMIT 3",
    "WITH w AS (SELECT 1 AS a) SELECT w.a, (SELECT max(b) FROM t WHERE t.a = w.a) FROM w",
    "SELECT json_agg(json_build_object('a', a, 'b', to_json(b))) FILTER (WHERE a > 1) FROM t",
    "SELECT CAST('[]' AS JSON), COALESCE(x, '{}'), a::text FROM (SELECT 1 AS a) q JOIN r ON TRUE",
    "SELECT CASE WHEN a BETWEEN 1 AND 2 THEN b ELSE c END, array_agg(d ORDER BY d) OVER () FROM t",
]


def _shared_or_misparented(root: exp.Expr) -> list[str]:
    """Each node two places hold, and each child whose parent pointer names another holder."""
    wrong: list[str] = []
    seen: set[int] = set()
    stack = [root]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            wrong.append(f"held twice: {node!r}")
            continue
        seen.add(id(node))
        for value in node.args.values():
            for child in value if isinstance(value, list) else [value]:
                if isinstance(child, exp.Expr):
                    if child.parent is not node:
                        wrong.append(f"parent of {child!r} is not {node!r}")
                    stack.append(child)
    return wrong


@pytest.mark.parametrize("sql", _STATEMENTS)
def test_a_parse_is_a_tree_nothing_else_holds(sql: str) -> None:
    """A step that parsed its statement edits and renders the parse in place: no copy of it can
    differ from it only if the parser never places one node in two positions."""
    assert _shared_or_misparented(sqlglot.parse_one(sql, read="postgres")) == []


def test_readable_keeps_the_text_of_a_statement_with_nothing_to_write() -> None:
    """``differs`` stands for the comparison of the tree with its copy: the text only mentions a
    form, so the statement is returned as it was written."""
    sql = "SELECT 'convert(x)'   AS y  FROM t"
    assert pg_functions.readable(sql) == sql


def test_readable_writes_what_it_replaces() -> None:
    assert pg_functions.readable("SELECT to_hex(1)") == "SELECT PG_TO_HEX(1)"
    assert pg_functions.readable("SELECT CAST(a AS bit varying) FROM t") == (
        "SELECT CAST(a AS BIT) FROM t"
    )


def test_a_memoized_json_form_is_not_what_the_statement_edits() -> None:
    """``_form`` keeps one tree per text. Filling it is a copy: without one, the first statement's
    columns would be every later statement's."""
    sql = "CASE WHEN _p IS NOT NULL THEN _a END"
    kept = pg_forms_json._form(sql).sql(dialect="duckdb")
    filled = pg_forms_json._duckdb(sql, _p=exp.column("x"), _a=exp.column("y"))
    filled.set("default", exp.Null())
    assert filled is not pg_forms_json._form(sql)
    assert pg_forms_json._form(sql).sql(dialect="duckdb") == kept


def test_a_memoized_macro_form_is_not_what_the_statement_edits() -> None:
    """``_written`` copies ``_parsed``'s tree before it binds the arguments."""
    macro = function_macros.MACROS[("text", 1)]
    kept = function_macros._parsed("text", 1, "trino").sql(dialect="trino")
    statement = sqlglot.parse_one("SELECT text(x)", read="postgres")
    assert isinstance(statement, exp.Select)
    written = function_macros._written(macro, [exp.column("x")], "trino", statement.selects[0])
    written.replace(exp.Null())
    assert function_macros._parsed("text", 1, "trino").sql(dialect="trino") == kept


def test_the_join_rewrite_edits_the_tree_it_is_given_for_its_one_caller() -> None:
    """``_rewrite_from_join_subqueries`` no longer copies: its one caller must be the function that
    parsed the tree and replaces its own with the answer."""
    tree = sqlglot.parse_one("SELECT a FROM (SELECT 1 AS a) q", read="postgres")
    assert isinstance(tree, exp.Select)
    out, _ = transpile_correlated._rewrite_from_join_subqueries(tree)
    assert out is tree

    source = Path(transpile_correlated.__file__).read_text()
    calls = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_rewrite_from_join_subqueries"
    ]
    assert len(calls) == 1


@pytest.mark.parametrize(("dialect", "sql"), _FORMS)
def test_a_step_of_the_chain_edits_the_tree_it_is_handed(dialect: str, sql: str) -> None:
    """``inline`` and what it calls no longer copy: ``transpile`` hands each its tree and keeps
    only the answer. A caller that kept using its tree would see it edited."""
    tree = sqlglot.parse_one(sql, read="postgres")
    assert function_macros.inline(tree, dialect) is tree


def test_the_held_types_step_edits_the_tree_it_is_handed() -> None:
    tree = sqlglot.parse_one("SELECT CAST(a AS tid) FROM t", read="postgres")
    assert pg_held_types.held(tree) is tree


@pytest.mark.parametrize(("dialect", "sql"), _FORMS)
def test_what_a_form_writes_holds_no_node_twice(dialect: str, sql: str) -> None:
    """The next step edits in place what this one wrote. A form that placed one node in two
    positions would have the edit of one reach the other, which a copy between them used to part."""
    out = function_macros.inline(sqlglot.parse_one(sql, read="postgres"), dialect)
    assert [w for w in _shared_or_misparented(out) if w.startswith("held twice")] == []


@pytest.mark.parametrize(("dialect", "sql"), _FORMS)
def test_a_statement_written_twice_is_written_the_same(dialect: str, sql: str) -> None:
    """The forms are memoized by name and text: an edit reaching one would show on the second."""
    assert transpile(sql, dialect) == transpile(sql, dialect)
