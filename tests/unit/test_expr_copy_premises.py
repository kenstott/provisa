# Copyright (c) 2026 Kenneth Stott
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

from provisa.transpiler import function_macros, pg_forms_json, pg_functions, transpile_correlated

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
    call = sqlglot.parse_one("SELECT text(x)", read="postgres").selects[0]
    written = function_macros._written(macro, [exp.column("x")], "trino", call)
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
