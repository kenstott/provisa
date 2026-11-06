# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB 2.0 binds a lambda only in its keyword form (``LAMBDA x : body``): the arrow form
(``x -> body``) is a binder error unless the deprecated ``lambda_syntax`` setting brings it back.
Every lambda the transpiler writes for DuckDB is therefore the keyword form, whatever built it."""

# Requirements: REQ-066, REQ-863

from __future__ import annotations

import duckdb
import pytest
from sqlglot import exp

import provisa.transpiler  # noqa: F401 -- registers the dialect adjustments
from provisa.transpiler.transpile import transpile_to_duckdb


def test_duckdb_refuses_the_arrow_lambda_by_default():
    # What the pin changes: the arrow form no longer binds.
    con = duckdb.connect()
    try:
        with pytest.raises(duckdb.BinderException, match="lambda"):
            con.execute("SELECT list_transform([1, 2], x -> x + 1)")
    finally:
        con.close()


def test_a_built_lambda_is_written_in_the_keyword_form():
    lam = exp.Lambda(
        this=exp.Add(this=exp.column("x"), expression=exp.Literal.number(1)),
        expressions=[exp.to_identifier("x")],
    )
    call = exp.Anonymous(this="list_transform", expressions=[exp.column("l"), lam])
    assert call.sql(dialect="duckdb") == "LIST_TRANSFORM(l, LAMBDA x : x + 1)"


def test_a_two_argument_lambda_is_written_in_the_keyword_form():
    lam = exp.Lambda(
        this=exp.Add(this=exp.column("a"), expression=exp.column("b")),
        expressions=[exp.to_identifier("a"), exp.to_identifier("b")],
    )
    call = exp.Anonymous(this="list_reduce", expressions=[exp.column("l"), lam])
    assert call.sql(dialect="duckdb") == "LIST_REDUCE(l, LAMBDA a, b : a + b)"


def test_the_semantic_filtered_list_runs_on_duckdb():
    # Cypher's [y IN l WHERE p] is kept FILTER(l, y -> p) in the semantic SQL (semantic_dialect).
    sql = transpile_to_duckdb("SELECT FILTER(ARRAY[1, 2, 3], y -> y > 1) AS v")
    assert "->" not in sql
    con = duckdb.connect()
    try:
        assert con.execute(sql).fetchall() == [([2, 3],)]
    finally:
        con.close()


def test_writing_a_lambda_leaves_the_tree_unchanged():
    lam = exp.Lambda(this=exp.column("x"), expressions=[exp.to_identifier("x")])
    lam.sql(dialect="duckdb")
    assert lam.args.get("colon") is None
    assert lam.sql(dialect="trino") == "x -> x"
