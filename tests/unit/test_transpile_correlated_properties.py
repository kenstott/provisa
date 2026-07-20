# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for the SQL transpiler (REQ-066, REQ-068, REQ-229).

The transpiler is a SQL->SQL rewriter — the platform's silent-wrong-answer surface:
a bug returns malformed or non-equivalent SQL with no error, and example-based tests
only cover the shapes someone thought to write. These generate structured SQL and
assert INVARIANTS that must hold for every input, so hypothesis searches the space
for a counterexample:

  * rewrite_correlated_subqueries_for_trino is a stable fixed point (idempotent),
    always emits parseable SQL, preserves the top-level projection, and its output
    survives transpilation to Trino;
  * transpile(sql, dialect) always emits SQL that parses in that dialect.

Invariants are chosen to be unambiguous — a rewriter that leaves an unhandled
pattern in place still satisfies all of them, so a failure is a real defect, not a
known limitation.
"""

from __future__ import annotations

import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.transpiler.transpile import SUPPORTED_DIALECTS, transpile
from provisa.transpiler.transpile_correlated import rewrite_correlated_subqueries_for_trino

_OUTER = "t0"
_INNER = "t1"
_COLS = ["id", "a", "b", "c"]
_AGGS = ["sum", "count", "max", "min", "avg"]


@st.composite
def _corr_item(draw, alias: str) -> str:
    """One correlated projection item keyed on the outer row: a bare scalar subquery,
    a json_object wrapping one, or a json_agg subquery — the three shapes
    rewrite_correlated_subqueries_for_trino is documented to handle."""
    agg = draw(st.sampled_from(_AGGS))
    col = draw(st.sampled_from(_COLS))
    key = draw(st.sampled_from(_COLS))
    corr = f"SELECT {agg}({_INNER}.{col}) FROM {_INNER} WHERE {_INNER}.{key} = {_OUTER}.{key}"
    kind = draw(st.sampled_from(["scalar", "json_object", "json_agg"]))
    if kind == "scalar":
        return f"({corr}) AS {alias}"
    if kind == "json_object":
        return f"json_object('{col}', ({corr})) AS {alias}"
    return f"(SELECT json_agg({_INNER}.{col}) FROM {_INNER} WHERE {_INNER}.{key} = {_OUTER}.{key}) AS {alias}"


@st.composite
def _correlated_select(draw) -> str:
    """A SELECT whose projection carries 1-2 correlated subqueries (scalar/json),
    optionally with a (possibly multi-condition) WHERE and an outer sampling wrapper —
    the shapes rewrite_correlated_subqueries_for_trino is documented to handle."""
    plain = draw(st.lists(st.sampled_from(_COLS), min_size=1, max_size=3, unique=True))
    n_corr = draw(st.integers(min_value=1, max_value=2))
    corr = [draw(_corr_item(alias=f"corr{i}")) for i in range(n_corr)]
    projection = ", ".join([f"{_OUTER}.{c}" for c in plain] + corr)
    sql = f"SELECT {projection} FROM {_OUTER}"

    preds = draw(
        st.lists(
            st.sampled_from([f"{_OUTER}.a > 0", f"{_OUTER}.b = 1", f"{_OUTER}.c < 100"]),
            max_size=2,
            unique=True,
        )
    )
    if preds:
        sql += " WHERE " + " AND ".join(preds)

    if draw(st.booleans()):  # outer sampling wrapper: SELECT * FROM (inner) AS s [LIMIT N]
        limit = draw(st.integers(min_value=1, max_value=1000))
        sql = f"SELECT * FROM ({sql}) AS s LIMIT {limit}"
    return sql


def _projection_count(sql: str, dialect: str = "postgres") -> int:
    tree = sqlglot.parse_one(sql, read=dialect)
    # For the sampling wrapper the top-level projection is `*`; descend to the inner
    # select so the count compares like-for-like across rewrite.
    while tree is not None and tree.args.get("from_") is not None:
        frm = tree.args["from_"].this
        inner = getattr(frm, "this", None)
        if isinstance(frm, sqlglot.exp.Subquery) and isinstance(inner, sqlglot.exp.Select):
            tree = inner
            continue
        break
    return len(tree.selects) if isinstance(tree, sqlglot.exp.Select) else 0


@settings(max_examples=200, deadline=None)
@given(sql=_correlated_select())
def test_rewrite_is_idempotent(sql: str) -> None:
    """Rewriting is a fixed point: a second pass changes nothing. Catches unstable
    recursion (the classic failure mode of a tree rewriter that hoists CTEs)."""
    once = rewrite_correlated_subqueries_for_trino(sql)
    twice = rewrite_correlated_subqueries_for_trino(once)
    assert twice == once


@settings(max_examples=200, deadline=None)
@given(sql=_correlated_select())
def test_rewrite_output_is_parseable(sql: str) -> None:
    """The rewrite never emits malformed SQL — its output re-parses as Postgres."""
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert sqlglot.parse_one(out, read="postgres") is not None


@settings(max_examples=200, deadline=None)
@given(sql=_correlated_select())
def test_rewrite_preserves_projection(sql: str) -> None:
    """Lifting correlated subqueries to CTEs must not add or drop output columns —
    the caller's result shape is invariant under the rewrite."""
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert _projection_count(out) == _projection_count(sql)


@settings(max_examples=200, deadline=None)
@given(sql=_correlated_select())
def test_rewrite_output_transpiles_to_trino(sql: str) -> None:
    """The whole point: the de-correlated SQL is Trino-executable — it transpiles to
    Trino and the result parses in the Trino dialect."""
    out = rewrite_correlated_subqueries_for_trino(sql)
    trino_sql = transpile(out, "trino")
    assert sqlglot.parse_one(trino_sql, read="trino") is not None


@st.composite
def _plain_select(draw) -> str:
    cols = draw(st.lists(st.sampled_from(_COLS), min_size=1, max_size=4, unique=True))
    sql = f"SELECT {', '.join(cols)} FROM {_OUTER}"
    if draw(st.booleans()):
        sql += f" WHERE {draw(st.sampled_from(_COLS))} > {draw(st.integers(0, 100))}"
    if draw(st.booleans()):
        sql += f" ORDER BY {draw(st.sampled_from(_COLS))}"
    if draw(st.booleans()):
        sql += f" LIMIT {draw(st.integers(1, 1000))}"
    return sql


@settings(max_examples=200, deadline=None)
@given(sql=_plain_select(), dialect=st.sampled_from(sorted(SUPPORTED_DIALECTS)))
def test_transpile_output_parses_in_target_dialect(sql: str, dialect: str) -> None:
    """transpile(sql, d) always emits SQL that parses in dialect d — a transpilation
    that produces invalid target SQL is a silent break for that source type."""
    out = transpile(sql, dialect)
    assert sqlglot.parse_one(out, read=dialect) is not None
