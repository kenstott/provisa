# Copyright (c) 2026 Kenneth Stott
# Canary: 2465a967-d2cc-4ad9-87b5-b6c04782eb4a
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for Cypher comprehension rewriting (REQ-345, REQ-347).

rewrite_list_comprehensions / rewrite_reduce turn Cypher list comprehensions and
reduce() into SQLGlot-parseable lambda syntax (transform/filter/reduce). A bug
produces an expression the engine cannot parse — a silent query failure — or an
unstable rewrite. Generate the documented Cypher shapes and assert, for every one:

  * the rewrite fires (output differs — the generator really feeds it a
    comprehension, so the checks below are not vacuous);
  * the result embeds into a Trino SELECT that parses (the whole purpose);
  * rewriting is idempotent (a second pass is a no-op).
"""

from __future__ import annotations

import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.cypher.comprehension import rewrite_list_comprehensions

_VARS = ["x", "y", "n", "i"]
_LISTS = ["list", "items", "nums", "vals"]
_INITS = ["0", "1", "10"]


@st.composite
def _list_comprehension(draw) -> str:
    v = draw(st.sampled_from(_VARS))
    lst = draw(st.sampled_from(_LISTS))
    body = f"{v} {draw(st.sampled_from(['+', '*', '-']))} {draw(st.integers(1, 9))}"
    if draw(st.booleans()):
        pred = f"{v} {draw(st.sampled_from(['>', '<', '=']))} {draw(st.integers(0, 9))}"
        return f"[{v} IN {lst} WHERE {pred} | {body}]"
    return f"[{v} IN {lst} | {body}]"


@st.composite
def _reduce(draw) -> str:
    acc = draw(st.sampled_from(["acc", "s", "total"]))
    v = draw(st.sampled_from(_VARS))
    lst = draw(st.sampled_from(_LISTS))
    init = draw(st.sampled_from(_INITS))
    body = f"{acc} {draw(st.sampled_from(['+', '*']))} {v}"
    return f"reduce({acc} = {init}, {v} IN {lst} | {body})"


_comprehension = st.one_of(_list_comprehension(), _reduce())


@settings(max_examples=200, deadline=None)
@given(expr=_comprehension)
def test_rewrite_fires_and_output_parses_in_trino(expr: str) -> None:
    out = rewrite_list_comprehensions(expr)
    assert out != expr, "generated comprehension was not rewritten (test would be vacuous)"
    # The rewritten expression must sit inside a Trino SELECT and parse.
    assert sqlglot.parse_one(f"SELECT {out}", read="trino") is not None


@settings(max_examples=200, deadline=None)
@given(expr=_comprehension)
def test_rewrite_is_idempotent(expr: str) -> None:
    once = rewrite_list_comprehensions(expr)
    assert rewrite_list_comprehensions(once) == once


@st.composite
def _two_comprehensions(draw) -> str:
    """Two comprehensions in one expression — exercises multi-match rewriting."""
    a = draw(_list_comprehension())
    b = draw(st.one_of(_list_comprehension(), _reduce()))
    return f"{a} + size({b})"


@settings(max_examples=150, deadline=None)
@given(expr=_two_comprehensions())
def test_multiple_comprehensions_all_rewritten(expr: str) -> None:
    out = rewrite_list_comprehensions(expr)
    # Both Cypher forms are gone: no residual `IN ... |` pipe survives.
    assert " | " not in out, f"a comprehension was left un-rewritten: {out}"
    assert sqlglot.parse_one(f"SELECT {out}", read="trino") is not None
    assert rewrite_list_comprehensions(out) == out
