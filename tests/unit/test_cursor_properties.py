# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for cursor pagination encoding (REQ-218).

A connection cursor encodes the sort-key values of the last row on a page; the next
page resumes strictly after them. If encode/decode is not an exact round-trip, the
resume key is wrong — rows are skipped or served twice across page boundaries.
Assert the codec's defining properties over arbitrary sort-key lists:

  * decode_cursor(encode_cursor(x)) == x — exact round-trip;
  * encoding is deterministic;
  * a malformed cursor is rejected (ValueError), never silently mis-decoded.
"""

from __future__ import annotations

import pytest
import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.compiler.cursor import (
    cursor_where_clause,
    decode_cursor,
    encode_cursor,
    reverse_order,
)
from provisa.compiler.params import ParamCollector

# JSON-native sort-key values (what a cursor carries): the codec is base64(JSON), so
# these round-trip exactly. NaN/inf excluded so equality is meaningful.
_json_scalar = st.one_of(
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
    st.none(),
    st.text(max_size=30),
)
_json = st.recursive(
    _json_scalar,
    lambda child: st.one_of(
        st.lists(child, max_size=5), st.dictionaries(st.text(max_size=8), child, max_size=5)
    ),
    max_leaves=10,
)
# A cursor is a LIST of sort-key values (one per ORDER BY column).
_sort_values = st.lists(_json, max_size=5)


@settings(max_examples=500, deadline=None)
@given(values=_sort_values)
def test_cursor_round_trips(values) -> None:
    """decode(encode(x)) == x — the resume key survives a page boundary intact."""
    assert decode_cursor(encode_cursor(values)) == values


@settings(max_examples=200, deadline=None)
@given(values=_sort_values)
def test_encode_is_deterministic(values) -> None:
    assert encode_cursor(values) == encode_cursor(values)


@pytest.mark.parametrize("bad", ["!!!", "@@@@", "not base64", "*", "%%%%", "a b c"])
def test_malformed_cursor_is_rejected(bad: str) -> None:
    """Garbage that is not valid base64(JSON) raises rather than mis-decoding to a
    bogus resume key."""
    with pytest.raises(ValueError):
        decode_cursor(bad)


# --------------------------------------------------------------------------- #
# reverse_order — used to flip an ORDER BY for backward pagination.
# --------------------------------------------------------------------------- #
_DIRECTIONS = [
    "ASC",
    "DESC",
    "ASC NULLS FIRST",
    "ASC NULLS LAST",
    "DESC NULLS FIRST",
    "DESC NULLS LAST",
]


@st.composite
def _order_by(draw) -> str:
    cols = draw(
        st.lists(st.sampled_from(["a", "b", "c", "t0.id"]), min_size=1, max_size=3, unique=True)
    )
    parts = [f"{c} {draw(st.sampled_from(_DIRECTIONS))}" for c in cols]
    return ", ".join(parts)


@settings(max_examples=200, deadline=None)
@given(clause=_order_by())
def test_reverse_order_is_an_involution(clause: str) -> None:
    """Reversing an ORDER BY twice restores it — so a backward page read forward is
    the original forward order (no drift in NULLS placement or direction)."""
    assert reverse_order(reverse_order(clause)) == clause


# --------------------------------------------------------------------------- #
# cursor_where_clause — the keyset predicate that resumes after a cursor.
# --------------------------------------------------------------------------- #
@st.composite
def _keyset(draw):
    n = draw(st.integers(min_value=1, max_value=3))
    cols = draw(
        st.lists(
            st.sampled_from(["id", "amount", "name", "ts"]), min_size=n, max_size=n, unique=True
        )
    )
    values = draw(st.lists(st.integers(-100, 100), min_size=len(cols), max_size=len(cols)))
    direction = draw(st.sampled_from(["forward", "backward"]))
    return cols, values, direction


@settings(max_examples=200, deadline=None)
@given(case=_keyset())
def test_cursor_where_clause_is_a_valid_keyset_predicate(case) -> None:
    """The resume predicate compares the sort tuple against the bound cursor values:
    forward uses `>`, backward uses `<`; it names every sort column, binds every
    cursor value as a param, and parses as a boolean expression."""
    cols, values, direction = case
    collector = ParamCollector()
    frag = cursor_where_clause(cols, values, direction, collector, "t0")

    assert sqlglot.parse_one(f"SELECT 1 WHERE {frag}", read="postgres") is not None
    for c in cols:
        assert f'"{c}"' in frag, f"sort column {c} missing from the keyset predicate"
    assert (">" in frag) == (direction == "forward")
    assert ("<" in frag) == (direction == "backward")
    assert len(collector.params) == len(values), "not every cursor value was bound as a param"
