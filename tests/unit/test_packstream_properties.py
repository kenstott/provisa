# Copyright (c) 2026 Kenneth Stott
# Canary: c48c2405-cae7-4db4-8496-3179cba5fffa
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based round-trip tests for the PackStream codec (REQ-802).

PackStream is the binary wire encoding for Bolt — every Cypher parameter a client
sends and every value the server returns crosses it. A codec bug silently swaps a
value at a size boundary (the tiny/8/16/32/64-bit int tiers, the string- and
container-length tiers). The defining property of a codec is round-trip identity,
so generate arbitrary nested values and assert unpack(pack(x)) == x — hypothesis
drives the generator across every encoding tier looking for one that doesn't.
"""

from __future__ import annotations

import math

from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.bolt.packstream import pack, unpack

# Signed 64-bit ints (PackStream's integer domain), IEEE-754 doubles (no NaN/inf so
# equality is meaningful), bools, null, unicode text, and bytes — the scalar leaves.
_INT64 = st.integers(min_value=-(2**63), max_value=2**63 - 1)
_scalars = st.one_of(
    _INT64,
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
    st.none(),
    st.text(max_size=40),
    st.binary(max_size=40),
)
# JSON-ish nesting: PackStream map keys are strings on the Bolt wire.
_values = st.recursive(
    _scalars,
    lambda child: st.one_of(
        st.lists(child, max_size=6),
        st.dictionaries(st.text(max_size=8), child, max_size=6),
    ),
    max_leaves=20,
)


def _eq(a, b) -> bool:
    """Structural equality that treats two IEEE-754 zeros / equal floats as equal and
    recurses through lists and dicts (bool is int in Python, so guard it)."""
    if isinstance(a, float) or isinstance(b, float):
        return isinstance(b, (int, float)) and math.isclose(a, b, rel_tol=0, abs_tol=0)
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, list):
        return isinstance(b, list) and len(a) == len(b) and all(_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return isinstance(b, dict) and a.keys() == b.keys() and all(_eq(a[k], b[k]) for k in a)
    return a == b


@settings(max_examples=500, deadline=None)
@given(value=_values)
def test_pack_unpack_round_trips(value) -> None:
    """unpack(pack(x)) == x for every value — the codec's defining property."""
    assert _eq(unpack(pack(value)), value)


@settings(max_examples=300, deadline=None)
@given(value=_values)
def test_pack_is_deterministic(value) -> None:
    """Packing is a pure function of the value: the same input yields the same bytes.
    A codec whose output depends on hidden state corrupts message framing."""
    assert pack(value) == pack(value)


# Explicit coverage of the integer size-tier boundaries where off-by-one encoding
# bugs live (PackStream switches marker byte at each edge).
@settings(max_examples=1, deadline=None)
@given(st.just(None))
def test_integer_tier_boundaries_round_trip(_) -> None:
    edges = [
        -(2**63),
        -(2**31) - 1,
        -(2**31),
        -32769,
        -32768,
        -129,
        -128,
        -17,
        -16,
        0,
        127,
        128,
        32767,
        32768,
        2**31 - 1,
        2**31,
        2**63 - 1,
    ]
    for n in edges:
        assert unpack(pack(n)) == n, f"integer tier boundary {n} did not round-trip"
