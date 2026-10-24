# Copyright (c) 2026 Kenneth Stott
# Canary: 4468f550-045d-4aea-8bdc-5bdae1e2faed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's string predicates answer as Cypher says (REQ-2135), on DuckDB: ``=~`` matches the
whole string, never a part of it; STARTS WITH, ENDS WITH and CONTAINS take their value literally,
a ``%`` or ``_`` in it a character like any other."""

# Requirements: REQ-2135

import pytest

from tests.unit import permits_on_duckdb as permits

_ADDRESSES = ["a%b", "axb", "a_b", "aab", "12 Oak St", "Oak", "xOak", "\\b", "b"]
_ROWS = [{"address": a} for a in _ADDRESSES]


@pytest.fixture(scope="module")
def model():
    return permits.model()


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("=~ 'Oak'", ["Oak"]),  # the whole string, not a part of it
        ("=~ '.*Oak.*'", ["12 Oak St", "Oak", "xOak"]),
        ("=~ 'Oak|axb'", ["Oak", "axb"]),  # an alternation, each side whole
        ("=~ '(?i)oak'", ["Oak"]),  # inline flags kept first
        ("=~ 'a.b'", ["a%b", "a_b", "aab", "axb"]),
        ("STARTS WITH 'a%'", ["a%b"]),
        ("STARTS WITH 'a_'", ["a_b"]),
        ("ENDS WITH '%b'", ["a%b"]),
        ("ENDS WITH '_b'", ["a_b"]),
        ("ENDS WITH '\\\\b'", ["\\b"]),
        ("ENDS WITH ''", sorted(_ADDRESSES)),
        ("ENDS WITH 'xb'", ["axb"]),
        ("ENDS WITH 'aab0'", []),
        ("CONTAINS '%'", ["a%b"]),
        ("CONTAINS '_'", ["a_b"]),
    ],
)
def test_a_string_predicate_answers_as_cypher_says(model, predicate, expected):
    text = f"MATCH (p:Permits) WHERE p.address {predicate} RETURN p.address AS a"
    assert sorted(permits.cypher_answer(text, model, _ROWS)) == sorted(expected)
