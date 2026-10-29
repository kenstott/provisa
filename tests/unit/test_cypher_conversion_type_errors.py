# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7b9e14-6c3a-4d81-9e02-b5a7c1d4e863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""openCypher's conversions in a session's graph (REQ-347, REQ-2176, REQ-2167), as the TCK's
TypeConversion1-4 state them: a value of a kind a conversion does not take -- a list, a map, a
node, a relationship, a path; toFloat a boolean, toBoolean a float -- is a TypeError, never null,
also as an item of a list of several types."""

# Requirements: REQ-347, REQ-2176, REQ-2167

from __future__ import annotations

import duckdb
import pytest

from provisa.gql.translator_types import CypherTranslateError
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_EDGE = "CREATE ()-[:T]->()"


def _session(monkeypatch, *setup: str) -> Session:
    s = Session(monkeypatch, {})
    for statement in setup:
        s.run(statement)
    return s


# -- TypeConversion1-4: a TypeError, never null --------------------------------------------- #


@pytest.mark.parametrize(
    ("call", "first", "invalid"),
    [
        ("toBoolean", "true", "[]"),
        ("toBoolean", "true", "{}"),
        ("toBoolean", "true", "1.0"),
        ("toBoolean", "true", "n"),
        ("toBoolean", "true", "r"),
        ("toBoolean", "true", "p"),
        ("toInteger", "1", "[]"),
        ("toInteger", "1", "{}"),
        ("toInteger", "1", "n"),
        ("toInteger", "1", "r"),
        ("toInteger", "1", "p"),
        ("toFloat", "1.0", "true"),
        ("toFloat", "1.0", "[]"),
        ("toFloat", "1.0", "{}"),
        ("toFloat", "1.0", "n"),
        ("toFloat", "1.0", "r"),
        ("toFloat", "1.0", "p"),
        ("toString", "1, ''", "[]"),
        ("toString", "1, ''", "{}"),
        ("toString", "1, ''", "n"),
        ("toString", "1, ''", "r"),
        ("toString", "1, ''", "p"),
    ],
)
def test_a_conversion_of_a_kind_it_does_not_take_is_a_type_error(monkeypatch, call, first, invalid):
    s = _session(monkeypatch, _EDGE)
    query = f"MATCH p = (n)-[r:T]->() RETURN [x IN [{first}, {invalid}] | {call}(x)] AS list"
    with pytest.raises(duckdb.InvalidInputException, match="TypeError"):
        s.run(query)


def test_a_conversion_over_no_row_raises_nothing(monkeypatch):
    # The TypeError is the row's: a read of no row converts nothing.
    s = _session(monkeypatch, "CREATE (:Z)")
    assert s.run("MATCH (n:Z) WHERE 1 = 2 RETURN [x IN [1, n] | toInteger(x)] AS l") == []


def test_a_conversion_of_a_kind_it_takes_converts(monkeypatch):
    s = _session(monkeypatch)
    assert s.run("RETURN [x IN [1, 2.3, true, 'apa'] | toString(x)] AS l") == [
        {"l": ["1", "2.3", "true", "apa"]}
    ]
    assert s.run("WITH [2, 2.9, '1.7'] AS t RETURN [n IN t | toInteger(n)] AS l") == [
        {"l": [2, 2, 1]}
    ]
    assert s.run("WITH [3.4, 3, '5'] AS t RETURN [n IN t | toFloat(n)] AS l") == [
        {"l": [3.4, 3.0, 5.0]}
    ]
    assert s.run("UNWIND [null, '', ' tru '] AS v RETURN toBoolean(v) AS b") == [
        {"b": None},
        {"b": None},
        {"b": None},
    ]


@pytest.mark.parametrize("query", ["RETURN toInteger([1]) AS v", "RETURN toFloat(true) AS v"])
def test_a_conversion_the_statement_types_wrong_is_refused_when_read(monkeypatch, query):
    s = _session(monkeypatch)
    with pytest.raises(CypherTranslateError, match="TypeError"):
        s.run(query)
