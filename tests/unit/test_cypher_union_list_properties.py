# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a6c17-2d48-4b0f-8f51-c7a4e9b2d630
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A list property of a node an unlabelled MATCH reads (REQ-347): the union of its labels' rows
casts it as an array, so it concatenates and sorts as a list. The cast had been built from the
session's own type name, list<bigint>, which the engine's parser refuses ("Expected a constant
as type modifier"). The scenarios are the openCypher TCK's (Return2 [7], WithOrderBy1 [31],
[32]), run on DuckDB over a session graph."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_LISTS = (
    "CREATE (:A {list: [2, -2]}), (:B {list: [1, 2]}), (:C {list: [300, 0]}), "
    "(:D {list: [1, -20]}), (:E {list: [2, -2, 100]})"
)


def test_list_properties_concatenate(monkeypatch):
    # Return2 [7]
    s = Session(monkeypatch, {})
    s.run("CREATE ({list1: [1, 2, 3], list2: [4, 5]})")
    assert s.run("MATCH (a) RETURN a.list2 + a.list1 AS foo") == [{"foo": [4, 5, 1, 2, 3]}]


@pytest.mark.parametrize(
    ("sort", "first"),
    [("list", [[1, -20], [1, 2], [2, -2]]), ("list DESC", [[300, 0], [2, -2, 100], [2, -2]])],
)
def test_a_list_variable_sorts_as_a_list(monkeypatch, sort, first):
    # WithOrderBy1 [31], [32]
    s = Session(monkeypatch, {})
    s.run(_LISTS)
    rows = s.run(
        f"MATCH (a) WITH a, a.list AS list WITH a, list ORDER BY {sort} LIMIT 3 RETURN list"
    )
    assert [r["list"] for r in rows] == first
