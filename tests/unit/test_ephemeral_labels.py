# Copyright (c) 2026 Kenneth Stott
# Canary: e36c16de-b701-4403-b74a-e5a5265895dc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A label SET or REMOVE, read from the graph IR (REQ-2176, REQ-2135): only one SET or REMOVE of
label items alone, ending the statement, is one; anything else is left to its own path."""

# Requirements: REQ-2176, REQ-2135

from __future__ import annotations

import pytest

from provisa.ephemeral.labels import LabelClause, label_clause

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("statement", "clause"),
    [
        (
            "MATCH (n:Person) WHERE n.id = 1 SET n:Vip:Gold",
            LabelClause("MATCH (n:Person) WHERE n.id = 1", "SET", (("n", ("Vip", "Gold")),)),
        ),
        (
            "MATCH (a), (b) REMOVE a:X, b:Y",
            LabelClause("MATCH (a), (b)", "REMOVE", (("a", ("X",)), ("b", ("Y",)))),
        ),
        ("MATCH (n) SET n:`Odd Name`", LabelClause("MATCH (n)", "SET", (("n", ("Odd Name",)),))),
    ],
)
def test_a_label_set_or_remove(statement, clause):
    assert label_clause(statement) == clause


@pytest.mark.parametrize(
    "statement",
    [
        "MATCH (n:Person) RETURN n",
        "MATCH (n) WHERE n.note = 'SET n:X' RETURN n",
        "MATCH (n:Person) SET n.age = 3",
        "MATCH (n:Person) SET n:Vip, n.age = 3",
        "MATCH (n) SET n += {a: 1}",
        "MATCH (n) REMOVE n.age",
        "MATCH (n) SET n:Vip RETURN n",
        "MATCH (n) SET n:Vip WITH n SET n:Gold",
        "CREATE (n:Temp)",
    ],
)
def test_anything_else_is_left_to_its_own_path(statement):
    assert label_clause(statement) is None
