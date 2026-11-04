# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view the reader is served no column of is refused, not read through its body.

A view is read as its SQL with the view table's own rules on top (mv/view_read.semantic_statement).
The view map names a view bare, and a reader's table map has the bare name only for the tables of
its compilation context: a view the reader is served nothing of is not among them, and its body was
put in with no rule of the view's on top -- every column the view's grant keeps from the reader,
read in clear. The banksecrecy demo's teller read the structuring view only compliance is granted
(REQ-2201)."""

# Requirements: REQ-133, REQ-2201

from types import SimpleNamespace

import pytest

from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.view_read import semantic_statement

_BODY = "SELECT c.person, c.amount FROM cash AS c"
_STATE = SimpleNamespace(mv_registry={}, tables=[], role_build_inputs={"draft_tables": []})


def _gov(*, served: bool) -> GovernanceContext:
    gov = GovernanceContext()
    # The stored names of every registered table; the bare names of the reader's own.
    gov.table_map = {"bank.cash": 1, "cash": 1, "bank.view_review": 2}
    gov.all_columns = {
        1: [("person", "varchar"), ("amount", "numeric")],
        2: [("person", "varchar")],
    }
    gov.visible_columns = {1: None, 2: None if served else frozenset()}
    if served:
        gov.table_map["view_review"] = 2
    return gov


def test_a_view_the_reader_is_served_nothing_of_is_refused():
    with pytest.raises(PermissionError, match="view_review"):
        semantic_statement(
            "SELECT person FROM view_review", {"view_review": _BODY}, _STATE, _gov(served=False)
        )


def test_a_view_the_reader_is_served_is_read_through_its_body():
    governed, _validated = semantic_statement(
        "SELECT person FROM view_review", {"view_review": _BODY}, _STATE, _gov(served=True)
    )
    assert "FROM cash AS c" in governed
