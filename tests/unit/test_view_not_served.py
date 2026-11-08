# Copyright (c) 2026 Kenneth Stott
# Canary: 6cf267d5-e985-401d-8aee-5996cdd3c663
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


# A view's stored name is what the view map keys it by; a reader's table map names it bare by its
# SEMANTIC name, which the naming rules may make differ from the stored one: a leading underscore
# is dropped, an alias renames it. Served or not is the reader's grant, never whether the two
# names happen to agree.
_STORED_VIEW = "_view_review"


def _gov_renamed(*, served: bool) -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"bank.cash": 1, "cash": 1, f"bank.{_STORED_VIEW}": 2}
    gov.all_columns = {
        1: [("person", "varchar"), ("amount", "numeric")],
        2: [("person", "varchar")],
    }
    gov.visible_columns = {1: None, 2: None if served else frozenset()}
    if served:
        gov.table_map["view_review"] = 2  # its semantic name, bare
    return gov


def test_a_served_view_whose_semantic_name_differs_from_its_stored_name_is_read():
    governed, _validated = semantic_statement(
        f"SELECT person FROM {_STORED_VIEW}",
        {_STORED_VIEW: _BODY},
        _STATE,
        _gov_renamed(served=True),
    )
    assert "FROM cash AS c" in governed


def test_an_unserved_view_whose_semantic_name_differs_from_its_stored_name_is_refused():
    with pytest.raises(PermissionError, match=_STORED_VIEW):
        semantic_statement(
            f"SELECT person FROM {_STORED_VIEW}",
            {_STORED_VIEW: _BODY},
            _STATE,
            _gov_renamed(served=False),
        )


def test_a_view_registered_with_no_column_is_served_nothing():
    # No column, so no compilation context holds it, and governance reads its (empty) column list
    # as all visible: served nothing all the same.
    gov = _gov_renamed(served=False)
    gov.visible_columns[2] = None
    gov.all_columns[2] = []
    with pytest.raises(PermissionError, match=_STORED_VIEW):
        semantic_statement(f"SELECT person FROM {_STORED_VIEW}", {_STORED_VIEW: _BODY}, _STATE, gov)
