# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2d71-0c5a-4f93-a6e1-7d2c9b3f5e08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1387: a term's status is set by people, its grounding is computed by looking down its own
paths to data, and what an agent sees follows from both."""

from __future__ import annotations

from provisa.core.glossary import (
    GROUNDED,
    GROUNDED_PENDING,
    NOT_GROUNDED,
    agent_paths,
    agent_view,
    approvals_that_ground,
    term_grounding,
    term_status,
)


def _t(tid: int, status: str = "draft", definition: str | None = "Defined.") -> dict:
    return {
        "id": tid,
        "approved": status == "approved",
        "deprecated": False,
        "deprecation_asserted": status == "deprecated",
        "retired": status == "retired",
        "definition": definition,
    }


# Ids: 1 is the data end (holds the binding); 2.. sit above it.
DATA = 1


def _k(narrower: int, broader: int) -> tuple[int, int, str]:
    """``narrower`` is a kind of ``broader``: grounding runs down from ``broader`` to it."""
    return (narrower, broader, "KIND_OF")


def test_status_is_set_by_people():
    assert term_status(_t(1)) == "draft"
    assert term_status(_t(1, "approved")) == "approved"
    assert term_status(_t(1, "deprecated")) == "deprecated"
    assert term_status(_t(1, "retired")) == "retired"
    assert term_status({**_t(1, "approved"), "deprecated": True}) == "deprecated"


def test_a_draft_outer_tip_stays_grounded():
    # tip(3, draft) - mid(2, approved) - data(1)
    terms = [_t(1), _t(2, "approved"), _t(3)]
    grounding = term_grounding(terms, [_k(2, 3), _k(1, 2)], {DATA})
    assert grounding == {1: GROUNDED, 2: GROUNDED, 3: GROUNDED}


def test_a_draft_in_the_middle_makes_a_term_pending():
    # top(3, approved) - mid(2, draft) - data(1)
    terms = [_t(1, "approved"), _t(2), _t(3, "approved")]
    grounding = term_grounding(terms, [_k(2, 3), _k(1, 2)], {DATA})
    assert grounding[3] == GROUNDED_PENDING
    assert grounding[2] == GROUNDED  # nothing strictly between it and data
    assert approvals_that_ground(terms, [_k(2, 3), _k(1, 2)], {DATA}) == {3: [2]}


def test_a_draft_beside_a_clean_path_does_not():
    # top(4) reaches data(1) through draft 2 and through approved 3
    terms = [_t(1), _t(2), _t(3, "approved"), _t(4, "approved")]
    edges = [_k(2, 4), _k(1, 2), _k(3, 4), _k(1, 3)]
    assert term_grounding(terms, edges, {DATA})[4] == GROUNDED
    assert approvals_that_ground(terms, edges, {DATA}) == {}


def test_an_auto_derived_draft_at_the_data_end_does_not():
    # top(3, approved) - mid(2, approved) - data(1, draft, undefined: derived from a column)
    terms = [_t(1, definition=None), _t(2, "approved"), _t(3, "approved")]
    grounding = term_grounding(terms, [_k(2, 3), _k(1, 2)], {DATA})
    assert grounding == {1: GROUNDED, 2: GROUNDED, 3: GROUNDED}


def test_a_draft_above_an_approved_chain_changes_nothing_below():
    terms = [_t(1), _t(2, "approved"), _t(3, "approved")]
    edges = [_k(2, 3), _k(1, 2)]
    before = term_grounding(terms, edges, {DATA})
    after = term_grounding([*terms, _t(9)], [*edges, _k(3, 9)], {DATA})
    assert {k: after[k] for k in before} == before
    assert after[9] == GROUNDED


def test_deprecated_and_retired_break_the_chain():
    for status in ("deprecated", "retired"):
        terms = [_t(1), _t(2, status), _t(3, "approved")]
        grounding = term_grounding(terms, [_k(2, 3), _k(1, 2)], {DATA})
        assert grounding[3] == NOT_GROUNDED, status
        assert grounding[2] == NOT_GROUNDED, status


def test_a_deprecated_data_end_reaches_nothing():
    terms = [_t(1, "deprecated"), _t(2, "approved")]
    assert term_grounding(terms, [_k(1, 2)], {DATA})[2] == NOT_GROUNDED


def test_no_path_is_not_grounded():
    terms = [_t(1), _t(2, "approved")]
    assert term_grounding(terms, [], {DATA})[2] == NOT_GROUNDED


def test_one_approval_names_each_draft_that_grounds():
    # top(4, approved) reaches data through draft 2 or through draft 3
    terms = [_t(1), _t(2), _t(3), _t(4, "approved")]
    edges = [_k(2, 4), _k(1, 2), _k(3, 4), _k(1, 3)]
    assert approvals_that_ground(terms, edges, {DATA}) == {4: [2, 3]}
    # two drafts in series: no single approval grounds it
    terms = [_t(1), _t(2), _t(3), _t(4, "approved")]
    edges = [_k(3, 4), _k(2, 3), _k(1, 2)]
    grounding = term_grounding(terms, edges, {DATA})
    assert grounding[4] == GROUNDED_PENDING
    assert 4 not in approvals_that_ground(terms, edges, {DATA})


# --- down is from broader to narrower (REQ-1387, DOWN IS FROM BROADER TO NARROWER) ---------------

SUPPLIER, PARTY, CUSTOMER, VENDOR, CLIENT = 11, 12, 13, 14, 15


def test_a_sibling_is_never_grounded_through_a_shared_parent():
    # Supplier and Customer are both kinds of Party (draft); only Customer holds data.
    terms = [_t(SUPPLIER, "approved"), _t(PARTY), _t(CUSTOMER, "approved")]
    edges = [_k(SUPPLIER, PARTY), _k(CUSTOMER, PARTY)]
    grounding = term_grounding(terms, edges, {CUSTOMER})
    assert grounding[SUPPLIER] == NOT_GROUNDED
    assert grounding[PARTY] == GROUNDED
    assert grounding[CUSTOMER] == GROUNDED
    assert SUPPLIER not in approvals_that_ground(terms, edges, {CUSTOMER})
    assert SUPPLIER not in agent_view(terms, edges, {CUSTOMER}).seen()


def test_a_parent_is_grounded_through_its_children():
    terms = [_t(PARTY, "approved"), _t(CUSTOMER), _t(VENDOR)]
    edges = [_k(CUSTOMER, PARTY), _k(VENDOR, PARTY)]
    assert term_grounding(terms, edges, {CUSTOMER, VENDOR})[PARTY] == GROUNDED
    # A child is never grounded through its parent.
    assert term_grounding(terms, edges, {PARTY}) == {
        PARTY: GROUNDED,
        CUSTOMER: NOT_GROUNDED,
        VENDOR: NOT_GROUNDED,
    }


def test_part_of_and_valid_value_of_run_down_to_the_part_and_the_value():
    for rel_type in ("PART_OF", "VALID_VALUE_OF"):
        terms = [_t(1), _t(2, "approved")]
        edges = [(1, 2, rel_type)]  # 1 is a part / a valid value of 2
        assert term_grounding(terms, edges, {1})[2] == GROUNDED, rel_type
        assert term_grounding(terms, edges, {2})[1] == NOT_GROUNDED, rel_type


def test_grounding_crosses_a_synonym_either_way():
    for edge in ((CLIENT, CUSTOMER, "SYNONYM_OF"), (CUSTOMER, CLIENT, "SYNONYM_OF")):
        terms = [_t(CLIENT, "approved"), _t(CUSTOMER)]
        assert term_grounding(terms, [edge], {CUSTOMER})[CLIENT] == GROUNDED, edge
        view = agent_view(terms, [edge], {CUSTOMER})
        assert agent_paths(view, {CUSTOMER}, CLIENT) == [[CLIENT, CUSTOMER]], edge


def test_a_synonym_up_a_chain_grounds_its_partner():
    # Client is a synonym of Party (approved), Customer (data) is a kind of Party.
    terms = [_t(CLIENT, "approved"), _t(PARTY, "approved"), _t(CUSTOMER)]
    edges = [(CLIENT, PARTY, "SYNONYM_OF"), _k(CUSTOMER, PARTY)]
    assert term_grounding(terms, edges, {CUSTOMER})[CLIENT] == GROUNDED
    # A draft Party in between makes it pending.
    terms = [_t(CLIENT, "approved"), _t(PARTY), _t(CUSTOMER)]
    assert term_grounding(terms, edges, {CUSTOMER})[CLIENT] == GROUNDED_PENDING
    assert approvals_that_ground(terms, edges, {CUSTOMER}) == {CLIENT: [PARTY]}


def test_other_edge_types_conduct_no_grounding():
    for rel_type in (
        "RELATED_TO",
        "DERIVED_FROM",
        "REPLACES",
        "TRANSLATION_OF",
        "ANTONYM_OF",
        "PREFERRED_TERM_FOR",
    ):
        for edge in ((2, 1, rel_type), (1, 2, rel_type)):
            terms = [_t(1), _t(2, "approved")]
            assert term_grounding(terms, [edge], {DATA})[2] == NOT_GROUNDED, edge
            assert 2 not in agent_view(terms, [edge], {DATA}).seen(), edge


# --- what an agent sees --------------------------------------------------------------------------


def test_an_approved_term_on_a_path_to_data_is_seen():
    terms = [_t(1), _t(2, "approved")]
    view = agent_view(terms, [_k(1, 2)], {DATA})
    assert view.named == {1: "draft", 2: "approved"}
    assert view.hops == frozenset()


def test_a_chain_with_no_approved_term_is_hidden():
    terms = [_t(1), _t(2), _t(3)]
    view = agent_view(terms, [_k(2, 3), _k(1, 2)], {DATA})
    assert view.named == {}
    assert view.hops == frozenset()


def test_undefined_terms_are_distinct_anonymous_hops():
    # approved(5) - undefined(4) - undefined(3) - defined draft(2) - data(1, undefined)
    terms = [_t(1, definition=None), _t(2), _t(3, definition=None), _t(4, definition=None)]
    terms.append(_t(5, "approved"))
    edges = [_k(4, 5), _k(3, 4), _k(2, 3), _k(1, 2)]
    view = agent_view(terms, edges, {DATA})
    assert view.named == {5: "approved", 2: "draft"}
    assert view.hops == frozenset({1, 3, 4})
    assert agent_paths(view, {DATA}, 5) == [[5, 4, 3, 2, 1]]


def test_an_approved_term_reaching_no_data_is_hidden():
    terms = [_t(1), _t(2, "approved"), _t(3)]
    view = agent_view(terms, [_k(1, 3)], {DATA})
    assert 2 not in view.seen()
    assert view.named == {}


def test_a_term_only_beside_an_approved_path_is_hidden():
    # approved(2) - data(1) - draft(3): 3's own path is 3-1, which holds no approved term
    terms = [_t(1), _t(2, "approved"), _t(3)]
    view = agent_view(terms, [_k(1, 2), _k(1, 3)], {DATA})
    assert view.named == {1: "draft", 2: "approved"}


def test_deprecated_and_retired_terms_are_never_seen():
    for status in ("deprecated", "retired"):
        terms = [_t(1), _t(2, status), _t(3, "approved")]
        view = agent_view(terms, [_k(2, 3), _k(1, 2)], {DATA})
        assert view.named == {}, status
