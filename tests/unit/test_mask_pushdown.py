# Copyright (c) 2026 Kenneth Stott
# Canary: 7f7ba484-deba-4e1d-93ba-a79b04c34f23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Data masking as an IR projection expression with pushdown/streaming (REQ-971)."""

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.federation.connector_base import Capability
from provisa.federation.promote import MaskEval, plan_mask_evaluation
from provisa.security.masking import MaskType, MaskingRule, apply_mask_to_value


# --- Masks are IR projection expressions (participate in projection pushdown) ---


def test_mask_is_a_projection_expression_in_the_governed_ir():
    """A masked column becomes a mask EXPRESSION in the SELECT projection — the pushdown vehicle.

    Because the mask rides in the governed SQL projection sent to the source, the raw column
    value is never selected into the tier: the source evaluates the expression.
    """
    gov = GovernanceContext(
        masking_rules={
            (7, "email"): (
                MaskingRule(mask_type=MaskType.regex, pattern=".", replace="*"),
                "varchar",
            )
        },
        table_map={"users": 7, "public.users": 7},
        all_columns={7: [("email", "varchar")]},
    )
    governed = apply_governance('SELECT "u"."email" FROM "public"."users" "u"', gov)
    assert "REGEXP_REPLACE" in governed.upper()  # mask expression is in the projection
    # The raw column is wrapped by the mask, not selected bare as the output value.
    assert 'SELECT "u"."email" FROM' not in governed


# --- Pushdown decision from the EXISTING connector capability query ---


def test_mask_pushes_down_when_connector_evaluates_expressions():
    cap = Capability(predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True)
    plan = plan_mask_evaluation(cap)
    assert plan.eval is MaskEval.SOURCE
    assert not plan.confidentiality_fallback


def test_mask_streams_when_source_cannot_evaluate():
    cap = Capability(predicate_pushdown=False)
    plan = plan_mask_evaluation(cap, can_stream=True)
    assert plan.eval is MaskEval.STREAM
    # Non-pushdown is acknowledged as a confidentiality fallback, never silent.
    assert plan.confidentiality_fallback


def test_mask_fails_loud_when_it_can_neither_push_down_nor_stream():
    cap = Capability(predicate_pushdown=False)
    with pytest.raises(ValueError, match="REQ-971"):
        plan_mask_evaluation(cap, can_stream=False)


# --- Bounded-memory streaming fallback: mask one event/row at a time (no buffer) ---


def test_streaming_mask_evaluates_a_single_row():
    """The streaming stage masks one value at a time (O(1) cleartext resident, no fetchall)."""
    rule = MaskingRule(mask_type=MaskType.constant, value="HIDDEN")
    assert apply_mask_to_value(rule, "secret", "varchar") == "HIDDEN"
