# Copyright (c) 2026 Kenneth Stott
# Canary: 7f7ba484-deba-4e1d-93ba-a79b04c34f23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Data masking as an IR projection expression with pushdown/streaming (REQ-971)."""

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance, build_governance_context
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
    assert "PROVISA_REGEX_REPLACE" in governed.upper()  # mask expression is in the projection
    # The raw column is wrapped by the mask in the governed relation, never served bare.
    assert '"u"."email" AS "email"' not in governed


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
    with pytest.raises(ValueError, match="can neither push down to the source nor stream"):
        plan_mask_evaluation(cap, can_stream=False)


# --- Bounded-memory streaming fallback: mask one event/row at a time (no buffer) ---


def test_streaming_mask_evaluates_a_single_row():
    """The streaming stage masks one value at a time (O(1) cleartext resident, no fetchall)."""
    rule = MaskingRule(mask_type=MaskType.constant, value="HIDDEN")
    assert apply_mask_to_value(rule, "secret", "varchar") == "HIDDEN"


# --- The governed pipeline: a mask the source cannot evaluate is computed by the engine ---


class _EngineOver:
    """The bound engine as governance reads it: each source's declared connector capability."""

    def __init__(self, cap: Capability) -> None:
        self._cap = cap

    def connector_pushdown(self, source) -> Capability:
        return self._cap


def test_a_masked_read_on_a_source_that_cannot_push_the_mask_down_is_masked_by_the_engine():
    """REQ-971, REQ-1494: a SQLite file on DuckDB pushes projections only (REQ-863), so the mask
    cannot be evaluated at the source. The governed statement carries the mask expression and the
    engine computes it as it streams the result -- the read is served masked, never refused, and
    the raw value is never served bare."""
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.federation.connector_duckdb import DuckDBSqliteConnector

    role = {"id": "sandbox", "domain_access": ["*"], "capabilities": ["query_development"]}
    tables = [
        {
            "id": 7,
            "source_id": "zoo-db",
            "domain_id": "zoo",
            "schema_name": "main",
            "table_name": "animals",
            "columns": [{"column_name": "name", "visible_to": ["sandbox"], "data_type": "varchar"}],
        }
    ]
    ctx = CompilationContext()
    ctx.tables = {
        "animals": TableMeta(
            table_id=7,
            field_name="animals",
            type_name="Animals",
            source_id="zoo-db",
            catalog_name="zoo_db",
            schema_name="main",
            table_name="animals",
            domain_id="zoo",
        )
    }
    cap = DuckDBSqliteConnector().capability()
    assert not cap.predicate_pushdown  # the case: the source cannot evaluate the mask
    masks = {
        (7, "sandbox"): {
            "name": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar")
        }
    }
    gov = build_governance_context(
        "sandbox",
        RLSContext.empty(),
        masks,
        ctx,
        tables,
        role,
        source_types={"zoo-db": "sqlite"},
        engine=_EngineOver(cap),
    )
    governed = apply_governance('SELECT "a"."name" FROM "main"."animals" "a"', gov)
    assert "'HIDDEN'" in governed
    assert '"a"."name" AS "name"' not in governed
