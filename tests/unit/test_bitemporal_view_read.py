# Copyright (c) 2026 Kenneth Stott
# Canary: 8e09e07f-d4da-4f9d-9844-e16f27d08f96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A bitemporal view (REQ-1162, REQ-1163) in the one semantic statement: its rows are what its
own append log reconstructs -- current by default, or as of a request's time -- never a reading
of its inputs. So it keeps its reference through governance for every read (its own rules apply
to it as the registered table it is), the V-rules judge its definition for the reader, and at
optimization it is answered from its log: its reconstruction, not the log's raw rows.

The current read is what was refused: its log reconstruction was put in the statement before
governance, and the log table, which is no relation a reader names, failed V006."""

# Requirements: REQ-1162, REQ-1163, REQ-135
from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot
from sqlglot import exp

from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.bitemporal import BitemporalSpec, as_of_view_map, parse_as_of, view_read_sql
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.view_read import semantic_statement, unnarrowed_view_bodies

pytestmark = pytest.mark.unit

LOG = '"mat_store"."org_default_mv_cache"."mv_bt_view"'
DEFINITION = "SELECT id, business_date, amount FROM sales.orders"
# REQ-2109: every bitemporal view's reads travel on its valid time.
SPEC = BitemporalSpec(key=("id",), mode="delta", valid_column="business_date")
CURRENT = view_read_sql(LOG, SPEC)
STATEMENT = "SELECT id, amount FROM bt_view"


def _state() -> SimpleNamespace:
    registry = MVRegistry()
    registry.register(
        MVDefinition(
            id="view-bt_view",
            source_tables=[],
            target_catalog="mat_store",
            target_schema="org_default_mv_cache",
            target_table="mv_bt_view",
            sql=DEFINITION,
            semantic_sql=DEFINITION,
            bitemporal=SPEC,
            status=MVStatus.FRESH,
        )
    )
    return SimpleNamespace(
        mv_registry=registry,
        bitemporal_view_reads={"bt_view": (LOG, SPEC)},
        role_build_inputs={"draft_tables": []},  # no table is out of service
    )


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"sales.orders": 1, "orders": 1, "bt_view": 2}
    return gov


def _relations(sql: str) -> set[str]:
    return {
        ".".join(p for p in (t.catalog, t.db, t.name) if p)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    }


def test_a_current_read_keeps_the_views_reference_through_governance():
    state = _state()
    governed, validated = semantic_statement(STATEMENT, {"bt_view": CURRENT}, state, _gov())
    # Governance judges the view as the registered table it is, its own rules on top.
    assert governed == STATEMENT
    # The V-rules judge what the view reads -- its definition -- and never its log.
    assert _relations(validated) == {"sales.orders"}


def test_a_current_read_is_answered_from_the_logs_current_state_not_its_raw_rows():
    state = _state()
    bodies = unnarrowed_view_bodies(STATEMENT, {"bt_view": CURRENT}, state)
    assert bodies == {"bt_view": CURRENT}
    assert bodies["bt_view"] != f"SELECT * FROM {LOG}"  # the log's every version


def test_an_as_of_read_is_answered_as_of_that_time_and_governed_the_same_way():
    state = _state()
    at = parse_as_of("2026-10-01T00:00:00Z").key()  # as the pipeline carries it
    governed, validated = semantic_statement(STATEMENT, {"bt_view": CURRENT}, state, _gov())
    assert governed == STATEMENT and _relations(validated) == {"sales.orders"}
    bodies = unnarrowed_view_bodies(STATEMENT, {"bt_view": CURRENT}, state, as_of=at)
    assert bodies == {
        "bt_view": as_of_view_map({"bt_view": CURRENT}, state.bitemporal_view_reads, at)["bt_view"]
    }


def test_a_bitemporal_view_without_its_definition_registered_is_refused_not_guessed():
    state = _state()
    state.mv_registry.unregister("view-bt_view")
    with pytest.raises(RuntimeError, match="bitemporal view 'bt_view' has no definition"):
        semantic_statement(STATEMENT, {"bt_view": CURRENT}, state, _gov())
