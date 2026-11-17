# Copyright (c) 2026 Kenneth Stott
# Canary: c61279d4-b0e3-41f6-830d-78325dd90635
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view applying a column transform (REQ-2131) in the one semantic statement: its outputs exist
only in its stored table, landed by its build. So, as a bitemporal view, it keeps its reference
through governance for every read (its own rules apply to it as the registered table it is), the
V-rules judge its definition for the reader, and at optimization it is answered from its stored
table -- whether or not its build is fresh. Its stored table put in before governance was refused
V006, a relation no reader names."""

# Requirements: REQ-2131
from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot
from sqlglot import exp

from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.view_read import semantic_statement, unnarrowed_view_bodies

pytestmark = pytest.mark.unit

STORED = '"mat_store"."org_default_mv_cache"."mv_clean_suppliers"'
READ = f"SELECT * FROM {STORED}"
DEFINITION = "SELECT id, clean_address(raw_address) FROM fx.suppliers"
STATEMENT = "SELECT id, city FROM clean_suppliers WHERE city = 'Springfield 3'"


def _state(status: MVStatus) -> SimpleNamespace:
    registry = MVRegistry()
    registry.register(
        MVDefinition(
            id="view-clean_suppliers",
            source_tables=[],
            target_catalog="mat_store",
            target_schema="org_default_mv_cache",
            target_table="mv_clean_suppliers",
            sql=DEFINITION,
            semantic_sql=DEFINITION,
            status=status,
        )
    )
    return SimpleNamespace(
        mv_registry=registry,
        stored_view_reads={"clean_suppliers"},
        role_build_inputs={"draft_tables": []},  # no table is out of service
    )


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"fx.suppliers": 1, "suppliers": 1, "clean_suppliers": 2}
    return gov


def _relations(sql: str) -> set[str]:
    return {
        ".".join(p for p in (t.catalog, t.db, t.name) if p)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    }


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_transform_view_keeps_its_reference_through_governance(status):
    governed, validated = semantic_statement(
        STATEMENT, {"clean_suppliers": READ}, _state(status), _gov()
    )
    # Governance judges the view as the registered table it is, its own rules on top.
    assert governed == STATEMENT
    # The V-rules judge what the view reads -- its definition -- never its stored table.
    assert _relations(validated) == {"fx.suppliers"}


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_transform_view_is_answered_from_its_stored_table(status):
    bodies = unnarrowed_view_bodies(STATEMENT, {"clean_suppliers": READ}, _state(status))
    assert bodies == {"clean_suppliers": READ}


# --- REQ-2131 (amended 2026-11-16): the stored build is read under the reader's role -------------


def _answered(statement: str, state: SimpleNamespace, gov: GovernanceContext) -> str:
    """The statement as it runs for the reader: governed, then each view it still references put
    in at optimization."""
    from provisa.compiler.view_expand import expand_view_refs

    governed, _ = semantic_statement(statement, {"clean_suppliers": READ}, state, gov)
    bodies = unnarrowed_view_bodies(governed, {"clean_suppliers": READ}, state)
    return expand_view_refs(governed, bodies)


def _narrowed_gov(*, row_rule: bool, mask: bool) -> GovernanceContext:
    from provisa.compiler.rls import read_rule

    gov = _gov()
    gov.all_columns = {1: [("id", "integer"), ("raw_address", "varchar"), ("region", "varchar")]}
    if row_rule:
        gov.rls_rules = {1: read_rule("region = 'east'")}
    if mask:
        gov.masking_rules = {(1, "raw_address"): ("***",)}
    return gov


@pytest.mark.parametrize(
    ("row_rule", "mask"), [(True, False), (False, True), (True, True)], ids=["rows", "mask", "both"]
)
@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_reader_with_a_rule_on_the_views_inputs_is_not_answered_from_the_stored_build(
    status, row_rule, mask
):
    """The stored table holds what the administrator's build read: every row, in clear. A reader
    whose row rule or mask narrows the view's inputs is never answered from it as it is."""
    answered = _answered(STATEMENT, _state(status), _narrowed_gov(row_rule=row_rule, mask=mask))
    assert STORED not in answered


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_reader_who_sees_every_input_whole_is_answered_from_the_stored_build(status):
    answered = _answered(STATEMENT, _state(status), _narrowed_gov(row_rule=False, mask=False))
    assert _relations(answered) == {"mat_store.org_default_mv_cache.mv_clean_suppliers"}
