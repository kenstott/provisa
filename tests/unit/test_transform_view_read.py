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
