# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0eaf03-397a-4a19-b514-90cc660bd9e8
# Canary: PENDING
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the Flight path's governance-context building (REQ-039).

After the fail-closed change (da35e5c82c), a table absent from
gov_ctx.visible_columns causes V003 for every column reference.  The Flight
server builds its governance context via governance_reader, which reads
state.tables to populate visible_columns.  When the pg_backed_flight
integration-test fixture omitted state.tables, every table was absent → V003
on all columns.

These tests reproduce that failure without containers and verify that setting
state.tables with the governed table's raw dict resolves it.
"""

from __future__ import annotations

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import build_governance_context

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE_ID = 1
_ROLE_ID = "admin"

# SQL produced by the Flight path for `{ orders { id region amount } }`.
# The GraphQL compiler emits double-quoted, schema-qualified identifiers.
_SQL = (
    'SELECT "public"."orders"."id", "public"."orders"."region",'
    ' "public"."orders"."amount" FROM "public"."orders"'
)

# Minimal role dict – the same shape the pg_backed_flight fixture supplies.
_ROLE: dict = {
    "id": _ROLE_ID,
    "capabilities": ["full_results"],
    "domain_access": ["*"],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx() -> CompilationContext:
    """CompilationContext with one table: orders (id=1) in the public schema."""
    return CompilationContext(
        tables={
            "orders": TableMeta(
                table_id=_TABLE_ID,
                field_name="orders",
                type_name="Order",
                source_id="test-pg",
                catalog_name="postgresql",
                schema_name="public",
                table_name="orders",
                domain_id="default",
            )
        }
    )


def _gov_ctx(raw_tables: list[dict]):
    """GovernanceContext built from raw_tables (what state.tables holds)."""
    ctx = _make_ctx()
    rls = RLSContext.empty()
    return build_governance_context(
        role_id=_ROLE_ID,
        rls_context=rls,
        masking_rules={},
        ctx=ctx,
        tables=raw_tables,
        role=_ROLE,
    )


# ---------------------------------------------------------------------------
# The raw table dict the fixture must supply (REQ-039, REQ-263)
# ---------------------------------------------------------------------------
# In production, every visible column carries a visible_to grant set during
# the model build (security.inheritance.expand_grants folds ancestor grants
# down to leaf roles).  Without visible_to, column_served returns False for
# every column and visible_columns[table_id] = frozenset() (fully restricted).
# The pg_backed_flight fixture must include visible_to: [role_id] on each
# column so build_governance_context derives the "all visible" None sentinel.

_ORDERS_TABLE_DICT: dict = {
    "id": _TABLE_ID,
    "domain_id": "default",
    "columns": [
        {"column_name": "id", "data_type": "integer", "visible_to": [_ROLE_ID]},
        {"column_name": "region", "data_type": "varchar", "visible_to": [_ROLE_ID]},
        {"column_name": "amount", "data_type": "numeric", "visible_to": [_ROLE_ID]},
    ],
}


# ---------------------------------------------------------------------------
# RED: reproduced failure
# ---------------------------------------------------------------------------


def test_v003_fires_when_state_tables_empty():
    """V003 fires for every column when state.tables is empty.

    Before the fix the pg_backed_flight fixture left state.tables as a
    MagicMock that iterates to nothing, so visible_columns was empty and the
    fail-closed rule (REQ-039) refused every column.
    """
    gov = _gov_ctx(raw_tables=[])  # state.tables = [] → table absent
    ctx = _make_ctx()

    assert _TABLE_ID not in gov.visible_columns, (
        "table must be absent from visible_columns when state.tables is empty"
    )

    violations = validate_sql(_SQL, ctx, gov, _ROLE, raw_tables=[])
    codes = [v.code for v in violations]
    assert "V003" in codes, "V003 must be reported when the table is absent from visible_columns"


# ---------------------------------------------------------------------------
# GREEN: verified fix
# ---------------------------------------------------------------------------


def test_v003_absent_when_state_tables_has_the_table():
    """No V003 when state.tables includes the governed table's raw dict.

    The fix is to populate state.tables in the pg_backed_flight fixture so
    that build_governance_context can derive visible_columns for the table.
    With all columns unrestricted (no visible_to grant → all visible),
    visible_columns[table_id] = None (the "all visible" sentinel).
    """
    gov = _gov_ctx(raw_tables=[_ORDERS_TABLE_DICT])
    ctx = _make_ctx()

    # None = "all visible" sentinel; the key must be present.
    assert _TABLE_ID in gov.visible_columns, (
        "table must be present in visible_columns when state.tables has its dict"
    )
    assert gov.visible_columns[_TABLE_ID] is None, (
        "visible_columns[table_id] should be None (all visible) for an unrestricted table"
    )

    violations = validate_sql(_SQL, ctx, gov, _ROLE, raw_tables=[_ORDERS_TABLE_DICT])
    v003 = [v for v in violations if v.code == "V003"]
    assert v003 == [], f"unexpected V003 violations: {v003}"
