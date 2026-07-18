# Copyright (c) 2026 Kenneth Stott
# Canary: c7e7884c-14c7-435e-b986-0dabd9f3eb0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""REQ-1134: the `view_governance` capability gates the GOVERNANCE column class of the
meta (catalog) domain. A role with a plain meta grant sees CORE (structural) columns for
discovery but NOT the GOVERNANCE columns (visible_to, masking rules, view_sql, …) — those
require view_governance (or admin) independently.

Enforcement lives in build_governance_context (SQL endpoint / cypher column projection),
sharing the GOVERNANCE_META_COLUMNS + META_DOMAIN_ID source of truth in provisa.security.rights.
"""

from __future__ import annotations

from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.security.rights import GOVERNANCE_META_COLUMNS, META_DOMAIN_ID


class _FakeRLSContext:
    def __init__(self, rules: dict[int, str]) -> None:
        self.rules = rules


_CORE_COL = "table_name"
_GOV_COLS = sorted(GOVERNANCE_META_COLUMNS)


def _meta_ctx(table_id: int = 1) -> CompilationContext:
    """A CompilationContext whose one table lives in the meta domain."""
    ctx = CompilationContext()
    ctx.tables["registered_tables"] = TableMeta(
        table_id=table_id,
        field_name="registered_tables",
        type_name="RegisteredTables",
        source_id="s",
        catalog_name="main",
        schema_name="public",
        table_name="registered_tables_meta",
        domain_id=META_DOMAIN_ID,
    )
    return ctx


def _meta_tables(table_id: int = 1) -> list[dict]:
    cols = [{"column_name": _CORE_COL, "data_type": "varchar"}]
    cols += [{"column_name": c, "data_type": "varchar"} for c in _GOV_COLS]
    return [{"id": table_id, "columns": cols}]


def _build(role: dict):
    return build_governance_context(
        role["id"], _FakeRLSContext({}), {}, _meta_ctx(), _meta_tables(), role
    )


class TestREQ1134GovernanceColumnVisibility:
    """REQ-1134"""

    def test_role_without_view_governance_sees_core_only(self):
        # REQ-1134 — meta grant, no view_governance: CORE visible, GOVERNANCE hidden.
        gov = _build({"id": "analyst", "capabilities": []})
        visible = gov.visible_columns[1]
        assert visible is not None
        assert _CORE_COL in visible
        assert not (set(visible) & GOVERNANCE_META_COLUMNS)

    def test_role_with_view_governance_sees_governance_columns(self):
        # REQ-1134 — view_governance granted independently: GOVERNANCE columns now visible.
        gov = _build({"id": "steward", "capabilities": ["view_governance"]})
        visible = gov.visible_columns[1]
        assert visible is not None
        assert _CORE_COL in visible
        assert GOVERNANCE_META_COLUMNS <= set(visible)

    def test_admin_sees_all_meta_columns(self):
        # REQ-1134 — admin bypass: no per-column meta filtering (None == all visible).
        gov = _build({"id": "admin", "capabilities": ["admin"]})
        assert gov.visible_columns[1] is None

    def test_meta_grant_alone_does_not_auto_grant_governance(self):
        # REQ-1134 — a meta DOMAIN grant must NOT imply view_governance.
        gov = _build(
            {"id": "meta_reader", "capabilities": [], "domain_access": [META_DOMAIN_ID]}
        )
        visible = gov.visible_columns[1]
        assert visible is not None
        assert "visible_to" not in visible
        assert "view_sql" not in visible


class TestREQ1132ColumnTiering:
    """REQ-1132 — the CORE/GOVERNANCE column-class split (the column half of the tiered
    meta visibility rule; the row-level neighbourhood scoping half is tracked separately)."""

    def test_default_tier_sees_core_meta_columns(self):
        # REQ-1132 — CORE (structural) columns are always discoverable by the default tier.
        gov = _build({"id": "analyst", "capabilities": []})
        visible = gov.visible_columns[1]
        assert visible is not None
        assert _CORE_COL in visible

    def test_core_and_governance_classes_are_disjoint(self):
        # REQ-1132 — the two column classes never overlap: CORE excludes every GOVERNANCE column.
        assert _CORE_COL not in GOVERNANCE_META_COLUMNS


# --------------------------------------------------------------------------- #
# REQ-1132 — row-level neighbourhood scoping of the meta views.               #
# --------------------------------------------------------------------------- #

# Described data tables: two in 'sales', one in 'hr'; two finance neighbours reached via edges.
_SALES_A, _SALES_B, _HR, _FIN_OPEN, _FIN_HIDDEN = 10, 11, 20, 30, 31
# Meta view table ids (the views whose rows describe the data tables above).
_RT_META_TID, _TC_META_TID = 100, 101


def _row_scope_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["registered_tables"] = TableMeta(
        table_id=_RT_META_TID,
        field_name="registered_tables",
        type_name="RegisteredTables",
        source_id="s",
        catalog_name="main",
        schema_name="public",
        table_name="registered_tables_meta",
        domain_id=META_DOMAIN_ID,
    )
    ctx.tables["table_columns"] = TableMeta(
        table_id=_TC_META_TID,
        field_name="table_columns",
        type_name="TableColumns",
        source_id="s",
        catalog_name="main",
        schema_name="public",
        table_name="table_columns_meta",
        domain_id=META_DOMAIN_ID,
    )
    return ctx


def _data_tables() -> list[dict]:
    return [
        {"id": _SALES_A, "domain_id": "sales", "columns": []},
        {"id": _SALES_B, "domain_id": "sales", "columns": []},
        {"id": _HR, "domain_id": "hr", "columns": []},
        {"id": _FIN_OPEN, "domain_id": "finance", "columns": []},
        {"id": _FIN_HIDDEN, "domain_id": "finance", "columns": []},
    ]


def _relationships() -> list[dict]:
    return [
        # sales → finance (open): a default sales role discovers _FIN_OPEN via this edge.
        {"source_table_id": _SALES_A, "target_table_id": _FIN_OPEN, "hide_target_meta": False},
        # sales → finance (opt-out): _FIN_HIDDEN is suppressed from default discovery.
        {"source_table_id": _SALES_A, "target_table_id": _FIN_HIDDEN, "hide_target_meta": True},
        # computed relationship (no concrete target): contributes no neighbour.
        {"source_table_id": _SALES_B, "target_table_id": None, "hide_target_meta": False},
    ]


def _row_gov(role: dict):
    return build_governance_context(
        role["id"],
        _FakeRLSContext({}),
        {},
        _row_scope_ctx(),
        _data_tables(),
        role,
        relationships=_relationships(),
    )


class TestREQ1132RowScope:
    """REQ-1132 — a DEFAULT-tier role sees meta rows only for its reachable neighbourhood."""

    def test_default_tier_row_predicate_covers_direct_plus_open_neighbor(self):
        # REQ-1132 — sales role: direct {10,11} + 1-hop open neighbour {30}; hidden {31} excluded.
        gov = _row_gov({"id": "sales", "capabilities": [], "domain_access": ["sales"]})
        assert gov.rls_rules[_RT_META_TID] == f"id IN ({_SALES_A},{_SALES_B},{_FIN_OPEN})"
        assert gov.rls_rules[_TC_META_TID] == f"table_id IN ({_SALES_A},{_SALES_B},{_FIN_OPEN})"

    def test_opt_out_target_not_discoverable(self):
        # REQ-1132 — the hide_target_meta edge's target must never appear in the row predicate.
        gov = _row_gov({"id": "sales", "capabilities": [], "domain_access": ["sales"]})
        assert str(_FIN_HIDDEN) not in gov.rls_rules[_RT_META_TID]

    def test_meta_domain_grant_sees_all_rows(self):
        # REQ-1132 — a meta domain grant lifts the row filter entirely (no predicate injected).
        gov = _row_gov(
            {"id": "cataloger", "capabilities": [], "domain_access": ["sales", META_DOMAIN_ID]}
        )
        assert _RT_META_TID not in gov.rls_rules
        assert _TC_META_TID not in gov.rls_rules

    def test_admin_sees_all_rows(self):
        # REQ-1132 — admin bypass: no meta row filter.
        gov = _row_gov({"id": "admin", "capabilities": ["admin"], "domain_access": ["sales"]})
        assert _RT_META_TID not in gov.rls_rules
        assert _TC_META_TID not in gov.rls_rules

    def test_role_with_no_reachable_tables_sees_no_rows(self):
        # REQ-1132 — a role whose domain has no tables gets a match-nothing predicate (fail-closed).
        gov = _row_gov({"id": "empty", "capabilities": [], "domain_access": ["marketing"]})
        assert gov.rls_rules[_RT_META_TID] == "id IN (-1)"
