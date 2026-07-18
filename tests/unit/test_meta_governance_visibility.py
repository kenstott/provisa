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
