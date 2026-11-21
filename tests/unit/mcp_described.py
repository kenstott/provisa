# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0e6c55-1d2b-4f0a-9a57-8c4b7f2e61d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role context and governance for a fake catalog, as describe_table reads them.

describe_table names each listed column's state from the role's governance context, for the table
its compiled context holds under the catalog's names. A test that feeds a fixed catalog gives the
role a context holding those tables (:func:`metas_for`) and a governance context that serves each
of them whole with no mask (:func:`serve_whole`): every listed column reads plain.
"""

from __future__ import annotations

from typing import Any

from provisa.api.flight.catalog import CatalogTable
from provisa.compiler.sql_types import TableMeta


def metas_for(catalog: list[CatalogTable], first_id: int = 1) -> dict[str, TableMeta]:
    """A TableMeta per catalog table, keyed by its name, numbered from ``first_id``."""
    return {
        f"{t.domain_id}.{t.table_name}": TableMeta(
            table_id=first_id + i,
            field_name=t.table_name,
            type_name=t.table_name.capitalize(),
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name=t.table_name,
            domain_id=t.domain_id,
        )
        for i, t in enumerate(catalog)
    }


def serve_whole(monkeypatch: Any) -> None:
    """Every role is served each table its context holds whole, and holds no mask."""
    from provisa.api.mcp import tools
    from provisa.compiler.stage2 import GovernanceContext

    def _governance(state: Any, role: str) -> GovernanceContext:
        gov = GovernanceContext()
        gov.visible_columns = {m.table_id: None for m in state.contexts[role].tables.values()}
        return gov

    monkeypatch.setattr(tools, "_role_governance", _governance)
