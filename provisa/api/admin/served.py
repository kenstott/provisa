# Copyright (c) 2026 Kenneth Stott
# Canary: 2c8f5a17-9e43-4d06-b1a2-7f0d3e6c9b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an admin-API caller reads (REQ-1161).

A caller reads the columns the role its request acts as is served: that role's compiled context,
built by the one column rule (``rights.column_served`` within the domains the role reaches), the
same narrowing every schema surface and the Flight and MCP catalogs answer with. A capability
never widens it. A request acting as no role reads nothing.
"""

# Requirements: REQ-1161

from __future__ import annotations

from typing import Any


def served_columns(request: Any, state: Any) -> dict[int, set[str]]:
    """table id → the physical columns the caller is served."""
    return served_columns_for(state, getattr(request.state, "role", None))


def served_columns_for(state: Any, role_id: str | None) -> dict[int, set[str]]:
    """table id → the physical columns ``role_id`` is served; nothing for no role. For a path
    with no request, such as a scheduled publish that names its role."""
    from provisa.api.flight.catalog import role_visibility

    return role_visibility(state, role_id) if role_id else {}


def served_in(request: Any, state: Any, table_id: int) -> set[str]:
    """The physical columns of ``table_id`` the caller is served."""
    return served_columns(request, state).get(table_id, set())
