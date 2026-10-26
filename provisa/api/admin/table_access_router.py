# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who can read or write each column of a table (REQ-2162): the inverse of a role's data access
(REQ-2161), resolved by the same rule over what the runtime serves."""

# Requirements: REQ-2162, REQ-2161, REQ-2152

from __future__ import annotations

import types

from fastapi import APIRouter, Request
from sqlalchemy import or_, select

from provisa.api.admin.capabilities import role_definitions_visible
from provisa.api.errors import ApiError
from provisa.core.schema_org import roles, table_columns

router = APIRouter(prefix="/admin/tables", tags=["admin", "tables"])


@router.get("/{table_id}/access")
async def table_access_endpoint(table_id: int, request: Request):  # REQ-2162
    """Per column of the table the caller may read, every role whose definition it may read that
    reads, writes, sees masked or has a row rule on it, with the grant and inheritance behind each.
    A table the caller reads no column of is not found, so it is not marked as existing."""
    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.api.admin.roles_router import _active_org, _pool
    from provisa.api.app import state
    from provisa.core.db import STANDARD_ROLES
    from provisa.security.effective_rights import table_access
    from provisa.security.inheritance import parent_map
    from provisa.security.meta_role import is_meta_role_id

    org_id = _active_org(request)
    scope = catalog_scope(types.SimpleNamespace(context={"request": request}))
    identity = getattr(request.state, "identity", None)
    full = role_definitions_visible(request, getattr(identity, "roles", []))
    async with _pool(request).acquire() as conn:
        rows = (
            await conn.execute_core(
                select(roles.c.id, roles.c.parents).where(
                    or_(roles.c.org_id.is_(None), roles.c.org_id == org_id)
                )
            )
        ).fetchall()
        stored = {
            (r.table_id, r.column_name): list(r.visible_to or [])
            for r in (
                await conn.execute_core(
                    select(
                        table_columns.c.table_id,
                        table_columns.c.column_name,
                        table_columns.c.visible_to,
                    ).where(table_columns.c.table_id == table_id)
                )
            ).fetchall()
        }
    access = table_access(
        state,
        table_id,
        roles=[rid for rid in state.roles if not is_meta_role_id(rid) and full(rid)],
        parents=parent_map(dict(r._mapping) for r in rows),
        stored=stored,
        readable=lambda tid, column: scope.whole or column in scope.served.get(tid, set()),
        governance=scope.governance,
        standard=STANDARD_ROLES,
    )
    if access is None or not access["columns"]:
        raise ApiError(404, "schema.table_not_found", f"Table {table_id} not found", table=table_id)
    return access
