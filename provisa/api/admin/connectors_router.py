# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7ac1e9-5d34-4b16-9c08-a1d3e6f0b752
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The connectors an organization offers, as Sources lists them (REQ-2221).

``GET /admin/orgs/{org_id}/connectors``: each connector of the model this environment holds, for
the source type picker and the setup form its ``requires`` builds. Never its build settings: the
server neither builds a connector nor reads how one is built.
"""

# Requirements: REQ-2221
from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.admin.code_uploads_router import _guard

router = APIRouter(prefix="/admin/orgs/{org_id}/connectors", tags=["admin"])


@router.get("")
async def offered(request: Request, org_id: str) -> dict:
    from provisa.api.admin.schema_mutation import _get_pool
    from provisa.core.repositories import connector as connector_repo

    await _guard(request, org_id)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        return {"connectors": await connector_repo.offered(conn)}
