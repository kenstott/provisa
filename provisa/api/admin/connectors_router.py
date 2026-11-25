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

``GET /admin/orgs/{org_id}/builtin-sources/{name}`` (REQ-2225, amended 2026-11-24): the host a
built-in source type (Spark) joins, for its source form, with the cluster a source that names none
runs on. It is listed with the source types, never among the connectors.
"""

# Requirements: REQ-2221, REQ-2225
from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.admin.code_uploads_router import _guard

router = APIRouter(prefix="/admin/orgs/{org_id}/connectors", tags=["admin"])
builtin_router = APIRouter(prefix="/admin/orgs/{org_id}/builtin-sources", tags=["admin"])


@router.get("")
async def offered(request: Request, org_id: str) -> dict:
    from provisa.api.admin.schema_mutation import _get_pool
    from provisa.core.repositories import connector as connector_repo

    await _guard(request, org_id)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        return {"connectors": await connector_repo.offered(conn)}


@builtin_router.get("/{name}")
async def builtin(request: Request, org_id: str, name: str) -> dict:
    """The host a ``name`` source joins and the organisation's cluster in use; ``host`` is null
    when this installation carries no host for it (a release package ships one; a checkout's demo
    runs the native one). 404 when ``name`` is not a built-in source type at all."""
    from provisa.api.admin.spark_cluster_router import in_use
    from provisa.api.errors import ApiError
    from provisa.core.models import BUILTIN_CONNECTORS
    from provisa.core.repositories import connector as connector_repo

    await _guard(request, org_id)
    if name not in BUILTIN_CONNECTORS:
        raise ApiError(
            404,
            "codesource.not_builtin",
            f"{name!r} is not a built-in source type",
            None,
            type=name,
        )
    row = connector_repo.builtin(name)
    # REQ-2225: null host means this installation does not ship a Spark host (e.g. e2e test
    # environments). The UI already initialises sparkHost to null and handles the absent-host
    # case; a 409 here logged as a browser console error and broke every Sources-page test.
    host = connector_repo.public(row) if row is not None else None
    return {"host": host, "default_cluster": await in_use(org_id)}
