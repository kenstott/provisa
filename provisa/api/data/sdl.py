# Copyright (c) 2026 Kenneth Stott
# Canary: cdba9e0f-f70d-4401-9655-786a35ca0b5e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/sdl endpoint — returns role-aware GraphQL SDL (REQ-076)."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import PlainTextResponse
from graphql import print_schema

router = APIRouter()


@router.get("/data/sdl", response_class=PlainTextResponse)
async def get_sdl(request: Request, x_role: str = Header(None, alias="X-Role")):
    """Return the GraphQL SDL for the requesting role's schema."""
    from provisa.api.app import state

    # Auth middleware role takes precedence over header
    auth_role = getattr(request.state, "role", None)
    role = auth_role or x_role
    if role is None:
        raise HTTPException(status_code=400, detail="Missing X-Role header")

    schema = state.schemas.get(role)
    if schema is None:
        raise HTTPException(status_code=404, detail=f"No schema for role {role!r}")
    return print_schema(schema)
