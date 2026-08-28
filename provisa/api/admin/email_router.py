# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import insert, select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.core.schema_admin import user_directory, email_send_authority_audit

if TYPE_CHECKING:
    from provisa.core.database import Database

router = APIRouter(prefix="/auth/email", tags=["auth"])


class EmailPreference(BaseModel):
    email_opt_in: bool


class EmailAuditEntry(BaseModel):
    email_address: str
    user_id: str | None
    granted_by: str | None
    org_id: str | None
    action: str
    reason: str | None
    created_at: datetime


def _pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    from provisa.api.app import state

    return state.pool  # pyright: ignore[reportAttributeAccessIssue]


@router.get("/preferences", response_model=EmailPreference)
async def get_email_preferences(request: Request) -> EmailPreference:
    identity = getattr(request.state, "identity", None)
    if not identity:
        raise ApiError(401, "auth.required", "Authentication required")

    pool = _pool(request)
    async with pool.acquire() as conn:
        stmt = select(user_directory.c.email_opt_in).where(
            user_directory.c.user_id == identity.user_id
        )
        row = await conn.execute_core(stmt)
        result = row.fetchone()
        if not result:
            return EmailPreference(email_opt_in=True)
        return EmailPreference(email_opt_in=result[0])


@router.patch("/preferences", response_model=EmailPreference)
async def update_email_preferences(pref: EmailPreference, request: Request) -> EmailPreference:
    identity = getattr(request.state, "identity", None)
    if not identity:
        raise ApiError(401, "auth.required", "Authentication required")

    pool = _pool(request)
    async with pool.acquire() as conn:
        stmt = (
            insert(user_directory)
            .values(user_id=identity.user_id, email_opt_in=pref.email_opt_in)
            .on_conflict_do_update(  # pyright: ignore[reportAttributeAccessIssue]
                index_elements=[user_directory.c.user_id],
                set_={"email_opt_in": pref.email_opt_in},
            )
        )
        await conn.execute_core(stmt)
    return pref


@router.get("/send-authority-audit", response_model=list[EmailAuditEntry])
async def get_email_audit(request: Request) -> list[EmailAuditEntry]:
    require_capability_request(request, "org_settings")
    org_id = getattr(request.state, "active_org_id", None)
    if not org_id:
        raise ApiError(401, "org.required", "Org selection required")

    pool = _pool(request)
    async with pool.acquire() as conn:
        stmt = (
            select(
                email_send_authority_audit.c.email_address,
                email_send_authority_audit.c.user_id,
                email_send_authority_audit.c.granted_by,
                email_send_authority_audit.c.org_id,
                email_send_authority_audit.c.action,
                email_send_authority_audit.c.reason,
                email_send_authority_audit.c.created_at,
            )
            .where(
                (email_send_authority_audit.c.org_id == org_id)
                | (email_send_authority_audit.c.org_id.is_(None))
            )
            .order_by(email_send_authority_audit.c.created_at.desc())
            .limit(1000)
        )
        rows = await conn.execute_core(stmt)
        return [
            EmailAuditEntry(
                email_address=row[0],
                user_id=row[1],
                granted_by=row[2],
                org_id=row[3],
                action=row[4],
                reason=row[5],
                created_at=row[6],
            )
            for row in rows.fetchall()
        ]
