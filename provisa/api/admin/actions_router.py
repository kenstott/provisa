# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin REST endpoints for tracked DB functions and webhooks (REQ-205-211)."""

# Requirements: REQ-004, REQ-062, REQ-205, REQ-206, REQ-207, REQ-208, REQ-209, REQ-210, REQ-211, REQ-245, REQ-253, REQ-304, REQ-305, REQ-306, REQ-434

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import delete as _delete, func, select, update

import httpx

from provisa.core.schema_org import tracked_functions, tracked_webhooks

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/actions", tags=["admin", "actions"])


async def _ensure_tables(pool: "Database") -> None:
    """Create the tracked-function/webhook tables via portable SQLAlchemy metadata."""
    from provisa.core.schema_org import metadata

    async with pool.engine.begin() as conn:
        await conn.run_sync(
            lambda sc: metadata.create_all(sc, tables=[tracked_functions, tracked_webhooks])
        )


def _row_to_function(row: dict) -> dict:
    return {
        "name": row["name"],
        "sourceId": row["source_id"],
        "schemaName": row["schema_name"],
        "functionName": row["function_name"],
        "returns": row["returns"],
        "arguments": row["arguments"] or [],
        "visibleTo": list(row["visible_to"] or []),
        "writableBy": list(row["writable_by"] or []),
        "domainId": row["domain_id"],
        "description": row.get("description"),
        "kind": row.get("kind", "mutation"),
        "returnSchema": row.get("return_schema"),
    }


def _row_to_webhook(row: dict) -> dict:
    return {
        "name": row["name"],
        "url": row["url"],
        "method": row["method"],
        "timeoutMs": row["timeout_ms"],
        "returns": row.get("returns"),
        "inlineReturnType": row["inline_return_type"] or [],
        "arguments": row["arguments"] or [],
        "visibleTo": list(row["visible_to"] or []),
        "domainId": row["domain_id"],
        "description": row.get("description"),
        "kind": row.get("kind", "mutation"),
    }


@router.get("")
async def list_actions():  # REQ-205, REQ-209
    """Return all tracked functions and webhooks."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    async with state.tenant_db.acquire() as conn:
        fn_result = await conn.execute_core(
            select(tracked_functions).order_by(tracked_functions.c.name)
        )
        fn_rows = fn_result.fetchall()
        wh_result = await conn.execute_core(
            select(tracked_webhooks).order_by(tracked_webhooks.c.name)
        )
        wh_rows = wh_result.fetchall()

    return {
        "functions": [_row_to_function(dict(r._mapping)) for r in fn_rows],
        "webhooks": [_row_to_webhook(dict(r._mapping)) for r in wh_rows],
    }


class FunctionInput(BaseModel):  # REQ-205, REQ-206, REQ-304, REQ-305, REQ-306
    name: str
    sourceId: str = ""
    schemaName: str = "public"
    functionName: str = ""
    returns: str = ""
    arguments: list[dict] = []
    visibleTo: list[str] = []
    writableBy: list[str] = []
    domainId: str = ""
    description: str | None = None
    kind: str = "mutation"
    returnSchema: dict | None = None


class WebhookInput(BaseModel):  # REQ-209, REQ-210, REQ-211
    name: str
    url: str = ""
    method: str = "POST"
    timeoutMs: int = 5000
    returns: str | None = None
    inlineReturnType: list[dict] = []
    arguments: list[dict] = []
    visibleTo: list[str] = []
    domainId: str = ""
    description: str | None = None
    kind: str = "mutation"


@router.post("/functions")
async def create_function(
    body: FunctionInput,
):  # REQ-205, REQ-206, REQ-207, REQ-208, REQ-253, REQ-304
    """Create a tracked DB function."""
    from provisa.api.app import state
    from provisa.core.models import Function, FunctionArgument
    from provisa.core.repositories import function as function_repo

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    func = Function(
        name=body.name,
        source_id=body.sourceId,
        schema_name=body.schemaName,
        function_name=body.functionName,
        returns=body.returns,
        arguments=[FunctionArgument(**a) for a in body.arguments],
        visible_to=body.visibleTo,
        writable_by=body.writableBy,
        domain_id=body.domainId,
        description=body.description,
        kind=body.kind,
    )
    # return_schema is a JSON column — pass the Python object directly (no double-encoding).
    async with state.tenant_db.acquire() as _conn:
        await function_repo.upsert_function(_conn, func, return_schema=body.returnSchema)

    log.info("Saved tracked function %s", body.name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": body.name}


@router.put("/functions/{name}")
async def update_function(name: str, body: FunctionInput):  # REQ-205, REQ-253, REQ-304
    """Update a tracked DB function by name."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    async with state.tenant_db.acquire() as conn:
        result = await conn.execute_core(
            update(tracked_functions)
            .where(tracked_functions.c.name == name)
            .values(
                source_id=body.sourceId,
                schema_name=body.schemaName,
                function_name=body.functionName,
                returns=body.returns,
                arguments=body.arguments,
                visible_to=body.visibleTo,
                writable_by=body.writableBy,
                domain_id=body.domainId,
                description=body.description,
                kind=body.kind,
                return_schema=body.returnSchema,
                updated_at=func.now(),
            )
        )

    if (result.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail=f"Function '{name}' not found")

    log.info("Updated tracked function %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


@router.delete("/functions/{name}")
async def delete_function(name: str):  # REQ-205, REQ-253
    """Delete a tracked DB function by name."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    async with state.tenant_db.acquire() as conn:
        result = await conn.execute_core(
            _delete(tracked_functions).where(tracked_functions.c.name == name)
        )

    if (result.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail=f"Function '{name}' not found")

    log.info("Deleted tracked function %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


@router.post("/webhooks")
async def create_webhook(body: WebhookInput):  # REQ-209, REQ-210, REQ-211, REQ-253, REQ-434
    """Create a tracked webhook."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    from provisa.core.repositories import creation_request as cr_repo

    async with state.tenant_db.acquire() as conn:
        await conn.upsert(
            tracked_webhooks,
            {
                "name": body.name,
                "url": body.url,
                "method": body.method,
                "timeout_ms": body.timeoutMs,
                "returns": body.returns,
                "inline_return_type": body.inlineReturnType,
                "arguments": body.arguments,
                "visible_to": body.visibleTo,
                "domain_id": body.domainId,
                "description": body.description,
                "kind": body.kind,
                "updated_at": func.now(),
            },
            index_elements=["name"],
            update_columns=[
                "url",
                "method",
                "timeout_ms",
                "returns",
                "inline_return_type",
                "arguments",
                "visible_to",
                "domain_id",
                "description",
                "kind",
                "updated_at",
            ],
        )
        # REQ-209: a webhook is exposed only after a steward approves it. Approval is tracked
        # via the creation_requests queue — a webhook is approved when its most recent
        # "webhook" request is executed. Registering or editing enqueues a fresh pending
        # request, so any edit resets approval until re-approved.
        request_id = await cr_repo.create(
            conn,
            "webhook",
            "webhook_registration",
            {"name": body.name},
            None,
        )

    log.info("Saved tracked webhook %s (pending approval, request #%s)", body.name, request_id)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {
        "success": True,
        "name": body.name,
        "approved": False,
        "creationRequestId": request_id,
        "message": (
            f"Webhook {body.name!r} registered — awaiting a steward holding "
            "'webhook_registration' to approve it before it is exposed."
        ),
    }


@router.put("/webhooks/{name}")
async def update_webhook(name: str, body: WebhookInput):  # REQ-209, REQ-253
    """Update a tracked webhook by name."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    async with state.tenant_db.acquire() as conn:
        result = await conn.execute_core(
            update(tracked_webhooks)
            .where(tracked_webhooks.c.name == name)
            .values(
                url=body.url,
                method=body.method,
                timeout_ms=body.timeoutMs,
                returns=body.returns,
                inline_return_type=body.inlineReturnType,
                arguments=body.arguments,
                visible_to=body.visibleTo,
                domain_id=body.domainId,
                description=body.description,
                kind=body.kind,
                updated_at=func.now(),
            )
        )

    if (result.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail=f"Webhook '{name}' not found")

    log.info("Updated tracked webhook %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


@router.delete("/webhooks/{name}")
async def delete_webhook(name: str):  # REQ-209, REQ-253
    """Delete a tracked webhook by name."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    async with state.tenant_db.acquire() as conn:
        result = await conn.execute_core(
            _delete(tracked_webhooks).where(tracked_webhooks.c.name == name)
        )

    if (result.rowcount or 0) == 0:
        raise HTTPException(status_code=404, detail=f"Webhook '{name}' not found")

    log.info("Deleted tracked webhook %s", name)
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {"success": True, "name": name}


class TestActionInput(BaseModel):  # REQ-004, REQ-062, REQ-245
    actionType: str  # "function" or "webhook"
    name: str
    role_id: str | None = None  # REQ-245: governance role selector


def _test_endpoints_enabled() -> bool:
    """REQ-004: developer test endpoints are opt-in and MUST NOT be exposed in production.

    Disabled unless ``PROVISA_ENABLE_TEST_ENDPOINTS`` is explicitly truthy, mirroring the
    opt-in pattern used for other non-production features (e.g. ``allow_simple_auth``).
    """
    import os

    return os.environ.get("PROVISA_ENABLE_TEST_ENDPOINTS", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _build_function_enforcement(
    table_id: int | None,
    role_id: str,
    state,
    rls_filter: str | None,
    masked_cols: list[str],
    excluded_cols: list[str],
) -> dict:
    """REQ-062: build enforcement metadata for the function test response."""
    masking_applied = []
    if table_id is not None:
        col_masking = state.masking_rules.get((table_id, role_id), {})
        for col, (rule, _) in col_masking.items():
            if col in masked_cols:
                masking_applied.append(f"{col} -> {rule.mask_type.value}")
    return {
        "role_used": role_id,
        "rls_filters_applied": [rls_filter] if rls_filter else [],
        "columns_excluded": excluded_cols,
        "masking_applied": masking_applied,
    }


def _apply_row_governance(  # REQ-062, REQ-207, REQ-245
    rows: list[dict],
    table_id: int | None,
    role_id: str,
    state,
    gov_ctx,
) -> tuple[list[dict], list[str], list[str]]:
    """Apply visibility and masking governance to Python result rows.

    Returns (governed_rows, masked_col_names, excluded_col_names).
    """
    from provisa.security.masking import apply_mask_to_value

    if table_id is None or not rows:
        return rows, [], []

    col_masking = state.masking_rules.get((table_id, role_id), {})
    visible = gov_ctx.visible_columns.get(table_id)  # None = all visible

    all_cols = set(rows[0].keys())
    excluded = [c for c in all_cols if visible is not None and c not in visible]
    masked_col_names: list[str] = []

    governed: list[dict] = []
    for row in rows:
        new_row: dict = {}
        for col, val in row.items():
            if col in excluded:
                continue
            if col in col_masking:
                rule, dtype = col_masking[col]
                new_row[col] = apply_mask_to_value(rule, val, dtype)
                if col not in masked_col_names:
                    masked_col_names.append(col)
            else:
                new_row[col] = val
        governed.append(new_row)

    return governed, masked_col_names, excluded


@router.post("/test")
async def test_action(body: TestActionInput):  # REQ-004, REQ-062, REQ-245
    """Run a no-arg test invocation of a tracked function or webhook."""
    if not _test_endpoints_enabled():
        raise HTTPException(
            status_code=404,
            detail="Test endpoint is disabled (set PROVISA_ENABLE_TEST_ENDPOINTS to enable in non-production).",
        )

    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.tenant_db)

    if body.actionType == "function":
        async with state.tenant_db.acquire() as conn:
            result = await conn.execute_core(
                select(tracked_functions).where(tracked_functions.c.name == body.name)
            )
            fetched = result.fetchone()
        if not fetched:
            raise HTTPException(status_code=404, detail=f"Function '{body.name}' not found")
        row = dict(fetched._mapping)

        src_id = row["source_id"]
        fn = row["function_name"]
        schema = row["schema_name"]
        returns = row["returns"] or ""

        if not state.source_pools.has(src_id):
            raise HTTPException(status_code=503, detail=f"Source '{src_id}' not connected")

        role_id = body.role_id
        gov_ctx = None
        table_id: int | None = None
        rls_filter: str | None = None

        if role_id:
            from provisa.compiler.stage2 import build_governance_context

            if role_id not in state.contexts:
                raise HTTPException(status_code=422, detail=f"Unknown role '{role_id}'")

            ctx = state.contexts[role_id]
            # Invariant: contexts and rls_contexts are written together per-role
            # (app.py). role_id is present in contexts (checked above), so it must
            # be in rls_contexts — a missing key is an invariant break, fail loud.
            rls = state.rls_contexts[role_id]
            role = state.roles.get(role_id)
            gov_ctx = build_governance_context(
                role_id,
                rls,
                state.masking_rules,
                ctx,
                getattr(state, "tables", []),
                role=role,
            )

            # Find return table_id by matching table_name to function's `returns` field
            for meta in ctx.tables.values():
                if meta.table_name == returns or meta.field_name == returns:
                    table_id = meta.table_id
                    break

            if table_id is not None:
                rls_filter = gov_ctx.rls_rules.get(table_id)

        # Build governed SQL — wrap in subquery to apply RLS WHERE
        base_sql = f'SELECT * FROM "{schema}"."{fn}"()'
        if rls_filter:
            exec_sql = f"SELECT * FROM ({base_sql}) AS _fn_result WHERE {rls_filter} LIMIT 5"
        else:
            exec_sql = f"{base_sql} LIMIT 5"

        result = await state.source_pools.execute(src_id, exec_sql)
        cols = result.column_names
        raw_rows = [dict(zip(cols, r)) for r in result.rows]

        if role_id and gov_ctx is not None:
            governed_rows, masked_cols, excluded_cols = _apply_row_governance(
                raw_rows, table_id, role_id, state, gov_ctx
            )
            enforcement = _build_function_enforcement(
                table_id, role_id, state, rls_filter, masked_cols, excluded_cols
            )
            return {"rows": governed_rows, "enforcement": enforcement}

        return {"rows": raw_rows}

    elif body.actionType == "webhook":
        async with state.tenant_db.acquire() as conn:
            result = await conn.execute_core(
                select(tracked_webhooks).where(tracked_webhooks.c.name == body.name)
            )
            fetched = result.fetchone()
        if not fetched:
            raise HTTPException(status_code=404, detail=f"Webhook '{body.name}' not found")
        row = dict(fetched._mapping)

        url = row["url"]
        method = row["method"].upper()
        timeout = row["timeout_ms"] / 1000
        role_id = body.role_id

        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, json={"_test": True})

        webhook_result: dict = {"status": resp.status_code, "body": resp.json()}
        if role_id:
            webhook_result["enforcement"] = {
                "role_used": role_id,
                "note": "Webhook responses are not subject to SQL-level RLS or column masking.",
            }
        return webhook_result

    raise HTTPException(status_code=400, detail=f"Unknown actionType '{body.actionType}'")
