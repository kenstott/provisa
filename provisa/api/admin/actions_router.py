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

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/actions", tags=["admin", "actions"])

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS tracked_functions (
    name          TEXT PRIMARY KEY,
    source_id     TEXT NOT NULL DEFAULT '',
    schema_name   TEXT NOT NULL DEFAULT 'public',
    function_name TEXT NOT NULL DEFAULT '',
    returns       TEXT NOT NULL DEFAULT '',
    arguments     JSONB NOT NULL DEFAULT '[]',
    visible_to    TEXT[] NOT NULL DEFAULT '{}',
    writable_by   TEXT[] NOT NULL DEFAULT '{}',
    domain_id     TEXT NOT NULL DEFAULT '',
    description   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tracked_webhooks (
    name               TEXT PRIMARY KEY,
    url                TEXT NOT NULL DEFAULT '',
    method             TEXT NOT NULL DEFAULT 'POST',
    timeout_ms         INTEGER NOT NULL DEFAULT 5000,
    returns            TEXT,
    inline_return_type JSONB NOT NULL DEFAULT '[]',
    arguments          JSONB NOT NULL DEFAULT '[]',
    visible_to         TEXT[] NOT NULL DEFAULT '{}',
    domain_id          TEXT NOT NULL DEFAULT '',
    description        TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


async def _ensure_tables(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_INIT_SQL)


def _row_to_function(row: dict) -> dict:
    return {
        "name": row["name"],
        "sourceId": row["source_id"],
        "schemaName": row["schema_name"],
        "functionName": row["function_name"],
        "returns": row["returns"],
        "arguments": json.loads(row["arguments"]) if isinstance(row["arguments"], str) else (row["arguments"] or []),
        "visibleTo": list(row["visible_to"] or []),
        "writableBy": list(row["writable_by"] or []),
        "domainId": row["domain_id"],
        "description": row.get("description"),
    }


def _row_to_webhook(row: dict) -> dict:
    return {
        "name": row["name"],
        "url": row["url"],
        "method": row["method"],
        "timeoutMs": row["timeout_ms"],
        "returns": row.get("returns"),
        "inlineReturnType": json.loads(row["inline_return_type"]) if isinstance(row["inline_return_type"], str) else (row["inline_return_type"] or []),
        "arguments": json.loads(row["arguments"]) if isinstance(row["arguments"], str) else (row["arguments"] or []),
        "visibleTo": list(row["visible_to"] or []),
        "domainId": row["domain_id"],
        "description": row.get("description"),
    }


@router.get("")
async def list_actions():
    """Return all tracked functions and webhooks."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        fn_rows = await conn.fetch("SELECT * FROM tracked_functions ORDER BY name")
        wh_rows = await conn.fetch("SELECT * FROM tracked_webhooks ORDER BY name")

    return {
        "functions": [_row_to_function(dict(r)) for r in fn_rows],
        "webhooks": [_row_to_webhook(dict(r)) for r in wh_rows],
    }


class FunctionInput(BaseModel):
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


class WebhookInput(BaseModel):
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


@router.post("/functions")
async def create_function(body: FunctionInput):
    """Create a tracked DB function."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tracked_functions
                (name, source_id, schema_name, function_name, returns,
                 arguments, visible_to, writable_by, domain_id, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (name) DO UPDATE SET
                source_id     = EXCLUDED.source_id,
                schema_name   = EXCLUDED.schema_name,
                function_name = EXCLUDED.function_name,
                returns       = EXCLUDED.returns,
                arguments     = EXCLUDED.arguments,
                visible_to    = EXCLUDED.visible_to,
                writable_by   = EXCLUDED.writable_by,
                domain_id     = EXCLUDED.domain_id,
                description   = EXCLUDED.description,
                updated_at    = NOW()
            """,
            body.name,
            body.sourceId,
            body.schemaName,
            body.functionName,
            body.returns,
            json.dumps(body.arguments),
            body.visibleTo,
            body.writableBy,
            body.domainId,
            body.description,
        )

    log.info("Saved tracked function %s", body.name)
    return {"success": True, "name": body.name}


@router.put("/functions/{name}")
async def update_function(name: str, body: FunctionInput):
    """Update a tracked DB function by name."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE tracked_functions SET
                source_id     = $2,
                schema_name   = $3,
                function_name = $4,
                returns       = $5,
                arguments     = $6,
                visible_to    = $7,
                writable_by   = $8,
                domain_id     = $9,
                description   = $10,
                updated_at    = NOW()
            WHERE name = $1
            """,
            name,
            body.sourceId,
            body.schemaName,
            body.functionName,
            body.returns,
            json.dumps(body.arguments),
            body.visibleTo,
            body.writableBy,
            body.domainId,
            body.description,
        )

    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail=f"Function '{name}' not found")

    log.info("Updated tracked function %s", name)
    return {"success": True, "name": name}


@router.delete("/functions/{name}")
async def delete_function(name: str):
    """Delete a tracked DB function by name."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM tracked_functions WHERE name = $1", name
        )

    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail=f"Function '{name}' not found")

    log.info("Deleted tracked function %s", name)
    return {"success": True, "name": name}


@router.post("/webhooks")
async def create_webhook(body: WebhookInput):
    """Create a tracked webhook."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tracked_webhooks
                (name, url, method, timeout_ms, returns,
                 inline_return_type, arguments, visible_to, domain_id, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (name) DO UPDATE SET
                url                = EXCLUDED.url,
                method             = EXCLUDED.method,
                timeout_ms         = EXCLUDED.timeout_ms,
                returns            = EXCLUDED.returns,
                inline_return_type = EXCLUDED.inline_return_type,
                arguments          = EXCLUDED.arguments,
                visible_to         = EXCLUDED.visible_to,
                domain_id          = EXCLUDED.domain_id,
                description        = EXCLUDED.description,
                updated_at         = NOW()
            """,
            body.name,
            body.url,
            body.method,
            body.timeoutMs,
            body.returns,
            json.dumps(body.inlineReturnType),
            json.dumps(body.arguments),
            body.visibleTo,
            body.domainId,
            body.description,
        )

    log.info("Saved tracked webhook %s", body.name)
    return {"success": True, "name": body.name}


@router.put("/webhooks/{name}")
async def update_webhook(name: str, body: WebhookInput):
    """Update a tracked webhook by name."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE tracked_webhooks SET
                url                = $2,
                method             = $3,
                timeout_ms         = $4,
                returns            = $5,
                inline_return_type = $6,
                arguments          = $7,
                visible_to         = $8,
                domain_id          = $9,
                description        = $10,
                updated_at         = NOW()
            WHERE name = $1
            """,
            name,
            body.url,
            body.method,
            body.timeoutMs,
            body.returns,
            json.dumps(body.inlineReturnType),
            json.dumps(body.arguments),
            body.visibleTo,
            body.domainId,
            body.description,
        )

    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail=f"Webhook '{name}' not found")

    log.info("Updated tracked webhook %s", name)
    return {"success": True, "name": name}


@router.delete("/webhooks/{name}")
async def delete_webhook(name: str):
    """Delete a tracked webhook by name."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM tracked_webhooks WHERE name = $1", name
        )

    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail=f"Webhook '{name}' not found")

    log.info("Deleted tracked webhook %s", name)
    return {"success": True, "name": name}
