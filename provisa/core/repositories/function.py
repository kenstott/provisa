# Copyright (c) 2025 Kenneth Stott
# Canary: 229d5d49-651c-4f41-aaa0-8228d1376c74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Function/webhook repository — CRUD for tracked functions and webhooks in PG config DB."""

from __future__ import annotations

import json

import asyncpg

from provisa.core.models import Function, FunctionArgument, InlineType, Webhook


async def upsert_function(conn: asyncpg.Connection, func: Function) -> int:
    """Upsert a tracked DB function. Returns the row id."""
    func_id = await conn.fetchval(
        """
        INSERT INTO tracked_functions
            (name, source_id, schema_name, function_name, returns,
             arguments, visible_to, writable_by, domain_id, description)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (name) DO UPDATE SET
            source_id = EXCLUDED.source_id,
            schema_name = EXCLUDED.schema_name,
            function_name = EXCLUDED.function_name,
            returns = EXCLUDED.returns,
            arguments = EXCLUDED.arguments,
            visible_to = EXCLUDED.visible_to,
            writable_by = EXCLUDED.writable_by,
            domain_id = EXCLUDED.domain_id,
            description = EXCLUDED.description
        RETURNING id
        """,
        func.name,
        func.source_id,
        func.schema_name,
        func.function_name,
        func.returns,
        json.dumps([a.model_dump() for a in func.arguments]),
        func.visible_to,
        func.writable_by,
        func.domain_id,
        func.description,
    )
    return func_id


async def get_function(conn: asyncpg.Connection, name: str) -> dict | None:
    """Get a tracked function by name."""
    row = await conn.fetchrow(
        "SELECT * FROM tracked_functions WHERE name = $1", name
    )
    if not row:
        return None
    result = dict(row)
    result["arguments"] = json.loads(result["arguments"]) if result["arguments"] else []
    return result


async def list_functions(conn: asyncpg.Connection) -> list[dict]:
    """List all tracked functions."""
    rows = await conn.fetch("SELECT * FROM tracked_functions ORDER BY id")
    result = []
    for row in rows:
        r = dict(row)
        r["arguments"] = json.loads(r["arguments"]) if r["arguments"] else []
        result.append(r)
    return result


async def delete_function(conn: asyncpg.Connection, name: str) -> bool:
    """Delete a tracked function by name."""
    result = await conn.execute(
        "DELETE FROM tracked_functions WHERE name = $1", name
    )
    return result == "DELETE 1"


async def upsert_webhook(conn: asyncpg.Connection, wh: Webhook) -> int:
    """Upsert a tracked webhook. Returns the row id."""
    wh_id = await conn.fetchval(
        """
        INSERT INTO tracked_webhooks
            (name, url, method, timeout_ms, returns, inline_return_type,
             arguments, visible_to, domain_id, description)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (name) DO UPDATE SET
            url = EXCLUDED.url,
            method = EXCLUDED.method,
            timeout_ms = EXCLUDED.timeout_ms,
            returns = EXCLUDED.returns,
            inline_return_type = EXCLUDED.inline_return_type,
            arguments = EXCLUDED.arguments,
            visible_to = EXCLUDED.visible_to,
            domain_id = EXCLUDED.domain_id,
            description = EXCLUDED.description
        RETURNING id
        """,
        wh.name,
        wh.url,
        wh.method,
        wh.timeout_ms,
        wh.returns,
        json.dumps([t.model_dump() for t in wh.inline_return_type]),
        json.dumps([a.model_dump() for a in wh.arguments]),
        wh.visible_to,
        wh.domain_id,
        wh.description,
    )
    return wh_id


async def get_webhook(conn: asyncpg.Connection, name: str) -> dict | None:
    """Get a tracked webhook by name."""
    row = await conn.fetchrow(
        "SELECT * FROM tracked_webhooks WHERE name = $1", name
    )
    if not row:
        return None
    result = dict(row)
    result["arguments"] = json.loads(result["arguments"]) if result["arguments"] else []
    result["inline_return_type"] = (
        json.loads(result["inline_return_type"]) if result["inline_return_type"] else []
    )
    return result


async def list_webhooks(conn: asyncpg.Connection) -> list[dict]:
    """List all tracked webhooks."""
    rows = await conn.fetch("SELECT * FROM tracked_webhooks ORDER BY id")
    result = []
    for row in rows:
        r = dict(row)
        r["arguments"] = json.loads(r["arguments"]) if r["arguments"] else []
        r["inline_return_type"] = (
            json.loads(r["inline_return_type"]) if r["inline_return_type"] else []
        )
        result.append(r)
    return result


async def delete_webhook(conn: asyncpg.Connection, name: str) -> bool:
    """Delete a tracked webhook by name."""
    result = await conn.execute(
        "DELETE FROM tracked_webhooks WHERE name = $1", name
    )
    return result == "DELETE 1"


def function_from_dict(d: dict) -> Function:
    """Reconstruct a Function model from a DB row dict."""
    return Function(
        name=d["name"],
        source_id=d["source_id"],
        schema_name=d["schema_name"],
        function_name=d["function_name"],
        returns=d["returns"],
        arguments=[FunctionArgument(**a) for a in d.get("arguments", [])],
        visible_to=d.get("visible_to", []),
        writable_by=d.get("writable_by", []),
        domain_id=d.get("domain_id", ""),
        description=d.get("description"),
    )


def webhook_from_dict(d: dict) -> Webhook:
    """Reconstruct a Webhook model from a DB row dict."""
    return Webhook(
        name=d["name"],
        url=d["url"],
        method=d.get("method", "POST"),
        timeout_ms=d.get("timeout_ms", 5000),
        returns=d.get("returns"),
        inline_return_type=[InlineType(**t) for t in d.get("inline_return_type", [])],
        arguments=[FunctionArgument(**a) for a in d.get("arguments", [])],
        visible_to=d.get("visible_to", []),
        domain_id=d.get("domain_id", ""),
        description=d.get("description"),
    )
