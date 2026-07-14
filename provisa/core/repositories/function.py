# Copyright (c) 2026 Kenneth Stott
# Canary: 229d5d49-651c-4f41-aaa0-8228d1376c74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Function/webhook repository — CRUD for tracked functions and webhooks, via SQLAlchemy Core."""

# Requirements: REQ-205, REQ-206, REQ-207, REQ-208, REQ-209, REQ-210, REQ-211, REQ-304, REQ-305, REQ-306, REQ-360, REQ-361, REQ-362

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, func as _sa_func, select

from provisa.core import domain_policy
from provisa.core.models import Function, FunctionArgument, InlineType, Webhook
from provisa.core.schema_org import tracked_functions, tracked_webhooks

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert_function(  # REQ-205, REQ-206, REQ-207, REQ-304, REQ-305, REQ-306
    conn: "Connection",
    func: Function,
    return_schema: dict | None = None,
) -> int | None:
    """Upsert a tracked DB function. Returns the row id."""
    vals = {
        "name": func.name,
        "source_id": func.source_id,
        "schema_name": func.schema_name,
        "function_name": func.function_name,
        "returns": func.returns,
        # JSON columns take Python objects directly.
        "arguments": [a.model_dump() for a in func.arguments],
        "visible_to": func.visible_to,
        "writable_by": func.writable_by,
        "domain_id": domain_policy.resolve_domain_id(func.domain_id),
        "description": func.description,
        "kind": func.kind,
        "return_schema": return_schema,
        # REQ-885: implementation kind + swappable binding (JSON), decoupled from addressing.
        "impl_kind": func.impl_kind,
        "binding": func.binding,
        "materialize": func.materialize,
    }
    # REQ-870: re-introspection registers discovered mutations with an empty writable_by; existing
    # admin grants are preserved. An explicit, non-empty writable_by still applies. The preserve
    # branch is on a Python value, so resolve it here — exclude writable_by from the update set when
    # empty (leaving the stored grants untouched); otherwise update it.
    update_cols = [
        "source_id",
        "schema_name",
        "function_name",
        "returns",
        "arguments",
        "visible_to",
        "domain_id",
        "description",
        "kind",
        "return_schema",
        "impl_kind",
        "binding",
        "materialize",
    ]
    if func.writable_by:
        update_cols.append("writable_by")
    return await conn.upsert_returning(
        tracked_functions,
        vals,
        index_elements=["name"],
        returning="id",
        update_columns=update_cols,
        set_extra={"updated_at": _sa_func.now()},
    )


async def get_function(conn: "Connection", name: str) -> dict | None:  # REQ-205, REQ-304
    """Get a tracked function by name."""
    result = await conn.execute_core(
        select(tracked_functions).where(tracked_functions.c.name == name)
    )
    row = result.fetchone()
    if row is None:
        return None
    r = dict(row._mapping)
    r["arguments"] = r["arguments"] or []
    return r


async def list_functions(conn: "Connection") -> list[dict]:  # REQ-205, REQ-360
    """List all tracked functions."""
    result = await conn.execute_core(select(tracked_functions).order_by(tracked_functions.c.id))
    out = []
    for row in result.fetchall():
        r = dict(row._mapping)
        r["arguments"] = r["arguments"] or []
        out.append(r)
    return out


async def delete_function(conn: "Connection", name: str) -> bool:  # REQ-205
    """Delete a tracked function by name."""
    result = await conn.execute_core(
        _delete(tracked_functions).where(tracked_functions.c.name == name)
    )
    return (result.rowcount or 0) > 0


async def upsert_webhook(
    conn: "Connection", wh: Webhook
) -> int | None:  # REQ-209, REQ-210, REQ-211
    """Upsert a tracked webhook. Returns the row id."""
    vals = {
        "name": wh.name,
        "url": wh.url,
        "method": wh.method,
        "timeout_ms": wh.timeout_ms,
        "returns": wh.returns,
        # JSON columns take Python objects directly.
        "inline_return_type": [t.model_dump() for t in wh.inline_return_type],
        "arguments": [a.model_dump() for a in wh.arguments],
        "visible_to": wh.visible_to,
        "domain_id": domain_policy.resolve_domain_id(wh.domain_id),
        "description": wh.description,
        "kind": wh.kind,
    }
    return await conn.upsert_returning(
        tracked_webhooks,
        vals,
        index_elements=["name"],
        returning="id",
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
        ],
    )


async def get_webhook(conn: "Connection", name: str) -> dict | None:  # REQ-209, REQ-210
    """Get a tracked webhook by name."""
    result = await conn.execute_core(
        select(tracked_webhooks).where(tracked_webhooks.c.name == name)
    )
    row = result.fetchone()
    if row is None:
        return None
    r = dict(row._mapping)
    r["arguments"] = r["arguments"] or []
    r["inline_return_type"] = r["inline_return_type"] or []
    return r


async def list_webhooks(conn: "Connection") -> list[dict]:  # REQ-209, REQ-360
    """List all tracked webhooks."""
    result = await conn.execute_core(select(tracked_webhooks).order_by(tracked_webhooks.c.id))
    out = []
    for row in result.fetchall():
        r = dict(row._mapping)
        r["arguments"] = r["arguments"] or []
        r["inline_return_type"] = r["inline_return_type"] or []
        out.append(r)
    return out


async def delete_webhook(conn: "Connection", name: str) -> bool:  # REQ-209
    """Delete a tracked webhook by name."""
    result = await conn.execute_core(
        _delete(tracked_webhooks).where(tracked_webhooks.c.name == name)
    )
    return (result.rowcount or 0) > 0


def function_from_dict(d: dict) -> Function:  # REQ-205, REQ-304
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
        kind=d.get("kind", "mutation"),
        impl_kind=d.get("impl_kind", "source_procedure"),
        binding=d.get("binding") or {},
        materialize=bool(d.get("materialize", False)),
    )


def webhook_from_dict(d: dict) -> Webhook:  # REQ-209, REQ-210
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
        kind=d.get("kind", "mutation"),
    )
