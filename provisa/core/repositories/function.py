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

from provisa.core import model_change

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, func as _sa_func, select

from provisa.core import domain_policy
from provisa.core.models import Function, FunctionArgument, InlineType, Webhook
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.repositories.origin import require as require_origin
from provisa.core.repositories.origin import take_over
from provisa.core.schema_org import tracked_functions, tracked_webhooks

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert_function(  # REQ-205, REQ-206, REQ-207, REQ-304, REQ-305, REQ-306
    conn: "Connection",
    func: Function,
    return_schema: dict | None = None,
    *,
    origin: str,
) -> int | None:
    """Upsert a tracked DB function. Returns the row id. ``origin`` says where the command
    comes from (``repositories.origin``): written at CREATE, left alone after, except that a
    config load takes over an admin-made one."""
    model_change.name("upsert", "command", func.name)  # REQ-1524
    require_origin(origin)
    domain_id = domain_policy.command_domain_id(func.domain_id, func.name)  # REQ-1531
    # REQ-1634: a DataProduct's member commands must all share its domain_id — same gate as
    # table.py's upsert, so config load, admin GraphQL, and introspection are all covered.
    if func.product_id is not None:
        product = await data_product_repo.get(conn, func.product_id)
        if product is None:
            raise ValueError(f"data product {func.product_id!r} does not exist")
        if product["domain_id"] != domain_id:
            raise ValueError(
                f"command {func.name} is in domain {domain_id!r} but data product "
                f"{func.product_id!r} belongs to domain {product['domain_id']!r}"
            )
    vals = {
        "name": func.name,
        "source_id": func.source_id,
        "schema_name": func.schema_name,
        "function_name": func.function_name,
        "returns": func.returns,
        # JSON columns take Python objects directly.
        "arguments": [a.model_dump() for a in func.arguments],
        "visible_to": func.visible_to,
        "domain_id": domain_id,
        "description": func.description,
        "kind": func.kind,
        "product_id": func.product_id,  # REQ-1634
        "return_schema": return_schema,
        # REQ-1159: canonical IR-typed output dataset contract (return_schema is its GraphQL projection).
        "output_columns": [c.model_dump() for c in func.output_columns]
        if func.output_columns
        else None,
        # REQ-885: implementation kind + swappable binding (JSON), decoupled from addressing.
        "impl_kind": func.impl_kind,
        "binding": func.binding,
        "materialize": func.materialize,
        "requires_approval": func.requires_approval,  # REQ-1924
        "writes_table": func.writes_table,  # REQ-1924, REQ-871
    }
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
        "product_id",
        "return_schema",
        "output_columns",
        "impl_kind",
        "binding",
        "materialize",
        "requires_approval",
        "writes_table",
    ]
    function_id = await conn.upsert_returning(
        tracked_functions,
        {**vals, "origin": origin},  # REQ-1919: on INSERT only — not among the update columns
        index_elements=["name"],
        returning="id",
        update_columns=update_cols,
        set_extra={"updated_at": _sa_func.now()},
    )
    await take_over(
        conn,
        tracked_functions,
        (tracked_functions.c.name == func.name,),
        kind="command",
        ident=func.name,
        origin=origin,
    )
    return function_id


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


class CommandDeleteRefused(Exception):
    """A command or webhook that may not be deleted because other objects refer to it;
    ``dependents`` lists them."""

    def __init__(self, name: str, dependents: list[Dependent]) -> None:
        self.name = name
        self.dependents = dependents
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"{name!r} is still referred to by: {named}")


async def _delete_one(conn: "Connection", kind: str, table, name: str) -> bool:  # REQ-1918
    """Delete one command or webhook through the dependency guard. Nothing in the model refers
    to one today, so the guard returns nothing; what goes with it is its parts — the row filters
    defined on it and its tag assignments — which no foreign key would remove. One transaction."""
    ref = ObjectRef(kind, name)
    async with conn.transaction():
        found = await conn.execute_core(select(table.c.name).where(table.c.name == name))
        if found.fetchone() is None:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise CommandDeleteRefused(name, blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(table).where(table.c.name == name))
    return True


async def delete_function(conn: "Connection", name: str) -> bool:  # REQ-205, REQ-1918
    """Delete a tracked function by name: THE delete, for every surface."""
    model_change.name("delete", "command", name)  # REQ-1524
    return await _delete_one(conn, "command", tracked_functions, name)


async def upsert_webhook(
    conn: "Connection", wh: Webhook, *, origin: str
) -> int | None:  # REQ-209, REQ-210, REQ-211, REQ-1919
    """Upsert a tracked webhook. Returns the row id. ``origin`` says where it comes from
    (``repositories.origin``): written at CREATE, left alone after, except that a config load
    takes over an admin-made one."""
    model_change.name("upsert", "webhook", wh.name)  # REQ-1524
    require_origin(origin)
    vals = {
        "origin": origin,  # REQ-1919: on INSERT only — not among the update columns
        "name": wh.name,
        "url": wh.url,
        "method": wh.method,
        "timeout_ms": wh.timeout_ms,
        "returns": wh.returns,
        # JSON columns take Python objects directly.
        "inline_return_type": [t.model_dump() for t in wh.inline_return_type],
        "arguments": [a.model_dump() for a in wh.arguments],
        "visible_to": wh.visible_to,
        "domain_id": domain_policy.command_domain_id(wh.domain_id, wh.name),  # REQ-1531
        "description": wh.description,
        "kind": wh.kind,
    }
    webhook_id = await conn.upsert_returning(
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
    await take_over(
        conn,
        tracked_webhooks,
        (tracked_webhooks.c.name == wh.name,),
        kind="webhook",
        ident=wh.name,
        origin=origin,
    )
    return webhook_id


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


async def delete_webhook(conn: "Connection", name: str) -> bool:  # REQ-209, REQ-1918
    """Delete a tracked webhook by name: THE delete, for every surface."""
    model_change.name("delete", "webhook", name)  # REQ-1524
    return await _delete_one(conn, "webhook", tracked_webhooks, name)


async def remove_all(conn: "Connection") -> None:
    """Remove every command and webhook: the config loader's full replace, which declares them
    as a set. It is not a deletion of one object and does not ask the dependency guard."""
    await conn.execute_core(_delete(tracked_functions))
    await conn.execute_core(_delete(tracked_webhooks))


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
        domain_id=d.get("domain_id", ""),
        description=d.get("description"),
        kind=d.get("kind", "mutation"),
        product_id=d.get("product_id"),
        impl_kind=d.get("impl_kind", "source_procedure"),
        binding=d.get("binding") or {},
        materialize=bool(d.get("materialize", False)),
        requires_approval=bool(d.get("requires_approval", False)),
        writes_table=d.get("writes_table"),
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
