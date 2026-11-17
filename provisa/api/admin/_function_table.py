# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f0a3e-5b7d-4e92-9a1c-2f8d4b6e7a10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A parameterized table is an attribute of its function (REQ-1986).

A source operation that reads rows and requires a parameter is a function. Whether it is also
exposed as a parameterized table is the function's ``exposed_as_table``. While it is on, the
table is derived from the function: registered from what the source offers for the operation,
exactly as Register Table would, and returned by the function. It is never registered on its own,
and its columns and parameters are not edited on the table. While it is off, the function returns
the rows of the source's plain list of the same type.

Its value at registration: on when no operation of the source without a required parameter
returns the same type (or the source names no type), off otherwise. It is set once; turning it
off is refused while anything refers to the derived table.
"""

# Requirements: REQ-1986

from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.api.errors import ApiError
from provisa.core.schema_org import registered_tables, tracked_functions

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.executor.source_operation import Operation


# The schema Register Table lists an operation's table under, by source type.
_TABLE_SCHEMA = {"openapi": "openapi", "graphql_remote": "graphql"}


def _same_name(a: str, b: str) -> bool:
    """Two names of one operation's table: an OpenAPI table is named after its operation id
    as its endpoint is matched (``getPetById`` is ``get_pet_by_id``)."""
    from provisa.api_source.openapi_endpoint import normalize_op_id

    return normalize_op_id(a) == normalize_op_id(b)


async def default_exposed(state: Any, source_id: str, returns_type: str | None) -> bool:
    """The attribute's value at registration: on unless an operation of ``source_id`` without a
    required parameter returns ``returns_type``. A source that names no type cannot be compared,
    so its operation gets its table."""
    from provisa.executor.source_operation import plain_lists

    if returns_type is None:
        return True
    return not any(p.returns_type == returns_type for p in await plain_lists(state, source_id))


async def _stored_table(conn: "Connection", source_id: str, table_name: str) -> dict | None:
    """The registered table of ``source_id`` that is the operation's table ``table_name``: a
    live one before a retired one; None when there is none."""
    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.retired,
            ).where(registered_tables.c.source_id == source_id)
        )
    ).fetchall()
    found = sorted(
        (dict(r._mapping) for r in rows if _same_name(r.table_name, table_name)),
        key=lambda r: bool(r["retired"]),
    )
    return found[0] if found else None


def _schema_table(row: dict) -> str:
    return f"{row['schema_name']}.{row['table_name']}"


async def plain_list_returned(
    conn: "Connection", state: Any, source_id: str, operation: "Operation"
) -> str:
    """What a function whose table is off returns: the registered table of the source's first
    plain list of the same type, as ``schema.table``. Refused, naming the plain lists, when none
    of them is registered."""
    from provisa.executor.source_operation import plain_lists

    same = (
        []
        if operation.returns_type is None
        else [
            p
            for p in await plain_lists(state, source_id)
            if p.returns_type == operation.returns_type
        ]
    )
    if not same:
        raise ApiError(
            422,
            "functions.no_plain_list",
            f"No operation of source {source_id!r} without a required parameter returns the rows "
            f"of {operation.name!r}; its table stays on",
            source_id=source_id,
            operation=operation.name,
        )
    for plain in same:
        stored = await _stored_table(conn, source_id, plain.table)
        if stored is not None and not stored["retired"]:
            return _schema_table(stored)
    lists = ", ".join(p.table for p in same)
    raise ApiError(
        422,
        "functions.plain_list_unregistered",
        f"Operation {operation.name!r} of source {source_id!r} returns {operation.returns_type} "
        f"rows, which the plain list {lists} returns; register it as a table first",
        source_id=source_id,
        operation=operation.name,
        lists=lists,
    )


async def derive_table(request: Any, source_id: str, domain_id: str, operation: "Operation") -> str:
    """The table derived from the function of ``operation``, as ``schema.table``: the one already
    registered for the operation, the retired one restored, or else registered by the registerTable
    mutation from the columns and parameters the source offers, as Register Table proposes them."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.admin.schema_mutation import Mutation, default_table_input
    from provisa.api.admin.schema_query import resolve_available_columns_metadata
    from provisa.api.app import state
    from provisa.core.repositories import table as table_repo

    assert operation.table is not None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        stored = await _stored_table(conn, source_id, operation.table)
        if stored is not None and stored["retired"]:
            await table_repo.restore(conn, stored["id"])
        if stored is not None:
            return _schema_table(stored)
    schema_name = _TABLE_SCHEMA[state.source_types[source_id]]
    columns = await resolve_available_columns_metadata(source_id, schema_name, operation.table)
    table = default_table_input(source_id, domain_id, schema_name, operation.table, columns)
    info = types.SimpleNamespace(context={"request": request})
    mutation = Mutation()
    result = await mutation.register_table(info, table)  # pyright: ignore[reportCallIssue]
    if result.success:
        # Derived from a function that is in service, it is in service with it: it is not a
        # registration of its own waiting on its domain's release (REQ-1921).
        derived = (result.params or {})["id"]
        result = await mutation.set_table_draft(info, derived, False)  # pyright: ignore[reportCallIssue]
    if not result.success:
        raise ApiError(
            422,
            result.code or "functions.table_not_derived",
            result.message,
            **(result.params or {}),
        )
    return f"{schema_name}.{operation.table}"


async def refuse_hiding(request: Any, conn: "Connection", function_name: str, returns: str) -> int:
    """The id of the table ``function_name`` derives (``returns``, ``schema.table``), once it may
    be hidden; refused, naming what the caller may read of each object that refers to it."""
    from provisa.api.admin.read_rights import reader_for_request, readable_dependents, referred_text
    from provisa.core.repositories.integrity import hiding_guard

    schema_name, _, table_name = returns.partition(".")
    row = (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                registered_tables.c.schema_name == schema_name,
                registered_tables.c.table_name == table_name,
                registered_tables.c.retired.is_(False),
            )
        )
    ).fetchone()
    assert row is not None, f"the table {returns!r} derived from {function_name!r} is registered"
    blocking = await hiding_guard(conn, row.id, function_name)
    if blocking:
        readable = await readable_dependents(conn, reader_for_request(request), blocking)
        raise ApiError(
            409,
            "functions.table_has_dependents",
            referred_text(
                f"The table {table_name!r} of function {function_name!r} is still referred to by",
                readable,
            ),
            name=function_name,
            table=table_name,
            dependents=[d.as_dict() for d in readable],
        )
    return row.id


async def remove_table(table_id: int) -> None:
    """Retire the derived table ``table_id``, once its function no longer returns it (the table
    delete, with what it removes outside the control plane)."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.app import state
    from provisa.cache.tenancy import invalidate_tables
    from provisa.core.repositories import table as table_repo

    pool = await _get_pool()
    async with pool.acquire() as conn:
        await table_repo.delete(conn, table_id, reason="its function no longer exposes it")
    await invalidate_tables(state, [table_id])
    if state.hot_manager is not None:
        await state.hot_manager.invalidate(table_id)


async def deriving_function(conn: "Connection", table: dict) -> str | None:
    """The function ``table`` (a registered table row) is derived from, or None when it is not
    derived: a function whose attribute is on and that returns it."""
    found = (
        await conn.execute_core(
            select(tracked_functions.c.name).where(
                tracked_functions.c.exposed_as_table.is_(True),
                tracked_functions.c.returns == _schema_table(table),
            )
        )
    ).fetchone()
    return None if found is None else found[0]


async def derived_edit_refusal(conn: "Connection", model: Any) -> str | None:
    """Why this save of the table ``model`` may not be made, or None: the table is derived from
    a function, and the save changes its columns or parameters, which follow from the function."""
    from provisa.core.schema_org import table_columns as tc

    row = (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                registered_tables.c.source_id == model.source_id,
                registered_tables.c.schema_name == model.schema_name,
                registered_tables.c.table_name == model.table_name,
            )
        )
    ).fetchone()
    if row is None:
        return None
    function = await deriving_function(
        conn, {"schema_name": model.schema_name, "table_name": model.table_name}
    )
    if function is None:
        return None
    stored = {
        r.column_name: (r.data_type, r.native_filter_type)
        for r in (
            await conn.execute_core(
                select(tc.c.column_name, tc.c.data_type, tc.c.native_filter_type).where(
                    tc.c.table_id == row.id
                )
            )
        ).fetchall()
    }
    saved = {c.name: (c.data_type, c.native_filter_type) for c in model.columns}
    if saved == stored:
        return None
    return (
        f"The columns and parameters of {model.table_name!r} follow from the function "
        f"{function!r} it is derived from; they are not edited on the table."
    )


async def derived_delete_refusal(conn: "Connection", table: dict) -> str | None:
    """Why the table ``table`` (a registered table row) may not be deleted on its own, or None:
    it is derived from a function, and goes only when the function's table is turned off."""
    function = await deriving_function(conn, table)
    if function is None:
        return None
    return (
        f"Table {table['table_name']!r} is derived from the function {function!r}; it is removed "
        "by turning off the function's table, where the function is edited."
    )
