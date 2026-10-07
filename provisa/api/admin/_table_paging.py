# Copyright (c) 2026 Kenneth Stott
# Canary: fc0f72c4-003e-4402-a6d6-c033989dae0e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table's paging in the admin API (REQ-318): what reads it page by page, its
declared paging as the admin shows it, and the save.

The table's ``pagination`` (provisa.core.paging) is authored here and in the config only. Saving
a REST table's paging writes the copy on its ``api_endpoints`` row and on the endpoint this
process serves it from; a connection table's bound is read off the registry at each read."""

# Requirements: REQ-318

from __future__ import annotations

from typing import Any

from pydantic import ValidationError
from sqlalchemy import select, update

from provisa.api.admin.types import MutationResult, PagingInput, PagingType
from provisa.core.paging import (
    PaginationConfig,
    PagingRefused,
    check_paging,
    paging_kind,
    paging_row,
    stored_paging,
)
from provisa.core.schema_org import api_endpoints, registered_tables, sources


def table_paging_kind(state: Any, source_type: str, source_id: str, table_name: str) -> str | None:
    """What reads ``table_name`` page by page. A remote GraphQL table is a connection when the
    registration this process reads it by has a row path (its rows sit under a Relay
    connection); one this process holds no registration for is not known to be one."""
    connection: bool | None = None
    if source_type == "graphql_remote":
        registration = (getattr(state, "graphql_remote_sources", None) or {}).get(source_id)
        tables = registration["tables"] if registration is not None else []
        spec = next((t for t in tables if t["sql_name"] == table_name), None)
        connection = spec is not None and bool(spec.get("rows_path"))
    return paging_kind(source_type, connection=connection)


def paging_type(stored: dict | None) -> PagingType | None:
    """The table's paging as declared, for the admin."""
    return None if stored is None else PagingType(**stored)


def _refused(code: str, params: dict, message: str) -> MutationResult:
    return MutationResult(success=False, message=message, code=code, params=params)


def declared_paging(
    state: Any, source_type: str, source_id: str, table_name: str, paging: PagingInput | None
) -> PaginationConfig | MutationResult | None:
    """The paging an admin input declares for a table, checked against what reads the table;
    the refusal, by name, when the table cannot take it. Nothing declared is no paging."""
    declared = {} if paging is None else {k: v for k, v in vars(paging).items() if v is not None}
    if not declared:
        return None
    try:
        pagination = PaginationConfig.model_validate(declared)
    except ValidationError as exc:
        reason = "; ".join(str(e["msg"]) for e in exc.errors())
        return _refused(
            "schema.paging_invalid",
            {"table": table_name, "reason": reason},
            f"table {table_name!r}: {reason}",
        )
    try:
        check_paging(
            pagination,
            table=table_name,
            kind=table_paging_kind(state, source_type, source_id, table_name),
            ceiling_rows=state.config.graphql_remote.max_rows,
        )
    except PagingRefused as refused:
        return _refused(refused.code, refused.params, str(refused))
    return pagination


async def save_table_paging(
    state: Any, conn: Any, table_id: int, paging: PagingInput | None
) -> MutationResult:
    """Replace ``table_id``'s paging (None clears it). Refused by name when the table cannot take
    it, or when a connection table's max_rows is above ``graphql_remote.max_rows``."""
    row = (
        await conn.execute_core(
            select(
                registered_tables.c.source_id,
                registered_tables.c.table_name,
                registered_tables.c.pagination,
                sources.c.type,
            )
            .join(sources, sources.c.id == registered_tables.c.source_id)
            .where(registered_tables.c.id == table_id)
        )
    ).fetchone()
    if row is None:
        return _refused(
            "schema.table_not_found", {"table": table_id}, f"Table {table_id} not found"
        )
    pagination = declared_paging(state, row.type, row.source_id, row.table_name, paging)
    if isinstance(pagination, MutationResult):
        return pagination
    # REQ-316: a table's columns are those of the objects at its row location, so the location is
    # set when the table is registered and an edit of its paging keeps it.
    kept = stored_paging(row.pagination)
    kept_field = None if kept is None else kept.rows_field
    if (None if pagination is None else pagination.rows_field) != kept_field:
        return _refused(
            "schema.paging_rows_field_fixed",
            {"table": row.table_name, "rows_field": kept_field or ""},
            f"table {row.table_name!r}: where its rows are ({kept_field!r}) is set when the table "
            "is registered; register the table again to read its rows from elsewhere",
        )
    stored = paging_row(pagination)
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(pagination=stored)
    )
    if pagination is None or pagination.is_endpoint:
        # The REST endpoint's copy: its row, and the endpoint this process serves it from.
        await conn.execute_core(
            update(api_endpoints)
            .where(api_endpoints.c.table_name == row.table_name)
            .values(pagination=stored)
        )
        endpoint = (getattr(state, "api_endpoints", None) or {}).get(row.table_name)
        if endpoint is not None:
            state.api_endpoints[row.table_name] = endpoint.model_copy(
                update={"pagination": stored_paging(stored)}
            )
    from provisa.federation.registered_tables_cache import get_cache_for

    get_cache_for(state).clear()  # a connection table's next read takes its new bound
    return MutationResult(
        success=True,
        message=f"Paging updated for table {row.table_name!r}",
        code="schema.table_paging_updated",
        params={"table": row.table_name},
    )
