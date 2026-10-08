# Copyright (c) 2026 Kenneth Stott
# Canary: e3998244-b56d-4bf8-9823-9999ddaa9da1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering an OpenAPI source's table in the admin (REQ-316, REQ-318): the table's
``api_endpoints`` row is derived from its registration by the same function the config path
uses (``api_source.openapi_endpoint.register_openapi_endpoint``), so every OpenAPI table is read
through the one paged caller whichever way it was registered."""

# Requirements: REQ-316, REQ-318

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import sources

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult
    from provisa.core.database import Connection
    from provisa.core.models import Table


async def persist_openapi_endpoint(
    state: Any, conn: Connection, model: Table
) -> MutationResult | None:
    """After the registered_tables row lands: write the endpoint an OpenAPI table is served from
    (its source's base URL and auth were stored when the source was registered).
    No-op for every other source type. Refused by name when the source's spec is not loaded in
    this process, or has no operation of the table's name: the table would have nothing to be
    read from."""
    from provisa.api.admin.types import MutationResult
    from provisa.api_source.openapi_endpoint import NoOperation, register_openapi_endpoint
    from provisa.openapi.mapper import NoRowsField

    row = (
        await conn.execute_core(
            select(sources.c.type, sources.c.cache_ttl).where(sources.c.id == model.source_id)
        )
    ).fetchone()
    if row is None or row.type != "openapi":
        return None
    entry = (getattr(state, "openapi_specs", None) or {}).get(model.source_id)
    if entry is None:
        return MutationResult(
            success=False,
            message=f"The spec of OpenAPI source {model.source_id!r} is not loaded",
            code="schema.openapi_spec_not_loaded",
            params={"source_id": model.source_id},
        )
    try:
        await register_openapi_endpoint(conn, model, spec=entry["spec"], ttl=row.cache_ttl or 300)
    except NoOperation as missing:
        return MutationResult(
            success=False,
            message=str(missing),
            code="schema.openapi_no_operation",
            params={"source_id": model.source_id, "table": model.table_name},
        )
    except NoRowsField as missing:
        return MutationResult(
            success=False,
            message=f"table {model.table_name!r}: {missing}",
            code="schema.openapi_no_rows_field",
            params={"table": model.table_name, "rows_field": missing.rows_field},
        )
    return None
