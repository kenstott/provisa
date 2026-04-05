# Copyright (c) 2026 Kenneth Stott
# Canary: a54c54bb-2714-489c-98f5-3e9a19218ee4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load API sources and endpoints from PG into app state (Phase U)."""

from __future__ import annotations

import json

import asyncpg

from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    ApiSource,
    ApiSourceType,
    PaginationConfig,
)
from provisa.api_source.schema_integration import register_api_columns
from provisa.compiler.introspect import ColumnMetadata


async def load_api_sources(
    conn: asyncpg.Connection,
    tables: list[dict],
    col_types: dict[int, list[ColumnMetadata]],
    roles: list[dict],
    source_types: dict[str, str],
) -> tuple[dict[str, ApiEndpoint], dict[str, ApiSource]]:
    """Load API sources/endpoints from PG. Register into schema tables/col_types.

    Returns (api_endpoints_by_table_name, api_sources_by_id).
    """
    # Load API sources
    src_rows = await conn.fetch("SELECT id, type, base_url, spec_url, auth FROM api_sources")
    api_sources: dict[str, ApiSource] = {}
    for r in src_rows:
        auth_data = json.loads(r["auth"]) if r["auth"] else None
        api_src = ApiSource(
            id=r["id"],
            type=ApiSourceType(r["type"]),
            base_url=r["base_url"],
            spec_url=r.get("spec_url"),
            auth=auth_data,
        )
        api_sources[api_src.id] = api_src
        source_types[api_src.id] = api_src.type.value

    # Load API endpoints
    ep_rows = await conn.fetch(
        "SELECT id, source_id, path, method, table_name, columns, ttl, "
        "response_root, pagination FROM api_endpoints"
    )
    api_endpoints: dict[str, ApiEndpoint] = {}
    api_endpoint_list: list[ApiEndpoint] = []
    for r in ep_rows:
        cols_raw = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        columns = [
            ApiColumn(
                name=c["name"],
                type=ApiColumnType(c.get("type", "string")),
                filterable=c.get("filterable", True),
                param_type=c.get("param_type"),
                param_name=c.get("param_name"),
            )
            for c in cols_raw
        ]
        pagination = None
        if r["pagination"]:
            pag_raw = json.loads(r["pagination"]) if isinstance(r["pagination"], str) else r["pagination"]
            pagination = PaginationConfig(**pag_raw)

        ep = ApiEndpoint(
            id=r["id"],
            source_id=r["source_id"],
            path=r["path"],
            method=r["method"],
            table_name=r["table_name"],
            columns=columns,
            ttl=r["ttl"],
            response_root=r.get("response_root"),
            pagination=pagination,
        )
        api_endpoints[ep.table_name] = ep
        api_endpoint_list.append(ep)

    if api_endpoint_list:
        role_ids = [r["id"] for r in roles]
        register_api_columns(
            tables, col_types, api_endpoint_list,
            domain_id="api", role_ids=role_ids,
        )

    return api_endpoints, api_sources
