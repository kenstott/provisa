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
    ParamType,
    PromotionConfig,
)
from provisa.api_source.schema_integration import register_api_columns
from provisa.compiler.introspect import ColumnMetadata


def _resolve_param_type(c: dict) -> str | None:
    """Read param_type from either 'param_type' or 'native_filter_type' key."""
    pt = c.get("param_type")
    if pt is not None:
        return pt
    nft = c.get("native_filter_type")
    if nft == "path_param":
        return "path"
    if nft == "query_param":
        return "query"
    return None


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
        "response_root, error_path, pk_column, pagination, max_concurrency, default_params, "
        "promotions FROM api_endpoints"
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
                param_type=ParamType(_resolve_param_type(c)) if _resolve_param_type(c) is not None else None,
                param_name=c.get("param_name"),
                object_fields=c.get("object_fields", []),
            )
            for c in cols_raw
        ]
        pagination = None
        if r["pagination"]:
            pag_raw = json.loads(r["pagination"]) if isinstance(r["pagination"], str) else r["pagination"]
            pagination = PaginationConfig(**pag_raw)

        dp_raw = r.get("default_params")
        default_params = (
            json.loads(dp_raw) if isinstance(dp_raw, str) else dp_raw
        ) or {}
        promo_raw = r.get("promotions")
        promo_list = (
            json.loads(promo_raw) if isinstance(promo_raw, str) else promo_raw
        ) or []
        promotions = [PromotionConfig(**p) for p in promo_list]
        ep = ApiEndpoint(
            id=r["id"],
            source_id=r["source_id"],
            path=r["path"],
            method=r["method"],
            table_name=r["table_name"],
            columns=columns,
            ttl=r["ttl"],
            response_root=r.get("response_root"),
            error_path=r.get("error_path"),
            pk_column=r.get("pk_column"),
            pagination=pagination,
            max_concurrency=r.get("max_concurrency"),
            default_params=default_params,
            promotions=promotions,
        )
        api_endpoints[ep.table_name] = ep
        api_endpoint_list.append(ep)

    if api_endpoint_list:
        role_ids = [r["id"] for r in roles]
        registered_source_ids = {t["source_id"] for t in tables if t.get("source_id")}
        unregistered = [ep for ep in api_endpoint_list if ep.source_id not in registered_source_ids]
        if unregistered:
            # REQ-119: promoted JSONB columns are registered as first-class columns.
            promotions_map = {
                ep.table_name: ep.promotions for ep in unregistered if ep.promotions
            }
            register_api_columns(
                tables, col_types, unregistered,
                domain_id="api", role_ids=role_ids,
                promotions_map=promotions_map,
            )

    return api_endpoints, api_sources
