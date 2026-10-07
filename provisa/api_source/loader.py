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
from typing import TYPE_CHECKING

from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    ApiSource,
    ApiSourceType,
    ParamType,
    PromotionConfig,
)
from provisa.core.paging import PaginationConfig

if TYPE_CHECKING:
    from provisa.core.database import Connection

# Requirements: REQ-119, REQ-314, REQ-316, REQ-322


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


def _resolve_param_only(c: dict) -> bool:
    """A column carries no response value when the writer marked it param_only, or when it is
    encoded as native-filter machinery (the `_nf_` columns, which never exist in the payload)."""
    if "param_only" in c:
        return bool(c["param_only"])
    return c.get("native_filter_type") is not None


async def load_api_sources(  # REQ-119, REQ-314, REQ-316, REQ-322
    conn: "Connection",
    source_types: dict[str, str],
) -> tuple[dict[tuple[str, str], ApiEndpoint], dict[str, ApiSource]]:
    """Load the API sources and the endpoints registered tables are served from.

    Returns (api_endpoints by (source_id, table_name), api_sources_by_id). An endpoint is derived from a
    table's registration and makes nothing readable by itself: a table is in the schema only
    because it is registered.
    """
    # Load API sources
    from provisa.encryption import encryption_service  # REQ-686

    _enc = encryption_service()
    # REQ-1942: a source bound to a synthetic store is no API here -- its tables are read from
    # the store, and nothing in the environment calls it.
    src_rows = await conn.fetch(
        "SELECT id, type, base_url, spec_url, auth FROM api_sources WHERE id NOT IN "
        "(SELECT id FROM sources WHERE binding = 'synthetic')"
    )
    api_sources: dict[str, ApiSource] = {}
    for r in src_rows:
        # REQ-686: auth is encrypted at rest — decrypt before use.
        auth_data = (
            json.loads(_enc.decrypt(bytes(r["auth"])).decode("utf-8")) if r["auth"] else None
        )
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
        "promotions, body_encoding, query_template, response_normalizer FROM api_endpoints "
        "WHERE source_id NOT IN (SELECT id FROM sources WHERE binding = 'synthetic')"
    )
    api_endpoints: dict[tuple[str, str], ApiEndpoint] = {}
    for r in ep_rows:
        cols_raw = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        columns = [
            ApiColumn(
                name=c["name"],
                type=ApiColumnType(c.get("type", "string")),
                filterable=c.get("filterable", True),
                param_type=ParamType(_resolve_param_type(c))
                if _resolve_param_type(c) is not None
                else None,
                param_name=c.get("param_name"),
                param_only=_resolve_param_only(c),
                object_fields=c.get("object_fields", []),
            )
            for c in cols_raw
        ]
        pagination = None
        if r["pagination"]:
            pag_raw = (
                json.loads(r["pagination"]) if isinstance(r["pagination"], str) else r["pagination"]
            )
            pagination = PaginationConfig(**pag_raw)

        dp_raw = r.get("default_params")
        default_params = (json.loads(dp_raw) if isinstance(dp_raw, str) else dp_raw) or {}
        promo_raw = r.get("promotions")
        promo_list = (json.loads(promo_raw) if isinstance(promo_raw, str) else promo_raw) or []
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
            # REQ-1668: a query-API endpoint (neo4j) is its query — without these three the
            # hydrated endpoint would GET a query URL with no body and serve nothing.
            body_encoding=r.get("body_encoding"),
            query_template=r.get("query_template"),
            response_normalizer=r.get("response_normalizer"),
        )
        api_endpoints[(ep.source_id, ep.table_name)] = ep

    return api_endpoints, api_sources
