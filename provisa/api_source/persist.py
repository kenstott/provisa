# Copyright (c) 2026 Kenneth Stott
# Canary: 681e3545-f054-4f72-a567-57ecc580b7a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Persist a query-API source and its query-backed tables to the control plane (REQ-1668).

One write path for every registration surface of a query-API source type (neo4j, sparql) — a
source in the config file, the registerTable mutation, the per-type admin REST routers — so a
table registered any way is the same ``api_sources`` + ``api_endpoints`` rows, hydrated at
startup by ``api_source.loader.load_api_sources`` like every other API-backed table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint, ApiSource
from provisa.core.schema_org import api_endpoints, api_sources

if TYPE_CHECKING:
    from provisa.core.database import Connection


# Config ``data_type`` (the engine type a steward writes on a column) → the API-column IR type
# an ``api_endpoints.columns`` entry carries. Closed map: an unknown type is a config error.
_DATA_TYPE_TO_API: dict[str, ApiColumnType] = {
    "varchar": ApiColumnType.string,
    "text": ApiColumnType.string,
    "string": ApiColumnType.string,
    "char": ApiColumnType.string,
    "integer": ApiColumnType.integer,
    "int": ApiColumnType.integer,
    "bigint": ApiColumnType.integer,
    "smallint": ApiColumnType.integer,
    "float": ApiColumnType.number,
    "double": ApiColumnType.number,
    "real": ApiColumnType.number,
    "decimal": ApiColumnType.number,
    "numeric": ApiColumnType.number,
    "number": ApiColumnType.number,
    "boolean": ApiColumnType.boolean,
    "bool": ApiColumnType.boolean,
    "json": ApiColumnType.jsonb,
    "jsonb": ApiColumnType.jsonb,
}


def api_column_type(data_type: str) -> ApiColumnType:  # REQ-1668
    """Map a config column ``data_type`` to the API-column IR type; unknown types are refused."""
    key = data_type.strip().lower()
    # ``varchar(120)`` / ``decimal(10,2)`` carry the base type before the parenthesis.
    key = key.split("(", 1)[0].strip()
    try:
        return _DATA_TYPE_TO_API[key]
    except KeyError as exc:
        raise ValueError(
            f"data_type {data_type!r} has no API column type; use one of "
            f"{sorted(_DATA_TYPE_TO_API)}"
        ) from exc


def api_columns_from_config(columns: list) -> list[ApiColumn]:  # REQ-1668
    """Build the endpoint's column list from a config table's columns (each typed — REQ-1426)."""
    out: list[ApiColumn] = []
    for col in columns:
        if col.data_type is None:
            raise ValueError(f"column {col.name!r}: a neo4j table column requires data_type")
        out.append(ApiColumn(name=col.name, type=api_column_type(col.data_type)))
    return out


# API-column IR type → the ``data_type`` a registered column carries when the preview infers it —
# the IR vocabulary (provisa/core/ir_types.py) the Register Table form offers, so the preview's
# type is one the picker already lists.
_API_TO_DATA_TYPE: dict[ApiColumnType, str] = {
    ApiColumnType.string: "text",
    ApiColumnType.integer: "integer",
    ApiColumnType.number: "double",
    ApiColumnType.boolean: "boolean",
    ApiColumnType.jsonb: "json",
}


def data_type_for_api_column(api_type: ApiColumnType) -> str:  # REQ-1670
    """The registered-column ``data_type`` for a preview-inferred API column type."""
    return _API_TO_DATA_TYPE[api_type]


async def persist_api_source(conn: "Connection", api_source: ApiSource) -> None:  # REQ-1668
    """Upsert the ``api_sources`` row for a query-API source (neo4j, sparql)."""
    await conn.upsert(
        api_sources,
        {
            "id": api_source.id,
            "type": api_source.type.value,
            "base_url": api_source.base_url,
            "auth": None,
        },
        index_elements=["id"],
        update_columns=["type", "base_url"],
    )


async def persist_api_endpoint(conn: "Connection", endpoint: ApiEndpoint) -> None:  # REQ-1668
    """Upsert the ``api_endpoints`` row for one query-backed table, keyed by table name."""
    await conn.upsert(
        api_endpoints,
        {
            "source_id": endpoint.source_id,
            "path": endpoint.path,
            "method": endpoint.method,
            "table_name": endpoint.table_name,
            "columns": [c.model_dump(mode="json") for c in endpoint.columns],
            "ttl": endpoint.ttl,
            "default_params": None,
            "promotions": [],
            "body_encoding": endpoint.body_encoding,
            "query_template": endpoint.query_template,
            "response_normalizer": endpoint.response_normalizer,
        },
        index_elements=["table_name"],
        update_columns=[
            "source_id",
            "path",
            "method",
            "columns",
            "ttl",
            "body_encoding",
            "query_template",
            "response_normalizer",
        ],
    )
