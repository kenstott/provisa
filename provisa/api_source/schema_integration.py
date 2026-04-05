# Copyright (c) 2026 Kenneth Stott
# Canary: 1a7e0b9f-0f60-47c5-9ef0-aeeeffdce4e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SDL generation integration for API sources (Phase U).

Registers API endpoint columns into SchemaInput so they appear in the GraphQL schema.
JSONB columns: exposed as JSON scalar, NOT in WHERE input, NOT in relationships.
Promoted columns: appear as regular filterable fields.
"""

from __future__ import annotations

from provisa.api_source.models import ApiColumnType, ApiEndpoint, PromotionConfig
from provisa.compiler.introspect import ColumnMetadata


# Map ApiColumnType to Trino-compatible type strings for schema generation
_API_TYPE_TO_TRINO: dict[ApiColumnType, str] = {
    ApiColumnType.string: "varchar",
    ApiColumnType.integer: "bigint",
    ApiColumnType.number: "double",
    ApiColumnType.boolean: "boolean",
    ApiColumnType.jsonb: "json",
}

_PROMOTION_TYPE_TO_TRINO: dict[str, str] = {
    "integer": "integer",
    "numeric": "double",
    "boolean": "boolean",
    "timestamptz": "timestamp",
    "text": "varchar",
}


def _endpoint_to_table_dict(
    endpoint: ApiEndpoint,
    domain_id: str,
    role_ids: list[str],
    promotions: list[PromotionConfig] | None = None,
) -> tuple[dict, list[ColumnMetadata]]:
    """Convert an ApiEndpoint to a table dict + column metadata for SchemaInput.

    Returns (table_dict, column_metadata_list).
    """
    table_id = 100000 + (endpoint.id or 0)  # offset to avoid collision with DB tables

    columns_for_table: list[dict] = []
    column_metadata: list[ColumnMetadata] = []

    for col in endpoint.columns:
        trino_type = _API_TYPE_TO_TRINO.get(col.type, "varchar")
        columns_for_table.append({
            "column_name": col.name,
            "visible_to": role_ids,
        })
        column_metadata.append(ColumnMetadata(
            column_name=col.name,
            data_type=trino_type,
            is_nullable=True,
        ))

    # Add promoted columns
    if promotions:
        for p in promotions:
            trino_type = _PROMOTION_TYPE_TO_TRINO.get(p.target_type, "varchar")
            columns_for_table.append({
                "column_name": p.target_column,
                "visible_to": role_ids,
            })
            column_metadata.append(ColumnMetadata(
                column_name=p.target_column,
                data_type=trino_type,
                is_nullable=True,
            ))

    table_dict = {
        "id": table_id,
        "source_id": endpoint.source_id,
        "domain_id": domain_id,
        "schema_name": "api",
        "table_name": endpoint.table_name,
        "governance": "pre-approved",
        "columns": columns_for_table,
    }

    return table_dict, column_metadata


def register_api_columns(
    tables: list[dict],
    column_types: dict[int, list[ColumnMetadata]],
    api_endpoints: list[ApiEndpoint],
    domain_id: str,
    role_ids: list[str],
    promotions_map: dict[str, list[PromotionConfig]] | None = None,
) -> None:
    """Add API endpoint columns to the schema input data (mutates tables and column_types).

    Args:
        tables: Existing list of table dicts (will be appended to).
        column_types: Existing table_id -> column metadata map (will be updated).
        api_endpoints: List of registered API endpoints.
        domain_id: Domain to assign API tables to.
        role_ids: Roles that can see API table columns.
        promotions_map: Optional table_name -> promotions mapping.
    """
    promotions_map = promotions_map or {}

    for endpoint in api_endpoints:
        promotions = promotions_map.get(endpoint.table_name, [])
        table_dict, col_meta = _endpoint_to_table_dict(
            endpoint, domain_id, role_ids, promotions,
        )
        tables.append(table_dict)
        column_types[table_dict["id"]] = col_meta
