# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Neo4j source builder (Phase AO).

Translates a Neo4j connection config into an ApiSource + ApiEndpoint
that the existing API source pipeline can execute.
"""

from __future__ import annotations

from dataclasses import dataclass

from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    ApiSource,
    ApiSourceType,
)


@dataclass
class Neo4jSourceConfig:
    """User-supplied Neo4j connection parameters."""

    source_id: str
    host: str
    port: int = 7474
    database: str = "neo4j"
    auth: object | None = None  # ApiAuth instance
    use_https: bool = False


def build_api_source(cfg: Neo4jSourceConfig) -> ApiSource:
    """Build an ApiSource record for a Neo4j instance."""
    scheme = "https" if cfg.use_https else "http"
    base_url = f"{scheme}://{cfg.host}:{cfg.port}"
    return ApiSource(
        id=cfg.source_id,
        type=ApiSourceType.openapi,  # treated as a generic POST API
        base_url=base_url,
        auth=cfg.auth,
    )


def build_endpoint(
    cfg: Neo4jSourceConfig,
    table_name: str,
    cypher: str,
    columns: list[ApiColumn],
    ttl: int = 300,
) -> ApiEndpoint:
    """Build an ApiEndpoint for a single Neo4j table (Cypher query).

    The endpoint POSTs the Cypher to Neo4j's HTTP transaction API v2
    and uses the neo4j_tabular normalizer to convert the response.
    """
    return ApiEndpoint(
        source_id=cfg.source_id,
        path=f"/db/{cfg.database}/query/v2",
        method="POST",
        table_name=table_name,
        columns=columns,
        ttl=ttl,
        response_root=None,
        body_encoding="json",
        query_template=cypher,
        response_normalizer="neo4j_tabular",
    )


def infer_columns(sample_rows: list[dict]) -> list[ApiColumn]:
    """Infer column definitions from a list of sample rows.

    Types are guessed from Python value types; unknown types default to string.
    """
    if not sample_rows:
        return []
    fields = list(sample_rows[0].keys())
    columns: list[ApiColumn] = []
    for field in fields:
        # Sample the first non-None value to guess type
        sample_value = next(
            (row[field] for row in sample_rows if row.get(field) is not None), None
        )
        if isinstance(sample_value, bool):
            col_type = ApiColumnType.boolean
        elif isinstance(sample_value, int):
            col_type = ApiColumnType.integer
        elif isinstance(sample_value, float):
            col_type = ApiColumnType.number
        elif isinstance(sample_value, (dict, list)):
            col_type = ApiColumnType.jsonb
        else:
            col_type = ApiColumnType.string
        columns.append(ApiColumn(name=field, type=col_type))
    return columns
