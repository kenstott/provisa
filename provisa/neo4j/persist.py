# Copyright (c) 2026 Kenneth Stott
# Canary: 4d1f0c2e-9b7a-4c1e-8f3d-2a6b5e7c9d10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Persist a Neo4j source and its Cypher-backed tables to the control plane (REQ-1668).

One write path for both registration surfaces — a ``neo4j`` source in the config file
(``config_loader._handle_neo4j_table``) and the admin REST router (``neo4j_router``) — so a
table registered either way is the same ``api_sources`` + ``api_endpoints`` rows, hydrated at
startup by ``api_source.loader.load_api_sources`` like every other API-backed table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.api_source.models import ApiEndpoint, ApiSource
from provisa.api_source.persist import (
    api_columns_from_config,
    persist_api_endpoint,
    persist_api_source,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.neo4j.source import Neo4jSourceConfig


def neo4j_config_from_source(
    *, source_id: str, host: str, port: int, database: str, base_url: str | None
) -> "tuple[Neo4jSourceConfig, ApiSource]":  # REQ-1668
    """The connection config and api_sources record for a ``neo4j`` Source row. ``base_url``
    (when the row carries one) is the endpoint verbatim; otherwise ``http://host:port``."""
    from provisa.neo4j.source import Neo4jSourceConfig, build_api_source

    cfg = Neo4jSourceConfig(
        source_id=source_id,
        host=host,
        port=port,
        database=database,
        use_https=(base_url or "").startswith("https://"),
    )
    api_source = build_api_source(cfg)
    if base_url:
        api_source = api_source.model_copy(update={"base_url": base_url})
    return cfg, api_source


async def persist_neo4j_table(  # REQ-1668
    conn: "Connection",
    *,
    source_id: str,
    host: str,
    port: int,
    database: str,
    base_url: str | None,
    table_name: str,
    query_template: str,
    columns: list,
    ttl: int,
) -> "tuple[ApiSource, ApiEndpoint]":
    """Persist one Cypher-backed table: its source's api_sources row and its api_endpoints row.
    The ONE write both registration surfaces (config load, the registerTable mutation) call.
    Returns the records so the caller can mirror them into live state."""
    from provisa.neo4j.source import build_endpoint

    cfg, api_source = neo4j_config_from_source(
        source_id=source_id, host=host, port=port, database=database, base_url=base_url
    )
    endpoint = build_endpoint(
        cfg, table_name, query_template, api_columns_from_config(columns), ttl
    )
    await persist_api_source(conn, api_source)
    await persist_api_endpoint(conn, endpoint)
    return api_source, endpoint
