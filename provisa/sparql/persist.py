# Copyright (c) 2026 Kenneth Stott
# Canary: 8c60231a-1e1d-4609-8564-389379cd7511
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Persist a SPARQL source and its query-backed tables (REQ-1683) — the SPARQL half of the
query-API persistence ``provisa/api_source/persist.py`` provides, mirroring ``provisa/neo4j/persist``.

A SPARQL source is one endpoint URL: the Sources form stores it in the source's ``host`` field
(``sparql-endpoint-input``), and a config source carries it the same way.
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
    from provisa.sparql.source import SparqlSourceConfig


def sparql_config_from_source(
    *, source_id: str, endpoint_url: str
) -> "tuple[SparqlSourceConfig, ApiSource]":  # REQ-1683
    """The endpoint config and api_sources record for a ``sparql`` Source row."""
    from provisa.sparql.source import SparqlSourceConfig, build_api_source

    cfg = SparqlSourceConfig(source_id=source_id, endpoint_url=endpoint_url)
    return cfg, build_api_source(cfg)


async def persist_sparql_table(  # REQ-1683
    conn: "Connection",
    *,
    source_id: str,
    endpoint_url: str,
    table_name: str,
    query_template: str,
    columns: list,
    ttl: int,
) -> "tuple[ApiSource, ApiEndpoint]":
    """Persist one SPARQL-backed table: its source's api_sources row and its api_endpoints row.
    Returns the records so the caller can mirror them into live state."""
    from provisa.sparql.source import build_endpoint

    cfg, api_source = sparql_config_from_source(source_id=source_id, endpoint_url=endpoint_url)
    endpoint = build_endpoint(
        cfg, table_name, query_template, api_columns_from_config(columns), ttl
    )
    await persist_api_source(conn, api_source)
    await persist_api_endpoint(conn, endpoint)
    return api_source, endpoint
