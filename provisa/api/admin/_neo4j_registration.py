# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2d8f31-4c7e-4b9a-9d5e-1f3b7c9a2e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table on a neo4j source (REQ-1670): the Cypher preview behind the form and the
endpoint persistence the ``registerTable`` mutation runs after the table row lands.

A neo4j source has no tables to list — its table IS a Cypher projection — so the form previews
the Cypher (rows + inferred column types), the operator names the table, and registration writes
the same ``api_sources``/``api_endpoints`` rows config load writes (REQ-1668).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import sources

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult, Neo4jPreviewType
    from provisa.core.database import Connection
    from provisa.core.models import Table


async def _source_row(conn: "Connection", source_id: str) -> dict[str, Any] | None:
    res = await conn.execute_core(
        select(
            sources.c.id, sources.c.type, sources.c.host, sources.c.port, sources.c.database
        ).where(sources.c.id == source_id)
    )
    row = res.fetchone()
    return dict(row._mapping) if row is not None else None


async def preview_neo4j(conn: "Connection", source_id: str, cypher: str) -> "Neo4jPreviewType":
    """Run the Cypher (LIMIT 5) against the source and infer the registered column types."""
    import httpx

    from provisa.api.admin.types import Neo4jPreviewColumnType, Neo4jPreviewType
    from provisa.neo4j.persist import data_type_for_api_column, neo4j_config_from_source
    from provisa.neo4j.preview import Neo4jNodeObjectError, preview_query, validate_shape
    from provisa.neo4j.source import infer_columns

    row = await _source_row(conn, source_id)
    if row is None or row["type"] != "neo4j":
        return Neo4jPreviewType(error=f"{source_id!r} is not a registered neo4j source")
    cfg, api_source = neo4j_config_from_source(
        source_id=source_id,
        host=row["host"],
        port=row["port"],
        database=row["database"],
        base_url=None,
    )
    try:
        rows = await preview_query(
            base_url=api_source.base_url, database=cfg.database, cypher=cypher
        )
        validate_shape(rows)
    except Neo4jNodeObjectError as exc:
        return Neo4jPreviewType(error=str(exc))
    except (httpx.HTTPError, ValueError) as exc:
        return Neo4jPreviewType(error=f"{type(exc).__name__}: {exc}")
    columns = [
        Neo4jPreviewColumnType(name=c.name, data_type=data_type_for_api_column(c.type))
        for c in infer_columns(rows)
    ]
    return Neo4jPreviewType(rows=list(rows), columns=columns)


async def persist_neo4j_registration(conn: "Connection", model: "Table") -> "MutationResult | None":
    """After the registered_tables row lands: refuse a neo4j table with no Cypher, else persist its
    endpoint and mirror it into the live endpoint map the schema builder and event loop read."""
    from provisa.api.admin.types import MutationResult
    from provisa.api.app import state
    from provisa.neo4j.persist import persist_neo4j_table

    row = await _source_row(conn, model.source_id)
    if row is None or row["type"] != "neo4j":
        return None
    if not model.query_template:
        return MutationResult(
            success=False,
            message="A table on a neo4j source requires the Cypher that produces its rows.",
            code="schema.neo4j_query_required",
            params={"table": model.table_name},
        )
    api_source, endpoint = await persist_neo4j_table(
        conn,
        source_id=model.source_id,
        host=row["host"],
        port=row["port"],
        database=row["database"],
        base_url=None,
        table_name=model.table_name,
        query_template=model.query_template,
        columns=model.columns,
        ttl=model.cache_ttl or 300,
    )
    if not isinstance(getattr(state, "api_sources", None), dict):
        state.api_sources = {}
    if not isinstance(getattr(state, "api_endpoints", None), dict):
        state.api_endpoints = {}
    state.api_sources[api_source.id] = api_source
    state.api_endpoints[endpoint.table_name] = endpoint
    return None
