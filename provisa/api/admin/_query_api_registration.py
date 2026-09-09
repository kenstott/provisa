# Copyright (c) 2026 Kenneth Stott
# Canary: ad7cb300-5a7f-4937-a662-ec8ac7f97d54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table on a query-API source (REQ-1670 neo4j, REQ-1683 sparql): the per-type preview
behind the form and the endpoint persistence the ``registerTable``/``updateTable`` mutations run.

A query-API source has no tables to list — its table IS a query projection — so the form previews
the query (rows + inferred column types), the operator names the table, and registration writes
the same ``api_sources``/``api_endpoints`` rows config load writes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import sources

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult, QueryPreviewType
    from provisa.core.database import Connection
    from provisa.core.models import Table

QUERY_API_TYPES = frozenset({"neo4j", "sparql"})


async def source_row(conn: "Connection", source_id: str) -> dict[str, Any] | None:
    res = await conn.execute_core(
        select(
            sources.c.id, sources.c.type, sources.c.host, sources.c.port, sources.c.database
        ).where(sources.c.id == source_id)
    )
    row = res.fetchone()
    return dict(row._mapping) if row is not None else None


async def preview_sparql(conn: "Connection", source_id: str, query: str) -> "QueryPreviewType":
    """Run the SPARQL SELECT (LIMIT 5) against the source's endpoint and infer the columns —
    every SPARQL binding is a string, so the columns are ``text``."""
    import httpx

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.sparql.persist import sparql_config_from_source
    from provisa.sparql.source import extract_variables, probe_endpoint

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "sparql":
        return QueryPreviewType(error=f"{source_id!r} is not a registered sparql source")
    cfg, _ = sparql_config_from_source(source_id=source_id, endpoint_url=row["host"])
    try:
        rows = await probe_endpoint(cfg, query)
    except (httpx.HTTPError, ValueError) as exc:
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    names = list(rows[0].keys()) if rows else extract_variables(query)
    if not names:
        return QueryPreviewType(
            error="The query returned no rows and names no SELECT variables; columns cannot be inferred."
        )
    return QueryPreviewType(
        rows=list(rows),
        columns=[QueryPreviewColumnType(name=n, data_type="text") for n in names],
    )


async def persist_query_api_registration(
    conn: "Connection", model: "Table"
) -> "MutationResult | None":
    """After the registered_tables row lands: refuse a query-API table with no query, else persist
    its endpoint and mirror it into the live endpoint map the schema builder and event loop read.
    No-op for every other source type."""
    from provisa.api.admin.types import MutationResult
    from provisa.api.app import state

    row = await source_row(conn, model.source_id)
    if row is None or row["type"] not in QUERY_API_TYPES:
        return None
    if not model.query_template:
        return MutationResult(
            success=False,
            message=f"A table on a {row['type']} source requires the query that produces its rows.",
            code="schema.query_template_required",
            params={"table": model.table_name, "source_type": row["type"]},
        )
    if row["type"] == "neo4j":
        from provisa.neo4j.persist import persist_neo4j_table

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
    else:
        from provisa.sparql.persist import persist_sparql_table

        api_source, endpoint = await persist_sparql_table(
            conn,
            source_id=model.source_id,
            endpoint_url=row["host"],
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
