# Copyright (c) 2026 Kenneth Stott
# Canary: dbd213fd-531e-44d6-941b-179405293d2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for GraphQL Remote Schema Connector (Phase AP).

Endpoints:
  POST /admin/sources/graphql-remote          — register source (introspect + auto-register)
  POST /admin/sources/graphql-remote/{id}/refresh — re-introspect, update registrations
"""
from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/graphql-remote", tags=["admin", "graphql-remote"])


class GraphQLRemoteSourceRequest(BaseModel):
    source_id: str
    url: str
    namespace: str
    domain_id: str = ""
    auth: dict | None = None
    cache_ttl: int = 300


class GraphQLRemoteRegistration(BaseModel):
    source_id: str
    url: str
    namespace: str
    domain_id: str = ""
    auth: dict | None = None
    cache_ttl: int = 300
    tables: list[dict] = []
    functions: list[dict] = []
    relationships: list[dict] = []


async def _introspect_and_map(
    source_id: str,
    url: str,
    namespace: str,
    domain_id: str,
    auth: dict | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    from provisa.graphql_remote.introspect import introspect_schema
    from provisa.graphql_remote.mapper import map_schema
    schema = await introspect_schema(url, auth)
    tables, functions, relationships = map_schema(schema, namespace, source_id, domain_id)
    return tables, functions, relationships


def _build_object_fields(raw_fields: list) -> list:
    """Recursively build ObjectField instances from structured gql_object_fields dicts."""
    from provisa.core.models import ObjectField
    result = []
    for f in (raw_fields or []):
        if isinstance(f, str):
            result.append(ObjectField(name=f, type="string"))
        else:
            result.append(ObjectField(
                name=f["name"],
                type=f.get("type", "string"),
                fields=_build_object_fields(f.get("fields") or []),
            ))
    return result


async def _upsert_tables_to_semantic_layer(
    source_id: str,
    domain_id: str,
    tables: list[dict],
    pg_pool,
) -> None:
    """Write discovered GraphQL tables into registered_tables with descriptions."""
    from provisa.core.models import Column, GovernanceLevel, Table
    from provisa.core.repositories import table as table_repo
    from provisa.api.admin.actions_router import _ensure_tables

    await _ensure_tables(pg_pool)
    async with pg_pool.acquire() as conn:
        for t in tables:
            tbl = Table(
                source_id=source_id,
                domain_id=domain_id or "",
                schema_name="graphql_remote",
                table_name=t["name"],
                governance=GovernanceLevel.pre_approved,
                description=t.get("description"),
                columns=[
                    Column(
                        name=c["name"],
                        visible_to=[],
                        description=c.get("description"),
                        object_fields=_build_object_fields(c.get("gql_object_fields") or []),
                    )
                    for c in t.get("columns", [])
                ],
            )
            await table_repo.upsert(conn, tbl)


async def _upsert_relationships_to_semantic_layer(
    relationships: list[dict],
    pg_pool,
    state=None,
) -> None:
    """Upsert detected intra-source relationships, then retry any config relationships deferred at startup."""
    from provisa.core.models import Cardinality, Relationship
    from provisa.core.repositories import relationship as rel_repo
    async with pg_pool.acquire() as conn:
        for r in (relationships or []):
            try:
                await rel_repo.upsert(conn, Relationship(
                    id=r["id"],
                    source_table_id=r["source_table_id"],
                    target_table_id=r["target_table_id"],
                    source_column=r["source_column"],
                    target_column=r["target_column"],
                    cardinality=Cardinality(r.get("cardinality", "many-to-one")),
                    source_json_key=r.get("source_json_key") or None,
                ))
            except Exception:
                log.warning("Failed to upsert relationship %s", r["id"], exc_info=True)
        # Retry config relationships deferred at startup (tables may now exist)
        cfg = getattr(state, "config", None) if state is not None else None
        if cfg is not None:
            for rel in cfg.relationships:
                try:
                    await rel_repo.upsert(conn, rel)
                except ValueError:
                    pass


@router.post("")
async def register_graphql_remote_source(
    body: GraphQLRemoteSourceRequest,
    request: Request,
):
    """Register a GraphQL remote source: introspect schema and auto-register tables/functions."""
    state = request.app.state

    try:
        tables, functions, relationships = await _introspect_and_map(
            body.source_id, body.url, body.namespace, body.domain_id, body.auth,
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Introspection failed: {exc}") from exc

    registration = GraphQLRemoteRegistration(
        source_id=body.source_id,
        url=body.url,
        namespace=body.namespace,
        domain_id=body.domain_id,
        auth=body.auth,
        cache_ttl=body.cache_ttl,
        tables=tables,
        functions=functions,
        relationships=relationships,
    )

    if not hasattr(state, "graphql_remote_sources"):
        state.graphql_remote_sources = {}
    state.graphql_remote_sources[body.source_id] = registration.model_dump()

    if getattr(state, "pg_pool", None) is not None:
        async with state.pg_pool.acquire() as _conn:
            await _conn.execute(
                """
                INSERT INTO sources (id, type, host, port, database, username, dialect, path)
                VALUES ($1, 'graphql_remote', '', 0, '', '', '', $2)
                ON CONFLICT (id) DO UPDATE SET path = EXCLUDED.path
                """,
                body.source_id,
                body.url,
            )
            if body.domain_id:
                await _conn.execute(
                    "INSERT INTO domains (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
                    body.domain_id,
                )
        await _upsert_tables_to_semantic_layer(
            body.source_id, body.domain_id, tables, state.pg_pool,
        )
        await _upsert_relationships_to_semantic_layer(relationships, state.pg_pool, state)
        try:
            from provisa.api.app import _rebuild_schemas
            await _rebuild_schemas()
        except Exception:
            log.warning("Schema rebuild failed after graphql-remote registration", exc_info=True)

    log.info(
        "Registered GraphQL remote source %s (%d tables, %d functions, %d relationships)",
        body.source_id, len(tables), len(functions), len(relationships),
    )
    return {
        "source_id": body.source_id,
        "tables": len(tables),
        "functions": len(functions),
        "relationships": len(relationships),
        "table_names": [t["name"] for t in tables],
        "function_names": [f["name"] for f in functions],
    }


@router.post("/{source_id}/refresh")
async def refresh_graphql_remote_source(source_id: str, request: Request):
    """Re-introspect a registered remote source and update its table/function registrations."""
    state = request.app.state
    sources = getattr(state, "graphql_remote_sources", {})
    if source_id not in sources:
        raise HTTPException(status_code=404, detail=f"GraphQL remote source {source_id!r} not found")

    reg = sources[source_id]
    try:
        tables, functions, relationships = await _introspect_and_map(
            source_id, reg["url"], reg["namespace"], reg.get("domain_id", ""), reg.get("auth"),
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Re-introspection failed: {exc}") from exc

    reg["tables"] = tables
    reg["functions"] = functions
    reg["relationships"] = relationships
    state.graphql_remote_sources[source_id] = reg

    if getattr(state, "pg_pool", None) is not None:
        await _upsert_tables_to_semantic_layer(
            source_id, reg.get("domain_id", ""), tables, state.pg_pool,
        )
        await _upsert_relationships_to_semantic_layer(relationships, state.pg_pool, state)
        try:
            from provisa.api.app import _rebuild_schemas
            await _rebuild_schemas()
        except Exception:
            log.warning("Schema rebuild failed after graphql-remote refresh", exc_info=True)

    log.info("Refreshed GraphQL remote source %s (%d relationships)", source_id, len(relationships))
    return {
        "source_id": source_id,
        "tables": len(tables),
        "functions": len(functions),
        "relationships": len(relationships),
        "table_names": [t["name"] for t in tables],
        "function_names": [f["name"] for f in functions],
    }


@router.get("")
async def list_graphql_remote_sources(request: Request):
    """List all registered GraphQL remote sources."""
    state = request.app.state
    sources = getattr(state, "graphql_remote_sources", {})
    return list(sources.values())
