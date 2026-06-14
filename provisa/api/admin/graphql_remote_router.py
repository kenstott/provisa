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

from fastapi import APIRouter, HTTPException
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
    description: str = ""
    field_overrides: dict[str, str] = {}  # {"fieldName": "query" | "mutation"}
    relationships: list[dict] = []


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
    field_overrides: dict[str, str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    from provisa.api.app import state
    from provisa.graphql_remote.introspect import introspect_schema
    from provisa.graphql_remote.mapper import map_schema

    max_depth = state.config.graphql_remote.max_object_depth
    max_list_depth = state.config.graphql_remote.max_list_depth
    max_list_items = state.config.graphql_remote.max_list_items
    schema = await introspect_schema(url, auth)
    tables, functions, relationships = map_schema(
        schema,
        namespace,
        source_id,
        domain_id,
        max_depth,
        max_list_depth,
        max_list_items,
        field_overrides=field_overrides,
    )
    return tables, functions, relationships


def _build_object_fields(raw_fields: list) -> list:
    """Recursively build ObjectField instances from structured gql_object_fields dicts."""
    from provisa.core.models import ObjectField

    result = []
    for f in raw_fields or []:
        if isinstance(f, str):
            result.append(ObjectField(name=f, type="string"))
        else:
            result.append(
                ObjectField(
                    name=f["name"],
                    type=f.get("type", "string"),
                    fields=_build_object_fields(f.get("fields") or []),
                )
            )
    return result


# Mapper provisa type → Trino type. Stored as table_columns.data_type so the type
# is valid for BOTH the graphql synth path (introspect._GQL_TYPE_MAP keys on these
# Trino names) and the SQL-catalog path used when a view references these columns.
_PROVISA_TO_TRINO_TYPE = {
    "text": "varchar",
    "integer": "integer",
    "numeric": "double",
    "boolean": "boolean",
    "jsonb": "json",
}


async def _upsert_tables_to_semantic_layer(
    source_id: str,
    domain_id: str,
    tables: list[dict],
    pg_pool,
) -> None:
    """Write discovered GraphQL tables into registered_tables with descriptions."""
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo
    from provisa.api.admin.actions_router import _ensure_tables

    await _ensure_tables(pg_pool)
    from provisa.compiler.naming import to_snake_case

    async with pg_pool.acquire() as conn:
        for t in tables:
            _snake = to_snake_case(t["name"])
            tbl = Table(
                source_id=source_id,
                domain_id=domain_id or "",
                schema_name="graphql",
                table_name=t["name"],
                description=t.get("description"),
                alias=_snake if _snake != t["name"] else None,
                columns=[
                    Column(
                        name=c["name"],
                        visible_to=[],
                        description=c.get("description"),
                        data_type=_PROVISA_TO_TRINO_TYPE.get(c.get("type") or "text", "varchar"),
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
        for r in relationships or []:
            try:
                await rel_repo.upsert(
                    conn,
                    Relationship(
                        id=r["id"],
                        source_table_id=r["source_table_id"],
                        target_table_id=r["target_table_id"],
                        source_column=r["source_column"],
                        target_column=r["target_column"],
                        cardinality=Cardinality(r.get("cardinality", "many-to-one")),
                        source_json_key=r.get("source_json_key") or None,
                    ),
                )
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
):
    """Register a GraphQL remote source: introspect schema and auto-register tables/functions."""
    from provisa.api.app import state

    try:
        tables, functions, auto_relationships = await _introspect_and_map(
            body.source_id,
            body.url,
            body.namespace,
            body.domain_id,
            body.auth,
            field_overrides=body.field_overrides or None,
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Introspection failed: {exc}") from exc

    all_relationships = auto_relationships + (body.relationships or [])
    registration = GraphQLRemoteRegistration(
        source_id=body.source_id,
        url=body.url,
        namespace=body.namespace,
        domain_id=body.domain_id,
        auth=body.auth,
        cache_ttl=body.cache_ttl,
        tables=tables,
        functions=functions,
        relationships=all_relationships,
    )

    if not hasattr(state, "graphql_remote_sources"):
        state.graphql_remote_sources = {}
    reg_dict = registration.model_dump()
    reg_dict["field_overrides"] = body.field_overrides or {}
    state.graphql_remote_sources[body.source_id] = reg_dict

    _pg_pool = state.pg_pool
    if _pg_pool is not None:
        async with _pg_pool.acquire() as _conn:
            await _conn.execute(
                """
                INSERT INTO sources (id, type, host, port, database, username, dialect, path, description)
                VALUES ($1, 'graphql_remote', '', 0, '', '', '', $2, $3)
                ON CONFLICT (id) DO UPDATE SET path = EXCLUDED.path, description = EXCLUDED.description
                """,
                body.source_id,
                body.url,
                body.description,
            )
            if body.domain_id:
                await _conn.execute(
                    "INSERT INTO domains (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
                    body.domain_id,
                )
        await _upsert_tables_to_semantic_layer(
            body.source_id,
            body.domain_id,
            tables,
            _pg_pool,
        )
        await _upsert_relationships_to_semantic_layer(all_relationships, _pg_pool, state)
        try:
            from provisa.api.app import _rebuild_schemas

            await _rebuild_schemas()
        except Exception:
            log.warning("Schema rebuild failed after graphql-remote registration", exc_info=True)

    log.info(
        "Registered GraphQL remote source %s (%d tables, %d functions, %d relationships)",
        body.source_id,
        len(tables),
        len(functions),
        len(all_relationships),
    )
    return {
        "source_id": body.source_id,
        "tables": len(tables),
        "functions": len(functions),
        "relationships": len(all_relationships),
        "table_names": [t["name"] for t in tables],
        "function_names": [f["name"] for f in functions],
    }


@router.post("/{source_id}/refresh")
async def refresh_graphql_remote_source(source_id: str):
    """Re-introspect a registered remote source and update its table/function registrations."""
    from provisa.api.app import state
    sources = getattr(state, "graphql_remote_sources", {})
    if source_id not in sources:
        raise HTTPException(
            status_code=404, detail=f"GraphQL remote source {source_id!r} not found"
        )

    reg = sources[source_id]
    try:
        tables, functions, auto_relationships = await _introspect_and_map(
            source_id,
            reg["url"],
            reg["namespace"],
            reg.get("domain_id", ""),
            reg.get("auth"),
            field_overrides=reg.get("field_overrides") or None,
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Re-introspection failed: {exc}") from exc

    manual_rels = [r for r in reg.get("relationships", []) if r.get("remote_managed") is not True]
    all_relationships = auto_relationships + manual_rels
    reg["tables"] = tables
    reg["functions"] = functions
    reg["relationships"] = all_relationships
    state.graphql_remote_sources[source_id] = reg

    _pg_pool = state.pg_pool
    if _pg_pool is not None:
        await _upsert_tables_to_semantic_layer(
            source_id,
            reg.get("domain_id", ""),
            tables,
            _pg_pool,
        )
        await _upsert_relationships_to_semantic_layer(all_relationships, _pg_pool, state)
        try:
            from provisa.api.app import _rebuild_schemas

            await _rebuild_schemas()
        except Exception:
            log.warning("Schema rebuild failed after graphql-remote refresh", exc_info=True)

    log.info(
        "Refreshed GraphQL remote source %s (%d relationships)", source_id, len(all_relationships)
    )
    return {
        "source_id": source_id,
        "tables": len(tables),
        "functions": len(functions),
        "relationships": len(all_relationships),
        "table_names": [t["name"] for t in tables],
        "function_names": [f["name"] for f in functions],
    }


@router.get("")
async def list_graphql_remote_sources():
    """List all registered GraphQL remote sources."""
    from provisa.api.app import state
    sources = getattr(state, "graphql_remote_sources", {})
    return list(sources.values())
