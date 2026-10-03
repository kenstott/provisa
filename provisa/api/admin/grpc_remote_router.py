# Copyright (c) 2026 Kenneth Stott
# Canary: bab62a3e-25cb-4c13-b423-a21a13bf2c52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for gRPC Remote Schema Connector (Phase AR).

Endpoints:
  POST /admin/grpc-remote/register         — compile stubs, add the source; no table is registered
                                             (REQ-322: each query method is a table on offer)
  POST /admin/grpc-remote/refresh/{id}     — re-compile, bring the registered tables up to date
  GET  /admin/grpc-remote/list             — list registered gRPC sources
  GET  /admin/grpc-remote/{id}/proto       — return stored proto text
  PUT  /admin/grpc-remote/{id}/proto       — store new proto text + re-register
"""

# Requirements: REQ-322, REQ-323, REQ-324, REQ-325, REQ-326, REQ-327, REQ-328, REQ-329, REQ-598, REQ-599

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.core.schema_org import domains, sources
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.schema_common import remote_source_counts
from provisa.grpc_remote.mapper import query_table_name

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/grpc-remote", tags=["admin", "grpc-remote"])


class GrpcRemoteRegisterRequest(BaseModel):
    source_id: str
    proto_path: str  # local path or http/https URL to .proto file
    server_address: str  # host:port
    namespace: str = ""
    domain_id: str = ""
    import_paths: list[str] = []
    tls: bool = False
    auth_config: dict | None = None
    cache_ttl: int = 300
    method_overrides: dict[str, str] = {}  # {"MethodName": "query" | "mutation"}
    relationships: list[dict] = []


async def _load_and_register(  # REQ-322, REQ-323, REQ-324, REQ-325, REQ-326, REQ-327, REQ-329
    source_id: str,
    proto_path: str,
    server_address: str,
    namespace: str,
    domain_id: str,
    import_paths: list[str],
    tls: bool,
    auth_config: dict | None,
    cache_ttl: int,
    state,
    method_overrides: dict[str, str] | None = None,
    relationships: list[dict] | None = None,
) -> tuple[str, dict[str, int]]:
    """Load proto, compile stubs, open channel, register tables/functions.

    Returns the proto text and what the source registered and offers (:func:`remote_source_counts`).
    """
    from provisa.grpc_remote.loader import load_proto, compile_proto_stubs
    from provisa.grpc_remote.mapper import map_proto
    from provisa.grpc_remote.executor import load_stubs

    proto_dict = await load_proto(proto_path, import_paths=import_paths or None)

    # Re-read raw text for storage
    if proto_path.startswith("http://") or proto_path.startswith("https://"):
        import httpx

        r = httpx.get(proto_path, timeout=30, follow_redirects=True)
        r.raise_for_status()
        proto_text = r.text
    else:
        from pathlib import Path

        proto_text = Path(proto_path).read_text()

    # REQ-1742 gap: this used to name the compiled stub package after state.catalog_for(source_id)
    # — the engine's PHYSICAL catalog name, which only resolves once the source is fully
    # registered (state.source_catalogs is populated by a schema rebuild, itself triggered by the
    # sources-table upsert below). proto_name is only ever used as a unique Python package name
    # for the generated stubs (compile_proto_stubs defaults it to the literal "remote"), not a
    # real catalog identifier, so calling catalog_for() here was an unnecessary, premature
    # dependency that made every first-time registration fail before the source existed at all.
    # A sanitized source_id is just as unique and needs nothing to exist first.
    proto_pkg_name = re.sub(r"\W|^(?=\d)", "_", source_id)
    pb2_path, pb2_grpc_path = compile_proto_stubs(
        proto_text,
        proto_name=proto_pkg_name,
        import_paths=import_paths or None,
    )
    pb2, _ = load_stubs(pb2_path, pb2_grpc_path)

    queries, mutations = map_proto(proto_dict, namespace, source_id, domain_id, method_overrides)

    if state.tenant_db is None:
        raise ApiError(503, "grpc_remote.database_not_connected", "Database not connected")

    # REQ-1742: self-register the `sources` row here, the same pattern
    # register_graphql_remote_source (graphql_remote_router.py) already uses — a generic
    # createSource call from the UI would need to send SourceType.grpc_remote, but the Sources
    # form's own internal type string for this connector is "grpc" (constants.ts), which is not a
    # valid SourceType; the backend registering itself sidesteps that mismatch entirely, matching
    # graphql_remote's own working design rather than requiring the frontend to reconcile two
    # different vocabularies.
    async with state.tenant_db.acquire() as conn:
        await conn.upsert(
            sources,
            {
                "id": source_id,
                "type": "grpc_remote",
                "host": server_address,
                "port": 0,
                "database": "",
                "username": "",
                "dialect": "",
                # REQ-1730: proto_path/namespace/tls/import_paths persisted here (path +
                # federation_hints — the same pattern exasol's tls_fingerprint and graphql_remote's
                # own path/url use) so _load_grpc_remote_sources_from_db (app_loaders.py) can
                # recompile stubs and reopen the channel on a process that never itself handled this
                # POST — a fresh worker, a restart, or another engine's backend in the REQ-1730
                # swap harness. Without this, `state.grpc_remote_sources` — pure in-memory, built
                # only by THIS handler — stays empty on any other process, and every query against
                # the table silently finds no registration to read from (verified live: a query
                # hung waiting on results that never landed, no error surfaced anywhere).
                "path": proto_path,
                "federation_hints": {
                    "namespace": namespace,
                    "tls": "true" if tls else "false",
                    "import_paths": ",".join(import_paths or []),
                    "cache_ttl": str(cache_ttl),
                },
                "description": "",
            },
            index_elements=["id"],
            update_columns=["host", "description", "path", "federation_hints"],
        )
        if domain_id:
            await conn.upsert(domains, {"id": domain_id}, index_elements=["id"], update_columns=[])

    # REQ-322 (amended 2026-10-02): adding or refreshing the source registers no table. The
    # tables already registered are brought up to date with the proto; every other query method
    # is on offer to the Register Table picker.
    async with state.tenant_db.acquire() as conn:
        n_tables = await _register_schema(
            source_id,
            queries,
            conn,
            namespace,
            domain_id,
            registered=await registered_query_tables(conn, source_id),
        )

    # Open gRPC channel and populate state.grpc_remote_sources BEFORE the rebuild/reconcile below
    # (REQ-1730: moved ahead of _rebuild_schemas, was after it). _load_grpc_remote_sources_from_db
    # (app_loaders.py) — wired into _rebuild_schemas so a process that never itself handled this
    # POST can reconstruct this same state on reload — guards against re-entry with
    # `if source_id in state.grpc_remote_sources: continue`. With the channel/state populated only
    # AFTER _rebuild_schemas() (the original order), that guard could never see this registration
    # yet, since _register_schema's INSERTs (just above) already made `sources`/`registered_tables`
    # visible to that reload query before this source_id ever reached state.grpc_remote_sources —
    # so _rebuild_schemas() recursed straight back into _load_and_register for the SAME source_id
    # it was still in the middle of registering (verified live: registration hung past 60s, no
    # error, no row ever listed).
    # No channel is opened here: a channel belongs to the event loop it is opened on, and each
    # request that calls the source opens its own on its own loop (executor.channel_for).
    if not hasattr(state, "grpc_remote_sources"):
        state.grpc_remote_sources = {}

    state.grpc_remote_sources[source_id] = {
        "proto_path": proto_path,
        "proto_text": proto_text,
        "server_address": server_address,
        "namespace": namespace,
        "domain_id": domain_id,
        "import_paths": import_paths,
        "tls": tls,
        "auth_config": auth_config,
        "cache_ttl": cache_ttl,
        "method_overrides": method_overrides or {},
        "relationships": relationships or [],
        "pb2_path": pb2_path,
        "pb2_grpc_path": pb2_grpc_path,
        "pb2": pb2,
        "queries": queries,
        "mutations": mutations,
    }

    # REQ-1742 gap: this used to call _rebuild_schemas() BEFORE _register_schema — but
    # graphql_remote_router.py's own working order (which this now mirrors exactly) rebuilds
    # AFTER its table registration, not before. Rebuilding first left state.tables/
    # registered_tables reflecting a snapshot from before _register_schema's INSERTs, so the
    # reconcile below (which reads state.tables) saw a source with a sources-table row but no
    # tables yet, and never created the landing view — a query against the table still failed
    # "no such table" even though _register_schema itself had already succeeded.
    from provisa.api.app import _rebuild_schemas

    try:
        await _rebuild_schemas()
    except Exception:
        log.warning("Schema rebuild failed after grpc-remote registration", exc_info=True)

    # REQ-1729/REQ-1742: same gap graphql_remote_router.py already fixed for itself — grpc_remote
    # has no live connector (not in the pgwire-replica _OPERAND_BUILDERS set), so its tables are
    # MATERIALIZED-only; this router upserts registered_tables/table_columns rows directly via
    # _register_schema above and never reconciled, so a query against a freshly grpc-remote-
    # registered table failed "no such table: grpc_remote.<table>" — the landing schema/view had
    # never been created in the engine catalog.
    try:
        await state.federation_engine.reconcile_landed_tables()
    except Exception:
        log.exception("landed-table reconcile after grpc-remote registration failed")

    if relationships:
        from provisa.api.admin.graphql_remote_router import _upsert_relationships_to_semantic_layer

        await _upsert_relationships_to_semantic_layer(relationships, state.tenant_db, state)

    return proto_text, remote_source_counts(n_tables, len(queries), len(mutations))


def query_columns(query) -> tuple[list, list]:
    """A query method's columns as they are stored: its response fields, and one native-filter
    column per request field (REQ-1426: each carries the type the proto resolves it to)."""
    from provisa.core.models import Column, ObjectField

    def _object_fields(defs):
        return [
            ObjectField(name=d.name, type=d.type, fields=_object_fields(d.object_fields))
            for d in defs
        ]

    output_cols = [
        Column(
            name=c.name,
            visible_to=[],
            data_type=c.type,
            object_fields=_object_fields(c.object_fields),
        )
        for c in query.columns
    ]
    nf_cols = [
        Column(
            name=f"_nf_{c.name}",
            visible_to=[],
            data_type=c.type,
            native_filter_type="grpc_input",
        )
        for c in query.input_fields
    ]
    return output_cols, nf_cols


async def record_query_registration(
    conn, source_id: str, query, namespace: str, domain_id: str
) -> None:
    """Note that a query method's table is registered (the remote-registration record the
    model store's integrity rules follow)."""
    col_defs = ", ".join(f"{c.name} {c.type}" for c in query.columns) or "result jsonb"
    await conn.execute(
        """
        INSERT INTO provisa_sources (source_id, source_type, table_name, column_defs,
                                     namespace, domain_id, extra)
        VALUES ($1, 'grpc_remote', $2, $3, $4, $5, $6)
        ON CONFLICT (source_id, table_name) DO UPDATE
          SET column_defs = EXCLUDED.column_defs,
              namespace   = EXCLUDED.namespace,
              domain_id   = EXCLUDED.domain_id,
              extra       = EXCLUDED.extra
        """,
        source_id,
        query_table_name(namespace, query),
        col_defs,
        namespace,
        domain_id,
        f"grpc_query:{query.full_method_path}",
    )


async def registered_query_tables(conn, source_id: str) -> dict[str, tuple[str, set[str]]]:
    """The source's registered tables: each one's domain and the columns it is registered with."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables, table_columns

    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.table_name,
                registered_tables.c.domain_id,
                table_columns.c.column_name,
            )
            .select_from(
                registered_tables.join(
                    table_columns, table_columns.c.table_id == registered_tables.c.id
                )
            )
            .where(
                registered_tables.c.source_id == source_id,
                registered_tables.c.schema_name == "grpc_remote",
            )
        )
    ).fetchall()
    registered: dict[str, tuple[str, set[str]]] = {}
    for row in rows:
        registered.setdefault(row.table_name, (row.domain_id or "", set()))[1].add(row.column_name)
    return registered


async def _register_schema(  # REQ-322, REQ-325, REQ-599
    source_id: str,
    queries,
    conn,
    namespace: str,
    domain_id: str,
    registered: dict[str, tuple[str, set[str]]] | None = None,
) -> int:
    """Bring the source's registered tables up to date with its proto; return how many.

    ``registered`` is the source's registered tables (:func:`registered_query_tables`): only
    those are written, each in the domain it was registered into and with the columns it was
    registered with -- a query method that is not registered is on offer and stays unregistered
    (REQ-322). None writes a table for every query method, which is what a caller that states
    the whole set (a test of the stored shape) asks for.
    """
    from provisa.core.models import Table
    from provisa.core.repositories import table as table_repo

    written = 0
    for q in queries:
        table_name = query_table_name(namespace, q)
        table_domain, kept = domain_id, None
        if registered is not None:
            if table_name not in registered:
                continue
            table_domain, kept = registered[table_name]
        await record_query_registration(conn, source_id, q, namespace, table_domain)
        output_cols, nf_cols = query_columns(q)
        if kept is not None:
            output_cols = [c for c in output_cols if c.name in kept]
        tbl = Table(
            source_id=source_id,
            domain_id=table_domain or "",
            schema_name="grpc_remote",
            table_name=table_name,
            columns=output_cols + nf_cols,
        )
        await table_repo.upsert(conn, tbl)
        written += 1

    return written


@router.post("/register")
async def register_grpc_remote_source(
    request: Request,
    body: GrpcRemoteRegisterRequest,
):  # REQ-322, REQ-323, REQ-324, REQ-325, REQ-326, REQ-598
    """Compile proto stubs and add the source. Its query methods are tables on offer; none is
    registered here (REQ-322)."""
    require_capability_request(request, "source_registration")
    # REQ-1742: this handler used `request.app.state` (Starlette's per-request state, a bare
    # object with none of provisa's attributes) instead of provisa's own app-state singleton —
    # every real call raised AttributeError ('State' object has no attribute 'catalog_for')
    # before it could compile a single proto. Every OTHER admin router (graphql_remote_router.py,
    # the sibling this one's own docstring points to) imports the singleton directly; this file
    # was the one place that never worked at all.
    from provisa.api.app import state

    try:
        _, counts = await _load_and_register(
            body.source_id,
            body.proto_path,
            body.server_address,
            body.namespace,
            body.domain_id,
            body.import_paths,
            body.tls,
            body.auth_config,
            body.cache_ttl,
            state,
            body.method_overrides or None,
            body.relationships or None,
        )
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise ApiError(
            422, "grpc_remote.registration_failed", f"Registration failed: {exc}", error=str(exc)
        ) from exc

    log.info("Added gRPC remote source %s (%s)", body.source_id, counts)
    return {"source_id": body.source_id, **counts}


@router.post("/refresh/{source_id}")
async def refresh_grpc_remote_source(request: Request, source_id: str):  # REQ-329
    """Re-compile proto stubs from stored path and re-run registration."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state  # REQ-1742: see register_grpc_remote_source

    sources = getattr(state, "grpc_remote_sources", {})
    if source_id not in sources:
        raise ApiError(
            404,
            "grpc_remote.source_not_registered",
            f"gRPC source {source_id!r} not registered",
            source_id=source_id,
        )

    reg = sources[source_id]
    try:
        _, counts = await _load_and_register(
            source_id,
            reg["proto_path"],
            reg["server_address"],
            reg.get("namespace", ""),
            reg.get("domain_id", ""),
            reg.get("import_paths", []),
            reg.get("tls", False),
            reg.get("auth_config"),
            reg.get("cache_ttl", 300),
            state,
            reg.get("method_overrides") or None,
            reg.get("relationships") or None,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise ApiError(
            422, "grpc_remote.refresh_failed", f"Refresh failed: {exc}", error=str(exc)
        ) from exc

    log.info("Refreshed gRPC remote source %s (%s)", source_id, counts)
    return {"source_id": source_id, **counts}


@router.get("/list")
async def list_grpc_remote_sources(request: Request):  # REQ-598
    """Return all registered gRPC remote sources (without channel/pb2 objects)."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state  # REQ-1742: see register_grpc_remote_source

    sources = getattr(state, "grpc_remote_sources", {})
    result = []
    for sid, reg in sources.items():
        result.append(
            {
                "source_id": sid,
                "server_address": reg.get("server_address"),
                "proto_path": reg.get("proto_path"),
                "namespace": reg.get("namespace", ""),
                "domain_id": reg.get("domain_id", ""),
                "tls": reg.get("tls", False),
                "import_paths": reg.get("import_paths", []),
                "cache_ttl": reg.get("cache_ttl", 300),
                "auth_config": reg.get("auth_config"),
                "available_tables": len(reg.get("queries", [])),
                "available_mutations": len(reg.get("mutations", [])),
            }
        )
    return result


@router.get("/{source_id}/proto")
async def get_grpc_proto(request: Request, source_id: str):  # REQ-525
    """Return stored proto text for a registered gRPC source."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state  # REQ-1742: see register_grpc_remote_source

    sources = getattr(state, "grpc_remote_sources", {})
    if source_id not in sources:
        raise ApiError(
            404,
            "grpc_remote.source_not_registered",
            f"gRPC source {source_id!r} not registered",
            source_id=source_id,
        )
    return {"source_id": source_id, "proto_text": sources[source_id].get("proto_text", "")}


@router.put("/{source_id}/proto")
async def put_grpc_proto(source_id: str, request: Request):  # REQ-329
    """Store new proto text and re-run registration."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state  # REQ-1742: see register_grpc_remote_source

    sources = getattr(state, "grpc_remote_sources", {})
    if source_id not in sources:
        raise ApiError(
            404,
            "grpc_remote.source_not_registered",
            f"gRPC source {source_id!r} not registered",
            source_id=source_id,
        )

    try:
        body = await request.json()
        proto_text = body["proto_text"]
    except Exception as exc:
        raise ApiError(
            422, "grpc_remote.invalid_request", f"Invalid request: {exc}", error=str(exc)
        ) from exc

    reg = sources[source_id]
    from provisa.grpc_remote.loader import compile_proto_stubs, parse_proto_text
    from provisa.grpc_remote.mapper import map_proto
    from provisa.grpc_remote.executor import load_stubs

    try:
        pb2_path, pb2_grpc_path = compile_proto_stubs(
            proto_text,
            proto_name=state.catalog_for(source_id),
            import_paths=reg.get("import_paths") or None,
        )
        pb2, _ = load_stubs(pb2_path, pb2_grpc_path)
        proto_dict = parse_proto_text(proto_text)
        queries, mutations = map_proto(
            proto_dict,
            reg.get("namespace", ""),
            source_id,
            reg.get("domain_id", ""),
            reg.get("method_overrides") or None,
        )
    except Exception as exc:
        raise ApiError(
            422,
            "grpc_remote.proto_compilation_failed",
            f"Proto compilation failed: {exc}",
            error=str(exc),
        ) from exc

    if state.tenant_db is None:
        raise ApiError(503, "grpc_remote.database_not_connected", "Database not connected")

    async with state.tenant_db.acquire() as conn:
        n_tables = await _register_schema(
            source_id,
            queries,
            conn,
            reg.get("namespace", ""),
            reg.get("domain_id", ""),
            # REQ-322: only the tables already registered are brought up to date.
            registered=await registered_query_tables(conn, source_id),
        )

    sources[source_id].update(
        {
            "proto_text": proto_text,
            "pb2_path": pb2_path,
            "pb2_grpc_path": pb2_grpc_path,
            "pb2": pb2,
            "queries": queries,
            "mutations": mutations,
        }
    )

    counts = remote_source_counts(n_tables, len(queries), len(mutations))
    log.info("Updated proto for gRPC source %s (%s)", source_id, counts)
    return {"source_id": source_id, **counts}
