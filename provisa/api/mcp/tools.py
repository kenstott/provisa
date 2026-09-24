# Copyright (c) 2026 Kenneth Stott
# Canary: 68162804-7347-4e29-a794-be8dc266ec09
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MCP tool implementations (REQ-1008, phase 1).

Pure functions over an ``AppState``. The MCP server (server.py) is a thin
FastMCP wrapper that binds these to the shared app state. Kept separate so they
are unit-testable against a fake state with no protocol/transport in the way.

Governance invariant: run_sql / explain_sql route SQL exclusively through
``_govern_and_route`` (the pgwire choke point). No governance decision is made
here — this module only shapes catalog metadata and paginates results.
"""

from __future__ import annotations

import os
import types
from typing import Any

from provisa.api.flight.catalog import CatalogTable, _build_catalog_tables_async
from provisa.kaggle.downloader import KAGGLE_TOKEN_SECRET_NAME  # REQ-1798/1819: one shared name

# Row ceiling for run_sql. An agent context must never absorb an unbounded
# result set, so every run_sql caps rows. Configurable via env; the role's own
# governed row-cap (resolve_row_cap inside _govern_and_route) still applies on
# top of this — this is an additional transport-level ceiling, not a bypass.
_DEFAULT_MAX_ROWS = 1000


def _max_rows() -> int:
    raw = os.environ.get("PROVISA_MCP_MAX_ROWS")
    if raw is None:
        return _DEFAULT_MAX_ROWS
    value = int(raw)
    if value <= 0:
        raise ValueError("PROVISA_MCP_MAX_ROWS must be a positive integer")
    return value


def require_role(role: str, state: Any) -> str:
    """Validate that a non-empty, known role was supplied.

    A role is REQUIRED on every tool call. There is no admin default and no
    silent fallback (CLAUDE.md): a missing role is a hard error, and an unknown
    role raises PermissionError exactly as the SQL pipeline would.
    """
    if not role or not str(role).strip():
        raise ValueError("role is required for every MCP tool call")
    role = str(role).strip()
    if role not in state.contexts:
        raise PermissionError(f"No schema for role {role!r}")
    return role


def _domain_descriptions(state: Any) -> dict[str, str]:
    """semantic schema name -> description from the loaded config domains (best-effort;
    a domain with no description legitimately yields "").

    Keyed by the semantic (SQL-queryable) schema name so it lines up with the semantic
    catalog, not the raw domain id.
    """
    from provisa.compiler.naming import domain_to_sql_name

    config = getattr(state, "config", None)
    domains = getattr(config, "domains", None) or []
    return {domain_to_sql_name(d.id): getattr(d, "description", "") or "" for d in domains}


def _meta_index(state: Any) -> dict[tuple[str, str], Any]:
    """(domain_id, registered table_name) -> TableMeta, unioned across every role context.

    Maps a raw catalog table to the TableMeta needed to derive its semantic (queryable)
    name. The registered name may equal the meta's post-alias ``table_name`` or its
    pre-alias ``original_table_name``, so both are indexed.
    """
    idx: dict[tuple[str, str], Any] = {}
    for ctx in getattr(state, "contexts", {}).values():
        for meta in getattr(ctx, "tables", {}).values():
            domain = getattr(meta, "domain_id", "")
            for name in (getattr(meta, "table_name", ""), getattr(meta, "original_table_name", "")):
                if name:
                    idx.setdefault((domain, name), meta)
    return idx


def _semantic_catalog(
    raw: list[CatalogTable], index: dict[tuple[str, str], Any]
) -> list[CatalogTable]:
    """Rewrite each raw catalog table's schema/table identifiers to the semantic names the
    SQL engine actually accepts — ``domain_to_sql_name`` + ``semantic_table_name``, the same
    naming authority the pgwire/JDBC path uses.

    Without this, raw domain ids (e.g. ``pet-store``) and domain-prefixed field names (e.g.
    ``ps__users``) reach an agent as if they were queryable, so its SQL plans but fails to
    execute ("schema doesn't exist"). Tables absent from every role context (no meta) fall
    back to ``apply_sql_name`` on the registered name — the best available semantic form.
    """
    from dataclasses import replace

    from provisa.compiler.naming import apply_sql_name, domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    out: list[CatalogTable] = []
    for t in raw:
        meta = index.get((t.domain_id, t.table_name))
        schema = domain_to_sql_name(t.domain_id)
        table = semantic_table_name(meta) if meta is not None else apply_sql_name(t.table_name)
        out.append(replace(t, domain_id=schema, table_name=table))
    return out


async def _catalog(state: Any) -> list[CatalogTable]:
    """The virtual catalog (schemas/tables/columns) via the Flight reference builder,
    with every schema/table identifier normalized to its semantic (SQL-queryable) name.

    Calls the ASYNC builder directly, on the caller's own event loop — not the sync
    ``build_catalog_tables`` wrapper (used by the Arrow Flight server, whose RPC handlers run on
    plain threads with no event loop of their own, so it must spin one up). Routing through that
    wrapper here via ``asyncio.to_thread`` created a SECOND event loop and tried to use
    ``state.tenant_db``'s pool — bound to the main loop — from within it, which async DB drivers
    reject (a cross-event-loop connection reuse, not something the driver treats as safe to
    share). Every MCP tool that reaches here (list_schemas, list_tables, search_catalog) was
    failing on that mismatch."""
    if not state.tenant_db:
        return []
    raw = await _build_catalog_tables_async(state)
    return _semantic_catalog(raw, _meta_index(state))


def _find_role_table(ctx: Any, schema: str, table: str) -> Any:
    """The TableMeta in ``ctx`` whose semantic (schema, table) names equal the given pair.

    Callers pass the semantic names the agent received from the catalog, so matching is
    done on the same semantic forms (``domain_to_sql_name`` / ``semantic_table_name``) —
    never on raw domain ids or domain-prefixed field names.
    """
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    for meta in getattr(ctx, "tables", {}).values():
        if domain_to_sql_name(getattr(meta, "domain_id", "")) != schema:
            continue
        if semantic_table_name(meta) == table:
            return meta
    return None


async def list_schemas(state: Any, role: str) -> list[dict]:
    """schema id + description + table count."""
    require_role(role, state)
    tables = await _catalog(state)
    descs = _domain_descriptions(state)
    counts: dict[str, int] = {}
    for t in tables:
        counts[t.domain_id] = counts.get(t.domain_id, 0) + 1
    return [
        {"schema": sid, "description": descs.get(sid, ""), "table_count": counts[sid]}
        for sid in sorted(counts)
    ]


async def list_tables(state: Any, role: str, schema: str) -> list[dict]:
    """table name + description + column count for one schema."""
    require_role(role, state)
    tables = await _catalog(state)
    out = [
        {
            "table": t.table_name,
            "description": t.description,
            "column_count": len(t.columns),
        }
        for t in tables
        if t.domain_id == schema
    ]
    if not out and schema not in {t.domain_id for t in tables}:
        raise ValueError(f"Unknown schema {schema!r}")
    return sorted(out, key=lambda r: r["table"])


def _foreign_keys(state: Any, role: str, schema: str, table: str) -> list[dict]:
    """FKs for (schema, table) from the role's compilation-context joins.

    The join registry is the authoritative relationship source (context.py
    _register_relationship_joins). Many-to-one edges are the FK side. Scoped to
    the caller's role so an agent only sees relationships it may traverse.
    """
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    ctx = state.contexts[role]
    tmeta = _find_role_table(ctx, schema, table)
    if tmeta is None:
        return []
    type_name = getattr(tmeta, "type_name", "")
    fks: list[dict] = []
    for (src_type, _field), jm in getattr(ctx, "joins", {}).items():
        if src_type != type_name or getattr(jm, "cardinality", "") != "many-to-one":
            continue
        target = jm.target
        fks.append(
            {
                "column": jm.source_column,
                "references_schema": domain_to_sql_name(getattr(target, "domain_id", "")),
                "references_table": semantic_table_name(target),
                "references_column": jm.target_column,
            }
        )
    return fks


def _unique_constraints(state: Any, role: str, schema: str, table: str) -> list[dict]:  # REQ-1093
    """Declared UNIQUE constraints for (schema, table) from the role's compilation context.

    ctx.unique_constraints is already filtered to columns visible in the role's projection
    (context.py), so this is inherently role-scoped — an agent only sees keys over columns
    it may read.
    """
    ctx = state.contexts[role]
    tmeta = _find_role_table(ctx, schema, table)
    if tmeta is None:
        return []
    return [
        {"name": name, "columns": cols}
        for name, cols in getattr(ctx, "unique_constraints", {}).get(tmeta.table_id, [])
    ]


async def describe_table(state: Any, role: str, schema: str, table: str) -> dict:
    """columns (name, type, description) + foreign keys + unique constraints for one table."""
    require_role(role, state)
    tables = await _catalog(state)
    match = next(
        (t for t in tables if t.domain_id == schema and t.table_name == table),
        None,
    )
    if match is None:
        raise ValueError(f"Table not found: {schema}.{table}")
    return {
        "schema": schema,
        "table": table,
        "description": match.description,
        "columns": [
            {"name": c.name, "type": c.data_type, "description": c.description}
            for c in match.columns
        ],
        "foreign_keys": _foreign_keys(state, role, schema, table),
        "unique_constraints": _unique_constraints(state, role, schema, table),  # REQ-1093
    }


async def graphql_field_names(state: Any, role: str, schema: str, table: str) -> dict:  # REQ-1847
    """The REAL field names a GraphQL query against /query must use for this table and its
    columns — NOT a guessed transform of describe_table's SQL-plane names. The two planes apply
    DIFFERENT naming conventions and the table name additionally gets a domain-uniqueness prefix
    (e.g. registered table `iris_iris` in domain `shelter` can be GraphQL field `s__irisIris`,
    not `irisIris`) that only the compiled schema itself knows — reproduced live: guessing this
    transform got the domain prefix wrong and the query failed schema validation. ALWAYS call
    this before writing a GraphQL query; never assume camelCase-plus-prefix yourself.

    `schema`/`table` are the same semantic names describe_table/list_tables use."""
    require_role(role, state)
    from provisa.api.admin._graphql_field_name import resolve_graphql_field_name
    from provisa.compiler.naming import apply_gql_name

    ctx = state.contexts[role]
    meta = _find_role_table(ctx, schema, table)
    if meta is None:
        raise ValueError(f"Table not found: {schema}.{table}")
    table_field = resolve_graphql_field_name(
        domain_id=meta.domain_id, schema_name=meta.schema_name, table_name=meta.table_name
    )
    if table_field is None:
        raise ValueError(
            f"{schema}.{table} is not exposed in any role's compiled GraphQL schema right now"
        )
    described = await describe_table(state, role, schema, table)
    return {
        "table_field": table_field,
        "columns": [
            {"name": c["name"], "graphql_name": apply_gql_name(c["name"])}
            for c in described["columns"]
        ],
    }


async def cypher_field_names(state: Any, role: str, schema: str, table: str) -> dict:  # REQ-1848
    """The REAL Cypher node label, id property, and column property names for this table — NOT
    a guessed PascalCase-plus-domain-prefix transform. Reuses the exact same CypherLabelMap the
    real /data/graph-schema endpoint and the Bolt/Cypher execution path build from the compiled
    schema (domain-collision handling included), so guessing is never necessary. ALWAYS call this
    before writing a Cypher query for a table — a guessed label can collide with a different
    table's in another domain and MATCH the wrong node type silently, or simply not exist.

    `schema`/`table` are the same semantic names describe_table/list_tables use."""
    require_role(role, state)
    from provisa.api.rest.cypher_exec import _build_label_map

    ctx = state.contexts[role]
    meta = _find_role_table(ctx, schema, table)
    if meta is None:
        raise ValueError(f"Table not found: {schema}.{table}")
    label_map = _build_label_map(ctx, role, state)
    node = label_map.nodes.get(meta.type_name)
    if node is None:
        raise ValueError(
            f"{schema}.{table} is not exposed in the compiled Cypher graph schema right now"
        )
    return {
        "label": node.label,
        "id_property": node.id_column,
        "columns": [
            {"name": phys, "cypher_property": cyp} for cyp, phys in node.physical_properties.items()
        ],
    }


async def list_native_tables(
    state: Any, role: str, source_id: str, schema_name: str = "public"
) -> list[dict]:  # REQ-1833
    """Real table names introspected DIRECTLY from `source_id`'s own native schema — NOT the
    already-registered governed catalog list_tables/describe_table read, which is empty for a
    source that has no tables registered yet. Use this (and describe_native_table) for a source
    you just created (or any source not yet fully registered) before calling propose_table/
    register_table_now — those need an exact table_name and real columns, and search_catalog/
    list_tables/describe_table can only ever see what's ALREADY registered, so they are the wrong
    tool for a brand-new source and will always come back empty for it, no matter how you search.

    This is the SAME native-introspection path (native_tables/the engine-attach fallback) the
    admin UI's Register Table form calls before ever showing a table picker."""
    require_role(role, state)
    from provisa.api.admin.schema_query import Query

    tables = await Query().available_tables(source_id, schema_name)  # pyright: ignore[reportCallIssue]
    return [{"name": t.name, "comment": t.comment} for t in tables]


async def describe_native_table(
    state: Any, role: str, source_id: str, schema_name: str, table_name: str
) -> list[dict]:  # REQ-1833
    """Real columns (name, data_type) introspected DIRECTLY from one table of `source_id` — the
    SAME resolution register_table's own validation re-checks any proposed columns against
    (_ensure_source_column_types), so a columns list built from this call's real result will
    always be accepted, unlike a guessed one. Call list_native_tables first if the exact
    table_name isn't already known."""
    require_role(role, state)
    from provisa.api.admin.schema_query import resolve_available_columns_metadata

    cols = await resolve_available_columns_metadata(source_id, schema_name, table_name)
    return [{"name": c.name, "data_type": c.data_type, "comment": c.comment} for c in cols]


async def list_glossary_terms(
    state: Any, role: str, request: Any, q: str | None = None, include_deprecated: bool = True
) -> list[dict]:  # REQ-1835
    """The org's glossary terms, optionally filtered by a search string. Each result carries
    `live` (grounded to a real column, or edge-connected to one — see create/update below for
    what 'finalizing' a term actually means) so you can tell an admitted term from a proposed
    (draft) one without a second call."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import _require_glossary_read, _view_scope
    from provisa.core.repositories import glossary as glossary_repo

    _require_glossary_read(request)
    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        return await glossary_repo.list_terms(
            conn, q=q, include_deprecated=include_deprecated, domains=_view_scope(request, None)
        )


async def create_glossary_term(
    state: Any,
    role: str,
    request: Any,
    name: str,
    definition: str | None = None,
    domains: list[str] | None = None,
) -> dict:  # REQ-1835
    """Create a new abstract glossary term (a definition with no physical column yet). It starts
    'proposed' — there is no separate live/finalized flag to flip: a term is 'live' automatically
    once it is grounded, either by a real registered column landing on it (sync happens on table
    registration, outside your control) or by connecting it via add_glossary_term_edge to a term
    that is already live. If the deployment is multi-domain, at least one domain is required."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import _declared_domains, _notify, _require_glossary_rw
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.api.admin._guards import require_active_org_id

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        declared = _declared_domains(request, domains, current=set())
        try:
            term_id = await glossary_repo.create_abstract_term(
                conn, name, definition=definition, domains=declared
            )
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    await _notify(org_id, "glossary term created")
    return {"id": term_id}


async def update_glossary_term(
    state: Any,
    role: str,
    request: Any,
    term_id: int,
    name: str | None = None,
    definition: str | None = None,
    export_excluded: bool | None = None,
    retired: bool | None = None,
) -> dict:  # REQ-1835
    """Rename a term, change its definition, exclude it from exports, or retire it. Only the
    fields you pass are changed. Retiring is the correct way to withdraw a term that no longer
    belongs — retired counts as curated (kept, out of service), unlike deleting it outright."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import (
        _notify,
        _require_glossary_rw,
        _require_term_curatable,
    )
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.api.admin._guards import require_active_org_id

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.tenant_db
    assert pool is not None
    found = False
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        try:
            if name is not None:
                found = await glossary_repo.rename_term(conn, term_id, name)
            if definition is not None:
                found = await glossary_repo.set_definition(conn, term_id, definition)
            if export_excluded is not None:
                found = await glossary_repo.set_export_excluded(conn, term_id, export_excluded)
            if retired is not None:
                found = await glossary_repo.set_retired(conn, term_id, retired)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    if not found:
        raise ValueError(f"glossary term {term_id} not found")
    await _notify(org_id, "glossary term updated")
    return {"ok": True}


async def delete_glossary_term(
    state: Any, role: str, request: Any, term_id: int
) -> dict:  # REQ-1835
    """Permanently delete a glossary term. Irreversible — confirm with the user first, the same
    as any other irreversible action. A term that carries curator work (a definition, a
    relationship, or a named expert) should usually be retired via update_glossary_term instead
    of deleted, so a future column reusing its name doesn't silently get a blank term."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import (
        _notify,
        _require_glossary_rw,
        _require_term_curatable,
    )
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.api.admin._guards import require_active_org_id

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        try:
            deleted = await glossary_repo.delete_term(conn, term_id)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    if not deleted:
        raise ValueError(f"glossary term {term_id} not found")
    await _notify(org_id, "glossary term deleted")
    return {"ok": True}


async def add_glossary_term_edge(
    state: Any, role: str, request: Any, term_id: int, to_term_id: int, rel_type: str
) -> dict:  # REQ-1835
    """Connect two glossary terms with a typed relationship (e.g. 'broader', 'narrower',
    'synonym' — check existing edges via list_glossary_terms/get_term for the vocabulary this
    org already uses). This is also how a proposed (not-yet-grounded) term is finalized without
    waiting for a column to land on it: connect it to an already-live term and it becomes live
    through that edge."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import (
        _notify,
        _require_glossary_rw,
        _require_term_curatable,
    )
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.api.admin._guards import require_active_org_id

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        await _require_term_curatable(conn, to_term_id, request)
        try:
            await glossary_repo.add_edge(conn, term_id, to_term_id, rel_type)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    await _notify(org_id, "glossary edge added")
    return {"ok": True}


async def remove_glossary_term_edge(
    state: Any, role: str, request: Any, term_id: int, to_term_id: int, rel_type: str
) -> dict:  # REQ-1835
    """Remove a relationship edge between two glossary terms."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import (
        _notify,
        _require_glossary_rw,
        _require_term_curatable,
    )
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.api.admin._guards import require_active_org_id

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        removed = await glossary_repo.remove_edge(conn, term_id, to_term_id, rel_type)
    if not removed:
        raise ValueError("edge not found")
    await _notify(org_id, "glossary edge removed")
    return {"ok": True}


def list_commands(state: Any, role: str) -> list[dict]:
    """Registered commands the role may invoke (REQ-1156).

    Without this an agent can invoke a command via `SELECT fn(...)` only if it already knows the
    name — the command is dark to discovery. Projects the shared command-listing (visible_to
    filtered) so MCP lists commands alongside the run_sql invocation path.
    """
    from provisa.api.data.action_exec import list_visible_commands

    require_role(role, state)
    return list_visible_commands(state, role)


async def run_sql(
    state: Any, role: str, sql: str, limit: int | None = None, offset: int = 0
) -> dict:
    """Route SQL through _govern_and_route under ``role`` and execute it.

    A PermissionError from governance propagates to the caller (surfaced as an
    MCP tool error) — it is never swallowed into an empty result. Rows are
    capped/paged so the full result never lands in an agent's context.
    """
    from provisa.pgwire._pipeline import execute_sql_batch

    require_role(role, state)
    if offset < 0:
        raise ValueError("offset must be >= 0")
    cap = _max_rows()
    page = cap if limit is None else min(int(limit), cap)
    if page <= 0:
        raise ValueError("limit must be a positive integer")

    # ONE pipeline: execute the (possibly multi-statement) batch statement-aware, governing+executing
    # each statement (last result returned) — a registered command per statement still routes through
    # the shared function hook (REQ-1156), and a multi-statement batch is never silently truncated.
    result = await execute_sql_batch(sql, role, state)  # raises PermissionError / ValueError

    total = len(result.rows)
    window = result.rows[offset : offset + page]
    cols = list(result.column_names)
    return {
        "columns": cols,
        "rows": [_row_to_json(cols, r) for r in window],
        "row_count": len(window),
        "offset": offset,
        "total_rows": total,
        "truncated": (offset + len(window)) < total,
    }


async def explain_sql(state: Any, role: str, sql: str) -> dict:
    """Validate + govern the query WITHOUT executing, and report that it plans cleanly.

    Physical is an internal lowering artifact: the federated ``physical_sql``, the raw
    ``source_id`` (e.g. "pet-store-sqlite"), and the source ``dialect`` are NEVER returned —
    exposing them let an agent read a physical name and replay it, bypassing the semantic
    layer. This tool only confirms the SQL is valid and governed for the role (it raises
    PermissionError / ValueError otherwise); there is nothing physical to reveal.
    """
    from provisa.pgwire._pipeline import _govern_and_route

    require_role(role, state)
    await _govern_and_route(sql, role)  # raises PermissionError / ValueError; result discarded
    return {"ok": True}


def _visible_metrics(state: Any, role: str) -> list[Any]:
    """Config metrics whose ``visible_to`` contains "*" or ``role`` (REQ-1319).

    Sourced from the loaded config (``state.config.metrics``) — the same reachable
    config object _domain_descriptions reads domains from.
    """
    config = getattr(state, "config", None)
    metrics = getattr(config, "metrics", None) or []
    return [m for m in metrics if "*" in m.visible_to or role in m.visible_to]


def list_metrics(state: Any, role: str) -> list[dict]:
    """Governed metric definitions visible to the role (REQ-1319).

    Projects each metric's name + description + ai_context (definition text written
    for AI consumers) + datatype + from_fact, filtered by ``visible_to``. Never the
    expression — an agent selects a meaning by name, not by SQL.
    """
    require_role(role, state)
    return [
        {
            "name": m.name,
            "description": m.description,
            "ai_context": m.ai_context,
            "datatype": m.datatype,
            "from_fact": m.from_fact,
        }
        for m in _visible_metrics(state, role)
    ]


def _metric_sql(metric: str, dimensions: list[str], filters: str | None) -> str:
    """The semantic SQL for one metric query (REQ-1319) — the ONE builder, shared
    across surfaces (MCP, Bolt, NL, Flight) via the compiler."""
    from provisa.compiler.metric_expand import metric_semantic_sql

    return metric_semantic_sql(metric, dimensions, filters)


async def query_metric(
    state: Any, role: str, metric: str, dimensions: list[str], filters: str | None = None
) -> list[dict]:
    """Query one governed metric at a caller-chosen grain (REQ-1319).

    Builds semantic SQL against the reserved ``metrics`` schema and executes it through
    the same governed pipeline run_sql uses (execute_sql_batch) — never a private
    execution path. A PermissionError from governance propagates to the caller.
    """
    from provisa.pgwire._pipeline import execute_sql_batch

    require_role(role, state)
    if metric not in {m.name for m in _visible_metrics(state, role)}:
        raise ValueError(f"Unknown metric {metric!r}")
    result = await execute_sql_batch(_metric_sql(metric, dimensions, filters), role, state)
    cols = list(result.column_names)
    return [_row_to_json(cols, r) for r in result.rows]


def _role_domains(state: Any, role: str) -> set[str]:
    """The schema ids ``role`` may access, or ``{"*"}`` for full access.

    Sourced from the loaded config roles' ``domain_access`` — the same list the SQL
    pipeline enforces. A search hit outside these domains is dropped, so the agent
    never sees an entity it could not query.
    """
    config = getattr(state, "config", None)
    for r in getattr(config, "roles", None) or []:
        if getattr(r, "id", None) == role:
            access = list(getattr(r, "domain_access", None) or [])
            return {"*"} if "*" in access else set(access)
    return set()


async def effective_config(state: Any) -> dict:
    """Deployment config with the acting org's overrides applied (REQ-1349).

    The SAME merge provisa/api/admin/ai_models_router.py's admin surface reads from
    (resolve_org_config) — vector_models, ai_models.mcp_chat, and ai_endpoints are all org-scoped
    settings an admin can change through the AI Models UI without a restart. Reading `state.config`
    (the static deployment object built once at startup) or the bare deployment file instead of
    this misses every org override entirely: an org that registers an embedding model or an MCP
    chat vendor through the UI would see none of it here. Falls back to `state.config` itself
    (not a disk re-read) when no tenant_db is bound — there is no org to layer overrides from, and
    a fresh read would ignore a config a caller (e.g. a test) built in memory rather than on disk.
    """
    tenant_db = getattr(state, "tenant_db", None)
    if tenant_db is None:
        config = getattr(state, "config", None)
        if config is None:
            return {}
        return config.model_dump() if hasattr(config, "model_dump") else vars(config)
    from provisa.core.org_settings import resolve_org_config

    return await resolve_org_config(tenant_db)


def _cfg_get(entry: Any, key: str, default: Any = None) -> Any:
    """Read `key` off a config entry that may be a plain dict (org_settings/deployment-YAML JSON)
    or an attribute-style object (a Pydantic model, or a test's SimpleNamespace)."""
    return entry.get(key, default) if isinstance(entry, dict) else getattr(entry, key, default)


async def _resolve_embedding_model(state: Any) -> Any:
    """The embedding model for catalog search — the first enabled ``vector_models`` entry.

    No silent fallback (CLAUDE.md): with no enabled embedding model registered, catalog
    search is unavailable and says so, rather than inventing a model.
    """
    from provisa.vector.registry import VectorModel

    cfg = await effective_config(state)
    for vm in cfg.get("vector_models") or []:
        if _cfg_get(vm, "enabled", True):
            return VectorModel(
                id=_cfg_get(vm, "id"),
                provider=_cfg_get(vm, "provider"),
                dimensions=_cfg_get(vm, "dimensions"),
                base_url=_cfg_get(vm, "base_url"),
            )
    raise ValueError(
        "catalog search requires an enabled embedding model — register one in "
        "vector_models (admin → AI Models)"
    )


async def build_catalog_index(state: Any, provider: Any = None) -> int:
    """(Re)build the server-lifetime catalog search index over the full catalog.

    Cached on ``state.mcp_catalog_index``. Called at startup / on catalog refresh; a
    cold build is just the full case. ``provider`` is injectable for tests.
    """
    from provisa.api.mcp.search import CatalogSearchIndex

    model = await _resolve_embedding_model(state)
    catalog = await _catalog(state)
    index = CatalogSearchIndex(model, provider)
    await index.build(catalog, _domain_descriptions(state))
    state.mcp_catalog_index = index
    return len(catalog)


async def _get_index(state: Any) -> Any:
    """The catalog index, built lazily on first use if startup did not build it."""
    index = getattr(state, "mcp_catalog_index", None)
    if index is None or not getattr(index, "built", False):
        await build_catalog_index(state)
        index = state.mcp_catalog_index
    return index


async def search_catalog(state: Any, role: str, nl_text: str, k: int = 5) -> list[dict]:
    """Semantic bottom-up catalog search, resolved up to authoritative table branches.

    Embeds ``nl_text``, finds the nearest chunks (schema/table/column), keeps only hits
    in domains ``role`` may access, then resolves each up to its parent table via
    describe_table — returning the full column list + FKs + a schema breadcrumb, plus
    which leaf matched. Deduplicated by table, best (closest) match wins.
    """
    require_role(role, state)
    if not nl_text or not nl_text.strip():
        raise ValueError("search text is required")
    if k <= 0:
        raise ValueError("k must be a positive integer")

    from provisa.compiler.naming import domain_to_sql_name

    index = await _get_index(state)
    # Over-fetch so role/domain filtering + table dedup still yields k branches.
    hits = await index.search(nl_text.strip(), max(k * 6, k))

    # Hits carry semantic schema names (the index is built over the semantic catalog), so
    # normalize the role's allowed domains to the same semantic form before comparing.
    allowed = {d if d == "*" else domain_to_sql_name(d) for d in _role_domains(state, role)}
    results: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for h in hits:
        if h.table is None:  # schema-tier hit has no table branch to resolve to
            continue
        if "*" not in allowed and h.schema not in allowed:
            continue
        key = (h.schema, h.table)
        if key in seen:
            continue
        seen.add(key)
        try:
            branch = await describe_table(state, role, h.schema, h.table)
        except (ValueError, PermissionError):
            continue  # table vanished or not visible to this role — skip, don't fail the search
        results.append(
            {
                "schema": h.schema,
                "table": h.table,
                "breadcrumb": f"{h.schema} > {h.table}",
                "matched_on": {"level": h.level, "column": h.column},
                "score": round(1.0 - float(h.distance), 4),  # cosine similarity
                "branch": branch,
            }
        )
        if len(results) >= k:
            break
    return results


async def search_terms(state: Any, role: str, query: str, *, limit: int = 25) -> list[dict]:
    """Business-glossary term lookup (REQ-1387): grounded vocabulary for agent surfaces.

    Matches term names and definitions; each hit carries its physical refs (which
    tables/columns mean this concept), typed relationships to other terms, and the
    experts who can answer questions about it. v1 scope is documentation and
    discovery — term membership does not drive policy.
    """
    role = require_role(role, state)
    if not query or not query.strip():
        raise ValueError("search text is required")
    from provisa.core.env_authority import domains_within
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.security.rights import domain_access_for_claims

    # REQ-1591: an agent reaches the vocabulary of the domains its ROLE reaches, by the same ANY
    # rule the admin list uses. The role name is the whole identity on this surface, so its
    # domain_access is read directly rather than through a request's claims. ``None`` back from
    # domains_within is an unlimited role — the one place that decides what ``*`` means.
    allowed = domains_within(sorted(domain_access_for_claims([role], state.roles)))
    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        return await glossary_repo.search_terms(
            conn,
            query.strip(),
            limit=limit,
            domains=None if allowed is None else frozenset(allowed),
        )


async def _jev_api_key(state: Any) -> str:
    from provisa.core.org_secrets import resolve_jev_api_key

    key = await resolve_jev_api_key(getattr(state, "tenant_db", None))
    if not key:
        raise ValueError(
            "Jev is not configured — set TYPESAFEAI_API_KEY or an org Jev key in Admin > AI Models"
        )
    return key


async def jev_evaluate(state: Any, role: str, jev_state: Any, questions: list[dict]) -> dict:
    """Evaluate typed decision questions (noul/choice/score) via TypeSafe's Jev API.

    A System One model for fast, cheap, calibrated machine-native decisions — not text
    generation. Each question gets a typed answer, a probability distribution, and a
    confidence score the caller can branch on (see docs.typesafe.ai/confidence for
    confidence-routing: escalate low-confidence answers instead of trusting them blind).
    role is required for audit attribution (REQ-074) even though this call touches no
    governed data — consistent with every other MCP tool. The acting org's own Jev key
    (Admin > AI Models) is preferred over the deployment's TYPESAFEAI_API_KEY.
    """
    from provisa.jev.client import evaluate

    require_role(role, state)
    return await evaluate(await _jev_api_key(state), jev_state, questions)


async def _queue_mcp_proposal(
    state: Any, role: str, request_type: str, capability: str, reason: str, rebuilt_input: Any
) -> dict:
    """Shared REQ-1792 tail: persist a pending creation request from an MCP-side proposal.

    Never creates the live entity itself — always lands in the same REQ-434 queue a low-privilege
    GraphQL caller falls back to, so a rights-holder must execute or reject it via the admin UI
    (Requests page) regardless of what capability the MCP credential itself carries."""
    import dataclasses

    from provisa.core.repositories import creation_request as cr_repo

    if not reason or not reason.strip():
        raise ValueError("reason is required — say why this was discovered/proposed")

    payload = dataclasses.asdict(rebuilt_input)
    payload["_proposed_reason"] = reason.strip()
    payload["_proposed_via"] = "mcp"

    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as conn:
        request_id = await cr_repo.create(conn, request_type, capability, payload, role)
    return {
        "request_id": request_id,
        "status": "pending",
        "message": (
            f"Queued as creation request #{request_id} — awaiting a user holding "
            f"{capability!r} to review and approve on the Requests page."
        ),
    }


def _role_has_capability(state: Any, role: str, capability: str, *, request: Any = None) -> bool:
    """Whether the caller carries `capability` — through EITHER the single pinned `role` (already
    verified by AuthMiddleware, see provisa/api/mcp/status.py) OR, when `request` is given, the
    UNION of every role assignment the caller's real identity holds (REQ-1799 follow-up).

    The UI's "Role: All" means every role GRANTED to the user, unioned — not a literal role named
    "All" (there is no such role in the registry). Its single `x-provisa-role` header collapses to
    whichever assignment happens to be first (see AuthContext.tsx's `activeRoles[0]`), which can
    be a control-plane role like platform_admin even when the same person also holds org_admin.
    Checking only that one pinned role — as this function originally did — answered "does
    platform_admin hold source_registration" (correctly no, by REQ-1297) instead of "does this
    PERSON hold it through ANY of their roles" (the question REQ-1799 actually needs answered),
    so a person who genuinely holds org_admin never got the confirm_required shortcut while acting
    as "All". `request.state.assignments` (also AuthMiddleware-verified) is the real fix: it is
    the SAME list AuthContext.tsx's "All" unions client-side, checked here server-side instead of
    trusted from the client."""
    from provisa.security.rights import capabilities_for_claims

    assignments = getattr(getattr(request, "state", None), "assignments", None) if request else None
    role_ids = [a.role_id for a in assignments] if assignments else [role]
    return capability in capabilities_for_claims(role_ids, state.roles)


async def propose_source(
    state: Any, role: str, source: dict, reason: str, *, request: Any = None
) -> dict:  # REQ-1792, REQ-1799
    """Queue a discovered data source as a pending creation request for a human to approve —
    UNLESS `role` already carries `source_registration` and `request` (the real, authenticated
    HTTP request AuthMiddleware verified `role` against) is available, in which case this returns
    a `confirm_required` result instead of queuing (REQ-1799): the chat assistant must ask the
    user before calling create_source_now with the same source/reason, rather than silently
    creating a live source or leaving a redundant pending request nobody but this same user would
    approve.

    `source` is validated as a well-formed SourceInput (required fields: id, type) before it is
    queued or offered, so a malformed proposal fails fast rather than sitting in the queue as
    garbage a human has to puzzle out. See `_queue_mcp_proposal` for why the queued path never
    creates a live Source."""
    require_role(role, state)
    from provisa.api.admin.schema_common import _rebuild_source_input

    try:
        source_input = _rebuild_source_input(dict(source))
    except TypeError as exc:
        raise ValueError(f"malformed source proposal: {exc}") from exc

    if request is not None and _role_has_capability(
        state, role, "source_registration", request=request
    ):
        return {
            "status": "confirm_required",
            "capability": "source_registration",
            "source": source,
            "reason": reason,
            "message": (
                "Your role already holds source_registration. Ask the user whether to create "
                "this source now instead of queuing it for someone else to approve; if they say "
                "yes, call create_source_now with this same source and reason."
            ),
        }
    return await _queue_mcp_proposal(
        state, role, "source", "source_registration", reason, source_input
    )


async def propose_table(
    state: Any, role: str, table: dict, reason: str, *, request: Any = None
) -> dict:  # REQ-1792, REQ-1799
    """Queue a table to register from an already-registered source, for a human to approve —
    UNLESS `role` already carries `table_registration` and `request` is available, in which case
    this returns a `confirm_required` result instead of queuing (REQ-1799) — see propose_source.

    `table` is a TableInput-shaped dict (required: source_id, domain_id, schema_name, table_name,
    columns — each column at minimum {"name", "visible_to"}). Column presets and unique constraints
    are optional. This proposes a PHYSICAL table registration, not a view: set `view_sql` in `table`
    only if you mean to propose a view instead — `register_table` (called at approval time, or by
    register_table_now) branches on that field the same way it does for a direct GraphQL caller."""
    require_role(role, state)
    from provisa.api.admin.schema_common import _rebuild_table_input

    try:
        table_input = _rebuild_table_input(dict(table))
    except TypeError as exc:
        raise ValueError(f"malformed table proposal: {exc}") from exc

    if request is not None and _role_has_capability(
        state, role, "table_registration", request=request
    ):
        return {
            "status": "confirm_required",
            "capability": "table_registration",
            "table": table,
            "reason": reason,
            "message": (
                "Your role already holds table_registration. Ask the user whether to register "
                "this table now instead of queuing it for someone else to approve; if they say "
                "yes, call register_table_now with this same table and reason."
            ),
        }
    return await _queue_mcp_proposal(
        state, role, "table", "table_registration", reason, table_input
    )


async def create_source_now(
    state: Any, role: str, source: dict, reason: str, *, request: Any
) -> dict:  # REQ-1799
    """Create a live Source directly, bypassing the REQ-434 review queue — only reachable after
    propose_source told the model to ask the user for confirmation first (REQ-1799); the model
    must never call this without that prior human confirmation in the SAME chat turn.

    Re-checks the capability itself (defense in depth: never trust that the caller only reaches
    this after propose_source's own check) against `role`, which AuthMiddleware already verified
    the real caller holds before this request reached any MCP tool. Executes through the EXACT
    same resolver a GraphQL caller with the capability would (`Mutation.create_source`), via a
    minimal Info shim wrapping the real `request` — so validation, secret handling, and engine
    provisioning behave identically to that path, not a second reimplementation of it."""
    require_role(role, state)
    if not _role_has_capability(state, role, "source_registration", request=request):
        raise PermissionError(f"role {role!r} does not hold source_registration")
    from provisa.api.admin.schema_common import _rebuild_source_input
    from provisa.api.admin.schema_mutation import Mutation

    try:
        source_input = _rebuild_source_input(dict(source))
    except TypeError as exc:
        raise ValueError(f"malformed source proposal: {exc}") from exc
    if not reason or not reason.strip():
        raise ValueError("reason is required — say why this is being created")

    info = types.SimpleNamespace(context={"request": request})
    # pyright mistypes strawberry.mutation-decorated methods' call signature (confirmed correct at
    # runtime via inspect.signature: (self, info, input) -> MutationResult).
    result = await Mutation().create_source(info, source_input)  # pyright: ignore[reportCallIssue]
    return {"success": result.success, "message": result.message, "code": result.code}


async def register_table_now(
    state: Any, role: str, table: dict, reason: str, *, request: Any
) -> dict:  # REQ-1799
    """Register a live Table/view directly, bypassing the REQ-434 review queue — only reachable
    after propose_table told the model to ask the user for confirmation first (REQ-1799); see
    create_source_now for the trust model and why the same GraphQL resolver is reused verbatim."""
    require_role(role, state)
    if not _role_has_capability(state, role, "table_registration", request=request):
        raise PermissionError(f"role {role!r} does not hold table_registration")
    from provisa.api.admin.schema_common import _rebuild_table_input
    from provisa.api.admin.schema_mutation import Mutation

    try:
        table_input = _rebuild_table_input(dict(table))
    except TypeError as exc:
        raise ValueError(f"malformed table proposal: {exc}") from exc
    if not reason or not reason.strip():
        raise ValueError("reason is required — say why this is being created")

    info = types.SimpleNamespace(context={"request": request})
    # pyright mistypes strawberry.mutation-decorated methods' call signature — see create_source_now.
    result = await Mutation().register_table(info, table_input)  # pyright: ignore[reportCallIssue]
    return {"success": result.success, "message": result.message, "code": result.code}


async def search_govdata_subjects(state: Any, role: str, query: str) -> list[dict]:  # REQ-1798
    """Match a free-text topic against GovData's (askamerica) real schema/table catalog.

    This org's GovData/Kaggle sources are its subscription sources (REQ-1798) — the chat
    assistant checks these before web_search or propose_source for a topical data request. Scores
    schemas by keyword hits against their real descriptions and table names/descriptions (see
    provisa.govdata.subjects.search_catalog, built from the govdata engine's own schema YAML — not
    a guessed synonym list), and reports whether this tenant is already subscribed to each match's
    subject, so the model can distinguish "already available" from "would need a subscription
    first"."""
    require_role(role, state)
    from provisa.core.models import GovDataSubject
    from provisa.govdata.subjects import search_catalog

    subscribed: set[GovDataSubject] = set()
    for sub in getattr(state.config, "govdata_subscriptions", None) or []:
        subscribed.update(sub.subjects)
    already_all = GovDataSubject.all in subscribed

    results = []
    for hit in search_catalog(query)[:8]:
        subject = GovDataSubject(hit["subject"]) if hit["subject"] else None
        results.append(
            {
                "schema": hit["schema"],
                "subject": hit["subject"],
                "tables": hit["tables"],
                "subscribed": subject is not None and (already_all or subject in subscribed),
            }
        )
    return results


async def search_kaggle_datasets(state: Any, role: str, query: str) -> list[dict]:  # REQ-1798
    """Search Kaggle's public dataset catalog by keyword.

    This org's GovData/Kaggle sources are its subscription sources (REQ-1798) — check this
    (and search_govdata_subjects) before web_search or propose_source for a topical data request.
    The Kaggle API token is read from the org secret named KAGGLE_TOKEN_SECRET_NAME, resolved via
    the standard ``${secret:NAME}`` grammar (provisa.core.secrets) — never taken as a raw token
    argument (that would put a credential in chat history/logs). The name is FIXED, not chosen per
    call: the model should never ask the user what to name it, only tell them (once, if the secret
    doesn't exist yet) to create one under exactly this name on the Secrets page (/admin/secrets)
    with their Kaggle API token as the value.

    ``${secret:...}`` resolves against whichever org's vault is BOUND to this context
    (provisa.core.secrets_store's ``bound_to_request_org``) — the same binding
    provisa/api/admin/schema_mutation.py's create_source wraps around its own secret reads. The
    MCP chat request never establishes that binding on its own (unlike a GraphQL admin mutation),
    so this tool must bind it explicitly or every resolution fails with "no organization is bound
    to this context" regardless of whether the secret actually exists."""
    require_role(role, state)
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.kaggle.client import search_datasets

    async with bound_to_request_org():
        token = resolve_secrets(f"${{secret:{KAGGLE_TOKEN_SECRET_NAME}}}")
    results = await search_datasets(token, query=query)
    return [
        {
            "ref": d.get("ref"),
            "title": d.get("title"),
            "description": d.get("subtitle") or d.get("description", ""),
        }
        for d in results[:10]
    ]


def _row_to_json(cols: list[str], row: Any) -> dict:
    """Map a result tuple to a JSON-safe {column: value} dict."""
    out: dict[str, Any] = {}
    for name, val in zip(cols, row, strict=False):
        out[name] = _json_safe(val)
    return out


def _json_safe(val: Any) -> Any:
    import datetime as _dt
    from decimal import Decimal

    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (bytes, bytearray)):
        return val.hex()
    if isinstance(val, (_dt.date, _dt.datetime, _dt.time)):
        return val.isoformat()
    return str(val)
