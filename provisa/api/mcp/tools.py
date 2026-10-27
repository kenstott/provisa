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

import json
import types
from pathlib import Path
from typing import Any

from provisa.api.flight.catalog import CatalogTable, _build_catalog_tables_async
from provisa.kaggle.downloader import api_token as kaggle_api_token  # the operator's setting


# Row ceiling for run_sql. An agent context must never absorb an unbounded
# result set, so every run_sql caps rows. An operator setting (``mcp.max_rows``, REQ-1913); the
# role's own governed row-cap (resolve_row_cap inside _govern_and_route) still applies on
# top of this — this is an additional transport-level ceiling, not a bypass.
def _max_rows() -> int:
    from provisa.core import settings_registry

    return settings_registry.value("mcp.max_rows")


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


async def _catalog(state: Any, role: str | None) -> list[CatalogTable]:
    """The virtual catalog (schemas/tables/columns) via the Flight reference builder,
    with every schema/table identifier normalized to its semantic (SQL-queryable) name.

    ``role`` narrows it to what that role is served — the tables and columns of its compiled
    context, by the builder's own narrowing (``flight.catalog.role_visibility``) — so a tool
    never lists a name the role could not read. None is the whole catalog, for the search
    index, which is built once for every role and filtered per caller when it is searched.

    Calls the ASYNC builder directly, on the caller's own event loop — not the sync
    ``build_catalog_tables`` wrapper (used by the Arrow Flight server, whose RPC handlers run on
    plain threads with no event loop of their own, so it must spin one up). Routing through that
    wrapper here via ``asyncio.to_thread`` created a SECOND event loop and tried to use
    ``state.model_db``'s pool — bound to the main loop — from within it, which async DB drivers
    reject (a cross-event-loop connection reuse, not something the driver treats as safe to
    share). Every MCP tool that reaches here (list_schemas, list_tables, search_catalog) was
    failing on that mismatch."""
    if not state.model_db:
        return []
    raw = await _build_catalog_tables_async(state, role)
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
    tables = await _catalog(state, role)
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
    tables = await _catalog(state, role)
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
        if src_type != type_name or getattr(jm, "cardinality", "") not in (
            "many-to-one",
            "one-to-one",
        ):
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
    tables = await _catalog(state, role)
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
        "time_travel": _time_travel(state, role, schema, table),  # REQ-1163
    }


def _time_travel(state: Any, role: str, schema: str, table: str) -> dict | None:
    """REQ-1163: how the table travels in time, for an agent writing a read of it: the arguments
    its reference takes in SQL (``"schema"."table"(as_recorded => '2026-10-01T17:00:00')``) and in
    GraphQL; None when it keeps no versions."""
    from provisa.mv.time_travel import relation_kinds

    meta = _find_role_table(state.contexts[role], schema, table)
    if meta is None:
        return None
    rows = [r for r in getattr(state, "tables", None) or [] if r["id"] == meta.table_id]
    kind = relation_kinds(state, rows).get(meta.table_id)
    if kind is None:
        return None
    return {
        "kind": kind,
        "arguments": ["as_of", "as_recorded"] if kind == "bitemporal" else ["as_recorded"],
    }


async def graphql_field_names(state: Any, role: str, schema: str, table: str) -> dict:  # REQ-1847
    """The REAL field names a GraphQL query against /query must use for this table and its
    columns — NOT a guessed transform of describe_table's SQL-plane names. The two planes apply
    DIFFERENT naming conventions and the table name additionally gets a domain-uniqueness prefix
    (e.g. registered table `iris_iris` in domain `shelter` can be GraphQL field `s__irisIris`,
    not `irisIris`) that only the compiled schema itself knows — reproduced live: guessing this
    transform got the domain prefix wrong and the query failed schema validation. ALWAYS call
    this before writing a GraphQL query; never assume camelCase-plus-prefix yourself.

    Also returns the real gRPC method names for this table (REQ-1849) — gRPC's rpc names are a
    deterministic proto3-cased transform of this same table_field, not an independent name.

    Also returns jsonapi_path/openapi_path (REQ-1851/1854) — JSON:API and OpenAPI key their routes on
    the table's RAW domain id (e.g. `pet-store`), not the sql-safe schema name
    (`domain_to_sql_name` turns it into `pet_store`) that `schema`/`table` themselves are. Reusing
    the sql-safe schema name in these two paths 404s for any domain id containing a character
    domain_to_sql_name changes (hyphens, spaces, ...) — reproduced live. Always use these two
    fields verbatim rather than reassembling `/{schema}/{table}` yourself.

    `schema`/`table` are the same semantic names describe_table/list_tables use."""
    require_role(role, state)
    from provisa.api.admin._graphql_field_name import resolve_graphql_field_name
    from provisa.compiler.naming import apply_gql_name, to_type_name
    from provisa.grpc.proto_gen import _to_proto_type_name

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
    # REQ-1849: the gRPC method name is a deterministic transform of the same table_field the
    # GraphQL schema compiles — proto_gen.py builds it as `Query` + the GQL type name (PascalCase
    # of table_field) re-cased to proto3 convention (PS__Pets -> PsPets), never independently
    # named. Reusing that exact transform here means this is a real name, not a guess.
    grpc_type_name = _to_proto_type_name(to_type_name(table_field))
    described = await describe_table(state, role, schema, table)
    return {
        "table_field": table_field,
        "grpc_query_method": f"Query{grpc_type_name}",
        "grpc_aggregate_method": f"Query{grpc_type_name}Aggregate",
        # REQ-1851: meta.domain_id/table_name are the RAW registered names JSON:API/OpenAPI route
        # on — NOT the sql-safe `schema` this function was called with.
        # REQ-1854: same default page size provisa/nl/runner.py's equivalent no-plan branches
        # use (?page[size]=20 / ?limit=20) — otherwise a plain browse silently fetches every row.
        "jsonapi_path": f"/data/jsonapi/{meta.domain_id}/{meta.table_name}?page[size]=20",
        "openapi_path": f"GET /data/rest/{meta.domain_id}/{meta.table_name}?limit=20",
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

    def _pattern(type_name: str) -> str:
        return f"(:{label_map.nodes[type_name].label})"

    relationships = sorted(
        {
            f"{_pattern(rel.source_label)}-[:{rel.rel_type}]->{_pattern(rel.target_label)}"
            for rels in label_map.aliases.values()
            for rel in rels
            if meta.type_name in (rel.source_label, rel.target_label)
            and rel.source_label in label_map.nodes
            and rel.target_label in label_map.nodes
        }
    )
    return {
        # A domain table's label is two labels, domain then table ("Shop:Orders"), each a plain
        # name: written as they are, never in backticks, which would make them one label that
        # matches nothing.
        "label": node.label,
        "node_pattern": f"(n:{node.label})",
        "id_property": node.id_column,
        "columns": [
            {"name": phys, "cypher_property": cyp} for cyp, phys in node.physical_properties.items()
        ],
        # Its registered relationships, typed and directed as a MATCH writes them: tables are
        # related only along these, never by matching columns in WHERE.
        "relationships": relationships,
    }


async def generate_explore_queries(state: Any, role: str, question: str) -> dict:  # REQ-1852
    """Generate ready-to-run queries for EVERY Explore query surface from one natural-language
    question, using the SAME pipeline the NL Explore page runs (REQ-355..359) — an LLM call of
    its own that resolves table selection, joins, and aggregation shape, not a guess. Use this
    (never graphql_field_names/cypher_field_names plus hand-built SQL/filters) for anything beyond
    browsing one table's plain columns: aggregation, GROUP BY, a business-term filter, or a
    question spanning more than one table. graphql_field_names/cypher_field_names remain the
    right, cheaper choice only for a flat single-table query with no aggregation.

    Returns {target: {"query": ..., "error": ...}} for sql/graphql/cypher/grpc/jsonapi/openapi,
    and for the three surfaces built on those (REQ-2114): nl (the question itself, for the
    Natural Language tab), python (a Python-tab script reading the SQL through provisa_client)
    and notebook (``"cells"`` in place of ``"query"``: the Notebook tab's cells running the SQL).
    The python and notebook entries carry the sql entry's error when it has one.
    A target with `error` set could not answer this question on that surface — say so plainly to
    the user rather than guessing a query for it. For every OTHER target, its `query` string is
    already the exact, complete value to navigate with: for jsonapi/openapi it IS the full
    deep-link path/method (do not reassemble it from schema/table names — see REQ-1851), for grpc
    it IS the exact rpc call signature, for sql/graphql/cypher it IS the query text. Use it
    verbatim; never rewrite, simplify, or re-derive it yourself."""
    require_role(role, state)
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.org_settings import resolve_org_config
    from provisa.llm.client import ProvisaLLMClient
    from provisa.nl.job import NlJob, make_job_store, new_job_id
    from provisa.nl.runner import run_nl_job

    # Same per-role NL rate limit /query/nl enforces (REQ-370) — this tool drives the identical
    # LLM pipeline, so it must not become an unlimited-cost bypass of that limit.
    limiter = getattr(state, "rate_limiter", None)
    nl_limit = state.config.nl.rate_limit if getattr(state, "config", None) else None
    if limiter and nl_limit and role:
        allowed, retry_after = await limiter.allow(f"rl:nl:{role}", nl_limit, 60.0)
        if not allowed:
            raise ValueError(f"NL query rate limit exceeded — retry in {int(retry_after + 0.999)}s")

    job_store = make_job_store()
    job_id = new_job_id()
    from provisa.nl.runner import effective_strict

    # REQ-603: non-strict, as this tool asks, only for a role that may bypass the relationship
    # guard; any other runs strict.
    strict = effective_strict(False, role, state)
    await job_store.put(NlJob(job_id=job_id, nl_query=question, role=role, strict=strict))

    cfg = await resolve_org_config(state.model_db)
    api_keys = await read_org_api_keys(state.model_db)
    # ProvisaLLMClient duck-types LLMClient's complete() (same pattern nl_router.py's own
    # untyped `llm` param relies on) rather than subclassing it.
    llm: Any = ProvisaLLMClient("sql_generation", config=cfg, api_keys=api_keys)

    await run_nl_job(job_id, question, role, state, job_store, llm, strict=strict, deliver=None)

    job = await job_store.get(job_id)
    if job is None:
        raise ValueError("NL job vanished from its own job store")
    queries: dict[str, dict] = {
        target: {"query": branch.query, "error": branch.error}
        for target, branch in job.branches.items()
    }
    return {**queries, **science_surfaces(question, queries.get("sql"))}


def science_surfaces(question: str, sql: dict | None) -> dict[str, dict]:
    """The Natural Language, Python and Notebook surfaces of one question (REQ-2114): the question
    as asked, and the SQL the NL pipeline built for it, read through provisa_client."""
    surfaces: dict[str, dict] = {"nl": {"query": question, "error": None}}
    statement = (sql or {}).get("query")
    error = (sql or {}).get("error") or (None if statement else "no SQL was built for it")
    if error:
        surfaces["python"] = {"query": None, "error": error}
        surfaces["notebook"] = {"cells": None, "error": error}
        return surfaces
    surfaces["python"] = {
        "query": (
            "import provisa_client\n"
            "provisa_client.sign_in()\n"
            f"df = provisa_client.query({statement!r})\n"
            "df"
        ),
        "error": None,
    }
    surfaces["notebook"] = {
        "cells": [
            "import provisa_client\nprovisa_client.sign_in()\n%load_ext provisa_client",
            f"%%sql\n{statement}",
        ],
        "error": None,
    }
    return surfaces


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
    """The org's ontology terms, optionally filtered by a search string. Each result carries
    `status` -- draft (not admitted), live (admitted: defined and grounded to a real column, or
    edge-connected to one), deprecated or retired -- so you can tell them apart without a second
    call; see create/update below for what 'finalizing' a term means."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import (
        _require_glossary_read,
        _view_scope,
        counted_as_served,
    )
    from provisa.core.repositories import glossary as glossary_repo

    _require_glossary_read(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        terms = await glossary_repo.list_terms(
            conn, q=q, include_deprecated=include_deprecated, domains=_view_scope(request, None)
        )
        return await counted_as_served(conn, request, terms)


async def create_glossary_term(
    state: Any,
    role: str,
    request: Any,
    name: str,
    definition: str | None = None,
    domains: list[str] | None = None,
) -> dict:  # REQ-1835
    """Create a new concept: an ontology term naming no column or table yet. It starts
    a draft — there is no separate live/finalized flag to flip: a term is 'live' automatically
    once it is grounded, either by a real registered column landing on it (sync happens on table
    registration, outside your control) or by connecting it via add_glossary_term_edge to a term
    that is already live. If the deployment is multi-domain, at least one domain is required."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import _declared_domains, _notify, _require_glossary_rw
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.api.admin._guards import require_active_org_id

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        declared = _declared_domains(request, domains, current=set())
        try:
            term_id = await glossary_repo.create_abstract_term(
                conn, name, definition=definition, domains=declared
            )
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    await _notify(org_id, "ontology term created")
    return {"id": term_id}


async def update_glossary_term(
    state: Any,
    role: str,
    request: Any,
    term_id: int,
    name: str | None = None,
    definition: str | None = None,
    export_excluded: bool | None = None,
) -> dict:  # REQ-1835
    """Rename a term, change its definition, or exclude it from exports. Only the fields you
    pass are changed. To withdraw a term use retire_glossary_term, with a reason."""
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
    pool = state.model_db
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
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    if not found:
        raise ValueError(f"ontology term {term_id} not found")
    await _notify(org_id, "ontology term updated")
    return {"ok": True}


async def change_glossary_term_status(
    state: Any, role: str, request: Any, term_id: int, action: str, body: dict
) -> dict:  # REQ-2144
    """Deprecate, retire or restore an ontology term -- the one status path the API has too."""
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.glossary_router import _notify, _require_glossary_rw, change_status
    from provisa.api.errors import ApiError
    from provisa.core.repositories import glossary as glossary_repo

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        try:
            changes = await change_status(conn, term_id, action, body, request)
        except ApiError as exc:
            if exc.status_code == 403:
                raise
            raise ValueError(exc.detail) from exc
        status = (await glossary_repo.term_statuses(conn))[term_id]
    from provisa.api.admin.glossary_router import _DONE

    await _notify(org_id, f"ontology term {_DONE[action]}")
    return {"ok": True, "term_id": term_id, "status": status, "tag_changes": changes}


async def list_glossary_findings(state: Any, role: str, request: Any) -> list[dict]:  # REQ-2146
    """What a curator should look at in the ontology: each term with its findings, each
    {kind, message, params} -- unlinked_tables, class_and_attribute, tag_wrong_kind,
    retirement_blocked, no_data. A plain draft term is not a finding."""
    require_role(role, state)
    from provisa.api.admin.glossary_router import _authority, _require_glossary_read, term_findings
    from provisa.core.glossary import readable_term
    from provisa.core.glossary_findings import findings

    _require_glossary_read(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        return await term_findings(conn, _authority(request), findings, readable_term)


async def delete_glossary_term(
    state: Any, role: str, request: Any, term_id: int
) -> dict:  # REQ-1835
    """Permanently delete a ontology term. Irreversible — confirm with the user first, the same
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
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        try:
            deleted = await glossary_repo.delete_term(conn, term_id)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    if not deleted:
        raise ValueError(f"ontology term {term_id} not found")
    await _notify(org_id, "ontology term deleted")
    return {"ok": True}


async def add_glossary_term_edge(
    state: Any, role: str, request: Any, term_id: int, to_term_id: int, rel_type: str
) -> dict:  # REQ-1835
    """Connect two ontology terms with a typed relationship (e.g. 'broader', 'narrower',
    'synonym' — check existing edges via list_glossary_terms/get_term for the vocabulary this
    org already uses). This is also how a draft (not-yet-grounded) term is finalized without
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
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        await _require_term_curatable(conn, to_term_id, request)
        try:
            await glossary_repo.add_edge(conn, term_id, to_term_id, rel_type)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
    await _notify(org_id, "ontology edge added")
    return {"ok": True}


async def remove_glossary_term_edge(
    state: Any, role: str, request: Any, term_id: int, to_term_id: int, rel_type: str
) -> dict:  # REQ-1835
    """Remove a relationship edge between two ontology terms."""
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
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        await _require_term_curatable(conn, term_id, request)
        removed = await glossary_repo.remove_edge(conn, term_id, to_term_id, rel_type)
    if not removed:
        raise ValueError("edge not found")
    await _notify(org_id, "ontology edge removed")
    return {"ok": True}


async def ground_glossary_term_to_table(
    state: Any, role: str, request: Any, term_id: int, table_id: int
) -> dict:  # REQ-2130
    """Make an ontology term the class term of a registered table or view: the table's rows are
    instances of what the term names. The table must have a primary key, and a table has one class
    term -- one already grounded by another term is refused, naming that term."""
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.glossary_router import _notify, _require_glossary_rw, ground_term_table
    from provisa.api.errors import ApiError

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        try:
            await ground_term_table(conn, term_id, table_id, request)
        except ApiError as exc:
            if exc.status_code == 403:
                raise
            raise ValueError(exc.detail) from exc
    await _notify(org_id, "ontology table grounded")
    return {"ok": True, "term_id": term_id, "table_id": table_id}


async def unground_glossary_term_from_table(
    state: Any, role: str, request: Any, term_id: int, table_id: int
) -> dict:  # REQ-2130
    """Withdraw an ontology term as a table's class term."""
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.glossary_router import _notify, _require_glossary_rw, unground_term_table
    from provisa.api.errors import ApiError

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        try:
            await unground_term_table(conn, term_id, table_id, request)
        except ApiError as exc:
            if exc.status_code == 403:
                raise
            raise ValueError(exc.detail) from exc
    await _notify(org_id, "ontology table ungrounded")
    return {"ok": True}


async def tag_glossary_term(
    state: Any, role: str, request: Any, term_id: int, tag_id: str, reason: str | None = None
) -> dict:  # REQ-2145
    """Put a tag (pii) on an ontology term that is a Property: every column the term holds, now
    and later, carries it. Says how many columns it covers."""
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.glossary_router import _notify, _require_glossary_rw, tag_term
    from provisa.api.admin.schema_mutation import _refresh_config_tags
    from provisa.api.errors import ApiError

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        try:
            covers = await tag_term(conn, request, term_id, tag_id, reason)
        except ApiError as exc:
            if exc.status_code == 403:
                raise
            raise ValueError(exc.detail) from exc
    await _refresh_config_tags()
    await _notify(org_id, "ontology term tagged")
    return {"ok": True, "term_id": term_id, "tag_id": tag_id, "covers": covers}


async def untag_glossary_term(
    state: Any, role: str, request: Any, term_id: int, tag_id: str
) -> dict:  # REQ-2145
    """Take a tag off an ontology term: its columns no longer carry it through the term."""
    require_role(role, state)
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.admin.glossary_router import _notify, _require_glossary_rw, untag_term
    from provisa.api.admin.schema_mutation import _refresh_config_tags

    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        removed = await untag_term(conn, request, term_id, tag_id)
    if not removed:
        raise ValueError(f"tag {tag_id!r} is not on term {term_id}")
    await _refresh_config_tags()
    await _notify(org_id, "ontology term untagged")
    return {"ok": True}


async def list_data_products(state: Any, role: str, request: Any) -> list[dict]:  # REQ-1855
    """The org's data products (id, domain, name, purpose, owner/team role, status, etc.)."""
    require_role(role, state)
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.core.repositories import data_product as data_product_repo

    require_capability_request(request, "data_product_read")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        return await data_product_repo.list_all(conn)


async def create_data_product(  # REQ-1855
    state: Any,
    role: str,
    request: Any,
    id: str,
    domain_id: str,
    name: str,
    owner_role: str | None = None,
    team_role: str | None = None,
    purpose: str = "",
    limitations: str = "",
    usage: str = "",
    version: str | None = None,
    status: str | None = None,
    sla: str | None = None,
    support: str | None = None,
) -> dict:
    """Create or replace a data product by id (an id that already exists is overwritten in
    full — fetch it first with list_data_products if you mean to change only some fields).
    `domain_id` must be a real, existing domain."""
    require_role(role, state)
    from provisa.core.models import DataProduct as DataProductModel
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.core.repositories import domain as domain_repo

    from provisa.api.admin.capabilities import data_product_edit_refusal
    from provisa.api.errors import ApiError

    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        # REQ-1944: the product's domain, and the one it is moved out of when it exists. REQ-2155:
        # a product's owner (data_product_owned_rw) edits the one it owns.
        stored = await data_product_repo.get(conn, id)
        refusal = data_product_edit_refusal(
            getattr(request.state, "identity", None),
            state,
            stored,
            {domain_id} | ({stored["domain_id"]} if stored is not None else set()),
        )
        if refusal is not None:
            raise ApiError(403, "auth.missing_capability", refusal)
        if await domain_repo.get(conn, domain_id) is None:
            raise ValueError(f"Domain not found: {domain_id}")
        model = DataProductModel(
            id=id,
            domain_id=domain_id,
            name=name,
            owner_role=owner_role,
            team_role=team_role,
            purpose=purpose,
            limitations=limitations,
            usage=usage,
            version=version,
            status=status,
            sla=sla,
            support=support,
        )
        await data_product_repo.upsert(conn, model)
    return {"id": id}


async def delete_data_product(state: Any, role: str, request: Any, id: str) -> dict:  # REQ-1855
    """Delete a data product by id. Irreversible — always confirm with the user first
    (present_choice, mode='yes_no')."""
    require_role(role, state)
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.core.repositories import data_product as data_product_repo

    require_capability_request(request, "data_product_rw")
    from provisa.api.admin.capabilities import require_right_in_domains_request

    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        stored = await data_product_repo.get(conn, id)
        if stored is not None:  # REQ-1944: an absent product is the not-found below
            require_right_in_domains_request(request, "data_product_rw", {stored["domain_id"]})
        deleted = await data_product_repo.delete(conn, id)
    if not deleted:
        raise ValueError(f"Data product not found: {id}")
    return {"ok": True}


async def upsert_metric(  # REQ-1856
    state: Any,
    role: str,
    request: Any,
    name: str,
    expression: str,
    datatype: str | None = None,
    description: str | None = None,
    ai_context: str | None = None,
    visible_to: list[str] | None = None,
) -> dict:
    """Create or replace a governed metric by name (a name that already exists is overwritten
    in full). `expression` is an aggregate ANSI-SQL expression over semantic table.column
    references (e.g. "SUM(orders.amount) - SUM(orders.refunds)") — must parse under sqlglot and
    contain at least one aggregate function, or this raises. `ai_context` is definition text
    written for AI consumers (yourself and other agents) — fill it in with what the metric means
    and when to use it, not just a repeat of the expression. `visible_to` defaults to the acting
    role and the standard Viewer role (every role built on Viewer) if omitted."""
    require_role(role, state)
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.core.models import Metric as MetricModel
    from provisa.core.repositories import metric as metric_repo

    require_capability_request(request, "table_registration")
    from provisa.security.default_grant import default_read_grant as _default_read_grant

    model = MetricModel(
        name=name,
        expression=expression,
        datatype=datatype,
        description=description,
        ai_context=ai_context,
        # REQ-1317: one created with no roles named is read by the acting role and Viewer.
        visible_to=list(visible_to) if visible_to else _default_read_grant(role),
    )
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        await metric_repo.upsert(conn, model)
        from provisa.api.admin._metric_views import regenerate_metric_views

        regenerated = await regenerate_metric_views(conn, name)
    from provisa.api.admin.schema_helpers import _rebuild_schemas

    await _rebuild_schemas()
    return {"name": name, "regenerated_views": regenerated}


async def delete_metric(state: Any, role: str, request: Any, name: str) -> dict:  # REQ-1856
    """Delete a governed metric by name. Irreversible — always confirm with the user first
    (present_choice, mode='yes_no')."""
    require_role(role, state)
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.core.repositories import metric as metric_repo

    require_capability_request(request, "table_registration")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        deleted = await metric_repo.delete(conn, name)
    if not deleted:
        raise ValueError(f"Metric not found: {name}")
    from provisa.api.admin.schema_helpers import _rebuild_schemas

    await _rebuild_schemas()
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
    state: Any,
    role: str,
    sql: str,
    limit: int | None = None,
    offset: int = 0,
    *,
    redirect: bool = False,
    redirect_format: str | None = None,
) -> dict:
    """Route SQL through _govern_and_route under ``role`` and execute it.

    A PermissionError from governance propagates to the caller (surfaced as an
    MCP tool error) — it is never swallowed into an empty result. Rows are
    capped/paged so the full result never lands in an agent's context.

    ``redirect`` forces a delivery to the results store (REQ-1194) in ``redirect_format``;
    the tool is a buffered transport, so REQ-1224's threshold also delivers a result over it
    when the operator has enabled it. A delivered result answers its handle and no rows.
    """
    from provisa.executor.redirect import delivery_from_request, parse_redirect_format
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
    delivery = delivery_from_request(
        force_redirect=redirect,
        # A format that is no format is refused by name (ValueError), never defaulted.
        redirect_format=parse_redirect_format(redirect_format) if redirect_format else None,
        threshold=None,
        role=role,
    )
    # raises PermissionError / ValueError
    result = await execute_sql_batch(sql, role, state, deliver=delivery, buffered=True)
    if result.redirect is not None:
        return {
            "columns": [],
            "rows": [],
            "row_count": 0,
            "offset": 0,
            "total_rows": result.redirect["row_count"],
            "truncated": False,
            "redirect": result.redirect,
        }

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


def cytoscape_elements(rows: list) -> dict:
    """The graph a Cypher answer holds, as Cytoscape elements: every node and relationship found
    anywhere in its rows (bare, inside a path, a list or a map), once each.

    The ids are the graph page's own (GraphCanvas.tsx): a node is ``<label>:<id>``, a relationship
    its identity, an edge's ends the ids of its nodes. Properties are the ones the Cypher
    pipeline returned under the caller's role -- columns hidden from it are not in the answer, and
    masked ones carry the masked value -- so nothing is added here."""
    nodes: dict[str, dict] = {}
    edges: dict[str, dict] = {}

    def node_id(n: dict) -> str:
        return f"{n['label']}:{n['id']}"

    def add_node(n: dict) -> None:
        nodes.setdefault(
            node_id(n),
            {
                "data": {
                    "id": node_id(n),
                    "label": n["label"],
                    "tableLabel": n["tableLabel"],
                    "properties": n["properties"],
                }
            },
        )

    def walk(v: Any) -> None:
        if isinstance(v, dict):
            if "startNode" in v and "endNode" in v and "type" in v:
                add_node(v["startNode"])
                add_node(v["endNode"])
                data = {
                    "id": str(v["identity"]),
                    "source": node_id(v["startNode"]),
                    "target": node_id(v["endNode"]),
                    "label": v["type"],
                    "properties": v["properties"],
                }
                if "junctionTable" in v:
                    data["junctionTable"] = v["junctionTable"]
                edges.setdefault(data["id"], {"data": data})
            elif "label" in v and "properties" in v and "id" in v:
                add_node(v)
            else:
                for item in v.values():
                    walk(item)
        elif isinstance(v, list):
            for item in v:
                walk(item)

    walk(rows)
    return {"nodes": list(nodes.values()), "edges": list(edges.values())}


async def run_cypher(
    state: Any, role: str, cypher: str, params: dict | None = None, limit: int | None = None
) -> dict:
    """Run Cypher under ``role`` through the pipeline every Cypher surface uses (the Bolt
    server's: parse, translate, govern, route, execute -- a write as a write) and answer its rows
    and the graph they hold as Cytoscape elements. Rows are capped like run_sql's; the elements
    are those of the rows returned. A PermissionError from governance propagates."""
    from provisa.bolt.session import _execute_cypher

    require_role(role, state)
    cap = _max_rows()
    page = cap if limit is None else min(int(limit), cap)
    if page <= 0:
        raise ValueError("limit must be a positive integer")
    columns, rows, _redirect = await _execute_cypher(cypher, params or {}, role, roles=[role])
    window = rows[:page]
    records = [dict(zip(columns, r)) for r in window]
    return {
        "columns": columns,
        "rows": records,
        "row_count": len(window),
        "total_rows": len(rows),
        "truncated": len(window) < len(rows),
        "elements": cytoscape_elements(records),
    }


#: render_graph's renderer: provisa-ui/graph-render bundled (the image builds it there; in a
#: checkout, build that package and link this directory to it).
GRAPH_RENDERER = Path(__file__).resolve().parents[2] / "_graph_render" / "dist" / "render.mjs"
GRAPH_LAYOUTS = ("force", "hierarchy", "elk")
GRAPH_THEMES = ("light", "dark")


def render_graph(
    elements: dict,
    layout: str = "force",
    theme: str = "light",
    width: int = 800,
    height: int = 600,
) -> bytes:
    """The graph run_cypher returned, drawn as the UI's graph page draws it, as a PNG: the page's
    own Cytoscape stylesheet and layouts (force, hierarchy, or the ERD's elk) on the page's
    background for the theme. Drawn by Node (provisa-ui/graph-render), one process per call,
    within mcp.render_timeout_s."""
    import shutil
    import subprocess

    from provisa.core import settings_registry

    if layout not in GRAPH_LAYOUTS:
        raise ValueError(f"layout must be one of {list(GRAPH_LAYOUTS)}, not {layout!r}")
    if theme not in GRAPH_THEMES:
        raise ValueError(f"theme must be one of {list(GRAPH_THEMES)}, not {theme!r}")
    for name, size in (("width", width), ("height", height)):
        if not 100 <= int(size) <= 4000:
            raise ValueError(f"{name} must be between 100 and 4000 pixels, not {size}")
    nodes, edges = elements.get("nodes"), elements.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise ValueError(
            "elements must be {nodes: [...], edges: [...]}, as run_cypher returns them"
        )
    for el in [*nodes, *edges]:
        if (
            not isinstance(el, dict)
            or not isinstance(el.get("data"), dict)
            or "id" not in el["data"]
        ):
            raise ValueError("every element is {data: {id, ...}}")
    ids = {n["data"]["id"] for n in nodes}
    for e in edges:
        for end in ("source", "target"):
            if e["data"].get(end) not in ids:
                raise ValueError(f"edge {e['data']['id']!r} {end} names no node in elements")

    node = shutil.which("node")
    if node is None:
        raise RuntimeError("render_graph draws with Node, and `node` is not on this host's PATH")
    if not GRAPH_RENDERER.is_file():
        raise RuntimeError(
            f"render_graph's renderer is not built: {GRAPH_RENDERER} is missing "
            "(npm ci && npm run build in provisa-ui/graph-render)"
        )
    timeout = settings_registry.value("mcp.render_timeout_s")
    request = {
        "elements": {"nodes": nodes, "edges": edges},
        "layout": layout,
        "theme": theme,
        "width": int(width),
        "height": int(height),
    }
    try:
        done = subprocess.run(  # noqa: S603 -- the host's node, our own bundled script
            [node, str(GRAPH_RENDERER)],
            input=json.dumps(request).encode(),
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"render_graph took longer than mcp.render_timeout_s ({timeout} s)"
        ) from exc
    if done.returncode != 0:
        raise RuntimeError(f"render_graph failed: {done.stderr.decode(errors='replace').strip()}")
    return done.stdout


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
    """Config metrics whose ``visible_to`` names ``role`` (REQ-1319, REQ-1317).

    Sourced from the loaded config (``state.config.metrics``) — the same reachable
    config object _domain_descriptions reads domains from.
    """
    config = getattr(state, "config", None)
    metrics = getattr(config, "metrics", None) or []
    return [m for m in metrics if role in m.visible_to]


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
            from provisa.security.rights import reaches_all_domains

            access = list(r.domain_access)
            return {"*"} if reaches_all_domains(access) else set(access)
    return set()


async def effective_config(state: Any) -> dict:
    """Deployment config with the acting org's overrides applied (REQ-1349).

    The SAME merge provisa/api/admin/ai_models_router.py's admin surface reads from
    (resolve_org_config) — vector_models, ai_models.mcp_chat, and ai_endpoints are all org-scoped
    settings an admin can change through the AI Models UI without a restart. Reading `state.config`
    (the static deployment object built once at startup) or the bare deployment file instead of
    this misses every org override entirely: an org that registers an embedding model or an MCP
    chat vendor through the UI would see none of it here. Falls back to `state.config` itself
    (not a disk re-read) when no model_db is bound — there is no org to layer overrides from, and
    a fresh read would ignore a config a caller (e.g. a test) built in memory rather than on disk.
    """
    model_db = getattr(state, "model_db", None)
    if model_db is None:
        config = getattr(state, "config", None)
        if config is None:
            return {}
        return config.model_dump() if hasattr(config, "model_dump") else vars(config)
    from provisa.core.org_settings import resolve_org_config

    return await resolve_org_config(model_db)


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
    catalog = await _catalog(state, None)
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
        # The index covers every column; a hit on a column this role is not served would name
        # it in ``matched_on``.
        if h.column is not None and h.column not in {c["name"] for c in branch["columns"]}:
            continue
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
    """Business-ontology term lookup (REQ-1387): grounded vocabulary for agent surfaces.

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
    pool = state.model_db
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

    key = await resolve_jev_api_key(getattr(state, "model_db", None))
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


def _proposer(request: Any, requested_by: str | None) -> str:
    """The USER a proposal is recorded as made by, so that the rule that nobody decides their own
    request compares a user with a user. The in-app chat hands the authenticated request; the MCP
    server, which has no request, names the user itself (the remote caller's validated identity,
    or its stdio principal)."""
    if requested_by:
        return requested_by
    identity = getattr(getattr(request, "state", None), "identity", None)
    user_id = getattr(identity, "user_id", None)
    if not user_id:
        raise ValueError("a proposal needs the user who makes it: no identity on this call")
    return user_id


async def _queue_mcp_proposal(
    state: Any,
    role: str,
    request_type: str,
    capability: str,
    reason: str,
    rebuilt_input: Any,
    request: Any,
    requested_by: str | None,
) -> dict:
    """Shared REQ-1792 tail: persist a pending creation request from an MCP-side proposal.

    Never creates the live entity itself — always lands in the same REQ-434 queue a low-privilege
    GraphQL caller falls back to, so a rights-holder must execute or reject it via the admin UI
    (Requests page) regardless of what capability the MCP credential itself carries."""

    from provisa.core.repositories import creation_request as cr_repo

    if not reason or not reason.strip():
        raise ValueError("reason is required — say why this was discovered/proposed")

    from provisa.api.admin.schema_common import request_payload

    proposer = _proposer(request, requested_by)
    payload = request_payload(rebuilt_input)
    payload["_proposed_reason"] = reason.strip()
    payload["_proposed_via"] = "mcp"
    payload["_proposed_role"] = role

    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        request_id = await cr_repo.create(conn, request_type, capability, payload, proposer)
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
    state: Any,
    role: str,
    source: dict,
    reason: str,
    *,
    request: Any = None,
    requested_by: str | None = None,
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
        state,
        role,
        "source",
        "source_registration",
        reason,
        source_input,
        request,
        requested_by,
    )


async def propose_table(
    state: Any,
    role: str,
    table: dict,
    reason: str,
    *,
    request: Any = None,
    requested_by: str | None = None,
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
        state,
        role,
        "table",
        "table_registration",
        reason,
        table_input,
        request,
        requested_by,
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
    The Kaggle API token is the operator setting "Kaggle API token" (``kaggle.api_token``): what
    the operator typed there -- the token, or a ``${secret:NAME}`` reference to a secret they
    created -- resolved by the one resolver (provisa.core.secrets). It is never taken as a tool
    argument (that would put a credential in chat history/logs), and the model neither chooses
    nor asks for a secret name.

    A ``${secret:...}`` reference resolves against whichever org's vault is BOUND to this context
    (provisa.core.secrets_store's ``bound_to_request_org``). The MCP chat request never
    establishes that binding on its own (unlike a GraphQL admin mutation), so this tool binds it
    explicitly."""
    require_role(role, state)
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.kaggle.client import search_datasets

    async with bound_to_request_org():
        token = kaggle_api_token()
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


def _described_role(state: Any, role_id: str) -> dict:  # REQ-2082
    """One role as an agent may know it: its id and what it is for. A derived value role's line is
    generated (REQ-2076); a derived parent's is its declaration's."""
    from provisa.auth.derived_roles import SEPARATOR, declared_derived_role_entries

    held = state.roles.get(role_id)
    if held is not None:
        return {"id": role_id, "description": held["description"]}
    declared = {e.attribute: e.description for e in declared_derived_role_entries(state)}
    attribute, sep, value = role_id.partition(SEPARATOR)
    if sep and attribute in declared:
        return {"id": role_id, "description": f"Held when the user's {attribute} is {value}"}
    return {"id": role_id, "description": declared[role_id]}


def describe_role(state: Any, role: str, switchable: set[str]) -> dict:  # REQ-2082
    """The role the caller acts as -- a meta-role with its members -- and the roles it may switch
    to, each with its description. Nothing about what any role is granted or denied: no row rule,
    mask, grant or condition, and no reason a result was narrowed. A role's data is its whole view,
    so an agent is told who it is, never what it cannot see."""
    out: dict[str, Any] = {"role": _described_role(state, role)}
    members = getattr(state, "meta_roles", {}).get(role)
    if members is not None:
        qualifiers = getattr(state, "meta_qualifiers", {}).get(role, ())
        out["members"] = [_described_role(state, m) for m in (*members, *qualifiers)]
    out["can_switch_to"] = [_described_role(state, r) for r in sorted(switchable)]
    return out


# --- statistics over a SQL-defined dataset (REQ-2090) ---------------------------------------------
#
# Each tool runs the caller's SQL through the ONE governed pipeline as the caller's role -- row
# rules, masks and redaction apply as they do to run_sql -- and answers results, never the rows.
# What the engine can compute (counts, distinct counts, means, deviations, a group's n, mean and
# variance) is asked of it, over every row the role reads; a model fitted here reads at most
# ``mcp.stats_max_rows`` rows, and the answer says when the result held more.

_STATS_ALIAS = "_provisa_stats"


def _stats_max_rows() -> int:
    from provisa.core import settings_registry

    return settings_registry.value("mcp.stats_max_rows")


def _subquery(sql: str) -> str:
    """The caller's SQL as a subquery, as written: SQL the pipeline refuses is refused here as
    run_sql refuses it."""
    body = sql.strip()
    while body.endswith(";"):
        body = body[:-1].rstrip()
    return f"(\n{body}\n) AS {_STATS_ALIAS}"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _stats_rows(state: Any, role: str, sql: str) -> tuple[Any, dict]:
    """At most ``mcp.stats_max_rows`` rows of the caller's result, as a frame, and what was read."""
    from provisa.api.mcp import stats
    from provisa.pgwire._pipeline import execute_sql_batch

    require_role(role, state)
    cap = _stats_max_rows()
    result = await execute_sql_batch(
        f"SELECT * FROM {_subquery(sql)} LIMIT {cap + 1}", role, state, buffered=True
    )
    rows = result.rows[:cap]
    read = {"rows_read": len(rows), "row_cap": cap, "truncated": len(result.rows) > cap}
    return stats.frame(list(result.column_names), rows), read


async def _stats_aggregate(state: Any, role: str, select: str, sql: str, tail: str = "") -> Any:
    from provisa.pgwire._pipeline import execute_sql_batch

    return await execute_sql_batch(
        f"SELECT {select} FROM {_subquery(sql)}{tail}", role, state, buffered=True
    )


async def stats_describe(state: Any, role: str, sql: str) -> dict:
    """Per column: count, missing, distinct, and for a number mean, sd, min and max, computed by
    the engine over the whole result; quantiles and the most frequent values over the rows read."""
    from provisa.api.mcp import stats

    df, read = await _stats_rows(state, role, sql)
    exprs = ["count(*) AS n"]
    for i, col in enumerate(df.columns):
        c = _ident(col)
        exprs += [f"count({c}) AS c{i}", f"count(DISTINCT {c}) AS d{i}"]
        if stats.is_numeric(df[col]):
            x = f"CAST({c} AS DOUBLE PRECISION)"
            exprs += [
                f"avg({x}) AS m{i}",
                f"stddev_samp({x}) AS s{i}",
                f"min({x}) AS lo{i}",
                f"max({x}) AS hi{i}",
            ]
    agg = await _stats_aggregate(state, role, ", ".join(exprs), sql)
    got = dict(zip(agg.column_names, agg.rows[0]))
    total = int(got["n"])
    aggregates = {
        col: {
            "count": got[f"c{i}"],
            "missing": total - int(got[f"c{i}"]),
            "distinct": got[f"d{i}"],
            "mean": got.get(f"m{i}"),
            "sd": got.get(f"s{i}"),
            "min": got.get(f"lo{i}"),
            "max": got.get(f"hi{i}"),
        }
        for i, col in enumerate(df.columns)
    }
    return {
        "method": (
            "count, missing, distinct, mean, sd, min and max computed by the engine over every "
            "row; quantiles and top values over the rows read"
        ),
        "n": total,
        "columns": stats.describe_shape(df, aggregates),
        **read,
    }


async def stats_correlate(
    state: Any, role: str, sql: str, columns: list[str], method: str = "pearson"
) -> dict:
    from provisa.api.mcp import stats

    df, read = await _stats_rows(state, role, sql)
    return {**stats.correlate(df, list(columns), method), **read}


async def stats_regress(
    state: Any, role: str, sql: str, target: str, features: list[str], kind: str = "linear"
) -> dict:
    from provisa.api.mcp import stats

    df, read = await _stats_rows(state, role, sql)
    return {**stats.regress(df, target, list(features), kind), **read}


async def stats_compare(state: Any, role: str, sql: str, value: str, group: str) -> dict:
    """Each group's n, mean and variance, computed by the engine over every row, then Welch's
    t-test (two groups) or one-way ANOVA (more)."""
    from provisa.api.mcp import stats

    require_role(role, state)
    v, g = _ident(value), _ident(group)
    x = f"CAST({v} AS DOUBLE PRECISION)"
    agg = await _stats_aggregate(
        state,
        role,
        f"{g} AS g, count({v}) AS n, avg({x}) AS m, var_samp({x}) AS v",
        sql,
        f" WHERE {v} IS NOT NULL GROUP BY {g}",
    )
    groups = [
        {
            "group": _json_value(r[0]),
            "n": int(r[1]),
            "mean": float(r[2]),
            "var": float(r[3]) if r[3] is not None else 0.0,
        }
        for r in agg.rows
    ]
    return {**stats.compare(groups, value, group), "groups_read": len(groups)}


async def stats_forecast(
    state: Any,
    role: str,
    sql: str,
    time: str,
    value: str,
    horizon: int,
    seasonal_periods: int | None = None,
) -> dict:
    from provisa.api.mcp import stats

    df, read = await _stats_rows(state, role, sql)
    return {**stats.forecast(df, time, value, int(horizon), seasonal_periods), **read}


def _json_value(value: Any) -> Any:
    """A group's value as JSON carries it."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
