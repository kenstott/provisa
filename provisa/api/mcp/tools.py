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

import asyncio
import os
from typing import Any

from provisa.api.flight.catalog import CatalogTable, build_catalog_tables

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

    build_catalog_tables is sync and drives its own event loop, so it runs in a worker
    thread to avoid nesting inside the MCP async loop.
    """
    raw = await asyncio.to_thread(build_catalog_tables, state)
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


def _resolve_embedding_model(state: Any) -> Any:
    """The embedding model for catalog search — the first enabled ``vector_models`` entry.

    No silent fallback (CLAUDE.md): with no enabled embedding model registered, catalog
    search is unavailable and says so, rather than inventing a model.
    """
    from provisa.vector.registry import VectorModel

    config = getattr(state, "config", None)
    for vm in getattr(config, "vector_models", None) or []:
        if getattr(vm, "enabled", True):
            return VectorModel(
                id=vm.id,
                provider=vm.provider,
                dimensions=vm.dimensions,
                base_url=getattr(vm, "base_url", None),
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

    model = _resolve_embedding_model(state)
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
