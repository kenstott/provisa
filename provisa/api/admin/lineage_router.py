# Copyright (c) 2026 Kenneth Stott
# Canary: a5f0c9e2-7d14-4b63-8a02-3e6f1c9d5b70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column-level lineage graph endpoint (REQ-1160).

Returns the full node+edge lineage DAG for a SQL statement (or a registered view's definition),
computed STATICALLY from the definition plus each command's declared I/O contract — command boundaries
are first-class, non-opaque nodes. The payload is render-ready graph JSON for the UI DAG viz.
"""

from __future__ import annotations

import dataclasses

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlglot.errors import SqlglotError

from provisa.api.admin.capabilities import has_capability_request, require_capability_request
from provisa.lineage.graph import LineageGraph, build_column_graph
from provisa.lineage.merge import MergedGraph
from provisa.lineage.native import NativeTable, natives_payload, sql_column_name, with_native_hop
from provisa.lineage.read_rights import readable_graph
from provisa.core.models import DERIVED_SOURCE_ID

router = APIRouter()


class LineageGraphRequest(BaseModel):
    sql: str
    dialect: str = "postgres"


def _referenced_relations(sql: str, dialect: str) -> set[str]:
    """The relation names a statement reads, as ``<schema>.<table>`` (or bare) — used to detect when a
    statement references a registered view so that view's lineage can be spliced in."""
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except SqlglotError:
        return set()
    return {f"{t.db}.{t.name}" if t.db else t.name for t in tree.find_all(exp.Table)}


def lineage_graph_for(
    sql: str,
    commands: dict[str, dict] | None,
    dialect: str = "postgres",
    *,
    views: list[tuple[str, str]] | None = None,
    materialized: set[str] | None = None,
    natives: list[NativeTable] | None = None,
    relations: list[str] | None = None,
) -> dict:
    """The render-ready lineage graph JSON for ``sql``: :func:`statement_graph`'s graph with every
    native box it draws."""
    graph = statement_graph(
        sql,
        commands,
        dialect,
        views=views,
        materialized=materialized,
        natives=natives,
        relations=relations,
    )
    return _payload(graph, natives_payload(graph, natives or []))


def statement_graph(
    sql: str,
    commands: dict[str, dict] | None,
    dialect: str = "postgres",
    *,
    views: list[tuple[str, str]] | None = None,
    materialized: set[str] | None = None,
    natives: list[NativeTable] | None = None,
    relations: list[str] | None = None,
) -> LineageGraph:
    """Build the lineage graph for ``sql`` (REQ-1160/REQ-1161). Pure core, testable
    without the app: ``commands`` maps command name → its registry dict so inline command nodes splice
    their declared taint-closure. When ``sql`` references a registered view (``views`` = (relation,
    definition) pairs), that view's own definition is expanded and stitched in — so selecting from a
    view or MV shows its FULL lineage down to base sources, not the view as an opaque leaf.
    ``natives`` are the base tables' native hops (REQ-2116), drawn beneath their source columns.
    ``relations`` are the registered base tables, by which a bare table reference is named in full
    (as the federation graph names it) so it is the registry's own node.
    Raises ValueError on unparseable SQL (surfaced as 422 by the endpoint)."""
    from provisa.lineage.graph import requalify_relations
    from provisa.lineage.merge import _bare_to_full, build_federation_graph, merge_graphs

    try:
        stmt = build_column_graph(sql, dialect=dialect, commands=commands or {})
    except SqlglotError as exc:
        raise ValueError(f"could not parse SQL for lineage: {exc}") from exc
    view_map = dict(views or [])
    # Transitive closure: a referenced view may itself read other views (pet_store.fun reads
    # pet_store.test). Expand ALL of them, not just the directly-referenced ones, so the statement
    # traces through every intervening view down to the base sources — the FULL lineage.
    closure = _referenced_view_closure(sql, view_map, dialect)
    # The statement graph names a ``schema.table`` reference by its bare table (sqlglot drops the
    # schema), so ``pet_store.test`` becomes ``test`` and would NOT match the view's qualified output
    # node. Requalify those refs to the full relation first, so the stitch lands.
    requalify_relations(stmt, _bare_to_full([rel for rel, _ in closure], relations or []))
    if not closure:
        with_native_hop(stmt, natives or [])
        return stmt
    # Expand each view to its own lineage (down to base sources), then stitch the statement on top: a
    # view's output node ``<schema>.<table>.<col>`` shares the id the statement (or a downstream view)
    # reads it by, so merge_graphs connects them. A ``SELECT *`` (empty statement graph) simply yields
    # the view's lineage — exactly "the lineage of the columns in this view".
    fed = build_federation_graph(
        closure,
        commands=commands or {},
        materialized_relations=materialized or set(),
        extra_relations=relations or [],
    )
    graph = merge_graphs([fed.graph, stmt]).graph
    with_native_hop(graph, natives or [])
    return graph


def _payload(graph: LineageGraph, natives: dict[str, dict]) -> dict:
    out = graph.to_dict()
    out["natives"] = natives
    return out


def _natives_for(request: Request, graph: LineageGraph, natives: list[NativeTable]) -> dict:
    """The native boxes ``graph`` draws, with their address and how each table is served: source
    configuration, so answered only to a user with source registration (REQ-2116). The native
    nodes themselves follow the read rights of the columns they feed (:func:`caller_lineage`)."""
    if not has_capability_request(request, "source_registration"):
        return {}
    return natives_payload(graph, natives)


def caller_lineage(
    request: Request, state, graph: LineageGraph, column_rows: list[dict]
) -> LineageGraph:
    """``graph`` cut to what the caller reads: the registered columns the role the request acts
    as is served (its compiled context, the narrowing every schema surface reads), the native
    fields beneath them, and nothing else (REQ-1161). Every lineage read answers through this."""
    registered, readable = _caller_reads(request, state, column_rows)
    return readable_graph(graph, registered=registered, readable=readable)


def _caller_reads(request: Request, state, column_rows: list[dict]) -> tuple[set[str], set[str]]:
    """(the registered relations, the node ids of their columns the caller is served)."""
    from provisa.api.admin.served import served_columns

    served = served_columns(request, state)
    registered: set[str] = set()
    readable: set[str] = set()
    for row in column_rows:
        relation = _relation_of(row["domain_id"], row["table_name"])
        registered.add(relation)
        if row["column_name"] in served.get(row["table_id"], ()):
            readable.add(f"{relation}.{sql_column_name(row)}")
    return registered, readable


def _referenced_view_closure(
    sql: str, view_map: dict[str, str], dialect: str
) -> list[tuple[str, str]]:
    """(relation, definition) for every view the statement reads, TRANSITIVELY — a referenced view's
    own view references are followed so the whole chain expands to base sources (REQ-1161)."""
    seen: set[str] = set()
    stack = [r for r in _referenced_relations(sql, dialect) if r in view_map]
    closure: list[tuple[str, str]] = []
    while stack:
        rel = stack.pop()
        if rel in seen:
            continue
        seen.add(rel)
        closure.append((rel, view_map[rel]))
        stack.extend(
            r
            for r in _referenced_relations(view_map[rel], dialect)
            if r in view_map and r not in seen
        )
    return closure


@router.post("/admin/lineage/graph")
async def lineage_graph(request: Request, body: LineageGraphRequest) -> dict:
    """Return the column-level lineage DAG (nodes + edges + outputs) for a SQL statement (REQ-1160)."""
    require_capability_request(request, "view_governance")
    from provisa.api.app import state

    # REQ-2131: a column transform is a named step of the projections that apply it.
    commands = {
        **(getattr(state, "tracked_functions", None) or {}),
        **(getattr(state, "column_transforms", None) or {}),
    }
    view_rows = await _fetch_view_rows(state)
    views, mats = _registry_views(view_rows, getattr(state, "mv_registry", None))
    natives = await _native_tables(state)
    column_rows = await _fetch_registry_columns(state)
    relations = sorted({_relation_of(r["domain_id"], r["table_name"]) for r in column_rows})
    try:
        graph = statement_graph(
            body.sql,
            commands,
            body.dialect,
            views=views,
            materialized=mats,
            natives=natives,
            relations=relations,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    graph = caller_lineage(request, state, graph, column_rows)
    return _payload(graph, _natives_for(request, graph, natives))


async def _fetch_view_rows(state) -> list[dict]:
    """Semantic view definitions from ``registered_tables`` — the AUTHORED SQL and USER-FACING names.

    Deliberately NOT ``state.view_sql_map``: that map is rewritten to a physical plan at startup
    (materialized targets like ``mv_test``, rewritten source refs), which is exactly what must not
    surface in lineage. The registry rows carry the semantic SQL plus the view's domain, so the graph
    speaks the names the user defined (``<domain>.<table>``, e.g. ``pet_store.test``)."""
    if getattr(state, "model_db", None) is None:
        return []
    async with state.model_db.acquire() as conn:
        return await _view_rows(conn)


async def _view_rows(conn) -> list[dict]:
    """The semantic view definitions (:func:`_fetch_view_rows`), read on ``conn``."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables

    res = await conn.execute_core(
        select(
            registered_tables.c.domain_id,
            registered_tables.c.table_name,
            registered_tables.c.view_sql,
        ).where(
            registered_tables.c.source_id == DERIVED_SOURCE_ID,
            registered_tables.c.view_sql.is_not(None),
        )
    )
    return [dict(r._mapping) for r in res.fetchall()]


async def _fetch_registry_columns(state) -> list[dict]:
    """Every registered relation's columns with their per-role visibility (REQ-1625/REQ-1626).

    One row per (relation, column): ``domain_id``, ``table_name``, ``column_name``, ``alias``,
    ``visible_to``. A column's node is named by its SQL name (``sql_column_name``), the name a view
    reads it by, so the registry's column and a view's reference to it are one node.
    This is the registry's whole surface — base tables as well as views — so Complete Lineage can show
    a registered table that nothing derives from, and can seed the role cut from what a role may query."""
    if getattr(state, "model_db", None) is None:
        return []
    async with state.model_db.acquire() as conn:
        return await _registry_columns(conn)


async def _registry_columns(conn) -> list[dict]:
    """The registry's columns (:func:`_fetch_registry_columns`), read on ``conn``, each with its
    table's id."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables, table_columns

    res = await conn.execute_core(
        select(
            registered_tables.c.domain_id,
            registered_tables.c.table_name,
            table_columns.c.column_name,
            table_columns.c.alias,
            table_columns.c.visible_to,
            table_columns.c.table_id,
        ).select_from(
            registered_tables.join(
                table_columns, table_columns.c.table_id == registered_tables.c.id
            )
        )
    )
    return [dict(r._mapping) for r in res.fetchall()]


async def write_lineage_edges(conn, state) -> None:
    """Replace the lineage edge table with the model's column lineage edges (REQ-2129).

    Run by every model build. The inputs are the federation graph's (GET /admin/lineage/federation):
    every registered view definition, the served commands, the registry's relations. The edges are
    folded from the same per-view sub-DAGs that graph is the union of, so the two never differ. An
    end that is a registered column is stored with its table and its stored name."""
    from sqlalchemy import delete, insert

    from provisa.core.schema_org import lineage_edges
    from provisa.lineage.commands import command_write_graphs
    from provisa.lineage.edges import folded_edges
    from provisa.lineage.merge import federation_subdags

    commands = state.tracked_functions
    column_rows = await _registry_columns(conn)
    views, mats = _registry_views(await _view_rows(conn), getattr(state, "mv_registry", None))
    registry_relations = sorted(
        {_relation_of(r["domain_id"], r["table_name"]) for r in column_rows}
    )
    edges = folded_edges(
        federation_subdags(
            views,
            commands=commands,
            materialized_relations=mats,
            extra_relations=registry_relations,
        ),
        command_write_graphs(commands),
    )
    registered = {
        (_relation_of(r["domain_id"], r["table_name"]), sql_column_name(r)): (
            r["table_id"],
            r["column_name"],
        )
        for r in column_rows
    }
    rows = []
    for e in sorted(edges, key=lambda e: dataclasses.astuple(e)):
        source = registered.get((e.source_relation, e.source_column), (None, None))
        target = registered.get((e.target_relation, e.target_column), (None, None))
        rows.append(
            {
                **dataclasses.asdict(e),
                "source_table_id": source[0],
                "source_column_name": source[1],
                "target_table_id": target[0],
                "target_column_name": target[1],
            }
        )
    await conn.execute_core(delete(lineage_edges))
    if rows:
        await conn.execute_core(insert(lineage_edges).values(rows))


def _relation_of(domain_id: str, table_name: str) -> str:
    """A relation's SQL-addressable name ``<domain>.<table>`` — the lineage node id prefix."""
    from provisa.compiler.naming import domain_to_sql_name

    return f"{domain_to_sql_name(domain_id)}.{table_name}"


def _role_seeds(
    column_rows: list[dict], roles: set[str], domains: set[str]
) -> tuple[set[str], set[str]]:
    """(seed node ids, seed relations) for the role perspective (REQ-1625).

    Complete Lineage is read FROM a role's vantage point: the seeds are the columns that role can
    query, and the graph is everything they derive from. A governance analyst holding several roles
    switches perspective by switching role, so the cut is analytical, not protective — nothing about an
    ancestor is withheld once a seed reaches it. Role membership is exact (``role in visible_to``), the
    same test the schema surfaces use (``provisa/security/visibility.py:54``); an empty ``roles`` is the
    "All roles" selection and seeds every registered column. ``domains`` restricts which relations may
    SEED — never which may be an ancestor, or a chain would break mid-way through another domain."""
    seeds: set[str] = set()
    relations: set[str] = set()
    for row in column_rows:
        if domains and row["domain_id"] not in domains:
            continue
        visible_to = row["visible_to"] or []
        if roles and not roles.intersection(visible_to):
            continue
        relation = _relation_of(row["domain_id"], row["table_name"])
        relations.add(relation)
        seeds.add(f"{relation}.{sql_column_name(row)}")
    return seeds, relations


def _view_relation(row: dict) -> str:
    """A view's SQL-addressable relation ``<domain>.<table>`` — exactly how a query references it
    (the domain is exposed as a SQL schema via ``domain_to_sql_name``, e.g. ``pet-store`` →
    ``pet_store``), so a statement's reference to the view stitches to this same node id."""
    from provisa.compiler.naming import domain_to_sql_name

    return f"{domain_to_sql_name(row['domain_id'])}.{row['table_name']}"


def _registry_views(view_rows: list[dict], mv_registry) -> tuple[list[tuple[str, str]], set[str]]:
    """(views as (relation, sql), materialized relation names) over EVERY registered view (REQ-1161).

    ``view_rows`` are the semantic definitions (schema_name, table_name, view_sql). The relation is
    the SQL-addressable user-facing name ``<schema>.<table>`` — never the physical materialized
    target. The MV registry contributes nothing new to the node set; it only marks which of those
    relations are materialization boundaries, so cycle characterization sees the version cuts. A
    deployment with no MVs still yields a full graph as long as views exist."""
    views: list[tuple[str, str]] = []
    name_to_relation: dict[str, str] = {}
    for r in view_rows:
        if not r.get("view_sql"):
            continue
        relation = _view_relation(r)
        name_to_relation[r["table_name"]] = relation
        views.append((relation, r["view_sql"]))
    mats: set[str] = set()
    for mv in mv_registry.all() if mv_registry is not None else []:
        # MV id is "view-<table>"; the materialization boundary is that view's user-facing relation.
        if mv.id.startswith("view-"):
            bare = mv.id[len("view-") :]
            if bare in name_to_relation:
                mats.add(name_to_relation[bare])
    return views, mats


@router.get("/admin/lineage/federation")
async def federation_graph(
    request: Request,
    focus: str | None = None,
    direction: str = "both",
    depth: int | None = None,
    domains: str | None = None,
    roles: str | None = None,
) -> dict:
    """Return the federation-wide merged provenance graph over all MV/view definitions (REQ-1161).

    Cycles are characterized (feedback vs error). At federation scale pass ``focus`` (a node id) with
    ``direction`` (upstream|downstream|both) and optional ``depth`` to scope the returned sub-graph —
    the graph is computed whole but rendered progressively. ``domains`` is a comma-separated list of
    domain ids that restricts the graph to views in those domains (empty = every domain), matching the
    NavBar domain filter the Views/Commands pages honour.

    ``roles`` (comma-separated, REQ-1625) is the perspective the lineage is read from: the graph becomes
    everything the columns THOSE roles can query derive from, so an analyst holding several roles reads
    the same federation from each one's vantage point. The lens narrows only within what the caller
    reads: every field the caller is not served is cut, whatever the lens (:func:`caller_lineage`).
    Empty = the "All roles" selection, every registered column seeds. Registered relations that nothing derives from still appear, as isolated source columns
    (REQ-1626), so the graph is the whole reachable model rather than only what a transform touched.

    ``roles`` names ANY role in the org, not only ones the caller holds — that is the point of the lens.
    Reading the federation from an arbitrary role's vantage point discloses which columns that role can
    query, which is the same disclosure ``visible_to`` is, so the endpoint carries the gate that
    governance metadata carries: ``view_governance`` (REQ-1628)."""
    from provisa.api.app import state
    from provisa.lineage.merge import (
        ancestor_closure,
        build_federation_graph_incremental,
        slice_graph,
    )

    require_capability_request(request, "view_governance")

    # REQ-2131: a column transform is a named step of the projections that apply it.
    commands = {
        **(getattr(state, "tracked_functions", None) or {}),
        **(getattr(state, "column_transforms", None) or {}),
    }
    view_rows = await _fetch_view_rows(state)
    column_rows = await _fetch_registry_columns(state)
    domain_filter = {d for d in (domains or "").split(",") if d}
    role_filter = {r for r in (roles or "").split(",") if r}
    seeds, seed_relations = _role_seeds(column_rows, role_filter, domain_filter)
    # Views are parsed WHOLE, unfiltered: the domain/role cut selects the seeds, and an upstream view in
    # a domain outside the filter is still a legitimate ancestor of one inside it. Filtering the parse
    # set instead would sever those chains and understate provenance.
    views, mats = _registry_views(view_rows, getattr(state, "mv_registry", None))
    registry_relations = sorted(
        {_relation_of(r["domain_id"], r["table_name"]) for r in column_rows}
    )
    # REQ-1161: incremental — only views whose SQL changed since the last request are re-parsed; the
    # rest of the federation graph is unioned from cached per-view sub-DAGs (never a full rebuild).
    merged = build_federation_graph_incremental(
        views, commands=commands, materialized_relations=mats, extra_relations=registry_relations
    )
    from provisa.lineage.commands import with_command_writes

    # REQ-2001: each statement command's write, its target's columns derived by the command.
    merged = with_command_writes(merged, commands)
    merged = _with_registry_columns(merged, column_rows, seed_relations)
    # A relation the registry has no column rows for cannot be seeded by visibility, and dropping it
    # would silently shrink the graph below what it shows today. Seed it: the perspective can only be
    # applied where the registry actually describes who sees what.
    described = set(registry_relations)
    seeds |= {
        n.id
        for n in merged.graph.nodes.values()
        if n.relation is None or n.relation not in described
    }
    # The lens narrows within what the caller reads, never past it: a registered seed the caller is
    # not served is no seed.
    registered, readable = _caller_reads(request, state, column_rows)
    seeds = {
        s
        for s in seeds
        if s in readable
        or merged.graph.nodes.get(s) is None
        or merged.graph.nodes[s].relation not in registered
    }
    # REQ-2116: the native hop joins after the seeds are taken: it is what a seed derives from.
    natives = await _native_tables(state)
    with_native_hop(merged.graph, natives)
    graph = ancestor_closure(merged.graph, seeds)
    # The caller's read rights cap the lens: the graph is what the lens reaches, of what the caller
    # reads. A cycle survives only if the whole loop is shown — a partially-cut cycle is not a cycle
    # in what is being shown, and reporting it would point at nodes that are not there.
    graph = caller_lineage(request, state, graph, column_rows)
    merged = MergedGraph(
        graph=graph, cycles=[c for c in merged.cycles if all(n in graph.nodes for n in c.nodes)]
    )
    if focus is None:
        out = merged.to_dict()
        out["natives"] = _natives_for(request, merged.graph, natives)
        return out
    try:
        scoped = slice_graph(merged.graph, focus, direction=direction, depth=depth)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    out = scoped.to_dict()
    kept = set(scoped.nodes)
    out["cycles"] = [c.to_dict() for c in merged.cycles if any(n in kept for n in c.nodes)]
    out["natives"] = _natives_for(request, scoped, natives)
    return out


def _with_registry_columns(
    merged: "MergedGraph", column_rows: list[dict], seed_relations: set[str]
) -> "MergedGraph":
    """Add every registered column the parsed views never mentioned (REQ-1626).

    A registered table with nothing derived from it contributes no lineage edge, so the merge alone
    leaves it out and the graph reads as though the model were only its transforms. Each missing column
    of a seed relation joins as an isolated ``source`` node — present, addressable, and collapsed by the
    UI until asked for. Relations outside the seed set are skipped: they are neither a seed nor,
    lacking any edge, an ancestor of one."""
    import copy

    from provisa.lineage.graph import Node

    # build_federation_graph_incremental hands back its CACHED merged graph; adding nodes in place would
    # leak this request's registry set into every later request's graph. Copy before touching it.
    graph = copy.deepcopy(merged.graph)
    merged = MergedGraph(graph=graph, cycles=merged.cycles)
    for row in column_rows:
        relation = _relation_of(row["domain_id"], row["table_name"])
        if relation not in seed_relations:
            continue
        column = sql_column_name(row)
        node_id = f"{relation}.{column}"
        if node_id in graph.nodes:
            continue
        graph.nodes[node_id] = Node(
            id=node_id,
            column=column,
            relation=relation,
            kind="source",
            materialized=False,
        )
    return merged


async def _native_tables(state) -> list[NativeTable]:
    """Every registered base table's native hop (REQ-2116): where its source reads it, the raw
    field of each column and how the table is served. Which nodes a caller is shown is
    :func:`caller_lineage`'s; the boxes' detail is :func:`_natives_for`'s."""
    from sqlalchemy import select

    from provisa.core.schema_org import api_endpoints, kafka_topics
    from provisa.federation.replica_routing import resolved_cache_ttl, served_tables
    from provisa.federation.replica_state import read_all
    from provisa.lineage.native import native_address, native_table, served

    if getattr(state, "model_db", None) is None:
        return []
    registered, sources, replicas = await served_tables(state)
    async with state.model_db.acquire() as conn:
        endpoints = {
            (r.source_id, r.table_name): dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(
                        api_endpoints.c.source_id,
                        api_endpoints.c.table_name,
                        api_endpoints.c.method,
                        api_endpoints.c.path,
                        api_endpoints.c.default_params,
                        api_endpoints.c.response_root,
                    )
                )
            ).fetchall()
        }
        topics = {
            (r.source_id, r.table_name): r.topic
            for r in (
                await conn.execute_core(
                    select(
                        kafka_topics.c.source_id, kafka_topics.c.table_name, kafka_topics.c.topic
                    )
                )
            ).fetchall()
        }
    records = {}
    if replicas:
        async with state.tenant_db.acquire() as conn:
            records = {r.key: r for r in await read_all(conn)}
    graphql = getattr(state, "graphql_remote_sources", {}) or {}
    out: list[NativeTable] = []
    for reg in registered:
        source = sources.get(reg["source_id"])
        if source is None:  # a view, or a built-in source: nothing native beneath it
            continue
        key = (reg["source_id"], reg["schema_name"], reg["table_name"])
        spec = next(
            (
                t
                for t in (graphql.get(source.id) or {}).get("tables", [])
                if t.get("sql_name") == reg["table_name"]
            ),
            None,
        )
        stype = source.type.value if hasattr(source.type, "value") else str(source.type)
        address = native_address(
            stype,
            reg,
            source,
            endpoint=endpoints.get((source.id, reg["table_name"])),
            graphql=spec,
            topic=topics.get((source.id, reg["table_name"])),
        )
        record = records.get(key)
        serving = served(
            key in replicas,
            resolved_cache_ttl(source, reg),
            record.current_at if record is not None else None,
            record.next_refresh_at if record is not None else None,
        )
        relation = _relation_of(reg["domain_id"], reg["table_name"])
        out.append(native_table(relation, source, reg, address, serving))
    return out
