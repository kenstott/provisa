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

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlglot.errors import SqlglotError

from provisa.lineage.graph import build_column_graph
from provisa.lineage.merge import MergedGraph
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
) -> dict:
    """Build the render-ready lineage graph JSON for ``sql`` (REQ-1160/REQ-1161). Pure core, testable
    without the app: ``commands`` maps command name → its registry dict so inline command nodes splice
    their declared taint-closure. When ``sql`` references a registered view (``views`` = (relation,
    definition) pairs), that view's own definition is expanded and stitched in — so selecting from a
    view or MV shows its FULL lineage down to base sources, not the view as an opaque leaf. Raises
    ValueError on unparseable SQL (surfaced as 422 by the endpoint)."""
    from provisa.lineage.graph import requalify_relations
    from provisa.lineage.merge import build_federation_graph, merge_graphs

    try:
        stmt = build_column_graph(sql, dialect=dialect, commands=commands or {})
    except SqlglotError as exc:
        raise ValueError(f"could not parse SQL for lineage: {exc}") from exc
    view_map = dict(views or [])
    # Transitive closure: a referenced view may itself read other views (pet_store.fun reads
    # pet_store.test). Expand ALL of them, not just the directly-referenced ones, so the statement
    # traces through every intervening view down to the base sources — the FULL lineage.
    closure = _referenced_view_closure(sql, view_map, dialect)
    if not closure:
        return stmt.to_dict()
    # The statement graph names a ``schema.table`` reference by its bare table (sqlglot drops the
    # schema), so ``pet_store.test`` becomes ``test`` and would NOT match the view's qualified output
    # node. Requalify those refs to the full relation first, so the stitch lands.
    requalify_relations(stmt, {rel.split(".")[-1]: rel for rel, _ in closure})
    # Expand each view to its own lineage (down to base sources), then stitch the statement on top: a
    # view's output node ``<schema>.<table>.<col>`` shares the id the statement (or a downstream view)
    # reads it by, so merge_graphs connects them. A ``SELECT *`` (empty statement graph) simply yields
    # the view's lineage — exactly "the lineage of the columns in this view".
    fed = build_federation_graph(
        closure, commands=commands or {}, materialized_relations=materialized or set()
    )
    return merge_graphs([fed.graph, stmt]).to_dict()


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
async def lineage_graph(body: LineageGraphRequest) -> dict:
    """Return the column-level lineage DAG (nodes + edges + outputs) for a SQL statement (REQ-1160)."""
    from provisa.api.app import state

    commands = getattr(state, "tracked_functions", None) or {}
    view_rows = await _fetch_view_rows(state)
    views, mats = _registry_views(view_rows, getattr(state, "mv_registry", None))
    try:
        return lineage_graph_for(body.sql, commands, body.dialect, views=views, materialized=mats)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


async def _fetch_view_rows(state) -> list[dict]:
    """Semantic view definitions from ``registered_tables`` — the AUTHORED SQL and USER-FACING names.

    Deliberately NOT ``state.view_sql_map``: that map is rewritten to a physical plan at startup
    (materialized targets like ``mv_test``, rewritten source refs), which is exactly what must not
    surface in lineage. The registry rows carry the semantic SQL plus the view's domain, so the graph
    speaks the names the user defined (``<domain>.<table>``, e.g. ``pet_store.test``)."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables

    if getattr(state, "tenant_db", None) is None:
        return []
    async with state.tenant_db.acquire() as conn:
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

    One row per (relation, column): ``domain_id``, ``table_name``, ``column_name``, ``visible_to``.
    This is the registry's whole surface — base tables as well as views — so Complete Lineage can show
    a registered table that nothing derives from, and can seed the role cut from what a role may query."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables, table_columns

    if getattr(state, "tenant_db", None) is None:
        return []
    async with state.tenant_db.acquire() as conn:
        res = await conn.execute_core(
            select(
                registered_tables.c.domain_id,
                registered_tables.c.table_name,
                table_columns.c.column_name,
                table_columns.c.visible_to,
            ).select_from(
                registered_tables.join(
                    table_columns, table_columns.c.table_id == registered_tables.c.id
                )
            )
        )
        return [dict(r._mapping) for r in res.fetchall()]


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
        seeds.add(f"{relation}.{row['column_name']}")
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
    everything the columns THOSE roles can query derive from. It is a lens, not a redaction — an ancestor
    is returned in full whether or not the roles reach it — so an analyst holding several roles reads the
    same federation from each one's vantage point. Empty = the "All roles" selection, every registered
    column seeds. Registered relations that nothing derives from still appear, as isolated source columns
    (REQ-1626), so the graph is the whole reachable model rather than only what a transform touched.

    ``roles`` names ANY role in the org, not only ones the caller holds — that is the point of the lens.
    Reading the federation from an arbitrary role's vantage point discloses which columns that role can
    query, which is the same disclosure ``visible_to`` is, so the endpoint carries the gate that
    governance metadata carries: ``view_governance`` (REQ-1628)."""
    from provisa.api.admin.capabilities import require_capability_request
    from provisa.api.app import state
    from provisa.lineage.merge import (
        ancestor_closure,
        build_federation_graph_incremental,
        slice_graph,
    )

    require_capability_request(request, "view_governance")

    commands = getattr(state, "tracked_functions", None) or {}
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
    if seeds:
        scoped_graph = ancestor_closure(merged.graph, seeds)
        # A cycle survives only if the whole loop is inside the perspective — a partially-cut cycle is
        # not a cycle in what is being shown, and reporting it would point at nodes that are not there.
        merged = MergedGraph(
            graph=scoped_graph,
            cycles=[c for c in merged.cycles if all(n in scoped_graph.nodes for n in c.nodes)],
        )
    if focus is None:
        return merged.to_dict()
    try:
        scoped = slice_graph(merged.graph, focus, direction=direction, depth=depth)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    out = scoped.to_dict()
    kept = set(scoped.nodes)
    out["cycles"] = [c.to_dict() for c in merged.cycles if any(n in kept for n in c.nodes)]
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
        node_id = f"{relation}.{row['column_name']}"
        if node_id in graph.nodes:
            continue
        graph.nodes[node_id] = Node(
            id=node_id,
            column=row["column_name"],
            relation=relation,
            kind="source",
            materialized=False,
        )
    return merged
