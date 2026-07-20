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

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlglot.errors import SqlglotError

from provisa.lineage.graph import build_column_graph

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
    referenced = [(rel, view_map[rel]) for rel in _referenced_relations(sql, dialect) if rel in view_map]
    if not referenced:
        return stmt.to_dict()
    # The statement graph names a ``schema.table`` reference by its bare table (sqlglot drops the
    # schema), so ``pet_store.test`` becomes ``test`` and would NOT match the view's qualified output
    # node. Requalify those refs to the full relation first, so the stitch lands.
    requalify_relations(stmt, {rel.split(".")[-1]: rel for rel, _ in referenced})
    # Expand each referenced view to its own lineage (down to base sources), then stitch the statement
    # on top: a view's output node ``<schema>.<table>.<col>`` shares the id the statement reads it by,
    # so merge_graphs connects them. A ``SELECT *`` (empty statement graph) simply yields the view's
    # lineage — exactly "the lineage of the columns in this view".
    fed = build_federation_graph(
        referenced, commands=commands or {}, materialized_relations=materialized or set()
    )
    return merge_graphs([fed.graph, stmt]).to_dict()


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
                registered_tables.c.source_id == "__provisa__",
                registered_tables.c.view_sql.is_not(None),
            )
        )
        return [dict(r._mapping) for r in res.fetchall()]


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
    focus: str | None = None,
    direction: str = "both",
    depth: int | None = None,
    domains: str | None = None,
) -> dict:
    """Return the federation-wide merged provenance graph over all MV/view definitions (REQ-1161).

    Cycles are characterized (feedback vs error). At federation scale pass ``focus`` (a node id) with
    ``direction`` (upstream|downstream|both) and optional ``depth`` to scope the returned sub-graph —
    the graph is computed whole but rendered progressively. ``domains`` is a comma-separated list of
    domain ids that restricts the graph to views in those domains (empty = every domain), matching the
    NavBar domain filter the Views/Commands pages honour."""
    from provisa.api.app import state
    from provisa.lineage.merge import build_federation_graph, slice_graph

    commands = getattr(state, "tracked_functions", None) or {}
    view_rows = await _fetch_view_rows(state)
    domain_filter = {d for d in (domains or "").split(",") if d}
    if domain_filter:
        view_rows = [r for r in view_rows if r["domain_id"] in domain_filter]
    views, mats = _registry_views(view_rows, getattr(state, "mv_registry", None))
    merged = build_federation_graph(views, commands=commands, materialized_relations=mats)
    if focus is None:
        return merged.to_dict()
    try:
        scoped = slice_graph(merged.graph, focus, direction=direction, depth=depth)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    out = scoped.to_dict()
    kept = set(scoped.nodes)
    out["cycles"] = [
        c.to_dict() for c in merged.cycles if any(n in kept for n in c.nodes)
    ]
    return out
