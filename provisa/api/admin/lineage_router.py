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


def lineage_graph_for(sql: str, commands: dict[str, dict] | None, dialect: str = "postgres") -> dict:
    """Build the render-ready lineage graph JSON for ``sql`` (REQ-1160). Pure core, testable without
    the app: ``commands`` maps command name → its registry dict so inline command nodes splice their
    declared taint-closure. Raises ValueError on unparseable SQL (surfaced as 422 by the endpoint)."""
    try:
        graph = build_column_graph(sql, dialect=dialect, commands=commands or {})
    except SqlglotError as exc:
        raise ValueError(f"could not parse SQL for lineage: {exc}") from exc
    return graph.to_dict()


@router.post("/admin/lineage/graph")
async def lineage_graph(body: LineageGraphRequest) -> dict:
    """Return the column-level lineage DAG (nodes + edges + outputs) for a SQL statement (REQ-1160)."""
    from provisa.api.app import state

    commands = getattr(state, "tracked_functions", None) or {}
    try:
        return lineage_graph_for(body.sql, commands, body.dialect)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _registry_views(state) -> tuple[list[tuple[str, str]], set[str]]:
    """(views as (relation, sql), materialized relation names) from the MV registry (REQ-1161).

    Every MV is a materialized version boundary; its ``target_table`` (or id) is the relation name
    downstream statements reference, so it is both a view input and a materialized node."""
    views: list[tuple[str, str]] = []
    mats: set[str] = set()
    registry = getattr(state, "mv_registry", None)
    for mv in registry.all() if registry is not None else []:
        if mv.sql:
            relation = mv.target_table or mv.id
            views.append((relation, mv.sql))
            mats.add(relation)
    return views, mats


@router.get("/admin/lineage/federation")
async def federation_graph(
    focus: str | None = None, direction: str = "both", depth: int | None = None
) -> dict:
    """Return the federation-wide merged provenance graph over all MV/view definitions (REQ-1161).

    Cycles are characterized (feedback vs error). At federation scale pass ``focus`` (a node id) with
    ``direction`` (upstream|downstream|both) and optional ``depth`` to scope the returned sub-graph —
    the graph is computed whole but rendered progressively."""
    from provisa.api.app import state
    from provisa.lineage.merge import build_federation_graph, slice_graph

    commands = getattr(state, "tracked_functions", None) or {}
    views, mats = _registry_views(state)
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
