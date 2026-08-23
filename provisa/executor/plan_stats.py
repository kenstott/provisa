# Copyright (c) 2026 Kenneth Stott
# Canary: a4e0e944-dc46-414c-9284-23cf87e004f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Plan-derived execution stats for the raw-SQL surfaces (REQ-1517).

The GraphQL surface builds its stats — the per-field source list and the Mermaid execution
DAG — from the compiled field plan (``provisa.api.data.endpoint_helpers``). A raw-SQL
statement has no fields, so that builder has nothing to describe it with, and the SQL
surface reported a single synthetic row labelled ``engine``/``batch`` with no diagram.

This module builds the same two artifacts from the governed plan instead, at the one
terminal every raw-SQL surface passes through. What the plan already carries — the semantic
sources the statement resolved to, the route the router picked and why, the post-governance
optimizations that fired — is exactly what the DAG needs, so the diagram reports the real
plan rather than a surface's guess at it.
"""

from __future__ import annotations

from typing import Any


def _node_id(s: str) -> str:
    return "n_" + "".join(ch if ch.isalnum() else "_" for ch in s)


def _optimization_note(label: str) -> tuple[str, str]:
    """Split an optimization label from ``_optimize_and_route`` into (kind, relation)."""
    kind, _, relation = label.partition(": ")
    return kind, relation


def build_plan_mermaid(
    *,
    sources: frozenset[str],
    source_types: dict[str, str],
    route: str,
    route_reason: str | None,
    optimizations: tuple[str, ...],
    direct_source_id: str | None,
    elapsed_ms: float,
    rows: int,
) -> str:
    """Render the governed plan as a Mermaid ``flowchart LR``.

    Scans on the left, the route node in the middle, the result on the right. An optimization
    that fired is its own node feeding the route, so a relation that never became a scan —
    inlined from a hot table, served from the API cache, dropped with its UNION branch — is
    named rather than silently missing from the diagram.
    """
    lines = ["flowchart LR"]
    optimized_relations = {rel for _, rel in map(_optimization_note, optimizations) if rel}

    route_label = "direct" if route == "DIRECT" else route.lower()
    if route == "DIRECT" and direct_source_id:
        route_node = f'route["{route_label}\\n({direct_source_id})"]'
    else:
        route_node = f'route["{route_label}"]'

    for src_id in sorted(sources):
        src_type = source_types.get(src_id, "")
        nid = _node_id(src_id)
        type_suffix = f"\\n{src_type}" if src_type else ""
        lines.append(f'    {nid}["{src_id}{type_suffix}"]')
        lines.append(f"    {nid} --> route")

    for label in optimizations:
        kind, relation = _optimization_note(label)
        nid = _node_id(f"opt_{relation or kind}")
        lines.append(f'    {nid}[/"{kind}\\n{relation}"/]')
        lines.append(f"    {nid} --> route")

    lines.append(f"    {route_node}")
    lines.append(f'    result(["{rows} rows"])')
    edge_label = route_reason or f"{round(elapsed_ms)}ms"
    lines.append(f'    route -->|"{edge_label}"| result')
    if optimized_relations:
        for label in optimizations:
            kind, relation = _optimization_note(label)
            lines.append(f"    class {_node_id(f'opt_{relation or kind}')} provisaOpt;")
        lines.append(
            "    classDef provisaOpt fill:#1f3b2d,stroke:#4ade80,color:#d1fae5,stroke-width:1px;"
        )
    return "\n".join(lines)


def record_plan_execution(plan: Any, state: Any, *, rows: int, elapsed_ms: float) -> None:
    """Record ``plan``'s execution against the active stats accumulator, diagram included.

    A no-op when the request did not opt into stats (no accumulator on the context var).
    """
    from provisa.executor import stats as _stats

    qs = _stats.current()
    if qs is None or not qs.plan_entries:
        return

    route_name = getattr(plan.route, "name", str(plan.route))
    source_types = getattr(state, "source_types", {}) or {}
    if route_name == "ENGINE":
        source = "engine"
        strategy = "federated:" + (plan.dialect or "engine")
        physical_sql = plan.physical_sql
    else:
        source = plan.source_id
        strategy = f"direct:{source_types.get(plan.source_id, plan.dialect or 'unknown')}"
        physical_sql = plan.sql

    qs.record(
        field=qs.statement_label,
        source=source,
        strategy=strategy,
        elapsed_ms=elapsed_ms,
        rows=rows,
        physical_sql=physical_sql,
    )
    # A batch executes statement-by-statement through this terminal; each statement contributes its
    # own chart, joined the same way the GraphQL builder joins per-root-field charts (the UI splits
    # on the blank line before `flowchart`).
    chart = build_plan_mermaid(
        sources=plan.sources,
        source_types=source_types,
        route=route_name,
        route_reason=plan.route_reason,
        optimizations=plan.optimizations,
        direct_source_id=None if route_name == "ENGINE" else plan.source_id,
        elapsed_ms=elapsed_ms,
        rows=rows,
    )
    qs.mermaid = f"{qs.mermaid}\n\n{chart}" if qs.mermaid else chart
