# Copyright (c) 2026 Kenneth Stott
# Canary: 03c91509-ed7f-4455-ad86-e218aecf4ebb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""EXPLAIN of a governed plan, and the Provisa annotations that ride on it (REQ-1519).

The statement a source or the federation engine finally sees is the last of several rewrites —
governance, semantic lowering, hot-table inlining, API-cache substitution, branch dropping,
routing. An engine's own EXPLAIN describes only that last form, so a user reading it cannot tell
why a table they wrote is missing from the scan list. This module renders both halves of that
story from ONE artifact: the engine's plan tree is the spine, and the Provisa rewrites that
produced the statement it explains are attached as their own nodes above it.

The EXPLAIN itself is not a second pipeline — ``_govern_and_route(..., explain=...)`` wraps the
final SQL at the bottom of the ONE pipeline, so the explained statement is the governed,
optimized, routed statement, and it executes through the same terminal with the same audit,
tier ceilings and provenance stamp.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any

# --------------------------------------------------------------------------- #
# Dialect syntax
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ExplainSyntax:
    """How one dialect spells EXPLAIN, and what shape its answer comes back in."""

    prefix: str
    analyze_prefix: str | None  # None when the dialect cannot time a real run
    fmt: str  # postgres_json | named_json | sqlite_qp | text_indent
    # Some dialects answer EXPLAIN ANALYZE in a different shape than EXPLAIN — Trino and MySQL
    # take no FORMAT option on the timed form and reply with indented text.
    analyze_fmt: str | None = None

    def format_for(self, *, analyze: bool) -> str:
        return (self.analyze_fmt or self.fmt) if analyze else self.fmt


# Only dialects whose EXPLAIN output this module can actually read are listed. A source whose
# dialect is absent is reported to the caller as unsupported — never explained by guesswork.
_SYNTAX: dict[str, ExplainSyntax] = {
    "postgres": ExplainSyntax(
        "EXPLAIN (FORMAT JSON) ", "EXPLAIN (ANALYZE, FORMAT JSON) ", "postgres_json"
    ),
    "postgresql": ExplainSyntax(
        "EXPLAIN (FORMAT JSON) ", "EXPLAIN (ANALYZE, FORMAT JSON) ", "postgres_json"
    ),
    "duckdb": ExplainSyntax(
        "EXPLAIN (FORMAT json) ", "EXPLAIN (ANALYZE, FORMAT json) ", "named_json"
    ),
    "trino": ExplainSyntax(
        "EXPLAIN (FORMAT JSON) ", "EXPLAIN ANALYZE ", "named_json", analyze_fmt="text_indent"
    ),
    "sqlite": ExplainSyntax("EXPLAIN QUERY PLAN ", None, "sqlite_qp"),
    "mysql": ExplainSyntax(
        "EXPLAIN FORMAT=JSON ", "EXPLAIN ANALYZE ", "mysql_json", analyze_fmt="text_indent"
    ),
}


class ExplainUnsupported(Exception):
    """Raised when a dialect has no EXPLAIN this module can read, or cannot time a run."""


def syntax_for(dialect: str) -> ExplainSyntax:
    syn = _SYNTAX.get((dialect or "").lower())
    if syn is None:
        raise ExplainUnsupported(f"EXPLAIN is not supported for dialect {dialect!r}")
    return syn


def wrap_explain(sql: str, dialect: str, *, analyze: bool) -> str:
    """``sql`` prefixed with the dialect's EXPLAIN. Raises when the dialect cannot answer."""
    syn = syntax_for(dialect)
    if analyze:
        if syn.analyze_prefix is None:
            raise ExplainUnsupported(f"dialect {dialect!r} cannot EXPLAIN ANALYZE a statement")
        return syn.analyze_prefix + sql
    return syn.prefix + sql


# --------------------------------------------------------------------------- #
# Normalized plan tree
# --------------------------------------------------------------------------- #


@dataclass
class ExplainNode:
    """One operator of an engine plan, normalized across dialects."""

    op: str
    detail: dict[str, str] = field(default_factory=dict)
    rows: float | None = None
    cost: float | None = None
    actual_ms: float | None = None
    children: list["ExplainNode"] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "detail": self.detail,
            "rows": self.rows,
            "cost": self.cost,
            "actual_ms": self.actual_ms,
            "children": [c.to_dict() for c in self.children],
        }


def _num(raw: Any) -> float | None:
    """One numeric estimate from a plan, or None when the engine did not have one.

    Engines write these three different ways: as a number (Postgres, Trino), as a string
    (MySQL costs, DuckDB cardinalities), and as NaN when Trino's optimizer could not estimate
    a node. NaN is not JSON, so it must not reach the response — an unestimated node is
    reported as having no estimate, which is what it is.
    """
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if math.isfinite(raw) else None
    if isinstance(raw, str):
        try:
            value = float(raw)
        except ValueError:
            return None
        return value if math.isfinite(value) else None
    return None


def _flatten_detail(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items() if v not in (None, "", [], {})}
    if isinstance(raw, list):
        return {str(i): str(v) for i, v in enumerate(raw) if v not in (None, "")}
    if raw in (None, ""):
        return {}
    return {"detail": str(raw)}


def _from_postgres(node: dict) -> ExplainNode:
    op = str(node.get("Node Type", "?"))
    rel = node.get("Relation Name")
    if rel:
        op = f"{op} {rel}"
    detail = {
        k: str(v)
        for k, v in node.items()
        if k not in ("Plans", "Node Type", "Relation Name") and not isinstance(v, (dict, list))
    }
    return ExplainNode(
        op=op,
        detail=detail,
        rows=node.get("Plan Rows"),
        cost=node.get("Total Cost"),
        actual_ms=node.get("Actual Total Time"),
        children=[_from_postgres(c) for c in node.get("Plans", [])],
    )


def _named_estimates(node: dict) -> tuple[float | None, float | None]:
    """The row and cost estimates of a DuckDB or Trino operator.

    Each engine names them somewhere else, and only one of these keys exists on any given
    node: DuckDB's profile (EXPLAIN ANALYZE) counts the rows an operator actually produced in
    ``operator_cardinality``, its plan (plain EXPLAIN) states the optimizer's guess as the
    ``Estimated Cardinality`` string inside ``extra_info``, and Trino carries a per-node
    ``estimates`` list whose first entry is the chosen alternative.
    """
    extra = node.get("extra_info") if isinstance(node.get("extra_info"), dict) else {}
    estimates = node.get("estimates")
    chosen = (
        estimates[0]
        if isinstance(estimates, list) and estimates and isinstance(estimates[0], dict)
        else {}
    )
    rows = (
        _num(node.get("operator_cardinality"))
        if "operator_cardinality" in node
        else _num(node.get("cardinality"))
        if "cardinality" in node
        else _num(chosen.get("outputRowCount"))
        if chosen
        else _num(extra.get("Estimated Cardinality"))
    )
    cost = _num(node.get("cost")) if "cost" in node else _num(chosen.get("cpuCost"))
    return rows, cost


def _from_named(node: dict) -> ExplainNode:
    """DuckDB (plan and profile) and Trino JSON — an operator name plus nested children."""
    op = node.get("operator_name") or node.get("name") or node.get("operator_type") or "?"
    timing = node.get("operator_timing")
    # Trino splits an operator's description across both keys; DuckDB fills only extra_info.
    detail = {
        **_flatten_detail(node.get("descriptor")),
        **_flatten_detail(node.get("extra_info") or node.get("details")),
    }
    rows, cost = _named_estimates(node)
    return ExplainNode(
        op=str(op),
        detail=detail,
        rows=rows,
        cost=cost,
        actual_ms=(timing * 1000) if isinstance(timing, (int, float)) else None,
        children=[_from_named(c) for c in node.get("children", []) if isinstance(c, dict)],
    )


# MySQL wraps its scans in operation objects; the label reads better than the raw key.
_MYSQL_LABELS = {
    "query_block": "Query block",
    "ordering_operation": "Ordering",
    "grouping_operation": "Grouping",
    "duplicates_removal": "Duplicate removal",
    "materialized_from_subquery": "Materialized subquery",
    "optimized_away_subqueries": "Optimized-away subqueries",
    "union_result": "Union",
}
# Lists whose entries are operators in their own right, rather than plain values.
_MYSQL_CHILD_LISTS = ("nested_loop", "windows", "attached_subqueries", "query_specifications")


def _from_mysql(payload: dict) -> list[ExplainNode]:
    """MySQL EXPLAIN FORMAT=JSON — nested operations, each carrying its own cost_info.

    The row estimate lives on the leaf ``table`` objects (``rows_examined_per_scan``); the
    costs are strings, on the block as ``query_cost`` and on a table as ``prefix_cost``.
    """
    return [_mysql_node("query_block", payload["query_block"])]


def _mysql_node(key: str, raw: dict) -> ExplainNode:
    cost_info = raw.get("cost_info") if isinstance(raw.get("cost_info"), dict) else {}
    if key == "table":
        op = f"{raw.get('access_type', '?')} {raw.get('table_name', '?')}"
        cost = _num(cost_info.get("prefix_cost"))
    else:
        op = _MYSQL_LABELS.get(key, key.replace("_", " ").capitalize())
        cost = _num(cost_info.get("query_cost"))
    node = ExplainNode(
        op=op,
        detail={
            k: str(v)
            for k, v in raw.items()
            if k != "cost_info" and not isinstance(v, (dict, list))
        },
        rows=_num(raw.get("rows_examined_per_scan")),
        cost=cost,
    )
    for child_key, value in raw.items():
        if child_key == "cost_info":
            continue
        if isinstance(value, dict):
            node.children.append(_mysql_node(child_key, value))
        elif isinstance(value, list) and child_key in _MYSQL_CHILD_LISTS:
            for item in value:
                if not isinstance(item, dict):
                    continue
                # A nested_loop entry is a wrapper holding exactly one table.
                inner = item.get("table")
                node.children.append(
                    _mysql_node("table", inner)
                    if isinstance(inner, dict)
                    else _mysql_node(child_key, item)
                )
    return node


def _from_sqlite(rows: list[tuple]) -> list[ExplainNode]:
    """EXPLAIN QUERY PLAN returns (id, parent, notused, detail) — a tree by parent id."""
    by_id: dict[Any, ExplainNode] = {}
    roots: list[ExplainNode] = []
    for row in rows:
        node_id, parent_id, _unused, detail = row[0], row[1], row[2], row[3]
        node = ExplainNode(op=str(detail))
        by_id[node_id] = node
        parent = by_id.get(parent_id)
        if parent is None:
            roots.append(node)
        else:
            parent.children.append(node)
    return roots


def _from_text_indent(text: str) -> list[ExplainNode]:
    """Indentation-nested plan text (Trino EXPLAIN ANALYZE, MySQL EXPLAIN ANALYZE)."""
    roots: list[ExplainNode] = []
    stack: list[tuple[int, ExplainNode]] = []
    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip().lstrip("-> ").strip()
        node = ExplainNode(op=line)
        while stack and stack[-1][0] >= indent:
            stack.pop()
        if stack:
            stack[-1][1].children.append(node)
        else:
            roots.append(node)
        stack.append((indent, node))
    return roots


def parse_explain(rows: list[tuple], column_names: list[str], fmt: str) -> list[ExplainNode]:
    """The engine's EXPLAIN result as a normalized operator tree."""
    if fmt == "sqlite_qp":
        return _from_sqlite(rows)
    if not rows:
        return []
    # Every JSON/text format returns the plan in one cell; DuckDB puts it in the second column.
    cell = rows[0][-1] if len(column_names) > 1 else rows[0][0]
    if fmt == "text_indent":
        return _from_text_indent(str(cell))
    payload = json.loads(cell) if isinstance(cell, str) else cell
    if fmt == "postgres_json":
        plans = payload if isinstance(payload, list) else [payload]
        return [_from_postgres(p["Plan"]) for p in plans if isinstance(p, dict) and "Plan" in p]
    if fmt == "mysql_json":
        return _from_mysql(payload)
    # named_json: a profile wrapper carries no operator of its own, only its children.
    if isinstance(payload, list):
        return [_from_named(p) for p in payload if isinstance(p, dict)]
    # Trino returns its plan as fragments keyed by id, each one a root of its own.
    if payload and all(str(k).isdigit() for k in payload):
        return [_from_named(v) for v in payload.values() if isinstance(v, dict)]
    if not (payload.get("operator_name") or payload.get("name")):
        return [_from_named(c) for c in payload.get("children", []) if isinstance(c, dict)]
    return [_from_named(payload)]


# --------------------------------------------------------------------------- #
# The one artifact: engine plan spine + Provisa annotations
# --------------------------------------------------------------------------- #


def _esc(text: str) -> str:
    return text.replace('"', "'").replace("\n", " ")[:90]


def _node_label(node: ExplainNode) -> str:
    parts = [_esc(node.op)]
    if node.rows is not None:
        parts.append(f"{node.rows:g} rows")
    if node.actual_ms is not None:
        parts.append(f"{node.actual_ms:.1f}ms")
    elif node.cost is not None:
        parts.append(f"cost {node.cost:g}")
    return "\\n".join(parts)


def build_explain_mermaid(
    nodes: list[ExplainNode],
    *,
    route: str,
    route_reason: str | None,
    optimizations: tuple[str, ...] = (),
) -> str:
    """The engine plan as a top-down tree, with the Provisa rewrites that shaped it called out.

    The engine plan is the spine because it is the last word on what ran. The Provisa
    optimizations are what the spine cannot say — a table the user wrote that no scan node
    mentions was inlined, served from a cache, or dropped before the engine ever saw it.
    """
    lines = ["flowchart TD"]
    counter = [0]

    def emit(node: ExplainNode, parent: str | None) -> None:
        counter[0] += 1
        nid = f"p{counter[0]}"
        shape = (
            f'{nid}["{_node_label(node)}"]' if node.children else f'{nid}("{_node_label(node)}")'
        )
        lines.append(f"  {shape}")
        if parent is not None:
            lines.append(f"  {parent} --> {nid}")
        for child in node.children:
            emit(child, nid)

    reason = f"\\n{_esc(route_reason)}" if route_reason else ""
    lines.append(f'  route{{{{"{route.lower()} route{reason}"}}}}')
    for root in nodes:
        emit(root, "route")

    for i, label in enumerate(optimizations):
        kind, _, relation = label.partition(": ")
        text = f"{_esc(kind)}\\n{_esc(relation)}" if relation else _esc(kind)
        lines.append(f'  o{i}[/"{text}"/]')
        lines.append(f"  o{i} --> route")
    if optimizations:
        lines.append("  classDef provisaOpt fill:#1f3b2d,stroke:#4ade80,color:#e6ffe6")
        lines.append(f"  class {','.join(f'o{i}' for i in range(len(optimizations)))} provisaOpt")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# The analyze entry point
# --------------------------------------------------------------------------- #


async def analyze_sql(
    sql: str,
    role_id: str,
    state: Any,
    *,
    analyze: bool = False,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Govern ``sql`` through the ONE pipeline and return its plan, explained and annotated.

    ``analyze=False`` describes the statement without running it. ``analyze=True`` runs it — the
    dialect's EXPLAIN ANALYZE — and the same governance, tier ceilings and audit apply, because
    it is the same plan reaching the same terminal.
    """
    from provisa.compiler.sql_rewrite import split_sql_statements
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route
    from provisa.transpiler.router import Route

    statements = split_sql_statements(sql)
    if len(statements) != 1:
        raise ValueError("EXPLAIN describes one statement; send a single statement")

    plan = await _govern_and_route(statements[0], role_id, as_of=as_of, explain=analyze)
    is_engine = plan.route == Route.ENGINE
    dialect = state.federation_engine.dialect if is_engine else plan.dialect
    explained_sql = plan.physical_sql if is_engine else plan.sql

    result = await _execute_plan(plan, state)
    nodes = parse_explain(
        result.rows, list(result.column_names), syntax_for(dialect).format_for(analyze=analyze)
    )
    route_name = "ENGINE" if is_engine else "DIRECT"
    return {
        "route": route_name,
        "route_reason": plan.route_reason,
        "dialect": dialect,
        "analyzed": analyze,
        "sources": sorted(plan.sources),
        "optimizations": list(plan.optimizations),
        "sql": explained_sql,
        # REQ-1322: the metric expansion in semantic terms — None when no metric was referenced.
        # `sql` above is the physical lowering: it names source catalogs and cannot be resubmitted,
        # so this is the form the SQL editor detaches to.
        "semantic_sql": plan.semantic_sql,
        "plan": [n.to_dict() for n in nodes],
        "mermaid": build_explain_mermaid(
            nodes,
            route=route_name,
            route_reason=plan.route_reason,
            optimizations=plan.optimizations,
        ),
    }
