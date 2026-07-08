# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MV lineage — the DAG edges, derived from SQL (REQ-939), that drive event fan-out.

Every MV is SQL authored to reference its inputs, so SQLGlot extracts the dependency edges — no
hand-declared lineage that can drift. ``extract_inputs`` gives one MV's input tables; ``dependents``
inverts the graph to source_table → the MVs that listen for it (what the dispatcher fans an event
out to); ``find_cycle`` enforces the acyclic invariant at registration (an MV that transitively
depends on itself would fan out forever).
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp


def extract_inputs(sql: str, dialect: str = "postgres") -> set[str]:
    """The qualified input tables an MV's SQL reads — its lineage edges. Names are joined
    ``[catalog.]schema.table`` (whatever parts the SQL qualifies) to match the ``source_table`` an
    event carries. A CTE name defined in the same query is not an input (it is resolved locally)."""
    tree = sqlglot.parse_one(sql, read=dialect)
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    out: set[str] = set()
    for t in tree.find_all(exp.Table):
        parts = [
            p.name
            for p in (t.args.get("catalog"), t.args.get("db"), t.this)
            if p is not None and p.name
        ]
        name = ".".join(parts)
        if name and t.this.name not in ctes:
            out.add(name)
    return out


def dependents(mvs: dict[str, str], dialect: str = "postgres") -> dict[str, list[str]]:
    """Invert the lineage: ``source_table -> [mv nodes that depend on it]`` — the fan-out target set
    the dispatcher uses when an event on ``source_table`` arrives. ``mvs`` maps mv node name → its
    SQL. Dependents are returned in a stable (sorted) order."""
    rev: dict[str, set[str]] = {}
    for mv, sql in mvs.items():
        for inp in extract_inputs(sql, dialect):
            rev.setdefault(inp, set()).add(mv)
    return {src: sorted(deps) for src, deps in rev.items()}


def find_cycle(mvs: dict[str, str], dialect: str = "postgres") -> list[str] | None:
    """Return a cycle in the MV DAG (an MV transitively depending on itself) as an ordered node list,
    or None if acyclic. Enforced at registration — a cycle would make fan-out never terminate. Only
    edges between MV nodes count (an input that is a base source is a leaf)."""
    graph = {mv: extract_inputs(sql, dialect) & set(mvs) for mv, sql in mvs.items()}
    WHITE, GREY, BLACK = 0, 1, 2
    color = dict.fromkeys(graph, WHITE)

    def visit(node: str, stack: list[str]) -> list[str] | None:
        color[node] = GREY
        stack.append(node)
        for dep in sorted(graph.get(node, ())):
            if color[dep] == GREY:  # back-edge → cycle
                return stack[stack.index(dep) :] + [dep]
            if color[dep] == WHITE:
                found = visit(dep, stack)
                if found:
                    return found
        color[node] = BLACK
        stack.pop()
        return None

    for mv in sorted(graph):
        if color[mv] == WHITE:
            cycle = visit(mv, [])
            if cycle:
                return cycle
    return None
