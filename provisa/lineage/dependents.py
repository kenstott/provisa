# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b1d90-6c47-4a25-9d13-84e0b7f5a621
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What breaks if a column is renamed or removed (REQ-1484).

Two reference styles exist in the registry and they answer different questions:

* **By exposed name** — the SQL/GraphQL surface only ever shows ``table_columns.alias`` falling back
  to ``apply_sql_name(column_name)``. Views, metric expressions, RLS predicates and DQ contracts are
  authored against that name, so they break on a RENAME as well as a removal.
* **By physical name** — relationships, glossary refs and tag assignments store ``column_name``, the
  identity that survives the table upsert's wholesale column replace. They break only on a REMOVAL.

This module is the pure part: locating a column's downstream artifacts inside an already-built
federation lineage graph. The registry scans that cover artifacts outside the graph live in
``provisa.api.admin.column_dependents``.
"""

from __future__ import annotations

from dataclasses import dataclass

from provisa.lineage.graph import LineageGraph
from provisa.lineage.merge import slice_graph


@dataclass(frozen=True)
class Dependent:  # REQ-1484
    """One artifact that references a column."""

    kind: str  # "view" | "metric" | "rls" | "dq" | "relationship" | "glossary" | "tag" | ...
    name: str  # the artifact's addressable name
    detail: str  # how it references the column
    breaks_on: str  # "rename" (exposed-name ref) | "remove" (physical ref)


def relation_candidates(domain_sql_name: str, table_sql_name: str) -> set[str]:
    """The relation names a view's SQL may use for this table.

    A federation graph node's relation is whatever the authoring SQL wrote. A reference to a
    registered VIEW is requalified to ``<domain>.<table>`` during the merge; a reference to a base
    table keeps whatever qualification the author typed, which may be bare."""
    return {table_sql_name, f"{domain_sql_name}.{table_sql_name}"}


def _matches(relation: str | None, candidates: set[str]) -> bool:
    if relation is None:
        return False
    return relation in candidates or relation.split(".")[-1] in {c.split(".")[-1] for c in candidates}


def graph_dependents(
    graph: LineageGraph, candidates: set[str], column: str
) -> list[Dependent]:
    """Views and MVs downstream of ``<candidates>.<column>`` in the federation graph.

    ``column`` is the EXPOSED name (alias, else the snake_case default) — that is the only name a
    view's SQL can have been written against. Each distinct downstream relation is reported once."""
    origins = [
        n.id for n in graph.nodes.values() if n.column == column and _matches(n.relation, candidates)
    ]
    if not origins:
        return []
    reached: dict[str, bool] = {}
    for origin in origins:
        scoped = slice_graph(graph, origin, direction="downstream")
        for node in scoped.nodes.values():
            if node.relation is None or _matches(node.relation, candidates):
                continue
            # A relation reached downstream is materialized or not; the flag distinguishes an MV
            # (whose stored rows go stale) from a view (which simply fails to compile).
            reached[node.relation] = reached.get(node.relation, False) or node.materialized
    return [
        Dependent(
            kind="mv" if materialized else "view",
            name=relation,
            detail=f"selects {column}",
            breaks_on="rename",
        )
        for relation, materialized in sorted(reached.items())
    ]
