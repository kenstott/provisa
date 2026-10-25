# Copyright (c) 2026 Kenneth Stott
# Canary: e93b0c47-2a18-4d65-8f7e-61c5d3a9b802
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's lineage and its nearest physical inputs (REQ-1640, REQ-2126).

The product's lineage is its member columns, everything they derive from and what derives from
them directly, a checker table's columns left out: a checker's rows are scan outcomes about a
member, not part of its data flow. Its inputs are found on that graph, after the caller's read
rights have cut it (REQ-1161): upstream of each member, through plain views and a view's internal
steps, to the first physical dataset, a registered source table or a materialized view; a member
read straight from a source has that source as its input. A member is never an input of its own
product.
"""

# Requirements: REQ-1640, REQ-2126, REQ-2157, REQ-2152

from __future__ import annotations

import copy

from provisa.lineage.graph import LineageGraph, Node
from provisa.lineage.merge import ancestor_closure

NATIVE = "native"


def product_graph(
    graph: LineageGraph, member_columns: set[str], *, checker_relations: set[str]
) -> LineageGraph:
    """The product's lineage over ``graph``: ``member_columns`` (node ids), their ancestry and one
    hop downstream. A member column nothing mentions joins as an isolated source node; every
    member column is an output. ``graph`` is not changed."""
    graph = copy.copy(graph)
    graph.nodes = dict(graph.nodes)
    for nid in member_columns - set(graph.nodes):
        relation, _, column = nid.rpartition(".")
        graph.nodes[nid] = Node(id=nid, column=column, relation=relation, kind="source")
    out = ancestor_closure(graph, member_columns)
    for e in graph.edges:
        if e.source not in member_columns:
            continue
        child = graph.nodes[e.target]
        if child.relation in checker_relations:
            continue
        out.nodes.setdefault(e.target, child)
        out.add_edge(e)
    out.outputs = sorted(member_columns)
    return out


def nearest_inputs(graph: LineageGraph, members: set[str], *, base: set[str]) -> list[str]:
    """The relations of the product's nearest physical inputs over ``graph``: ``members`` are its
    member relations, ``base`` the registered source tables. A native relation stands for the
    source a base member is read from."""
    up: dict[str, list[str]] = {}
    for e in graph.edges:
        up.setdefault(e.target, []).append(e.source)
    found: set[str] = set()
    for nid, node in graph.nodes.items():
        if node.relation not in members:
            continue
        if node.relation in base:
            found.update(
                graph.nodes[p].relation or ""
                for p in up.get(nid, [])
                if graph.nodes[p].kind == NATIVE
            )
            continue
        seen = {nid}
        stack = [nid]
        while stack:
            for p in up.get(stack.pop(), []):
                if p in seen:
                    continue
                seen.add(p)
                parent = graph.nodes[p]
                if parent.relation in members:
                    continue
                if parent.relation in base or parent.materialized or parent.kind == NATIVE:
                    found.add(parent.relation or "")
                else:
                    stack.append(p)
    found.discard("")
    return sorted(found)


#: REQ-2157: a product's data-quality lists, in the order the panel shows them.
DQ_LISTS = ("expectations", "promises", "related", "unattached")


def product_dq(
    product_id: str,
    checkers: list[dict],
    *,
    members: set[int],
    inputs: set[int],
    served: dict[int, set[str]],
) -> dict[str, list[dict]]:
    """A product's data quality (REQ-2157), each checker table in one list.

    ``checkers``: every checker table -- its id, names, description, owning ``product_id``, the
    ``checked_table_id`` its contract scans and that table's names (``checked_table``), the
    ``rule_columns`` its checks name and its latest run. ``members`` and ``inputs`` are the product's member and nearest input table ids, as its
    reader's lineage gives them; ``served`` what that reader is served (table id → physical
    columns). A promise is the product's check on a member, an expectation its check on an
    input; another owner's (or no owner's) check on either is related; the product's check on
    neither is unattached, a curation gap for its owner. A check the reader cannot read -- its
    checker table, or the checked columns its rules name (one column of the checked table when
    they name none) -- is in no list (REQ-2152)."""
    out: dict[str, list[dict]] = {name: [] for name in DQ_LISTS}
    for c in sorted(checkers, key=lambda c: (c["table_name"], c["id"])):
        if not _reads_check(c, served):
            continue
        checked = c["checked_table_id"]
        owned = c["product_id"] == product_id
        if owned and checked in members:
            kind = "promises"
        elif owned and checked in inputs:
            kind = "expectations"
        elif owned:
            kind = "unattached"
        elif checked in members or checked in inputs:
            kind = "related"
        else:
            continue
        out[kind].append(
            {
                "checker_table_id": c["id"],
                "domain_id": c["domain_id"],
                "table_name": c["table_name"],
                "alias": c["alias"],
                "description": c["description"],
                "owner_product_id": c["product_id"],
                "checked_table_id": checked,
                "checked_table": c["checked_table"],
                "latest_outcome": c["latest_outcome"],
                "latest_run_at": c["latest_run_at"],
            }
        )
    return out


def _reads_check(checker: dict, served: dict[int, set[str]]) -> bool:
    if not served.get(checker["id"]):
        return False
    readable = served.get(checker["checked_table_id"], set())
    named = {c for c in checker["rule_columns"] if c}
    return named <= readable if named else bool(readable)
