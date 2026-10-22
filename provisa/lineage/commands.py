# Copyright (c) 2026 Kenneth Stott
# Canary: 220d11da-a52b-4087-9848-e15288876ce7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement command's write in the lineage graph (REQ-2001).

A statement command that writes a table derives that table's rows from the tables it reads. Its
edges -- each written column from the columns it is computed from -- enter the federation graph
beside the views', from the statement as registered (its contract's ``write_select``), never from
its runs. Each edge carries a ``command_write`` operation naming the command, so the written table
reads as derived by it, and impact analysis names the command a column's rename would break.
"""

# Requirements: REQ-2001

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlglot.errors import SqlglotError

from provisa.lineage.graph import (
    LineageGraph,
    TransformOp,
    build_column_graph,
    qualify_outputs,
    requalify_relations,
)

if TYPE_CHECKING:
    from provisa.lineage.merge import MergedGraph

#: The operation an edge of a command's write carries; its name is the command's.
COMMAND_WRITE = "command_write"


def command_write_graphs(commands: dict[str, dict]) -> list[tuple[str, LineageGraph]]:
    """(command name, its write graph) for each statement command in ``commands`` (the served
    commands, a command registered under more than one name counted once) whose contract resolves
    and writes."""
    graphs: list[tuple[str, LineageGraph]] = []
    seen: set[str] = set()
    for fn in commands.values():
        if fn.get("impl_kind") != "statement" or fn["name"] in seen:
            continue
        seen.add(fn["name"])
        contract = fn["contract"]
        if contract["broken"] is not None or contract["write_select"] is None:
            continue
        try:
            graph = build_column_graph(contract["write_select"])
        except SqlglotError:
            continue
        bare = {relation.split(".")[-1]: relation for relation in contract["reads"]}
        bare[contract["writes"].split(".")[-1]] = contract["writes"]
        requalify_relations(graph, bare)
        qualify_outputs(graph, contract["writes"])
        op = TransformOp(name=fn["name"], kind=COMMAND_WRITE)
        for edge in graph.edges:
            edge.ops = (*edge.ops, op)
        graphs.append((fn["name"], graph))
    return graphs


def with_command_writes(merged: "MergedGraph", commands: dict[str, dict]) -> "MergedGraph":
    """The federation graph ``merged`` with each statement command's write stitched in."""
    from provisa.lineage.merge import merge_graphs

    graphs = [graph for _, graph in command_write_graphs(commands)]
    return merge_graphs([merged.graph, *graphs]) if graphs else merged
