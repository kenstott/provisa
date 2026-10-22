# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e7c3a-2f84-4d69-9a0e-c7d3f18b6e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which result columns of a Cypher read the graph explorer draws (REQ-392): the nodes,
relationships and paths the translation returns. A read with any of them is answered inline,
never delivered to a file; one with none is rows, answered as any other surface answers them.
Leaf module (no route handlers).
"""

# Requirements: REQ-392, REQ-768, REQ-769

from __future__ import annotations

from provisa.gql.translator_types import GraphVarKind

__all__ = ["graph_columns"]

#: The result columns the explorer draws: a node, a relationship, a path, or the pre-built
#: node/relationship JSON of a union subquery. A map or a scalar is a value in a row.
_DRAWN = {
    GraphVarKind.NODE: "node",
    GraphVarKind.EDGE: "edge",
    GraphVarKind.PATH: "path",
    GraphVarKind.PASSTHROUGH: "node",
}


def graph_columns(graph_vars: dict[str, GraphVarKind]) -> dict[str, str]:
    """Each result column the explorer draws, by name: ``node``, ``edge`` or ``path``, from the
    translation's own knowledge of what each returned variable is (``cypher_to_sql``'s
    graph_vars), never from the values. Empty for a read that returns rows only."""
    return {name: _DRAWN[kind] for name, kind in graph_vars.items() if kind in _DRAWN}
