# Copyright (c) 2026 Kenneth Stott
# Canary: 0a93b0f9-5c8d-403c-9644-b42a50723875
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a path over the session's graph starts (REQ-2176, "SESSION GRAPHS ARE INDEXED AND
COUNTED"): from the end fewer nodes hold.

The translator reads a path from its first node. Where the overlaid label map carries the
session's counts (Selectivity), a statement's first MATCH whose path runs between two session
labels is read from its last node when that end is the more selective -- the path written
backward, each relationship's direction turned, so every variable binds as it did; an end's
estimate is its label's count of nodes. Anything else is read as written: a map without counts (no session), an end a model label
or no label names, an OPTIONAL MATCH, a named path, a variable-length hop, or a step of several
patterns.
"""

# Requirements: REQ-2176

from __future__ import annotations

import dataclasses

from provisa.gql.ir import CypherAST, MatchStep, NodePattern, PathPattern
from provisa.gql.label_map import CypherLabelMap, Selectivity

_TURNED = {"right": "left", "left": "right", "none": "none"}


def _label(node: NodePattern, sel: Selectivity, lm: CypherLabelMap) -> str | None:
    if len(node.labels) != 1:
        return None
    label = lm.canonical_label(node.labels[0])
    return label if label in sel.node_counts else None


def _turned(path: PathPattern) -> PathPattern:
    rels = [dataclasses.replace(r, direction=_TURNED[r.direction]) for r in reversed(path.rels)]
    return dataclasses.replace(path, nodes=list(reversed(path.nodes)), rels=rels)


def reordered(ast: CypherAST, lm: CypherLabelMap) -> CypherAST:
    """``ast`` with its first MATCH's path read from its more selective end; ``ast`` itself when
    it is read as written."""
    sel = lm.selectivity
    if sel is None or not ast.pipeline or not isinstance(ast.pipeline[0], MatchStep):
        return ast
    step = ast.pipeline[0]
    if len(step.matches) != 1:
        return ast
    clause = step.matches[0]
    path = clause.pattern
    if clause.optional or clause.variable is not None or not isinstance(path, PathPattern):
        return ast
    if not path.rels or any(
        r.variable_length or r.path is not None or len(r.types) != 1 for r in path.rels
    ):
        return ast
    first, last = _label(path.nodes[0], sel, lm), _label(path.nodes[-1], sel, lm)
    if first is None or last is None:
        return ast
    # The rows a read starts with: the end's nodes, each then reaching its relationships by the
    # index on their ends. (A relationship's count is the same from either of its ends.)
    if sel.node_counts[last] >= sel.node_counts[first]:
        return ast
    turned = dataclasses.replace(clause, pattern=_turned(path))
    pipeline = [dataclasses.replace(step, matches=[turned]), *ast.pipeline[1:]]
    return dataclasses.replace(ast, pipeline=pipeline)
