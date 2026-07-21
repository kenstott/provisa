# Copyright (c) 2026 Kenneth Stott
# Canary: 9d8ba2c2-8cfd-4005-9fa0-517c4a79f38c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-role query-complexity limits (REQ-1174) — Hasura api_limits parity.

Rate limiting caps request VOLUME; these cap a SINGLE query's cost, which volume limits cannot — one
deeply-nested or huge query is far more damaging than many small ones. Enforced at the GraphQL→IR
compile boundary (:func:`provisa.compiler.stage1.compile_graphql`), where the parsed document is in
hand:

- ``max_query_depth`` — the deepest selection nesting. Depth is an AST property (the normalized IR
  flattens nesting into joins, so it is NOT recoverable there); measured on the document, resolving
  named fragments (a classic depth-attack vector) with a cycle guard.
- ``max_query_nodes`` — the total number of selected fields (the query's breadth × depth). A proxy
  for node/join count that is cheap to compute pre-compile and blocks the query before any SQL runs.

``max_query_time_ms`` is a wall-clock cap applied at execution, not here (see the request-timeout
resolution in the data endpoint). ``None`` on any dimension = unlimited.
"""

from __future__ import annotations

from typing import Any

from graphql import (
    DocumentNode,
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    InlineFragmentNode,
    OperationDefinitionNode,
    SelectionSetNode,
)


class QueryLimitError(Exception):
    """A query exceeded a role's complexity limit (REQ-1174). Carries the offending dimension so the
    endpoint can return a precise 4xx (a client error — the query is too expensive, not a server fault)."""

    def __init__(self, dimension: str, limit: int, actual: int) -> None:
        self.dimension = dimension
        self.limit = limit
        self.actual = actual
        super().__init__(
            f"query {dimension} {actual} exceeds the role limit of {limit}"
        )


def measure_query(document: DocumentNode) -> tuple[int, int]:
    """Return ``(max_depth, node_count)`` for ``document``.

    ``max_depth`` is the deepest field-selection nesting (top-level fields are depth 1); ``node_count``
    is the total number of selected fields. ``__typename`` is ignored (it is free/meta). Named
    fragments are resolved at their spread site (so fragment nesting counts toward depth), with a
    cycle guard against a self-referential fragment. Inline fragments do not add a depth level."""
    fragments = {
        d.name.value: d for d in document.definitions if isinstance(d, FragmentDefinitionNode)
    }
    max_depth = 0
    node_count = 0
    active_spreads: set[str] = set()

    def walk(selection_set: SelectionSetNode | None, depth: int) -> None:
        nonlocal max_depth, node_count
        if selection_set is None:
            return
        for sel in selection_set.selections:  # type: ignore[attr-defined]
            if isinstance(sel, FieldNode):
                if sel.name.value == "__typename":
                    continue
                node_count += 1
                d = depth + 1
                if d > max_depth:
                    max_depth = d
                walk(sel.selection_set, d)
            elif isinstance(sel, InlineFragmentNode):
                walk(sel.selection_set, depth)  # inline fragment shares its parent's depth
            elif isinstance(sel, FragmentSpreadNode):
                frag = fragments.get(sel.name.value)
                if frag is not None and sel.name.value not in active_spreads:
                    active_spreads.add(sel.name.value)
                    walk(frag.selection_set, depth)
                    active_spreads.discard(sel.name.value)

    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode):
            walk(defn.selection_set, 0)
    return max_depth, node_count


def enforce_limits(
    document: DocumentNode, *, max_depth: int | None = None, max_nodes: int | None = None
) -> None:
    """Raise :class:`QueryLimitError` if ``document`` exceeds a limit. ``None`` limits are skipped, and
    when BOTH are None the document is not even walked (zero overhead for roles without limits)."""
    if max_depth is None and max_nodes is None:
        return
    depth, nodes = measure_query(document)
    if max_depth is not None and depth > max_depth:
        raise QueryLimitError("depth", max_depth, depth)
    if max_nodes is not None and nodes > max_nodes:
        raise QueryLimitError("nodes", max_nodes, nodes)


def role_query_limits(role: Any) -> tuple[int | None, int | None, int | None]:
    """Extract ``(max_query_depth, max_query_nodes, max_query_time_ms)`` from a role's rate_limit,
    tolerating both the dict shape (config/DB load) and the :class:`RoleRateLimit` model. Missing /
    absent → all None (unlimited)."""
    rl: object | None
    if isinstance(role, dict):
        rl = role.get("rate_limit")
    else:
        rl = getattr(role, "rate_limit", None)
    if rl is None:
        return (None, None, None)
    getter = rl.get if isinstance(rl, dict) else lambda k: getattr(rl, k, None)  # type: ignore[attr-defined]
    return (getter("max_query_depth"), getter("max_query_nodes"), getter("max_query_time_ms"))
