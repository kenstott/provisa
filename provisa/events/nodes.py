# Copyright (c) 2026 Kenneth Stott
# Canary: 326950f0-52b3-4fa2-953c-1299a5c039f9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The event graph's node names, and its edges resolved from the model (REQ-939).

A node is either a SOURCE TABLE — named by its registered identity, the same key its replica has
(``source_id``, ``schema``, ``table``), so two sources that both hold ``public.orders`` are two
nodes — or a MATERIALIZED VIEW, named by its target table. Every place that keys a node (the
processor, the queue's ``source_table``, the freshness state, the poll job, the land and refresh
paths) takes its name from here.

A view's edges are its inputs, resolved against the model by ``provisa.mv.view_inputs`` — never
by matching the spelling its SQL uses to a node's name. A view input that is another view is that
view's node; an input that is a view held only as SQL (not materialized) contributes the inputs it
reads. A reference that does not resolve here is a view that should have been refused when it was
declared: it raises, naming the view and the reference.

A node name carries no region: a region is a property of where a node runs, not of what it is.
"""

# Requirements: REQ-939, REQ-1912, REQ-1674

from __future__ import annotations

from typing import Any


def source_node(source_id: str, schema_name: str, table_name: str) -> str:
    """The node of the registered source table ``(source_id, schema_name, table_name)``."""
    return f"{source_id}/{schema_name}.{table_name}"


def view_node(mv: Any) -> str:
    """The node of the materialized view ``mv``: its target table."""
    return f"{mv.target_schema}.{mv.target_table}"


def lineage_graph(mvs: list[Any], state: Any) -> dict[str, set[str]]:
    """``{view node: the nodes it reads}`` for every view in ``mvs``, resolved against the model.

    A join-pattern view has no SQL; its inputs are the tables it joins, resolved the same way."""
    from provisa.mv.view_inputs import ModelIndex, resolve_view_inputs

    index = ModelIndex(state)
    return {
        view_node(mv): {
            node
            for resolved in resolve_view_inputs(mv, state, index)
            for node in _nodes_of(resolved, mv.id, index, state, frozenset({mv.id}))
        }
        for mv in mvs
    }


def expected_event_nodes(mv: Any, state: Any, graph: dict[str, set[str]]) -> list[str]:
    """The nodes a periodic view's freshness contract checks (REQ-961): the inputs it declares in
    ``expected_events``, resolved against the model like its SQL's, else every input it reads."""
    declared = getattr(mv, "expected_events", None)
    if declared is None:
        return sorted(graph[view_node(mv)])
    from provisa.mv.view_inputs import ModelIndex

    index = ModelIndex(state)
    return sorted(
        {
            node
            for name in declared
            for node in _nodes_of(
                index.resolve(mv.id, tuple(name.split("."))), mv.id, index, state, frozenset()
            )
        }
    )


def _nodes_of(
    resolved: Any, view_id: str, index: Any, state: Any, seen: frozenset[str]
) -> set[str]:
    """The nodes one resolved input stands for (see the module docstring)."""
    from provisa.core.models import DERIVED_SOURCE_ID
    from provisa.mv.view_inputs import resolve_view_inputs

    registry = state.mv_registry
    if resolved.view is not None:
        target = registry.get(resolved.view)
        if target is None:
            raise RuntimeError(
                f"materialized view {view_id!r} reads view {resolved.view!r}, "
                "which is not in the registry"
            )
        return {view_node(target)}
    row = index.row(resolved.table_id)
    if row["source_id"] != DERIVED_SOURCE_ID:
        return {source_node(row["source_id"], row["schema_name"], row["table_name"])}
    # A view registered as a table: its materialized form is a node; a view held only as SQL is
    # read through, so its own inputs are what the reading view reads.
    materialized = registry.get(f"view-{row['table_name']}")
    if materialized is not None:
        return {view_node(materialized)}
    name = f"view-{row['table_name']}"
    if name in seen:
        raise ValueError(f"view {row['table_name']!r} reads itself through other views")
    inline = _Inline(name, row["view_sql"])
    return {
        node
        for inner in resolve_view_inputs(inline, state, index)
        for node in _nodes_of(inner, name, index, state, seen | {name})
    }


class _Inline:
    """A view held only as SQL, read through by the views that name it."""

    def __init__(self, view_id: str, sql: str) -> None:
        self.id = view_id
        self.sql = sql
        self.source_tables: list[str] = []
