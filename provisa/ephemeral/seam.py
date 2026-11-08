# Copyright (c) 2026 Kenneth Stott
# Canary: ca64f254-341c-46a9-8f4f-0e10cc66e052
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a Cypher surface meets the ephemeral layer (REQ-2176): the one API a transport calls.

- :func:`write` before the governed write path: an ephemeral write, a CREATE split across
  both paths, a label SET or REMOVE, a property SET or REMOVE of the session's objects, or an
  index statement, is done here and its answer returned; one followed by reads is done, then
  the rest read over the session (:mod:`.then`), its rows the answer. Anything else (the
  governed write, a read) is None, for the surface's own path.
- :func:`label_map` before compiling a read, and :func:`expand` after: the overlay of the
  session's labels and types. A session holding nothing gives back the very label map and SQL
  it was given, so a statement with no ephemeral involvement compiles as it does without it.
"""

# Requirements: REQ-2176

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import read, store, then
from provisa.ephemeral.delete import Deleted, delete, delete_bound
from provisa.ephemeral.indexes import Indexed, index_statement
from provisa.ephemeral.labels import Labelled, write_labels, write_labels_bound
from provisa.ephemeral.merge import _governed as merge_governed
from provisa.ephemeral.merge import merge, merge_bound
from provisa.ephemeral.props import Propertied, write_props
from provisa.ephemeral.then import Answered, counters
from provisa.ephemeral.write import Created, Read, Write, create, create_bound
from provisa.pgwire import temp_exec

__all__ = ["Answered", "answered", "counters", "expand", "label_map", "rows_of", "write"]


def label_map(model: Any, session: TempSession | None) -> Any:
    """The label map a read compiles over: ``model`` itself outside a session; in a session
    holding no ephemeral object, ``model`` with the session's (empty) relationships to walk
    (REQ-348), which no other read touches; else the session's overlay of it."""
    if session is None:
        return model
    if not read.involved(session):
        return read.walkable(model)
    return read.overlay(model, session)


def expand(
    sql: exp.Expression, compiled_over: Any, model: Any, session: TempSession | None
) -> exp.Expression:
    """``sql``, compiled over ``compiled_over``, with the session's relations in place; ``sql``
    itself when it was compiled over ``model``."""
    if session is None or compiled_over is model:
        return sql
    return read.expand(sql, compiled_over, model, session)


async def write(
    statement: str,
    params: dict[str, Any],
    model: Any,
    session: TempSession | None,
    state: Any,
    run_read: Read,
    run_write: Write,
) -> Created | Deleted | Labelled | Propertied | Indexed | Answered | None:
    """Do ``statement`` when the governing rule does not make it wholly the governed write (its
    governed parts through ``run_write``, the surface's own governed write); else None. A write
    followed by reads answers with the rest's rows (an Answered). A write that would take the
    session past its bounds (REQ-1926) is refused by name."""
    try:
        return await _write(statement, params, model, session, state, run_read, run_write)
    except temp_exec.TempTableLimit as exc:
        raise CypherParseError(str(exc)) from exc


async def _write(
    statement: str,
    params: dict[str, Any],
    model: Any,
    session: TempSession | None,
    state: Any,
    run_read: Read,
    run_write: Write,
) -> Created | Deleted | Labelled | Propertied | Indexed | Answered | None:
    if session is None:
        return None
    indexed = await index_statement(statement, model, session, state)
    if indexed is not None:
        return indexed

    async def head(split: then.Split) -> Any:
        keep = split.scope
        if split.kind == "create":
            return await create_bound(
                split.head, params, model, session, state, run_read, run_write, keep
            )
        if split.kind == "merge":
            # Whether a MERGE is the governed path's is the whole statement's to say (all its
            # MERGE clauses), never its first clause's alone.
            if merge_governed(statement, model, store.graph(session)):
                return None
            return await merge_bound(
                split.head, params, model, session, state, run_read, run_write, keep
            )
        if split.kind == "delete":
            return await delete_bound(
                split.head, params, model, session, state, run_read, run_write, keep, statement
            )
        if split.kind == "labels":
            return await write_labels_bound(
                split.head, params, model, session, state, run_read, keep
            )
        return await write_props(split.head, params, model, session, state, run_read, keep)

    async def again(statement: str, values: dict[str, Any]) -> Any:
        return await write(statement, values, model, session, state, run_read, run_write)

    answered = await then.write_then_read(
        statement, params, session, head, again, run_read, run_write
    )
    if answered is not None:
        return answered
    made = await create(statement, params, model, session, state, run_read, run_write)
    if made is None:
        made = await merge(statement, params, model, session, state, run_read, run_write)
    if made is None:
        made = await delete(statement, params, model, session, state, run_read, run_write)
    if made is not None:
        return made
    labelled = await write_labels(statement, params, model, session, state, run_read)
    if labelled is not None:
        return labelled
    done = await write_props(statement, params, model, session, state, run_read)
    return None if done is None else done[0]


def rows_of(made: Any, session: TempSession | None = None) -> tuple[list[str], list[list[Any]]]:
    """A write's answer as columns and rows of values: a write followed by reads answers with
    the rest's (its session nodes as :func:`answered` gives them); any other with none."""
    if isinstance(made, Answered):
        rows = answered(made.rows, session) if session is not None else made.rows
        return list(made.columns), [[row.get(c) for c in made.columns] for row in rows]
    return [], []


def answered(rows: list[dict[str, Any]], session: TempSession) -> list[dict[str, Any]]:
    """``rows`` (an answer's, serialized) with each of the session's own nodes as the session
    holds it: its id within the session, whatever label read it; every label it carries
    (``labels``); and only the properties written to it -- an ephemeral node's built-in ``_id`` is
    that id, read by the session's own writes, never one of its properties. A model node is as
    the model reads it."""
    graph = store.graph(session)
    if not graph.node_keys:
        return rows
    return [{k: _answered(v, graph) for k, v in row.items()} for row in rows]


def _session_node_id(node_id: Any, graph: store.Graph) -> str | None:
    """The ephemeral node a node's answered id is (``e:1``, or ``Label|e:1`` read by a label or
    none), or None for any other node."""
    if not isinstance(node_id, str):
        return None
    if node_id in graph.node_keys:
        return node_id
    after = node_id.split("|", 1)[1] if "|" in node_id else None
    return after if after in graph.node_keys else None


def _answered(value: Any, graph: store.Graph) -> Any:
    from provisa.gql.select_builder import NODE_KEY_PROPERTY

    if isinstance(value, list):
        return [_answered(v, graph) for v in value]
    if not isinstance(value, dict):
        return value
    if "label" in value and "properties" in value and "startNode" not in value:
        node_id = _session_node_id(value.get("id"), graph)
        if node_id is None:
            return value
        props = {k: v for k, v in (value["properties"] or {}).items() if k != NODE_KEY_PROPERTY}
        labels = [label for label in graph.node_keys[node_id] if label]
        return {**value, "id": node_id, "labels": labels, "properties": props}
    out = {k: _answered(v, graph) if isinstance(v, (dict, list)) else v for k, v in value.items()}
    if "startNode" in value:
        # A relationship's ends by the ids its end nodes are answered with.
        for end in ("start", "end"):
            held = _session_node_id(out.get(end), graph)
            if held is not None:
                out[end] = held
    return out
