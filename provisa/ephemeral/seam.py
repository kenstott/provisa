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
from provisa.ephemeral import read, store, then
from provisa.ephemeral.delete import Deleted, delete
from provisa.ephemeral.indexes import Indexed, index_statement
from provisa.ephemeral.labels import Labelled, write_labels, write_labels_bound
from provisa.ephemeral.merge import _governed as merge_governed
from provisa.ephemeral.merge import merge, merge_bound
from provisa.ephemeral.props import Propertied, write_props
from provisa.ephemeral.then import Answered, counters
from provisa.ephemeral.write import Created, Read, Write, create, create_bound

__all__ = ["Answered", "counters", "expand", "label_map", "rows_of", "write"]


def label_map(model: Any, session: TempSession | None) -> Any:
    """The label map a read compiles over: ``model`` itself unless the session holds
    ephemeral objects."""
    if session is None or not read.involved(session):
        return model
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
    followed by reads answers with the rest's rows (an Answered)."""
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


def rows_of(made: Any) -> tuple[list[str], list[list[Any]]]:
    """A write's answer as columns and rows of values: a write followed by reads answers with
    the rest's; any other with none."""
    if isinstance(made, Answered):
        return list(made.columns), [[row.get(c) for c in made.columns] for row in made.rows]
    return [], []
