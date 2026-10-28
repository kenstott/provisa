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
  both paths, or a label SET or REMOVE, is done here and its answer returned; anything else (the governed write, a read)
  is None, for the surface's own path.
- :func:`label_map` before compiling a read, and :func:`expand` after: the overlay of the
  session's labels and types. A session holding nothing gives back the very label map and SQL
  it was given, so a statement with no ephemeral involvement compiles as it does without it.
"""

# Requirements: REQ-2176

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.compiler.temp_tables import TempSession
from provisa.ephemeral import read
from provisa.ephemeral.labels import Labelled, write_labels
from provisa.ephemeral.merge import merge
from provisa.ephemeral.write import Created, Read, Write, create


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
) -> Created | Labelled | None:
    """Do ``statement`` when the governing rule does not make it wholly the governed write (its
    governed parts through ``run_write``, the surface's own governed write); else None."""
    if session is None:
        return None
    made = await create(statement, params, model, session, state, run_read, run_write)
    if made is None:
        made = await merge(statement, params, model, session, state, run_read, run_write)
    if made is not None:
        return made
    return await write_labels(statement, params, model, session, state, run_read)
