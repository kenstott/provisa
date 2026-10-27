# Copyright (c) 2026 Kenneth Stott
# Canary: 0cbb298a-86c4-4df5-bf0a-343ee8105e7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SET n:L and REMOVE n:L (REQ-2176): a label write is a node_labels row, on an ephemeral node or
a model node alike; a node never moves table, and the model never changes.

``[MATCH ... | UNWIND ... | WITH ...] SET a:L[:M][, b:N ...]`` (or REMOVE) reads the nodes it
names through the surface's own governed read, as the session's role, then adds (or deletes) one
node_labels row per node and label. A label already carried is not added again. A model node's
own label is its table, which no label write removes.
"""

# Requirements: REQ-2176

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import pattern, store
from provisa.ephemeral.classify import resolve
from provisa.ephemeral.write import Read, _identity

_ITEM = re.compile(rf"^({pattern._IDENT})((?:\s*:\s*{pattern._IDENT})+)$")


@dataclass(frozen=True)
class LabelClause:
    """A statement's leading clauses, its verb (SET or REMOVE), and each (variable, labels)."""

    prefix: str
    verb: str
    items: tuple[tuple[str, tuple[str, ...]], ...]


def label_clause(statement: str) -> LabelClause | None:
    """The statement's label SET or REMOVE, or None when it is anything else (a property SET, a
    read, a CREATE): only a clause of label items alone, ending the statement, is one."""
    scanned = pattern._scan(statement)
    plain = "".join(ch if depth == 0 else " " for depth, ch in scanned)
    found = list(re.finditer(r"\b(SET|REMOVE)\b", plain, re.IGNORECASE))
    if len(found) != 1:
        return None
    at = found[0]
    if re.search(
        r"\b(RETURN|WITH|DELETE|MERGE|MATCH|UNWIND|CREATE)\b", plain[at.end() :], re.IGNORECASE
    ):
        return None
    items: list[tuple[str, tuple[str, ...]]] = []
    for part in pattern.split_top(statement[at.end() :]):
        m = _ITEM.match(part)
        if m is None:
            return None
        names = tuple(pattern._unquote(n.strip()) for n in m.group(2).split(":") if n.strip())
        items.append((pattern._unquote(m.group(1)), names))
    return LabelClause(statement[: at.start()].strip(), at.group(1).upper(), tuple(items))


@dataclass(frozen=True)
class Labelled:
    """What a label SET or REMOVE did, for its answer's counters."""

    added: int
    removed: int


async def write_labels(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
) -> Labelled | None:
    """Run ``statement`` when it is a label SET or REMOVE; None when it is anything else."""
    clause = label_clause(statement)
    if clause is None:
        return None
    graph = store.graph(session)
    variables = sorted({var for var, _ in clause.items})
    answer = await read(
        f"{clause.prefix} RETURN {', '.join(f'{v} AS _v_{v}' for v in variables)}".strip(), params
    )
    held = await store.held_labels(state, session)
    keys = {var: resolve(list(names), label_map).keys for var, names in clause.items}
    given: list[tuple[str, str, str | None]] = []  # (node id, label, model type name)
    for row in answer:
        for var in variables:
            node_id, read_by = _identity(row[f"_v_{var}"], label_map, graph)
            model = None if node_id in graph.node_keys else read_by[0]
            given.extend((node_id, key, model) for key in keys[var])
    if clause.verb == "REMOVE":
        own = sorted({key for _, key, model in given if key == model})
        if own:
            raise CypherParseError(
                f"{', '.join(own)} is a model node's own label: it is its own table, which no "
                f"label write removes"
            )
        removed = {(n, key) for n, key, _ in given if (n, key) in held}
        if removed:
            await store.remove_labels(state, session, removed)
        return Labelled(0, len(removed))
    writes = store.Writes()
    seen: set[tuple[str, str]] = set()
    for node_id, key, model in given:
        if (node_id, key) in held or (node_id, key) in seen or key == model:
            continue
        seen.add((node_id, key))
        if model is None:
            writes.labels.append(store.NewLabel(node_id, key))
        else:
            writes.tags.append(store.Tag(node_id, model, key))
    if seen:
        await store.write(state, session, writes)
    return Labelled(len(seen), 0)
