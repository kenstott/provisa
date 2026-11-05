# Copyright (c) 2026 Kenneth Stott
# Canary: 6fb72ddf-e60e-4eaa-a893-77b407c0875e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's index statements are the graph session's own indexes (REQ-2176).

- ``CREATE INDEX [name] [IF NOT EXISTS] FOR (n:L) ON (n.p, ...)`` indexes the session's anchor
  table for the label L in the scratch store, and ``... FOR ()-[r:T]-() ON (r.p, ...)`` the
  property table of the relationship type T (store.build_indexes: once the table holds the
  columns, and again whenever it is landed). A label or type of the model -- a governed table,
  not in the scratch store -- is refused by name.
- A session index lives as long as the graph session (it is held in its graph) and goes when the
  session is cleared or reaped, its tables with it.
- ``SHOW INDEXES`` lists the session's indexes and the built-in ones on its tables (each table's
  key; rels on from_id, to_id and type; node_labels on label and node_id), marked so.
- ``DROP INDEX name [IF EXISTS]`` drops a session index and refuses a built-in one.

No right beyond the session's own is needed.
"""

# Requirements: REQ-2176

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store

_NAME = r"(?:[A-Za-z_][A-Za-z0-9_]*|`(?:[^`]|``)*`)"
_KINDS = r"(?:(?P<kind>[A-Za-z]+)\s+)?"
_CREATE = re.compile(
    rf"\s*CREATE\s+{_KINDS}INDEX(?:\s+(?!IF\b|FOR\b|ON\b)(?P<name>{_NAME}))?"
    rf"(?P<ifnot>\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+"
    rf"(?:\(\s*(?P<nv>{_NAME})\s*:\s*(?P<label>{_NAME})\s*\)"
    rf"|\(\s*\)\s*<?-\s*\[\s*(?P<rv>{_NAME})\s*:\s*(?P<type>{_NAME})\s*\]\s*->?\s*\(\s*\))"
    rf"\s+ON\s*\((?P<props>[^)]*)\)\s*;?\s*\Z",
    re.I | re.S,
)
_CREATE_OLD = re.compile(
    rf"\s*CREATE\s+INDEX\s+ON\s*:\s*(?P<label>{_NAME})\s*\((?P<props>[^)]*)\)\s*;?\s*\Z", re.I
)
_SHOW = re.compile(
    r"\s*SHOW\s+(?:(?P<kind>[A-Za-z]+)\s+)?INDEX(?:ES)?\b(?P<more>.*?);?\s*\Z", re.I | re.S
)
_DROP = re.compile(
    rf"\s*DROP\s+INDEX\s+(?P<name>{_NAME})(?P<ifexists>\s+IF\s+EXISTS)?\s*;?\s*\Z", re.I
)
_DROP_OLD = re.compile(
    rf"\s*DROP\s+INDEX\s+ON\s*:\s*(?P<label>{_NAME})\s*\((?P<props>[^)]*)\)\s*;?\s*\Z", re.I
)
_STARTS = re.compile(
    r"\s*(?:CREATE\s+(?:[A-Za-z]+\s+)?INDEX|SHOW\s+(?:[A-Za-z]+\s+)?INDEX(?:ES)?|DROP\s+INDEX)\b",
    re.I,
)

#: SHOW INDEXES' columns.
COLUMNS = ["name", "type", "entityType", "labelsOrTypes", "properties", "table", "builtIn"]


@dataclass(frozen=True)
class Indexed:
    """What an index statement did, for its answer's counters."""

    indexes_added: int
    indexes_removed: int


def _unquoted(name: str) -> str:
    return name[1:-1].replace("``", "`") if name.startswith("`") else name


def is_index_statement(statement: str) -> bool:
    """Whether ``statement`` is one of Cypher's index statements."""
    return _STARTS.match(statement) is not None


def _props(text: str, var: str | None) -> tuple[str, ...]:
    """The properties an index's ON list names (``n.p, n.q``; ``p, q`` in the old form)."""
    out: list[str] = []
    for item in text.split(","):
        item = item.strip()
        m = re.fullmatch(rf"({_NAME})\s*\.\s*({_NAME})", item) if var else None
        if var and (m is None or _unquoted(m.group(1)) != var):
            raise CypherParseError(f"An index is ON its pattern's own properties: {item!r}")
        name = _unquoted(m.group(2)) if m else _unquoted(item)
        if not name:
            raise CypherParseError("An index names at least one property")
        out.append(name)
    return tuple(out)


def _kind(kind: str | None) -> None:
    if kind is not None and kind.upper() not in ("RANGE", "BTREE"):
        raise CypherParseError(
            f"A {kind.upper()} index is not made: a session index is an index on its table's "
            f"columns (a RANGE index)"
        )


def _model(label_map: Any, kind: str, of: str) -> bool:
    """Whether ``of`` is the model's label (kind "node") or relationship type (kind "rel")."""
    from provisa.ephemeral.classify import _model_type, resolve

    if kind == "node":
        return bool(resolve([of], label_map).models)
    return _model_type(label_map, of)


def _builtin(session: TempSession) -> list[dict[str, Any]]:
    """The built-in indexes on the session's tables: each table's key, and the lookups."""
    out: list[dict[str, Any]] = []
    for table in sorted(session.tables):
        if not table.startswith("cypher__"):
            continue
        columns = session.tables[table].columns
        if not columns:
            continue
        indexed = [store.key_of(table, columns), *store._LOOKUPS.get(table, ())]
        for i, props in enumerate(indexed):
            out.append(
                {
                    "name": f"{'key' if i == 0 else 'on'}__{table}__{'_'.join(props)}",
                    "type": "RANGE",
                    "entityType": "RELATIONSHIP"
                    if table == store.RELS or table.startswith("cypher__rel__")
                    else "NODE",
                    "labelsOrTypes": None,
                    "properties": list(props),
                    "table": table,
                    "builtIn": True,
                }
            )
    return out


def _session_rows(session: TempSession) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    graph = store.graph(session)
    for name, (kind, of, props) in sorted(graph.indexes.items()):
        out.append(
            {
                "name": name,
                "type": "RANGE",
                "entityType": "NODE" if kind == "node" else "RELATIONSHIP",
                "labelsOrTypes": [of],
                "properties": list(props),
                "table": store.index_table(graph, kind, of),
                "builtIn": False,
            }
        )
    return out


async def index_statement(statement: str, label_map: Any, session: TempSession, state: Any) -> Any:
    """Do ``statement`` when it is an index statement (an Indexed, or SHOW's then.Answered);
    None when it is anything else."""
    from provisa.ephemeral.then import Answered
    from provisa.pgwire import temp_exec

    if not is_index_statement(statement):
        return None
    graph = store.graph(session)
    shown = _SHOW.match(statement)
    if shown is not None:
        if shown.group("kind") and shown.group("kind").upper() not in ("ALL", "RANGE", "BTREE"):
            rows: list[dict[str, Any]] = []
        elif shown.group("more").strip():
            raise CypherParseError(
                f"SHOW INDEXES is answered whole: {shown.group('more').strip()!r} is not read"
            )
        else:
            rows = _session_rows(session) + _builtin(session)
        return Answered(list(COLUMNS), rows, {})
    m = _CREATE.match(statement) or _CREATE_OLD.match(statement)
    if m is not None:
        groups = m.groupdict()
        _kind(groups.get("kind"))
        if groups.get("label"):
            kind, of = "node", _unquoted(groups["label"])
            props = _props(groups["props"], _unquoted(groups["nv"]) if groups.get("nv") else None)
        else:
            kind, of = "rel", _unquoted(groups["type"])
            props = _props(groups["props"], _unquoted(groups["rv"]))
        if _model(label_map, kind, of):
            what = "label" if kind == "node" else "relationship type"
            raise CypherParseError(
                f"{of} is a model {what}: its table is governed, not in the graph session's "
                f"scratch store, and CREATE INDEX indexes only the session's own"
            )
        name = (
            _unquoted(groups["name"])
            if groups.get("name")
            else (
                "index_" + hashlib.sha256(f"{kind}|{of}|{'|'.join(props)}".encode()).hexdigest()[:8]
            )
        )
        quiet = bool(groups.get("ifnot"))
        same = [n for n, held in graph.indexes.items() if held == (kind, of, props)]
        if name in graph.indexes or same or name in {r["name"] for r in _builtin(session)}:
            if quiet:
                return Indexed(0, 0)
            held_as = name if name in graph.indexes else (same[0] if same else name)
            raise CypherParseError(f"An index {held_as} already exists")
        graph.indexes[name] = (kind, of, props)
        await store.build_indexes(
            session, store.index_table(graph, kind, of), temp_exec.in_place(state)
        )
        return Indexed(1, 0)
    m = _DROP.match(statement)
    old = _DROP_OLD.match(statement)
    if m is None and old is None:
        raise CypherParseError(f"This index statement is not read: {statement.strip()!r}")
    if old is not None:
        wanted = ("node", _unquoted(old.group("label")), _props(old.group("props"), None))
        named = [n for n, held in graph.indexes.items() if held == wanted]
        if not named:
            raise CypherParseError(f"No session index is on :{wanted[1]}({', '.join(wanted[2])})")
        name = named[0]
    else:
        assert m is not None
        name = _unquoted(m.group("name"))
        if name in {r["name"] for r in _builtin(session)}:
            raise CypherParseError(
                f"{name} is a built-in index of the graph session's tables, which is not dropped"
            )
        if name not in graph.indexes:
            if m.group("ifexists"):
                return Indexed(0, 0)
            raise CypherParseError(f"No session index is named {name}")
    kind, of, props = graph.indexes.pop(name)
    table = store.index_table(graph, kind, of)
    writer = temp_exec.in_place(state)
    held = {c for c, _ in session.tables[table].columns} if table in session.tables else set()
    if writer is not None and set(props) <= held:
        await temp_exec.drop_lookup_index(session, table, props, writer)
    return Indexed(0, 1)
