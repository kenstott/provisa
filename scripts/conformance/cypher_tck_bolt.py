# Copyright (c) 2026 Kenneth Stott
# Canary: acf89d8f-44be-4924-9844-420951c4c99c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A TCK scenario run in one ephemeral session over Bolt (REQ-2167, amended 2026-10-27;
REQ-2176), for scripts/conformance/cypher_tck.py.

A scenario whose setup builds its graph runs its setup's statements and then its query on one
Bolt connection: the setup's CREATEs make the session's own nodes and relationships, and the query
reads them. A scenario on an empty graph that matches a pattern runs the same way with no setup.
Each scenario has a connection of its own (the driver's pool holds one, closed after it); the
user's graph session is the same on every connection, so the runner empties it before each
scenario and a session's graph is never read by the next. The connection signs in with the TCK session user's
token and acts as ``tck_empty``, the role that user's name maps to, which reaches no model domain:
its graph is the session's alone, the TCK's empty graph.

Values come back as the driver reads them and are put in the forms the runner compares: a node
as ("node", labels, properties), a relationship as ("relationship", type, properties), a path as
("path", [its nodes and relationships in turn]), and a temporal value as the text openCypher
writes it."""

# Requirements: REQ-2167, REQ-2176

from __future__ import annotations

import re
from typing import Any

#: The role a session scenario acts as: it reaches no model domain (pgwire_catalog_types.py).
EMPTY_ROLE = "tck_empty"

#: The Bolt endpoint and the TCK session user's session token (main sets them).
_BOLT: dict[str, str] = {}


def configure(uri: str, token: str) -> None:
    _BOLT.update(uri=uri, token=token)


def configured() -> bool:
    return bool(_BOLT)


def authorization() -> str:
    """The TCK session user's credential over HTTP: its session token as a bearer token."""
    return f"Bearer {_BOLT['token']}"


def run(setups: list[str], query: str, params: dict) -> tuple[int, Any]:
    """The setup's statements and then the query, on one connection: (200, the answer's columns,
    rows and the query's summary counters as Bolt sends them) or (400, the refusal's words)."""
    from neo4j import GraphDatabase, bearer_auth
    from neo4j.exceptions import Neo4jError

    driver = GraphDatabase.driver(
        _BOLT["uri"], auth=bearer_auth(_BOLT["token"]), max_connection_pool_size=1
    )
    try:
        with driver.session(database=f"provisa_{EMPTY_ROLE}") as session:
            for statement in setups:
                try:
                    session.run(statement).consume()
                except Neo4jError as exc:
                    return 400, {"detail": f"setup: {exc.message}"}
            try:
                result = session.run(query, params)
                columns = list(result.keys())
                rows = [[value(v) for v in record.values()] for record in result]
                # Bolt sends a summary's "stats" only when the statement changed something, and
                # only its non-zero counters: an absent map is every counter at zero.
                stats = dict(result.consume().metadata.get("stats", {}))
            except Neo4jError as exc:
                return 400, {"detail": exc.message}
    finally:
        driver.close()
    return 200, {"columns": columns, "rows": rows, "stats": stats}


# --- values as the driver reads them ----------------------------------------------------------


def value(v: Any) -> Any:
    """A driver value in the form the runner compares."""
    from neo4j import graph, time

    if isinstance(v, graph.Node):
        return ("node", frozenset(v.labels), {k: value(x) for k, x in v.items()})
    if isinstance(v, graph.Relationship):
        return ("relationship", v.type, {k: value(x) for k, x in v.items()})
    if isinstance(v, graph.Path):
        elements: list[Any] = [value(v.start_node)]
        for rel, node in zip(v.relationships, v.nodes[1:], strict=True):
            elements += [value(rel), value(node)]
        return ("path", elements)
    if isinstance(v, list):
        return [value(x) for x in v]
    if isinstance(v, dict):
        return {k: value(x) for k, x in v.items()}
    if isinstance(v, time.Duration):
        return _duration_text(v)
    if isinstance(v, (time.Date, time.Time, time.DateTime)):
        return _temporal_text(v)
    return v


def _fraction(nanosecond: int) -> str:
    return f".{nanosecond:09d}".rstrip("0") if nanosecond else ""


def _clock(t: Any) -> str:
    """hh:mm, then :ss and the fraction where there are any, as openCypher writes a time."""
    text = f"{t.hour:02d}:{t.minute:02d}"
    if t.second or t.nanosecond:
        text += f":{t.second:02d}{_fraction(t.nanosecond)}"
    return text


def _offset(seconds: int) -> str:
    if seconds == 0:
        return "Z"
    sign = "-" if seconds < 0 else "+"
    minutes, rest = divmod(abs(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{sign}{hours:02d}:{minutes:02d}" + (f":{rest:02d}" if rest else "")


def _temporal_text(v: Any) -> str:
    from neo4j import time

    if isinstance(v, time.Date):
        return v.iso_format()
    if isinstance(v, time.Time):
        if v.tzinfo is None:
            return _clock(v)
        offset = v.utcoffset()
        return _clock(v) + _offset(int(offset.total_seconds()) if offset is not None else 0)
    text = f"{v.date().iso_format()}T{_clock(v.time())}"
    if v.tzinfo is None:
        return text
    offset = v.utcoffset()
    text += _offset(int(offset.total_seconds()) if offset is not None else 0)
    zone = getattr(v.tzinfo, "key", None) or getattr(v.tzinfo, "zone", None)
    return text + (f"[{zone}]" if zone else "")


def _duration_text(d: Any) -> str:
    """openCypher's ISO-8601 text of a duration, each part with its own sign, PT0S for none."""
    years, months = int(d.months / 12), d.months - 12 * int(d.months / 12)
    seconds_total = d.seconds
    hours = int(seconds_total / 3600)
    minutes = int((seconds_total - hours * 3600) / 60)
    seconds = seconds_total - hours * 3600 - minutes * 60
    date = "".join(f"{n}{u}" for n, u in ((years, "Y"), (months, "M"), (d.days, "D")) if n)
    nanos = seconds * 1_000_000_000 + d.nanoseconds
    sec = ""
    if nanos:
        whole, frac = divmod(abs(nanos), 1_000_000_000)
        sec = f"{'-' if nanos < 0 else ''}{whole}{_fraction(frac)}S"
    clock = "".join(f"{n}{u}" for n, u in ((hours, "H"), (minutes, "M")) if n) + sec
    text = "P" + date + (f"T{clock}" if clock else "")
    return "PT0S" if text == "P" else text


# --- the TCK's nodes, relationships and paths -------------------------------------------------

_LABELS = re.compile(r":\s*`?([A-Za-z_][A-Za-z0-9_]*)`?")


def _properties(text: str, read: Any) -> dict:
    start = text.find("{")
    return read(text[start : text.rfind("}") + 1]) if start >= 0 else {}


def expected_element(kind: str, text: str, read: Any) -> Any:
    """A TCK node ``(:A {k: 1})``, relationship ``[:T {k: 1}]`` or path ``<(a)-[:R]->(b)>`` in
    the form :func:`value` gives; ``read`` reads a TCK map."""
    if kind == "node":
        inner = text.strip()[1:-1]
        head = inner.split("{", 1)[0]
        return ("node", frozenset(_LABELS.findall(head)), _properties(inner, read))
    if kind == "relationship":
        inner = text.strip()[1:-1]
        head = inner.split("{", 1)[0]
        types = _LABELS.findall(head)
        return ("relationship", types[0] if types else None, _properties(inner, read))
    elements: list[Any] = []
    depth, start = 0, None
    for at, ch in enumerate(text):
        if ch in "([" and depth == 0:
            start = at
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0 and start is not None:
                part = text[start : at + 1]
                elements.append(
                    expected_element("node" if ch == ")" else "relationship", part, read)
                )
                start = None
    return ("path", elements)


def same_element(expected: Any, got: Any, same: Any) -> bool:
    """Whether ``got`` is the node, relationship or path ``expected`` is (direction not
    compared); ``same`` compares two values."""
    if not (isinstance(got, tuple) and got and got[0] == expected[0]):
        return False
    if expected[0] == "path":
        return len(expected[1]) == len(got[1]) and all(
            same_element(e, g, same) for e, g in zip(expected[1], got[1], strict=True)
        )
    _, name, props = expected
    _, got_name, got_props = got
    return name == got_name and same(props, got_props, False)
