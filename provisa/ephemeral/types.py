# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The column types of a session's ephemeral tables (REQ-2176): inferred from the values
written, and only ever widened -- a bigint to a double, anything incompatible to JSON. A NULL
adds no column. On Trino, which reads the store through its PostgreSQL connector, a duration, a
time with its offset or a list is held as JSON text."""

# Requirements: REQ-2176

from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from typing import Any

JSON = "json"
_NUMBERS = {"bigint", "double"}
#: The types an engine reading the store through a connector cannot read: held as JSON there.
_CONNECTOR_UNREADABLE = ("interval", "zoned_time")


def value_type(value: Any) -> str | None:
    """The column type a value is held as; None for a NULL."""
    from provisa.gql.temporal import Duration

    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, (float, Decimal)):
        # Cypher has no decimal: an engine's exact numeric for a float literal is a float.
        return "double"
    if isinstance(value, str):
        return "text"
    if isinstance(value, Duration):
        return "interval"
    if isinstance(value, _dt.datetime):
        return "timestamp" if value.tzinfo is None else "timestamptz"
    if isinstance(value, _dt.date):
        return "date"
    if isinstance(value, _dt.time):
        return "time" if value.tzinfo is None else "zoned_time"
    if isinstance(value, list):
        return _list_type(value)
    return JSON


def _list_type(items: list) -> str:
    """A list of one element type (NULLs aside) is a list of it; a mixed or nested one JSON."""
    held: str | None = None
    for item in items:
        if isinstance(item, (list, dict)):
            return JSON
        held = widened(held, value_type(item))
    if held is None or held == JSON:
        return JSON
    return f"list<{held}>"


def widened(held: str | None, given: str | None) -> str | None:
    """The type a column holding ``held`` takes when a value of ``given`` arrives."""
    if given is None or held == given:
        return held
    if held is None:
        return given
    if {held, given} == _NUMBERS:
        return "double"
    if held.startswith("list<") and given.startswith("list<"):
        inner = widened(held[5:-1], given[5:-1])
        return JSON if inner == JSON else f"list<{inner}>"
    return JSON


def for_engine(kind: str, engine: str) -> str:
    """The type ``kind`` is landed as for the serving ``engine``."""
    if engine == "trino" and (kind in _CONNECTOR_UNREADABLE or kind.startswith("list<")):
        return JSON
    return kind


def columns_for(
    held: list[tuple[str, str]], keys: tuple[str, ...], rows: list[dict[str, Any]], engine: str
) -> list[tuple[str, str]]:
    """A table's columns besides ``keys`` once ``rows`` (column -> value) are written: those it
    ``held`` widened by the values, then each new one in order of arrival."""
    types: dict[str, str | None] = {name: kind for name, kind in held}
    for row in rows:
        for name, value in row.items():
            if name in keys:
                continue
            given = value_type(value)
            types[name] = widened(types.get(name), for_engine(given, engine) if given else None)
    return [(name, kind) for name, kind in types.items() if kind is not None]


def relanded(
    held: list[tuple[str, str]],
    key_columns: tuple[tuple[str, str], ...],
    held_rows: list[tuple],
    new_rows: list[dict[str, Any]],
    engine: str,
) -> tuple[list[tuple[str, str]], list[tuple]]:
    """A table's columns and every row once ``new_rows`` join the ``held_rows`` it has (in its
    ``held`` columns' order): its keys as given, the other columns widened and grown, each held
    value converted to its column's type now, a column a row lacks NULL."""
    keys = tuple(k for k, _ in key_columns)
    columns = [
        *key_columns,
        *columns_for([c for c in held if c[0] not in keys], keys, new_rows, engine),
    ]
    was = dict(held)
    names = [k for k, _ in held]
    rows = [
        tuple(as_type(dict(zip(names, r)).get(k), kind, was.get(k)) for k, kind in columns)
        for r in held_rows
    ]
    rows += [tuple(as_type(r.get(k), kind, None) for k, kind in columns) for r in new_rows]
    return columns, rows


def as_type(value: Any, kind: str, was: str | None) -> Any:
    """``value`` (of a column that was ``was``, or new) as a column of ``kind`` holds it."""
    import json

    if value is None or kind == was:
        return value
    if kind == JSON:
        return value if was == JSON else json.dumps(_jsonable(value))
    if kind == "double":
        return float(value)
    if kind.startswith("list<") and kind[5:-1] == "double":
        return [None if v is None else float(v) for v in value]
    return value


def _jsonable(value: Any) -> Any:
    from provisa.gql.assembler import to_serializable

    return to_serializable(value)
