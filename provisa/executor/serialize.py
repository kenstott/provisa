# Copyright (c) 2026 Kenneth Stott
# Canary: c88a327d-c038-4dec-a4c5-f292b2675f6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A result value as a Python consumer of rows takes it (REQ-1867): ``column_value`` (a DECIMAL's
exact digits, PostgreSQL's infinities, JSON parsed only from a json/jsonb column, integers by
type), ``result_records`` and the Relay connection (REQ-218).

The JSON surfaces (GraphQL, REST, JSON:API, OData) do not come here: their answers are written by
the Rust encoders from the engine's Arrow batches (executor/json_encoders.py, REQ-2194), the
nesting of flat JOIN rows (REQ-047) included.
"""

# Requirements: REQ-047, REQ-218, REQ-1867

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta
from decimal import Decimal

from provisa.compiler.cursor import encode_cursor
from provisa.compiler.sql_types import is_integer_type, is_json_type
from provisa.core.ir_types import bytea_hex, infinity_of, iso8601_duration
from provisa.core.json_refusal import JsonValueRefused
from provisa.core.statement_warnings import ServerWarning
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.executor.result import QueryResult


_FRACTION_ZEROS = re.compile(r"(\.\d*?[1-9])0+(?=$|[+-])")


def temporal_text(value: date) -> str:
    """A date, time or timestamp as JSON writes it at any depth: PostgreSQL's json, which has the T
    and no trailing zeros in a fraction. PostgreSQL's infinite date or timestamp is its own text,
    ``infinity`` or ``-infinity``, as pgwire writes it (REQ-1867)."""
    sign = infinity_of(value)
    if sign:
        return "infinity" if sign > 0 else "-infinity"
    return _FRACTION_ZEROS.sub(r"\1", value.isoformat())


def decimal_text(value: Decimal) -> str:
    """A DECIMAL as the JSON string of its exact digits (REQ-1867): never a float, which loses
    digits past about 17 and cannot hold a value past 2^64. Positional, never an exponent:
    ``Decimal("1E+3")`` is ``"1000"``, ``Decimal("0E-7")`` is ``"0.0000000"``."""
    return format(value, "f")


class JsonNumber(float):
    """A number inside JSON text, carrying the text it was written as (REQ-1867): the response
    writes that text (``orjson.Fragment``, json_response), so a number past a float's 17 digits
    keeps them. It is a float everywhere else -- a surface the standard library or pydantic
    writes still writes a JSON number, the nearest float."""

    text: str

    def __new__(cls, text: str) -> JsonNumber:
        number = super().__new__(cls, text)
        number.text = text
        return number

    def exact(self) -> Decimal:
        """The number's exact value."""
        return Decimal(self.text)


def integer_value(
    val: object,  # object-ok: an integer column's value — int | Decimal | None
    field: str,
) -> object:
    """An integer-typed column's value as the JSON number it is (REQ-1867), whatever a reader
    represented it as: a DuckDB HUGEINT read through Arrow is a ``decimal128(38,0)`` Decimal."""
    if isinstance(val, Decimal):
        if val != val.to_integral_value():
            raise ValueError(f"field {field!r} is typed integer but holds {val}")
        return int(val)
    return val


def _convert_value(
    val: object,  # object-ok: arbitrary SQL column value — Decimal | date | str | int | dict | list | None
) -> object:
    """Convert database types to JSON-safe Python types. Text is the string it is, even when it
    reads as JSON (REQ-1867): only a JSON-typed column's text is JSON (:func:`column_value`)."""
    if isinstance(val, Decimal):
        return decimal_text(val)
    if isinstance(val, (timedelta, relativedelta)):
        return iso8601_duration(val)  # an IR interval's canonical text form
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytea_hex(val)  # an IR bytea's canonical text form
    if hasattr(val, "isoformat"):
        from typing import cast as _cast

        return temporal_text(_cast(date, val))  # guarded by hasattr — any date/datetime-like type
    # A native nested value (a struct, a list, a JSON value a driver already parsed): each
    # element converted as a column value is.
    if isinstance(val, dict):
        return {k: _convert_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_convert_value(v) for v in val]
    return val


def _refuse_constant(field: str) -> Callable[[str], object]:
    def refuse(token: str) -> object:
        raise JsonValueRefused(field, f"holds {token}, a number JSON cannot carry")

    return refuse


def _no_lone_surrogate(
    val: object,  # object-ok: a parsed JSON value — dict | list | str | number | bool | None
    field: str,
) -> None:
    """Refuse a parsed JSON value holding a lone UTF-16 surrogate (``"\\ud800"``): it is no
    character, and no UTF-8 response can carry it."""
    if isinstance(val, dict):
        for k, v in val.items():
            _no_lone_surrogate(k, field)
            _no_lone_surrogate(v, field)
    elif isinstance(val, list):
        for v in val:
            _no_lone_surrogate(v, field)
    elif isinstance(val, str):
        try:
            val.encode("utf-8")
        except UnicodeEncodeError:
            raise JsonValueRefused(
                field, "holds a lone UTF-16 surrogate escape, which is no character"
            ) from None


def _parse_json_text(text: str, field: str) -> object:
    """JSON text a JSON-typed column (or the engine, for a relationship) wrote, as its value.
    What JSON cannot carry is refused, naming the field: NaN and Infinity, a lone surrogate,
    nesting deeper than the parser reaches, text that is not JSON."""
    try:
        # A number keeps its own text (JsonNumber); an integer is Python's, any size.
        parsed = json.loads(text, parse_float=JsonNumber, parse_constant=_refuse_constant(field))
    except RecursionError:
        raise JsonValueRefused(field, "nests deeper than a JSON response can carry") from None
    except json.JSONDecodeError as exc:
        raise JsonValueRefused(field, f"is typed JSON but its text is not JSON ({exc})") from None
    if "\\u" in text:
        _no_lone_surrogate(parsed, field)
    return parsed


def _json_walk(
    val: object,  # object-ok: a JSON value — dict | list | str | number | bool | None
    field: str,
    json_text: frozenset[str],
    path: str,
) -> object:
    """A JSON value's elements as JSON writes them. A string at one of ``json_text``'s key paths
    (a list's elements share their list's path) is JSON text the engine wrote and is parsed; any
    other string is text."""
    if isinstance(val, dict):
        out: dict = {}
        for k, v in val.items():
            at = f"{path}{k}"
            if isinstance(v, str) and at in json_text:
                v = _parse_json_text(v, field)
            out[k] = _json_walk(v, field, json_text, f"{at}.")
        return out
    if isinstance(val, list):
        return [_json_walk(v, field, json_text, path) for v in val]
    return _convert_value(val)


def json_value(
    val: object,  # object-ok: a JSON column's value — JSON text, or a value a driver parsed
    field: str,
    json_text: frozenset[str] = frozenset(),
) -> object:
    """A JSON-typed value (a json or jsonb column, a relationship built as JSON) as the JSON it
    holds: its text parsed, refused by name where JSON cannot carry it (REQ-1867)."""
    if isinstance(val, str):
        val = _parse_json_text(val, field)
    return _json_walk(val, field, json_text, "")


def column_value(
    val: object,  # object-ok: arbitrary SQL column value
    col: ColumnRef,
) -> object:
    """One result value of ``col`` as JSON writes it: a JSON-typed column's as its JSON, an
    integer-typed column's as its number, any other as :func:`_convert_value` converts it
    (REQ-1867)."""
    if col.json:
        return json_value(val, col.field_name, col.json_text)
    if col.integer:
        return integer_value(val, col.field_name)
    return _convert_value(val)


def result_records(result: QueryResult) -> list[dict]:
    """A result's rows as ``{column: value}``, each value as JSON writes it: a column the engine
    types json or jsonb as its JSON, one it types an integer as its number (by the engine's type,
    not its reader's representation), any other as :func:`_convert_value` converts it."""
    names = result.column_names
    types = result.column_types or [None] * len(names)
    cols = [
        ColumnRef(None, c, c, None, json=is_json_type(t), integer=is_integer_type(t))
        for c, t in zip(names, types, strict=True)
    ]
    return [
        {col.field_name: column_value(v, col) for col, v in zip(cols, r, strict=True)}
        for r in result.rows
    ]


#: REQ-1350: the code of the warning a to-one relationship that matched several rows raises
TO_ONE_TRUNCATED = "graphql.to_one_truncated"
#: Its message, ``{path}`` where the relationship's path goes (the JSON encoders write it too).
TO_ONE_TRUNCATED_MESSAGE = (
    "many-to-one relationship '{path}' returned multiple rows "
    "for the same parent; only the first value was used. "
    "Check cardinality — this relationship may need to be one-to-many."
)


def to_one_truncated(path: str) -> ServerWarning:
    """The warning a many-to-one relationship at ``path`` that matched several rows for one
    parent raises: only the first row's object was answered."""
    return ServerWarning(
        code=TO_ONE_TRUNCATED,
        message=TO_ONE_TRUNCATED_MESSAGE.format(path=path),
        params={"path": path},
    )


def _cursor_value(
    val: object,  # object-ok: a sort column's value
    col: ColumnRef,
) -> object:
    """A sort key's value in the opaque cursor, which compiles back into a keyset comparison
    (cursor.cursor_where_clause): a DECIMAL stays the Decimal it is, which the cursor carries by
    its exact digits and the comparison binds as a decimal (REQ-218, REQ-1867)."""
    if isinstance(val, Decimal) and not col.integer:
        return val
    return column_value(val, col)


def serialize_connection(  # REQ-218
    rows: list[tuple],
    compiled: CompiledQuery,
) -> dict:
    """Serialize flat SQL rows into Relay-style connection format."""
    columns = compiled.columns
    sort_columns = compiled.sort_columns
    page_size = compiled.page_size
    is_backward = compiled.is_backward

    has_more = False
    if page_size is not None and len(rows) > page_size:
        has_more = True
        rows = rows[:page_size]

    if is_backward:
        rows = list(reversed(rows))

    col_index = {col.field_name: i for i, col in enumerate(columns)}

    edges = []
    for row in rows:
        node: dict = {}
        for i, col in enumerate(columns):
            node[col.field_name] = column_value(row[i], col)

        cursor_vals = []
        for sc in sort_columns:
            idx = col_index.get(sc)
            if idx is not None:
                cursor_vals.append(_cursor_value(row[idx], columns[idx]))
        cursor = encode_cursor(cursor_vals)
        edges.append({"cursor": cursor, "node": node})

    if not is_backward:
        has_next = has_more
        has_prev = compiled.has_cursor
    else:
        has_next = compiled.has_cursor
        has_prev = has_more

    page_info = {
        "hasNextPage": has_next,
        "hasPreviousPage": has_prev,
        "startCursor": edges[0]["cursor"] if edges else None,
        "endCursor": edges[-1]["cursor"] if edges else None,
    }

    return {"data": {compiled.root_field: {"edges": edges, "pageInfo": page_info}}}
