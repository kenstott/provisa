# Copyright (c) 2026 Kenneth Stott
# Canary: 3f2b8c41-7d6e-4a95-b1c0-9e8d2a4f6c13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Typed arrays, a type of the IR (REQ-1965, amended 2026-11-11, TYPED ARRAYS ARE AN IR TYPE).

An array whose innermost element is one primitive type is a semantic SQL type: the array of that
element type at its depth, spelled as PostgreSQL spells it -- ``integer[]``, ``text[][]``,
``numeric(10,2)[]``. It is recorded from the depth and element type every such column already
carries (``ir_types.ColumnTyping``). Structs, maps, arrays of structs and arrays of mixed elements
stay JSON columns.

An engine holds the type in its own array type where it has one that can hold the value
(PostgreSQL, DuckDB) and as JSON where it has none; :class:`TypedArray` is that choice for a
store, and the translation between the two forms is its own, so a reader never sees which.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from dateutil.relativedelta import relativedelta
from sqlalchemy import JSON
from sqlalchemy.engine import Dialect
from sqlalchemy.types import ARRAY, TypeDecorator, TypeEngine

from provisa.core.json_refusal import JsonRefused, NonFiniteNumber

ARRAY_SUFFIX = "[]"

# The IR types an array's element may be: a primitive. JSON is no primitive (an array of JSON is
# a JSON column), and an embedding is a vector of its own (REQ-424).
_NOT_ELEMENTS = frozenset({"json", "vector"})
_SIZED_NUMERIC = re.compile(r"\Anumeric\((\d+),(\d+)\)\Z")

# The stores whose own array type holds every value of a typed array, at any depth and with NULL
# elements: the array is held natively there, as JSON everywhere else.
NATIVE_ARRAY_DIALECTS = frozenset({"postgresql", "duckdb"})


class ArrayValueRefused(JsonRefused):
    """A value given for a typed array that is not an array of its element type at its depth."""

    code = "array.value_refused"

    def __init__(self, array_type: str, reason: str) -> None:
        self.params = {"type": array_type, "reason": reason}
        super().__init__(f"{self.code}: a value of {array_type} {reason}")


@dataclass(frozen=True)
class ArrayType:
    """An array of the primitive ``element`` (an IR type, a decimal with its precision and
    scale), nested ``depth`` levels deep."""

    element: str
    depth: int

    @property
    def name(self) -> str:
        """The IR spelling: the element's, a ``[]`` per level."""
        return self.element + ARRAY_SUFFIX * self.depth

    @property
    def inner(self) -> ArrayType | str:
        """What one level down holds: the array a level shallower, or the element."""
        return self.element if self.depth == 1 else ArrayType(self.element, self.depth - 1)


def is_element_type(name: str) -> bool:
    """Whether ``name`` is the canonical IR name of a primitive an array may hold."""
    from provisa.core.ir_types import IR_TYPES

    text = name.strip().lower()
    if _SIZED_NUMERIC.match(text):
        return True
    return text in IR_TYPES and text not in _NOT_ELEMENTS


def array_type(element: str, depth: int) -> ArrayType:
    """The array of ``element`` at ``depth``; raises ValueError for anything that is not one."""
    text = element.strip().lower()
    if depth < 1 or not is_element_type(text):
        raise ValueError(f"no IR array of {element!r} at depth {depth}")
    return ArrayType(text, depth)


def array_of(type_name: str | None) -> ArrayType | None:
    """The typed array ``type_name`` spells in the IR, or None when it spells none."""
    if not type_name:
        return None
    text = type_name.strip().lower()
    depth = 0
    while text.endswith(ARRAY_SUFFIX):
        depth += 1
        text = text[: -len(ARRAY_SUFFIX)].rstrip()
    if depth == 0 or not is_element_type(text):
        return None
    return ArrayType(text, depth)


def column_array_type(array_depth: int | None, element_type: str | None) -> ArrayType | None:
    """The typed array a column's hidden attributes record (REQ-1965), or None for a column that
    is no array of a primitive."""
    if not array_depth or not element_type or not is_element_type(element_type):
        return None
    return ArrayType(element_type.strip().lower(), array_depth)


def column_ir_type(data_type: str, array_depth: int | None, element_type: str | None) -> str:
    """A column's IR type: its typed array where its hidden attributes record one, else the type
    it is registered as."""
    typed = column_array_type(array_depth, element_type)
    return typed.name if typed is not None else data_type


# ---- an element in and out of its JSON form --------------------------------------------------


def _infinity(value: Any) -> int:  # object-ok: a date or timestamp element
    from provisa.core.ir_types import infinity_of

    return infinity_of(value)


def _temporal_text(value: Any) -> str:  # object-ok: a date or timestamp element
    sign = _infinity(value)
    if sign:
        return "infinity" if sign > 0 else "-infinity"
    return value.isoformat()


def _encode_float(value: Any) -> float:  # object-ok: a float element
    number = float(value)
    if not math.isfinite(number):
        raise NonFiniteNumber()
    return number


def _encode_timestamptz(value: Any) -> str:  # object-ok: an instant element
    if isinstance(value, datetime) and not _infinity(value):
        zoned = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return zoned.astimezone(timezone.utc).isoformat()
    return _temporal_text(value)


def _encode_interval(value: Any) -> str:  # object-ok: an interval element
    from provisa.core.ir_types import interval_parts

    months, days, micros = interval_parts(value)
    return f"{months} {days} {micros}"


def _encode_bytea(value: Any) -> str:  # object-ok: a bytea element
    from provisa.core.ir_types import bytea_hex

    return bytea_hex(value)


_ENCODERS: dict[str, Callable[[Any], Any]] = {
    "smallint": int,
    "integer": int,
    "bigint": int,
    "boolean": bool,
    "text": str,
    "float": _encode_float,
    "double": _encode_float,
    # A decimal is written as its digits, so it is exact on every engine.
    "numeric": lambda v: str(Decimal(str(v))),
    "date": _temporal_text,
    "timestamp": _temporal_text,
    "timestamptz": _encode_timestamptz,
    "time": lambda v: v.isoformat(),
    "interval": _encode_interval,
    "uuid": str,
    "bytea": _encode_bytea,
}


def _decode_date(text: str) -> date:
    from provisa.core.ir_types import infinite_date

    if text in ("infinity", "-infinity"):
        return infinite_date(-1 if text[0] == "-" else 1)
    return date.fromisoformat(text)


def _decode_timestamp(text: str, *, aware: bool) -> datetime:
    from provisa.core.ir_types import infinite_timestamp

    if text in ("infinity", "-infinity"):
        return infinite_timestamp(-1 if text[0] == "-" else 1, aware=aware)
    parsed = datetime.fromisoformat(text)
    return parsed.astimezone(timezone.utc) if aware else parsed


def _decode_interval(text: str) -> timedelta | relativedelta:
    from provisa.core.ir_types import interval_value

    months, days, micros = (int(part) for part in text.split(" "))
    return interval_value(months, days, micros)


_DECODERS: dict[str, Callable[[Any], Any]] = {
    "smallint": int,
    "integer": int,
    "bigint": int,
    "boolean": bool,
    "text": str,
    "float": float,
    "double": float,
    "numeric": lambda v: Decimal(str(v)),
    "date": _decode_date,
    "timestamp": lambda v: _decode_timestamp(v, aware=False),
    "timestamptz": lambda v: _decode_timestamp(v, aware=True),
    "time": time.fromisoformat,
    "interval": _decode_interval,
    "uuid": UUID,
    "bytea": lambda v: bytes.fromhex(v[2:]),
}


def _codec_key(element: str) -> str:
    return "numeric" if _SIZED_NUMERIC.match(element) else element


def _walk(
    value: Any, array: ArrayType, leaf: Callable[[Any], Any]
) -> list[Any]:  # object-ok: an array value at any level
    if not isinstance(value, (list, tuple)):
        raise ArrayValueRefused(array.name, f"holds {type(value).__name__} where a list belongs")
    inner = array.inner
    out: list[Any] = []
    for item in value:
        if item is None:
            out.append(None)
        elif isinstance(inner, ArrayType):
            out.append(_walk(item, inner, leaf))
        elif isinstance(item, (list, tuple)):
            raise ArrayValueRefused(array.name, f"is nested deeper than {array.depth}")
        else:
            out.append(leaf(item))
    return out


def to_json_form(value: Any, array: ArrayType) -> list[Any]:  # object-ok: an array value
    """``value`` (a list, nested to the array's depth) in the JSON form an engine with no array
    type holds it in: each element as JSON carries it exactly -- a decimal as its digits, a date
    or time as ISO 8601, bytes as ``\\x`` hex. NaN and the infinities are refused by name."""
    return _walk(value, array, _ENCODERS[_codec_key(array.element)])


def from_json_form(value: Any, array: ArrayType) -> list[Any]:  # object-ok: an array's JSON
    """The typed array the JSON form ``value`` (decoded or text) holds."""
    if isinstance(value, str):
        value = json.loads(value)
    return _walk(value, array, _DECODERS[_codec_key(array.element)])


class TypedArray(TypeDecorator):
    """A typed array in a store: the store's own array type where it has one that holds every
    value (``NATIVE_ARRAY_DIALECTS``), JSON elsewhere. A value is the typed list in and out on
    every store; a JSON text given for one is read as its JSON form."""

    impl = JSON
    cache_ok = True

    def __init__(self, element: str, depth: int) -> None:
        super().__init__()
        self.array = array_type(element, depth)

    def _native(self) -> TypeEngine[Any]:
        from provisa.core.ir_types import to_sqlalchemy

        element = to_sqlalchemy(self.array.element)
        element = element() if isinstance(element, type) else element
        return ARRAY(element, dimensions=self.array.depth)

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name in NATIVE_ARRAY_DIALECTS:
            return dialect.type_descriptor(self._native())
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:  # object-ok: a bound array
        if value is None:
            return None
        if isinstance(value, str):
            value = from_json_form(value, self.array)
        if dialect.name in NATIVE_ARRAY_DIALECTS:
            return _walk(value, self.array, lambda v: v)
        return to_json_form(value, self.array)

    def process_result_value(self, value: Any, dialect: Dialect) -> Any:  # object-ok: a read array
        if value is None:
            return None
        if dialect.name in NATIVE_ARRAY_DIALECTS:
            return _walk(value, self.array, lambda v: v)
        return from_json_form(value, self.array)
