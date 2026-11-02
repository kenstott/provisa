# Copyright (c) 2026 Kenneth Stott
# Canary: 043edfb8-4d25-4b32-9a51-b251cc3cdade
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OData's primitive types: what a registered column is in $metadata, and how its value is
written in a response (REQ-2193).

A response's values are written from the statement's own rows, not through
``executor.serialize``: that one writes the GraphQL form, where a decimal may become an integer, a
string holding JSON becomes an object and a repeated row is dropped, and each of those breaks a
property's declared type or a page's row count.
"""

# Requirements: REQ-2193

from __future__ import annotations

import base64
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

import orjson

from provisa.core.ir_types import to_ir


@dataclass(frozen=True)
class EdmType:
    name: str
    precision: int | None = None
    scale: int | str | None = None


_BINARY = "Edm.Binary"
_BOOLEAN = "Edm.Boolean"
_DATE = "Edm.Date"
_DATETIME = "Edm.DateTimeOffset"
_DECIMAL = "Edm.Decimal"
_DOUBLE = "Edm.Double"
_GUID = "Edm.Guid"
_INT16 = "Edm.Int16"
_INT32 = "Edm.Int32"
_INT64 = "Edm.Int64"
_SBYTE = "Edm.SByte"
_SINGLE = "Edm.Single"
_STRING = "Edm.String"
_TIME = "Edm.TimeOfDay"

# The spellings the IR does not name (each source's own), then the IR's. An interval is a string:
# Edm.Duration holds days and seconds, and an interval may hold months.
_EDM_NAMES: dict[str, str] = {
    "tinyint": _SBYTE,
    "int64": _INT64,
    "long": _INT64,
    "float64": _DOUBLE,
    "bignumeric": _DECIMAL,
    "varbinary": _BINARY,
    "bytes": _BINARY,
    "smallint": _INT16,
    "integer": _INT32,
    "bigint": _INT64,
    "float": _SINGLE,
    "double": _DOUBLE,
    "numeric": _DECIMAL,
    "boolean": _BOOLEAN,
    "date": _DATE,
    "time": _TIME,
    "timestamp": _DATETIME,
    "timestamptz": _DATETIME,
    "uuid": _GUID,
    "bytea": _BINARY,
}

_PRECISION_SCALE = re.compile(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")


def edm_type(data_type: str) -> EdmType:
    """The Edm type of a column of ``data_type``; a type with no Edm primitive of its own (JSON, an
    array, a network address) is a string, as it is a string field on gRPC."""
    normalized = data_type.lower().strip()
    base = normalized.split("(")[0].strip()
    name = _EDM_NAMES.get(base) or _EDM_NAMES.get(to_ir(normalized), _STRING)
    if name != _DECIMAL:
        return EdmType(name)
    sized = _PRECISION_SCALE.search(normalized)
    if sized is None:
        return EdmType(name, scale="variable")
    return EdmType(name, int(sized.group(1)), int(sized.group(2) or 0))


def _datetime_offset(value: Any) -> str:  # noqa: ANN401 — an engine's timestamp
    """A timestamp with its offset, which Edm.DateTimeOffset requires: a timestamp that has none
    is read as UTC. Engine text is parsed, and a value that is no timestamp raises."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    elif not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _binary(value: Any) -> str:  # noqa: ANN401 — an engine's binary value, or a masked one's text
    """Edm.Binary is base64url. Text is PostgreSQL's ``\\x`` form or, from a regex mask, the bytes
    of the masked text."""
    if isinstance(value, str):
        value = bytes.fromhex(value[2:]) if value.startswith("\\x") else value.encode()
    return base64.urlsafe_b64encode(bytes(value)).decode()


def _floating(value: Any) -> Any:  # noqa: ANN401 — an engine's float
    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "INF" if number > 0 else "-INF"
    return number


def _decimal(value: Any) -> Any:  # noqa: ANN401 — an engine's exact number
    """Written as the exact number it is: a float would round a long one."""
    exact = value if isinstance(value, Decimal) else Decimal(str(value))
    if not exact.is_finite():
        return _floating(exact)
    return orjson.Fragment(format(exact, "f"))


def _string(value: Any) -> str:  # noqa: ANN401 — a value with no Edm primitive of its own
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return orjson.dumps(value, default=str).decode()
    return str(value)


_WRITERS = {
    _BINARY: _binary,
    _BOOLEAN: bool,
    _DATE: lambda v: v.date().isoformat() if isinstance(v, datetime) else _iso(v),
    _DATETIME: _datetime_offset,
    _DECIMAL: _decimal,
    _DOUBLE: _floating,
    _GUID: str,
    _INT16: int,
    _INT32: int,
    _INT64: int,
    _SBYTE: int,
    _SINGLE: _floating,
    _STRING: _string,
    _TIME: lambda v: v.replace(tzinfo=None).isoformat() if isinstance(v, time) else v,
}


def _iso(value: Any) -> str:  # noqa: ANN401 — an engine's date
    return value.isoformat() if isinstance(value, date) else value


def edm_value(value: Any, edm: EdmType) -> Any:  # noqa: ANN401 — a statement's value
    """``value`` as a response writes a property of type ``edm``."""
    return None if value is None else _WRITERS[edm.name](value)
