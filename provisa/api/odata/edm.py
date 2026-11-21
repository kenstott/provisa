# Copyright (c) 2026 Kenneth Stott
# Canary: 043edfb8-4d25-4b32-9a51-b251cc3cdade
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OData's primitive types: what a registered column is in $metadata (REQ-2193).

A response's values are written by the Rust OData encoder (executor/json_encoders.py, REQ-2194)
from the statement's own Arrow batches, each by its property's Edm type: not the GraphQL form,
where a decimal may become an integer, a string holding JSON becomes an object and a repeated row
is dropped, and each of those breaks a property's declared type or a page's row count.
"""

# Requirements: REQ-2193, REQ-2194

from __future__ import annotations

import re
from dataclasses import dataclass


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
