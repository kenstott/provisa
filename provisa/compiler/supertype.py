# Copyright (c) 2026 Kenneth Stott
# Canary: a82a1edb-d02b-46c7-babd-bc51447e3d52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The common supertype of column types (REQ-2163).

A Property bound to columns of several tables is one column of its Class, so it takes one type:
the types' common supertype where one exists -- integers widen to the widest, an integer and a
decimal to a decimal holding both, any number and a float to a double, a date and a timestamp to
the timestamp, character types to varchar -- and varchar where none does.
"""

# Requirements: REQ-2163

from __future__ import annotations

import re

#: Integer types, each with its rank and the decimal digits it holds.
_INTEGERS: dict[str, tuple[int, int]] = {
    "tinyint": (1, 3),
    "smallint": (2, 5),
    "int2": (2, 5),
    "integer": (3, 10),
    "int": (3, 10),
    "int4": (3, 10),
    "bigint": (4, 19),
    "int8": (4, 19),
    "int64": (4, 19),
    "long": (4, 19),
}
_INTEGER_BY_RANK = {1: "tinyint", 2: "smallint", 3: "integer", 4: "bigint"}
_FLOATS: dict[str, str] = {
    "real": "real",
    "float4": "real",
    "float": "double",
    "float8": "double",
    "double": "double",
    "double precision": "double",
}
_CHARACTERS = {"varchar", "text", "char", "bpchar", "string", "nvarchar", "varchar2", "nvarchar2"}
_DECIMAL = re.compile(r"(?:decimal|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")
_TIMESTAMP = re.compile(r"timestamp(?:\s*\((\d+)\))?")
#: The widest decimal every engine declares.
_MAX_PRECISION = 38


def _kind(data_type: str) -> tuple[str, tuple] | None:
    """``data_type``'s family and its parameters, or None for a type no other widens to."""
    t = " ".join(data_type.lower().split())
    if t in _INTEGERS:
        return "integer", _INTEGERS[t]
    if t in _FLOATS:
        return "float", (_FLOATS[t],)
    m = _DECIMAL.fullmatch(t)
    if m:
        return "decimal", (int(m.group(1)), int(m.group(2) or 0))
    if t in ("decimal", "numeric"):
        return "decimal", (_MAX_PRECISION, 0)
    if t.split("(")[0] in _CHARACTERS:
        return "character", ()
    if t == "date":
        return "date", ()
    m = _TIMESTAMP.fullmatch(t)
    if m:
        return "timestamp", (int(m.group(1)) if m.group(1) else None,)
    return None


def _decimal(precision: int, scale: int) -> str:
    return f"decimal({min(precision, _MAX_PRECISION)},{scale})"


def _widen(a: tuple[str, tuple], b: tuple[str, tuple]) -> tuple[str, tuple] | None:
    """The common supertype of two kinds, or None where there is none."""
    (ka, pa), (kb, pb) = sorted((a, b))
    if ka == kb == "integer":
        return "integer", max(pa, pb)
    if ka == kb == "float":
        return "float", ("double" if "double" in (pa[0], pb[0]) else "real",)
    if ka == kb == "decimal":
        scale = max(pa[1], pb[1])
        return "decimal", (max(pa[0] - pa[1], pb[0] - pb[1]) + scale, scale)
    if (ka, kb) == ("decimal", "integer"):
        return "decimal", (max(pa[0] - pa[1], pb[1]) + pa[1], pa[1])
    if "float" in (ka, kb) and {ka, kb} <= {"float", "integer", "decimal"}:
        return "float", ("double",)
    if ka == kb == "character":
        return "character", ()
    if ka == kb == "timestamp":
        precisions = [p for p in (pa[0], pb[0]) if p is not None]
        return "timestamp", (max(precisions) if precisions else None,)
    if (ka, kb) == ("date", "timestamp"):
        return "timestamp", pb
    if ka == kb == "date":
        return "date", ()
    return None


def _spelled(kind: tuple[str, tuple]) -> str:
    family, params = kind
    if family == "integer":
        return _INTEGER_BY_RANK[params[0]]
    if family == "float":
        return params[0]
    if family == "decimal":
        return _decimal(*params)
    if family == "character":
        return "varchar"
    if family == "timestamp":
        return "timestamp" if params[0] is None else f"timestamp({params[0]})"
    return family


def common_supertype(data_types: list[str]) -> str:
    """The one type every one of ``data_types`` is read as: itself when they are all one type,
    their common supertype where one exists, else varchar."""
    distinct = list(dict.fromkeys(" ".join(t.lower().split()) for t in data_types))
    if len(distinct) == 1:
        return distinct[0]
    kinds = [_kind(t) for t in distinct]
    if any(k is None for k in kinds):
        return "varchar"
    widest = kinds[0]
    for kind in kinds[1:]:
        widest = _widen(widest, kind)  # type: ignore[arg-type]
        if widest is None:
            return "varchar"
    return _spelled(widest)  # type: ignore[arg-type]
