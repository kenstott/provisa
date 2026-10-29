# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7f9a41-5b3e-4e0d-9f6a-8d1b4c0e7a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The shape of a source's native type, read from its own spelling (REQ-1965).

A source or engine reports a column's type as text: ``VARCHAR[]`` (DuckDB), ``text[]`` and
``_int4`` (PostgreSQL), ``array(row(a integer))`` (Trino), ``Array(Nullable(String))``
(ClickHouse), ``ARRAY<STRUCT<a INT64>>`` (BigQuery), ``map<string,int>`` (Databricks, Hive),
``VARIANT`` (Snowflake). ``parse_native_type`` turns any of them into one structure -- a scalar,
an array of something, a struct, a map, or a value whose shape the type does not state -- and
nothing else: what a shape MEANS for the model (its ``data_type``, its array depth, its element
type) is decided in one place, ``provisa.core.ir_types.column_typing``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, TypeGuard

Kind = Literal["scalar", "array", "struct", "map", "untyped"]

# Type constructors, by what they build. ``nested`` is ClickHouse's array-of-struct column.
_ARRAY_CONSTRUCTORS = frozenset({"array", "list"})
_STRUCT_CONSTRUCTORS = frozenset({"struct", "row", "tuple", "nested"})
_MAP_CONSTRUCTORS = frozenset({"map"})
# Types whose value is nested but whose shape the type does not state: Snowflake's
# semi-structured types and a bare array/struct/map marker with no element types.
_UNTYPED = frozenset({"variant", "object"})
# ClickHouse wraps a scalar without changing what it is.
_TRANSPARENT_WRAPPERS = frozenset({"nullable", "lowcardinality"})

_HEAD = re.compile(r"([a-z_][a-z0-9_]*)\s*([(<])?")
_SUFFIX = re.compile(r"\s*\[\s*\d*\s*\]\s*\Z")
_CLOSER = {"(": ")", "<": ">"}


@dataclass(frozen=True)
class NativeType:
    """One node of a native type. ``spelling`` is this node as the source wrote it, lower-cased.

    ``element`` is an array's element or a map's value; ``key`` a map's key; ``fields`` a struct's
    members as written (a struct is plain JSON to the model, so its members are not parsed)."""

    kind: Kind
    spelling: str
    element: "NativeType | None" = None
    key: "NativeType | None" = None
    fields: tuple[str, ...] = ()

    @property
    def nested(self) -> bool:
        """Whether a value of this type is an array, a struct or a map."""
        return self.kind != "scalar"


def _split_arguments(body: str) -> list[str]:
    """``body`` split at its top-level commas: ``a integer, b array(row(x int, y int))`` -> two."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(body):
        if ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(body[start:i].strip())
            start = i + 1
    tail = body[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _arguments(spelling: str, opener_at: int) -> list[str] | None:
    """The arguments of the constructor whose opening bracket is at ``opener_at``, when its
    closing bracket ends the spelling; None when it does not (``timestamp(3) with time zone``)."""
    closer = _CLOSER[spelling[opener_at]]
    depth = 0
    for i in range(opener_at, len(spelling)):
        ch = spelling[i]
        if ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
            if depth == 0:
                if ch != closer or spelling[i + 1 :].strip():
                    return None
                return _split_arguments(spelling[opener_at + 1 : i])
    return None


def parse_native_type(native_type: str) -> NativeType:
    """The structure of a native type spelling. Total: a spelling that names no array, struct or
    map is a scalar, whatever it is -- resolving a scalar's name is the IR table's job."""
    spelling = native_type.strip().lower()

    # ``T[]`` / ``T[3]``: DuckDB lists and fixed-size arrays, PostgreSQL's format_type output.
    suffix = _SUFFIX.search(spelling)
    if suffix is not None:
        return NativeType("array", spelling, element=parse_native_type(spelling[: suffix.start()]))
    # ``_int4``: PostgreSQL's catalog name for the array type of ``int4``.
    if spelling.startswith("_") and len(spelling) > 1:
        return NativeType("array", spelling, element=parse_native_type(spelling[1:]))

    head = _HEAD.match(spelling)
    if head is None:
        return NativeType("scalar", spelling)
    name = head[1]
    args = _arguments(spelling, head.end() - 1) if head[2] else None
    bare = head.end() == len(spelling) and head[2] is None

    if name in _TRANSPARENT_WRAPPERS and args is not None and len(args) == 1:
        return parse_native_type(args[0])
    if name in _UNTYPED:
        if bare:
            return NativeType("untyped", spelling)
        if args is not None:  # Snowflake's structured OBJECT(a T, b U)
            return NativeType("struct", spelling, fields=tuple(args))
    if name in _ARRAY_CONSTRUCTORS:
        if args is not None and len(args) == 1:
            return NativeType("array", spelling, element=parse_native_type(args[0]))
        if bare:  # information_schema's ARRAY marker, Snowflake's ARRAY of VARIANT
            return NativeType("array", spelling, element=NativeType("untyped", ""))
    if name in _MAP_CONSTRUCTORS:
        if args is not None and len(args) == 2:
            return NativeType(
                "map", spelling, element=parse_native_type(args[1]), key=parse_native_type(args[0])
            )
        if bare:
            return NativeType("map", spelling)
    if name in _STRUCT_CONSTRUCTORS and (args is not None or bare):
        fields = tuple(args or ())
        if name == "nested":  # ClickHouse Nested(a T, b U): an array of that struct
            return NativeType(
                "array", spelling, element=NativeType("struct", spelling, fields=fields)
            )
        return NativeType("struct", spelling, fields=fields)
    return NativeType("scalar", spelling)


def natively_nested(native_type: str | None) -> TypeGuard[str]:
    """Whether a column whose recorded native type is ``native_type`` is natively an array, a
    struct or a map (REQ-1965). The same hidden attribute keeps a character column's native type
    (REQ-1961, VARCHAR AND CHAR KEEP THEIR TYPE), which is none of them; a column with none
    recorded is none of them either."""
    return bool(native_type) and parse_native_type(native_type).nested
