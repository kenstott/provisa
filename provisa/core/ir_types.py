# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The canonical IR type vocabulary (REQ-846/932) — the ONE engine-independent type registry.

SQLAlchemy generic types are the intermediate representation, so the type system is a hub, not a
spoke of any fed engine: **sources map native → IR** (at registration), **fed engines map IR →
their physical dialect** (at query/DDL time), and the **write face maps IR → SQLAlchemy** (store
DDL, free). Swap the fed engine and only its IR→physical map changes; sources are untouched.

``data_type`` on a column is an IR name. For a LANDED table it is authoritative (it IS the store
DDL); for a LIVE source it is informational (the engine's live resolution wins). Either way it is
resolved at registration — never lazily backfilled, and never silently defaulted to ``varchar``.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    Text,
    Time,
    Uuid,
)

# Canonical IR name → SQLAlchemy generic type. The KEYS are the IR vocabulary. Generic types render
# per-dialect at DDL time, so a landed table is portable across every relational store backend.
_IR_TO_SA: dict[str, Any] = {
    "smallint": Integer,
    "integer": Integer,
    "bigint": BigInteger,
    "text": Text,
    "boolean": Boolean,
    "float": Float,
    "double": Float,
    "numeric": Numeric,
    "date": Date,
    "timestamp": DateTime,
    "time": Time,
    "uuid": Uuid,
    "bytea": LargeBinary,
}

# Native / dialect spelling → canonical IR name. Everything a source or reflection reports maps
# through here to ONE canonical IR name (e.g. ``varchar``/``character varying`` → ``text``).
_ALIASES: dict[str, str] = {
    "int": "integer",
    "int4": "integer",
    "int2": "smallint",
    "int8": "bigint",
    "varchar": "text",
    "character varying": "text",
    "char": "text",
    "character": "text",
    "string": "text",
    "json": "text",  # reflection collapses json/array to text (matches the engine surface)
    "jsonb": "text",
    "bool": "boolean",
    "real": "float",
    "float4": "float",
    "float8": "double",
    "double precision": "double",
    "decimal": "numeric",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp",
    "timestamptz": "timestamp",
    "datetime": "timestamp",
    "blob": "bytea",
}

IR_TYPES: frozenset[str] = frozenset(_IR_TO_SA)


def to_ir(native_type: str) -> str:
    """Normalize a native/source SQL type string to a canonical IR name (REQ-846).

    Strips a length/precision qualifier (``varchar(255)`` → ``varchar``), lowercases, and maps
    dialect aliases to the one canonical name. Raises on an unknown type — never a silent
    ``varchar`` default: a landed column's type must be known at design time, and an unmapped type
    is a gap to close in the vocabulary, not to paper over."""
    base = native_type.split("(", 1)[0].strip().lower()
    canon = _ALIASES.get(base, base)
    if canon not in _IR_TO_SA:
        raise ValueError(
            f"unknown SQL type {native_type!r}: not in the IR vocabulary {sorted(IR_TYPES)} "
            f"(add it to ir_types if a source legitimately produces it)"
        )
    return canon


def is_ir_type(name: str) -> bool:
    """Whether ``name`` is a resolvable IR type (canonical or a known alias)."""
    base = name.split("(", 1)[0].strip().lower()
    return _ALIASES.get(base, base) in _IR_TO_SA


def to_sqlalchemy(type_name: str) -> Any:
    """The SQLAlchemy generic type for an IR name (or a native spelling, normalized via ``to_ir``).
    This is the write face's IR → SQLAlchemy mapping; it renders per-dialect at DDL time."""
    return _IR_TO_SA[to_ir(type_name)]
