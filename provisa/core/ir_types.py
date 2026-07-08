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

# Per-platform native→IR translators (REQ-846). Each platform has type spellings the generic
# aliases don't cover — SQL Server bit/nvarchar/uniqueidentifier, Trino varbinary/row, DuckDB
# hugeint/blob. These overlays take precedence over the generic aliases; common types (integer,
# varchar, boolean, …) fall through to _ALIASES. Keyed by the SQLAlchemy/engine dialect name.
_PLATFORM_ALIASES: dict[str, dict[str, str]] = {
    "trino": {
        "tinyint": "smallint",
        "real": "float",
        "varbinary": "bytea",
        "json": "text",
        "timestamp with time zone": "timestamp",
        "time with time zone": "time",
        "ipaddress": "text",
        "uuid": "uuid",
        "row": "text",
        "array": "text",
        "map": "text",
    },
    "sqlserver": {
        "bit": "boolean",
        "tinyint": "smallint",
        "int": "integer",
        "money": "numeric",
        "smallmoney": "numeric",
        "datetime": "timestamp",
        "datetime2": "timestamp",
        "smalldatetime": "timestamp",
        "datetimeoffset": "timestamp",
        "nvarchar": "text",
        "nchar": "text",
        "ntext": "text",
        "binary": "bytea",
        "varbinary": "bytea",
        "image": "bytea",
        "uniqueidentifier": "uuid",
        "xml": "text",
    },
    "duckdb": {
        "tinyint": "smallint",
        "utinyint": "smallint",
        "usmallint": "integer",
        "uinteger": "bigint",
        "ubigint": "bigint",
        "hugeint": "bigint",
        "uhugeint": "bigint",
        "real": "float",
        "blob": "bytea",
        "timestamptz": "timestamp",
        "timestamp with time zone": "timestamp",
        "json": "text",
        "list": "text",
        "struct": "text",
        "map": "text",
    },
}

# Optional value transforms (REQ-846). A translator is primarily a TYPE lookup (native→IR name). A
# few native types also need the VALUE modified on landing — a transform is a SQL EXPRESSION, not a
# Python callable, so it applies SET-BASED in the land/generation query (pushed to the engine/store);
# a per-cell Python function would pull every value through Python and never scale. Each entry is a
# SQL-expression template with a ``{col}`` placeholder for the column reference (e.g. bit→boolean is
# ``"{col} <> 0"``). SPARSE: most native types need none (the driver already yields a store-
# compatible value). Absent → identity (project the column as-is).
_PLATFORM_TRANSFORMS: dict[tuple[str, str], str] = {
    # Most native types are passthrough; the handful that need the VALUE reshaped are complex types
    # — chiefly date/time. Each expression is a SQL projection over the source, so it runs set-based.
    ("sqlserver", "bit"): "{col} <> 0",  # bit 0/1 → boolean
    (
        "trino",
        "timestamp with time zone",
    ): "CAST({col} AT TIME ZONE 'UTC' AS timestamp)",  # tz→UTC naive
    ("duckdb", "timestamptz"): "{col} AT TIME ZONE 'UTC'",  # tz-aware → UTC timestamp
    # A column whose STRING content is a date (e.g. a varchar in "MM/DD/YYYY") is NOT a type-level
    # transform — that is a per-column declared expression (the enrich path), not a platform native
    # type. Register only TYPE-level reshapes here.
}

IR_TYPES: frozenset[str] = frozenset(_IR_TO_SA)


def value_transform(native_type: str, platform: str | None = None) -> str | None:
    """The optional SQL-EXPRESSION transform for a (platform, native type), or None (identity).

    Returns a SQL-expression template with a ``{col}`` placeholder — the land/generation path
    substitutes the column reference and injects it into the projection SQL, so the transform runs
    SET-BASED in the engine/store (never per-cell in Python — that would not scale). The type NAME
    mapping is the lookup (``to_ir``); this is the VALUE side, and it stays SQL for the same reason
    the whole write face does."""
    if not platform:
        return None
    base = native_type.split("(", 1)[0].strip().lower()
    return _PLATFORM_TRANSFORMS.get((platform.split("+", 1)[0], base))


def to_ir(native_type: str, platform: str | None = None) -> str:
    """Translate a native/source SQL type string to a canonical IR name (REQ-846).

    ``platform`` (a dialect name — trino, sqlserver, duckdb, postgresql, mysql, …) selects a
    per-platform translator whose spellings take precedence; without it, only the generic aliases
    apply. Strips a length/precision qualifier (``varchar(255)`` → ``varchar``) and lowercases.
    Raises on an unknown type — never a silent ``varchar`` default: a landed column's type must be
    known at design time, and an unmapped type is a vocabulary gap to close, not to paper over."""
    base = native_type.split("(", 1)[0].strip().lower()
    canon: str | None = None
    if platform:
        canon = _PLATFORM_ALIASES.get(platform.split("+", 1)[0], {}).get(base)
    if canon is None:
        canon = _ALIASES.get(base, base)
    if canon not in _IR_TO_SA:
        raise ValueError(
            f"unknown SQL type {native_type!r}"
            f"{f' for platform {platform!r}' if platform else ''}: not in the IR vocabulary "
            f"{sorted(IR_TYPES)} (add it to ir_types if a source legitimately produces it)"
        )
    return canon


def is_ir_type(name: str, platform: str | None = None) -> bool:
    """Whether ``name`` is a resolvable IR type (canonical, a generic alias, or platform-specific)."""
    try:
        to_ir(name, platform)
        return True
    except ValueError:
        return False


def to_sqlalchemy(type_name: str) -> Any:
    """The SQLAlchemy generic type for an IR name (or a native spelling, normalized via ``to_ir``).
    This is the write face's IR → SQLAlchemy mapping; it renders per-dialect at DDL time."""
    return _IR_TO_SA[to_ir(type_name)]


def to_physical(type_name: str, dialect_name: str) -> str:
    """Render an IR (or native) type as a platform's PHYSICAL column type — the IR → native
    equivalence (REQ-846). ``dialect_name`` is a SQLAlchemy dialect (postgresql, mysql, sqlite, …);
    the equivalence comes free from SQLAlchemy's own dialect compiler, so there is no hand-maintained
    per-platform table to drift. e.g. ``to_physical("text","mysql")`` → ``TEXT``; ``to_physical(
    "timestamp","mysql")`` → ``DATETIME``. This is the engine edge of the type hub: sources map
    native→IR (``to_ir``); engines map IR→their physical type (here); the store maps IR→SQLAlchemy."""
    from sqlalchemy.dialects import registry

    dialect = registry.load(dialect_name)()
    return to_sqlalchemy(type_name)().compile(dialect=dialect)
