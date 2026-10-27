# Copyright (c) 2026 Kenneth Stott
# Canary: 66774352-ff8d-4de8-abaa-84e99bf7b5f4
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

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dateutil.relativedelta import relativedelta

from provisa.core.native_types import NativeType, parse_native_type

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Double,
    Float,
    Integer,
    Interval,
    LargeBinary,
    Numeric,
    Text,
    Time,
    Uuid,
)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import UserDefinedType


class Embedding(UserDefinedType):  # REQ-424
    """An embedding vector in a replica, stored as a list of floats where the engine that reads
    the replica can index it: ``REAL[]`` in a PostgreSQL store (indexed through its pgvector
    cast), ``FLOAT[]`` in a DuckDB store. Any other store holds it as text, the vector's JSON
    form."""

    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        del kw
        return "TEXT"


@compiles(Embedding, "postgresql")
def _embedding_postgresql(type_: Embedding, compiler: Any, **kw: Any) -> str:
    del type_, compiler, kw
    return "REAL[]"


@compiles(Embedding, "duckdb")
def _embedding_duckdb(type_: Embedding, compiler: Any, **kw: Any) -> str:
    del type_, compiler, kw
    return "FLOAT[]"


# Canonical IR name → SQLAlchemy generic type. The KEYS are the IR vocabulary. Generic types render
# per-dialect at DDL time, so a landed table is portable across every relational store backend.
_IR_TO_SA: dict[str, Any] = {
    "smallint": Integer,
    "integer": Integer,
    "bigint": BigInteger,
    "text": Text,
    "boolean": Boolean,
    "float": Float,
    "double": Double,  # DOUBLE PRECISION — distinct from single-precision Float (no silent precision loss)
    "numeric": Numeric,
    "date": Date,
    "timestamp": DateTime,
    # An instant (REQ-2050): a zoned source's timestamp, carried as UTC with its zone marked --
    # timestamptz on PostgreSQL, TIMESTAMP WITH TIME ZONE on DuckDB, the store's zoned type
    # elsewhere. ``timestamp`` is a local date and time with no zone.
    "timestamptz": DateTime(timezone=True),
    "time": Time,
    # A duration. SQLAlchemy's generic Interval is native INTERVAL on Postgres/DuckDB and its own
    # epoch-offset emulation elsewhere; its canonical TEXT form is ISO 8601 (iso8601_duration).
    "interval": Interval,
    "uuid": Uuid,
    "bytea": LargeBinary,
    # REQ-980: first-class JSON. SQLAlchemy's generic JSON renders as native JSON on
    # Postgres/MySQL/DuckDB and TEXT-affinity on SQLite — portable via the dialect compiler, no
    # hand-maintained per-store fallback. A native array, struct or map is a JSON column too
    # (REQ-1965, ``column_typing``).
    # REQ-1965: on PostgreSQL the replica lands the indexable JSON type, jsonb.
    "json": JSON().with_variant(JSONB(), "postgresql"),
    # REQ-424: an embedding column of a replica the engine indexes (see Embedding).
    "vector": Embedding,
}


@dataclass(frozen=True)
class Subtype:  # REQ-2171
    """An IR type that is a subtype of another: ``base`` is the IR type it is stored and computed
    as, ``pg_oid`` the PostgreSQL type pgwire and its catalog report it as. A type mapping with no
    entry for a subtype maps it as its base.

    Native semantics (amended 2026-10-27): ``semantics`` names what a sort, filter, join or
    operator on the subtype means beyond its base (None: its base's semantics are its own).
    ``native`` is, per SQL dialect, the form that gives a value of it the native type there: a
    type name to cast to, or a call (``ST_GeomFromText()``, innermost last). A source dialect in
    it is one an operation pushes down to. ``emulated`` is the dialects with a portable emulation
    (``"*"``: every dialect)."""

    base: str
    pg_oid: int
    semantics: str | None = None
    native: Mapping[str, str] = field(default_factory=dict)
    emulated: frozenset[str] = frozenset()
    #: Per reader -- a SQL dialect a landing reads the column in, or an adapter source's type --
    #: the expression over ``{col}`` that gives its value in the subtype's own form (its
    #: PostgreSQL text form on a text base). A reader with none cannot land the column.
    landing: Mapping[str, str] = field(default_factory=dict)


# REQ-2171: the subtypes, each declared once: one per distinct PostgreSQL type the IR's base
# types would hide. A subtype's name is its PostgreSQL type name; its values are held in
# PostgreSQL's own text form on a text base.
_RANGE = frozenset({"duckdb", "trino"})  # the dialects a range's bounds are emulated on
_EVERY_DIALECT = frozenset({"*"})


def _range(name: str, oid: int) -> Subtype:
    return Subtype("text", oid, "range", {"postgres": name.upper()}, _RANGE)


def _shape(name: str, oid: int) -> Subtype:
    return Subtype("text", oid, "shape", {"postgres": name.upper()})


SUBTYPES: dict[str, Subtype] = {
    # DuckDB's inet extension holds an address with its mask, as PostgreSQL does. Trino's
    # IPADDRESS holds no mask and its IPPREFIX no host bits, so an inet is emulated there.
    "inet": Subtype(
        "text", 869, "address", {"postgres": "INET", "duckdb": "INET"}, frozenset({"trino"})
    ),
    "cidr": Subtype(
        "text", 650, "address", {"postgres": "CIDR", "duckdb": "INET", "trino": "IPPREFIX"}
    ),
    "macaddr": Subtype("text", 829, "mac", {"postgres": "MACADDR"}, _EVERY_DIALECT),
    "macaddr8": Subtype("text", 774, "mac", {"postgres": "MACADDR8"}, _EVERY_DIALECT),
    "xml": Subtype("text", 142),
    "oid": Subtype("bigint", 26),
    # Sized so every source's money is held exactly: PostgreSQL's (int64 cents, 17 integer
    # digits) and SQL Server's (int64 ten-thousandths, 4 decimals).
    "money": Subtype("numeric(21,4)", 790),
    # PostgreSQL's built-in shapes.
    "point": _shape("point", 600),
    "line": _shape("line", 628),
    "lseg": _shape("lseg", 601),
    "box": _shape("box", 603),
    "path": _shape("path", 602),
    "polygon": _shape("polygon", 604),
    "circle": _shape("circle", 718),
    # PostGIS's planar and geodetic types, in WKT. PostGIS has no fixed type OID (the extension
    # assigns one at install), so these are Provisa's own, in the private range pgwire's PostGIS
    # surface declares them in.
    "geometry": Subtype(
        "text",
        8310,
        "spatial",
        {
            "postgres": "GEOMETRY",
            "duckdb": "ST_GeomFromText()",
            "trino": "ST_GeometryFromText()",
            "mysql": "ST_GeomFromText()",
        },
    ),
    "geography": Subtype(
        "text",
        8311,
        "spatial",
        {
            "postgres": "GEOGRAPHY",
            "trino": "to_spherical_geography(ST_GeometryFromText())",
        },
    ),
    # The range types, in PostgreSQL's range text form ("[1,10)").
    "int4range": _range("int4range", 3904),
    "int8range": _range("int8range", 3926),
    "numrange": _range("numrange", 3906),
    "tsrange": _range("tsrange", 3908),
    "tstzrange": _range("tstzrange", 3910),
    "daterange": _range("daterange", 3912),
    # Bit strings, in PostgreSQL's text form ("0101"): ordered as text orders them.
    "bit": Subtype("text", 1560),
    "varbit": Subtype("text", 1562),
    "tsvector": Subtype("text", 3614, "text_search", {"postgres": "TSVECTOR"}),
}

# REQ-2171: how a landing reads each subtype in its own form, beyond PostgreSQL (which casts any
# subtype to its base, a PostGIS one through ST_AsText). Never a form other than the subtype's own
# (no hex WKB for a geometry): a reader not named here cannot land the column.
_LANDING: dict[str, dict[str, str]] = {
    "inet": {
        "duckdb": "CAST({col} AS VARCHAR)",
        "trino": "CAST({col} AS VARCHAR)",
        "clickhouse": "toString({col})",
        # An adapter that hands the address over as its text.
        "elasticsearch": "{col}",
        "cassandra": "{col}",
    },
    "cidr": {"duckdb": "CAST({col} AS VARCHAR)", "trino": "CAST({col} AS VARCHAR)"},
    "geometry": {
        "duckdb": "ST_AsText({col})",
        "mysql": "ST_AsText({col})",
        "tsql": "{col}.STAsText()",
    },
    "geography": {"tsql": "{col}.STAsText()"},
    "xml": {
        "tsql": "CAST({col} AS NVARCHAR(MAX))",
        "oracle": "XMLSERIALIZE(CONTENT {col} AS CLOB)",
    },
    "money": {"tsql": "CAST({col} AS DECIMAL(21,4))"},
}


def _with_landing(name: str, sub: Subtype) -> Subtype:
    at_postgres = (
        "ST_AsText({col})"
        if name in ("geometry", "geography")
        else f"CAST({{col}} AS {sub.base.upper()})"
    )
    return replace(sub, landing={"postgres": at_postgres, **_LANDING.get(name, {})})


SUBTYPES = {name: _with_landing(name, sub) for name, sub in SUBTYPES.items()}


def landing_form(subtype: str, reader: str) -> str | None:
    """The expression over ``{col}`` that lands a column of ``subtype`` read by ``reader`` (a
    SQL dialect, or an adapter source's type) in the subtype's own form; None when it has none."""
    return SUBTYPES[subtype].landing.get(reader)


# REQ-2171: a spelling of a subtype that is not its name, on any platform.
_SUBTYPE_SPELLINGS: dict[str, str] = {"bit varying": "varbit"}

# MySQL's spatial types are PostGIS's planar geometry, whichever shape a column declares.
_MYSQL_SPATIAL = {
    name: "geometry"
    for name in (
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "geomcollection",
    )
}

# REQ-2171: a native spelling equivalent to a subtype, per source platform. A subtype's own name
# (PostgreSQL's, which CockroachDB, YugabyteDB, DuckDB and SQL Server share where they have the
# type) needs no entry.
_PLATFORM_SUBTYPES: dict[str, dict[str, str]] = {
    "trino": {"ipaddress": "inet"},
    "clickhouse": {"ipv4": "inet", "ipv6": "inet"},
    "sqlserver": {"money": "money", "smallmoney": "money", "xml": "xml"},
    "oracle": {"xmltype": "xml"},
    **{kind: _MYSQL_SPATIAL for kind in ("mysql", "mariadb", "tidb", "singlestore")},
}


def subtype_of(native_type: str, platform: str | None = None) -> str | None:
    """The subtype a native spelling registers as (REQ-2171), or None when it has none. A
    platform's own meaning of a spelling (SQL Server's ``bit`` is a boolean) is never a
    subtype's."""
    name = type_name(native_type)
    if platform:
        kind = platform.split("+", 1)[0]
        own = _PLATFORM_SUBTYPES.get(kind, {}).get(name)
        if own is not None:
            return own
        if name in _PLATFORM_ALIASES.get(kind, {}):
            return None
    return subtype_named(name)


def subtype_named(type_spelling: str) -> str | None:
    """The subtype ``type_spelling`` names, in any of its spellings (REQ-2171), or None."""
    name = type_name(type_spelling)
    name = _SUBTYPE_SPELLINGS.get(name, name)
    return name if name in SUBTYPES else None


def base_type(type_spelling: str) -> str:
    """A subtype's base type (REQ-2171); any other type is its own."""
    named = subtype_named(type_spelling)
    return SUBTYPES[named].base if named is not None else type_spelling


# Native / dialect spelling → canonical IR name. Everything a source or reflection reports maps
# through here to ONE canonical IR name (e.g. ``varchar``/``character varying`` → ``text``).
_ALIASES: dict[str, str] = {
    "int": "integer",
    "int4": "integer",
    "int2": "smallint",
    "int8": "bigint",
    "varchar": "text",
    "character varying": "text",
    # SAP HANA's (and SQL Server's) Unicode varchar, as a registered column of such a source
    # stores it and a landing reads it back with no platform to name (REQ-1730 swap lane).
    "nvarchar": "text",
    "char": "text",
    "character": "text",
    "string": "text",
    "json": "json",  # REQ-980: first-class JSON (jsonb narrows to portable generic JSON)
    "jsonb": "json",
    "super": "json",  # Redshift: holds any JSON value, so JSON at its source (REQ-1965)
    "bool": "boolean",
    "real": "float",
    "float4": "float",
    "float8": "double",
    "double precision": "double",
    "decimal": "numeric",
    # Oracle's and Snowflake's decimal, as a registered column of such a source stores it with
    # no size and a reader with no platform to name reads it back.
    "number": "numeric",
    "timestamp without time zone": "timestamp",
    # An instant (REQ-2050), however a source spells one.
    "timestamp with time zone": "timestamptz",
    "timestamp with local time zone": "timestamptz",
    "timestamptz": "timestamptz",
    "time without time zone": "time",
    "time with time zone": "time",
    "timetz": "time",  # postgres time with time zone alias
    "datetime": "timestamp",
    "blob": "bytea",
}

# Per-platform native→IR translators (REQ-846). Each platform has type spellings the generic
# aliases don't cover — SQL Server bit/nvarchar/uniqueidentifier, Trino varbinary, DuckDB
# hugeint/blob. These overlays take precedence over the generic aliases; common types (integer,
# varchar, boolean, …) fall through to _ALIASES. Keyed by the SQLAlchemy/engine dialect name.
_PLATFORM_ALIASES: dict[str, dict[str, str]] = {
    "trino": {
        "tinyint": "smallint",
        "real": "float",
        "varbinary": "bytea",
        "json": "json",  # REQ-980
        "timestamp with time zone": "timestamptz",
        "time with time zone": "time",
        "uuid": "uuid",
    },
    # REQ-1981: Amazon Athena's catalog spells a column's type as Hive DDL does; a composite
    # (array<...>, map<...>, struct<...>) is text, as Trino's composites are.
    "athena": {
        "tinyint": "smallint",
        "char": "text",
        "binary": "bytea",
        "array": "text",
        "map": "text",
        "struct": "text",
    },
    "sqlserver": {
        "bit": "boolean",
        "tinyint": "smallint",
        "int": "integer",
        "datetime": "timestamp",
        "datetime2": "timestamp",
        "smalldatetime": "timestamp",
        "datetimeoffset": "timestamptz",
        "nvarchar": "text",
        "nchar": "text",
        "ntext": "text",
        "binary": "bytea",
        "varbinary": "bytea",
        "image": "bytea",
        "uniqueidentifier": "uuid",
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
        "timestamptz": "timestamptz",
        "timestamp with time zone": "timestamptz",
        "json": "json",  # REQ-980
    },
    # The scalar names these platforms use for the element of an array (REQ-1965): the element
    # type of an array of a primitive is a semantic SQL type, resolved here like any other.
    "clickhouse": {
        "string": "text",
        "fixedstring": "text",
        "int8": "smallint",  # 8-bit here; the generic int8 alias is PostgreSQL's 8-BYTE integer
        "int16": "smallint",
        "int32": "integer",
        "int64": "bigint",
        "uint8": "smallint",
        "uint16": "integer",
        "uint32": "bigint",
        "uint64": "bigint",
        "float32": "float",
        "float64": "double",
        "bool": "boolean",
        "date32": "date",
        "datetime": "timestamp",
        "datetime64": "timestamp",
    },
    # REQ-2050: MySQL's TIMESTAMP is an instant (stored in UTC, read in the session's zone, which
    # the driver sets to UTC); its DATETIME is a civil date and time.
    # REQ-2171: a YEAR is a four-digit integer.
    **{
        kind: {"timestamp": "timestamptz", "year": "integer"}
        for kind in ("mysql", "mariadb", "tidb", "singlestore")
    },
    "bigquery": {
        "int64": "bigint",
        "float64": "double",
        "bytes": "bytea",
        "bignumeric": "numeric",
        # BigQuery's TIMESTAMP is an instant; its DATETIME is a civil date and time.
        "timestamp": "timestamptz",
    },
    "databricks": {
        "byte": "smallint",
        "tinyint": "smallint",
        "short": "smallint",
        "long": "bigint",
        "binary": "bytea",
        "timestamp_ntz": "timestamp",
    },
    "hiveserver2": {
        "tinyint": "smallint",
        "binary": "bytea",
    },
    # Oracle as ALL_TAB_COLUMNS prints it. DATE carries a time of day; LONG is long text (not a
    # 64-bit integer); FLOAT is binary precision up to 126 bits. A sized NUMBER is the source's
    # reported decimal (numeric_type), an unsized one the generic alias.
    "oracle": {
        "varchar2": "text",
        "nvarchar2": "text",
        "nchar": "text",
        "char": "text",
        "clob": "text",
        "nclob": "text",
        "long": "text",
        "rowid": "text",
        "urowid": "text",
        "blob": "bytea",
        "raw": "bytea",
        "long raw": "bytea",
        "bfile": "bytea",
        "date": "timestamp",
        "interval year to month": "interval",
        "interval day to second": "interval",
        "binary_float": "float",
        "binary_double": "double",
        "float": "double",
        "json": "json",
    },
    "snowflake": {
        "number": "numeric",
        "binary": "bytea",
        "timestamp_ntz": "timestamp",
        "timestamp_ltz": "timestamptz",
        "timestamp_tz": "timestamptz",
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
    # A column whose STRING content is a date (e.g. a varchar in "MM/DD/YYYY") is NOT a type-level
    # transform — that is a per-column declared expression (the enrich path), not a platform native
    # type. Register only TYPE-level reshapes here.
}

IR_TYPES: frozenset[str] = frozenset(_IR_TO_SA)
# A parenthesized size anywhere in a type's spelling.
_SIZES = re.compile(r"\s*\([^)]*\)")
# A character type's character set, printed after it: Exasol's ``VARCHAR(64) UTF8``,
# ``CHAR(1) ASCII``. The set is the source's storage; the type is the base type before it.
_CHARSET = re.compile(r"^((?:var)?char|character(?: varying)?) (?:utf8|ascii)$")
_NUMERIC_SIZE = re.compile(r"\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*\Z")

# REQ-1908: a registered temporal column whose source stores it as an epoch number declares the
# number's unit (Column.epoch_unit). Never inferred: seconds and milliseconds overlap for plausible
# values, so a guessed unit silently shifts every instant.
EPOCH_UNITS: dict[str, timedelta] = {
    "s": timedelta(seconds=1),
    "ms": timedelta(milliseconds=1),
    "us": timedelta(microseconds=1),
}
# The registered types an epoch number can represent: an instant, or its UTC calendar day.
EPOCH_TEMPORAL_TYPES = frozenset(
    {"timestamp", "timestamp without time zone", "timestamptz", "timestamp with time zone", "date"}
)


def bytea_hex(value: bytes | bytearray | memoryview) -> str:
    """The canonical text form of an IR ``bytea`` value: Postgres's own hex output (``\\x0001ff``,
    ``bytea_output = hex``). Every transport that renders binary as text (GraphQL's String-typed
    bytea columns, JSON results) uses it; binary transports (gRPC ``bytes``, pgwire, Arrow) carry
    the bytes themselves."""
    return "\\x" + bytes(value).hex()


def interval_parts(value: timedelta | relativedelta) -> tuple[int, int, int]:
    """An IR ``interval`` value as Postgres holds one: (months, days, microseconds). A duration with
    no months is a ``timedelta``; one with months (a year-month interval: Oracle's INTERVAL YEAR TO
    MONTH, Trino's and BigQuery's) is a ``relativedelta``, since a month has no fixed length."""
    if isinstance(value, timedelta):
        # A negative timedelta is held as negative days and positive seconds; its parts share a sign.
        if value < timedelta(0):
            _, days, micros = interval_parts(-value)
            return 0, -days, -micros
        return 0, value.days, value.seconds * 1_000_000 + value.microseconds
    seconds = (value.hours * 60 + value.minutes) * 60 + value.seconds
    return value.years * 12 + value.months, value.days, seconds * 1_000_000 + value.microseconds


def interval_value(months: int, days: int, microseconds: int) -> timedelta | relativedelta:
    """The IR ``interval`` value of (months, days, microseconds), as a reader hands one on: a
    ``timedelta`` when it has no months, else a ``relativedelta``."""
    if not months:
        return timedelta(days=days, microseconds=microseconds)
    return relativedelta(months=months, days=days, microseconds=microseconds)


class _InfiniteDate(date):
    """PostgreSQL's ``infinity`` or ``-infinity`` date: the nearest date Python holds, marked with
    its sign (``pg_infinity``) so the wire writes PostgreSQL's own value."""

    pg_infinity: int


class _InfiniteDatetime(datetime):
    """PostgreSQL's ``infinity`` or ``-infinity`` timestamp, as :class:`_InfiniteDate`."""

    pg_infinity: int


def infinite_date(sign: int) -> date:
    """The IR ``date`` value of PostgreSQL's ``infinity`` (``sign`` 1) or ``-infinity`` (-1)."""
    edge = date.max if sign > 0 else date.min
    value = _InfiniteDate(edge.year, edge.month, edge.day)
    value.pg_infinity = sign
    return value


def infinite_timestamp(sign: int, *, aware: bool) -> datetime:
    """The IR ``timestamp`` (``aware``: ``timestamptz``, in UTC) value of PostgreSQL's
    ``infinity`` (``sign`` 1) or ``-infinity`` (-1)."""
    edge = datetime.max if sign > 0 else datetime.min
    value = _InfiniteDatetime(
        *edge.timetuple()[:6], edge.microsecond, tzinfo=timezone.utc if aware else None
    )
    value.pg_infinity = sign
    return value


def infinity_of(value: object) -> int:
    """1 for an infinite date or timestamp, -1 for a negatively infinite one, else 0."""
    return getattr(value, "pg_infinity", 0)


def iso8601_duration(value: timedelta | relativedelta) -> str:
    """The canonical text form of an IR ``interval`` value: an ISO 8601 duration (``P1Y2M3DT4.5S``),
    the form Postgres itself emits under ``IntervalStyle = iso_8601``. Every transport that renders
    an interval as text (GraphQL's ``Interval`` scalar, JSON results, gRPC string fields) uses it,
    so a duration reads the same on every surface. A duration whose parts are none of them positive
    is ``-`` + its magnitude; one of mixed signs writes each part with its own sign, as Postgres
    does."""
    months, days, micros = interval_parts(value)
    if (months, days, micros) != (0, 0, 0) and max(months, days, micros) <= 0:
        return "-" + _iso8601(-months, -days, -micros)
    return _iso8601(months, days, micros)


def _iso8601(months: int, days: int, micros: int) -> str:
    years, months = int(months / 12), months - int(months / 12) * 12
    sign = "-" if micros < 0 else ""
    hours, rem = divmod(abs(micros), 3_600_000_000)
    minutes, rem = divmod(rem, 60_000_000)
    secs, frac = divmod(rem, 1_000_000)
    date = (
        (f"{years}Y" if years else "")
        + (f"{months}M" if months else "")
        + (f"{days}D" if days else "")
    )
    time = (f"{sign}{hours}H" if hours else "") + (f"{sign}{minutes}M" if minutes else "")
    if secs or frac:
        time += sign + f"{secs}.{frac:06d}".rstrip("0").rstrip(".") + "S"
    if not date and not time:
        return "PT0S"
    return "P" + date + ("T" + time if time else "")


def type_name(native_type: str) -> str:
    """A spelling's type name, its sizes and a character type's character set aside:
    ``varchar(64)`` is ``varchar``, ``timestamp(6) with time zone`` is ``timestamp with time
    zone`` (Oracle prints a size in the middle), ``varchar(64) utf8`` is ``varchar`` (Exasol
    prints the set after)."""
    name = " ".join(_SIZES.sub(" ", native_type.strip().lower()).split())
    return _CHARSET.sub(r"\1", name)


def value_transform(native_type: str, platform: str | None = None) -> str | None:
    """The optional SQL-EXPRESSION transform for a (platform, native type), or None (identity).

    Returns a SQL-expression template with a ``{col}`` placeholder — the land/generation path
    substitutes the column reference and injects it into the projection SQL, so the transform runs
    SET-BASED in the engine/store (never per-cell in Python — that would not scale). The type NAME
    mapping is the lookup (``to_ir``); this is the VALUE side, and it stays SQL for the same reason
    the whole write face does."""
    if not platform:
        return None
    return _PLATFORM_TRANSFORMS.get((platform.split("+", 1)[0], type_name(native_type)))


def to_ir(native_type: str, platform: str | None = None) -> str:
    """Translate a native/source SQL type string to a canonical IR name (REQ-846).

    ``platform`` (a dialect name — trino, sqlserver, duckdb, postgresql, mysql, …) selects a
    per-platform translator whose spellings take precedence; without it, only the generic aliases
    apply. Strips a length/precision qualifier (``varchar(255)`` → ``varchar``) and lowercases.
    A native array, struct or map, in any spelling, is ``json`` (REQ-1965). REQ-2171: every type
    maps onto the vocabulary; one that names no type of it is ``text``, its values held in the
    source's own text form."""
    named = _named_ir(native_type, platform)
    return named if named is not None else "text"


def _named_ir(native_type: str, platform: str | None) -> str | None:
    """The IR type ``native_type`` names, or None when it names none of the vocabulary."""
    if parse_native_type(native_type).nested:
        return "json"
    subtype = subtype_of(native_type, platform)
    if subtype is not None:
        return to_ir(SUBTYPES[subtype].base)  # REQ-2171: a subtype is its base to the IR
    base = type_name(native_type)
    canon: str | None = None
    if platform:
        canon = _PLATFORM_ALIASES.get(platform.split("+", 1)[0], {}).get(base)
    if canon is None:
        canon = _ALIASES.get(base, base)
    if canon in _IR_TO_SA:
        return canon
    # A platform alias may name a sized type (Cassandra's varint is numeric(38,0)).
    return type_name(canon) if type_name(canon) in _IR_TO_SA else None


# The source kinds whose array type records no dimensions (PostgreSQL: "declaring the array size
# or number of dimensions in CREATE TABLE is simply documentation"), or that cannot nest arrays.
# An array column of one of them has depth 1 however its type is spelled (REQ-1965).
_ONE_LEVEL_ARRAY_PLATFORMS = frozenset(
    {"postgresql", "postgres", "greenplum", "yugabytedb", "opengauss", "cockroachdb", "bigquery"}
)


@dataclass(frozen=True)
class ColumnTyping:
    """What registration records for a column from its source's native type (REQ-1965).

    ``data_type`` is the type the model shows. The other three are the hidden attributes of a
    natively nested column: ``array_depth`` (0 unless it is an array of one primitive type),
    ``element_type`` (the semantic SQL type of its innermost element, with precision and scale)
    and ``native_type`` (the source's own spelling). A column that is not natively an array, a
    struct or a map carries none of the three.

    ``native_platform`` says whose spelling ``native_type`` is: the kind of the source, or of the
    engine when the engine listed the column. A value is cast INTO the native type only by a
    statement that runs on that platform; ``array(varchar)`` means nothing to a PostgreSQL
    source."""

    data_type: str
    array_depth: int = 0
    element_type: str | None = None
    native_type: str | None = None
    native_platform: str | None = None


def _semantic_type(native_scalar: str, platform: str | None) -> str:
    """The semantic SQL type of a native scalar, keeping a decimal's precision and scale in the
    REQ-1961 spelling (``numeric(18,2)``)."""
    ir = to_ir(native_scalar, platform)
    if ir == "numeric":
        sized = _NUMERIC_SIZE.search(native_scalar)
        if sized is not None:
            return f"numeric({int(sized[1])},{int(sized[2])})"
    return ir


def _array_of_a_primitive(shape: NativeType, platform: str | None) -> tuple[int, str] | None:
    """``(depth, element type)`` when ``shape`` is an array, at any nesting, whose innermost
    element is one primitive type; None for any other shape."""
    depth = 0
    node = shape
    while node.kind == "array" and node.element is not None:
        depth += 1
        node = node.element
    if depth == 0 or node.kind != "scalar":
        return None
    if not is_ir_type(node.spelling, platform):
        # REQ-1965: registering a natively nested column is never an error, and only an array
        # whose innermost element is a semantic SQL type has a depth above 0. An element with no
        # semantic type (a ClickHouse Nothing) leaves the column plain JSON:
        # the whole value and the untyped operations work, nothing inside is typed.
        return None
    if platform and platform.split("+", 1)[0] in _ONE_LEVEL_ARRAY_PLATFORMS:
        depth = 1
    return depth, _semantic_type(node.spelling, platform)


def column_typing(native_type: str, platform: str | None = None) -> ColumnTyping:
    """The ONE decision of what a native type registers as (REQ-1965). ``platform`` is the kind
    of the source that reported the spelling.

    An array, struct or map is ``json``, with the source's spelling recorded and, for an array
    of one primitive type at any nesting, its depth and element type. Anything else keeps the
    spelling it was listed with and carries no hidden attribute -- except a spelling only its
    platform has (Databricks' ``long``), which is no type anything after registration can read
    without knowing the platform: it registers as its IR name. A sized spelling keeps its size."""
    spelling = native_type.strip().lower()
    shape = parse_native_type(spelling)
    if not shape.nested:
        subtype = subtype_of(spelling, platform)
        if subtype is not None:
            return ColumnTyping(subtype)  # REQ-2171: registration records the subtype
        own = _PLATFORM_ALIASES.get(platform.split("+", 1)[0], {}) if platform else {}
        # By the spelling's name, its size aside (Oracle's varchar2(64), interval day(2) to
        # second(6)) -- but a decimal keeps its size, which is its type (REQ-1961).
        from provisa.core.numeric_type import DECIMAL_NAMES

        if spelling.split("(", 1)[0].strip() in DECIMAL_NAMES:
            return ColumnTyping(spelling)
        if not is_ir_type(spelling, platform):
            return ColumnTyping("text")  # REQ-2171: a type naming none of the IR's is text
        return ColumnTyping(own.get(spelling) or own.get(type_name(spelling)) or spelling)
    read_from = platform.split("+", 1)[0] if platform else None
    typed = _array_of_a_primitive(shape, platform)
    if typed is None:
        return ColumnTyping("json", native_type=spelling, native_platform=read_from)
    return ColumnTyping(
        "json",
        array_depth=typed[0],
        element_type=typed[1],
        native_type=spelling,
        native_platform=read_from,
    )


def composite_typing(native_type: str, platform: str) -> ColumnTyping:
    """What a column of a named composite type registers as (REQ-1965): a struct, so plain JSON
    with its native type recorded. A composite's name does not say it is one -- only its
    catalog does -- so its reader states it here rather than through a spelling."""
    return ColumnTyping(
        "json", native_type=native_type.strip().lower(), native_platform=platform.split("+", 1)[0]
    )


def is_ir_type(name: str, platform: str | None = None) -> bool:
    """Whether ``name`` names a type of the IR vocabulary (canonical, a generic alias, a
    platform's own spelling or a subtype), rather than being text because it names none."""
    return _named_ir(name, platform) is not None


def indexed_string_length() -> int:
    """The bounded length a ``text``-mapped column gets when it is a PRIMARY KEY (or otherwise
    indexed) column, config-driven via ``$PROVISA_INDEXED_STRING_LENGTH`` (default 255). An
    unbounded type cannot be a key/index column on several dialects — verified live (REQ-1730
    engine-swap harness, 2026-09-20): SQL Server refused a ``VARCHAR(max)`` primary key with
    ``Column ... is of a type that is invalid for use as a key column in an index``. 255 is well
    under every mainstream RDBMS's own index-key byte limit (SQL Server: 900 bytes; MySQL innodb:
    767/3072 bytes depending on row format) even at 4 bytes/char utf8mb4, so it is a safe portable
    default without being dialect-specific — read once here, not re-guessed per call site."""
    import os

    return int(os.environ.get("PROVISA_INDEXED_STRING_LENGTH", "255"))


def _session_only_sqlalchemy(type_name: str) -> Any:
    """REQ-2176: the column types only a session's ephemeral table holds, which no source's type
    names (a source's time with a zone is ``time``, its array ``json``): ``zoned_time``, a time
    of day with its offset, and ``list<elem>``, a list of one element type. None for any other."""
    if type_name == "zoned_time":
        return Time(timezone=True)
    if type_name.startswith("list<") and type_name.endswith(">"):
        from sqlalchemy import ARRAY

        return ARRAY(to_sqlalchemy(type_name[5:-1]))
    return None


def to_sqlalchemy(type_name: str, *, indexed: bool = False) -> Any:
    """The SQLAlchemy generic type for an IR name (or a native spelling, normalized via ``to_ir``).
    This is the write face's IR → SQLAlchemy mapping; it renders per-dialect at DDL time.

    ``indexed=True`` (a PRIMARY KEY or otherwise indexed column, REQ-1730): an IR ``text`` column
    gets a BOUNDED ``String(indexed_string_length())`` instead of unbounded ``Text`` — the single
    place this substitution happens, so every engine/dialect that lands a keyed column through
    ``materialize_exec.build_table`` gets it uniformly, not just the one dialect that happened to
    surface the gap. Every other IR type is already bounded/fixed-width and needs no substitution."""
    session_only = _session_only_sqlalchemy(type_name)
    if session_only is not None:
        return session_only
    type_name = base_type(type_name)  # REQ-2171: a subtype is stored as its base
    sa_type = _IR_TO_SA[to_ir(type_name)]
    if sa_type is Numeric:
        # A decimal named with its precision and scale lands with them: an unqualified NUMERIC is
        # the store's own default, which on some stores holds fewer digits than the value.
        from provisa.core.numeric_type import numeric_size

        size = numeric_size(type_name)
        if size is not None:
            return Numeric(*size)
    if indexed and sa_type is Text:
        from sqlalchemy import String

        return String(indexed_string_length())
    return sa_type


def to_physical(type_name: str, dialect_name: str) -> str:
    """Render an IR (or native) type as a platform's PHYSICAL column type — the IR → native
    equivalence (REQ-846). ``dialect_name`` is a SQLAlchemy dialect (postgresql, mysql, sqlite, …);
    the equivalence comes free from SQLAlchemy's own dialect compiler, so there is no hand-maintained
    per-platform table to drift. e.g. ``to_physical("text","mysql")`` → ``TEXT``; ``to_physical(
    "timestamp","mysql")`` → ``DATETIME``. This is the engine edge of the type hub: sources map
    native→IR (``to_ir``); engines map IR→their physical type (here); the store maps IR→SQLAlchemy."""
    from sqlalchemy.dialects import registry

    dialect = registry.load(dialect_name)()
    generic = to_sqlalchemy(type_name)
    # A sized decimal is already an instance; every other is a generic type to instantiate.
    return (generic() if isinstance(generic, type) else generic).compile(dialect=dialect)
