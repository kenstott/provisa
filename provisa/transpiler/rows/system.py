# Copyright (c) 2026 Kenneth Stott
# Canary: 2511aec9-add8-41ea-817c-4c502732c6ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of system, catalog and server values (REQ-2167, REQ-2171): ``ROWS``,
which provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.core.pg_error import MARK, raising
from provisa.transpiler import pg_forms, pg_forms_system, pg_forms_types, pg_zones
from provisa.transpiler.metadata_functions import VALUE_FORMS
from provisa.transpiler.rows.base import (
    _EVERY,
    _TRINO,
    Macro,
    Pg,
    _comparisons,
    _macro,
    _pg,
    _udf,
    strict,
)

#: typeof without a list's brackets or a decimal's size.
_TYPE_BASE = "split_part(replace(typeof(value), '[]', ''), '(', 1)"
#: pg_typeof: DuckDB's name of the value's type as format_type writes PostgreSQL's, with each of a
#: list's dimensions. A type that holds another's value is named by the statement
#: (pg_functions._type_said).
_PG_TYPEOF = (
    f"CASE {_TYPE_BASE} WHEN 'VARCHAR' THEN 'text' WHEN 'DOUBLE' THEN 'double precision' "
    "WHEN 'FLOAT' THEN 'real' WHEN 'DECIMAL' THEN 'numeric' WHEN 'HUGEINT' THEN 'numeric' "
    "WHEN 'TINYINT' THEN 'smallint' WHEN 'TIMESTAMP' THEN 'timestamp without time zone' "
    "WHEN 'TIME' THEN 'time without time zone' WHEN 'BLOB' THEN 'bytea' "
    f"WHEN '\"NULL\"' THEN 'unknown' ELSE lower({_TYPE_BASE}) END || "
    "repeat('[]', (length(typeof(value)) - length(replace(typeof(value), '[]', ''))) // 2)"
)

#: oid_send: an oid's four bytes, for the send functions of the object identifier types.
_OID_BYTES = "unhex(printf('%08x', CAST(v AS BIGINT)))"
_INT8_BYTES = "unhex(printf('%016x', CAST(v AS UBIGINT)))"


_LSN_NUMBER = (
    "((CAST('0x' || split_part({l}, '/', 1) AS BIGINT) << 32) + "
    "CAST('0x' || split_part({l}, '/', 2) AS BIGINT))"
)
WAL_SEGMENT = 16777216
_SEGMENTS_PER_ID = 256


def lsn_number(value: str) -> str:
    """A pg_lsn's text as its 64-bit position."""
    return _LSN_NUMBER.format(l=value)


def wal_file(value: str) -> str:
    """The name of the WAL file holding a pg_lsn, on timeline 1."""
    segment = f"({lsn_number(value)} // {WAL_SEGMENT})"
    return (
        f"upper(printf('%08x%08x%08x', 1, {segment} // {_SEGMENTS_PER_ID}, "
        f"{segment} % {_SEGMENTS_PER_ID}))"
    )


#: Each encoding's longest character in bytes, by its number (common/wchar.c's pg_wchar_table).
_BUILT_IN_ACCESS_METHODS = (403, 405, 783, 2742, 3580, 4000)
#: What each built-in index access method declares of itself (its IndexAmRoutine).
_INDEXAM_PROPERTIES = {
    "can_order": (403,),
    "can_unique": (403,),
    "can_multi_col": (403, 783, 2742, 3580),
    "can_exclude": (403, 405, 783, 4000),
    "can_include": (403, 783, 4000),
}
_ENCODING_MAX_LENGTHS = (1, 3, 3, 3, 4, 3, 4, 4, *(1,) * 27, 2, 2, 2, 2, 4, 3, 2)

_XIP = (
    "list_transform(string_split(nullif(split_part(s, ':', 3), ''), ','), _x -> CAST(_x AS BIGINT))"
)
_XMIN = "CAST(split_part(s, ':', 1) AS BIGINT)"
_XMAX = "CAST(split_part(s, ':', 2) AS BIGINT)"


def _snapshot(family: str, visible: str) -> tuple[Macro, ...]:
    """A snapshot's value functions, read from its text form xmin:xmax:xip,..."""
    return (
        _macro(f"{family}_xmin", "s", strict("s", "BIGINT", _XMIN)),
        _macro(f"{family}_xmax", "s", strict("s", "BIGINT", _XMAX)),
        _macro(f"{family}_xip", "s", f"unnest({_XIP})"),
        _macro(
            f"{family}_send",
            "s",
            strict(
                "s",
                "BLOB",
                f"unhex(printf('%08x%016x%016x', coalesce(len({_XIP}), 0), {_XMIN}, {_XMAX}) || "
                f"coalesce(array_to_string(list_transform({_XIP}, _x -> printf('%016x', _x)), ''), "
                "''))",
            ),
        ),
        _macro(
            visible,
            "x, s",
            strict(
                "x, s",
                "BOOLEAN",
                # A txid is read unsigned, so a negative one is past any snapshot's xmax.
                f"CASE WHEN x < 0 THEN false WHEN x < {_XMIN} THEN true WHEN x >= {_XMAX} THEN false "
                f"ELSE NOT coalesce(list_contains({_XIP}, x), false) END",
            ),
        ),
    )


#: Functions of their arguments alone that a catalog statement calls, called as a catalog function
#: where a statement reads no table (provisa.pgwire.catalog_server_answers.ANSWERED).
VALUE_FUNCTIONS_OF_THE_SERVER = (
    _macro("pg_walfile_name", "lsn", strict("lsn", "VARCHAR", wal_file("lsn"))),
    _macro(
        "pg_wal_lsn_diff",
        "a, b",
        strict(
            "a, b",
            "DECIMAL(38,0)",
            f"CAST({lsn_number('a')} AS DECIMAL(38,0)) - CAST({lsn_number('b')} AS DECIMAL(38,0))",
        ),
    ),
    _macro(
        "pg_lsn",
        "value",
        strict(
            "value",
            "VARCHAR",
            "CASE WHEN value < 0 OR value >= 18446744073709551616 THEN "
            + raising("22023", "'pg_lsn out of range'")
            + " ELSE upper(printf('%x/%x', CAST(value AS UBIGINT) >> 32, "
            "CAST(value AS UBIGINT) & 4294967295)) END",
        ),
    ),
    *_snapshot("pg_snapshot", "pg_visible_in_snapshot"),
    *_snapshot("txid_snapshot", "txid_visible_in_snapshot"),
    _macro(
        "mxid_age",
        "x",
        strict("x", "INTEGER", "CASE WHEN x = 0 THEN 2147483647 ELSE CAST(1 - x AS INTEGER) END"),
    ),
    _macro(
        "pg_encoding_max_length",
        "e",
        strict(
            "e",
            "INTEGER",
            f"CASE WHEN e >= 0 THEN [{', '.join(map(str, _ENCODING_MAX_LENGTHS))}][e + 1] END",
        ),
    ),
    _pg("aclitemeq", "a, b", strict("a, b", "BOOLEAN", "a = b")),
    _macro("aclcontains", "acl, item", strict("acl, item", "BOOLEAN", "list_contains(acl, item)")),
    # REQ-2190: functions of a value, the engine's since a statement with no table is.
    _macro(
        "pg_walfile_name_offset",
        "lsn",
        strict(
            "lsn",
            "STRUCT(file_name VARCHAR, file_offset INTEGER)",
            f"struct_pack(file_name := {wal_file('lsn')}, "
            f"file_offset := CAST({lsn_number('lsn')} % {WAL_SEGMENT} AS INTEGER))",
        ),
    ),
    _macro(
        "pg_split_walfile_name",
        "f",
        strict(
            "f",
            "STRUCT(segment_number DECIMAL(38,0), timeline_id BIGINT)",
            "CASE WHEN regexp_full_match(f, '[0-9A-F]{24}') THEN struct_pack(segment_number := "
            "CAST((CAST('0x' || substr(f, 9, 8) AS BIGINT) * 256) + "
            "CAST('0x' || substr(f, 17, 8) AS BIGINT) AS DECIMAL(38,0)), "
            "timeline_id := CAST('0x' || substr(f, 1, 8) AS BIGINT)) ELSE "
            + raising("22023", "'invalid WAL file name \"' || f || '\"'")
            + " END",
        ),
    ),
    # PostgreSQL's Default abbreviations (pg_zones); its zones are the polyfill grid's row.
    _macro("pg_timezone_abbrevs", "", f"unnest((SELECT list(_z) FROM ({pg_zones.ABBREVS}) _z))"),
    _macro(
        "pg_indexam_has_property",
        "am, prop",
        f"CASE WHEN am IN ({', '.join(map(str, _BUILT_IN_ACCESS_METHODS))}) THEN CASE "
        + " ".join(
            f"WHEN prop = '{name}' THEN am IN ({', '.join(map(str, methods))})"
            for name, methods in _INDEXAM_PROPERTIES.items()
        )
        + " END END",
    ),
    _macro(
        "provisa_xid_age",
        "x",
        strict(
            "x",
            "INTEGER",
            "CASE WHEN x < 3 THEN 2147483647 ELSE CAST(txid_current() - x AS INTEGER) END",
        ),
    ),
    # REQ-2190: a catalog form's PostgreSQL error, where its statement reads no table. The control
    # plane's own provisa_raise is a function of that database (provisa.api.startup_seed).
    _macro(
        "provisa_raise",
        "sqlstate, message",
        f"error('{MARK} ' || sqlstate || ': ' || message)",
        others={
            _TRINO: f"fail('{MARK} ' || sqlstate || ': ' || message)",
            frozenset({"clickhouse"}): (
                f"toString(throwIf(1, '{MARK} ' || sqlstate || ': ' || message))"
            ),
        },
    ),
)

#: A value of a sequence, which only its database can answer: a NULL is strict's.
_OPTION = "string_split(_o, '=')"
_OTHERS: tuple[Macro, ...] = (
    _macro(
        "int8inc_any",
        "a, b",
        strict("a, b", "BIGINT", pg_forms.in_range("CAST(a AS HUGEINT) + 1", "BIGINT")),
    ),
    _macro(
        "int8dec_any",
        "a, b",
        strict("a, b", "BIGINT", pg_forms.in_range("CAST(a AS HUGEINT) - 1", "BIGINT")),
    ),
    _macro(
        "satisfies_hash_partition",
        "parent, modulus, remainder, value",
        "CASE WHEN parent IS NULL OR modulus IS NULL OR remainder IS NULL THEN false ELSE "
        + raising("0A000", f"'satisfies_hash_partition {pg_forms_types.REFUSED}'")
        + " END",
    ),
    # An ACL is always NULL here: PostgreSQL's items name roles this server does not keep.
    _macro(
        "aclexplode",
        "acl",
        "unnest(CASE WHEN acl IS NULL OR len(acl) = 0 THEN CAST([] AS STRUCT(grantor INTEGER, "
        "grantee INTEGER, privilege_type VARCHAR, is_grantable BOOLEAN)[]) ELSE "
        + raising("0A000", f"'aclexplode {pg_forms_types.REFUSED}'")
        + " END)",
    ),
    _macro(
        "pg_options_to_table",
        "options",
        "unnest(list_transform(options, _o -> struct_pack(option_name := split_part(_o, '=', 1), "
        "option_value := CASE WHEN contains(_o, '=') THEN "
        "substr(_o, strpos(_o, '=') + 1) END)))",
    ),
)

ROWS: tuple[Macro, ...] = (
    Macro("current_query", ()),  # the engine's is the statement as Provisa wrote it
    _udf("pg_size_bytes", "size", "provisa_size_bytes(size)"),
    _macro("pg_typeof", "value", _PG_TYPEOF),
    _macro(
        "oidvectorsend",
        "v",
        "(SELECT CASE WHEN v IS NOT NULL THEN unhex("
        "printf('%08x%08x%08x%08x%08x', 1, 0, 26, len(_v), 0) || "
        "array_to_string(list_transform(_v, _o -> printf('00000004%08x', CAST(_o AS BIGINT))), "
        "'')) END FROM (SELECT list_filter(string_split(v, ' '), _o -> _o <> '') AS _v))",
    ),
    *_comparisons("oidvector", "eq", "ne", "lt", "le", "gt", "ge"),
    *_comparisons("xid8", "eq", "ne", "lt", "le", "gt", "ge"),
    _pg("xid8cmp", "a, b", "CASE WHEN a < b THEN -1 WHEN a > b THEN 1 WHEN a = b THEN 0 END"),
    _pg("xid8_larger", "a, b", "CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN GREATEST(a, b) END"),
    _pg("xid8_smaller", "a, b", "CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN LEAST(a, b) END"),
    _macro("xid8send", "v", _INT8_BYTES),
    *(
        _macro(f"{name}send", "v", _OID_BYTES)
        for name in (
            "regclass",
            "regcollation",
            "regconfig",
            "regdictionary",
            "regnamespace",
            "regoper",
            "regoperator",
            "regprocedure",
            "regproc",
            "regrole",
            "regtype",
        )
    ),
    *(Macro(name, params, forms) for name, params, forms in pg_forms_system.ROWS),
    *_OTHERS,
    *VALUE_FUNCTIONS_OF_THE_SERVER,
    *(Macro(name, params, {_EVERY: Pg(form)}) for name, params, form in VALUE_FORMS),  # REQ-2190
)
