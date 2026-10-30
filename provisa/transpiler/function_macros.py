# Copyright (c) 2026 Kenneth Stott
# Canary: 9cbebdc0-4125-4511-801d-b79d35074070
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's functions as compiler pushdown (REQ-2167, FUNCTIONS ARE COMPILER PUSHDOWN): a macro
is a per-dialect rewrite of a function a dialect does not have, applied at transpile time, so every
transport and every engine that can run it gets it.

``MACROS`` is the one table: a row is a function's overload and its form on each dialect. A form is
SQL over the overload's parameters, in the target's own dialect, or in PostgreSQL's (``Pg``) where
sqlglot writes one rewrite for each dialect listed. PostgreSQL has every one of them itself. A
call on a dialect with no form is refused by name, never sent to fail as an engine error.

A DuckDB form that names a ``provisa_`` function calls a Python UDF of
provisa.federation.duckdb_pg_scalars: that function is DuckDB's alone until it is written as SQL."""

# Requirements: REQ-2167

from __future__ import annotations

import functools
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import raising

NATIVE = frozenset({"postgres"})
"""The dialects that have every function of the table themselves."""

_DUCKDB = frozenset({"duckdb"})
_TRINO = frozenset({"trino", "presto", "athena"})
_SPARK = frozenset({"spark", "databricks"})
_EVERY = frozenset({"*"})
#: A case-sensitive LIKE that takes an ESCAPE clause, and sqlglot's writing of ILIKE beside it.
_LIKES = _TRINO | _SPARK | {"snowflake", "redshift"}
#: Text concatenation, NULL for a NULL operand.
_CONCATS = _LIKES | {"mysql", "bigquery", "clickhouse", "tsql", "sqlite"}
_STRINGS = _LIKES | {"mysql", "bigquery"}
_CBRTS = _LIKES | {"bigquery", "clickhouse"}
_TRUNCATES = _TRINO | {"snowflake", "mysql", "bigquery", "tsql", "redshift", "clickhouse"}
_BOOL_ANDS = _LIKES | {"bigquery"}
_SPLIT_PARTS = _LIKES
#: A regexp_substr(value, pattern, start, occurrence) of the dialect's own, sqlglot's to write.
_REGEXP_SUBSTRS = frozenset({"snowflake", "oracle", "mysql", "bigquery", "redshift"})


class Pg(str):
    """A form in PostgreSQL's dialect, written for each dialect it is listed under."""


#: The form of a function sqlglot already writes for the dialect: the call is left to it.
MAPPED = None

Forms = Mapping[frozenset[str], "str | None"]


@dataclass(frozen=True)
class Macro:
    """One overload of a function: its parameters and its form on each dialect."""

    name: str
    params: tuple[str, ...]
    forms: Forms

    def form(self, dialect: str) -> str | None | Literal[False]:
        """The form on ``dialect``; False where it has none."""
        for dialects, sql in self.forms.items():
            if dialect in dialects or "*" in dialects:
                return sql
        return False


class FunctionRefused(ValueError):
    """A call of a function the routed dialect neither has nor has a form of."""

    code = "query.function_unsupported"

    def __init__(self, function: str, dialect: str) -> None:
        self.params = {"function": function, "dialect": dialect}
        super().__init__(f"{function}() has no form on {dialect}")


def strict(params: str, sql_type: str, answer: str) -> str:
    """``answer``, or a NULL of ``sql_type`` for a NULL argument: a strict function, of its
    PostgreSQL type where the form's own NULL would have none."""
    if not params:
        return answer
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


def quoted_identifier(name: str) -> str:
    """A name as quote_ident writes it: bare when it is a lowercase identifier."""
    return (
        f"CASE WHEN regexp_full_match({name}, '[a-z_][a-z0-9_$]*') THEN {name} "
        f"ELSE '\"' || replace({name}, '\"', '\"\"') || '\"' END"
    )


# --- the forms ----------------------------------------------------------------------------------

_BACKSLASH = "\\"
_LIKE = f"(a LIKE b ESCAPE '{_BACKSLASH}')"
_ILIKE = f"(a ILIKE b ESCAPE '{_BACKSLASH}')"
_REGEX = "regexp_matches(a, b)"
_IREGEX = "regexp_matches(a, b, 'i')"

#: Each dialect's regular expression match, and the case-insensitive one: anywhere in the value.
_MATCHES: tuple[tuple[frozenset[str], str, str], ...] = (
    (_DUCKDB, _REGEX, _IREGEX),
    (_TRINO, "regexp_like(a, b)", "regexp_like(a, '(?i)' || b)"),
    (_SPARK, "a RLIKE b", "a RLIKE CONCAT('(?i)', b)"),
    (frozenset({"bigquery"}), "REGEXP_CONTAINS(a, b)", "REGEXP_CONTAINS(a, CONCAT('(?i)', b))"),
    (frozenset({"clickhouse"}), "match(a, b)", "match(a, CONCAT('(?i)', b))"),
    # REGEXP_LIKE matches the whole value there (transpile._WHOLE_VALUE_REGEXP_LIKE).
    (frozenset({"snowflake"}), "REGEXP_INSTR(a, b) > 0", "REGEXP_INSTR(a, b, 1, 1, 0, 'i') > 0"),
    (frozenset({"redshift"}), "a ~ b", "a ~* b"),
)


def _regex_forms(which: int, negated: bool) -> dict[frozenset[str], str]:
    not_ = "NOT " if negated else ""
    return {row[0]: f"{not_}({row[which]})" for row in _MATCHES}


def _text_matches() -> tuple[Macro, ...]:
    like = {_DUCKDB: _LIKE, _LIKES: Pg(_LIKE)}
    ilike = {_DUCKDB: _ILIKE, _LIKES: Pg(_ILIKE)}
    negated_like = {k: type(v)(f"NOT {v}") for k, v in like.items()}
    negated_ilike = {k: type(v)(f"NOT {v}") for k, v in ilike.items()}
    family: dict[str, Forms] = {
        "like": like,
        "nlike": negated_like,
        "iclike": ilike,
        "icnlike": negated_ilike,
        "regexeq": _regex_forms(1, False),
        "regexne": _regex_forms(1, True),
        "icregexeq": _regex_forms(2, False),
        "icregexne": _regex_forms(2, True),
    }
    return tuple(
        Macro(f"{prefix}{suffix}", ("a", "b"), forms)
        for prefix in ("text", "name", "bpchar")
        for suffix, forms in family.items()
    )


def _comparisons(prefix: str, *suffixes: str) -> tuple[Macro, ...]:
    operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
    return tuple(
        Macro(f"{prefix}{suffix}", ("a", "b"), {_DUCKDB: f"a {operators[suffix]} b"})
        for suffix in suffixes
    )


def _macro(name: str, params: str, duckdb: str, others: Forms | None = None) -> Macro:
    """An overload, its DuckDB form and any other dialect's."""
    return Macro(
        name, tuple(p for p in params.split(", ") if p), {_DUCKDB: duckdb, **(others or {})}
    )


#: oid_send: an oid's four bytes, for the send functions of the object identifier types.
_OID_BYTES = "unhex(printf('%08x', CAST(v AS BIGINT)))"
_INT8_BYTES = "unhex(printf('%016x', CAST(v AS UBIGINT)))"

#: quote_literal: an E'' string, its backslashes doubled, when it holds one.
_QUOTED_LITERAL = (
    "CAST(CASE "
    f"WHEN contains(CAST(value AS VARCHAR), '{_BACKSLASH}') THEN 'E''' || "
    f"replace(replace(CAST(value AS VARCHAR), '{_BACKSLASH}', '{_BACKSLASH * 2}'), '''', '''''') "
    "|| '''' ELSE '''' || replace(CAST(value AS VARCHAR), '''', '''''') || '''' END AS VARCHAR)"
)
_PG_QUOTED_LITERAL = (
    "CAST(CASE "
    f"WHEN STRPOS(CAST(value AS TEXT), '{_BACKSLASH}') > 0 THEN 'E''' || "
    f"REPLACE(REPLACE(CAST(value AS TEXT), '{_BACKSLASH}', '{_BACKSLASH * 2}'), '''', '''''') "
    "|| '''' ELSE '''' || REPLACE(CAST(value AS TEXT), '''', '''''') || '''' END AS TEXT)"
)

#: A geometric value's numbers, in the order its text form writes them.
_NUMBERS = (
    "regexp_extract_all(CAST(value AS VARCHAR), '[-+]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?')"
)


def _number(position: int) -> str:
    return f"CAST({_NUMBERS}[{position}] AS DOUBLE)"


#: The bits of an integer's two's complement, as to_bin and to_oct read them.
_TWOS_COMPLEMENT = (
    "(CAST(value AS HUGEINT) & CASE WHEN typeof(value) IN ('BIGINT', 'HUGEINT') "
    "THEN CAST(18446744073709551615 AS HUGEINT) ELSE CAST(4294967295 AS HUGEINT) END)"
)


def _in_bytea(index: str, size: str) -> str:
    """get_byte's and set_byte's refusal of an index outside the value."""
    return (
        f"WHEN {index} < 0 OR {index} >= {size} THEN "
        + raising(
            "2202E",
            f"'index ' || {index} || ' out of valid range, 0..' || ({size} - 1)",
        )
        + " "
    )


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
                f"CASE WHEN x < {_XMIN} THEN true WHEN x >= {_XMAX} THEN false "
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
        strict("e", "INTEGER", f"[{', '.join(map(str, _ENCODING_MAX_LENGTHS))}][e + 1]"),
    ),
    _macro("aclitemeq", "a, b", strict("a, b", "BOOLEAN", "a = b")),
    _macro("aclcontains", "acl, item", strict("acl, item", "BOOLEAN", "list_contains(acl, item)")),
)

_ROWS: tuple[Macro, ...] = (
    *_text_matches(),
    _macro("like", "a, b", _LIKE, {_LIKES: Pg(_LIKE)}),
    _macro("notlike", "a, b", f"NOT {_LIKE}", {_LIKES: Pg(f"NOT {_LIKE}")}),
    _macro("textcat", "a, b", "CAST(a || b AS VARCHAR)", {_CONCATS: Pg("CAST(a || b AS TEXT)")}),
    _macro("byteacat", "a, b", "CAST(a || b AS BLOB)"),
    _macro("bytealike", "a, b", "provisa_bytea_like(a, b)"),
    _macro("byteanlike", "a, b", "NOT provisa_bytea_like(a, b)"),
    _macro("bitcat", "a, b", "CAST(CAST(a AS VARCHAR) || CAST(b AS VARCHAR) AS BIT)"),
    _macro(
        "bitshiftleft",
        "value, n",
        "CAST(substr(CAST(value AS VARCHAR), n + 1) || "
        "repeat('0', least(n, length(CAST(value AS VARCHAR)))) AS BIT)",
    ),
    _macro(
        "bitshiftright",
        "value, n",
        "CAST(repeat('0', least(n, length(CAST(value AS VARCHAR)))) || "
        "left(CAST(value AS VARCHAR), greatest(length(CAST(value AS VARCHAR)) - n, 0)) AS BIT)",
    ),
    _macro(
        "get_byte",
        "value, n",
        f"CASE {_in_bytea('n', 'octet_length(value)')}"
        "ELSE CAST('0x' || substr(hex(value), n * 2 + 1, 2) AS INTEGER) END",
    ),
    _macro(
        "set_byte",
        "value, n, new_byte",
        f"CASE {_in_bytea('n', 'octet_length(value)')}"
        "ELSE unhex(left(hex(value), n * 2) || printf('%02X', new_byte & 255) || "
        "substr(hex(value), n * 2 + 3)) END",
    ),
    _macro("quote_ident", "value", quoted_identifier("CAST(value AS VARCHAR)")),
    _macro("quote_literal", "value", _QUOTED_LITERAL, {_STRINGS: Pg(_PG_QUOTED_LITERAL)}),
    _macro(
        "quote_nullable",
        "value",
        f"CASE WHEN value IS NULL THEN 'NULL' ELSE {_QUOTED_LITERAL} END",
        {_STRINGS: Pg(f"CASE WHEN value IS NULL THEN 'NULL' ELSE {_PG_QUOTED_LITERAL} END")},
    ),
    _macro("dcbrt", "x", "cbrt(x)", {_CBRTS: Pg("CBRT(x)")}),
    _macro(
        "dround",
        "x",
        "round_even(CAST(x AS DOUBLE), 0)",
        {
            _STRINGS: Pg(
                "CASE WHEN CAST(x AS DOUBLE PRECISION) - FLOOR(CAST(x AS DOUBLE PRECISION)) = 0.5 "
                "THEN 2 * ROUND(CAST(x AS DOUBLE PRECISION) / 2) "
                "ELSE ROUND(CAST(x AS DOUBLE PRECISION)) END"
            )
        },
    ),
    _macro(
        "dtrunc",
        "x",
        "trunc(CAST(x AS DOUBLE))",
        {_TRUNCATES: Pg("TRUNC(CAST(x AS DOUBLE PRECISION))")},
    ),
    _macro(
        "sind",
        "x",
        "CASE WHEN isnan(CAST(x AS DOUBLE)) THEN CAST(x AS DOUBLE) ELSE provisa_sind(x) END",
    ),
    _macro(
        "tand",
        "x",
        "CASE WHEN isnan(CAST(x AS DOUBLE)) THEN CAST(x AS DOUBLE) ELSE provisa_tand(x) END",
    ),
    _macro("every", "x", "bool_and(x)", {_BOOL_ANDS: Pg("BOOL_AND(x)")}),
    _macro(
        "scale",
        "x",
        "CASE WHEN x IS NULL THEN CAST(NULL AS INTEGER) "
        "WHEN contains(CAST(x AS VARCHAR), '.') "
        "THEN CAST(length(split_part(CAST(x AS VARCHAR), '.', 2)) AS INTEGER) ELSE 0 END",
        {
            _SPLIT_PARTS: Pg(
                "CASE WHEN x IS NULL THEN CAST(NULL AS INTEGER) "
                "WHEN STRPOS(CAST(x AS TEXT), '.') > 0 "
                "THEN CAST(LENGTH(SPLIT_PART(CAST(x AS TEXT), '.', 2)) AS INTEGER) ELSE 0 END"
            )
        },
    ),
    _macro("to_bin", "value", f"format('{{:b}}', {_TWOS_COMPLEMENT})"),
    _macro("to_oct", "value", f"format('{{:o}}', {_TWOS_COMPLEMENT})"),
    _macro("pg_size_bytes", "size", "provisa_size_bytes(size)"),
    _macro(
        "pg_convert",
        "value, source, destination",
        "provisa_convert(value, source, destination)",
    ),
    _macro("pg_json_object", "value", "CAST(provisa_json_object(value, NULL) AS JSON)"),
    _macro(
        "pg_json_object",
        "ks, vs",
        "CAST(CASE WHEN ks IS NOT NULL AND vs IS NOT NULL THEN "
        "provisa_json_object(ks, vs) END AS JSON)",
    ),
    _macro("convert_from", "value, source", "decode(provisa_convert(value, source, 'UTF8'))"),
    _macro(
        "convert_to", "value, destination", "provisa_convert(encode(value), 'UTF8', destination)"
    ),
    _macro("regexp_match", "value, pattern", "provisa_regexp_match(value, pattern, '')"),
    _macro("regexp_match", "value, pattern, flags", "provisa_regexp_match(value, pattern, flags)"),
    *(
        Macro(
            "regexp_substr",
            params,
            {_DUCKDB: f"provisa_regexp_substr(value, pattern, {rest})", **others},
        )
        for params, rest, others in (
            (
                ("value", "pattern"),
                "1, 1, '', 0",
                # Trino has no regexp_substr, which sqlglot writes; regexp_extract is its first match.
                {_TRINO: "regexp_extract(value, pattern)", _REGEXP_SUBSTRS: MAPPED},
            ),
            (("value", "pattern", "start"), "start, 1, '', 0", {_REGEXP_SUBSTRS: MAPPED}),
            (("value", "pattern", "start", "n"), "start, n, '', 0", {_REGEXP_SUBSTRS: MAPPED}),
            (
                ("value", "pattern", "start", "n", "flags"),
                "start, n, flags, 0",
                {_REGEXP_SUBSTRS - {"bigquery"}: MAPPED},  # BigQuery's stops at the occurrence
            ),
            (
                ("value", "pattern", "start", "n", "flags", "subexpr"),
                "start, n, flags, subexpr",
                {frozenset({"snowflake", "oracle"}): MAPPED},  # the others take no group
            ),
        )
    ),
    _macro(
        "string_to_table",
        "value, delimiter",
        "unnest(provisa_string_to_array(value, delimiter, NULL))",
    ),
    _macro(
        "string_to_table",
        "value, delimiter, null_string",
        "unnest(provisa_string_to_array(value, delimiter, null_string))",
    ),
    _macro("similar_escape", "pattern, esc", "provisa_similar_escape(pattern, esc)"),
    _macro(
        "similar_to_escape",
        "pattern",
        "CASE WHEN pattern IS NOT NULL THEN provisa_similar_escape(pattern, NULL) END",
    ),
    _macro(
        "similar_to_escape",
        "pattern, esc",
        "CASE WHEN pattern IS NOT NULL AND esc IS NOT NULL THEN "
        "provisa_similar_escape(pattern, esc) END",
    ),
    _macro("is_normalized", "value, form", "provisa_is_normalized(value, form)"),
    _macro("unicode_assigned", "value", "provisa_unicode_assigned(value)"),
    Macro("unicode_version", (), {_EVERY: Pg("'15.1'")}),
    Macro("icu_unicode_version", (), {_EVERY: Pg("CAST(NULL AS TEXT)")}),
    _macro("unistr", "value", "provisa_unistr(value)"),
    # The database's encoding is UTF8, which to_ascii refuses; a named single-byte one it strips.
    _macro("to_ascii", "value", "provisa_to_ascii(value, 'UTF8')"),
    _macro("to_ascii", "value, encoding", "provisa_to_ascii(value, CAST(encoding AS VARCHAR))"),
    _macro("parse_ident", "value", "provisa_parse_ident(value, true)"),
    _macro("parse_ident", "value, strict_", "provisa_parse_ident(value, strict_)"),
    _macro(
        "abbrev",
        "value",
        "provisa_abbrev(CAST(value AS VARCHAR), CASE typeof(value) WHEN 'INET' THEN 'inet' "
        "ELSE 'cidr' END)",
    ),
    _macro(
        "nameconcatoid",
        "name, oid",
        "CAST(left(name, 63 - length('_' || oid)) || '_' || oid AS VARCHAR)",
        {
            _STRINGS: Pg(
                "LEFT(name, 63 - LENGTH('_' || CAST(oid AS TEXT))) || '_' || CAST(oid AS TEXT)"
            )
        },
    ),
    _macro(
        "jsonb_typeof",
        "value",
        "CASE WHEN value IS NULL THEN CAST(NULL AS VARCHAR) "
        "ELSE CASE json_type(value) WHEN 'OBJECT' THEN 'object' "
        "WHEN 'ARRAY' THEN 'array' WHEN 'VARCHAR' THEN 'string' WHEN 'BOOLEAN' THEN 'boolean' "
        "WHEN 'NULL' THEN 'null' ELSE 'number' END END",
    ),
    _macro("area", "value", "provisa_area(CAST(value AS VARCHAR))"),
    _macro("center", "value", "provisa_center(CAST(value AS VARCHAR))"),
    _macro("diagonal", "value", "provisa_diagonal(CAST(value AS VARCHAR))"),
    _macro("height", "value", f"abs({_number(4)} - {_number(2)})"),
    _macro("width", "value", f"abs({_number(3)} - {_number(1)})"),
    _macro("radius", "value", _number(3)),
    _macro("isclosed", "value", "starts_with(trim(CAST(value AS VARCHAR)), '(')"),
    _macro("isopen", "value", "NOT starts_with(trim(CAST(value AS VARCHAR)), '(')"),
    _macro("ishorizontal", "a", "provisa_is_horizontal(CAST(a AS VARCHAR))"),
    _macro(
        "ishorizontal", "a, b", "provisa_points_horizontal(CAST(a AS VARCHAR), CAST(b AS VARCHAR))"
    ),
    _macro("isvertical", "a", "provisa_is_vertical(CAST(a AS VARCHAR))"),
    _macro("isvertical", "a, b", "provisa_points_vertical(CAST(a AS VARCHAR), CAST(b AS VARCHAR))"),
    _macro("isparallel", "a, b", "provisa_is_parallel(CAST(a AS VARCHAR), CAST(b AS VARCHAR))"),
    _macro("isperp", "a, b", "provisa_is_perpendicular(CAST(a AS VARCHAR), CAST(b AS VARCHAR))"),
    _macro(
        "pt_contained_circle",
        "p, c",
        "provisa_point_in_circle(CAST(p AS VARCHAR), CAST(c AS VARCHAR))",
    ),
    _macro(
        "pt_contained_poly",
        "p, poly",
        "provisa_point_in_polygon(CAST(p AS VARCHAR), CAST(poly AS VARCHAR))",
    ),
    _macro("strip", "value", "provisa_tsvector_strip(CAST(value AS VARCHAR))"),
    _macro(
        "mul_d_interval",
        "d, i",
        "to_microseconds(CAST(round(epoch(i) * 1000000 * d) AS BIGINT))",
    ),
    _macro(
        "oidvectorsend",
        "v",
        "CASE WHEN v IS NOT NULL THEN unhex("
        "printf('%08x%08x%08x%08x%08x', 1, 0, 26, len(v), 0) || "
        "array_to_string(list_transform(v, _o -> printf('00000004%08x', _o)), '')) END",
    ),
    _macro(
        "numrange",
        "a, b",
        "CASE WHEN a IS NOT NULL AND b IS NOT NULL AND a > b THEN "
        + raising("22000", "'range lower bound must be less than or equal to range upper bound'")
        + " WHEN a IS NOT NULL AND a = b THEN 'empty' "
        "ELSE CASE WHEN a IS NULL THEN '(' ELSE '[' END || coalesce(CAST(a AS VARCHAR), '') "
        "|| ',' || coalesce(CAST(b AS VARCHAR), '') || ')' END",
    ),
    _macro(
        "varbit",
        "value, n, explicit",
        "CASE WHEN value IS NOT NULL AND n IS NOT NULL THEN "
        "CAST(CASE WHEN n < 0 THEN CAST(value AS VARCHAR) ELSE left(CAST(value AS VARCHAR), n) END "
        "AS BIT) END",
    ),
    _macro(
        "json_each",
        "v",
        "unnest(list_transform(json_keys(v), "
        "_k -> struct_pack(key := _k, value := json_extract(v, '$.\"' || _k || '\"'))))",
    ),
    *_comparisons("oidvector", "eq", "ne", "lt", "le", "gt", "ge"),
    *_comparisons("xid8", "eq", "ne", "lt", "le", "gt", "ge"),
    _macro("xid8cmp", "a, b", "CASE WHEN a < b THEN -1 WHEN a > b THEN 1 WHEN a = b THEN 0 END"),
    _macro(
        "xid8_larger",
        "a, b",
        "CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN greatest(a, b) END",
    ),
    _macro(
        "xid8_smaller",
        "a, b",
        "CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN least(a, b) END",
    ),
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
    # A character value of a "char".
    _macro("bpchar", "value", "CAST(value AS VARCHAR)", {_STRINGS: Pg("CAST(value AS TEXT)")}),
    # An encoding's number: UTF8 is 6, a name naming none -1.
    _macro(
        "pg_char_to_encoding",
        "name",
        "CASE WHEN lower(regexp_replace(name, '[^A-Za-z0-9]', '', 'g')) IN ('utf8', 'unicode') "
        "THEN 6 WHEN name IS NOT NULL THEN -1 END",
    ),
    # Strict; on a value PostgreSQL refuses this form: a record needs a column definition list.
    _macro(
        "json_to_record",
        "value",
        "CASE WHEN value IS NOT NULL THEN "
        "error('function returning record called in context that cannot accept type record') "
        "ELSE CAST(NULL AS VARCHAR) END",
    ),
    *VALUE_FUNCTIONS_OF_THE_SERVER,
)

#: A call's name and argument count -> its overload: the one table.
MACROS: dict[tuple[str, int], Macro] = {(m.name, len(m.params)): m for m in _ROWS}
_NAMES = frozenset(name for name, _ in MACROS)
_MENTIONED = re.compile(r"\b(" + "|".join(sorted(_NAMES)) + r")\"?\s*\(", re.IGNORECASE)
#: The names provisa.pgwire.catalog answers where a statement reads no table.
SERVER_VALUE_NAMES = frozenset(m.name for m in VALUE_FUNCTIONS_OF_THE_SERVER)


def outcome(name: str, arity: int, dialect: str) -> str:
    """What a call does on ``dialect``: native, mapped, emulated or refused."""
    macro = MACROS[(name, arity)]
    if dialect in NATIVE:
        return "native"
    form = macro.form(dialect)
    if form is False:
        return "refused"
    return "mapped" if form is MAPPED else "emulated"


def mentioned(sql: str, dialect: str) -> bool:
    """Whether ``sql`` may call a function of the table that ``dialect`` does not have itself; a
    statement that calls none is not parsed."""
    return dialect not in NATIVE and _MENTIONED.search(sql) is not None


@functools.lru_cache(maxsize=2048)
def _parsed(name: str, arity: int, dialect: str) -> exp.Expr:
    form = MACROS[(name, arity)].form(dialect)
    assert isinstance(form, str)
    return sqlglot.parse_one(form, read="postgres" if isinstance(form, Pg) else dialect)


#: An argument that needs no parentheses wherever a form writes it.
_ATOMIC = (exp.Column, exp.Literal, exp.Func, exp.Cast, exp.Paren, exp.Null, exp.Boolean)


def _argument(value: exp.Expr) -> exp.Expr:
    return value if isinstance(value, _ATOMIC) else exp.Paren(this=value)


def _written(macro: Macro, args: list[exp.Expr], dialect: str) -> exp.Expr:
    """The call's form with each parameter its argument. As a DuckDB macro does, an argument is
    written wherever the form names it; it is evaluated each time."""
    values = dict(zip(macro.params, args))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name]).copy()
        return node

    return _parsed(macro.name, len(args), dialect).copy().transform(bound)


def _call(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of a call the table may hold."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if isinstance(node, exp.RegexpSubstr):
        keys = ("this", "expression", "position", "occurrence", "parameters", "group")
        return "regexp_substr", [node.args[k] for k in keys if node.args.get(k) is not None]
    return None


def inline(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` with each call of a function of the table written in its form for ``dialect``.
    Raises :class:`FunctionRefused` for a call of one it has no form of. A call with no overload
    of its argument count is left, for the engine to say so."""
    if dialect in NATIVE:
        return tree

    def rewrite(node: exp.Expr) -> exp.Expr:
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        call = _call(target)
        if call is None:
            return node
        name, args = call
        macro = MACROS.get((name, len(args)))
        if macro is None:
            return node
        form = macro.form(dialect)
        if form is False:
            raise FunctionRefused(name, dialect)
        if form is MAPPED:
            return target
        # transform does not revisit what a replaced node holds: a call in an argument.
        written = _written(macro, [a.transform(rewrite) for a in args], dialect)
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, name)  # the column is named for the function
        return written

    return tree.transform(rewrite)
