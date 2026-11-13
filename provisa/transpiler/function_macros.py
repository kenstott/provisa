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

The rule (REQ-2171, THE RULE FOR TYPES AND FUNCTIONS, THE SAME TYPE UNDER ANOTHER NAME) is here and
nowhere else, in this order: :class:`Step`. ``Macro.resolved`` is the one place it is applied:

1. the dialect's own function, under any name (``own``, and PostgreSQL has every one of them);
2. the functions the dialect does have (``forms``);
3. its inline operators (``operators``);
4. otherwise the call is refused by name, never sent to fail as an engine error.

``MACROS`` is the one table: a row is a function's overload and its forms. A form is SQL over the
overload's parameters, in the target's own dialect, or in PostgreSQL's (``Pg``) where sqlglot writes
one rewrite for each dialect listed. An overload is chosen by the type the statement says of its
first argument (``OVERLOADS``), never by the engine's type: PostgreSQL's own name for it, as pg_proc
has it, is its row. The row of the function's own name answers an argument whose type the statement
does not say.

A DuckDB form that names a ``provisa_`` function calls a Python UDF of
provisa.federation.duckdb_pg_scalars: that function is DuckDB's alone until it is written as SQL."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import enum
import functools
import itertools
import re
import string
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import Dialects

from provisa.core.pg_error import MARK, PostgresError, raising
from provisa.core.pg_range import (
    MULTIRANGE_TYPES,
    RANGE_TYPES,
    parse_multirange,
    parse_range,
    range_text,
)
from provisa.transpiler import (
    engine_refusals,
    polyfill,
    pg_forms,
    pg_forms_aggregate,
    pg_forms_enum,
    pg_forms_geometric,
    pg_forms_hash,
    pg_forms_index,
    pg_forms_json,
    pg_forms_system,
    pg_forms_textsearch,
    pg_forms_type_io,
    pg_forms_types,
    pg_zones,
)
from provisa.transpiler.metadata_functions import VALUE_FORMS
from provisa.transpiler.polyfill import notation as polyfill_notation
from provisa.transpiler.polyfill.regex import said_text
from provisa.transpiler.pg_literals import literal_input as postgres_literal
from provisa.transpiler.pg_literals import values_cast

NATIVE = frozenset({"postgres"})
"""The dialects that have every function of the table themselves."""

_DUCKDB = frozenset({"duckdb"})
_TRINO = frozenset({"trino", "presto", "athena"})
_SPARK = frozenset({"spark", "databricks"})
_EVERY = frozenset({"*"})
#: Every dialect sqlglot writes but DuckDB: ``_EVERY`` in ``own`` would answer before DuckDB's form.
_EVERY_BUT_DUCKDB = frozenset(d.value for d in Dialects if d.value) - _DUCKDB - NATIVE
#: LIKE ignores case by the collation here, and a replay of a dialect that differs refuses the form.
_NO_CASE_SENSITIVE_LIKE = frozenset({"mysql", "tsql", "oracle", "sqlite"})
#: No regular expression match, or none sqlglot can write.
_NO_REGEX = frozenset({"tsql", "sqlite", "oracle", "mysql"})
_CLICKHOUSE = frozenset({"clickhouse"})
_NO_SPLIT_PART = frozenset({"tsql"})
#: A time of day is no type of these engines: a cast to it is refused by name.
_NO_TIME = frozenset({"trino", "presto", "athena", "clickhouse"})
#: A regexp_substr(value, pattern, start, occurrence) of the dialect's own, sqlglot's to write.
_REGEXP_SUBSTRS = frozenset({"snowflake", "oracle", "mysql", "bigquery", "redshift"})
#: A cast of a text that is no number is an error here, so a form can raise PostgreSQL's refusal.
#: MySQL and SQLite read such a text as 0, and Spark and Databricks outside ANSI mode as NULL: a row
#: that raises is refused on them by name until a replay shows otherwise.
_CASTS_CHECKED = _DUCKDB | _TRINO | {
    "snowflake", "bigquery", "clickhouse", "tsql", "redshift", "oracle",
}  # fmt: skip
#: round/trunc(numeric, n) on DuckDB: NULL where the scale is NULL, which DuckDB 2.0 refuses to bind.
_NULL_SCALE = "CASE WHEN n IS NULL THEN NULL ELSE {}(x, COALESCE(n, 0)) END"


class Pg(str):
    """A form in PostgreSQL's dialect, written for each dialect it is listed under."""


#: The form of a function sqlglot already writes for the dialect: the call is left to it.
MAPPED = None

Forms = Mapping[frozenset[str], "str | None"]


class Step(enum.Enum):
    """The rule's steps, in the order it takes them."""

    OWN = "the dialect's own function, under any name"
    FUNCTIONS = "the functions the dialect has"
    OPERATORS = "the dialect's inline operators"
    REFUSED = "refused by name"


@dataclass(frozen=True)
class Macro:
    """One overload of a function: its parameters and its forms, by the step of the rule."""

    name: str
    params: tuple[str, ...]
    forms: Forms = field(default_factory=dict)
    own: Forms = field(default_factory=dict)
    operators: Forms = field(default_factory=dict)
    #: The dialects replay found another answer on (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED).
    differs_on: frozenset[str] = frozenset()

    def resolved(self, dialect: str) -> tuple[Step, str | None]:
        """The rule: the first step with a form on ``dialect``, and that form. ``differs_on`` holds
        the dialects a replay refused the shared form on (REQ-2171, A VARIATION IS ADDED ONLY AFTER
        A REPLAY FAILS)."""
        if dialect in NATIVE:
            return Step.OWN, MAPPED
        if dialect in self.differs_on:
            return Step.REFUSED, None
        steps = (
            (Step.OWN, self.own),
            (Step.FUNCTIONS, self.forms),
            (Step.OPERATORS, self.operators),
        )
        for step, forms in steps:
            for dialects, sql in forms.items():
                if dialect in dialects or "*" in dialects:
                    return step, sql
        return Step.REFUSED, None


class FunctionRefused(ValueError):
    """A call of a function the routed dialect neither has nor has a form of."""

    code = "query.function_unsupported"

    def __init__(self, function: str, dialect: str) -> None:
        self.params = {"function": function, "dialect": dialect}
        super().__init__(f"{function}() has no form on {dialect}")


class RowRefused(FunctionRefused):
    """A call the polyfill grid's row refuses on the routed engine (REQ-2236, THE REFUSAL LIVES IN
    THE GRID): the client receives the row's SQLSTATE and message."""

    def __init__(self, function: str, dialect: str, refusal: polyfill.Refusal) -> None:
        super().__init__(function, dialect)
        self.sqlstate = refusal.sqlstate
        self.args = (refusal.message,)


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
#: REGEXP_LIKE matches the whole value on Snowflake (transpile._WHOLE_VALUE_REGEXP_LIKE).
_SNOWFLAKE = frozenset({"snowflake"})


def _text_matches() -> tuple[Macro, ...]:
    operators = {
        "like": _LIKE,
        "nlike": f"NOT {_LIKE}",
        "iclike": _ILIKE,
        "icnlike": f"NOT {_ILIKE}",
    }
    functions = {
        "regexeq": ("(a ~ b)", "(REGEXP_INSTR(a, b) > 0)"),
        "regexne": ("NOT (a ~ b)", "NOT (REGEXP_INSTR(a, b) > 0)"),
        "icregexeq": ("(a ~* b)", "(REGEXP_INSTR(a, b, 1, 1, 0, 'i') > 0)"),
        "icregexne": ("NOT (a ~* b)", "NOT (REGEXP_INSTR(a, b, 1, 1, 0, 'i') > 0)"),
    }

    def of(prefix: str, form: str) -> str:
        """A bpchar is its text: an engine's char type takes no regular expression or LIKE."""
        return form.replace("(a ", "(CAST(a AS TEXT) ") if prefix == "bpchar" else form

    return (
        *(
            Macro(
                f"{prefix}{suffix}",
                ("a", "b"),
                operators={_EVERY: Pg(of(prefix, form))},
                differs_on=_NO_CASE_SENSITIVE_LIKE,
            )
            for prefix in ("text", "name", "bpchar")
            for suffix, form in operators.items()
        ),
        *(
            Macro(
                f"{prefix}{suffix}",
                ("a", "b"),
                {_SNOWFLAKE: variation, _EVERY: Pg(of(prefix, form))},
                differs_on=_NO_REGEX,
            )
            for prefix in ("text", "name", "bpchar")
            for suffix, (form, variation) in functions.items()
        ),
    )


def _comparisons(prefix: str, *suffixes: str) -> tuple[Macro, ...]:
    operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
    return tuple(
        _operators(f"{prefix}{suffix}", "a, b", f"a {operators[suffix]} b") for suffix in suffixes
    )


def _pg(name: str, params: str, form: str, differs_on: frozenset[str] = frozenset()) -> Macro:
    """An overload whose form is PostgreSQL's syntax, which sqlglot writes for every dialect."""
    where = _CASTS_CHECKED if MARK in form else _EVERY
    return Macro(
        name, tuple(p for p in params.split(", ") if p), {where: Pg(form)}, differs_on=differs_on
    )


def _udf(name: str, params: str, form: str) -> Macro:
    """An overload that calls a Python UDF of provisa.federation.duckdb_pg_scalars: DuckDB's alone
    until it is written as SQL."""
    return Macro(name, tuple(p for p in params.split(", ") if p), {_DUCKDB: Pg(form)})


def _macro(
    name: str,
    params: str,
    duckdb: str,
    others: Forms | None = None,
    own: Forms | None = None,
    differs_on: frozenset[str] = frozenset(),
) -> Macro:
    """An overload, its DuckDB form, any other dialect's, and any dialect's own function."""
    return Macro(
        name,
        tuple(p for p in params.split(", ") if p),
        {_DUCKDB: duckdb, **(others or {})},
        own or {},
        differs_on=differs_on,
    )


def _operators(name: str, params: str, sql: str, others: Forms | None = None) -> Macro:
    """An overload whose form is the dialect's operators alone."""
    return Macro(name, tuple(params.split(", ")), operators={_DUCKDB: sql, **(others or {})})


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

#: uuid_extract_timestamp, as PostgreSQL 17's uuid.c reads it: a version 7 UUID's milliseconds
#: (DuckDB's own), a version 1 UUID's 100-nanosecond count since 1582-10-15, and NULL for any other
#: version or a variant that is not RFC 9562's. DuckDB's own refuses all but version 7.
_UUID_HEX = "replace(CAST(u AS VARCHAR), '-', '')"
_UUID_TIMESTAMP = (
    f"CASE WHEN (CAST('0x' || substr({_UUID_HEX}, 17, 1) AS INTEGER) & 12) <> 8 THEN NULL "
    "WHEN uuid_extract_version(u) = 7 THEN uuid_extract_timestamp(u) "
    "WHEN uuid_extract_version(u) = 1 THEN TIMESTAMPTZ '1970-01-01 00:00:00+00' + to_microseconds("
    f"((CAST('0x' || substr({_UUID_HEX}, 14, 3) AS BIGINT) << 48) "
    f"| (CAST('0x' || substr({_UUID_HEX}, 9, 4) AS BIGINT) << 32) "
    f"| CAST('0x' || substr({_UUID_HEX}, 1, 8) AS BIGINT)) // 10 - 12219292800000000) END"
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


_QUOTED_IDENTIFIER = (
    "CASE WHEN CAST(value AS TEXT) ~ '^[a-z_][a-z0-9_$]*$' THEN CAST(value AS TEXT) "
    "ELSE '\"' || REPLACE(CAST(value AS TEXT), '\"', '\"\"') || '\"' END"
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

#: An XML value's escaped text, as an element's content or an attribute's value reads it.
_XML_TEXT = (
    "provisa_xml_value(CAST(value AS VARCHAR), typeof(value), CASE WHEN typeof(value) LIKE '%[]' "
    "THEN CAST(to_json(value) AS VARCHAR) END, {content})"
)
_XML_SPACES = "CAST(to_json(spaces) AS VARCHAR)"

#: xmlEncodeSpecialChars, which any dialect's REPLACE can write.
_XML_ESCAPED = (
    "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(value, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), "
    "'\"', '&quot;'), CHR(13), '&#13;')"
)
_COMMENT_REFUSED = (
    "strpos(value, '--') > 0 OR substr(value, length(value)) = '-' THEN fail('invalid XML comment')"
)

#: XML: DuckDB's forms, over the UDFs of provisa.federation.duckdb_pg_xml, and what another dialect
#: can do itself. A dialect with no form is refused by provisa.transpiler.pg_xml.for_dialect before
#: any row is read.
_XML: tuple[Macro, ...] = (
    # A value that needs no reading: what query_to_xml wrote (provisa.compiler.xml_mapping).
    _pg("provisa_xml", "value", "CAST(value AS TEXT)"),
    _macro("provisa_xml_text", "value", _XML_TEXT.format(content="true")),
    _macro("provisa_xml_attribute", "value", _XML_TEXT.format(content="false")),
    _udf("xml_is_well_formed", "value", "provisa_xml_well_formed(value, false)"),
    _udf("xml_is_well_formed_content", "value", "provisa_xml_well_formed(value, false)"),
    _macro(
        "xml_is_well_formed_document",
        "value",
        "provisa_xml_well_formed(value, true)",
        own={
            frozenset(
                {"snowflake"}
            ): "CASE WHEN value IS NOT NULL THEN CHECK_XML(value) IS NULL END"
        },
    ),
    _udf("xml_send", "value", "provisa_xml_send(value)"),
    _macro(
        "xmlcomment",
        "value",
        "provisa_xmlcomment(value)",
        {_TRINO: f"CASE WHEN {_COMMENT_REFUSED} ELSE '<!--' || value || '-->' END"},
    ),
    _macro("xmltext", "value", "provisa_xmltext(value)", {_EVERY: Pg(_XML_ESCAPED)}),
    _udf("xmlconcat2", "a, b", "provisa_xmlconcat2(a, b)"),
    _udf("xmlexists", "path, value", "provisa_xpath_exists(path, value, '[]')"),
    _udf("xpath", "path, value", "provisa_xpath(path, value, '[]')"),
    _macro("xpath", "path, value, spaces", f"provisa_xpath(path, value, {_XML_SPACES})"),
    _udf("xpath_exists", "path, value", "provisa_xpath_exists(path, value, '[]')"),
    _macro(
        "xpath_exists", "path, value, spaces", f"provisa_xpath_exists(path, value, {_XML_SPACES})"
    ),
    _macro(
        "xmlvalidate",
        "value, dtd",
        strict("value, dtd", "BOOLEAN", raising("0A000", "'xmlvalidate is not implemented'")),
    ),
)

#: A shape's functions. Trino's form is its geometry where a geometry function answers
#: (REQ-2171: Trino uses its geometry, not inline arithmetic).
_NOT_OPEN = "NOT starts_with(ltrim(CAST(value AS VARCHAR)), '(')"
_SHAPES: tuple[Macro, ...] = (
    _macro("area", "value", pg_forms.AREA, own={_TRINO: pg_forms.TRINO_AREA}),
    _macro("box_area", "value", pg_forms.BOX_AREA, own={_TRINO: pg_forms.TRINO_BOX_AREA}),
    _macro("path_area", "value", pg_forms.PATH_AREA, own={_TRINO: pg_forms.TRINO_PATH_AREA}),
    _macro("circle_area", "value", pg_forms.CIRCLE_AREA, {_TRINO: pg_forms.CIRCLE_AREA}),
    _macro(
        "center",
        "value",
        pg_forms.CENTER,
        own={_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_CENTER)},
    ),
    _macro(
        "box_center",
        "value",
        pg_forms.BOX_CENTER,
        own={_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_BOX_CENTER)},
    ),
    _macro(
        "circle_center",
        "value",
        pg_forms.CIRCLE_CENTER,
        {_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_CIRCLE_CENTER)},
    ),
    _macro(
        "diagonal",
        "value",
        pg_forms.BOX_DIAGONAL,
        own={_TRINO: strict("value", "VARCHAR", pg_forms.TRINO_BOX_DIAGONAL)},
    ),
    _macro("lseg_length", "value", pg_forms.LSEG_LENGTH, own={_TRINO: pg_forms.TRINO_LSEG_LENGTH}),
    _macro("path_length", "value", pg_forms.PATH_LENGTH, own={_TRINO: pg_forms.TRINO_PATH_LENGTH}),
    _macro(
        "height",
        "value",
        f"abs({pg_forms.number('value', 4)} - {pg_forms.number('value', 2)})",
        own={_TRINO: pg_forms.TRINO_HEIGHT},
    ),
    _macro(
        "width",
        "value",
        f"abs({pg_forms.number('value', 3)} - {pg_forms.number('value', 1)})",
        own={_TRINO: pg_forms.TRINO_WIDTH},
    ),
    _macro("radius", "value", pg_forms.number("value", 3), {_TRINO: pg_forms.number("value", 3)}),
    _macro("isclosed", "value", pg_forms.is_closed("value"), {_TRINO: pg_forms.is_closed("value")}),
    _macro("isopen", "value", _NOT_OPEN, {_TRINO: _NOT_OPEN}),
    *(
        _macro(name, params, form, {_TRINO: form})
        for of in (None, "lseg", "line")
        for name, params, form in (
            (f"{of}_horizontal" if of else "ishorizontal", "value", pg_forms.horizontal(of)),
            (f"{of}_vertical" if of else "isvertical", "value", pg_forms.vertical(of)),
            (f"{of}_parallel" if of else "isparallel", "a, b", pg_forms.parallel(of)),
            (f"{of}_perp" if of else "isperp", "a, b", pg_forms.perpendicular(of)),
        )
    ),
    _macro(
        "ishorizontal",
        "a, b",
        pg_forms.points_differ_by_nothing(2),
        {_TRINO: pg_forms.points_differ_by_nothing(2)},
    ),
    _macro(
        "isvertical",
        "a, b",
        pg_forms.points_differ_by_nothing(1),
        {_TRINO: pg_forms.points_differ_by_nothing(1)},
    ),
    _macro(
        "pt_contained_circle",
        "p, c",
        pg_forms.POINT_IN_CIRCLE,
        own={_TRINO: pg_forms.TRINO_POINT_IN_CIRCLE},
    ),
    _macro(
        "pt_contained_poly",
        "p, poly",
        pg_forms.POINT_IN_POLYGON,
        own={_TRINO: pg_forms.TRINO_POINT_IN_POLYGON},
    ),
)

_TSVECTOR: tuple[Macro, ...] = (
    _macro(
        "tsvector_strip",
        "value",
        pg_forms.TSVECTOR_STRIP,
        {_TRINO: pg_forms.TRINO_TSVECTOR_STRIP},
    ),
    _macro("strip", "value", pg_forms.TSVECTOR_STRIP, {_TRINO: pg_forms.TRINO_TSVECTOR_STRIP}),
    _macro(
        "tsvector_length",
        "value",
        pg_forms.TSVECTOR_LENGTH,
        {_TRINO: pg_forms.TRINO_TSVECTOR_LENGTH},
    ),
    _macro("tsvector_unnest", "value", pg_forms.TSVECTOR_UNNEST),
)

_ABBREVS: tuple[Macro, ...] = (
    _macro("inet_abbrev", "value", pg_forms.INET_ABBREV, {_TRINO: pg_forms.INET_ABBREV}),
    _macro("cidr_abbrev", "value", pg_forms.CIDR_ABBREV, {_TRINO: pg_forms.TRINO_CIDR_ABBREV}),
    # A value whose type the statement does not say is the engine's inet, or a cidr's text.
    _macro(
        "abbrev",
        "value",
        f"CASE typeof(value) WHEN 'INET' THEN {pg_forms.INET_ABBREV} "
        f"ELSE {pg_forms.CIDR_ABBREV} END",
    ),
)

#: Money over its numeric base, for provisa.transpiler.pg_money: no name of PostgreSQL's, so these
#: never read a statement's own call. money_text is the polyfill grid's (polyfill.money).
_MONEY: tuple[Macro, ...] = (
    _macro("money_words", "value", pg_forms.MONEY_WORDS),
    _macro("money_bytes", "value", pg_forms.MONEY_BYTES, own={_TRINO: pg_forms.TRINO_MONEY_BYTES}),
    _macro("money_input", "value", pg_forms.MONEY_INPUT),
)


#: The array operators' forms on Trino, which has none of PostgreSQL's: arraycontains (``@>``) is
#: NULL of a NULL array, false where the second holds a NULL (a NULL element is equal to nothing),
#: and otherwise whether the first holds each of the second's elements (a NULL of the first is
#: equal to none of them, so CONTAINS' NULL there is false).
_ARRAY_OPERATORS_ON_TRINO: dict[str, Forms] = {
    "arraycontains": {
        _TRINO: "CASE WHEN a IS NULL OR b IS NULL THEN NULL "
        "ELSE all_match(b, _x -> _x IS NOT NULL AND coalesce(contains(a, _x), FALSE)) END"
    },
}
#: An array operator the semantic statement writes as PostgreSQL's operator, read on Trino as the
#: row of the table it is: a range's, a jsonb's and a subtyped column's were written before the
#: functions (transpile), so what is left is an array's.
_ARRAY_OPERATOR_ROWS: dict[type[exp.Expr], str] = {exp.ArrayContainsAll: "arraycontains"}


def _operator(form: pg_forms_types.Form) -> Macro:
    """A row of the generated rule: a form that raises on the dialects that can raise it (REQ-2171,
    AN ANSWER THAT DIFFERS IS REFUSED)."""
    tweak = pg_forms_types.DUCKDB_TWEAKS.get(form.name)
    checked = form.raises or form.name in pg_forms_types.OVERFLOW_UNCHECKED
    forms = {_CASTS_CHECKED if checked else _EVERY: Pg(form.sql)}
    return Macro(
        form.name,
        tuple(form.params.split(", ")),
        operators={_DUCKDB: tweak, **forms} if tweak else forms,
    )


def _operated(name: str, of: str) -> Macro:
    """A row whose form is the generated row ``of``'s."""
    form = pg_forms_types.FORMS_BY_NAME[of]
    return replace(_operator(form), name=name)


#: Integers of two widths: each pair's result is the wider of the two.
_INTEGER_PAIRS = (
    ("int24", "SMALLINT", "INTEGER"),
    ("int42", "INTEGER", "SMALLINT"),
    ("int28", "SMALLINT", "BIGINT"),
    ("int82", "BIGINT", "SMALLINT"),
    ("int48", "INTEGER", "BIGINT"),
    ("int84", "BIGINT", "INTEGER"),
)
_FLOAT_PAIRS = (("float48", "FLOAT", "DOUBLE"), ("float84", "DOUBLE", "FLOAT"))
_ARITHMETIC = {"pl": "+", "mi": "-", "mul": "*", "div": "/"}
_ORDER = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}


def _wider(left: str, right: str) -> str:
    return "BIGINT" if "BIGINT" in (left, right) else "INTEGER"


def _integer_pairs() -> tuple[Macro, ...]:
    def compared(operator: str, left: str, right: str) -> str:
        return f"CAST(a AS {left}) {operator} CAST(b AS {right})"

    wider = {"INTEGER": "int4", "BIGINT": "int8"}
    return (
        *(
            # An integer of two widths is its wider type's operator: PostgreSQL's own promotion.
            _operated(f"{prefix}{suffix}", f"{wider[_wider(left, right)]}{suffix}")
            for prefix, left, right in _INTEGER_PAIRS
            for suffix in _ARITHMETIC
        ),
        *(
            _operators(
                f"{prefix}{suffix}",
                "a, b",
                compared(operator, left, right),
                {_EVERY: Pg(compared(operator, left, right))},
            )
            for prefix, left, right in _INTEGER_PAIRS
            for suffix, operator in _ORDER.items()
        ),
    )


def _float_pairs() -> tuple[Macro, ...]:
    """A real and a double precision are double precision's operator, as PostgreSQL promotes."""
    return tuple(
        _operated(f"{prefix}{suffix}", f"float8{suffix}")
        for prefix, _, _ in _FLOAT_PAIRS
        for suffix in (*_ARITHMETIC, *_ORDER)
    )


def _rounded(integer: str) -> str:
    """A float's cast to an integer: rint's, and in that type's range."""
    return pg_forms_types.integer_of(pg_forms_types.rint("value"), integer)


def _numeric_conversions() -> tuple[Macro, ...]:
    """A function named for a number type: its own row reads what the engine holds; a numeric, a
    jsonb, a bit and a "char" have rows of their own, as pg_proc's are, chosen by OVERLOADS."""
    digits = pg_forms.JSONB_DIGITS

    def as_integer(sql_type: str) -> str:
        return pg_forms.jsonb_number(
            pg_forms.numeric_as_integer(digits, sql_type), pg_forms.PG_INTEGERS[sql_type]
        )

    def as_float(sql_type: str) -> str:
        return pg_forms.jsonb_number(
            pg_forms.numeric_as_float(digits, sql_type), pg_forms.PG_FLOATS[sql_type]
        )

    integers = (("int2", "SMALLINT"), ("int4", "INTEGER"), ("int8", "BIGINT"))
    floats = (("float4", "FLOAT"), ("float8", "DOUBLE"))
    own_floats = {"FLOAT": pg_forms.AS_REAL, "DOUBLE": "CAST(value AS DOUBLE)"}
    pg_floats = {"float4": pg_forms.PG_AS_REAL, "float8": "CAST(value AS DOUBLE PRECISION)"}
    return (
        *(
            _macro(
                name,
                "value",
                strict(
                    "value",
                    sql_type,
                    pg_forms.of_any_number(
                        "value", as_integer(sql_type), pg_forms.in_range("value", sql_type)
                    ),
                ),
                {_CASTS_CHECKED: Pg(pg_forms_types.integer_of("value", name))},
            )
            for name, sql_type in integers
        ),
        *(
            _pg(f"{of}to{name[3:] and 'i' + name[3:]}", "value", _rounded(name))
            for of in ("d", "f")
            for name, _ in integers
        ),
        *(
            _macro(
                name,
                "value",
                strict(
                    "value",
                    sql_type,
                    pg_forms.of_any_number("value", as_float(sql_type), own_floats[sql_type]),
                ),
                {_CASTS_CHECKED: Pg(pg_floats[name])},
            )
            for name, sql_type in floats
        ),
        *(
            _macro(
                f"numeric_{name}",
                "value",
                strict("value", sql_type, pg_forms.numeric_as_integer(digits, sql_type)),
                {_CASTS_CHECKED: Pg(pg_forms_types.integer_of("ROUND(value)", name))},
            )
            for name, sql_type in integers
        ),
        *(
            _macro(
                f"numeric_{name}",
                "value",
                strict("value", sql_type, pg_forms.numeric_as_float(digits, sql_type)),
                {_CASTS_CHECKED: Pg(pg_floats[name])},
            )
            for name, sql_type in floats
        ),
        _macro(
            "jsonb_numeric",
            "value",
            strict("value", "VARCHAR", pg_forms.jsonb_number(digits, "numeric")),
        ),
        *(
            _macro(f"jsonb_{name}", "value", strict("value", sql_type, as_integer(sql_type)))
            for name, sql_type in integers
        ),
        *(
            _macro(f"jsonb_{name}", "value", strict("value", sql_type, as_float(sql_type)))
            for name, sql_type in floats
        ),
        _macro(
            "bittoint4", "value", strict("value", "INTEGER", pg_forms.bit_as_integer("INTEGER", 32))
        ),
        _macro(
            "bittoint8", "value", strict("value", "BIGINT", pg_forms.bit_as_integer("BIGINT", 64))
        ),
        _macro("chartoi4", "value", strict("value", "INTEGER", pg_forms.CHAR_AS_INTEGER)),
    )


#: The regression aggregates read only the rows where both arguments are not NULL.
_PAIRED_X = "CASE WHEN y IS NOT NULL THEN x END"
_PAIRED_Y = "CASE WHEN x IS NOT NULL THEN y END"
_SLOPE = f"COVAR_POP(y, x) / VAR_POP({_PAIRED_X})"


def _regression(name: str, form: str, native: frozenset[str] = frozenset()) -> Macro:
    native_call = Pg(f"{name.upper()}(y, x)")
    forms: dict[frozenset[str], str] = {_DUCKDB: native_call, _EVERY: Pg(form)}
    return Macro(name, ("y", "x"), forms, own={native: MAPPED} if native else {})


_REGRESSIONS: tuple[Macro, ...] = (
    _regression("regr_count", "COUNT(CASE WHEN y IS NOT NULL AND x IS NOT NULL THEN 1 END)"),
    _regression("regr_avgx", f"AVG({_PAIRED_X})"),
    _regression("regr_avgy", f"AVG({_PAIRED_Y})"),
    _regression("regr_sxx", f"VAR_POP({_PAIRED_X}) * COUNT({_PAIRED_X})"),
    _regression("regr_syy", f"VAR_POP({_PAIRED_Y}) * COUNT({_PAIRED_Y})"),
    _regression("regr_sxy", f"COVAR_POP(y, x) * COUNT({_PAIRED_X})"),
    _regression(
        "regr_r2",
        f"CASE WHEN VAR_POP({_PAIRED_X}) = 0 THEN NULL WHEN VAR_POP({_PAIRED_Y}) = 0 THEN 1.0 "
        "ELSE POWER(CORR(y, x), 2) END",
    ),
    _regression("regr_slope", f"CASE WHEN VAR_POP({_PAIRED_X}) <> 0 THEN {_SLOPE} END", _TRINO),
    _regression(
        "regr_intercept",
        f"CASE WHEN VAR_POP({_PAIRED_X}) <> 0 "
        f"THEN AVG({_PAIRED_Y}) - {_SLOPE} * AVG({_PAIRED_X}) END",
        _TRINO,
    ),
)

_NUMBERS: tuple[Macro, ...] = (
    *_integer_pairs(),
    *_float_pairs(),
    *_numeric_conversions(),
    _pg("dexp", "x", strict("x", "DOUBLE", pg_forms.DEXP), _CLICKHOUSE),  # engine_refusals: exp
    _pg("dlog1", "x", strict("x", "DOUBLE", pg_forms.DLOG1), _CLICKHOUSE),  # and ln
    _pg("dlog10", "x", strict("x", "DOUBLE", pg_forms.DLOG10)),
    _pg("dsqrt", "x", strict("x", "DOUBLE", pg_forms.DSQRT)),
    _pg("dpow", "x, y", strict("x, y", "DOUBLE", pg_forms.DPOW)),
    _pg("cosd", "x", strict("x", "DOUBLE", pg_forms.COSD)),
    _pg("cotd", "x", strict("x", "DOUBLE", pg_forms.COTD)),
    _pg("asind", "x", strict("x", "DOUBLE", pg_forms.ASIND)),
    _pg("acosd", "x", strict("x", "DOUBLE", pg_forms.ACOSD)),
    _pg("atand", "x", pg_forms.ATAND),
    _pg("atan2d", "x, y", pg_forms.ATAN2D),
    Macro(
        "erf",
        ("x",),
        {_EVERY: Pg(strict("x", "DOUBLE", pg_forms.ERF))},
        own={_CLICKHOUSE: "erf(x)"},
    ),
    Macro(
        "erfc",
        ("x",),
        {_EVERY: Pg(strict("x", "DOUBLE", pg_forms.ERFC))},
        own={_CLICKHOUSE: "erfc(x)"},
    ),
    *(
        Macro(
            name,
            ("x",),
            {_DUCKDB: Pg(f"{name.upper()}(x)"), _EVERY: Pg(form)},
            own={_CLICKHOUSE: MAPPED},
        )
        for name, form in pg_forms.HYPERBOLICS.items()
    ),
    Macro("cot", ("x",), {_DUCKDB: Pg("COT(x)"), _EVERY: Pg(pg_forms.COT)}),
    _pg("log", "x", "LOG10(x)"),
    _pg("log10", "x", "LOG10(x)"),
    _pg("random_normal", "", pg_forms.RANDOM_NORMAL.format(mean="0.0", stddev="1.0")),
    _pg(
        "random_normal",
        "mean",
        strict("mean", "DOUBLE", pg_forms.RANDOM_NORMAL.format(mean="mean", stddev="1.0")),
    ),
    _pg(
        "random_normal",
        "mean, stddev",
        strict(
            "mean, stddev",
            "DOUBLE",
            pg_forms.RANDOM_NORMAL.format(mean="mean", stddev="stddev"),
        ),
    ),
    _pg("trim_scale", "value", strict("value", "TEXT", pg_forms.TRIM_SCALE)),
    _pg("min_scale", "value", strict("value", "INTEGER", pg_forms.MIN_SCALE), _NO_SPLIT_PART),
    *_REGRESSIONS,
    _pg("int8dec", "x", pg_forms_types.integer_of("(CAST(x AS DECIMAL(38, 0)) - 1)", "int8")),
    *(
        Macro(
            f"numeric_{name}",
            ("x",),
            {
                _DUCKDB: Pg(f"{name.upper()}(x)"),
                _EVERY: Pg(f"CAST({name.upper()}(x) AS DECIMAL(38, 0))"),
            },
        )
        for name in ("ceil", "floor", "round", "trunc")
    ),
    _pg("bool_int4", "value", "CASE WHEN value THEN 1 WHEN NOT value THEN 0 END"),
    _pg("float4_avg", "x", "AVG(CAST(x AS DOUBLE PRECISION))"),
    _macro("int2int4_sum", "v", strict("v", "BIGINT", pg_forms.INT2INT4_SUM)),
    _macro("uuid_extract_timestamp", "u", strict("u", "TIMESTAMPTZ", _UUID_TIMESTAMP)),
    _macro("int2vectorsend", "v", strict("v", "BLOB", pg_forms.INT2VECTOR_SEND)),
    _macro(
        "integer_pl_date",
        "n, d",
        strict(
            "n, d",
            "DATE",
            f"CASE WHEN isfinite(d) THEN {pg_forms.DATE_PLUS_DAYS} ELSE d END",
        ),
    ),
)


def _enum_ordered(answer: str) -> str:
    """``answer`` over two values of an enum, in its labels' order. A value the engine holds as
    text has lost that order: compared, it would answer the alphabet's (REQ-2171, AN ANSWER THAT
    DIFFERS IS REFUSED)."""
    refused = raising("0A000", f"'enum order {pg_forms_types.REFUSED}'")
    return f"CASE WHEN typeof(a) = 'VARCHAR' THEN {refused} ELSE {answer} END"


_ENUMS: tuple[Macro, ...] = (
    *(
        _macro(f"enum_{suffix}", "a, b", _enum_ordered(f"a {operator} b"))
        for suffix, operator in _ORDER.items()
    ),
    _macro(
        "enum_cmp",
        "a, b",
        _enum_ordered("CASE WHEN a < b THEN -1 WHEN a > b THEN 1 WHEN a = b THEN 0 END"),
    ),
    _macro("enum_larger", "a, b", _enum_ordered("CASE WHEN a > b THEN a WHEN a <= b THEN b END")),
    _macro("enum_smaller", "a, b", _enum_ordered("CASE WHEN a < b THEN a WHEN a >= b THEN b END")),
    # DuckDB's own, under PostgreSQL's names: only an engine with enum types has a form.
    _macro("enum_first", "a", "enum_first(a)"),
    _macro("enum_last", "a", "enum_last(a)"),
    _macro("enum_range", "a", "enum_range(a)"),
    _macro("enum_range", "a, b", "enum_range_boundary(a, b)"),
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

_BIGQUERY_HAS_IT: Forms = {frozenset({"bigquery"}): MAPPED}
_MAKE_INTERVAL_PARAMS = "years, months, weeks, days, hours, mins, secs"
_DATES: tuple[Macro, ...] = (
    Macro("statement_timestamp", (), {_EVERY: Pg("CURRENT_TIMESTAMP")}),
    _macro("clock_timestamp", "", pg_forms.CLOCK_TIMESTAMP),
    _macro("timeofday", "", pg_forms.TIMEOFDAY),
    *(
        _macro("justify_" + of, "value", strict("value", "INTERVAL", form), own=_BIGQUERY_HAS_IT)
        for of, form in (
            ("hours", pg_forms.JUSTIFY_HOURS),
            ("days", pg_forms.JUSTIFY_DAYS),
            ("interval", pg_forms.JUSTIFY_INTERVAL),
        )
    ),
    _macro(
        "make_interval_fields",
        _MAKE_INTERVAL_PARAMS,
        strict(_MAKE_INTERVAL_PARAMS, "INTERVAL", pg_forms.MAKE_INTERVAL),
    ),
    _pg("overlaps_timestamp", "s1, e1, s2, e2", pg_forms.OVERLAPS),
    _macro(
        "datetime_pl",
        "d, t",
        strict("d, t", "TIMESTAMP", pg_forms.DATETIME_PL),
        {_EVERY: Pg(strict("d, t", "TIMESTAMP", pg_forms.DATE_AND_TIME))},
    ),
    _macro(
        "timedate_pl",
        "t, d",
        strict("d, t", "TIMESTAMP", pg_forms.DATETIME_PL),
        {_EVERY: Pg(strict("d, t", "TIMESTAMP", pg_forms.DATE_AND_TIME))},
    ),
    _macro(
        "isfinite",
        "value",
        "isfinite(value)",
        {_EVERY: Pg("CASE WHEN value IS NOT NULL THEN TRUE END")},
    ),
    _pg("timestamp", "value", "CAST(value AS TIMESTAMP)"),
    _pg("timestamptz_timestamp", "value", "CAST(value AT TIME ZONE 'UTC' AS TIMESTAMP)"),
    *(
        Macro(
            name,
            ("part", "value"),
            {_DUCKDB: Pg("DATE_PART(part, value)"), _EVERY: Pg(pg_forms.date_part_form(name))},
            own={_CLICKHOUSE: pg_forms.clickhouse_date_part()},
        )
        for name in ("date_part", "extract")
    ),
    _macro("datetimetz_pl", "d, t", strict("d, t", "TIMESTAMPTZ", pg_forms.DATETIMETZ_PL)),
    _macro("timetzdate_pl", "t, d", strict("d, t", "TIMESTAMPTZ", pg_forms.DATETIMETZ_PL)),
    _macro(
        "timestamp_scale", "value, n", strict("value, n", "TIMESTAMP", pg_forms.TIMESTAMP_SCALE)
    ),
    _macro(
        "timestamptz_scale",
        "value, n",
        strict("value, n", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_SCALE),
    ),
    _macro("time_scale", "value, n", strict("value, n", "TIME", pg_forms.TIME_SCALE)),
    _macro("timetz", "value, n", strict("value, n", "TIMETZ", pg_forms.TIMETZ_SCALE)),
    _macro("interval_scale", "value, n", strict("value, n", "INTERVAL", pg_forms.INTERVAL_SCALE)),
    _macro("interval_time", "value", strict("value", "TIME", pg_forms.INTERVAL_TIME)),
    _macro("time_interval", "value", strict("value", "INTERVAL", pg_forms.TIME_INTERVAL)),
    _macro(
        "timestamp_time",
        "value",
        strict("value", "TIME", pg_forms.TIMESTAMP_TIME),
        {_EVERY: Pg("CAST(value AS TIME)")},
        differs_on=_CLICKHOUSE,
    ),
    _macro(
        "timestamptz_time",
        "value",
        strict("value", "TIME", pg_forms.TIMESTAMPTZ_TIME),
        {_EVERY: Pg("CAST(value AT TIME ZONE 'UTC' AS TIME)")},
        differs_on=_CLICKHOUSE,
    ),
    _pg("timetz_time", "value", "CAST(value AS TIME)"),
    _pg("time", "value", "CAST(value AS TIME)", _NO_TIME),
    _macro(
        "timestamptz",
        "value",
        strict("value", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_OF),
        {_EVERY: Pg("CAST(value AS TIMESTAMPTZ)")},
        differs_on=_CLICKHOUSE,  # it has no zoned value
    ),
    _macro(
        "timestamptz",
        "d, t",
        strict("d, t", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_OF_DATE_AND_TIME),
    ),
    _macro("timestamptz_timetz", "value", strict("value", "TIMETZ", pg_forms.TIMESTAMPTZ_TIMETZ)),
    _macro("time_timetz", "value", strict("value", "TIMETZ", pg_forms.TIME_TIMETZ)),
    _pg("timetz", "value", "CAST(value AS TIMETZ)", _NO_TIME),
)

#: md5 and the SHA-2 digests (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED). A dialect is listed
#: where replaying postgres:17's answers on its engine matched (Trino 481, Spark 3.5.1, ClickHouse
#: 24.3, MariaDB 11.8, SQL Server 2022, DuckDB); a dialect replay has not reached is refused, as
#: Oracle (its empty string is NULL, so a digest of nothing is NULL) and SQLite (no digest) are.
#: sqlglot's own writing is not an answer: it writes sha384 and sha512 as SHA256 on DuckDB and
#: sha384 as HASH_SHA512 on Exasol, a digest as its hex text on Trino, and HASHBYTES('SHA2_384'),
#: which is NULL, on SQL Server. Athena and Databricks stand with the engines they run.
_TRINOS = frozenset({"trino", "athena"})
_MD5S = _SPARK | {"duckdb", "mysql", "clickhouse"}
#: md5(text) hashes what the dialect holds of the text; SQL Server's nvarchar holds UTF-16, so only
#: a value the statement says is a bytea has PostgreSQL's digest there.
_MD5_FORMS: Forms = {
    _MD5S: Pg("md5(value)"),
    _TRINOS: "lower(to_hex(md5(CAST(value AS VARBINARY))))",
}
#: SHA2 answers the digest's hex text here; PostgreSQL's digest is its bytes.
_SHA2_HEX = _SPARK | {"mysql"}


def _sha(bits: int) -> Macro:
    """sha<bits>, of a bytea: PostgreSQL has no overload of text."""
    extension = f"crypto_hash('sha2-{bits}', CAST(value AS BLOB))"
    forms: dict[frozenset[str], str] = {
        _DUCKDB: "unhex(sha256(CAST(value AS BLOB)))" if bits == 256 else extension,
        _SHA2_HEX: f"unhex(sha2(value, {bits}))",
        frozenset({"clickhouse"}): f"SHA{bits}(value)",
    }
    if bits in (256, 512):  # the digests Trino and SQL Server have
        forms[_TRINOS] = f"sha{bits}(value)"
        forms[frozenset({"tsql"})] = Pg(f"sha{bits}(value)")
    return Macro(f"sha{bits}", ("value",), forms)


_DIGESTS: tuple[Macro, ...] = (
    Macro("md5", ("value",), _MD5_FORMS),
    Macro(
        "md5_bytea",
        ("value",),
        {
            **_MD5_FORMS,
            frozenset({"tsql"}): "LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', value), 2))",
        },
    ),
    *(_sha(bits) for bits in (224, 256, 384, 512)),
    # encode's formats: sqlglot reads the format as a charset, which it writes as TO_UTF8 on Trino.
    Macro(
        "encode_hex",
        ("value",),
        {
            _DUCKDB: "lower(hex(value))",
            _TRINOS: "lower(to_hex(value))",
            _SPARK | {"mysql", "clickhouse"}: "lower(hex(value))",
            frozenset({"tsql"}): "LOWER(CONVERT(VARCHAR(MAX), value, 2))",
        },
    ),
    # base64: PostgreSQL wraps a line at 76 characters, which no engine's own does.
    Macro("encode_base64", ("value",), {_DUCKDB: "to_base64(value)"}),
    Macro("encode_escape", ("value",), {_DUCKDB: "provisa_encode_escape(value)"}),
)
#: The rows whose argument is a bytea: a literal there is bytea's input, not text.
_BYTEA_INPUT = frozenset({"md5_bytea", "sha224", "sha256", "sha384", "sha512"})
#: What a statement says of a value PostgreSQL has no bytea overload for.
_TEXT_TYPES = frozenset({"text", "varchar", "character varying", "bpchar", "char", "character"})


#: PostgreSQL's substr counts positions before the first: a start at or below 0 takes fewer.
_SUBSTR_START = "GREATEST(start, 1)"
#: inet's text always says its mask; the engine's value says it only where it is not the widest.
_INET_TEXT = (
    "CASE WHEN STRPOS(value, '/') > 0 THEN value WHEN STRPOS(value, ':') > 0 "
    "THEN value || '/128' ELSE value || '/32' END"
)


def _hashes() -> tuple[Macro, ...]:
    """Each hash function on each engine with a fold; on any other its call is refused by name."""
    forms: dict[tuple[str, str], dict[frozenset[str], str]] = {}
    for dialect, engine in pg_forms_hash.ENGINES:
        for key, form in pg_forms_hash.forms(engine).items():
            forms.setdefault(key, {})[frozenset({dialect})] = form
    # A composite of no said type has no row of its element's: it is refused by name.
    bare = tuple(
        Macro(name, tuple(params.split(", "))) for name, params in pg_forms_hash.COMPOSITES
    )
    return bare + tuple(
        Macro(name, tuple(params.split(", ")), written) for (name, params), written in forms.items()
    )


_ROWS: tuple[Macro, ...] = (
    *_text_matches(),
    Macro("substr", ("value", "start"), {_EVERY: Pg(f"SUBSTRING(value, {_SUBSTR_START})")}),
    Macro(
        "substr",
        ("value", "start", "n"),
        {
            _EVERY: Pg(
                f"SUBSTRING(value, {_SUBSTR_START}, GREATEST(start + n - {_SUBSTR_START}, 0))"
            )
        },
    ),
    # DuckDB 2.0 refuses a NULL scale when it binds (1.5 answered NULL, as PostgreSQL does).
    Macro(
        "round_numeric",
        ("x", "n"),
        {_DUCKDB: Pg(_NULL_SCALE.format("ROUND")), _EVERY: Pg("ROUND(x, n)")},
    ),
    Macro(
        "trunc_numeric",
        ("x", "n"),
        {_DUCKDB: Pg(_NULL_SCALE.format("TRUNC")), _EVERY: Pg("TRUNC(x, n)")},
    ),
    Macro("inet_text", ("value",), {_EVERY: Pg(strict("value", "TEXT", _INET_TEXT))}),
    # The session's collation, en_US.UTF-8 as pg_database says: DuckDB compares bytes.
    _macro("max_collated", "value", "arg_max(value, provisa_en_us_key(value, 'max'))"),
    _macro("min_collated", "value", "arg_min(value, provisa_en_us_key(value, 'min'))"),
    _macro("to_number", "value, format", "provisa_to_number(value, format)"),
    Macro("current_query", ()),  # the engine's is the statement as Provisa wrote it
    _macro("to_hex", "value", f"format('{{:x}}', {_TWOS_COMPLEMENT})"),
    _macro("pg_to_hex", "value", f"format('{{:x}}', {_TWOS_COMPLEMENT})"),  # as readable writes it
    _macro("netmask", "value", "host(netmask(value))"),
    # DuckDB's broadcast of an IPv4 address of the high bit holds it as a negative integer, which
    # is no address; its text is the address, read again.
    _macro(
        "broadcast",
        "value",
        "CAST(CAST(broadcast(value) AS VARCHAR) AS INET)",
        own={_EVERY_BUT_DUCKDB: MAPPED},
    ),
    _macro(
        "uuid_extract_version",
        "value",
        "CASE WHEN substr(CAST(value AS VARCHAR), 20, 1) IN ('8', '9', 'a', 'b') "
        "THEN uuid_extract_version(value) END",
    ),
    _macro("array_remove", "a, v", "list_filter(a, _x -> _x IS DISTINCT FROM v)"),
    _pg("like", "a, b", _LIKE, _NO_CASE_SENSITIVE_LIKE),
    _pg("notlike", "a, b", f"NOT {_LIKE}", _NO_CASE_SENSITIVE_LIKE),
    _pg("textcat", "a, b", "CAST(a || b AS TEXT)"),
    _pg("byteacat", "a, b", "CAST(a || b AS BYTEA)"),
    _udf("bytealike", "a, b", "provisa_bytea_like(a, b)"),
    _udf("byteanlike", "a, b", "NOT provisa_bytea_like(a, b)"),
    _pg("bitcat", "a, b", "CAST(CAST(a AS TEXT) || CAST(b AS TEXT) AS BIT)"),
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
    _pg("quote_ident", "value", _QUOTED_IDENTIFIER),
    Macro(
        "repeat",
        ("value", "n"),
        {
            _TRINO: pg_forms_system.TRINO_REPEAT,
            _EVERY: Pg(
                "CASE WHEN n > 0 THEN REPEAT(value, n) "
                "WHEN n IS NOT NULL AND value IS NOT NULL THEN '' END"
            ),
        },
    ),
    _pg("textanycat", "a, b", "CAST(CAST(a AS TEXT) || CAST(b AS TEXT) AS TEXT)"),
    _pg("anytextcat", "a, b", "CAST(CAST(a AS TEXT) || CAST(b AS TEXT) AS TEXT)"),
    _pg("text", "value", "CAST(value AS TEXT)"),
    _pg("name", "value", "LEFT(CAST(value AS TEXT), 63)"),
    Macro(
        "ascii",
        ("value",),
        {_EVERY: Pg("ASCII(value)")},
        own={
            _TRINO: "CASE WHEN value = '' THEN 0 ELSE codepoint(cast(substr(value, 1, 1) AS varchar(1))) END"
        },
    ),
    Macro(
        "octet_length",
        ("value",),
        {_EVERY: Pg("OCTET_LENGTH(value)")},
        own={_TRINO: "length(to_utf8(value))"},
    ),
    Macro(
        "bit_length",
        ("value",),
        {
            # A bit string's length is its bits; a text's and a bytea's, their bytes times 8.
            _DUCKDB: "CASE WHEN typeof(value) = 'BIT' THEN bit_length(TRY_CAST(value AS BIT)) "
            "ELSE 8 * OCTET_LENGTH(value) END",
            _EVERY: Pg("8 * OCTET_LENGTH(value)"),
        },
        own={_TRINO: "8 * length(to_utf8(value))", _CLICKHOUSE: "8 * length(value)"},
    ),
    _pg("quote_literal", "value", _PG_QUOTED_LITERAL),
    _pg(
        "quote_nullable",
        "value",
        f"CASE WHEN value IS NULL THEN 'NULL' ELSE {_PG_QUOTED_LITERAL} END",
    ),
    _pg("dcbrt", "x", "CBRT(x)"),
    _macro(
        "dround",
        "x",
        "round_even(CAST(x AS DOUBLE), 0)",
        {_EVERY: Pg(pg_forms_types.rint("x"))},
    ),
    _pg("dtrunc", "x", "TRUNC(CAST(x AS DOUBLE PRECISION))"),
    _macro(
        "sind",
        "x",
        "CASE WHEN isnan(CAST(x AS DOUBLE)) THEN CAST(x AS DOUBLE) ELSE provisa_sind(x) END",
        {_CASTS_CHECKED: Pg(strict("x", "DOUBLE", pg_forms.SIND))},
    ),
    _macro(
        "tand",
        "x",
        "CASE WHEN isnan(CAST(x AS DOUBLE)) THEN CAST(x AS DOUBLE) ELSE provisa_tand(x) END",
        {_CASTS_CHECKED: Pg(strict("x", "DOUBLE", pg_forms.TAND))},
    ),
    _pg("every", "x", "BOOL_AND(x)"),
    _pg(
        "scale",
        "x",
        "CASE WHEN x IS NULL THEN CAST(NULL AS INTEGER) "
        "WHEN STRPOS(CAST(x AS TEXT), '.') > 0 "
        "THEN CAST(LENGTH(SPLIT_PART(CAST(x AS TEXT), '.', 2)) AS INTEGER) ELSE 0 END",
        _NO_SPLIT_PART,
    ),
    _macro("to_bin", "value", f"format('{{:b}}', {_TWOS_COMPLEMENT})"),
    _macro("to_oct", "value", f"format('{{:o}}', {_TWOS_COMPLEMENT})"),
    _udf("pg_size_bytes", "size", "provisa_size_bytes(size)"),
    _macro(
        "pg_convert",
        "value, source, destination",
        "provisa_convert(value, source, destination)",
    ),
    _udf("pg_json_object", "value", "CAST(provisa_json_object(value, NULL) AS JSON)"),
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
    _udf("regexp_match", "value, pattern", "provisa_regexp_match(value, pattern, '')"),
    _udf("regexp_match", "value, pattern, flags", "provisa_regexp_match(value, pattern, flags)"),
    *(
        Macro(
            "regexp_substr",
            params,
            {_DUCKDB: f"provisa_regexp_substr(value, pattern, {rest})"},
            others,
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
    *(
        # sqlglot's DuckDB regexp_instr reads no endoption, flags or subexpression. Every other
        # dialect keeps sqlglot's own, and the federation engines their refusal (engine_refusals).
        Macro(
            "regexp_instr",
            params,
            {_DUCKDB: f"provisa_regexp_instr(value, pattern, {rest})"},
            {_EVERY_BUT_DUCKDB: MAPPED},
            differs_on=_TRINO | _CLICKHOUSE,
        )
        for params, rest in (
            (("value", "pattern"), "1, 1, 0, '', 0"),
            (("value", "pattern", "start"), "start, 1, 0, '', 0"),
            (("value", "pattern", "start", "n"), "start, n, 0, '', 0"),
            (("value", "pattern", "start", "n", "endoption"), "start, n, endoption, '', 0"),
            (
                ("value", "pattern", "start", "n", "endoption", "flags"),
                "start, n, endoption, flags, 0",
            ),
            (
                ("value", "pattern", "start", "n", "endoption", "flags", "subexpr"),
                "start, n, endoption, flags, subexpr",
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
    _udf("similar_escape", "pattern, esc", "provisa_similar_escape(pattern, esc)"),
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
    _udf("is_normalized", "value, form", "provisa_is_normalized(value, form)"),
    _udf("unicode_assigned", "value", "provisa_unicode_assigned(value)"),
    Macro("unicode_version", (), {_EVERY: Pg("'15.1'")}),
    Macro("icu_unicode_version", (), {_EVERY: Pg("'16.0'")}),  # postgres:17's ICU
    _udf("unistr", "value", "provisa_unistr(value)"),
    # The database's encoding is UTF8, which to_ascii refuses; a named single-byte one it strips.
    _udf("to_ascii", "value", "provisa_to_ascii(value, 'UTF8')"),
    _macro("to_ascii", "value, encoding", "provisa_to_ascii(value, CAST(encoding AS VARCHAR))"),
    _udf("parse_ident", "value", "provisa_parse_ident(value, true)"),
    _udf("parse_ident", "value, strict_", "provisa_parse_ident(value, strict_)"),
    *_ABBREVS,
    _pg(
        "nameconcatoid",
        "name, oid",
        "CAST(LEFT(name, 63 - LENGTH('_' || CAST(oid AS TEXT))) || '_' || CAST(oid AS TEXT) AS TEXT)",
    ),
    _macro("pg_typeof", "value", _PG_TYPEOF),
    *_SHAPES,
    *_TSVECTOR,
    *(
        Macro("mul_d_interval", ("b", "a"), operators={_DUCKDB: Pg(form.sql)})
        for form in pg_forms_types.OPERATORS
        if form.name == "interval_mul"
    ),
    _macro(
        "oidvectorsend",
        "v",
        "(SELECT CASE WHEN v IS NOT NULL THEN unhex("
        "printf('%08x%08x%08x%08x%08x', 1, 0, 26, len(_v), 0) || "
        "array_to_string(list_transform(_v, _o -> printf('00000004%08x', CAST(_o AS BIGINT))), "
        "'')) END FROM (SELECT list_filter(string_split(v, ' '), _o -> _o <> '') AS _v))",
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
    # A character value of a "char".
    _pg("bpchar", "value", "CAST(value AS TEXT)"),
    *_NUMBERS,
    *_DATES,
    *_ENUMS,
    *(Macro(name, params, forms) for name, params, forms in pg_forms_system.ROWS),
    *(Macro(name, params, forms) for name, params, forms in pg_forms_enum.ROWS),
    *_OTHERS,
    *_XML,
    *_MONEY,
    *VALUE_FUNCTIONS_OF_THE_SERVER,
    *(Macro(name, params, {_EVERY: Pg(form)}) for name, params, form in VALUE_FORMS),  # REQ-2190
    *(Macro(name, params, forms) for name, params, forms in pg_forms_json.ROWS),
    *(Macro(name, params, forms) for name, params, forms in pg_forms_textsearch.ROWS),
    *(Macro(name, params, forms) for name, params, forms in pg_forms_type_io.ROWS),
    *_DIGESTS,
    *map(_operator, pg_forms_types.OPERATORS),
    *(
        Macro(
            form.name,
            tuple(form.params.split(", ")),
            operators={_DUCKDB: Pg(form.sql), **_ARRAY_OPERATORS_ON_TRINO.get(form.name, {})},
        )
        for form in pg_forms_types.ARRAY_OPERATORS
    ),
)

#: Where an engine's own function, given these, answers in other digits or another type than
#: PostgreSQL's exact numeric (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): DuckDB's forms read
#: the value's digits (provisa.federation.duckdb_pg_numeric), and no other dialect has a form.
_NUMERIC_TEXT = "CAST(a0 AS VARCHAR)"
_SECOND_NUMERIC = "CAST(a1 AS VARCHAR)"
_AVG = "provisa_numeric_quotient(CAST(sum(a0) AS VARCHAR), CAST(count(a0) AS VARCHAR))"
_WHOLE_SQUARES = "sum(CAST(a0 AS HUGEINT) * CAST(a0 AS HUGEINT))"


def _exact_unary(function: str) -> str:
    return f"provisa_numeric_unary('{function}', {_NUMERIC_TEXT})"


def _exact_binary(function: str) -> str:
    return f"provisa_numeric_binary('{function}', {_NUMERIC_TEXT}, {_SECOND_NUMERIC})"


def _spread(function: str, squares: str) -> str:
    return (
        f"provisa_numeric_spread('{function}', count(a0), CAST(sum(a0) AS VARCHAR), "
        f"CAST({squares} AS VARCHAR))"
    )


_SPREADS = ("stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp")
_EXACT_ROWS: tuple[Macro, ...] = (
    Macro("exact_avg", ("a0",), {_DUCKDB: _AVG}),
    *(Macro(f"exact_{n}", ("a0",), {_DUCKDB: _spread(n, _WHOLE_SQUARES)}) for n in _SPREADS),
    *(
        Macro(f"exact_{n}_numeric", ("a0",), {_DUCKDB: _spread(n, "sum(a0 * a0)")})
        for n in _SPREADS
    ),
    *(
        Macro(f"exact_{n}", ("a0",), {_DUCKDB: _exact_unary(n)})
        for n in ("exp", "ln", "log10", "sqrt")
    ),
    Macro("exact_log", ("a0",), {_DUCKDB: _exact_unary("log10")}),
    Macro("exact_log", ("a0", "a1"), {_DUCKDB: _exact_binary("log")}),
    Macro("exact_power", ("a0", "a1"), {_DUCKDB: _exact_binary("power")}),
    Macro("exact_pow", ("a0", "a1"), {_DUCKDB: _exact_binary("power")}),
    Macro("exact_div", ("a0", "a1"), {_DUCKDB: _exact_binary("div")}),
    Macro("exact_float8_numeric", ("a0",), {_DUCKDB: "provisa_numeric_of_float(a0, 15)"}),
    Macro("exact_float4_numeric", ("a0",), {_DUCKDB: "provisa_numeric_of_float(a0, 6)"}),
    Macro("exact_pg_size_pretty", ("a0",), {_DUCKDB: _exact_unary("pg_size_pretty")}),
    # interval_avg cascades a month's remainder, and DuckDB has no sum of an interval.
    Macro("exact_avg_interval", ("a0",)),
    # What regexp_replace's Nth match is takes the engine's own search.
    Macro(
        "regexp_replace_at",
        ("value", "pattern", "replacement", "start"),
        {
            _DUCKDB: "CASE WHEN contains(pattern, '^') OR start < 1 THEN "
            + pg_forms_types.refused(
                "0A000", f"'regexp_replace {pg_forms_types.REFUSED}'", "VARCHAR"
            )
            + " ELSE left(value, start - 1) || "
            "regexp_replace(substring(value, start), pattern, replacement) END"
        },
    ),
)
#: The overloads of the exact rows, by what the statement says of their first argument.
_EXACT_OVERLOADS: dict[tuple[str, int, str], str] = {
    **{("avg", 1, of): "exact_avg" for of in ("smallint", "int", "bigint", "decimal")},
    ("avg", 1, "interval"): "exact_avg_interval",
    **{(n, 1, of): f"exact_{n}" for n in _SPREADS for of in ("smallint", "int", "bigint")},
    **{(n, 1, "decimal"): f"exact_{n}_numeric" for n in _SPREADS},
    **{(n, 1, "decimal"): f"exact_{n}" for n in ("exp", "ln", "log10", "sqrt", "log")},
    **{(n, 2, "decimal"): f"exact_{n}" for n in ("log", "power", "pow", "div")},
    ("numeric", 1, "double precision"): "exact_float8_numeric",
    ("numeric", 1, "real"): "exact_float4_numeric",
    ("pg_size_pretty", 1, "decimal"): "exact_pg_size_pretty",
}

#: A row replay found another answer of, by the dialects it is refused on.
_DIFFERS_ON: dict[str, frozenset[str]] = {
    # Two of its times differ by seconds, not an interval.
    "time_mi_time": _CLICKHOUSE,
    # Its decimals overflow on the form's modulus and its digits of a pg_lsn are not read back.
    "numeric_pl_pg_lsn": _CLICKHOUSE,
    "pg_lsn_mii": _CLICKHOUSE,
    "pg_lsn_pli": _CLICKHOUSE,
    # Two timestamps differ by seconds there, and by Trino's own text of an interval.
    "timestamp_mi": _CLICKHOUSE | _TRINO,
    "timestamptz_mi": _CLICKHOUSE | _TRINO,
    # T-SQL has no array, and reads a form's a[1] as a name in brackets.
    **dict.fromkeys(pg_forms_aggregate.ARRAY_INPUT, frozenset({"tsql"})),
}
#: A dialect's form where the generated one's arithmetic does not hold: Int64 wraps as the sum does.
_WRAPPING_SUM = Pg(
    "CASE WHEN a IS NULL THEN CAST(b AS BIGINT) WHEN b IS NULL THEN CAST(a AS BIGINT) "
    "ELSE CAST(a AS BIGINT) + CAST(b AS BIGINT) END"
)
_ENGINE_FORMS: dict[str, Forms] = {
    "int2_sum": {_CLICKHOUSE: _WRAPPING_SUM},
    "int4_sum": {_CLICKHOUSE: _WRAPPING_SUM},
}


def _with_replay(macro: Macro) -> Macro:
    differs = macro.differs_on | _DIFFERS_ON.get(macro.name, frozenset())
    forms = {**_ENGINE_FORMS.get(macro.name, {}), **macro.operators}
    return replace(macro, differs_on=differs, operators=forms)


#: The rows of no form: what a dialect's answer differs in and no form writes.
REFUSED_ROWS = frozenset(
    {
        ("exact_avg_interval", 1),
        ("current_query", 0),
        *((name, len(params.split(", "))) for name, params in pg_forms_hash.COMPOSITES),
        # A shape's name of one argument whose type the statement does not say: no overload.
        *((name, 1) for name, _ in pg_forms_geometric.BARE),
    }
)


def _geometric() -> tuple[Macro, ...]:
    """Each geometric row, and its plain name, on each engine of pg_forms_geometric."""
    rows: dict[tuple[str, str], dict[frozenset[str], str]] = {}
    for engine in pg_forms_geometric.ENGINES:
        for key, form in pg_forms_geometric.forms(engine).items():
            rows.setdefault(key, {})[frozenset({engine.name})] = form
    plain = {
        alias: rows[next(key for key in rows if key[0] == row)]
        for alias, row in pg_forms_geometric.ALIASES.items()
    }
    bare = (Macro(n, (p,)) for n, p in pg_forms_geometric.BARE)
    return (
        *bare,
        *(Macro(n, tuple(p.split(", ")), f) for (n, p), f in {**rows, **plain}.items()),
    )


_ROWS += (
    *_hashes(),
    *(_operator(form) for form in pg_forms_aggregate.STATE_FUNCTIONS),
    *(_operator(form) for form in pg_forms_aggregate.FINAL_FUNCTIONS),
    *(_operator(form) for form in pg_forms_index.SUPPORT_FUNCTIONS),
    *(
        Macro(name, tuple(params.split(", ")), {frozenset({d}): sql for d, sql in forms.items()})
        for (name, params), forms in pg_forms_index.per_engine().items()
    ),
)
#: A call's name and argument count -> its overload: the one table.
MACROS: dict[tuple[str, int], Macro] = {
    (m.name, len(m.params)): _with_replay(m) for m in (*_ROWS, *_EXACT_ROWS, *_geometric())
}
#: (function, argument count, the type the statement says of its first argument) -> its row:
#: PostgreSQL's own name for the overload. The operation is chosen by the type, not the engine's.
OVERLOADS: dict[tuple[str, int, str], str] = {
    **pg_forms_system.OVERLOADS,
    ("area", 1, "box"): "box_area",
    ("area", 1, "path"): "path_area",
    ("area", 1, "circle"): "circle_area",
    ("center", 1, "box"): "box_center",
    ("center", 1, "circle"): "circle_center",
    ("length", 1, "lseg"): "lseg_length",
    ("length", 1, "path"): "path_length",
    ("length", 1, "tsvector"): "tsvector_length",
    ("unnest", 1, "tsvector"): "tsvector_unnest",
    ("strip", 1, "tsvector"): "tsvector_strip",
    ("abbrev", 1, "inet"): "inet_abbrev",
    ("abbrev", 1, "cidr"): "cidr_abbrev",
    ("set_masklen", 2, "cidr"): "cidr_set_masklen",
    **{
        (name, 1, of): f"{of_row}_{name}"
        for name in ("int2", "int4", "int8", "float4", "float8")
        for of, of_row in (("decimal", "numeric"), ("jsonb", "jsonb"))
    },
    ("numeric", 1, "jsonb"): "jsonb_numeric",
    **{
        (name, 1, of): f"{row}to{'i' + name[3:]}"
        for name in ("int2", "int4", "int8")
        for of, row in (("double precision", "d"), ("float", "f"), ("real", "f"))
    },
    ("int4", 1, "boolean"): "bool_int4",
    ("avg", 1, "real"): "float4_avg",
    ("avg", 1, "float"): "float4_avg",
    ("round", 1, "double precision"): "dround",
    ("trunc", 1, "double precision"): "dtrunc",
    **{(name, 1, "decimal"): f"numeric_{name}" for name in ("ceil", "floor", "round", "trunc")},
    ("md5", 1, "bytea"): "md5_bytea",
    **pg_forms_geometric.OVERLOADS,
    **pg_forms_hash.OVERLOADS,
    ("int4", 1, "bit"): "bittoint4",
    ("int8", 1, "bit"): "bittoint8",
    ("int4", 1, '"char"'): "chartoi4",
    ("time", 1, "interval"): "interval_time",
    ("time", 1, "timestamp"): "timestamp_time",
    ("time", 1, "timestamptz"): "timestamptz_time",
    ("time", 1, "timetz"): "timetz_time",
    ("time", 2, "time"): "time_scale",
    ("timestamp", 2, "date"): "datetime_pl",
    ("timestamp", 1, "timestamptz"): "timestamptz_timestamp",
    ("time", 2, "date"): "datetime_pl",
    ("timestamp", 2, "timestamp"): "timestamp_scale",
    ("timestamptz", 2, "timestamptz"): "timestamptz_scale",
    ("timetz", 1, "timestamptz"): "timestamptz_timetz",
    ("timetz", 1, "time"): "time_timetz",
    ("interval", 1, "time"): "time_interval",
    ("interval", 2, "interval"): "interval_scale",
    **_EXACT_OVERLOADS,
    ("round", 1, "real"): "dround",
    ("round", 2, "decimal"): "round_numeric",
    ("trunc", 2, "decimal"): "trunc_numeric",
    ("text", 1, "inet"): "inet_text",
    **{
        (name, 1, of): f"{name}_collated"
        for name in ("max", "min")
        for of in ("text", "char", "bpchar", "varchar")
    },
    **{
        (name, arity, of): f"{of}_{suffix}"
        for of in ("lseg", "line")
        for name, arity, suffix in (
            ("ishorizontal", 1, "horizontal"),
            ("isvertical", 1, "vertical"),
            ("isparallel", 2, "parallel"),
            ("isperp", 2, "perp"),
        )
    },
}
#: The calls of no row of the table, which ``inline`` writes itself.
_COUNTS_NAMES = (
    "num_nulls", "num_nonnulls", "rank", "dense_rank", "percent_rank", "cume_dist",
    "percentile_cont", "percentile_disc", "ntile", "mode",
)  # fmt: skip
#: An overload's row -> the function a statement called, which names its column.
_CALLED = {
    **{row: name for (name, _, _), row in OVERLOADS.items()},
    **{row: "encode" for row in ("encode_hex", "encode_base64", "encode_escape")},
    "regexp_replace_at": "regexp_replace",
    "pg_to_hex": "to_hex",
}
#: How a statement already written through sqlglot spells a row's call, and how a client's spells
#: one pg_syntax reads by its routine's name (overlaps, overlaps_timestamp's).
_SPELLED = {
    "encode", "substring", "regexp_replace", "array_concat", "to_number",
    "date_trunc", "substr", "left", "right", "overlaps",
}  # fmt: skip
_NAMES = (
    frozenset(name for name, _ in MACROS)
    | {name for name, _, _ in OVERLOADS}
    | _SPELLED
    | set(_COUNTS_NAMES)
    | engine_refusals.NAMES
    | polyfill.GRID.names
)
_MENTIONED = re.compile(
    r"\b("
    + "|".join(sorted(_NAMES | pg_forms_textsearch.NAMES))
    + r")\"?\s*\(|"
    + f"{pg_forms_json.OPERATORS}|{pg_forms_textsearch.OPERATORS}"
    + "".join(f"|{mentioned}" for mentioned in polyfill.GRID.mentions),
    re.IGNORECASE,
)
#: A call by pg_catalog, or a value of a type a call of it may be refused for.
_QUALIFIED = re.compile(r"\bpg_catalog\s*\.\s*\"?\w+\"?\s*\(|::\s*interval\b", re.IGNORECASE)
#: The names provisa.pgwire.catalog answers where a statement reads no table.
SERVER_VALUE_NAMES = frozenset(m.name for m in VALUE_FUNCTIONS_OF_THE_SERVER)


#: Functions whose overloads differ by a type every dialect here holds as the same text: only the
#: statement can tell them apart. An unknown-typed literal is the first, as PostgreSQL resolves it.
_TOLD_ONLY_BY_THE_STATEMENT = {"set_masklen": frozenset({"inet", "cidr"})}


def _is_told(name: str, args: list[exp.Expr]) -> bool:
    """Whether a call says which overload it means; a column of cidr does not, and the inet row
    would keep what its mask clears (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED)."""
    if name in pg_forms_system.OF_A_STORED_VALUE:
        return args[0].find(exp.Column) is None
    types = _TOLD_ONLY_BY_THE_STATEMENT.get(name)
    if types is None:
        return True
    return said_type(args[0]) in types or isinstance(args[0].unnest(), (exp.Literal, exp.Null))


def said_type(node: exp.Expr) -> str | None:
    """The PostgreSQL type the statement says an argument is; None where it does not."""
    if isinstance(node, exp.Paren):
        return said_type(node.this)
    if isinstance(node, exp.Column):
        said = values_cast(node)
        return None if said is None else said_type(said)
    return node.to.sql(dialect="postgres").lower() if isinstance(node, exp.Cast) else None


#: The rows of a type whose NULL answers NULL, whatever digits the function would give a value.
_ANSWERED_BY_A_NULL = frozenset(m.name for m in _EXACT_ROWS)
_COLLATED = frozenset({"max_collated", "min_collated"})


def _bucket_said(value: exp.Expr) -> str | None:
    """What width_bucket's resolution reads of an argument; an untyped number's literal says it."""
    node = value.unnest()
    node = node.this if isinstance(node, exp.Neg) else node
    written = isinstance(node, exp.Literal) and not node.is_string and not node.is_int
    return said_type(value) or ("decimal" if written else None)


def _bucket_row(args: list[exp.Expr]) -> str:
    """width_bucket's row: PostgreSQL picks numeric or float8 from all three values, and float8 of
    integers. What no statement says of a column, the engine's type answers in the row of the name."""
    if len(args) == 2:
        nested = isinstance(args[1].unnest(), exp.Array) and any(
            isinstance(item, exp.Array) for item in args[1].unnest().expressions
        )
        return (
            pg_forms_geometric.OVERLOADS[("width_bucket", 2, pg_forms_geometric.NESTED)]
            if (nested)
            else "width_bucket"
        )
    said = {_bucket_said(a) for a in args[:3]}
    if said & {"double precision", "real"}:
        return "width_bucket_float8"
    if any(s and s.startswith("decimal") for s in said if s):
        told = all(a.find(exp.Column) is None for a in args[:3]) or any(
            (said_type(a) or "").startswith("decimal") for a in args[:3]
        )
        if told:
            return "width_bucket_numeric"
    return "width_bucket" if any(a.find(exp.Column) for a in args[:3]) else "width_bucket_float8"


def overload(name: str, args: list[exp.Expr]) -> str:
    """The row a call reads: the overload of the type its first argument is said to be, else the
    row of its own name."""
    if name == "width_bucket" and len(args) in (2, 4):
        return _bucket_row(args)
    said = said_type(args[0]) if args else None
    if said is None:
        return name
    row = OVERLOADS.get((name, len(args), said.split("(")[0]), name)
    if row in _ANSWERED_BY_A_NULL and isinstance(args[0].unnest().this, exp.Null):
        return name
    if row in _COLLATED and args[0].find(exp.Column) is None:
        return name  # one value, which no order changes
    return row


def outcome(name: str, arity: int, dialect: str) -> str:
    """What a call does on ``dialect``: native, mapped, emulated or refused."""
    step, _ = MACROS[(name, arity)].resolved(dialect)
    if dialect in NATIVE:
        return "native"
    if step is Step.REFUSED:
        return "refused"
    return "mapped" if step is Step.OWN else "emulated"


def mentioned(sql: str, dialect: str) -> bool:
    """Whether ``sql`` may call a function of the table that ``dialect`` does not have itself, or
    names one by pg_catalog; a statement that does neither is not parsed."""
    if dialect in NATIVE:
        return False
    return _MENTIONED.search(sql) is not None or _QUALIFIED.search(sql) is not None


@functools.lru_cache(maxsize=2048)
def _parsed(name: str, arity: int, dialect: str) -> exp.Expr:
    _, form = MACROS[(name, arity)].resolved(dialect)
    assert isinstance(form, str)
    parsed = sqlglot.parse_one(form, read="postgres" if isinstance(form, Pg) else dialect)
    return parsed.transform(_thrown) if dialect == "clickhouse" else parsed


def _thrown(node: exp.Expr) -> exp.Expr:
    """A refusal on ClickHouse: it reads a bad text cast to a Nullable as NULL, and folds a cast
    of a constant in every branch of a CASE, where throwIf is left to the branch that runs. It has
    no NULL of an array either, and a column of one is never NULL."""
    if not isinstance(node, exp.Cast):
        return node
    text = node.this
    if isinstance(text, exp.Null) and node.to.is_type("array"):
        return exp.Cast(this=exp.Array(expressions=[]), to=node.to)
    if not isinstance(text, exp.Cast) or not text.is_type(*exp.DataType.INTEGER_TYPES):
        return node
    if MARK not in text.this.sql():
        return node
    return exp.Cast(
        this=exp.Anonymous(this="throwIf", expressions=[exp.Literal.number(1), text.this]),
        to=node.to,
    )


#: An argument that needs no parentheses wherever a form writes it.
_ATOMIC = (exp.Column, exp.Literal, exp.Func, exp.Cast, exp.Paren, exp.Null, exp.Boolean)


#: The types a row of an overload reads as their text, which is how a dialect without them holds
#: them: once the type has chosen the row, a cast to it says nothing more.
_HELD_AS_TEXT = frozenset(
    {
        "point", "line", "lseg", "box", "path", "polygon", "circle", "tsvector", "inet", "cidr",
        "macaddr", "macaddr8", "pg_lsn", "cid", "record", "cstring",
    }
)  # fmt: skip


def _argument(value: exp.Expr, held: bool = False) -> exp.Expr:
    size = _padded_to(value) if held else None
    if said_type(value) in _HELD_AS_TEXT or (held and isinstance(value.unnest(), exp.Cast)):
        value = value.unnest().this
    if size is not None:  # bpchar's input pads to its length, which the text does not hold
        value = exp.Anonymous(this="rpad", expressions=[value, size, exp.Literal.string(" ")])
    return value if isinstance(value, _ATOMIC) else exp.Paren(this=value)


def _padded_to(value: exp.Expr) -> exp.Expr | None:
    """The length a cast to character(n) says."""
    node = value.unnest()
    if isinstance(node, exp.Cast) and node.to.is_type("char"):
        # character alone is character(1).
        return node.to.expressions[0].this if node.to.expressions else exp.Literal.number(1)
    return None


def _in_a_subquery(form: exp.Expr, params: tuple[str, ...], args: list[exp.Expr]) -> exp.Expr:
    """``form`` over its parameters as the columns of a row of its arguments: a lambda cannot read
    a subquery, and a form may be one."""
    row = exp.select(*(exp.alias_(a, p) for p, a in zip(params, args)))
    return exp.select(form).from_(row.subquery("_args")).subquery()


#: What answers differently each time it is evaluated (PostgreSQL's pg_proc.provolatile).
_VOLATILE = frozenset(
    {
        "array_sample", "array_shuffle", "clock_timestamp", "currval", "gen_random_uuid", "lastval",
        "nextval", "random", "random_normal", "setval", "timeofday",
    }
)  # fmt: skip


def _is_volatile(value: exp.Expr) -> bool:
    return any(
        isinstance(node, (exp.Rand, exp.Uuid))
        or (isinstance(node, exp.Anonymous) and node.name.lower() in _VOLATILE)
        for node in value.walk()
    )


def _repeats_volatile(named: list[str], params: tuple[str, ...], args: list[exp.Expr]) -> bool:
    """Whether a form names twice (``named``: each parameter it names, as often as it does) a
    parameter whose argument differs each time it is evaluated."""
    return any(named.count(p) > 1 and _is_volatile(a) for p, a in zip(params, args))


def _held_once(
    form: exp.Expr, params: tuple[str, ...], args: list[exp.Expr], node: exp.Expr, name: str,
    dialect: str,
) -> exp.Expr:  # fmt: skip
    """``form`` over a row of its arguments, each evaluated once. A row of arguments naming no
    column is the same row for every row of a FROM, so that call is refused by name rather than
    answered with one value for all of them."""
    select = node.find_ancestor(exp.Select)
    reads_rows = select is not None and select.args.get("from_") is not None
    if reads_rows and not any(a.find(exp.Column) is not None for a in args):
        raise FunctionRefused(name, dialect)
    return _in_a_subquery(form, params, args)


def _written(macro: Macro, args: list[exp.Expr], dialect: str, node: exp.Expr) -> exp.Expr:
    """The call's form with each parameter its argument. An argument is written wherever the form
    names it, and evaluated each time; one that differs each time is held once (REQ-2171, REWRITE
    AFTER ROUTING). A row of arguments naming no column is the same row for every row of a FROM,
    so that call is refused by name rather than answered with one value for all of them."""
    form = _parsed(macro.name, len(args), dialect).copy()
    held = macro.name in pg_forms_types.HELD | pg_forms_hash.HELD | pg_forms_index.HELD
    named = [c.name for c in form.find_all(exp.Column) if not c.table]
    if _repeats_volatile(named, macro.params, args):
        each = [_argument(a, held) for a in args]
        return _held_once(form, macro.params, each, node, macro.name, dialect)
    if form.find(exp.Lambda) is not None and any(a.find(exp.Select) is not None for a in args):
        return _in_a_subquery(form, macro.params, [_argument(a) for a in args])
    values = dict(zip(macro.params, args))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name], held).copy()
        return node

    return form.transform(bound)


@functools.lru_cache(maxsize=512)
def _replacement(form: str, dialect: str, pattern: str) -> exp.Expr:
    """A grid rule's replacement on ``dialect``, as a form here is read: on ClickHouse, with its
    refusals thrown (:func:`_thrown`). The tree is shared: graft copies it."""
    tree = polyfill_notation.replacement(form, polyfill.written_in(dialect), pattern)
    return tree.transform(_thrown) if dialect == "clickhouse" else tree


def _grafted(
    replacement: exp.Expr, params: tuple[str, ...], args: list[exp.Expr], node: exp.Expr,
    name: str, dialect: str,
) -> exp.Expr:  # fmt: skip
    """A grid rule's replacement with each hole its argument, as :func:`_written` binds a form."""
    bound = [_argument(a) for a in args]
    named = [h for h in map(polyfill.hole_name, replacement.walk()) if h is not None]
    once = _repeats_volatile(named, params, args) or (
        replacement.find(exp.Lambda) is not None and any(a.find(exp.Select) for a in args)
    )
    if once:
        over_columns = polyfill.graft(replacement, {p: exp.column(p) for p in params})
        return _held_once(over_columns, params, bound, node, name, dialect)
    return polyfill.graft(replacement, dict(zip(params, bound)))


def _polyfilled(
    target: exp.Expr, node: exp.Expr, dialect: str, rewrite: Callable[[exp.Expr], exp.Expr]
) -> exp.Expr | None:
    """A call a rule of the polyfill grid matches (REQ-2236), written in the routed engine's form;
    None where no rule matches. A dialect that is no column of the grid has no form of it, and the
    call is refused by name. A rewrite function's refusal of a value is raised as the row's."""
    try:
        return _polyfilled_by_rule(target, node, dialect, rewrite)
    except polyfill_notation.Refused as refused:
        called = target.name.lower() if isinstance(target, exp.Anonymous) else _feature_of(target)
        raise RowRefused(called, dialect, refused.refusal) from refused


def _feature_of(target: exp.Expr) -> str:
    """The name of the grid's feature a typed node's rule is of."""
    names = type(target).sql_names() if isinstance(target, exp.Func) else ()
    return names[0].lower() if names else type(target).__name__.lower()


def _polyfilled_by_rule(
    target: exp.Expr, node: exp.Expr, dialect: str, rewrite: Callable[[exp.Expr], exp.Expr]
) -> exp.Expr | None:
    """:func:`_polyfilled`, a rewrite function's refusal raised as it is."""
    applied = polyfill.GRID.read(target, dialect)
    if applied is None:
        return None
    called = target.name.lower() if isinstance(target, exp.Anonymous) else applied.feature.name
    form = applied.form
    if polyfill.column_of(dialect) is None:
        raise FunctionRefused(called, dialect)
    if form is None:
        return node  # the engine's own, as the statement writes it (by pg_catalog on DuckDB)
    if isinstance(form, polyfill.Refusal):
        raise RowRefused(called, dialect, form)
    if isinstance(form, polyfill.Rewrite):
        written = applied.rewritten
        if written is None:
            # A rule's own pattern: its arguments in their forms first, as a form's holes are
            # bound, since the walk does not descend into what a rule writes.
            argued = target.copy()
            # The copy reads where the call stands (a window's, a filter's), as the call does.
            argued.parent, argued.arg_key = target.parent, target.arg_key
            for argument in list(argued.iter_expressions()):
                argument.replace(argument.transform(rewrite))
            written = form.function(argued, dialect)
        if written is None:
            return None
    else:
        assert applied.rule.pattern is not None
        params = applied.rule.holes()
        args = [applied.bindings[p].transform(rewrite) for p in params]
        replacement = _replacement(form, dialect, applied.rule.pattern)
        written = _grafted(replacement, params, args, node, called, dialect)
    if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
        return exp.alias_(written, called)  # named for the function
    return written


#: The functions that count the NULLs of any number of arguments: which of the two a form answers.
_COUNTS = {"num_nulls": "IS NULL", "num_nonnulls": "IS NOT NULL"}


def _counted(name: str, args: list[exp.Expr], dialect: str) -> exp.Expr:
    """num_nulls and num_nonnulls, of arguments or of an array's elements (VARIADIC)."""
    test = _COUNTS[name]
    values = args
    if len(args) == 1 and isinstance(args[0], exp.Variadic):
        array = args[0].this
        if isinstance(array, exp.Array):
            values = list(array.expressions)
        elif dialect in _DUCKDB:
            written = f"CAST(len(list_filter(a, _x -> _x {test})) AS INTEGER)"
            bound = sqlglot.parse_one(written, read="duckdb")
            return bound.transform(lambda n: array.copy() if n == exp.column("a") else n)
        else:
            raise FunctionRefused(name, dialect)
    terms = " + ".join(
        f"CASE WHEN ({v.sql(dialect='postgres')}) {test} THEN 1 ELSE 0 END" for v in values
    )
    return sqlglot.parse_one(f"CAST({terms or '0'} AS INTEGER)", read="postgres")


#: What a hypothetical-set aggregate answers of a row placed among the rows, over ``ahead``, the
#: rows sorting before it, ``peers``, those level with it, and ``groups``, the sets of level rows
#: before it.
_HYPOTHETICAL = {
    "rank": "CAST(1 + {ahead} AS BIGINT)",
    "dense_rank": "CAST(1 + {groups} AS BIGINT)",
    "percent_rank": (
        "CASE WHEN COUNT(*) = 0 THEN CAST(0 AS DOUBLE PRECISION) "
        "ELSE CAST({ahead} AS DOUBLE PRECISION) / COUNT(*) END"
    ),
    "cume_dist": "CAST({ahead} + {peers} + 1 AS DOUBLE PRECISION) / (COUNT(*) + 1)",
}


def _ordered_set_name(node: exp.WithinGroup) -> str:
    call = node.this
    return call.name.lower() if isinstance(call, exp.Anonymous) else call.sql_name().lower()


def _hypothetical(node: exp.WithinGroup, dialect: str) -> exp.Expr:
    """rank(v) WITHIN GROUP (ORDER BY k), which no engine has, as the counts it is. Of one sort key:
    a row of several compares its keys in turn, which no form here holds."""
    name = _ordered_set_name(node)
    direct, keys = node.this.expressions, node.expression.expressions
    if len(direct) != 1 or len(keys) != 1:
        raise FunctionRefused(name, dialect)
    key, value = (f"({e.sql(dialect='postgres')})" for e in (keys[0].this, direct[0]))
    sooner = ">" if keys[0].args.get("desc") else "<"
    nulls = (key, value) if keys[0].args.get("nulls_first") else (value, key)
    before = f"({key} {sooner} {value} OR ({nulls[0]} IS NULL AND {nulls[1]} IS NOT NULL))"
    level = f"(({key} = {value}) OR ({key} IS NULL AND {value} IS NULL))"
    ahead = f"COALESCE(SUM(CASE WHEN {before} THEN 1 ELSE 0 END), 0)"
    peers = f"COALESCE(SUM(CASE WHEN {level} THEN 1 ELSE 0 END), 0)"
    # COUNT(DISTINCT) leaves out NULL, which sorts as one group where it is before the row.
    groups = (
        f"(COUNT(DISTINCT CASE WHEN {before} THEN {key} END) + "
        f"COALESCE(MAX(CASE WHEN {before} AND {key} IS NULL THEN 1 ELSE 0 END), 0))"
    )
    written = _HYPOTHETICAL[name].format(ahead=ahead, peers=peers, groups=groups)
    return sqlglot.parse_one(written, read="postgres")


def _fractions(argument: exp.Expr) -> list[float | None]:
    """The fractions a statement's percentile says, a NULL as None."""
    node = argument.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Array):
        return [value for item in node.expressions for value in _fractions(item)]
    if isinstance(node, exp.Literal) and node.is_string:  # an array's text: '{0.25,0.5}'
        items = node.this.strip().removeprefix("{").removesuffix("}").split(",")
        return [None if i.strip().upper() == "NULL" else float(i) for i in items if i.strip()]
    if isinstance(node, exp.Null):
        return [None]
    if isinstance(node, exp.Neg):
        return [None if v is None else -v for v in _fractions(node.this)]
    if not isinstance(node, exp.Literal):  # a bound parameter: the engine checks its value
        return []
    return [float(node.sql(dialect="postgres"))]


#: DuckDB takes a negative fraction as its sort descending, and 0 has none: a smallest positive
#: fraction is the same row.
_FIRST_DESCENDING = 5e-324


def _of_intervals(node: exp.WithinGroup, fractions: list[float], array: bool) -> exp.Expr:
    """DuckDB's percentile_cont of intervals interpolates microseconds, carrying months and days
    into hours: as PostgreSQL's, each fraction's two neighbours among the keys sorted as
    PostgreSQL compares intervals, and interval_lerp between them where they differ."""
    ordered = node.expression.expressions[0]
    key = ordered.this.sql(dialect="duckdb")
    direction = " DESC" if ordered.args.get("desc") else ""
    place = "(_f * (_n - 1))"
    lo = f"_k[CAST(floor({place}) AS BIGINT) + 1]"
    hi = f"_k[CAST(ceil({place}) AS BIGINT) + 1]"
    each = (
        f"CASE WHEN ceil({place}) = floor({place}) THEN {lo} "
        f"ELSE {pg_forms.interval_lerp(lo, hi, f'{place} - floor({place})')} END"
    )
    said = ", ".join(map(repr, fractions))
    answer = (
        f"list_transform([{said}], _f -> {each})" if array else pg_forms.bound("_f", said, each)
    )
    written = pg_forms.bound(
        "_k",
        f"list({key} ORDER BY {pg_forms.interval_span(key)}{direction}) "
        f"FILTER (WHERE {key} IS NOT NULL)",
        pg_forms.bound("_n", f"count({key})", f"CASE WHEN _n = 0 THEN NULL ELSE {answer} END"),
    )
    return sqlglot.parse_one(written, read="duckdb")


#: The rows' keys, sorted, bound to ``_s``, and each fraction, bound to ``_f`` where there are
#: several: Trino's approx_percentile and ClickHouse's quantile answer approximately.
_EXACT = {
    "trino": {
        "sorted": "array_sort(filter(array_agg({k}), _e -> _e IS NOT NULL))",
        "descending": "reverse({sorted})",
        # No keys answer NULL, as PostgreSQL does: the line between two of none would read
        # element_at(_s, 0), which Trino refuses ("SQL array indices start at 1"). Only NULL keys
        # leave _s empty; no rows leave it NULL (array_agg over none), whose cardinality is NULL.
        "bound": (
            "element_at(transform(ARRAY[{sorted}], "
            "_s -> CASE WHEN coalesce(cardinality(_s), 0) = 0 THEN NULL ELSE {answer} END), 1)"
        ),
        "each": "transform(ARRAY[{fractions}], _f -> {answer})",
        "disc": "element_at(_s, CAST(greatest(ceiling({f} * cardinality(_s)), 1) AS BIGINT))",
        "cont": (
            "element_at(_s, {low}) + (element_at(_s, {high}) - element_at(_s, {low})) * "
            "({place} - {low})"
        ),
        "place": "({f} * (cardinality(_s) - 1) + 1)",
        "low": "CAST(floor({place}) AS BIGINT)",
        "high": "CAST(ceiling({place}) AS BIGINT)",
        "number": "CAST({k} AS DOUBLE)",
    },
    "clickhouse": {
        "sorted": "arraySort(groupArray({k}))",
        "descending": "arrayReverseSort(groupArray({k}))",
        "bound": "arrayElement(arrayMap(_s -> {answer}, [{sorted}]), 1)",
        "each": "arrayMap(_f -> {answer}, [{fractions}])",
        "disc": ("if(length(_s) = 0, NULL, _s[toUInt64(greatest(ceil({f} * length(_s)), 1))])"),
        "cont": (
            "if(length(_s) = 0, NULL, _s[{low}] + (_s[{high}] - _s[{low}]) * ({place} - {low}))"
        ),
        "place": "({f} * (length(_s) - 1) + 1)",
        "low": "toUInt64(floor({place}))",
        "high": "toUInt64(ceil({place}))",
        "number": "toFloat64({k})",
    },
}


def _exact(node: exp.WithinGroup, fractions: list[float], array: bool, dialect: str) -> exp.Expr:
    """A percentile from the sorted keys, as PostgreSQL's: the first key past the fraction, or the
    line between the two either side of it."""
    forms = _EXACT["trino" if dialect in _TRINO else dialect]
    ordered = node.expression.expressions[0]
    interpolated = isinstance(node.this, exp.PercentileCont)
    if interpolated and said_type(ordered.this) == "interval":
        raise FunctionRefused(node.this.sql_name().lower(), dialect)
    key = ordered.this.sql(dialect=dialect)
    key = forms["number"].format(k=key) if interpolated else key
    sorted_ = forms["sorted"].format(k=key)
    if ordered.args.get("desc"):
        sorted_ = forms["descending"].format(sorted=sorted_, k=key)

    def answer(fraction: str) -> str:
        if not interpolated:
            return forms["disc"].format(f=fraction)
        place = forms["place"].format(f=fraction)
        return forms["cont"].format(
            place=place,
            low=forms["low"].format(place=place),
            high=forms["high"].format(place=place),
        )

    each = (
        forms["each"].format(fractions=", ".join(map(repr, fractions)), answer=answer("_f"))
        if array
        else answer(repr(fractions[0]))
    )
    written = forms["bound"].format(sorted=sorted_, answer=each)
    return sqlglot.parse_one(written, read=dialect)


#: An element of an array, counted from 1, as each dialect reads one.
_ELEMENT = {"trino": "element_at", "clickhouse": "arrayElement", "duckdb": "list_extract"}


def _in_place(written: exp.Expr, fractions: list[float | None], dialect: str) -> exp.Expr:
    """``written``, the answers of the fractions that are not NULL, with each NULL fraction's NULL
    back in its place, as PostgreSQL answers an array that holds one: a NULL of the aggregate's
    type over the same rows."""
    element = _ELEMENT["trino" if dialect in _TRINO else dialect]

    def at(index: int) -> exp.Expr:
        return exp.Anonymous(this=element, expressions=[written.copy(), exp.Literal.number(index)])

    places = itertools.count(1)
    return exp.Array(
        expressions=[
            exp.Case(ifs=[exp.If(this=exp.false(), true=at(1))])
            if fraction is None
            else at(next(places))
            for fraction in fractions
        ]
    )


def _percentile(node: exp.WithinGroup, dialect: str) -> exp.Expr:
    """percentile_cont and percentile_disc as PostgreSQL has them: a NULL fraction answers NULL of
    the aggregate's type over the same rows, in its place in an array of them, and one outside 0
    to 1 is refused."""
    argument = node.this.this
    array = isinstance(argument.unnest(), exp.Array) or (
        isinstance(argument.unnest(), exp.Cast) and argument.unnest().is_type("array")
    )
    named = "percentile_cont" if isinstance(node.this, exp.PercentileCont) else "percentile_disc"
    fractions = _fractions(argument)
    if not fractions and not array:  # bound: DuckDB's own percentile checks the value it is given
        if dialect in _DUCKDB:
            return node
        raise FunctionRefused(named, dialect)
    for fraction in fractions:
        if fraction is not None and not 0 <= fraction <= 1:
            raise PostgresError("22003", f"percentile value {fraction:g} is not between 0 and 1")
    held = argument.unnest()
    while isinstance(held, exp.Cast):
        held = held.this.unnest()
    whole = isinstance(held, exp.Null)  # the fraction, or the array of them, is NULL
    # REQ-2167: let ClickHouse return its own engine error for a NULL array fraction rather than
    # raising FunctionRefused (commit 4c510070b2 changed MATRIX["clickhouse"] to expect "error").
    placed = "trino" if dialect in _TRINO else dialect
    if None in fractions and not whole and placed not in _ELEMENT:
        raise FunctionRefused(named, dialect)
    known = [fraction for fraction in fractions if fraction is not None] or [0.0]
    written = _of_the_known(node, known, array, dialect)
    if whole:
        return exp.Case(ifs=[exp.If(this=exp.false(), true=written)])
    return _in_place(written, fractions, dialect) if None in fractions else written


def _of_the_known(node: exp.WithinGroup, known: list[float], array: bool, dialect: str) -> exp.Expr:
    """The percentile of each fraction in ``known``, none of them NULL."""
    ordered = node.expression.expressions[0]
    if dialect in _TRINO or dialect == "clickhouse":
        return _exact(node, known, array, dialect)
    said = said_type(ordered.this) == "interval"
    if said and isinstance(node.this, exp.PercentileCont) and dialect in _DUCKDB:
        return _of_intervals(node, known, array)
    if dialect in _DUCKDB and ordered.args.get("desc"):
        known = [fraction or _FIRST_DESCENDING for fraction in known]
    numbers = [exp.Literal.number(repr(fraction)) for fraction in known]
    node.this.set("this", exp.Array(expressions=numbers) if array else numbers[0])
    return node


#: The functions in FROM with columns of their own, which DuckDB's unnest of a struct does not make.
_ROWS_OF_COLUMNS = frozenset(
    {"pg_options_to_table", "aclexplode", "pg_timezone_names", "pg_timezone_abbrevs"}
)


#: byteain's escapes: a doubled backslash, or a byte in three octal digits.
_BYTEA_ESCAPE = re.compile(r"\\\\|\\([0-3][0-7]{2})")


#: A class sqlglot reads a call of a function of the table as.
_READ_AS: dict[type[exp.Expr], str] = {
    exp.JustifyDays: "justify_days",
    exp.JustifyHours: "justify_hours",
    exp.JustifyInterval: "justify_interval",
    exp.MD5: "md5",
    exp.Acosh: "acosh",
    exp.Asinh: "asinh",
    exp.Atanh: "atanh",
    exp.Avg: "avg",
    exp.Ascii: "ascii",
    exp.BitLength: "bit_length",
    exp.Ceil: "ceil",
    exp.Cot: "cot",
    exp.Floor: "floor",
    exp.Max: "max",
    exp.Min: "min",
    exp.Exp: "exp",
    exp.Ln: "ln",
    exp.Sqrt: "sqrt",
    exp.Stddev: "stddev",
    exp.StddevPop: "stddev_pop",
    exp.StddevSamp: "stddev_samp",
    exp.Variance: "variance",
    exp.VariancePop: "var_pop",
}


_REGRESSIONS_READ: dict[type[exp.Expr], str] = {
    getattr(exp, class_name): macro.name
    for macro in _REGRESSIONS
    if hasattr(exp, class_name := "".join(part.title() for part in macro.name.split("_")))
}


def _counts_from_the_first(start: exp.Expr | None) -> bool:
    """Whether a substring's start is a position no dialect counts otherwise: 1 or more."""
    return (_constant(start) or 0) >= 1


def _call(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of a call the table may hold."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if type(node) in _READ_AS:
        return _READ_AS[type(node)], [node.this]
    if type(node) in _REGRESSIONS_READ:
        return _REGRESSIONS_READ[type(node)], [node.this, node.expression]
    if isinstance(node, exp.Timestamp):
        return "timestamp", [a for a in (node.this, node.args.get("zone")) if a is not None]
    if isinstance(node, exp.ToNumber):
        return "to_number", [node.this, node.args["format"]]
    if isinstance(node, exp.WidthBucket):
        at = ("this", "min_value", "max_value", "num_buckets", "threshold")
        return "width_bucket", [node.args[k] for k in at if node.args.get(k) is not None]
    if isinstance(node, exp.Length):  # an overload of a type: length(text) has no row
        return "length", [node.this]
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.IntDiv):  # sqlglot's reading of div
        return "div", [node.this.this, node.this.expression]
    if isinstance(node, (exp.Round, exp.Trunc)):
        called = "round" if isinstance(node, exp.Round) else "trunc"
        return called, [a for a in (node.this, node.args.get("decimals")) if a is not None]
    if isinstance(node, exp.ArrayConcat) and len(node.expressions) == 1:
        return "array_cat", [node.this, node.expressions[0]]
    if isinstance(node, exp.Repeat):
        return "repeat", [node.this, node.args["times"]]
    if isinstance(node, exp.ArrayRemove):
        return "array_remove", [node.this, node.expression]
    if isinstance(node, exp.Substring) and not _counts_from_the_first(node.args.get("start")):
        length = node.args.get("length")
        return "substr", [node.this, node.args["start"], *([] if length is None else [length])]
    if isinstance(node, exp.RegexpReplace) and node.args.get("position") is not None:
        at = ("this", "expression", "replacement", "position", "occurrence")
        if node.args.get("modifiers") is None:
            return "regexp_replace_at", [node.args[k] for k in at if node.args.get(k) is not None]
    if isinstance(node, exp.Log) and node.this == exp.Literal.number(10):  # sqlglot's log10
        return "log10", [node.args["expression"]]
    if isinstance(node, (exp.Log, exp.Pow)):
        called = "log" if isinstance(node, exp.Log) else "power"
        return called, [a for a in (node.this, node.expression) if a is not None]
    if isinstance(node, exp.Time):  # what sqlglot reads as a zone is an overload's second argument
        return "time", [a for a in (node.this, node.args.get("zone")) if a is not None]
    if isinstance(node, exp.SHA2):  # sqlglot's reading of sha256, sha384 and sha512
        length = node.args.get("length")
        return f"sha{length.this if isinstance(length, exp.Literal) else 256}", [node.this]
    if isinstance(node, exp.Encode) and isinstance(node.args.get("charset"), exp.Literal):
        return f"encode_{node.args['charset'].this.lower()}", [node.this]
    if isinstance(node, exp.RegexpSubstr):
        keys = ("this", "expression", "position", "occurrence", "parameters", "group")
        return "regexp_substr", [node.args[k] for k in keys if node.args.get(k) is not None]
    if isinstance(node, exp.RegexpInstr):
        keys = ("this", "expression", "position", "occurrence", "option", "parameters", "group")
        return "regexp_instr", [node.args[k] for k in keys if node.args.get(k) is not None]
    return None


def _columns_of(table: exp.Table, rewrite: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A function of ``_ROWS_OF_COLUMNS`` in FROM: a row of its struct's fields, under the name the
    statement gives it."""
    call = table.this
    name = call.name.lower()
    written = rewrite(call)
    written = written.this if isinstance(written, exp.Alias) else written
    rows = written.this if isinstance(written, exp.Explode) else written.expressions[0]
    expanded = exp.Anonymous(
        this="unnest",
        expressions=[rows, exp.Kwarg(this=exp.var("recursive"), expression=exp.true())],
    )
    alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(name))
    return exp.Subquery(this=exp.select(expanded), alias=alias)


def _bytea_digits(text: str) -> str | None:
    """The hex digits of byteain's reading of ``text``; None for what it refuses."""
    if text.startswith("\\x"):
        digits = text[2:]
        valid = len(digits) % 2 == 0 and all(c in string.hexdigits for c in digits)
        return digits if valid else None
    found = b""
    position = 0
    for escape in _BYTEA_ESCAPE.finditer(text):
        found += text[position : escape.start()].encode()
        code = escape.group(1)
        found += b"\\" if code is None else bytes([int(code, 8)])
        position = escape.end()
    rest = text[position:]
    return None if "\\" in rest else (found + rest.encode()).hex()


def _bytea_input(value: exp.Expr, called: str, dialect: str) -> exp.Expr:
    """A literal that is a bytea as those bytes, on any dialect: PostgreSQL reads the literal of
    sha256('\\x6162') as byteain does, where a dialect would hash its text."""
    said = said_type(value)
    if said is not None and said.split("(")[0] in _TEXT_TYPES:
        raise FunctionRefused(called, dialect)  # function sha256(text) does not exist
    literal = value.this if said == "bytea" else value
    if isinstance(literal, exp.Null) and dialect == "mysql":  # it has no CAST AS VARBINARY
        return literal
    if not isinstance(literal, exp.Literal) or not literal.is_string:
        return value
    digits = _bytea_digits(literal.this)
    if digits is None:
        raise FunctionRefused(called, dialect)  # invalid input syntax for type bytea
    if dialect == "tsql":  # it has no UNHEX
        return exp.HexString(this=digits)
    return exp.Unhex(this=exp.Literal.string(digits))


def _digits_as_written(value: exp.Expr) -> exp.Expr:
    """A numeric literal as the text the statement wrote: an engine's float would lose digits."""
    node = value.unnest()
    if isinstance(node, exp.Cast):
        node = node.this.unnest()
    negated = isinstance(node, exp.Neg)
    number = node.this.unnest() if negated else node
    if not isinstance(number, exp.Literal) or number.is_string:
        return value
    return exp.Literal.string(f"{'-' if negated else ''}{number.this}")


def _range_as_written(value: exp.Expr) -> exp.Expr:
    """A range literal as range_out writes it: a column's text is that already."""
    node = value.unnest()
    if not isinstance(node, exp.Cast):
        return value
    literal, said = node.this.unnest(), node.to.sql(dialect="postgres").lower()
    if isinstance(literal, exp.Null):
        return literal
    if not isinstance(literal, exp.Literal) or not literal.is_string:
        return node.this
    if said in RANGE_TYPES:
        return exp.Literal.string(range_text(parse_range(RANGE_TYPES[said], literal.this)))
    rtype = MULTIRANGE_TYPES[said]
    ranges = parse_multirange(rtype, literal.this).ranges
    return exp.Literal.string("{" + ",".join(range_text(r) for r in ranges) + "}")


def range_rows_chosen(tree: exp.Expr) -> exp.Expr:
    """Each hash of a range in the row of the type its statement says: the pass that reads ranges
    runs first, and leaves no cast to say it."""

    def chosen(node: exp.Expr) -> exp.Expr:
        dotted = isinstance(node, exp.Dot) and isinstance(node.this, exp.Identifier)
        target = node.expression if dotted and node.this.name.lower() == "pg_catalog" else node
        call = _call(target)
        if call is None:
            return node
        name = overload(call[0], call[1])
        if name not in pg_forms_hash.RANGE_ROWS:
            return node
        args = call[1]
        return exp.Anonymous(this=name, expressions=[_range_as_written(args[0]), *args[1:]])

    return tree.transform(chosen)


def _array_elements_as_written(value: exp.Expr) -> exp.Expr:
    """An array literal as its elements: an array_in text is read by no engine's cast."""
    node = value.unnest()
    return postgres_literal(node) if isinstance(node, exp.Cast) else value


_ARRAY_INPUT = (
    pg_forms_hash.ARRAY_ROWS | pg_forms_aggregate.ARRAY_INPUT | pg_forms_index.ARRAY_INPUT
)


def _strict_null(name: str, arity: int, dialect: str) -> exp.Expr | None:
    """A statement's NULL of a strict function whose form reads its bytes, or on ClickHouse, which
    has no NULL of an array, of one over an array. None where the form answers it."""
    if name in pg_forms_hash.ROWS:
        return exp.cast(exp.null(), "BIGINT" if arity > 1 else "INT")
    if name in pg_forms_index.PER_ENGINE_RESULTS:
        return exp.cast(exp.null(), pg_forms_index.PER_ENGINE_RESULTS[name])
    return exp.null() if dialect == "clickhouse" and name in _ARRAY_INPUT else None


def _is_null(value: exp.Expr) -> bool:
    """Whether an argument is a NULL, of any type the statement says."""
    node = value.unnest()
    return isinstance(node.this.unnest() if isinstance(node, exp.Cast) else node, exp.Null)


def _array_as_text(value: exp.Expr) -> exp.Expr:
    """An array literal as its elements' text: no engine's decimal holds NaN or every digit, and
    no engine's date holds infinity."""
    node = value.unnest()
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Array):
        return exp.Cast(this=node.this, to=exp.DataType.build("TEXT[]", dialect="postgres"))
    return value


#: The names sqlglot would read as something else: a row of the table, by its own name.
_KEPT_BY_NAME = frozenset(name for name, _ in MACROS) | {"char"}


#: PostgreSQL's reading of a position before the value, or a negative length, which these engines
#: read from its end.
_FROM_THE_END = frozenset({"trino", "presto", "athena", "clickhouse"})
_EDGE_FORMS: dict[tuple[type[exp.Expr], bool], str] = {
    (exp.Substring, False): "SUBSTRING(v, GREATEST(s, 1))",
    (exp.Substring, True): "SUBSTRING(v, GREATEST(s, 1), GREATEST(s + n - GREATEST(s, 1), 0))",
    (exp.Left, True): (
        "CASE WHEN n < 0 THEN SUBSTRING(v, 1, GREATEST(LENGTH(v) + n, 0)) ELSE LEFT(v, n) END"
    ),
    (exp.Right, True): (
        "CASE WHEN n < 0 THEN SUBSTRING(v, LEAST(1 - n, LENGTH(v) + 1)) "
        "ELSE SUBSTRING(v, GREATEST(LENGTH(v) - n + 1, 1)) END"
    ),
}


def _constant(node: exp.Expr | None) -> int | None:
    """The whole number ``node`` always is; None for anything a row decides."""
    node = node.unnest() if node is not None else None
    if isinstance(node, exp.Neg):
        inner = _constant(node.this)
        return None if inner is None else -inner
    if isinstance(node, exp.Add):
        left, right = _constant(node.this), _constant(node.expression)
        return None if left is None or right is None else left + right
    if isinstance(node, exp.Literal) and node.is_int:
        return int(node.this)
    return None


def _past_the_edge(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """substring, left or right as PostgreSQL reads their position, where a row decides it or a
    constant is one the engine would read from the end; a safe constant stays the engine's own."""
    if isinstance(node, exp.Substring):
        start, length = node.args.get("start"), node.args.get("length")
        if start is not None and (start.unnest().is_string or said_type(start) == "text"):
            raise FunctionRefused("substring", dialect)  # substring(text, pattern)
        safe = (_constant(start) or 0) >= 1
        values = {"v": node.this, "s": start, "n": length}
        key = (exp.Substring, length is not None)
        if start is None:
            return None
    else:
        count = node.expression
        constant = _constant(count)
        safe = isinstance(node, exp.Left) and constant is not None and constant >= 0
        values = {"v": node.this, "n": count}
        key = (type(node), True)
    present = [value for value in values.values() if value is not None]
    if safe or any(_is_volatile(value) for value in present):
        return None

    def bound(part: exp.Expr) -> exp.Expr:
        if isinstance(part, exp.Column) and part.name in values:
            return _argument(values[part.name]).copy()
        return part

    return sqlglot.parse_one(_EDGE_FORMS[key], read="postgres").transform(bound)


#: The most digits past the point a decimal of 38 holds and leaves room for a whole part.
_MOST_SCALE = 18
#: The most digits past the point DuckDB's DECIMAL holds: its width is 38 at most.
_DUCKDB_MOST_SCALE = 38


def refuse_named(tree: exp.Expr, dialect: str) -> None:
    """A statement a later rewrite would take for something else, read for what a replay refused:
    in_range names no range, and pg_ranges reads it as one."""
    for node in _unqualified(tree.copy()).walk():
        name = engine_refusals.refused_call(node, dialect, said_type)
        if name is not None:
            raise FunctionRefused(name, dialect)


def _unqualified(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each pg_catalog.f(...) the call of f, as sqlglot reads it: no engine but
    PostgreSQL and DuckDB has that schema, and sqlglot writes a qualified call as it stands."""
    for dot in reversed(list(tree.find_all(exp.Dot))):
        call = dot.expression
        if not isinstance(dot.this, exp.Identifier) or dot.this.name.lower() != "pg_catalog":
            continue
        if isinstance(call, exp.Anonymous) and call.name.lower() in _KEPT_BY_NAME:
            call = exp.Anonymous(this=call.name.lower(), expressions=call.expressions)
        elif isinstance(call, exp.Anonymous):
            args = [said_text(a) for a in call.expressions]
            call = exp.func(call.name, *args, dialect="postgres")
        if isinstance(call, (exp.Func, exp.Binary)):  # mod is a binary as sqlglot reads it
            replaced = dot.replace(call)
            tree = replaced if dot is tree else tree
    return tree


#: The rows whose result has the scale their second argument says.
_OF_A_SAID_SCALE = frozenset({"round_numeric", "trunc_numeric"})


def _said_scale(scale: exp.Expr, called: str, dialect: str) -> int | None:
    """The digits a call says it keeps, None for a NULL, which answers NULL. A dialect's decimal
    keeps its argument's scale, and a scale only a row knows cannot be a type. A replay on the other
    engines refused a scale below 0 and one a decimal of 38 has no room for."""
    said = scale.unnest()
    if isinstance(said, exp.Cast):
        said = said.this.unnest()
    if isinstance(said, exp.Null):
        return None
    digits = _constant(said)
    most = _DUCKDB_MOST_SCALE if dialect in _DUCKDB else _MOST_SCALE
    if digits is None or digits > most or (dialect not in _DUCKDB and digits < 0):
        raise FunctionRefused(called, dialect)
    return digits


def inline(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` with each call of a function of the table written in its form for ``dialect``.
    Raises :class:`FunctionRefused` for a call of one it has no form of. A call with no overload
    of its argument count is left, for the engine to say so."""
    if dialect in NATIVE:
        return tree
    if dialect not in _DUCKDB:
        tree = _unqualified(tree)
        for node in tree.walk():
            refused_call = engine_refusals.refused_call(node, dialect, said_type)
            if refused_call is not None:
                raise FunctionRefused(refused_call, dialect)
    try:
        tree = pg_forms_textsearch.lowered(tree, dialect)
        tree = pg_forms_json.lowered(tree, dialect)
    except (pg_forms_json.NoForm, pg_forms_textsearch.NoForm) as refused:
        raise FunctionRefused(refused.called, dialect) from refused

    def rewrite(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.WithinGroup):
            if isinstance(node.this, (exp.PercentileCont, exp.PercentileDisc)):
                return _percentile(node, dialect)
            if _ordered_set_name(node) in _HYPOTHETICAL:
                return _hypothetical(node, dialect)
            if dialect in _TRINO or dialect == "clickhouse":
                raise FunctionRefused(_ordered_set_name(node), dialect)  # mode: no engine's own
            return node
        if (
            isinstance(node, exp.Table)
            and isinstance(node.this, exp.Anonymous)
            and node.this.name.lower() in _ROWS_OF_COLUMNS
            and dialect in _DUCKDB
        ):
            return _columns_of(node, rewrite)
        if isinstance(node, exp.RegexpReplace) and dialect in _FROM_THE_END:
            parts = (node.this, node.expression, node.args["replacement"])
            if any(_is_null(part) for part in parts):
                return exp.Cast(this=exp.Null(), to=exp.DataType.build("TEXT"))
        if (
            isinstance(node, exp.Anonymous)
            and node.name.lower() in ("date_part", "extract")
            and dialect in _FROM_THE_END
            and any(_is_null(part) for part in node.expressions)
        ):  # strict, and an engine's own part of a NULL is its error
            return exp.Cast(this=exp.Null(), to=exp.DataType.build("DOUBLE"))
        if isinstance(node, (exp.Substring, exp.Left, exp.Right)) and dialect in _FROM_THE_END:
            edged = _past_the_edge(node, dialect)
            if edged is not None:
                return edged
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        first = polyfill.GRID.read_first(target, dialect)  # a grid rule over every call
        if first is not None and first.rewritten is not None:
            in_select = isinstance(node.parent, exp.Select) and node.arg_key == "expressions"
            return first.rewritten if in_select else first.rewritten.unalias()
        polyfilled = _polyfilled(target, node, dialect, rewrite)
        if polyfilled is not None:
            return polyfilled
        if type(node) in _ARRAY_OPERATOR_ROWS and dialect in _TRINO:
            row = _ARRAY_OPERATOR_ROWS[type(node)]
            target = exp.Anonymous(this=row, expressions=[node.this, node.expression])
        call = _call(target)
        if call is None:
            return node
        called, args = call
        if called in _COUNTS:
            return _counted(called, [a.transform(rewrite) for a in args], dialect)
        name = overload(called, args)
        macro = MACROS.get((name, len(args)))
        if macro is None:
            return node
        step, form = macro.resolved(dialect)
        told = _is_told(called, args)  # by the name as written: a row's own name says its overload
        called = _CALLED.get(name, called)
        if step is Step.REFUSED or not told:
            raise FunctionRefused(called, dialect)
        if form is MAPPED:
            return target
        if name in _BYTEA_INPUT:
            args = [_bytea_input(a, called, dialect) for a in args]
        if name in pg_forms_hash.NUMERIC_INPUT:
            args = [_digits_as_written(args[0]), *args[1:]]
        if name in _ARRAY_INPUT:
            args = [_array_elements_as_written(a) for a in args]
        if name in pg_forms_hash.TEXT_ARRAY_ROWS:
            args = [_array_as_text(args[0]), *args[1:]]
        if any(_is_null(a) for a in args):
            answer = _strict_null(name, len(args), dialect)
            if answer is not None:
                in_select = isinstance(node.parent, exp.Select) and node.arg_key == "expressions"
                return exp.alias_(answer, called) if in_select else answer
        digits = _said_scale(args[1], called, dialect) if name in _OF_A_SAID_SCALE else None
        if digits is not None:  # an engine takes a scale only as a constant
            args = [args[0], exp.Literal.number(digits)]
        # transform does not revisit what a replaced node holds: a call in an argument.
        written = _written(macro, [a.transform(rewrite) for a in args], dialect, node)
        if digits is not None:  # the scale PostgreSQL's numeric answers with, 0 for one below it
            scaled = f"DECIMAL(38, {max(digits, 0)})"
            written = exp.Cast(this=written, to=exp.DataType.build(scaled, dialect="postgres"))
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, called)  # named for the function
        return written

    return tree.transform(rewrite, copy=False)


#: The name and arguments of a call the table may hold, for the grid's rules that read calls.
call_of = _call
