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

import dataclasses
import enum
import functools
import re
import string
from collections.abc import Mapping
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import MARK, raising
from provisa.transpiler import engine_refusals, pg_forms, pg_forms_json, pg_forms_types
from provisa.transpiler.metadata_functions import ENCODINGS, VALUE_FORMS

NATIVE = frozenset({"postgres"})
"""The dialects that have every function of the table themselves."""

_DUCKDB = frozenset({"duckdb"})
_TRINO = frozenset({"trino", "presto", "athena"})
_SPARK = frozenset({"spark", "databricks"})
_EVERY = frozenset({"*"})
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
    refused: frozenset[str] = frozenset()

    def resolved(self, dialect: str) -> tuple[Step, str | None]:
        """The rule: the first step with a form on ``dialect``, and that form. ``refused`` holds
        the dialects a replay refused the shared form on (REQ-2171, A VARIATION IS ADDED ONLY AFTER
        A REPLAY FAILS)."""
        if dialect in NATIVE:
            return Step.OWN, MAPPED
        if dialect in self.refused:
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
    return (
        *(
            Macro(
                f"{prefix}{suffix}",
                ("a", "b"),
                operators={_EVERY: Pg(form)},
                refused=_NO_CASE_SENSITIVE_LIKE,
            )
            for prefix in ("text", "name", "bpchar")
            for suffix, form in operators.items()
        ),
        *(
            Macro(
                f"{prefix}{suffix}",
                ("a", "b"),
                {_SNOWFLAKE: variation, _EVERY: Pg(form)},
                refused=_NO_REGEX,
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


def _pg(name: str, params: str, form: str, refused: frozenset[str] = frozenset()) -> Macro:
    """An overload whose form is PostgreSQL's syntax, which sqlglot writes for every dialect."""
    where = _CASTS_CHECKED if MARK in form else _EVERY
    return Macro(
        name, tuple(p for p in params.split(", ") if p), {where: Pg(form)}, refused=refused
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
) -> Macro:
    """An overload, its DuckDB form, any other dialect's, and any dialect's own function."""
    return Macro(
        name,
        tuple(p for p in params.split(", ") if p),
        {_DUCKDB: duckdb, **(others or {})},
        own or {},
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
    _macro(
        "pg_timezone_names",
        "",
        "unnest((SELECT list(_z) FROM pg_timezone_names() _z))",
    ),
    _macro(
        "pg_timezone_abbrevs",
        "",
        "unnest((SELECT list(_z) FROM (SELECT DISTINCT COLUMNS(_c -> _c <> 'name') "
        "FROM pg_timezone_names()) _z))",
    ),
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
        others={_TRINO: f"fail('{MARK} ' || sqlstate || ': ' || message)"},
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
    _macro("center", "value", pg_forms.CENTER, own={_TRINO: pg_forms.TRINO_CENTER}),
    _macro("box_center", "value", pg_forms.BOX_CENTER, own={_TRINO: pg_forms.TRINO_BOX_CENTER}),
    _macro(
        "circle_center", "value", pg_forms.CIRCLE_CENTER, {_TRINO: pg_forms.TRINO_CIRCLE_CENTER}
    ),
    _macro("diagonal", "value", pg_forms.BOX_DIAGONAL, own={_TRINO: pg_forms.TRINO_BOX_DIAGONAL}),
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
#: never read a statement's own call.
_MONEY: tuple[Macro, ...] = (
    _macro("money_text", "value", pg_forms.MONEY_TEXT, {_TRINO: pg_forms.TRINO_MONEY_TEXT}),
    _macro("money_words", "value", pg_forms.MONEY_WORDS),
    _macro("money_bytes", "value", pg_forms.MONEY_BYTES, own={_TRINO: pg_forms.TRINO_MONEY_BYTES}),
    _macro("money_input", "value", pg_forms.MONEY_INPUT),
)


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
    return dataclasses.replace(_operator(form), name=name)


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
    _pg("dexp", "x", strict("x", "DOUBLE", pg_forms.DEXP)),
    _pg("dlog1", "x", strict("x", "DOUBLE", pg_forms.DLOG1)),
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
    _macro("overlaps_timestamp", "s1, e1, s2, e2", pg_forms.OVERLAPS),
    _macro("datetime_pl", "d, t", strict("d, t", "TIMESTAMP", pg_forms.DATETIME_PL)),
    _macro("timedate_pl", "t, d", strict("d, t", "TIMESTAMP", pg_forms.DATETIME_PL)),
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
    _macro("timestamp_time", "value", strict("value", "TIME", pg_forms.TIMESTAMP_TIME)),
    _macro("timestamptz_time", "value", strict("value", "TIME", pg_forms.TIMESTAMPTZ_TIME)),
    _pg("timetz_time", "value", "CAST(value AS TIME)"),
    _pg("time", "value", "CAST(value AS TIME)", _NO_TIME),
    _macro("timestamptz", "value", strict("value", "TIMESTAMPTZ", pg_forms.TIMESTAMPTZ_OF)),
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
    Macro("encode_base64", ("value",), {_DUCKDB: "to_base64(value)"}),
    Macro("encode_escape", ("value",), {_DUCKDB: "provisa_encode_escape(value)"}),
)
#: The rows whose argument is a bytea: a literal there is bytea's input, not text.
_BYTEA_INPUT = frozenset({"md5_bytea", "sha224", "sha256", "sha384", "sha512"})
#: What a statement says of a value PostgreSQL has no bytea overload for.
_TEXT_TYPES = frozenset({"text", "varchar", "character varying", "bpchar", "char", "character"})


_ROWS: tuple[Macro, ...] = (
    *_text_matches(),
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
    _macro("quote_ident", "value", quoted_identifier("CAST(value AS VARCHAR)")),
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
    Macro("icu_unicode_version", (), {_EVERY: Pg("CAST(NULL AS TEXT)")}),
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
        "LEFT(name, 63 - LENGTH('_' || CAST(oid AS TEXT))) || '_' || CAST(oid AS TEXT)",
    ),
    _macro("pg_typeof", "value", _PG_TYPEOF),
    *_SHAPES,
    *_TSVECTOR,
    _macro(
        "mul_d_interval",
        "d, i",
        "to_microseconds(CAST(round(epoch(i) * 1000000 * d) AS BIGINT))",
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
    # An encoding's number, its name compared as PostgreSQL's does; a name naming none -1.
    _macro(
        "pg_char_to_encoding",
        "name",
        "CASE lower(regexp_replace(name, '[^A-Za-z0-9]', '', 'g')) WHEN 'unicode' THEN 6 "
        + " ".join(
            f"WHEN '{encoding.lower().replace('_', '')}' THEN {number}"
            for number, encoding in enumerate(ENCODINGS)
        )
        + " ELSE CASE WHEN name IS NOT NULL THEN -1 END END",
    ),
    *_NUMBERS,
    *_DATES,
    *_XML,
    *_MONEY,
    *VALUE_FUNCTIONS_OF_THE_SERVER,
    *(Macro(name, params, {_EVERY: Pg(form)}) for name, params, form in VALUE_FORMS),  # REQ-2190
    *(Macro(name, params, forms) for name, params, forms in pg_forms_json.ROWS),
    *_DIGESTS,
    *map(_operator, pg_forms_types.OPERATORS),
    *(
        Macro(form.name, tuple(form.params.split(", ")), operators={_DUCKDB: Pg(form.sql)})
        for form in pg_forms_types.ARRAY_OPERATORS
    ),
)

#: A call's name and argument count -> its overload: the one table.
MACROS: dict[tuple[str, int], Macro] = {(m.name, len(m.params)): m for m in _ROWS}
#: (function, argument count, the type the statement says of its first argument) -> its row:
#: PostgreSQL's own name for the overload. The operation is chosen by the type, not the engine's.
OVERLOADS: dict[tuple[str, int, str], str] = {
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
    ("int4", 1, "bit"): "bittoint4",
    ("int8", 1, "bit"): "bittoint8",
    ("int4", 1, '"char"'): "chartoi4",
    ("time", 1, "interval"): "interval_time",
    ("time", 1, "timestamp"): "timestamp_time",
    ("time", 1, "timestamptz"): "timestamptz_time",
    ("time", 1, "timetz"): "timetz_time",
    ("time", 2, "time"): "time_scale",
    ("timestamp", 2, "date"): "datetime_pl",
    ("timestamp", 2, "timestamp"): "timestamp_scale",
    ("timestamptz", 2, "timestamptz"): "timestamptz_scale",
    ("timetz", 1, "timestamptz"): "timestamptz_timetz",
    ("timetz", 1, "time"): "time_timetz",
    ("interval", 1, "time"): "time_interval",
    ("interval", 2, "interval"): "interval_scale",
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
#: An overload's row -> the function a statement called, which names its column.
_CALLED = {
    **{row: name for (name, _, _), row in OVERLOADS.items()},
    **{row: "encode" for row in ("encode_hex", "encode_base64", "encode_escape")},
}
_NAMES = (
    frozenset(name for name, _ in MACROS)
    | {name for name, _, _ in OVERLOADS}
    | {"encode"}
    | engine_refusals.NAMES
)
_MENTIONED = re.compile(
    r"\b(" + "|".join(sorted(_NAMES)) + r")\"?\s*\(|" + pg_forms_json.OPERATORS, re.IGNORECASE
)
_QUALIFIED = re.compile(r"\bpg_catalog\s*\.\s*\"?\w+\"?\s*\(", re.IGNORECASE)
#: The names provisa.pgwire.catalog answers where a statement reads no table.
SERVER_VALUE_NAMES = frozenset(m.name for m in VALUE_FUNCTIONS_OF_THE_SERVER)


#: Functions whose overloads differ by a type every dialect here holds as the same text: only the
#: statement can tell them apart. An unknown-typed literal is the first, as PostgreSQL resolves it.
_TOLD_ONLY_BY_THE_STATEMENT = {"set_masklen": frozenset({"inet", "cidr"})}


def _is_told(name: str, args: list[exp.Expr]) -> bool:
    """Whether a call says which overload it means; a column of cidr does not, and the inet row
    would keep what its mask clears (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED)."""
    types = _TOLD_ONLY_BY_THE_STATEMENT.get(name)
    if types is None:
        return True
    return said_type(args[0]) in types or isinstance(args[0].unnest(), (exp.Literal, exp.Null))


def said_type(node: exp.Expr) -> str | None:
    """The PostgreSQL type the statement says an argument is; None where it does not."""
    if isinstance(node, exp.Paren):
        return said_type(node.this)
    return node.to.sql(dialect="postgres").lower() if isinstance(node, exp.Cast) else None


def type_of(node: exp.Expr) -> str | None:
    """The type a statement says of an argument, or of the column of a VALUES list it reads."""
    return _said_by_values(node) if isinstance(node, exp.Column) else said_type(node)


def _said_by_values(column: exp.Column) -> str | None:
    """The type the first row of the VALUES list ``column`` reads says of it."""
    select = column.find_ancestor(exp.Select)
    source = select.args.get("from_") if select is not None else None
    values = source.this if source is not None else None
    if not isinstance(values, exp.Values) or values.args.get("alias") is None:
        return None
    names = [c.name for c in values.args["alias"].columns]
    if column.name not in names or not values.expressions:
        return None
    row = values.expressions[0]
    cells = row.expressions if isinstance(row, exp.Tuple) else [row]
    return said_type(cells[names.index(column.name)])


def overload(name: str, args: list[exp.Expr]) -> str:
    """The row a call reads: the overload of the type its first argument is said to be, else the
    row of its own name."""
    said = type_of(args[0]) if args else None
    if said is None:
        return name
    return OVERLOADS.get((name, len(args), said.split("(")[0]), name)


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
    return sqlglot.parse_one(form, read="postgres" if isinstance(form, Pg) else dialect)


#: An argument that needs no parentheses wherever a form writes it.
_ATOMIC = (exp.Column, exp.Literal, exp.Func, exp.Cast, exp.Paren, exp.Null, exp.Boolean)


#: The types a row of an overload reads as their text, which is how a dialect without them holds
#: them: once the type has chosen the row, a cast to it says nothing more.
_HELD_AS_TEXT = frozenset(
    {
        "point", "line", "lseg", "box", "path", "polygon", "circle", "tsvector", "inet", "cidr",
        "macaddr", "macaddr8", "pg_lsn", "cid", "record",
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
    if isinstance(node, exp.Cast) and node.to.is_type("char") and node.to.expressions:
        return node.to.expressions[0].this
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


def _repeats_volatile(form: exp.Expr, params: tuple[str, ...], args: list[exp.Expr]) -> bool:
    """Whether the form names twice a parameter whose argument differs each time it is evaluated."""
    named = [c.name for c in form.find_all(exp.Column) if not c.table]
    return any(named.count(p) > 1 and _is_volatile(a) for p, a in zip(params, args))


def _written(macro: Macro, args: list[exp.Expr], dialect: str, node: exp.Expr) -> exp.Expr:
    """The call's form with each parameter its argument. An argument is written wherever the form
    names it, and evaluated each time; one that differs each time is held once (REQ-2171, REWRITE
    AFTER ROUTING). A row of arguments naming no column is the same row for every row of a FROM,
    so that call is refused by name rather than answered with one value for all of them."""
    form = _parsed(macro.name, len(args), dialect).copy()
    held = macro.name in pg_forms_types.HELD
    if _repeats_volatile(form, macro.params, args):
        select = node.find_ancestor(exp.Select)
        reads_rows = select is not None and select.args.get("from_") is not None
        if reads_rows and not any(a.find(exp.Column) is not None for a in args):
            raise FunctionRefused(macro.name, dialect)
        return _in_a_subquery(form, macro.params, [_argument(a, held) for a in args])
    if form.find(exp.Lambda) is not None and any(a.find(exp.Select) is not None for a in args):
        return _in_a_subquery(form, macro.params, [_argument(a) for a in args])
    values = dict(zip(macro.params, args))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name], held).copy()
        return node

    return form.transform(bound)


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
    exp.Ceil: "ceil",
    exp.Cot: "cot",
    exp.Floor: "floor",
}


_REGRESSIONS_READ: dict[type[exp.Expr], str] = {
    getattr(exp, "".join(part.title() for part in macro.name.split("_"))): macro.name
    for macro in _REGRESSIONS
    if macro.name not in ("regr_r2", "regr_count")  # sqlglot has no class of these
}


def _call(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of a call the table may hold."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if type(node) in _READ_AS:
        return _READ_AS[type(node)], [node.this]
    if type(node) in _REGRESSIONS_READ:
        return _REGRESSIONS_READ[type(node)], [node.this, node.expression]
    if isinstance(node, exp.Log):  # sqlglot reads log10(x) as log(10, x)
        base, value = node.this, node.expression
        if value is None:
            return "log", [base]
        return ("log10", [value]) if base.name == "10" else ("log", [base, value])
    if isinstance(node, (exp.Round, exp.Trunc)) and node.args.get("decimals") is None:
        return ("round" if isinstance(node, exp.Round) else "trunc"), [node.this]
    if isinstance(node, exp.Length):  # an overload of a type: length(text) has no row
        return "length", [node.this]
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
    return None


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


def _unqualified(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each pg_catalog.f(...) the call of f, as sqlglot reads it: no engine but
    PostgreSQL and DuckDB has that schema, and sqlglot writes a qualified call as it stands."""
    for dot in reversed(list(tree.find_all(exp.Dot))):
        call = dot.expression
        if not isinstance(dot.this, exp.Identifier) or dot.this.name.lower() != "pg_catalog":
            continue
        if isinstance(call, exp.Anonymous):
            call = exp.func(call.name, *call.expressions, dialect="postgres")
        if isinstance(call, (exp.Func, exp.Binary)):  # mod is a binary as sqlglot reads it
            replaced = dot.replace(call)
            tree = replaced if dot is tree else tree
    return tree


def inline(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` with each call of a function of the table written in its form for ``dialect``.
    Raises :class:`FunctionRefused` for a call of one it has no form of. A call with no overload
    of its argument count is left, for the engine to say so."""
    if dialect in NATIVE:
        return tree
    if dialect not in _DUCKDB:
        tree = _unqualified(tree)
        for node in tree.walk():
            refused_call = engine_refusals.refused_call(node, dialect, type_of)
            if refused_call is not None:
                raise FunctionRefused(refused_call, dialect)
    try:
        tree = pg_forms_json.lowered(tree, dialect)
    except pg_forms_json.NoForm as refused:
        raise FunctionRefused(refused.called, dialect) from refused

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
        called, args = call
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
        # transform does not revisit what a replaced node holds: a call in an argument.
        written = _written(macro, [a.transform(rewrite) for a in args], dialect, node)
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, called)  # named for the function
        return written

    return tree.transform(rewrite)
