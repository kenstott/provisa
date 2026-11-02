# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9e41d7-6a08-4f3b-b7d2-e15a0c83f964
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements of the type operators family (REQ-2171): each type's input, output and send
function, its typmod functions, and the family's support functions, each with the values that tell
a form from a guess: bounds, a text PostgreSQL normalizes, a text it refuses. A statement's answer
is recorded from postgres:17.11 by tests/integration/test_pg17_type_io_recording.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

BOOLEAN, REAL, DOUBLE, NUMERIC = 16, 700, 701, 1700
CLOCKS = frozenset({1083, 1114, 1184, 1266})
ENUM = "type_io_enum"
SESSION = (
    "SET TimeZone TO 'UTC'",
    "SET DateStyle TO 'ISO, MDY'",
    "SET IntervalStyle TO 'postgres'",
    "SET extra_float_digits TO 1",
)
SETUP = (f"DROP TYPE IF EXISTS {ENUM}", f"CREATE TYPE {ENUM} AS ENUM ('a', 'b')")


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def called(function: str, *arguments: str) -> str:
    return f'SELECT pg_catalog."{function}"({", ".join(arguments)}) AS v'


@dataclass(frozen=True)
class Type:
    """A type's three functions. ``typed`` is the type a value is cast to; ``extra`` what an input
    function reads after the text (its oid and typmod), ``good`` texts of the type, ``bad`` texts
    it refuses."""

    in_: str | None
    out: str | None
    send: str | None
    typed: str
    good: tuple[str, ...]
    bad: tuple[str, ...] = ()
    oid: int | None = None
    typmods: tuple[tuple[str, int], ...] = ()


def _t(stem: str, typed: str, good: str, bad: str = "", *, oid: int | None = None) -> Type:
    """A type whose functions are ``stem``in, out and send; values separated by ``|``."""
    return _of(f"{stem}in", f"{stem}out", f"{stem}send", typed, good, bad, oid)


def _u(stem: str, typed: str, good: str, bad: str = "", *, oid: int | None = None) -> Type:
    """A type whose functions are ``stem``_in, _out and _send."""
    return _of(f"{stem}_in", f"{stem}_out", f"{stem}_send", typed, good, bad, oid)


def _of(
    in_: str | None,
    out: str | None,
    send: str | None,
    typed: str,
    good: str,
    bad: str = "",
    oid: int | None = None,
) -> Type:
    return Type(
        in_, out, send, typed, tuple(good.split("|")), tuple(bad.split("|")) if bad else (), oid
    )


_TEXTS = "abc||Hello World|雪|a'b|trailing  |  leading"
TYPES: tuple[Type, ...] = (
    _t("int2", "smallint", "0|1|-1|32767|-32768|  42  |+7", "x|32768|1.5|-32769"),
    _t(
        "int4",
        "integer",
        "0|42|-7|2147483647|-2147483648| 12 |0x1F|1_000|0b101|0o17",
        "x|2147483648|1.5|",
    ),
    _t(
        "int8",
        "bigint",
        "0|5000000000|-5|-9223372036854775808|9223372036854775807",
        "9223372036854775808|x",
    ),
    _t(
        "float4",
        "real",
        "0|-0|0.5|-2.25|100|1.5e10|1e-10|3.4028235e38|1.4e-45|NaN|Infinity|-Infinity|inf|0.1"
        "|1e6|123456.7|1e7|1e-5|0.0001",
        "x|1e39",
    ),
    _t(
        "float8",
        "double precision",
        "0|-0|0.5|-2.25|100|1e15|1e16|1e17|1.5e300|5e-324|0.1|0.30000000000000004|NaN|Infinity"
        "|-Infinity|123456789012345678|1e-4|1e-5",
        "x|1e309",
    ),
    _u(
        "numeric",
        "numeric",
        "0|2.5|-7.125|1000|1.10|0.00|1e3|1.5e-3|NaN|Infinity|-Infinity"
        "|123456789012345678901234567890.123456789|0.000001|-0.5|10000|1.0e-1",
        "x|1.2.3",
        oid=1700,
    ),
    _t("bool", "boolean", "true|false|t|f|yes|no|on|off|1|0| TRUE |y|n", "x|tr u|2"),
    _t("text", "text", _TEXTS),
    _t("varchar", "character varying", _TEXTS, oid=1043),
    _t("bpchar", "character(5)", "abc|abcde||雪|a ", oid=1042),
    _t("name", "name", _TEXTS + "|" + "n" * 70 + "|" + "雪" * 30),
    _t("char", '"char"', "a|B|1||ab|雪"),
    _t("bytea", "bytea", "\\xdeadbeef|abc|\\x00ff||a\\000b|\\\\|\\xDEADBEEF", "\\xz|\\x1|\\9"),
    _t("cid", "cid", "0|1|4294967295"),
    _t("oid", "oid", "0|26|4294967295|-1", "x|4294967296"),
    _t("xid", "xid", "0|3|4294967295"),
    _of("xid8in", "xid8out", None, "xid8", "0|3|18446744073709551615"),
    _t("tid", "tid", "(0,1)|(4294967295,65535)|(0,0)", "x|(1)|(-1,1)"),
    _u(
        "date",
        "date",
        "2024-06-15|1999-12-31|2000-02-29|infinity|-infinity|0001-01-01 BC|June 15 2024"
        "|5874897-12-31|1970-01-01",
        "x|2024-02-30|2024-13-01",
    ),
    _u(
        "time",
        "time without time zone",
        "12:30:45|00:00:00|23:59:59.5|24:00:00|12:30|1:02:03.123456",
        "25:00:00|x",
        oid=1083,
    ),
    _u(
        "timetz",
        "time with time zone",
        "12:30:45+02|00:00:00+00|23:59:59.5-05|12:30:45+05:30|12:30:45.25-11:15:30",
        "25:00:00+00|x",
        oid=1266,
    ),
    _u(
        "timestamp",
        "timestamp without time zone",
        "2024-06-15 12:30:45|1999-12-31 23:59:59|2000-02-29 00:00:00|2000-01-01 00:00:00"
        "|infinity|-infinity|0044-03-15 12:00:00 BC|2024-06-15 12:30:45.123456|1970-01-01",
        "x|2024-02-30 00:00:00",
        oid=1114,
    ),
    _u(
        "timestamptz",
        "timestamp with time zone",
        "2024-06-15 12:30:45+00|1999-12-31 23:59:59+02|2000-02-29 00:00:00-05|infinity|-infinity"
        "|2000-01-01 00:00:00+00|2024-06-15 12:30:45.5+05:30",
        "x",
        oid=1184,
    ),
    _u(
        "interval",
        "interval",
        "1 day 02:03:04|3 mons|-1 year 2 days|1 year 2 mons 3 days 04:05:06.789|00:00:00"
        "|-00:00:01|100 hours|P1Y2M3DT4H5M6S|1 day -01:00:00|infinity|-infinity|13 mons|0.5 days",
        "x",
        oid=1186,
    ),
    _u(
        "uuid",
        "uuid",
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|00000000-0000-0000-0000-000000000000"
        "|ffffffff-ffff-ffff-ffff-ffffffffffff|A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"
        "|{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}|a0eebc999c0b4ef8bb6d6bb9bd380a11",
        "x|a0eebc99-9c0b-4ef8-bb6d",
    ),
    _u(
        "json",
        "json",
        '{"a": 1, "b": [1, 2]}|[1, "two", null]|3|"s"|{"b":1,"a":2}|  [1]  |null|true|{"a":1,"a":2}',
        "{|x",
    ),
    _u(
        "jsonb",
        "jsonb",
        '{"a": 1, "b": [1, 2]}|[1, "two", null]|3|"s"|{"bb":1,"a":2}|  [1]  |null|true'
        '|{"a":1,"a":2}|1.50|{"é":1}',
        "{|x",
    ),
    _u("jsonpath", "jsonpath", "$.a|$[0]|$|$.a ? (@ > 1)|$.a[*]|lax $.b", "x y|$."),
    _of("xml_in", "xml_out", None, "xml", "<a/>|<a>b</a>|text|<a  b='c'/>", "<a|<a></b>"),
    _u(
        "inet",
        "inet",
        "192.168.0.1/24|10.1.2.3|::1|10.1.2.3/32|2001:db8::1/64|::ffff:1.2.3.4",
        "x|300.1.1.1",
    ),
    _u(
        "cidr",
        "cidr",
        "192.168.0.0/24|10.0.0.0/8|::1/128|10.0.0.0|2001:db8::/32",
        "x|192.168.0.1/24",
    ),
    _u(
        "macaddr",
        "macaddr",
        "08:00:2b:01:02:03|00:00:00:00:00:00|ff:ff:ff:ff:ff:ff|08-00-2B-01-02-03|0800.2b01.0203"
        "|08002b010203",
        "x|08:00:2b",
    ),
    _u(
        "macaddr8",
        "macaddr8",
        "08:00:2b:01:02:03:04:05|00:00:00:00:00:00:00:00|ff:ff:ff:ff:ff:ff:ff:ff|08:00:2b:01:02:03",
        "x",
    ),
    _u("point", "point", "(1,2)|(0,0)|(-3.5,4)|1,2| ( 1.5 , 2.5 ) |(1e300,1)", "x|(1)"),
    _u("line", "line", "{1,2,3}|{1,0,0}|{0,1,-2}|[(0,0),(1,1)]", "x|{0,0,1}"),
    _u("lseg", "lseg", "[(0,0),(3,4)]|[(1,1),(1,5)]|((0,0),(1,1))|0,0,1,1", "x"),
    _u("box", "box", "(1,1),(0,0)|(0,0),(1,1)|((2,3),(4,5))|1,2,3,4", "x"),
    _u("path", "path", "((0,0),(1,1),(2,0))|[(0,0),(1,1)]|(0,0),(1,1)", "x"),
    _of("poly_in", "poly_out", "poly_send", "polygon", "((0,0),(1,1),(2,0))|(0,0),(1,1)", "x"),
    _u("circle", "circle", "<(0,0),1>|<(1.5,-2),3>|((0,0),1)|0,0,1", "x|<(0,0),-1>"),
    _u("bit", "bit(3)", "101|000|111", "2|x", oid=1560),
    _u("varbit", "bit varying", "101||0|11111111|111111111", "2|x", oid=1562),
    _t("tsvector", "tsvector", "a b|'a b' c|a:1 b:2,3|b a|a:1A|''''", ""),
    _t("tsquery", "tsquery", "a|a & b|a | b|!a|a <-> b|'a b'", "a &|&"),
    _of("int2vectorin", "int2vectorout", None, "int2vector", "1 2 3||0|-1", "x"),
    _of("oidvectorin", "oidvectorout", None, "oidvector", "1 2 3||0", "x"),
    _u("pg_lsn", "pg_lsn", "16/B374D848|0/0|FFFFFFFF/FFFFFFFF", "x|1"),
    _of(
        "pg_snapshot_in", "pg_snapshot_out", None, "pg_snapshot", "10:20:10,14,15|1:1:", "x|20:10:"
    ),
    _of("txid_snapshot_in", "txid_snapshot_out", None, "txid_snapshot", "10:20:10,14,15|1:1:", "x"),
    _of("aclitemin", "aclitemout", None, "aclitem", "postgres=arwd/postgres|=r/postgres", "x"),
    _of("cstring_in", "cstring_out", "cstring_send", "cstring", _TEXTS),
    _of("unknownin", "unknownout", "unknownsend", "unknown", "abc||雪"),
    _of("void_in", "void_out", "void_send", "void", ""),
    _of(
        "regclassin",
        "regclassout",
        None,
        "regclass",
        "pg_class|pg_catalog.pg_type|-|1259",
        "no_such",
    ),
    _of("regtypein", "regtypeout", None, "regtype", "integer|int4|text|-|23", "no_such"),
    _of("regprocin", "regprocout", None, "regproc", "now|-|1299", "no_such"),
    _of(
        "regprocedurein",
        "regprocedureout",
        None,
        "regprocedure",
        "now()|abs(integer)|-",
        "no_such()",
    ),
    _of("regoperin", "regoperout", None, "regoper", "!~~|-|", "no_such"),
    _of(
        "regoperatorin",
        "regoperatorout",
        None,
        "regoperator",
        "+(integer,integer)|-",
        "+(no_such,x)",
    ),
    _of(
        "regnamespacein",
        "regnamespaceout",
        None,
        "regnamespace",
        "pg_catalog|public|-|11",
        "no_such",
    ),
    _of("regrolein", "regroleout", None, "regrole", "postgres|-|10", "no_such"),
    _of("regconfigin", "regconfigout", None, "regconfig", "simple|english|-", "no_such"),
    _of("regdictionaryin", "regdictionaryout", None, "regdictionary", "simple|-", "no_such"),
    _of("regcollationin", "regcollationout", None, "regcollation", '"C"|"default"|-', "no_such"),
)

#: Each type with a typmod, and what its functions read.
TYPMODS: tuple[tuple[str, tuple[str, ...], tuple[int, ...]], ...] = (
    (
        "numeric",
        ("{10,2}", "{10}", "{1000,0}", "{1001}", "{10,-5}", "{10,20}", "{0}", "{1,2,3}", "{}"),
        (-1, 655366, 655364, 65540, 4, 3, 0),
    ),
    (
        "varchar",
        ("{10}", "{1}", "{0}", "{10485760}", "{10485761}", "{1,2}", "{-1}"),
        (-1, 14, 5, 4),
    ),
    ("bpchar", ("{10}", "{1}", "{0}", "{10485761}", "{1,2}"), (-1, 14, 5, 4)),
    ("bit", ("{3}", "{0}", "{83886080}", "{1,2}", "{-1}"), (-1, 3, 0)),
    ("varbit", ("{3}", "{0}", "{1,2}"), (-1, 3, 0)),
    ("time", ("{3}", "{0}", "{6}", "{7}", "{-1}", "{1,2}"), (-1, 3, 0, 6)),
    ("timetz", ("{3}", "{0}", "{6}", "{7}", "{-1}"), (-1, 3, 0, 6)),
    ("timestamp", ("{3}", "{0}", "{6}", "{7}", "{-1}"), (-1, 3, 0, 6)),
    ("timestamptz", ("{3}", "{0}", "{6}", "{7}", "{-1}"), (-1, 3, 0, 6)),
    (
        "interval",
        ("{32767}", "{4}", "{8}", "{12}", "{1024}", "{4,3}", "{3072,6}", "{9}", "{1,2,3}", "{4,7}"),
        (-1, 2147418111, 327679, 262147, 67108863, 201392127),
    ),
)

#: The three-argument input functions over a type named by its oid, and what they refuse.
_COMPOSED = (
    (
        "array_in",
        23,
        ("{1,2,3}", "{{1,2},{3,4}}", "{}", "{1,NULL}", "[0:1]={1,2}", "{1,x}", "{1", "x"),
    ),
    ("array_in", 25, ('{a,"b c",NULL}', '{"x\\"y"}', "{}")),
    ("range_in", 3904, ("[1,5)", "(1,5]", "empty", "[,5)", "[1,)", "(,)", "[5,1)", "x", "[1,1)")),
    ("range_in", 3908, ("[2024-01-01,2024-02-01)", "empty")),
    ("multirange_in", 4451, ("{[1,5),[7,9)}", "{}", "{[1,5),[3,7)}", "x")),
    ("record_in", 2249, ("(1,2)",)),
    ("domain_in", 23, ("1",)),
    ("anycompatiblerange_in", 3904, ("[1,5)",)),
    ("anycompatiblemultirange_in", 4451, ("{[1,5)}",)),
    ("anyrange_in", 3904, ("[1,5)",)),
    ("anymultirange_in", 4451, ("{[1,5)}",)),
)
_ARRAYS = (
    "'{1,2,3}'::integer[]",
    "'{{1,2},{3,4}}'::integer[]",
    "'{}'::integer[]",
    "'{1,NULL}'::integer[]",
    "'{a,\"b c\",NULL}'::text[]",
    "'{1.5,2}'::numeric[]",
    "'{t,f}'::boolean[]",
    "'[0:1]={1,2}'::integer[]",
)
_RANGES = (
    "'[1,5)'::int4range",
    "'empty'::int4range",
    "'[,5)'::int4range",
    "'[1,)'::int8range",
    "'[1.5,2.5]'::numrange",
    "'[2024-01-01,2024-02-01)'::daterange",
)
_MULTIRANGES = ("'{[1,5),[7,9)}'::int4multirange", "'{}'::int4multirange")
_RECORDS = ("ROW(1, 'a')", "ROW(NULL, 'b c')", "ROW(1.5, true)", "ROW()")
#: The pseudo types with no value: a text in is refused, as a value out.
_PSEUDO_IN = (
    "any_in", "anyarray_in", "anycompatible_in", "anycompatiblearray_in",
    "anycompatiblenonarray_in", "anyelement_in", "anyenum_in", "anynonarray_in",
    "pg_ddl_command_in", "pg_dependencies_in", "pg_mcv_list_in", "pg_ndistinct_in",
    "pg_node_tree_in",
)  # fmt: skip
_PSEUDO_OUT = {
    "any_out": ("1::integer",),
    "anycompatible_out": ("1::integer",),
    "anycompatiblenonarray_out": ("1::integer",),
    "anyelement_out": ("1::integer", "'a'::text"),
    "anynonarray_out": ("1::integer", "'a'::text"),
    "anyarray_out": _ARRAYS,
    "anycompatiblearray_out": _ARRAYS,
    "array_out": _ARRAYS,
    "anyrange_out": _RANGES,
    "anycompatiblerange_out": _RANGES,
    "range_out": _RANGES,
    "anymultirange_out": _MULTIRANGES,
    "anycompatiblemultirange_out": _MULTIRANGES,
    "multirange_out": _MULTIRANGES,
    "record_out": _RECORDS,
    "anyarray_send": _ARRAYS,
    "anycompatiblearray_send": _ARRAYS,
    "array_send": _ARRAYS,
    "range_send": _RANGES,
    "multirange_send": _MULTIRANGES,
    "record_send": _RECORDS,
    "anyenum_out": (f"'a'::{ENUM}",),
    "enum_out": (f"'a'::{ENUM}",),
    "enum_send": (f"'a'::{ENUM}",),
    "jsonb_out": (),
}
#: Each call of the family's support functions.
_FLOATS = "'{4,10,30}'::double precision[]"
_STATES = "'{4,10,30,20,120,70}'::double precision[]"
_SUPPORT: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("float8_avg", (_FLOATS, "'{0,0,0}'::double precision[]", "'{1,2}'::double precision[]")),
    *((name, (_FLOATS, "'{1,2,0}'::double precision[]", "'{0,0,0}'::double precision[]"))
      for name in ("float8_var_pop", "float8_var_samp", "float8_stddev_pop", "float8_stddev_samp")),
    *((name, (_STATES, "'{0,0,0,0,0,0}'::double precision[]", "'{1,2,3,4,5,6}'::double precision[]",
              "'{1,2}'::double precision[]"))
      for name in ("float8_corr", "float8_covar_pop", "float8_covar_samp", "float8_regr_avgx",
                   "float8_regr_avgy", "float8_regr_intercept", "float8_regr_r2",
                   "float8_regr_slope", "float8_regr_sxx", "float8_regr_sxy", "float8_regr_syy")),
    ("int8_avg", ("'{4,10}'::bigint[]", "'{0,0}'::bigint[]", "'{1}'::bigint[]")),
)  # fmt: skip
_CALLS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("int8_sum", ("1.5::numeric, 3::bigint", "NULL::numeric, 3::bigint", "1::numeric, NULL::bigint")),
    ("int8inc_float8_float8", ("1::bigint, 2::double precision, 3::double precision",
                               "1::bigint, NULL::double precision, 3::double precision")),
    ("numeric_div", ("1::numeric, 3::numeric", "10::numeric, 4::numeric", "1::numeric, 0::numeric",
                     "1.50::numeric, 0.5::numeric", "'NaN'::numeric, 1::numeric")),
    ("numeric_div_trunc", ("7::numeric, 2::numeric", "-7::numeric, 2::numeric", "1::numeric, 0::numeric")),
    ("numeric_exp", ("0::numeric", "1::numeric", "2.5::numeric", "(-1)::numeric")),
    ("numeric_ln", ("1::numeric", "2.5::numeric", "0::numeric", "(-1)::numeric")),
    ("numeric_log", ("10::numeric, 1000::numeric", "2::numeric, 8::numeric", "2::numeric, 0::numeric")),
    ("numeric_power", ("2::numeric, 10::numeric", "2.5::numeric, 2::numeric", "2::numeric, 0.5::numeric",
                       "0::numeric, (-1)::numeric")),
    ("numeric_sqrt", ("2::numeric", "16::numeric", "0::numeric", "(-1)::numeric", "2.25::numeric")),
    ("inet_merge", ("'192.168.1.5/24'::inet, '192.168.2.5/24'::inet",
                    "'10.0.0.1'::inet, '10.0.0.2'::inet", "'10.0.0.1'::inet, '::1'::inet")),
    ("interval_hash", ("'1 day'::interval", "'3 mons'::interval", "'0'::interval")),
    ("interval_hash_extended", ("'1 day'::interval, 0::bigint", "'1 day'::interval, 7::bigint")),
    ("pg_lsn_hash", ("'16/B374D848'::pg_lsn", "'0/0'::pg_lsn")),
    ("pg_lsn_hash_extended", ("'16/B374D848'::pg_lsn, 0::bigint", "'0/0'::pg_lsn, 7::bigint")),
    ("time_hash", ("'12:30:45'::time", "'00:00:00'::time")),
    ("time_hash_extended", ("'12:30:45'::time, 0::bigint", "'00:00:00'::time, 7::bigint")),
    ("timetz_hash", ("'12:30:45+02'::timetz", "'00:00:00+00'::timetz")),
    ("timetz_hash_extended", ("'12:30:45+02'::timetz, 0::bigint", "'00:00:00+00'::timetz, 7::bigint")),
    ("timestamp_hash", ("'2024-06-15 12:30:45'::timestamp", "'2000-01-01'::timestamp", "'infinity'::timestamp")),
    ("timestamp_hash_extended", ("'2024-06-15 12:30:45'::timestamp, 0::bigint", "'2000-01-01'::timestamp, 7::bigint")),
    ("uuid_hash", ("'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid", "'00000000-0000-0000-0000-000000000000'::uuid")),
    ("uuid_hash_extended", ("'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, 0::bigint",
                            "'00000000-0000-0000-0000-000000000000'::uuid, 7::bigint")),
    ("enum_in", (f"'a'::cstring, '{ENUM}'::regtype::oid", f"'x'::cstring, '{ENUM}'::regtype::oid")),
    ("anyenum_in", ("'a'::cstring",)),
    ("shell_out", ("NULL::void",)),
    ("pg_ddl_command_out", ("NULL::pg_ddl_command",)),
    ("pg_ddl_command_send", ("NULL::pg_ddl_command",)),
    ("pg_dependencies_out", ("NULL::pg_dependencies",)),
    ("pg_mcv_list_out", ("NULL::pg_mcv_list",)),
    ("pg_ndistinct_out", ("NULL::pg_ndistinct",)),
    ("pg_node_tree_out", ("NULL::pg_node_tree",)),
    ("json_out", ()),
)  # fmt: skip


def _input(kind: Type, text: str) -> list[str]:
    literal = f"{_literal(text)}::cstring"
    if kind.oid is None:
        return [called(kind.in_, literal)]
    return [called(kind.in_, literal, f"{kind.oid}::oid", "(-1)::integer")]


def statements() -> list[str]:
    """Every statement, once, in a stable order."""
    written: list[str] = []
    for kind in TYPES:
        for text in kind.good:
            if kind.in_:
                written.extend(_input(kind, text))
            literal = f"{_literal(text)}::{kind.typed}"
            if kind.out:
                written.append(called(kind.out, literal))
            if kind.send:
                written.append(called(kind.send, literal))
        for text in kind.bad:
            written.extend(_input(kind, text))
        if kind.in_:
            nulls = ("NULL::cstring",) if kind.oid is None else (
                "NULL::cstring", "NULL::oid", "NULL::integer",
            )  # fmt: skip
            written.append(called(kind.in_, *nulls))
        if kind.out:
            written.append(called(kind.out, f"NULL::{kind.typed}"))
        if kind.send:
            written.append(called(kind.send, f"NULL::{kind.typed}"))
    written.extend(
        called("bpcharin", f"{_literal(t)}::cstring", "1042::oid", f"{m}::integer")
        for t in ("abc", "abcdefgh", "abc   ", "abcde   ")
        for m in (9, 7)
    )
    written.extend(
        called("varcharin", f"{_literal(t)}::cstring", "1043::oid", f"{m}::integer")
        for t in ("abc", "abcdefgh", "abc   ")
        for m in (9, 6)
    )
    written.extend(
        called("numeric_in", f"{_literal(t)}::cstring", "1700::oid", f"{m}::integer")
        for t in ("1234.567", "123456789.5", "1.005", "NaN", "Infinity")
        for m in (393222, 655366)
    )
    written.extend(
        called(name, f"{_literal(t)}::cstring", f"{o}::oid", f"{m}::integer")
        for name, o, t, m in (
            ("time_in", 1083, "12:30:45.123456", 3),
            ("timetz_in", 1266, "12:30:45.123456+02", 3),
            ("timestamp_in", 1114, "2024-06-15 12:30:45.123456", 0),
            ("timestamptz_in", 1184, "2024-06-15 12:30:45.123456+00", 2),
            ("interval_in", 1186, "1 year 2 mons 3 days", 2147418111),
            ("interval_in", 1186, "1 year 2 mons 3 days", 327679),
            ("bit_in", 1560, "1", 1),
            ("bit_in", 1560, "101", 3),
            ("bit_in", 1560, "101", 4),
            ("varbit_in", 1562, "101", 3),
            ("varbit_in", 1562, "1010", 3),
        )
    )
    for stem, typmods, numbers in TYPMODS:
        for text in typmods:
            written.append(called(f"{stem}typmodin", f"{_literal(text)}::cstring[]"))
        written.append(called(f"{stem}typmodin", "NULL::cstring[]"))
        written.extend(called(f"{stem}typmodout", f"({n})::integer") for n in numbers)
        written.append(called(f"{stem}typmodout", "NULL::integer"))
    for name, oid, texts in _COMPOSED:
        written.extend(
            called(name, f"{_literal(t)}::cstring", f"{oid}::oid", "(-1)::integer") for t in texts
        )
        written.append(called(name, "NULL::cstring", f"{oid}::oid", "(-1)::integer"))
    for name in _PSEUDO_IN:
        written.append(called(name, "'x'::cstring"))
        written.append(called(name, "NULL::cstring"))
    for name, values in _PSEUDO_OUT.items():
        written.extend(called(name, value) for value in values)
    for name, values in _SUPPORT:
        written.extend(called(name, value) for value in values)
        written.append(called(name, f"NULL::{values[0].rsplit('::', 1)[1]}"))
    for name, arguments in _CALLS:
        written.extend(called(name, a) for a in arguments)
    return list(dict.fromkeys(written))


#: Statements of each function refused by name, on the engine that refuses the most: a value a form
#: cannot read as PostgreSQL does. A slice may lower a number, never raise it.
REFUSALS: dict[str, int] = {
    "aclitemin": 3,
    "aclitemout": 2,
    "array_in": 11,
    "bit_in": 1,
    "box_in": 5,
    "box_out": 4,
    "box_send": 4,
    "byteain": 5,
    "byteaout": 2,
    "byteasend": 2,
    "charin": 1,
    "charout": 1,
    "charsend": 1,
    "cidr_in": 5,
    "cidr_out": 3,
    "circle_in": 4,
    "circle_out": 2,
    "circle_send": 2,
    "date_in": 12,
    "date_out": 9,
    "date_send": 9,
    "domain_in": 2,
    "enum_in": 2,
    "float4out": 1,
    "float8in": 1,
    "float8out": 1,
    "float8send": 1,
    "inet_in": 6,
    "inet_merge": 3,
    "inet_out": 4,
    "int2vectorin": 1,
    "int8_avg": 3,
    "int8inc_float8_float8": 1,
    "interval_hash": 3,
    "interval_hash_extended": 2,
    "interval_in": 2,
    "jsonpath_in": 8,
    "jsonpath_out": 6,
    "jsonpath_send": 6,
    "line_in": 6,
    "line_out": 4,
    "line_send": 4,
    "lseg_in": 3,
    "lseg_out": 2,
    "lseg_send": 2,
    "macaddr8_in": 2,
    "macaddr8_out": 1,
    "macaddr8_send": 1,
    "macaddr_in": 2,
    "multirange_in": 4,
    "namein": 1,
    "nameout": 1,
    "namesend": 1,
    "numeric_div": 5,
    "numeric_div_trunc": 3,
    "numeric_exp": 4,
    "numeric_in": 6,
    "numeric_ln": 4,
    "numeric_log": 3,
    "numeric_power": 4,
    "numeric_sqrt": 5,
    "oidvectorin": 1,
    "path_in": 2,
    "path_out": 1,
    "path_send": 1,
    "pg_lsn_in": 2,
    "pg_snapshot_in": 4,
    "pg_snapshot_out": 2,
    "point_in": 5,
    "point_out": 3,
    "point_send": 3,
    "poly_in": 2,
    "poly_out": 1,
    "poly_send": 1,
    "range_in": 11,
    "record_send": 4,
    "regclassin": 5,
    "regclassout": 4,
    "regcollationin": 4,
    "regcollationout": 3,
    "regconfigin": 4,
    "regconfigout": 3,
    "regdictionaryin": 3,
    "regdictionaryout": 2,
    "regnamespacein": 5,
    "regnamespaceout": 4,
    "regoperatorin": 3,
    "regoperatorout": 2,
    "regoperin": 4,
    "regoperout": 3,
    "regprocedurein": 4,
    "regprocedureout": 3,
    "regprocin": 4,
    "regprocout": 3,
    "regrolein": 4,
    "regroleout": 3,
    "regtypein": 6,
    "regtypeout": 5,
    "tidin": 3,
    "time_in": 9,
    "time_out": 6,
    "time_send": 6,
    "timestamp_hash": 3,
    "timestamp_hash_extended": 2,
    "timestamp_in": 12,
    "timestamp_out": 9,
    "timestamp_send": 9,
    "timestamptz_in": 9,
    "timestamptz_out": 7,
    "timestamptz_send": 7,
    "timetz_in": 8,
    "timetz_out": 5,
    "timetz_send": 5,
    "tsqueryin": 9,
    "tsqueryout": 7,
    "tsquerysend": 7,
    "txid_snapshot_in": 3,
    "txid_snapshot_out": 2,
    "uuid_in": 2,
    "varbit_in": 1,
    "xid8in": 3,
    "xid8out": 3,
    "xml_in": 6,
    "xml_out": 4,
}
NAME_OF_THE_REFUSAL = "does not read this value"
REFUSED = "refused by name"


def verdict(
    sql: str, want: tuple[int | None, str | None], got: Any, said: str | None
) -> str | None:
    """How an engine's answer differs from PostgreSQL's: ``got``, or ``said``, what it raised as
    "SQLSTATE: message". REFUSED for a refusal by name; None where it does not differ."""
    oid, text = want
    if said is not None:
        if oid is None and said == text:
            return None
        if NAME_OF_THE_REFUSAL in said:
            return REFUSED
        return f"raised {said}, PostgreSQL {text!r}"
    if oid is None:
        return f"answered {got!r}, PostgreSQL refused: {text}"
    return None if same_answer(got, oid, text) else f"{got!r}, PostgreSQL {text!r}"


#: The functions whose forms read a column too, where a literal of the same value is folded: each
#: statement of theirs, its arguments a row's columns, has the answer its literal call recorded.
IN_COLUMNS = frozenset(
    {
        *(f"{stem}_hash{extended}" for stem in ("time", "timestamp", "timetz", "uuid", "pg_lsn")
          for extended in ("", "_extended")),
        *(name for name, _ in _SUPPORT if name.startswith("float8_")),
    }
)  # fmt: skip
_FLOAT8_ARRAY = re.compile(r"'\{([^}]*)\}'::double precision\[\]")
#: A timetz's zone of hours, which Trino's cast reads only with its minutes: the same value.
_ZONE_OF_HOURS = re.compile(r"([+-][0-9]{2})'::timetz")


def _arguments(text: str) -> list[str]:
    """A call's arguments, split at the commas outside quotes and parentheses."""
    found, depth, quoted, start = [], 0, False, 0
    for at, char in enumerate(text):
        quoted ^= char == "'"
        if not quoted:
            depth += (char in "([") - (char in ")]")
            if char == "," and depth == 0:
                found.append(text[start:at].strip())
                start = at + 1
    return [*found, text[start:].strip()]


def in_columns(sql: str) -> str:
    """``sql`` with its call's arguments read from a row: no literal reaches the form. An array of
    float8s is written by ARRAY[], whose elements no engine's cast of a text need read."""
    function = function_of(sql)
    inner = sql.split(f'"{function}"(', 1)[1].removesuffix(") AS v")
    columns = [
        _ZONE_OF_HOURS.sub(
            r"\1:00'::timetz", _FLOAT8_ARRAY.sub(r"ARRAY[\1]::double precision[]", a)
        )
        for a in _arguments(inner)
    ]
    names = [f"a{at}" for at in range(len(columns))]
    row = ", ".join(f"{c} AS {n}" for c, n in zip(columns, names, strict=True))
    return f'SELECT pg_catalog."{function}"({", ".join(names)}) AS v FROM (SELECT {row}) AS t'


#: What a column of a dialect cannot hold: a Trino or ClickHouse timestamp holds no infinity, and
#: ClickHouse has no timetz.
_NO_COLUMN = {
    "duckdb": (),
    "trino": ("'infinity'::timestamp",),
    "clickhouse": ("'infinity'::timestamp", "::timetz"),
}


def column_statements(recorded: Iterable[str], dialect: str) -> dict[str, str]:
    """Each statement of IN_COLUMNS whose value a column of ``dialect`` holds, as in_columns writes
    it -> the statement whose answer it has."""
    return {
        in_columns(sql): sql
        for sql in recorded
        if function_of(sql) in IN_COLUMNS
        and "(NULL::" not in sql
        and not any(held in sql for held in _NO_COLUMN[dialect])
    }


def function_of(sql: str) -> str:
    """The function a statement calls."""
    return sql.split('"')[1]


def by_function(sqls: Iterable[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for sql in sqls:
        grouped.setdefault(function_of(sql), []).append(sql)
    return grouped


def _clock(text: str) -> str:
    """A time's or timestamp's text as an engine's: no zone of UTC, no zeros after its seconds."""
    bare = text.removesuffix("+00:00").removesuffix("+00").removesuffix(" UTC")
    return re.sub(r"\.?0+$", "", bare) if "." in bare else bare


def _as_text(got: Any) -> str:
    if isinstance(got, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(got).hex()
    if isinstance(got, bool):
        return "t" if got else "f"
    return str(got)


def same_answer(got: Any, oid: int, want: str | None) -> bool:
    """Whether an engine's value is PostgreSQL's, read as the type's text. A float compares as its
    width's bits, a numeric by value: the wire writes each."""
    if want is None or got is None:
        return want is got
    text = _as_text(got)
    if oid == BOOLEAN:
        return text.lower().startswith(want)
    if oid == REAL or oid == DOUBLE:
        pack = "f" if oid == REAL else "d"
        return struct.pack(pack, float(text)) == struct.pack(pack, float(want))
    if oid == NUMERIC:
        return Decimal(text) == Decimal(want)
    if oid in CLOCKS:
        return _clock(text) == _clock(want)
    return text == want
