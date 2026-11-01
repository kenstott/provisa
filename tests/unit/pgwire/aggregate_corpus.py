# Copyright (c) 2026 Kenneth Stott
# Canary: e4a7c613-0f92-4b58-97d2-38c1b6f50a7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements that prove PostgreSQL's aggregate state functions as forms (REQ-2171): each
function's NULLs, its steps from an empty and a filled state, an infinity and a NaN in a value and in
the state, a state of the wrong size and an overflow. Beside them the index access methods' support
functions a client can call."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import math
from typing import Any

from provisa.core.pg_error import MARK

_F3 = "double precision[]"
_THREE = ("{0,0,0}", "{1,2,0}", "{2,3,0.5}", "{1,Infinity,NaN}", "{3,6,2}")
_SIX = ("{0,0,0,0,0,0}", "{1,2,0,3,0,0}", "{2,3,0.5,4,0.5,0.5}", "{1,Infinity,NaN,1,0,NaN}")
_FLOATS = ("0.5", "(-2.25)", "'Infinity'", "'NaN'", "100")


def _state(text: str) -> str:
    return f"'{text}'::{_F3}"


def _call(function: str, *args: str) -> str:
    return f"SELECT {function}({', '.join(args)}) AS v"


def statements() -> list[str]:
    """Every statement, each of one call."""
    out = [
        _call("booland_statefunc", "true::boolean", "false::boolean"),
        _call("booland_statefunc", "true::boolean", "true::boolean"),
        _call("booland_statefunc", "NULL::boolean", "true::boolean"),
        _call("boolor_statefunc", "false::boolean", "false::boolean"),
        _call("boolor_statefunc", "true::boolean", "false::boolean"),
        _call("boolor_statefunc", "false::boolean", "NULL::boolean"),
        _call("any_value_transfn", "3::integer", "4::integer"),
        _call("any_value_transfn", "NULL::integer", "4::integer"),
        _call("any_value_transfn", "3::integer", "NULL::integer"),
        _call("int4_avg_combine", "NULL::bigint[]", "NULL::bigint[]"),
        _call("int4_avg_combine", "'{1,2}'::bigint[]", "'{3,4}'::bigint[]"),
        _call("range_intersect_agg_transfn", "NULL::int4range", "NULL::int4range"),
        _call("range_intersect_agg_transfn", "'[1,5)'::int4range", "'[2,3)'::int4range"),
        _call("multirange_intersect_agg_transfn", "NULL::int4multirange", "NULL::int4multirange"),
        _call(
            "multirange_intersect_agg_transfn",
            "'{[1,5)}'::int4multirange",
            "'{[2,3)}'::int4multirange",
        ),
    ]
    for function, cast in (
        ("int2_avg_accum", "smallint"),
        ("int4_avg_accum", "integer"),
        ("int2_avg_accum_inv", "smallint"),
        ("int4_avg_accum_inv", "integer"),
    ):
        for state in ("{0,0}", "{2,10}", "{5,-3}"):
            out.append(_call(function, f"'{state}'::bigint[]", f"(-7)::{cast}"))
        out.append(_call(function, "NULL::bigint[]", f"3::{cast}"))
        out.append(_call(function, "'{1,2}'::bigint[]", f"NULL::{cast}"))
        out.append(_call(function, "'{1,2,3}'::bigint[]", f"3::{cast}"))
        out.append(_call(function, "'{}'::bigint[]", f"3::{cast}"))
        out.append(_call(function, "'{1,NULL}'::bigint[]", f"3::{cast}"))
    for function, cast in (("float8_accum", "double precision"), ("float4_accum", "real")):
        for state in _THREE:
            for value in _FLOATS:
                out.append(_call(function, _state(state), f"{value}::{cast}"))
        out.append(_call(function, f"NULL::{_F3}", f"1::{cast}"))
        out.append(_call(function, _state("{0,0,0}"), f"NULL::{cast}"))
        out.append(_call(function, _state("{1,2}"), f"1::{cast}"))
        out.append(_call(function, _state("{1,NULL,3}"), f"1::{cast}"))
    out.append(_call("float8_accum", _state("{1,1e308,0}"), "1.7e308::double precision"))
    for a in _THREE:
        for b in _THREE:
            out.append(_call("float8_combine", _state(a), _state(b)))
    out.append(_call("float8_combine", f"NULL::{_F3}", _state("{0,0,0}")))
    out.append(_call("float8_combine", _state("{1,2}"), _state("{0,0,0}")))
    out.append(_call("float8_combine", _state("{1,1e308,0}"), _state("{1,1e308,0}")))
    for state in _SIX:
        for y in _FLOATS:
            for x in ("1.5", "'Infinity'", "'NaN'"):
                out.append(
                    _call(
                        "float8_regr_accum",
                        _state(state),
                        f"{y}::double precision",
                        f"{x}::double precision",
                    )
                )
    out.append(
        _call("float8_regr_accum", f"NULL::{_F3}", "1::double precision", "2::double precision")
    )
    out.append(
        _call(
            "float8_regr_accum",
            _state("{0,0,0,0,0}"),
            "1::double precision",
            "2::double precision",
        )
    )
    out.append(
        _call(
            "float8_regr_accum",
            _state("{1,1e308,0,1,0,0}"),
            "1::double precision",
            "1.7e308::double precision",
        )
    )
    for a in _SIX:
        for b in _SIX:
            out.append(_call("float8_regr_combine", _state(a), _state(b)))
    out.append(_call("float8_regr_combine", f"NULL::{_F3}", _state("{0,0,0,0,0,0}")))
    return out


_TEXTS = ("abc", "abd", "abcd", "", "12", "Hello World", "héllo")
_INTEGER_KINDS = (
    ("btint4cmp", "integer", "integer"),
    ("btint8cmp", "bigint", "bigint"),
    ("btint24cmp", "smallint", "integer"),
    ("btint28cmp", "smallint", "bigint"),
    ("btint42cmp", "integer", "smallint"),
    ("btint48cmp", "integer", "bigint"),
    ("btint82cmp", "bigint", "smallint"),
    ("btint84cmp", "bigint", "integer"),
    ("btoidcmp", "oid", "oid"),
    ("btint2cmp", "smallint", "smallint"),
)
_FLOAT_KINDS = (
    ("btfloat4cmp", "real", "real"),
    ("btfloat8cmp", "double precision", "double precision"),
    ("btfloat48cmp", "real", "double precision"),
    ("btfloat84cmp", "double precision", "real"),
)
_TEXT_KINDS = (
    ("btnamecmp", "name", "name"),
    ("btnametextcmp", "name", "text"),
    ("bttextnamecmp", "text", "name"),
    ("bttext_pattern_cmp", "text", "text"),
    ("btbpchar_pattern_cmp", "character(12)", "character(12)"),
    ("gin_cmp_tslexeme", "text", "text"),
    ("gin_compare_jsonb", "text", "text"),
)


def _pairs(function: str, left: str, right: str, values: tuple[str, ...]) -> list[str]:
    out = [_call(function, f"{a}::{left}", f"{b}::{right}") for a in values for b in values]
    out.append(_call(function, f"NULL::{left}", f"{values[0]}::{right}"))
    out.append(_call(function, f"{values[0]}::{left}", f"NULL::{right}"))
    return out


def index_statements() -> list[str]:
    """The index access methods' functions a client can call."""
    out: list[str] = []
    for function, left, right in _INTEGER_KINDS:
        values = ("1", "7", "0") if left == "oid" else ("3", "(-7)", "0")
        out.extend(_pairs(function, left, right, values))
    for function, left, right in _FLOAT_KINDS:
        out.extend(_pairs(function, left, right, ("0.5", "(-2.25)", "'NaN'", "'Infinity'")))
    for function, left, right in _TEXT_KINDS:
        out.extend(_pairs(function, left, right, tuple(f"'{v}'" for v in _TEXTS)))
    out.extend(_pairs("btboolcmp", "boolean", "boolean", ("true", "false")))
    out.extend(_pairs("bttextcmp", "text", "text", ("'abc'", "'abd'", "'12'")))
    out.extend(_pairs("btcharcmp", '"char"', '"char"', ("'a'", "'z'", "'1'")))
    out.extend(_pairs("bttidcmp", "tid", "tid", ("'(0,1)'", "'(1,0)'", "'(1,5)'")))
    out.extend(
        _pairs("btarraycmp", "integer[]", "integer[]", ("'{1,2}'", "'{1,2,3}'", "'{}'", "'{2}'"))
    )
    out.extend(_pairs("btoidvectorcmp", "oidvector", "oidvector", ("'1 2'", "'1 3'", "'1 2 3'")))
    out.extend(_call("btequalimage", f"{v}::oid") for v in ("25", "23", "NULL"))
    out.append(_call("btvarstrequalimage", "NULL::oid"))
    out.append(_call("btvarstrequalimage", "25::oid"))
    for function in ("brin_bloom_summary_in", "brin_minmax_multi_summary_in", "gtsvectorin"):
        out.append(_call(function, "NULL::cstring"))
        out.append(_call(function, "'x'::cstring"))
    for function, of in (
        ("brin_bloom_summary_out", "pg_brin_bloom_summary"),
        ("brin_bloom_summary_send", "pg_brin_bloom_summary"),
        ("brin_minmax_multi_summary_out", "pg_brin_minmax_multi_summary"),
        ("brin_minmax_multi_summary_send", "pg_brin_minmax_multi_summary"),
        ("gtsvectorout", "gtsvector"),
    ):
        out.append(_call(function, f"NULL::{of}"))
    for function in ("btrecordcmp", "btrecordimagecmp"):
        out.append(_call(function, "NULL::record", "NULL::record"))
    out.extend(
        _call("spg_poly_quad_compress", f"{v}::polygon")
        for v in ("'((0,0),(1,0),(1,1))'", "'((-1.5,2),(3,4),(0,-7.25))'", "NULL")
    )
    return out


def _float_list(value: str) -> list[float | None] | None:
    if not value.startswith("{"):
        return None
    return [None if v == "NULL" else float(v) for v in value[1:-1].split(",") if v]


def _close(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is b
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return a == b or abs(a - b) <= abs(b) * 1e-14


def _sign(value: Any) -> int:
    number = int(value)
    return (number > 0) - (number < 0)


def same_answer(sql: str, got: Any, want: tuple[str, Any]) -> bool:
    """Whether an engine's value or raised error is PostgreSQL's, floats within one part in 1e14.
    A cmp function of text promises its sign: the magnitude is strncmp's and strcoll's."""
    kind, value = want
    if kind == "err":
        return isinstance(got, Exception) and value in str(got)
    if isinstance(got, Exception):
        return MARK in str(got) and "does not read" in str(got)
    if value is None or got is None:
        return value is got
    if isinstance(got, (list, tuple)):
        parsed = _float_list(value)
        return (
            parsed is not None
            and len(parsed) == len(got)
            and all(_close(None if g is None else float(g), p) for g, p in zip(got, parsed))
        )
    if isinstance(got, bool):
        return value == ("t" if got else "f")
    if value in ("t", "f"):
        return value == ("t" if got else "f")
    if "cmp" in sql or "compare" in sql:
        return _sign(got) == _sign(value)
    return str(got) == value
