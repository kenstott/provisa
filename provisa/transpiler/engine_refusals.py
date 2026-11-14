# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1d4e07-6c2a-4f58-8a3e-71d0c5e2f6b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a replay on a real engine refused (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): a function
or an overload of it that no form serves there, refused by name rather than sent to fail or to
answer differently. tests/integration/test_forms_on_engines.py replays these tables against
postgres:17's answers on Trino and ClickHouse.

The overloads are by the type a statement says of any argument; a column of no said type is the
engine's to read."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot.expressions as exp

from provisa.transpiler.pg_syntax import TO_CHAR_TEMPLATE
from provisa.transpiler.to_char import plain_strftime

_T, _C = "trino", "clickhouse"
_ENGINES = {"trino": _T, "presto": _T, "athena": _T, "clickhouse": _C}
_ANY = None
_NUMBERS = frozenset({"decimal", "double precision", "real", "float", "bigint", "int", "smallint"})
_NUMERIC = frozenset({"decimal"})
_JSON = frozenset({"jsonb", "json"})
_BYTES = frozenset({"bytea"})
_CLOCKS = frozenset({"date", "time", "timetz", "timestamp", "timestamptz", "interval"})
_REALS = frozenset({"real", "float"})
_TIME = frozenset({"time"})
_OID = frozenset({"oid"})

_NOT_ANY = frozenset({"interval", "bit", "bit varying", "varbit", "tid", "aclitem", "xml"})
#: No value of these types reaches a call: the engine has no type to hold it, and sqlglot's nearest
#: (BOOLEAN of a bit, for one) answers something else.
_NOT_HELD: dict[str, frozenset[str]] = {
    _T: _NOT_ANY | {"timetz"},
    _C: _NOT_ANY | {"timetz", "timestamptz"},  # no zoned time of day, and no zoned instant
}
#: What of those a function's form reads from the text the engine holds, replayed against
#: postgres:17 in tests/integration/test_hashes_on_engines.py. An aclitem's hash is refused by its
#: form past a NULL (pg_forms_hash.REFUSED_VALUES).
_READ_AS_HELD: dict[str, frozenset[str]] = {
    "hashtid": frozenset({"tid"}),
    "hashtidextended": frozenset({"tid"}),
    "bttidcmp": frozenset({"tid"}),
    "hash_aclitem": frozenset({"aclitem"}),
    "hash_aclitem_extended": frozenset({"aclitem"}),
}

#: What both engines refuse: its name, and the said types of the overloads refused (None for all).
_ON_BOTH: dict[str, frozenset[str] | None] = {
    # An oid is held as a bigint (provisa.transpiler.pg_held_types); this call of one, and int8's
    # below, were replayed refused, and stay so.
    "nameconcatoid": _OID,
    # numeric's exact digits, which an engine's double or 38-digit decimal does not reach; and
    # the scale of the value it picked, or a time's digits, which an engine's type evens out.
    "max": _NUMERIC | {"time"},
    "min": _NUMERIC | {"time"},
    "avg": _NUMERIC,
    "exp": _NUMERIC,
    "ln": _NUMERIC,
    "log": _NUMERIC,
    "log10": _NUMERIC,
    "pow": _NUMERIC,
    "power": _NUMERIC,
    "sqrt": _NUMERIC,
    "stddev": _NUMERIC,
    "stddev_pop": _NUMERIC,
    "stddev_samp": _NUMERIC,
    "variance": _NUMERIC,
    "variance_pop": _NUMERIC,
    "numeric": frozenset({"decimal", "double precision", "real", "float"}),
    "to_char": _NUMBERS,
    "time_to_str": _NUMBERS,
    "exploding_generate_series": _NUMERIC,
    "generate_series": _NUMERIC,
    "rand": _NUMERIC,
    # No form, and nothing the engine has answers as PostgreSQL does.
    "pg_size_pretty": _ANY,
    "pg_sleep": _ANY,
    "setseed": _ANY,
    "pg_replication_origin_xact_setup": _ANY,
    "pg_sleep_for": _ANY,
    "pg_sleep_until": _ANY,
    # An interval is no value of these engines. age is the polyfill grid's row (polyfill.server).
    "make_interval": _ANY,
    "time_from_parts": _ANY,
    "make_timestamptz": _ANY,
    "timezone": _ANY,
    # A set of rows, a bit string, or a function of the server.
    "regexp_matches": _ANY,
    "regexp_split_to_table": _ANY,
    "regexp_instr": _ANY,
    "bit_count": _ANY,
    "get_bit": _ANY,
    "getbit": _ANY,
    "set_bit": _ANY,
    "overlay": _ANY,
    "like_escape": _ANY,
    "format": _ANY,
    "normalize": _ANY,
    "to_number": _ANY,
    "str_to_time": _ANY,
    "str_to_date": _ANY,
    "to_date": _ANY,
    "decode": _ANY,
    "char": _ANY,
    "make_timestamp": _ANY,
    "timestamp_from_parts": _ANY,
    "string_to_array": _ANY,
    "regexp_split_to_array": _ANY,
    "in_range": _CLOCKS,
    "date_part": frozenset({"time"}),
    "extract": frozenset({"time"}),
    "encode": frozenset({"text"}),  # a format no literal says
    "bit_length": _BYTES,
    "octet_length": _BYTES,
    "position": _BYTES,
    "strpos": _BYTES,
    "convert": _ANY,
    "makeaclitem": _ANY,
    "currtid2": _ANY,
    "ts_token_type": _ANY,
    "row_security_active": _ANY,
    "set_config": _ANY,
    "regclass": _ANY,
    "varchar": _ANY,
    "bit": _ANY,
    "int2": _JSON,
    "int4": _JSON,
    "int8": _JSON | _OID,
    "float4": _JSON,
    "float8": _JSON,
    # bytes, which the engine's own string function reads as text.
    "substr": _BYTES,
    "substring": _BYTES,
    "btrim": _BYTES,
    "ltrim": _BYTES,
    "rtrim": _BYTES,
    "trim": _BYTES,
    "byteacat": _BYTES,
    "like": _BYTES,
    "notlike": _BYTES,
}
#: The most arguments of an overload a replay served: past it the engine's own takes none.
_MOST_ARGUMENTS = {
    "bpchar": 1,
    "length": 1,
    "regexp_count": 2,
    "regexp_replace": 5,
    "regexp_like": 2,
    "timestamp_trunc": 2,
    "date_trunc": 2,
}
#: A ClickHouse pattern (the second argument) of nothing is its error; PostgreSQL's answer is the
#: value. A replacement of nothing is a deletion ClickHouse writes as PostgreSQL does.
_NOT_EMPTY = frozenset({"replace", "trim", "btrim", "ltrim", "rtrim"})
PLAIN_PARTS = frozenset({"year", "quarter", "month", "day", "hour", "minute", "dow", "doy"})
#: The formats whose every element a dialect's own format writes as PostgreSQL's does.
_PLAIN_FORMAT = re.compile(r"(?:%[YmdHiMSs]|[-/: T])+")
#: sqlglot's name of a call, and the name a statement gave it.
_NAMED = {
    "exploding_generate_series": "generate_series",
    "rand": "random",
    "str_to_time": "to_timestamp",
    "time_to_str": "to_char",
    "str_to_date": "to_date",
    "time_from_parts": "make_time",
    "timestamp_from_parts": "make_timestamp",
    "timestamp_trunc": "date_trunc",
    "variance_pop": "var_pop",
    "overlaps_timestamp": "overlaps",
}
_ON: dict[str, dict[str, frozenset[str] | None]] = {
    _T: {"gcd": _ANY, "lcm": _ANY},
    _C: {
        "gcd": _NUMERIC,
        "lcm": _NUMERIC,
        # Its double precision differs from libm's past the tenth digit.
        "exp": _ANY,
        "ln": _ANY,
        "tanh": _ANY,
        "to_timestamp": _NUMBERS,
        "unix_to_time": _NUMBERS,
        "length": _BYTES,
        "translate": _ANY,
        "concat_ws": _NUMBERS,
        # A time of day beside a date: toTime64 is no function of the server replayed, only of the
        # embedded engine, so these would reach it to fail.
        "datetime_pl": _TIME,
        "timedate_pl": _TIME,
        "timestamp": _TIME,
        "overlaps_timestamp": _TIME,
        # An aggregate of a real answers a real there, and PostgreSQL a double precision.
        "stddev": _REALS,
        "stddev_pop": _REALS,
        "stddev_samp": _REALS,
        "variance": _REALS,
        "variance_pop": _REALS,
    },
}

NAMES = frozenset({*_ON_BOTH, *_ON[_T], *_ON[_C], *_MOST_ARGUMENTS})


def _called(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """A call's name, as PostgreSQL's or sqlglot's, and its arguments."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if not isinstance(node, exp.Func):
        return None
    args = [value for value in node.args.values() if isinstance(value, exp.Expr)]
    args.extend(item for value in node.args.values() if isinstance(value, list) for item in value)
    return node.sql_name().lower(), args


def _is_empty(value: exp.Expr) -> bool:
    value = value.unnest()
    literal = value.this if isinstance(value, exp.Cast) else value
    return isinstance(literal, exp.Null) or (
        isinstance(literal, exp.Literal) and literal.is_string and literal.this == ""
    )


def _plain_format(node: exp.TimeToStr, engine: str) -> bool:
    """Whether the engine's format of to_char's template writes PostgreSQL's text. sqlglot's
    strftime text of a template no longer tells YYYY from a literal %Y, nor SS twice from SSSS, so
    the template the statement wrote is read (pg_syntax keeps it): its strftime form must be the
    one sqlglot wrote."""
    written = node.args.get("format")
    if not isinstance(written, exp.Literal):
        return False
    template = node.meta.get(TO_CHAR_TEMPLATE)
    if template is None or plain_strftime(template) != written.this:
        return False
    if engine == _C and "%M" in written.this:  # a month's name there, and a minute here
        return False
    return _PLAIN_FORMAT.fullmatch(written.this) is not None


def _plain_part(part: exp.Expr, engine: str) -> bool:
    """Whether a part is a NULL or one a form reads; ClickHouse evaluates a refusal of a constant,
    so what it cannot read is refused here."""
    part = part.unnest()
    literal = part.this if isinstance(part, exp.Cast) else part
    if isinstance(literal, exp.Null):
        return True
    if isinstance(literal, exp.Literal):
        return literal.this.lower() in PLAIN_PARTS
    if isinstance(literal, exp.Var) and literal.name.lower() in PLAIN_PARTS:
        return True  # extract(hour from c): its part a keyword, not a text
    return engine != _C


def refused_call(
    node: exp.Expr, dialect: str, said_type: Callable[[exp.Expr], str | None]
) -> str | None:
    """The name of the function ``node`` calls, where a replay refused that call on ``dialect``."""
    engine = _ENGINES.get(dialect)
    if engine is None:
        return None
    call = _called(node)
    if call is None:
        return None
    name, args = call
    said = {type_name.split("(")[0] for type_name in map(said_type, args) if type_name}
    if engine == _C and name in _NOT_EMPTY and len(args) > 1 and _is_empty(args[1]):
        return name
    if isinstance(node, exp.TimeToStr) and not _plain_format(node, engine):
        return "to_char"
    if name in ("date_part", "extract") and not _plain_part(args[0], engine):
        return name
    types = [table[name] for table in (_ON[engine], _ON_BOTH) if name in table]
    if (
        said & _NOT_HELD[engine] - _READ_AS_HELD.get(name, frozenset())
        or None in types
        or any(said & of for of in types if of is not None)
        or len(args) > _MOST_ARGUMENTS.get(name, len(args))
    ):
        return _NAMED.get(name, name)
    return None
