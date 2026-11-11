# Copyright (c) 2026 Kenneth Stott
# Canary: 410fc6ab-8a63-4b9b-98c9-b11ef1ed3ee9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's functions on DuckDB where the arguments, not a name, choose the rewrite (REQ-2167,
PGWIRE'S CATALOG FUNCTIONS); a rewrite of a name is a row of provisa.transpiler.function_macros.

``before`` reads the PostgreSQL statement, ahead of sqlglot's transpile, for the calls that
transpile drops arguments of or sends to a DuckDB function of the same name and another meaning:
``random(min, max)``, ``regexp_matches``, ``substring(text FROM pattern)``, ``btrim`` of a bytea,
and so on. ``after`` writes, on
the DuckDB statement, what PostgreSQL's parser would not keep (it reads ``->`` as a JSON operator,
a third REGEXP_EXTRACT argument as a position, and DATE_PART as EXTRACT): those calls cross the transpile as a
placeholder function."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.postgres import Postgres

from provisa.core.pg_error import raising
from provisa.transpiler import function_macros, pg_syntax, pg_xml
from provisa.transpiler.pg_literals import postgres_literals

_REGEXP_MATCHES = "provisa_regexp_matches"
_ENCODE_ESCAPE = "provisa_encode_escape"
_REGEXP_GROUP = "provisa_regexp_group"
_DATE_PART = "provisa_date_part"

#: A function named for a type: a cast to it.
_TYPE_CALLS = {
    "numeric": "text",  # a numeric's digits, exact: pgwire sends them as the numeric declared
    "text": "text",
    "name": "text",
    "varchar": "varchar",
    "bool": "boolean",
    "oid": "oid",
    "regclass": "regclass",
}
#: A length-carrying cast function: (value, typmod, is_explicit); a typmod counts the header.
_SIZED_CALLS = {"varchar": "varchar", "bpchar": "bpchar"}
#: Calls an overload of whose DuckDB's function of the name does not take, or sqlglot cannot write:
#: (name, argument count) -> the UDF that answers it.
_UDF_CALLS = {
    ("like_escape", 2): "PROVISA_LIKE_ESCAPE",
    ("normalize", 2): "PROVISA_NORMALIZE",
    ("regexp_split_to_array", 3): "PROVISA_REGEXP_SPLIT",
}

#: How a row reads its argument, by the type that opens the row's name.
_SPELLED = {
    "numeric": lambda node: _numeric_digits(node),
    "jsonb": lambda node: _jsonb_digits(node),
}

#: The numeric rows whose form is the engine's own function of a DECIMAL: a literal reaches them
#: as the sized DECIMAL postgres_literals casts it to, never as its digits' text, which DuckDB's
#: ceil and its kin cannot choose an overload for.
_OF_A_DECIMAL = frozenset(f"numeric_{name}" for name in ("ceil", "floor", "round", "trunc"))

#: The statements ``before`` has anything to do in.
_NAMED = re.compile(
    r"\b(random|regexp_matches|timezone|to_timestamp|[blr]?trim|setseed"
    r"|overlay|substring|extract|date_part|decode|encode|pg_size_pretty|current_schemas)\b",
    re.IGNORECASE,
)
#: The statements ``typed_calls`` has anything to do in.
_CALLED = re.compile(
    r"\b("
    + "|".join(
        sorted(
            {
                *_TYPE_CALLS,
                *_SIZED_CALLS,
                *(n for n, _ in _UDF_CALLS),
                *(n for n, _, _ in function_macros.OVERLOADS),
            }
        )
    )
    + r"|bit_count|get_bit|set_bit|substr|substring|overlay|gcd|lcm|factorial|generate_series"
    r"|isfinite|date_add|timezone|length|string_agg|position|sum|char|chr|bit|xid|age|like"
    r"|notlike|like_escape|convert|json_object|unnest|trunc|pg_get_object_address"
    r"|regexp_split_to_table|strpos|pg_typeof|make_interval|overlaps|random_normal|trim_scale"
    r"|min_scale|int2vectorsend)\"?\s*\(",
    re.IGNORECASE,
)

#: A cast's type that DuckDB holds as another, so its typeof cannot say it.
_HELD_AS_ANOTHER = {
    exp.DataType.Type.JSONB: "jsonb",
    exp.DataType.Type.VARCHAR: "character varying",
    exp.DataType.Type.CHAR: "character",
    exp.DataType.Type.BPCHAR: "character",
    exp.DataType.Type.NAME: "name",
}

#: make_interval's parameters, and what one not given is.
_MAKE_INTERVAL = ("years", "months", "weeks", "days", "hours", "mins", "secs")
_NO_INTERVAL = ("0", "0", "0", "0", "0", "0", "0.0")

_RANDOM_BOUNDS = "lower bound must be less than or equal to upper bound"


#: The names PostgreSQL reads a part of a second as (datetktbl).
_SECOND_PARTS = frozenset({"s", "sec", "secs", "second", "seconds"})


def _of_a_second(node: exp.Extract) -> bool:
    """Whether ``node`` reads a second (of a value that has one: PostgreSQL refuses a date's)."""
    part = node.this.this if isinstance(node.this, exp.Cast) else node.this
    named = part.name if isinstance(part, exp.Var) else part.this if part.is_string else None
    if not isinstance(named, str) or named.strip().lower() not in _SECOND_PARTS:
        return False
    value = node.expression
    return not (isinstance(value, exp.Cast) and value.to.is_type("date"))


def _is_null(node: exp.Expr) -> bool:
    return isinstance(node, exp.Null) or (isinstance(node, exp.Cast) and _is_null(node.this))


def _cast_to(node: exp.Expr, *kinds: Any) -> bool:
    return isinstance(node, exp.Cast) and node.to.this in kinds


def _is_bytea(node: exp.Expr) -> bool:
    if isinstance(node, (exp.Unhex, exp.Decode)):
        return True  # a bytea literal, as postgres_literals writes it
    if isinstance(node, exp.Anonymous):
        return node.name.lower() == "unhex"
    return _cast_to(node, exp.DataType.Type.VARBINARY)


def _is_text(node: exp.Expr) -> bool:
    if isinstance(node, exp.Literal):
        return node.is_string
    return isinstance(node, exp.DPipe) or _cast_to(
        node, exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR
    )


def capture_groups(pattern: str) -> int:
    """How many capturing groups a regular expression has: each ``(`` not escaped, not in a
    bracket expression and not opening a ``(?`` group."""
    count = 0
    position = 0
    in_bracket = False
    while position < len(pattern):
        char = pattern[position]
        if char == "\\":
            position += 2
            continue
        if in_bracket:
            in_bracket = char != "]"
        elif char == "[":
            in_bracket = True
            if pattern[position + 1 : position + 2] == "]":
                position += 1  # a ] first in a bracket expression is a member of it
        elif char == "(" and pattern[position + 1 : position + 2] != "?":
            count += 1
        position += 1
    return count


def _scale(node: exp.Expr) -> int | None:
    """The scale a numeric bound says; None where the statement does not say it."""
    if isinstance(node, exp.Literal) and not node.is_string and "." in node.this:
        return len(node.this.partition(".")[2])
    return None


def _said_scale(node: exp.Expr) -> int | None:
    """A numeric bound's scale (its dscale) where the statement says it: a literal's digits after
    the point, a whole number's none, a numeric cast's typmod scale; None where the engine alone
    knows it (a column)."""
    if isinstance(node, (exp.Paren, exp.Neg)):
        return _said_scale(node.this)
    if isinstance(node, exp.Literal) and not node.is_string:
        if node.is_int:
            return 0
        written = node.this.lower()
        return None if "e" in written else len(written.partition(".")[2])
    if _cast_to(node, exp.DataType.Type.INT, exp.DataType.Type.SMALLINT, exp.DataType.Type.BIGINT):
        return 0
    if isinstance(node, exp.Cast) and _cast_to(node, exp.DataType.Type.DECIMAL):
        typmod = node.to.expressions
        if not typmod:
            return _said_scale(node.this)
        return int(typmod[1].this.name) if len(typmod) > 1 else 0
    return None


def _numeric_draw(low: exp.Expr, high: exp.Expr, scale: int) -> exp.Expr:
    """numeric_random's draw: a whole number of steps of ``scale`` from ``low``, uniform over the
    steps up to ``high``, both bounds among them, as a numeric of that scale."""
    unit = exp.cast(
        exp.Literal.number(f"0.{'0' * (scale - 1)}1" if scale else "1"),
        exp.DataType.build(f"DECIMAL(38, {scale})"),
    )
    tens = exp.cast(exp.Literal.number(10**scale), exp.DataType.build("DECIMAL(38, 0)"))
    steps = exp.cast((high.copy() - low.copy()) * tens, exp.DataType.build("BIGINT"))
    step = exp.cast(
        exp.Floor(this=exp.Rand() * (steps + exp.Literal.number(1))),
        exp.DataType.build("DECIMAL(38, 0)"),
    )
    return exp.cast(low.copy() + step * unit, exp.DataType.build(f"DECIMAL(38, {scale})"))


def _random(low: exp.Expr, high: exp.Expr) -> exp.Expr:
    """random(min, max): an integer of the bounds' type, or a numeric of the larger scale of the
    two, uniform between them, refused as PostgreSQL refuses inverted bounds."""
    type_ = exp.DataType.Type
    nodes = (low, high)
    whole = low.copy() + exp.Floor(
        this=exp.Rand() * (high.copy() - low.copy() + exp.Literal.number(1))
    )
    between = low.copy() + (high.copy() - low.copy()) * exp.Rand()
    said = [_said_scale(n) for n in nodes]
    drawn: exp.Expr
    if any(_cast_to(n, type_.BIGINT) for n in nodes):
        drawn = exp.cast(whole, exp.DataType.build("BIGINT"))
    elif all(
        _cast_to(n, type_.INT, type_.SMALLINT) or (isinstance(n, exp.Literal) and n.is_int)
        for n in nodes
    ):
        drawn = exp.cast(whole, exp.DataType.build("INT"))
    elif any(_cast_to(n, type_.DECIMAL) or _scale(n) is not None for n in nodes):
        if None not in said:
            drawn = _numeric_draw(low, high, max(s for s in said if s is not None))
        else:
            scales = [s for s in map(_scale, nodes) if s is not None]
            drawn = (
                exp.Round(this=between, decimals=exp.Literal.number(max(scales)))
                if scales
                else between
            )
    else:
        # The statement does not say the bounds' type: the engine knows it when it binds.
        drawn = exp.Case(
            ifs=[
                exp.If(
                    this=exp.Like(
                        this=exp.Anonymous(this="TYPEOF", expressions=[low.copy()]),
                        expression=exp.Literal.string("DECIMAL%"),
                    ),
                    true=between,
                )
            ],
            default=whole,
        )
    refused = exp.Anonymous(this="ERROR", expressions=[exp.Literal.string(_RANDOM_BOUNDS)])
    return exp.Case(
        ifs=[exp.If(this=exp.GT(this=low.copy(), expression=high.copy()), true=refused)],
        default=drawn,
    )


def _hex_trimmed(value: str, chars: str, leading: bool, trailing: bool) -> str:
    """btrim, ltrim or rtrim of a bytea: DuckDB trims only text, so its hex is trimmed, pair by
    pair. Each alternative is two digits and the leading pattern is anchored at the start and the
    trailing one at the (even) end, so a match always falls on a byte."""
    bytes_ = f"ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(HEX({chars}), '..'), '|')"
    out = f"HEX({value})"
    if leading:
        out = f"REGEXP_REPLACE({out}, '^(?:' || {bytes_} || ')*', '')"
    if trailing:
        out = f"REGEXP_REPLACE({out}, '(?:' || {bytes_} || ')*$', '')"
    return f"UNHEX({out})"


def _regex_substring(value: str, pattern: exp.Expr) -> str:
    """substring(string FROM pattern): the first group's text if the pattern has one, else the
    match's; NULL for no match. DuckDB chooses a group by a constant, so a pattern is a literal."""
    if _is_null(pattern):
        return "CAST(NULL AS TEXT)"
    if not (isinstance(pattern, exp.Literal) and pattern.is_string):
        raise NotImplementedError(
            "substring: a pattern that is not a literal is not supported; its group is chosen "
            "where the statement is read"
        )
    text = pattern.sql(dialect="postgres")
    group = 1 if capture_groups(pattern.this) else 0
    return f"CASE WHEN {value} ~ {text} THEN {_REGEXP_GROUP}({value}, {text}, {group}) END"


def _decoded(value: str, fmt: exp.Expr, name: str) -> str:
    """encode(bytea, format) and decode(text, format), by the format the statement names."""
    if _is_null(fmt):
        return f"CAST(NULL AS {'TEXT' if name == 'encode' else 'BYTEA'})"
    written = fmt.this.lower() if isinstance(fmt, exp.Literal) and fmt.is_string else None
    if written is None:
        raise NotImplementedError(
            f"{name}: a format that is not a literal is not supported; it is chosen where the "
            "statement is read"
        )
    calls = {
        ("encode", "hex"): f"LOWER(HEX({value}))",
        ("encode", "base64"): f"TO_BASE64({value})",
        ("encode", "escape"): f"{_ENCODE_ESCAPE}({value})",
        ("decode", "hex"): f"UNHEX({value})",
        ("decode", "base64"): f"FROM_BASE64({value})",
    }
    if (name, written) not in calls:
        raise NotImplementedError(f"{name}: format {written} is not supported")
    return calls[(name, written)]


def _rewrite(node: exp.Expr) -> exp.Expr:  # noqa: PLR0911, PLR0912 -- one branch per function
    def text(arg: exp.Expr) -> str:
        return arg.transform(_rewrite).sql(dialect="postgres")

    written: str | None = None
    if isinstance(node, exp.CurrentSchemas):
        # The search path's schemas (pgwire's catalog: public), not DuckDB's.
        implicit = node.this.unnest()
        while isinstance(implicit, exp.Cast) and implicit.to.is_type(exp.DataType.Type.BOOLEAN):
            implicit = implicit.this.unnest()  # true::boolean says what true does
        if isinstance(implicit, exp.Boolean):
            written = "ARRAY['pg_catalog', 'public']" if implicit.this else "ARRAY['public']"
        elif isinstance(implicit, exp.Null):
            written = "CAST(NULL AS TEXT[])"  # strict: a NULL argument
        else:
            written = (
                f"CASE WHEN {text(node.this)} THEN ARRAY['pg_catalog', 'public'] "
                f"WHEN NOT {text(node.this)} THEN ARRAY['public'] END"
            )
    elif isinstance(node, exp.Rand) and node.args.get("lower") is not None:
        return _random(node.this.transform(_rewrite), node.args["lower"].transform(_rewrite))
    elif isinstance(node, exp.StrToTime):
        # PostgreSQL's to_timestamp says an instant, read in the session's time zone.
        node.set("this", node.this.transform(_rewrite))
        return exp.cast(node.copy(), "TIMESTAMPTZ")
    elif isinstance(node, exp.Trim) and _is_bytea(node.this):
        position = (node.args.get("position") or "").upper()
        written = _hex_trimmed(
            text(node.this),
            text(node.expression),
            position != "TRAILING",
            position != "LEADING",
        )
    elif isinstance(node, exp.Overlay) and not _is_bytea(node.this):
        value, placing, start = text(node.this), text(node.expression), text(node.args["from_"])
        length = node.args.get("for_")
        skip = text(length) if length is not None else f"LENGTH({placing})"
        written = (
            f"CAST(SUBSTRING({value}, 1, {start} - 1) || {placing} || "
            f"SUBSTRING({value}, {start} + {skip}) AS TEXT)"
        )
    elif (
        isinstance(node, exp.Substring)
        and node.args.get("length") is None
        and node.args.get("start") is not None
        and _is_text(node.args["start"])
    ):
        written = _regex_substring(text(node.this), node.args["start"])
    elif isinstance(node, exp.Extract) and _of_a_second(node):
        # DuckDB's second is a whole number: PostgreSQL's keeps its fraction, the second's
        # microseconds over a million, a numeric of scale 6 for EXTRACT and a float8 for
        # date_part (whose part is a text).
        micros = f"{_DATE_PART}('microseconds', {text(node.expression)})"
        exact = f"(CAST({micros} AS DECIMAL(20, 0)) * CAST(0.000001 AS DECIMAL(7, 6)))"
        written = exact if isinstance(node.this, exp.Var) else f"CAST({exact} AS DOUBLE PRECISION)"
    elif isinstance(node, exp.Extract) and not isinstance(node.this, exp.Var):
        written = f"{_DATE_PART}({text(node.this)}, {text(node.expression)})"
    elif isinstance(node, exp.Encode) and node.args.get("charset") is not None:
        written = _decoded(text(node.this), node.args["charset"], "encode")
    elif isinstance(node, exp.Decode) and node.args.get("charset") is not None:
        written = _decoded(text(node.this), node.args["charset"], "decode")
    elif isinstance(node, exp.Anonymous):
        name = node.name.lower()
        args = node.expressions
        if name == "timezone" and len(args) == 1:
            written = f"TIMEZONE(CURRENT_SETTING('TimeZone'), {text(args[0])})"
        elif name == "regexp_matches":
            written = f"{_REGEXP_MATCHES}({', '.join(text(a) for a in args)})"
        elif name == "setseed" and len(args) == 1:
            seed = text(args[0])
            written = (
                f"COALESCE(CAST(SETSEED({seed}) AS TEXT), CASE WHEN {seed} IS NOT NULL THEN '' END)"
            )
        elif name == "pg_size_pretty" and len(args) == 1:
            written = f"PROVISA_SIZE_PRETTY(CAST({text(args[0])} AS HUGEINT))"
    return node if written is None else sqlglot.parse_one(written, read="postgres")


def _is_kind(node: exp.Expr, *names: str) -> bool:
    return isinstance(node, exp.Cast) and node.to.sql(dialect="postgres").lower() in names


_UNICODE_ESCAPE = re.compile(r"\\(\\|\+[0-9A-Fa-f]{6}|[0-9A-Fa-f]{4})")


def _unicode_char(found: re.Match[str]) -> str:
    digits = found[1]
    return "\\" if digits == "\\" else chr(int(digits.removeprefix("+"), 16))


def _text_array(node: exp.Expr) -> str:
    """A text[] argument: an unknown-type '{a,b}' literal is that array."""
    if not (isinstance(node, exp.Literal) and node.is_string):
        return _sql(node)
    elements = [e.strip().strip('"') for e in node.this.strip("{}").split(",") if e.strip()]
    written = ", ".join(exp.Literal.string(e).sql(dialect="postgres") for e in elements)
    return f"CAST(ARRAY[{written}] AS TEXT[])"


def _mac_call(name: str, args: list[exp.Expr]) -> str | None:
    if name == "trunc" and len(args) == 1 and _is_kind(args[0], "macaddr", "macaddr8"):
        zeros = "00:00:00" if _is_kind(args[0], "macaddr") else "00:00:00:00:00"
        return f"CAST(LEFT({_sql(args[0])}, 9) || '{zeros}' AS TEXT)"
    return None


def _is_numeric(node: exp.Expr) -> bool:
    return _cast_to(node, exp.DataType.Type.DECIMAL) or _scale(node) is not None


def _digits(text: str) -> str | None:
    """A number's text as numeric_out writes it; None for what numeric_in would refuse."""
    try:
        number = Decimal(text.strip())
    except InvalidOperation:
        return None
    if number.is_nan():
        return "NaN"
    if number.is_infinite():
        return "Infinity" if number > 0 else "-Infinity"
    return f"{number:f}"


def _numeric_digits(node: exp.Expr) -> str:
    """An argument of a numeric row: its digits. A cast to the engine's DECIMAL would keep three of
    a fraction, and could not hold a NaN, an infinity or more than 38 digits, so a numeric of no
    stated size reaches the row uncast: what it reads is its text."""
    if not (isinstance(node, exp.Cast) and _cast_to(node, exp.DataType.Type.DECIMAL)):
        return _sql(node)
    if node.to.expressions:
        return _sql(node)
    held = node.this.unnest()
    digits = _digits(held.this) if isinstance(held, exp.Literal) else None
    if digits is None:
        return _sql(node.this)
    return exp.Literal.string(digits).sql(dialect="postgres")


def _jsonb_digits(node: exp.Expr) -> str:
    """An argument of a jsonb row: a number's digits as jsonb_out writes them, which is how a
    refusal of it quotes it. A string, an object and the like are left as they are."""
    if _cast_to(node, exp.DataType.Type.JSONB) and isinstance(node.this, exp.Literal):
        digits = _digits(node.this.this)
        if digits is not None and digits.lstrip("-")[0].isdigit():
            return f"CAST({exp.Literal.string(digits).sql(dialect='postgres')} AS JSONB)"
    return _sql(node)


def _sql(node: exp.Expr) -> str:
    return node.transform(_call).sql(dialect="postgres")


def _as_text(node: exp.Expr) -> str:
    return f"CAST({_sql(node)} AS TEXT)"


def _type_call(name: str, args: list[exp.Expr]) -> str | None:  # noqa: PLR0911 -- one per form
    """A call of a function named for a type, as the cast it is."""
    if name == "char" and len(args) == 1 and _is_text(args[0]):
        return f'CAST(LEFT({_sql(args[0])}, 1) AS "char")'
    if name == "xid" and len(args) == 1:  # an xid8's low 32 bits
        return f"CAST(({_sql(args[0])}) % 4294967296 AS xid)"
    if name == "age" and len(args) == 1 and _is_kind(args[0], "xid"):
        return f"PROVISA_XID_AGE({_sql(args[0])})"
    if name in _TYPE_CALLS and len(args) == 1:
        return f"CAST({_sql(args[0])} AS {_TYPE_CALLS[name]})"
    if name == "numeric" and len(args) == 2:  # noqa: PLR2004 -- value and typmod
        typmod = args[1]
        if _is_null(typmod):
            return "CAST(NULL AS TEXT)"
        said = _int_said(typmod)
        if said is not None:
            packed = said - 4
            if packed < 0:  # below the header: no precision, the value as it is (numeric.c)
                return _sql(exp.Cast(this=args[0].copy(), to=exp.DataType.build("TEXT")))
            sized = f"NUMERIC({packed >> 16}, {packed & 0xFFFF})"
            return f"CAST(CAST({_sql(args[0])} AS {sized}) AS TEXT)"
    if name in _SIZED_CALLS and len(args) == 3:  # noqa: PLR2004 -- value, typmod, explicit
        value, typmod = _sql(args[0]), _sql(args[1])
        # A typmod below the header says no length; a character(n) is padded to it.
        sized = "RPAD({v}, ({t}) - 4, ' ')" if name == "bpchar" else "LEFT({v}, ({t}) - 4)"
        return f"CASE WHEN ({typmod}) < 4 THEN {value} ELSE {sized.format(v=value, t=typmod)} END"
    if name == "bit" and len(args) == 3 and _is_null(args[1]):  # noqa: PLR2004
        return "CAST(NULL AS BIT)"
    if name == "bit" and len(args) == 3 and _cast_to(args[0], exp.DataType.Type.BIT):  # noqa: PLR2004
        return _sql(_bit_resized(*args))
    if name == "bit" and len(args) == 2:  # noqa: PLR2004 -- an integer, as so many bits
        return _sql(_integer_bits(args[0], args[1]))
    return None


def _int_said(node: exp.Expr) -> int | None:
    """The integer a constant argument says, through its casts and sign: ``(-7)::integer``."""
    inner = node.unnest()
    while isinstance(inner, exp.Cast):
        inner = inner.this.unnest()
    if isinstance(inner, exp.Neg):
        said = _int_said(inner.this)
        return None if said is None else -said
    if isinstance(inner, exp.Literal) and inner.is_int:
        return int(inner.this)
    return None


def _number(value: int) -> exp.Expr:
    return exp.Literal.number(value)


def _integer_bits(value: exp.Expr, typmod: exp.Expr) -> exp.Expr:
    """bit(integer, typmod), bitfromint4's: the value's low bits, as many as the typmod says; a
    typmod of 0 or below says no length, which is one bit."""
    width = exp.Case(
        ifs=[exp.If(this=exp.LTE(this=typmod.copy(), expression=_number(0)), true=_number(1))],
        default=typmod.copy(),
    )
    huge = exp.DataType.build("HUGEINT", dialect="duckdb")
    mask = exp.Sub(
        this=exp.Paren(
            this=exp.BitwiseLeftShift(
                this=exp.Cast(this=_number(1), to=huge.copy()), expression=exp.Paren(this=width)
            )
        ),
        expression=_number(1),
    )
    bits = exp.BitwiseAnd(
        this=exp.Cast(this=value.copy(), to=huge), expression=exp.Paren(this=mask)
    )
    binary = exp.Anonymous(this="FORMAT", expressions=[exp.Literal.string("{:b}"), bits])
    padded = exp.Anonymous(this="LPAD", expressions=[binary, width.copy(), exp.Literal.string("0")])
    return exp.Cast(this=padded, to=exp.DataType.build("BIT"))


def _bit_resized(value: exp.Expr, length: exp.Expr, explicit: exp.Expr) -> exp.Expr:
    """bit(bit, length, explicit), the cast to bit(n) as varbit.c's bit() makes it: unchanged for a
    length of 0 or below or its own; an explicit cast truncates or pads with zeros on the right;
    an implicit one of another length is PostgreSQL's 22026."""
    text = exp.Cast(this=value.copy(), to=exp.DataType.build("TEXT"))
    own = exp.Length(this=text.copy())
    unchanged = exp.Or(
        this=exp.LTE(this=length.copy(), expression=_number(0)),
        expression=exp.EQ(this=own.copy(), expression=length.copy()),
    )
    refused = sqlglot.parse_one(
        raising(
            "22026",
            exp.DPipe(
                this=exp.DPipe(
                    this=exp.DPipe(
                        this=exp.DPipe(
                            this=exp.Literal.string("bit string length "),
                            expression=exp.Cast(this=own.copy(), to=exp.DataType.build("TEXT")),
                        ),
                        expression=exp.Literal.string(" does not match type bit("),
                    ),
                    expression=exp.Cast(this=length.copy(), to=exp.DataType.build("TEXT")),
                ),
                expression=exp.Literal.string(")"),
            ).sql(dialect="postgres"),
        ),
        read="postgres",
    )
    cut = exp.Anonymous(
        this="RPAD",
        expressions=[
            exp.Left(this=text.copy(), expression=length.copy()),
            length.copy(),
            exp.Literal.string("0"),
        ],
    )
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.Or(this=_is_null_of(length), expression=_is_null_of(explicit)),
                true=exp.Cast(this=exp.Null(), to=exp.DataType.build("BIT")),
            ),
            exp.If(this=unchanged, true=value.copy()),
            exp.If(this=exp.Not(this=explicit.copy()), true=refused),
        ],
        default=exp.Cast(this=cut, to=exp.DataType.build("BIT")),
    )


def _is_null_of(node: exp.Expr) -> exp.Expr:
    return exp.Is(this=node.copy(), expression=exp.Null())


def _int2vector(value: exp.Expr) -> str:
    """An int2vector's value: the engine holds a list, and its literal is its numbers."""
    if isinstance(value, exp.Literal):
        numbers = ", ".join(value.this.split())
        return f"CAST(ARRAY[{numbers}] AS SMALLINT[])"
    return f"CAST({_sql(value)} AS SMALLINT[])"


def _numeric_call(name: str, args: list[exp.Expr]) -> str | None:
    if name in ("trim_scale", "min_scale") and len(args) == 1:
        return f"{name}({_numeric_digits(args[0])})"
    if name == "int2vectorsend" and len(args) == 1 and _is_kind(args[0], "int2vector"):
        return f"int2vectorsend({_int2vector(args[0].this)})"
    if name == "random_normal" and any(isinstance(a, exp.Kwarg) for a in args):
        return f"random_normal({_by_keyword(('mean', 'stddev'), ('0.0', '1.0'), args)})"
    if name in ("gcd", "lcm") and len(args) == 2 and any(map(_is_numeric, args)):  # noqa: PLR2004
        return f"PROVISA_NUMERIC_{name.upper()}({_as_text(args[0])}, {_as_text(args[1])})"
    if name == "factorial" and len(args) == 1:
        return f"PROVISA_FACTORIAL({_sql(args[0])})"
    if name == "generate_series" and args and any(map(_is_numeric, args)):
        step = _as_text(args[2]) if len(args) > 2 else "NULL"  # noqa: PLR2004
        return f"UNNEST(PROVISA_NUMERIC_SERIES({_as_text(args[0])}, {_as_text(args[1])}, {step}))"
    return None


def _bytea_call(name: str, args: list[exp.Expr]) -> str | None:  # noqa: PLR0911 -- one per function
    if not args or not _is_bytea(args[0]):
        return None
    written = [_sql(a) for a in args]
    if name == "bit_count":
        return f"PROVISA_BIT_COUNT({written[0]})"
    if name in ("get_bit", "set_bit"):
        return f"PROVISA_{name.upper()}({', '.join(written)})"
    if name in ("substr", "substring"):
        length = written[2] if len(args) > 2 else "NULL"  # noqa: PLR2004
        return f"PROVISA_BYTEA_SUBSTR({written[0]}, {written[1]}, {length})"
    if name == "overlay":
        length = written[3] if len(args) > 3 else "NULL"  # noqa: PLR2004
        return f"PROVISA_BYTEA_OVERLAY({written[0]}, {written[1]}, {written[2]}, {length})"
    if name == "length" and len(args) == 2:  # noqa: PLR2004 -- a bytea and its encoding
        return f"PROVISA_ENCODED_LENGTH({written[0]}, {written[1]})"
    if name == "string_agg" and len(args) == 2:  # noqa: PLR2004
        return f"UNHEX(STRING_AGG(HEX({written[0]}), HEX({written[1]})))"
    return None


def _bit_call(name: str, args: list[exp.Expr]) -> str | None:
    if not args or not _cast_to(args[0], exp.DataType.Type.BIT):
        return None
    if name in ("substr", "substring"):
        rest = ", ".join(_sql(a) for a in args[1:])
        return f"CAST(SUBSTRING({_as_text(args[0])}, {rest}) AS BIT)"
    if name in ("position", "strpos") and len(args) == 2:  # noqa: PLR2004
        return f"STRPOS({_as_text(args[0])}, {_as_text(args[1])})"
    if name == "overlay":
        value, placing, start = _as_text(args[0]), _as_text(args[1]), _sql(args[2])
        skip = _sql(args[3]) if len(args) > 3 else f"LENGTH({placing})"  # noqa: PLR2004
        return (
            f"CAST(SUBSTRING({value}, 1, {start} - 1) || {placing} || "
            f"SUBSTRING({value}, {start} + {skip}) AS BIT)"
        )
    return None


def _by_keyword(names: tuple[str, ...], defaults: tuple[str, ...], args: list[exp.Expr]) -> str:
    """A call's arguments in the order of its parameters: by position, then named as ``=>`` names
    them, with each default for one not given."""
    given = dict(zip(names, defaults))
    for position, arg in enumerate(args):
        if isinstance(arg, exp.Kwarg):
            given[arg.this.name.lower()] = _sql(arg.expression)
        else:
            given[names[position]] = _sql(arg)
    return ", ".join(given.values())


def _ended_at(start: str, end: exp.Expr) -> str:
    """An end of an overlaps period; pg_proc writes an interval's overload as start + interval."""
    is_interval = isinstance(end, exp.Interval) or _cast_to(end, exp.DataType.Type.INTERVAL)
    return f"{start} + {_sql(end)}" if is_interval else _sql(end)


def _time_call(name: str, args: list[exp.Expr]) -> str | None:  # noqa: PLR0911 -- one per form
    interval = exp.DataType.Type.INTERVAL
    if name == "make_interval":
        return f"make_interval_fields({_by_keyword(_MAKE_INTERVAL, _NO_INTERVAL, args)})"
    written = [_sql(a) for a in args]
    if name == "time" and len(args) == 1:  # a value whose type the statement does not say
        return f"CAST({written[0]} AS TIME)"
    if name == "time" and len(args) == 2:  # noqa: PLR2004
        return f"time_scale({written[0]}, {written[1]})"
    if name == "interval" and len(args) == 1:  # pg_proc's other overload takes a typmod
        return f"time_interval({written[0]})"
    if name == "interval" and len(args) == 2:  # noqa: PLR2004
        return f"interval_scale({written[0]}, {written[1]})"
    if name == "timestamp" and len(args) == 2:  # noqa: PLR2004
        row = "datetime_pl" if _cast_to(args[1], exp.DataType.Type.TIME) else "timestamp_scale"
        return f"{row}({written[0]}, {written[1]})"
    if name == "overlaps" and len(args) == 4:  # noqa: PLR2004
        first, second = written[0], written[2]
        return (
            f"overlaps_timestamp({first}, {_ended_at(first, args[1])}, "
            f"{second}, {_ended_at(second, args[3])})"
        )
    if (
        name == "isfinite"
        and len(args) == 1
        and _cast_to(args[0], interval)
        and args[0].this.name.lower().lstrip("+-") == "infinity"
    ):
        return "FALSE"  # DuckDB has no infinite interval to hold it in
    if name == "isfinite" and len(args) == 1 and _cast_to(args[0], interval):
        # DuckDB's intervals are finite.
        return f"CASE WHEN {written[0]} IS NOT NULL THEN TRUE END"
    if name == "date_add" and len(args) == 2:  # noqa: PLR2004
        return f"CAST({written[0]} AS TIMESTAMPTZ) + {written[1]}"
    if name == "date_add" and len(args) == 3 and isinstance(args[2], exp.Var):  # noqa: PLR2004
        zone = exp.Literal.string(args[2].name).sql(dialect="postgres")  # sqlglot's unit: a zone
        return f"TIMEZONE({zone}, TIMEZONE({zone}, {written[0]}) + {written[1]})"
    if name == "date_add" and len(args) == 3 and _is_text(args[2]):  # noqa: PLR2004 -- a zone
        zone = written[2]
        return f"TIMEZONE({zone}, TIMEZONE({zone}, {written[0]}) + {written[1]})"
    if name == "timezone" and len(args) == 2 and _cast_to(args[0], interval):  # noqa: PLR2004
        kinds = exp.DataType.Type
        if _cast_to(args[1], kinds.TIMESTAMPTZ):
            return f"(TIMEZONE('UTC', {written[1]}) + {written[0]})"
        if _cast_to(args[1], kinds.TIMETZ):
            # timetz_izone: a timetz stays a timetz, moved to the zone the interval names
            return f"TIMEZONE({written[0]}, {written[1]})"
        return f"(CAST({written[1]} - {written[0]} AS TIMESTAMP) AT TIME ZONE 'UTC')"
    if name == "sum" and len(args) == 1 and _cast_to(args[0], interval):
        # Field by field, as interval_accum adds: a day does not become 24 hours.
        value = written[0]
        return (
            f"TO_MONTHS(CAST(SUM(EXTRACT(YEAR FROM {value}) * 12 + EXTRACT(MONTH FROM {value})) "
            f"AS INT)) + TO_DAYS(CAST(SUM(EXTRACT(DAY FROM {value})) AS INT)) + "
            f"TO_MICROSECONDS(CAST(SUM(EXTRACT(HOUR FROM {value}) * 3600000000 + "
            f"EXTRACT(MINUTE FROM {value}) * 60000000 + EXTRACT(MICROSECOND FROM {value})) "
            "AS BIGINT))"
        )
    return None


def _named(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of a call of a function a parser class stands for."""
    if isinstance(node, exp.Anonymous):
        name = node.name.lower()
        # pg_syntax reads a call of overlaps as its routine's; its overloads are overlaps' own.
        return "overlaps" if name == pg_syntax.OVERLAPS else name, list(node.expressions)
    if isinstance(node, exp.Overlaps):
        periods = (node.this, node.expression)
        if not all(isinstance(p, exp.Tuple) for p in periods):
            return None  # && of two values of a range or a shape
        return "overlaps", [e for p in periods for e in p.expressions]
    if isinstance(node, exp.Normalize):
        form = node.args.get("form")
        if isinstance(form, (exp.Column, exp.Var)):
            form = exp.Literal.string(form.name)
        return "normalize", [node.this, form or exp.Literal.string("NFC")]
    args = {
        exp.Substring: ("substring", ("this", "start", "length")),
        exp.Factorial: ("factorial", ("this",)),
        exp.ExplodingGenerateSeries: ("generate_series", ("start", "end", "step")),
        exp.GenerateSeries: ("generate_series", ("start", "end", "step")),
        exp.Getbit: ("get_bit", ("this", "expression")),
        exp.Trunc: ("trunc", ("this", "decimals")),
        exp.Explode: ("unnest", ("this",)),
        exp.Unnest: ("unnest", ("expressions",)),
        exp.Overlay: ("overlay", ("this", "expression", "from_", "for_")),
        exp.Length: ("length", ("this", "encoding")),
        exp.GroupConcat: ("string_agg", ("this", "separator")),
        exp.StrPosition: ("position", ("this", "substr")),
        exp.Sum: ("sum", ("this",)),
        exp.DateAdd: ("date_add", ("this", "expression", "unit")),
        exp.Time: ("time", ("this", "zone")),
        exp.Timestamp: ("timestamp", ("this", "zone")),
        exp.MakeInterval: (
            "make_interval",
            ("year", "month", "week", "day", "hour", "minute", "second"),
        ),
    }.get(type(node))
    if args is None:
        return None
    values = [node.args[k] for k in args[1] if node.args.get(k) is not None]
    flat = [v for value in values for v in (value if isinstance(value, list) else [value])]
    return args[0], flat


def _type_said(value: exp.Expr) -> str | None:
    """pg_typeof of what the statement says and the engine's type does not: a literal of unknown
    type, a type DuckDB holds as another, or a call of pg_typeof. None: the engine's type says it
    (the ``pg_typeof`` macro)."""
    if isinstance(value, exp.Null) or (isinstance(value, exp.Literal) and value.is_string):
        return "unknown"
    if isinstance(value, exp.Cast):
        return _HELD_AS_ANOTHER.get(value.to.this)
    named = _named(value)
    return "regtype" if named is not None and named[0] == "pg_typeof" else None


#: The collations every PostgreSQL 17 has, whatever its locales (initdb), by name.
_BUILT_IN_COLLATIONS = frozenset({"default", "C", "POSIX", "ucs_basic", "unicode", "pg_c_utf8"})


def _collation_named(value: exp.Collate) -> exp.Expr | None:
    """pg_collation_for of a value a COLLATE clause gives a built-in collation: its name, as
    quote_ident writes it ("default" is a keyword, so quoted); None for any other collation."""
    name = value.expression.name
    if name not in _BUILT_IN_COLLATIONS:
        return None
    bare = re.fullmatch(r"[a-z_][a-z0-9_$]*", name) is not None and name != "default"
    return exp.Literal.string(name if bare else '"' + name.replace('"', '""') + '"')


def _call(node: exp.Expr) -> exp.Expr:
    """A call of an overload DuckDB's function of the name does not take."""
    if isinstance(node, exp.Dot) and node.this.name.lower() == "pg_catalog":
        inner = _call(node.expression)
        return node if inner is node.expression else inner
    if isinstance(node, exp.UnicodeString) and not node.args.get("escape"):
        # U&'...': sqlglot writes it for DuckDB with its escapes unread.
        return exp.Literal.string(_UNICODE_ESCAPE.sub(_unicode_char, node.this))
    if isinstance(node, exp.BitString):
        # A bit string's digits: sqlglot writes it for DuckDB as an integer, of another length.
        return exp.Cast(this=exp.Literal.string(node.this), to=exp.DataType.build("BIT"))
    xml = pg_xml.rewritten(node, _sql)
    if xml is not None:
        return xml
    named = _named(node)
    if named is None:
        return node
    name, args = named
    if name == "pg_collation_for" and len(args) == 1 and isinstance(args[0], exp.Collate):
        collated = _collation_named(args[0])
        if collated is not None:
            return collated
    if name == "pg_typeof" and len(args) == 1:
        said = _type_said(args[0])
        return node if said is None else exp.Literal.string(said)
    if name == "pg_get_object_address" and len(args) == 3:  # noqa: PLR2004
        written = f"pg_get_object_address({_sql(args[0])}, {_text_array(args[1])}, {_text_array(args[2])})"
        return sqlglot.parse_one(written, read="postgres")
    chosen = function_macros.overload(name, args)
    if chosen != name:  # REQ-2171: by the type the statement says, while the cast is there to read
        spell = _sql if chosen in _OF_A_DECIMAL else _SPELLED.get(chosen.split("_")[0], _sql)
        written = f"{chosen}({', '.join(spell(a) for a in args)})"
        return sqlglot.parse_one(written, read="postgres")
    written = (
        _type_call(name, args)
        or _numeric_call(name, args)
        or _bytea_call(name, args)
        or _bit_call(name, args)
        or _time_call(name, args)
        or _mac_call(name, args)
    )
    if written is None and name == "convert" and len(args) == 3:  # noqa: PLR2004
        written = f"pg_convert({', '.join(_sql(a) for a in args)})"
    if written is None and name == "json_object" and len(args) in (1, 2):
        written = f"pg_json_object({', '.join(_sql(a) for a in args)})"
    if written is None and name in ("like", "notlike") and len(args) == 2:  # noqa: PLR2004
        # Not sqlglot's LIKE function, whose arguments are SQLite's way round.
        value, pattern = _sql(args[0]), _sql(args[1])
        written = (
            f"{'NOT ' if name == 'notlike' else ''}PROVISA_BYTEA_LIKE({value}, {pattern})"
            if _is_bytea(args[0])
            else f"{value} {'NOT ' if name == 'notlike' else ''}LIKE {pattern} ESCAPE '\\'"
        )
    if written is None and name == "like_escape" and args and _is_bytea(args[0]):
        written = f"PROVISA_BYTEA_LIKE_ESCAPE({', '.join(_sql(a) for a in args)})"
    if written is None and (name, len(args)) in _UDF_CALLS:
        written = f"{_UDF_CALLS[(name, len(args))]}({', '.join(_sql(a) for a in args)})"
    if written is None and name == "regexp_split_to_table" and len(args) == 3:  # noqa: PLR2004
        written = f"UNNEST(PROVISA_REGEXP_SPLIT_TABLE({', '.join(_sql(a) for a in args)}))"
    if written is None and name == "substring" and len(args) == 3 and _is_text(args[1]):  # noqa: PLR2004
        # substring(string SIMILAR pattern ESCAPE escape).
        written = (
            "REGEXP_REPLACE(CAST(NULL AS TEXT), '', '') "
            if any(map(_is_null, args))
            else f"PROVISA_SIMILAR_SUBSTRING({', '.join(_sql(a) for a in args)})"
        )
    return node if written is None else sqlglot.parse_one(written, read="postgres")


#: What sqlglot's parser reads as syntax of its own (pg_syntax): PostgreSQL's calls of the name.
_SYNTAX_CALLS = re.compile(r"\b(convert|json_object)\s*\(", re.IGNORECASE)
_BIT_VARYING = re.compile(r"\bbit\s+varying\b", re.IGNORECASE)
#: sqlglot reads to_hex as DuckDB's HEX of any value; a form's HEX of its bytes is written the same.
_TO_HEX = re.compile(r"\"?\bto_hex\"?\s*\(", re.IGNORECASE)
_SPECIAL_LITERAL = re.compile(r"\bB'[01]*'|\bU&'", re.IGNORECASE)


class _HexKept(Postgres):
    """PostgreSQL, with to_hex read as the call it is (not sqlglot's HEX of any value)."""

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "TO_HEX": lambda args: exp.Anonymous(this="to_hex", expressions=args),
        }


#: PostgreSQL's calls sqlglot reads as syntax of its own, and the form DuckDB's engine has of each.
_FORMS = {"convert": "pg_convert", "json_object": "pg_json_object", "to_hex": "pg_to_hex"}


def readable(pg_sql: str) -> str:
    """``pg_sql`` with a call of a function sqlglot reads as syntax its plain call of the form the
    engine has (pg_syntax reads each as PostgreSQL's call): a statement with none is left as it
    is. ``bit varying`` is the bit string the engine holds it as."""
    if not any(p.search(pg_sql) for p in (_SYNTAX_CALLS, _BIT_VARYING, _TO_HEX)):
        return pg_sql
    converted = False
    differs = False

    def held(node: exp.Expr) -> exp.Expr:
        nonlocal converted
        if isinstance(node, exp.DataType) and node.this == exp.DataType.Type.USERDEFINED:
            if node.args["kind"].name.lower() == "varbit":
                return exp.DataType(this=exp.DataType.Type.BIT, expressions=node.expressions)
        if isinstance(node, exp.Anonymous) and node.name.lower() in _FORMS:
            converted = converted or node.name.lower() == "convert"
            return exp.Anonymous(this=_FORMS[node.name.lower()], expressions=node.expressions)
        return node

    def _watched(node: exp.Expr) -> exp.Expr:
        nonlocal differs
        out = held(node)
        # A replacement always differs: each of _FORMS is another name, varbit another type.
        differs = differs or out is not node
        return out

    written = sqlglot.parse_one(pg_sql, read=_HexKept).transform(_watched, copy=False)
    if not differs:
        return pg_sql
    rendered = written.sql(dialect="postgres", copy=False)
    return postgres_literals(rendered) if converted else rendered


def typed_calls(pg_sql: str) -> str:
    """``pg_sql`` with each call of an overload DuckDB's function does not take rewritten."""
    pg_sql = readable(pg_sql)
    if (
        _CALLED.search(pg_sql) is None
        and _SPECIAL_LITERAL.search(pg_sql) is None
        and pg_xml.MENTIONED.search(pg_sql) is None
    ):
        return pg_sql
    rewritten = False

    def _watched(node: exp.Expr) -> exp.Expr:
        nonlocal rewritten
        out = _call(node)
        rewritten = rewritten or out is not node
        return out

    tree = sqlglot.parse_one(pg_sql, read="postgres").transform(_watched, copy=False)
    if not rewritten:
        return pg_sql
    # A root the rewrite lifted out of its parent keeps its copy: the generator reads a root's parent.
    return tree.sql(dialect="postgres", copy=tree.parent is not None)


def before(pg_sql: str) -> str:
    """``pg_sql`` with each call DuckDB's own function of the name does not carry rewritten."""
    pg_sql = typed_calls(pg_sql)
    if _NAMED.search(pg_sql) is None:
        return pg_sql
    rewritten = False

    def _watched(node: exp.Expr) -> exp.Expr:
        nonlocal rewritten
        out = _rewrite(node)
        rewritten = rewritten or out is not node
        return out

    tree = sqlglot.parse_one(pg_sql, read="postgres").transform(_watched, copy=False)
    if not rewritten:
        return pg_sql
    return tree.sql(dialect="postgres", copy=tree.parent is not None)


def _regexp_matches(args: list[exp.Expr]) -> str:
    """regexp_matches(string, pattern [, flags]): set-returning, one text[] per match -- the
    groups' texts, or the match's where the pattern has no group -- the first match only without
    the g flag. The group count is the pattern's, so a pattern is a literal."""
    value, pattern = args[0].sql(dialect="duckdb"), args[1]
    if _is_null(pattern):
        return "UNNEST(CAST(NULL AS VARCHAR[][]))"
    if not (isinstance(pattern, exp.Literal) and pattern.is_string):
        raise NotImplementedError(
            "regexp_matches: a pattern that is not a literal is not supported; its groups are "
            "counted where the statement is read"
        )
    flags = args[2].this if len(args) > 2 else ""  # noqa: PLR2004
    if set(flags) - {"g", "i"}:
        raise NotImplementedError(f"regexp_matches: flags {flags!r} are not supported")
    source = pattern.this
    written = ("(?i)" if "i" in flags else "") + source
    regex = exp.Literal.string(written).sql(dialect="duckdb")
    groups = range(1, capture_groups(source) + 1) or (0,)
    if "g" in flags:
        items = ", ".join(f"REGEXP_EXTRACT_ALL({value}, {regex}, {g})[_m]" for g in groups)
        return (
            f"UNNEST(LIST_TRANSFORM(RANGE(1, LEN(REGEXP_EXTRACT_ALL({value}, {regex}, 0)) + 1), "
            f"_m -> [{items}]))"
        )
    items = ", ".join(f"REGEXP_EXTRACT({value}, {regex}, {g})" for g in groups)
    return (
        f"UNNEST(CASE WHEN REGEXP_MATCHES({value}, {regex}) THEN [[{items}]] "
        "ELSE CAST([] AS VARCHAR[][]) END)"
    )


def _encode_escape(value: str) -> str:
    """encode(bytea, 'escape'): a zero or high byte as \\ooo, a backslash doubled, any other
    byte as its character."""
    return (
        f"ARRAY_TO_STRING(LIST_TRANSFORM(REGEXP_EXTRACT_ALL(HEX({value}), '..'), _h -> "
        "CASE WHEN _h = '5C' THEN '\\\\' "
        "WHEN _h = '00' OR _h >= '80' THEN PRINTF('\\%03o', CAST('0x' || _h AS INT)) "
        "ELSE CHR(CAST('0x' || _h AS INT)) END), '')"
    )


_TO_JSON = "to_json"


def _instant_to_json(value: str) -> str:
    """to_json(value): an instant as PostgreSQL's JSON writes it, the ISO form with a whole offset
    (2026-07-01T08:00:00.5-04:00), where DuckDB's is its text (REQ-2171). Which it is is the
    value's type, which DuckDB knows and the statement's text does not say."""
    # The value is written once, as the one element of a list the lambda reads: its own to_json
    # calls nest, and a form that wrote it three times grew as 3 to the depth of the nesting.
    written = "CAST(to_json(x) AS VARCHAR)"
    iso = rf"""regexp_replace({written}, '^"(\d+-\d\d-\d\d) ', '"\1T')"""
    return (
        f"list_transform([{value}], x -> CASE WHEN typeof(x) = 'TIMESTAMP WITH TIME ZONE' "
        rf"""THEN CAST(regexp_replace({iso}, '([+-]\d\d)"$', '\1:00"') AS JSON) """
        "ELSE to_json(x) END)[1]"
    )


def _typed_by_its_form(node: exp.Expr) -> bool:
    """Whether this to_json is in a branch of the form ``pg_forms_json.json_encoded`` wrote
    (``CASE WHEN typeof(v) IN ('TIMESTAMP', ...) THEN ... ELSE to_json(v) END``): the instant is
    already told apart there. Written again, each to_json of the form's output would carry the
    whole form, and a statement of nested objects grows past what can be parsed."""
    case = node.find_ancestor(exp.Case)
    if case is None or not case.args.get("ifs"):
        return False
    test = case.args["ifs"][0].this
    return isinstance(test, exp.In) and isinstance(test.this, exp.Typeof)


def _unwrapped(value: exp.Expr) -> exp.Expr:
    while isinstance(value, (exp.Filter, exp.Paren)):
        value = value.this
    return value


def _never_an_instant(value: exp.Expr) -> bool:
    """Whether the value is an object or array built here (a JSON_OBJECT, a JSON_ARRAY, filtered
    or not, or a list of them): its members were each told apart as they were written, so nothing
    is told apart again."""
    value = _unwrapped(value)
    if isinstance(value, exp.ArrayAgg):  # json_agg(json_object(...)): the objects are such
        value = _unwrapped(value.this)
    elif isinstance(value, exp.Array) and value.expressions:
        return all(_never_an_instant(item) for item in value.expressions)
    return isinstance(value, (exp.JSONObject, exp.JSONArray))


def _is_list(value: exp.Expr) -> bool:
    """Whether the value is a list of the statement's own (an ARRAY_AGG or an ARRAY[...]): to_json
    of it holds instants DuckDB writes as its text, by the list's type, which only it knows."""
    return isinstance(_unwrapped(value), (exp.ArrayAgg, exp.Array))


_INSTANT_LISTS = "('TIMESTAMP WITH TIME ZONE[]', 'TIMESTAMP[]')"


def _list_to_json(value: str) -> str:
    """to_json(list): a list of instants with each written as PostgreSQL's JSON writes it (the ISO
    form, a whole offset), a list of anything else left to to_json. The value is named once, as
    in _instant_to_json."""
    iso = (
        r"regexp_replace(regexp_replace(CAST(e AS VARCHAR), '^(\d+-\d\d-\d\d) ', '\1T'), "
        r"'([+-]\d\d)$', '\1:00')"
    )
    return (
        f"list_transform([{value}], x -> CASE WHEN typeof(x) IN {_INSTANT_LISTS} "
        f"THEN to_json(list_transform(x, e -> {iso})) ELSE to_json(x) END)[1]"
    )


def after(sql: str) -> str:
    """The DuckDB statement with each placeholder ``before`` left written out."""
    lowered = sql.lower()
    placeholders = (_REGEXP_MATCHES, _ENCODE_ESCAPE, _REGEXP_GROUP, _DATE_PART, _TO_JSON)
    if not any(name in lowered for name in placeholders):
        return sql
    return after_in(sqlglot.parse_one(sql, read="duckdb")).sql(dialect="duckdb")


def after_in(tree: exp.Expr) -> exp.Expr:
    """``tree``, a DuckDB statement, with each placeholder ``before`` left written out."""

    def _written(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Anonymous):
            name = node.name.lower()
            if name == _REGEXP_MATCHES:
                return sqlglot.parse_one(_regexp_matches(node.expressions), read="duckdb")
            if name == _DATE_PART:  # DuckDB's parser would read it as EXTRACT, field not a Var
                return exp.Anonymous(this="DATE_PART", expressions=node.expressions)
            if name == _REGEXP_GROUP:
                written = ", ".join(a.sql(dialect="duckdb") for a in node.expressions)
                return sqlglot.parse_one(f"REGEXP_EXTRACT({written})", read="duckdb")
            if name == _ENCODE_ESCAPE:
                value = node.expressions[0].transform(_written).sql(dialect="duckdb")
                return sqlglot.parse_one(_encode_escape(value), read="duckdb")
            if name == _TO_JSON and len(node.expressions) == 1:
                value = node.expressions[0]
                listed = _is_list(value)
                # A list is told apart here even in the branch of the scalar form, whose
                # typeof test names no list.
                if (not listed and _typed_by_its_form(node)) or _never_an_instant(value):
                    return node
                # Its value's own placeholders written first: the form replacing the call is
                # not walked again.
                written = value.transform(_written).sql(dialect="duckdb")
                form = _list_to_json if listed else _instant_to_json
                return sqlglot.parse_one(form(written), read="duckdb")
        return node

    return tree.transform(_written, copy=False)
