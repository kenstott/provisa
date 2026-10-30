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
``random(min, max)``, ``regexp_matches``, ``substring(text FROM pattern)``, ``sha256`` (DuckDB's
answers hex text, PostgreSQL's the digest), ``btrim`` of a bytea, and so on. ``after`` writes, on
the DuckDB statement, what PostgreSQL's parser would not keep (it reads ``->`` as a JSON operator,
a third REGEXP_EXTRACT argument as a position, and DATE_PART as EXTRACT): those calls cross the transpile as a
placeholder function."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

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
    "money": "money",
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

#: The statements ``before`` has anything to do in.
_NAMED = re.compile(
    r"\b(sha(224|256|384|512)|random|regexp_matches|timezone|to_timestamp|[blr]?trim|setseed"
    r"|overlay|substring|extract|decode|encode|pg_size_pretty|current_schemas)\b",
    re.IGNORECASE,
)
#: The statements ``typed_calls`` has anything to do in.
_CALLED = re.compile(
    r"\b("
    + "|".join(sorted({*_TYPE_CALLS, *_SIZED_CALLS, *(n for n, _ in _UDF_CALLS)}))
    + r"|bit_count|get_bit|set_bit|substr|substring|overlay|gcd|lcm|factorial|generate_series"
    r"|isfinite|date_add|timezone|length|string_agg|position|sum|char|chr|bit|xid|age|like"
    r"|notlike|like_escape|convert|json_object|unnest|trunc|pg_get_object_address"
    r"|regexp_split_to_table|strpos)\"?\s*\(",
    re.IGNORECASE,
)

_RANDOM_BOUNDS = "lower bound must be less than or equal to upper bound"


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


def _random(low: str, high: str, nodes: tuple[exp.Expr, exp.Expr]) -> str:
    """random(min, max): an integer of the bounds' type, or a numeric of the larger scale of the
    two, uniform between them, refused as PostgreSQL refuses inverted bounds."""
    type_ = exp.DataType.Type
    whole = f"{low} + FLOOR(RANDOM() * ({high} - {low} + 1))"
    between = f"{low} + ({high} - {low}) * RANDOM()"
    if any(_cast_to(n, type_.BIGINT) for n in nodes):
        drawn = f"CAST({whole} AS BIGINT)"
    elif all(
        _cast_to(n, type_.INT, type_.SMALLINT) or (isinstance(n, exp.Literal) and n.is_int)
        for n in nodes
    ):
        drawn = f"CAST({whole} AS INT)"
    elif any(_cast_to(n, type_.DECIMAL) or _scale(n) is not None for n in nodes):
        scales = [s for s in map(_scale, nodes) if s is not None]
        drawn = f"ROUND({between}, {max(scales)})" if scales else between
    else:
        # The statement does not say the bounds' type: the engine knows it when it binds.
        drawn = f"CASE WHEN TYPEOF({low}) LIKE 'DECIMAL%' THEN {between} ELSE {whole} END"
    return f"CASE WHEN {low} > {high} THEN ERROR('{_RANDOM_BOUNDS}') ELSE {drawn} END"


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
        implicit = node.this
        if not isinstance(implicit, exp.Boolean):
            written = "CAST(NULL AS TEXT[])"  # strict: a NULL argument
        elif implicit.this:
            written = "ARRAY['pg_catalog', 'public']"
        else:
            written = "ARRAY['public']"
    elif isinstance(node, exp.SHA2):
        length = node.args.get("length")
        bits = length.this if isinstance(length, exp.Literal) else "256"
        value = text(node.this)
        written = f"UNHEX(SHA256({value}))" if bits == "256" else f"PROVISA_SHA{bits}({value})"
    elif isinstance(node, exp.Rand) and node.args.get("lower") is not None:
        low, high = node.this, node.args["lower"]
        written = _random(text(low), text(high), (low, high))
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
    elif isinstance(node, exp.Extract) and not isinstance(node.this, exp.Var):
        written = f"{_DATE_PART}({text(node.this)}, {text(node.expression)})"
    elif isinstance(node, exp.Encode) and node.args.get("charset") is not None:
        written = _decoded(text(node.this), node.args["charset"], "encode")
    elif isinstance(node, exp.Decode) and node.args.get("charset") is not None:
        written = _decoded(text(node.this), node.args["charset"], "decode")
    elif isinstance(node, exp.Anonymous):
        name = node.name.lower()
        args = node.expressions
        if name == "sha224" and len(args) == 1:
            written = (
                f"PROVISA_SHA224({text(args[0])})"  # sha256's kin: a UDF, as it is not DuckDB's
            )
        elif name == "timezone" and len(args) == 1:
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


def _tsvector_call(name: str, args: list[exp.Expr]) -> str | None:
    if not args or not _is_kind(args[0], "tsvector"):
        return None
    value = _sql(args[0])
    if name == "unnest":
        return f"UNNEST(PROVISA_TSVECTOR_ITEMS({value}))"
    if name == "length":
        return f"LEN(PROVISA_TSVECTOR_ITEMS({value}))"
    return None


def _geometry_call(name: str, args: list[exp.Expr]) -> str | None:
    """length of a line segment or a path, which DuckDB's length would count as text."""
    if name == "length" and len(args) == 1 and _is_kind(args[0], "lseg", "path"):
        return f"PROVISA_GEOMETRY_LENGTH({_sql(args[0])})"
    return None


def _mac_call(name: str, args: list[exp.Expr]) -> str | None:
    if name == "trunc" and len(args) == 1 and _is_kind(args[0], "macaddr", "macaddr8"):
        zeros = "00:00:00" if _is_kind(args[0], "macaddr") else "00:00:00:00:00"
        return f"CAST(LEFT({_sql(args[0])}, 9) || '{zeros}' AS TEXT)"
    return None


def _is_numeric(node: exp.Expr) -> bool:
    return _cast_to(node, exp.DataType.Type.DECIMAL) or _scale(node) is not None


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
        if isinstance(typmod, exp.Literal) and typmod.is_int:
            packed = int(typmod.this) - 4
            sized = f"NUMERIC({packed >> 16}, {packed & 0xFFFF})"
            return f"CAST(CAST({_sql(args[0])} AS {sized}) AS TEXT)"
    if name in _SIZED_CALLS and len(args) == 3:  # noqa: PLR2004 -- value, typmod, explicit
        value, typmod = _sql(args[0]), _sql(args[1])
        return f"CASE WHEN ({typmod}) < 0 THEN {value} ELSE LEFT({value}, ({typmod}) - 4) END"
    if name == "bit" and len(args) == 3 and _is_null(args[1]):  # noqa: PLR2004
        return "CAST(NULL AS BIT)"
    if name == "bit" and len(args) == 2:  # noqa: PLR2004 -- an integer, as so many bits
        value, width = _sql(args[0]), _sql(args[1])
        bits = f"CAST({value} AS HUGEINT) & ((CAST(1 AS HUGEINT) << ({width})) - 1)"
        return f"CAST(FORMAT('{{:b}}', {bits}) AS BIT)"
    return None


def _numeric_call(name: str, args: list[exp.Expr]) -> str | None:
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


def _time_call(name: str, args: list[exp.Expr]) -> str | None:
    interval = exp.DataType.Type.INTERVAL
    written = [_sql(a) for a in args]
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
        return f"TO_SECONDS(SUM(EPOCH({written[0]})))"
    return None


def _named(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of a call of a function a parser class stands for."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if isinstance(node, exp.Chr):
        return "char", list(node.expressions)
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
        exp.Trunc: ("trunc", ("this",)),
        exp.Explode: ("unnest", ("this",)),
        exp.Unnest: ("unnest", ("expressions",)),
        exp.Overlay: ("overlay", ("this", "expression", "from_", "for_")),
        exp.Length: ("length", ("this", "encoding")),
        exp.GroupConcat: ("string_agg", ("this", "separator")),
        exp.StrPosition: ("position", ("this", "substr")),
        exp.Sum: ("sum", ("this",)),
        exp.DateAdd: ("date_add", ("this", "expression", "unit")),
    }.get(type(node))
    if args is None:
        return None
    values = [node.args[k] for k in args[1] if node.args.get(k) is not None]
    flat = [v for value in values for v in (value if isinstance(value, list) else [value])]
    return args[0], flat


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
    named = _named(node)
    if named is None:
        return node
    name, args = named
    if name == "pg_get_object_address" and len(args) == 3:  # noqa: PLR2004
        written = f"pg_get_object_address({_sql(args[0])}, {_text_array(args[1])}, {_text_array(args[2])})"
        return sqlglot.parse_one(written, read="postgres")
    written = (
        _type_call(name, args)
        or _numeric_call(name, args)
        or _bytea_call(name, args)
        or _bit_call(name, args)
        or _time_call(name, args)
        or _tsvector_call(name, args)
        or _geometry_call(name, args)
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


#: What sqlglot's parser cannot read as a call: CONVERT's and JSON_OBJECT's own syntax.
_UNPARSED_CALLS = re.compile(r"\b(convert|json_object)\s*\(", re.IGNORECASE)
_BIT_VARYING = re.compile(r"::\s*bit varying\b", re.IGNORECASE)
_JSON_OBJECT_OF_ARRAYS = re.compile(r"\bjson_object\s*\(\s*(?='\{|ARRAY\[)", re.IGNORECASE)
_SPECIAL_LITERAL = re.compile(r"\bB'[01]*'|\bU&'", re.IGNORECASE)


def readable(pg_sql: str) -> str:
    """``pg_sql`` with a call of a function sqlglot reads as syntax spelled as a plain call: a
    statement that parses is left as it is. ``bit varying``, which sqlglot reads as a bit and an
    alias, is the bit string the engine holds it as."""
    pg_sql = _JSON_OBJECT_OF_ARRAYS.sub("pg_json_object(", _BIT_VARYING.sub("::bit", pg_sql))
    if _UNPARSED_CALLS.search(pg_sql) is None:
        return pg_sql
    try:
        sqlglot.parse_one(pg_sql, read="postgres")
    except ParseError:
        return postgres_literals(_UNPARSED_CALLS.sub(lambda m: f"pg_{m[1].lower()}(", pg_sql))
    return pg_sql


def typed_calls(pg_sql: str) -> str:
    """``pg_sql`` with each call of an overload DuckDB's function does not take rewritten."""
    pg_sql = readable(pg_sql)
    if _CALLED.search(pg_sql) is None and _SPECIAL_LITERAL.search(pg_sql) is None:
        return pg_sql
    rewritten = False

    def _watched(node: exp.Expr) -> exp.Expr:
        nonlocal rewritten
        out = _call(node)
        rewritten = rewritten or out is not node
        return out

    tree = sqlglot.parse_one(pg_sql, read="postgres").transform(_watched)
    return tree.sql(dialect="postgres") if rewritten else pg_sql


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

    tree = sqlglot.parse_one(pg_sql, read="postgres").transform(_watched)
    return tree.sql(dialect="postgres") if rewritten else pg_sql


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


def after(sql: str) -> str:
    """The DuckDB statement with each placeholder ``before`` left written out."""
    lowered = sql.lower()
    placeholders = (_REGEXP_MATCHES, _ENCODE_ESCAPE, _REGEXP_GROUP, _DATE_PART)
    if not any(name in lowered for name in placeholders):
        return sql

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
        return node

    return sqlglot.parse_one(sql, read="duckdb").transform(_written).sql(dialect="duckdb")
