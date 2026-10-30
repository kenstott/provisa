# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's functions on DuckDB where a name alone does not carry them (REQ-2167, PGWIRE'S
CATALOG FUNCTIONS).

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

_REGEXP_MATCHES = "provisa_regexp_matches"
_ENCODE_ESCAPE = "provisa_encode_escape"
_REGEXP_GROUP = "provisa_regexp_group"
_DATE_PART = "provisa_date_part"

#: The statements ``before`` has anything to do in.
_NAMED = re.compile(
    r"\b(sha(224|256|384|512)|random|regexp_matches|timezone|to_timestamp|[blr]?trim|setseed"
    r"|overlay|substring|extract|decode|encode|pg_size_pretty|current_schemas)\b",
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


def before(pg_sql: str) -> str:
    """``pg_sql`` with each call DuckDB's own function of the name does not carry rewritten."""
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
