# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2d8e-91c4-4a7e-b5d2-7e0c4a19f863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""regexp_replace's flags (REQ-2236; replayed against postgres:17.11 by the function support
measure, REQ-2171).

PostgreSQL's regexp_replace replaces the first match unless its flags say ``g`` (every match), and
reads ``i`` as case-insensitive and ``c`` as case-sensitive, the last of the two winning. Trino's
and ClickHouse's three-argument form replaces every match and takes no flags: a fourth argument was
sent as written, and Trino's reading of the statement dropped it, so ``'i'`` answered every match,
case-sensitively. Here the flags are read from the call: a case-insensitive pattern is the
pattern in ``(?i:...)``; the first match is the pattern with the text before and after it held,
so the replacement is that match's alone. A flag of another kind, a flag that is no constant, and a
first match whose pattern or replacement is no constant or names the whole match (``\\0``) or a
group by number in the pattern are refused by name. The start and N forms are other rows."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import re

import sqlglot.expressions as exp

from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite

FAMILY = "regular expressions"

#: The dialects whose own regexp_replace replaces every match and reads no flags.
_FLAGS_READ_HERE = ("trino", "presto", "athena", "clickhouse")

#: The flags read here: every match, case-insensitive, case-sensitive, and tight syntax (the
#: default, which changes nothing).
_EVERY, _NO_CASE, _CASE, _TIGHT = "g", "i", "c", "t"


def refusal(dialect: str) -> Refusal:
    """What a call no form writes on ``dialect`` is refused with."""
    return Refusal("0A000", f"regexp_replace() has no form on {dialect}")


def said_text(value: exp.Expr) -> exp.Expr:
    """A string literal cast to text as the literal, which is what sqlglot reads a format from."""
    if isinstance(value, exp.Cast) and value.to.is_type("text") and value.this.is_string:
        return value.this
    return value


#: A group the pattern captures by number: its numbers move with the text held before it.
_BACKREFERENCE = re.compile(r"\\\d")
#: What a pattern's text can be: an escape, a bracket expression, or a group it captures by.
_PATTERN_PART = re.compile(r"\\.|\[\^?\]?(?:\\.|\[:\w+:\]|[^\]])*\]|\((?!\?)", re.DOTALL)
#: What a replacement says other than its literal text: \\, \N, or a $.
_REPLACEMENT_PART = re.compile(r"\\(\\|\d)|\$")


def _past_the_text_before(replacement: str, pattern: str, dialect: str) -> str:
    """``replacement`` for the first match's form, where the text before the match is group 1:
    PostgreSQL's \\N is the engine's $N+1, nothing for a group the pattern lacks, and a $ is
    literal."""
    groups = sum(part.group() == "(" for part in _PATTERN_PART.finditer(pattern))

    def written(part: re.Match[str]) -> str:
        said = part.group(1)
        if said is None:
            return "\\$"
        if said == "\\":
            return part.group()
        if said == "0":
            raise Refused(refusal(dialect))
        return f"${int(said) + 1}" if int(said) <= groups else ""

    return _REPLACEMENT_PART.sub(written, replacement)


def _flags(said: exp.Expr | None, dialect: str) -> tuple[bool, bool]:
    """Whether the flags say every match, and whether they say case-insensitive."""
    if said is None:
        return False, False
    written = said_text(said)
    if not (isinstance(written, exp.Literal) and written.is_string):
        raise Refused(refusal(dialect))
    every, no_case = False, False
    for flag in written.this:
        if flag == _EVERY:
            every = True
        elif flag in (_NO_CASE, _CASE):
            no_case = flag == _NO_CASE
        elif flag != _TIGHT:
            raise Refused(refusal(dialect))
    return every, no_case


def _is_null(value: exp.Expr) -> bool:
    value = value.unnest()
    return isinstance(value.this if isinstance(value, exp.Cast) else value, exp.Null)


def _no_case(pattern: exp.Expr) -> exp.Expr:
    """``pattern`` read case-insensitively: in a group of its own, so its alternatives stay in it."""
    if isinstance(pattern, exp.Literal) and pattern.is_string:
        return exp.Literal.string(f"(?i:{pattern.this})")
    return exp.DPipe(
        this=exp.DPipe(this=exp.Literal.string("(?i:"), expression=pattern),
        expression=exp.Literal.string(")"),
    )


def _first_match(value: exp.Expr, pattern: exp.Expr, replacement: exp.Expr, dialect: str):
    """The first match replaced alone, or a refusal of what no form writes."""
    if not (isinstance(pattern, exp.Literal) and isinstance(replacement, exp.Literal)):
        raise Refused(refusal(dialect))
    if dialect == "clickhouse":
        return exp.Anonymous(this="replaceRegexpOne", expressions=[value, pattern, replacement])
    if _BACKREFERENCE.search(pattern.this):  # its group numbers move with the text before
        raise Refused(refusal(dialect))
    written = _past_the_text_before(replacement.this, pattern.this, dialect)
    return exp.RegexpReplace(
        this=value,
        expression=exp.Literal.string(f"(?s)^(?<before>.*?)(?:{pattern.this})(?<after>.*)$"),
        replacement=exp.Literal.string(f"${{before}}{written}${{after}}"),
    )


def flagged(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """regexp_replace with PostgreSQL's flags read: the first match or every one, its case."""
    if not isinstance(node, exp.RegexpReplace):
        return None
    said = node.args.get("modifiers")
    if said is not None and _is_null(said):
        return exp.Cast(this=exp.Null(), to=exp.DataType.build("TEXT"))  # strict
    every, no_case = _flags(said, dialect)
    value = node.this
    pattern = said_text(node.expression)
    replacement = said_text(node.args["replacement"])
    if no_case:
        pattern = _no_case(pattern)
    if every:
        return exp.RegexpReplace(this=value, expression=pattern, replacement=replacement)
    return _first_match(value, pattern, replacement, dialect)


_LIMITATIONS = {
    dialect: (
        Limitation(
            "A flag other than g, i, c and t, a flag that is no constant, and a first match whose "
            "pattern or replacement is no constant, whose replacement names the whole match "
            "(\\0) or whose pattern names a group by number are refused.",
            refusal(dialect),
        ),
    )
    for dialect in _FLAGS_READ_HERE
}

REGEXP_REPLACE = Feature(
    "regexp_replace",
    FAMILY,
    "The first match replaced, or every one with the g flag; i and c set the case.",
    read=Side(
        rules=(
            # The flags are text: an integer fourth argument is the start, another row's. A call
            # the statement gives no flags is given none ('') before the grid reads it
            # (transpile._no_flags_said): one of three arguments a rewrite writes is every match.
            Rule(
                "regexp_replace($s, $p, $r, $f::text)",
                dict.fromkeys(_FLAGS_READ_HERE, Rewrite(flagged)),
            ),
        ),
        limitations=_LIMITATIONS,
    ),
)

FEATURES: tuple[Feature, ...] = (REGEXP_REPLACE,)
