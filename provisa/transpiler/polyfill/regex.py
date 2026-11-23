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

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

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

# --- regexp_count, regexp_instr, regexp_substr, regexp_like, regexp_split_to_array on ClickHouse ---

_CH = "clickhouse"
#: The functions written here on ClickHouse, each by its PostgreSQL name.
_ON_CLICKHOUSE = (
    "regexp_count",
    "regexp_instr",
    "regexp_substr",
    "regexp_like",
    "regexp_split_to_array",
)
#: The escapes PostgreSQL's advanced expressions and ClickHouse's RE2 both read as one thing: a
#: digit class (PostgreSQL's reads the ASCII digits in a UTF-8 database, as RE2's does), a control
#: character, the start of the text, and an escaped character that is no letter or digit.
_SAME_ESCAPE = re.compile(r"\\(?:[dDntrfvA]|[^0-9A-Za-z])")
#: What RE2 reads otherwise or not at all: a back reference, \b (a backspace in PostgreSQL's, a
#: word boundary in RE2's), the classes of a locale (\w, \s, [[:alpha:]]), a collating element,
#: lookaround, and PostgreSQL's director prefixes (***=, ***:).
_OTHER_SYNTAX = re.compile(r"\[[:.=]|\(\?[=!<]|^\*\*\*")


def _ch_refusal(function: str, why: str) -> Refusal:
    return Refusal("0A000", f"{function}() {why} has no form on clickhouse")


def _re2_pattern(function: str, pattern: exp.Expr, no_case: bool) -> str:
    """A literal pattern as RE2 reads it as PostgreSQL does: a newline is an ordinary character
    ((?s)), case-insensitive where the flags say so. A pattern that is no literal, or that holds
    a construct RE2 reads otherwise or not at all, is refused by name."""
    written = said_text(pattern)
    if not (isinstance(written, exp.Literal) and written.is_string):
        raise Refused(_ch_refusal(function, "of a pattern that is no literal"))
    text = written.this
    escapes = re.findall(r"\\.?", text, re.DOTALL)
    if any(not _SAME_ESCAPE.fullmatch(e) for e in escapes) or _OTHER_SYNTAX.search(text):
        raise Refused(_ch_refusal(function, "of a construct RE2 reads otherwise"))
    return f"(?s){'(?i)' if no_case else ''}{text}"


def _ch_flags(function: str, said: exp.Expr | None) -> bool | None:
    """Whether the flags say case-insensitive; None for a NULL (a strict call answers NULL). The
    global flag is PostgreSQL's 22023 for these functions; a flag other than i and c, or one that
    is no literal, is refused by name."""
    if said is None:
        return False
    if _is_null(said):
        return None
    written = said_text(said)
    if not (isinstance(written, exp.Literal) and written.is_string):
        raise Refused(_ch_refusal(function, "of flags that are no literal"))
    no_case = False
    for flag in written.this:
        if flag == _EVERY and function != "regexp_split_to_array":
            raise Refused(
                Refusal(
                    "22023",
                    f'{function}() does not support the "global" option',
                ),
                ruling=True,
            )
        if flag in (_NO_CASE, _CASE):
            no_case = flag == _NO_CASE
        elif flag != _TIGHT:
            raise Refused(_ch_refusal(function, f"of the flag {flag!r}"))
    return no_case


def _invalid(parameter: str) -> str:
    return ch_fail("22023", f'invalid value for parameter "{parameter}"')


#: The text from the start (1 where no start is said).
_FROM = "substringUTF8($s, toInt64($b))"
#: The pattern matching the empty text: a count, a position or a split of it is not RE2's.
_EMPTY_MATCH = ch_fail("0A000", "a pattern that matches the empty text has no form on clickhouse")


def _with_checks(checks: list[str], answer: str) -> str:
    return f"if({' + '.join(checks)} = 0, {answer}, NULL)"


def _ch_form(function: str, count: int, re2: str) -> str:
    """The ClickHouse form of ``function`` of ``count`` arguments, its holes $s (the text), $b (the
    start), $n (the occurrence), $e (the end option) and $x (the subexpression), of the RE2 pattern
    ``re2``."""
    pattern = "'" + re2.replace("\\", "\\\\").replace("'", "\\'") + "'"
    whole = "'(" + re2.replace("\\", "\\\\").replace("'", "\\'") + ")'"
    empty = _EMPTY_MATCH.format(condition=f"match('', {pattern})")
    start = _invalid("start").format(condition="toInt64($b) < 1")
    if function == "regexp_like":
        return f"CAST(match($s, {pattern}) AS Nullable(Bool))"
    if function == "regexp_count":
        return _with_checks(
            [start, empty], f"CAST(countMatches({_FROM}, {pattern}) AS Nullable(Int32))"
        )
    if function == "regexp_split_to_array":
        null = ch_fail(
            "0A000",
            "regexp_split_to_array() of a NULL text that is no literal has no form on clickhouse",
        )
        pieces = f"splitByRegexp({pattern}, ifNull($s, ''))"
        return f"if({null.format(condition='$s IS NULL')} + {empty} = 0, {pieces}, {pieces})"
    occurrence = _invalid("n").format(condition="toInt64($n) < 1")
    matches = f"extractAll({_FROM}, {whole})"
    if function == "regexp_substr":
        subexpression = _invalid("subexpr").format(condition="toInt64($x) < 0")
        found = f"if(length({matches}) >= toInt64($n), {matches}[toInt64($n)], NULL)"
        return _with_checks(
            [start, occurrence, subexpression], f"CAST({found} AS Nullable(String))"
        )
    # regexp_instr: the text before the nth match, in characters, past the start
    end = _invalid("endoption").format(condition="toInt64($e) NOT IN (0, 1)")
    subexpression = _invalid("subexpr").format(condition="toInt64($x) < 0")
    pieces = f"splitByRegexp({pattern}, {_FROM})"
    before = (
        f"arraySum(arrayMap(_p -> lengthUTF8(_p), arraySlice({pieces}, 1, toInt64($n)))) + "
        f"arraySum(arrayMap(_m -> lengthUTF8(_m), arraySlice({matches}, 1, toInt64($n) - 1)))"
    )
    past = f"if(toInt64($e) = 1, lengthUTF8({matches}[toInt64($n)]), 0)"
    position = f"if(length({matches}) >= toInt64($n), toInt64($b) + {before} + {past}, 0)"
    return _with_checks(
        [start, occurrence, end, subexpression, empty], f"CAST({position} AS Nullable(Int32))"
    )


#: Each function's arguments by position past the text and the pattern, and what each is.
_SHAPES = {
    "regexp_count": ("b", "f"),
    "regexp_instr": ("b", "n", "e", "f", "x"),
    "regexp_substr": ("b", "n", "f", "x"),
    "regexp_like": ("f",),
    "regexp_split_to_array": ("f",),
}
_DEFAULTS = {"b": "1", "n": "1", "e": "0", "x": "0"}


def _ch_regexp(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A regexp function of a literal pattern and literal flags on ClickHouse (_ch_form)."""
    call = trino.called(node)
    if call is None:
        return None
    names, args = call
    function = next((n for n in _ON_CLICKHOUSE if n in names), None)
    if function is None or not 2 <= len(args) <= 2 + len(_SHAPES[function]):
        return None
    said = dict(zip(_SHAPES[function], args[2:], strict=False))
    if function == "regexp_split_to_array" and node.find_ancestor(exp.Unnest) is not None:
        # The rows of its pieces (a server form's, oidvectortypes'): ClickHouse runs no UNNEST
        # WITH ORDINALITY of them, so the call is refused by name, as it was.
        raise Refused(_ch_refusal(function, "of pieces read as a set of rows"), ruling=True)
    if function == "regexp_like" and len(args) == 2:
        return None  # ClickHouse's own match, sqlglot's form of it
    if function == "regexp_count" and len(args) == 2 and not _is_literal_text(args[1]):
        return None  # ClickHouse's own countMatches, sqlglot's form of it
    # regexp_count of a literal pattern takes the form of its start of 1: countMatches skips an
    # empty match (regexp_count('abc', '') is 4 on PostgreSQL, 0 on ClickHouse), so such a
    # pattern raises 0A000, and a newline is an ordinary character, as PostgreSQL reads it.
    if any(_is_null(a) for a in args[:2]) or any(_is_null(a) for a in said.values()):
        typed = {
            "regexp_like": "Nullable(Bool)",
            "regexp_substr": "Nullable(String)",
            "regexp_split_to_array": None,
        }.get(function, "Nullable(Int32)")
        if typed is None:
            return exp.null()  # strict; ClickHouse holds no NULL of an array
        return exp.cast(exp.null(), exp.DataType.build(typed, dialect=dialect))
    no_case = _ch_flags(function, said.get("f"))
    assert no_case is not None  # a NULL flag is a NULL argument
    subexpression = said.get("x")
    if subexpression is not None and _constant_integer(subexpression) != 0:
        raise Refused(_ch_refusal(function, "of a subexpression"))
    re2 = _re2_pattern(function, args[1], no_case)
    holes = ("s", "b", "n", "e", "x")
    bindings = {"s": args[0]}
    for hole in holes[1:]:
        bindings[hole] = said.get(hole) or exp.Literal.number(_DEFAULTS[hole])
    form = _ch_form(function, len(args), re2)
    pattern = "f(" + ", ".join("$" + h for h in holes) + ")"
    return graft(replacement(form, dialect, pattern), bindings)


def _is_literal_text(value: exp.Expr) -> bool:
    written = said_text(value)
    return isinstance(written, exp.Literal) and written.is_string


def _constant_integer(value: exp.Expr) -> int | None:
    node = value.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    negative = False
    while isinstance(node, (exp.Neg, exp.Paren)):
        negative ^= isinstance(node, exp.Neg)
        node = node.this
    if isinstance(node, exp.Literal) and not node.is_string and node.is_int:
        return -int(node.this) if negative else int(node.this)
    return None


CH_REGEXPS = Feature(
    "regular expressions on ClickHouse",
    FAMILY,
    "regexp_count, regexp_instr, regexp_substr, regexp_like and regexp_split_to_array of a literal "
    "pattern and literal flags.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    _CH: Rewrite(
                        _ch_regexp,
                        mentions=r"\bregexp_(?:count|instr|substr|like|split_to_array)\b",
                    )
                },
            ),
        ),
        limitations={
            _CH: (
                Limitation(
                    "The pattern and the flags are literals, read by RE2 with a newline an "
                    "ordinary character: a pattern of a back reference, a locale's class (\\w, "
                    "\\s, [[:alpha:]]), \\b, lookaround or a director prefix, a flag other than "
                    "i and c, and a subexpression other than 0 are refused by name; a pattern "
                    "that matches the empty text raises 0A000 where it counts, places or splits, "
                    "and a NULL text that is no literal raises 0A000 in regexp_split_to_array "
                    "(ClickHouse holds no NULL of an array).",
                ),
            )
        },
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 80
FEATURES: tuple[Feature, ...] = (REGEXP_REPLACE, CH_REGEXPS)
