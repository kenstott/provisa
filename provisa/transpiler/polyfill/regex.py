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

from provisa.transpiler import are_re2
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "regular expressions"

#: The meta key of a match whose pattern is the engine's own flavour, written by Provisa
#: (transpile._regex_found): it is no PostgreSQL pattern, so it is not read as one here.
ENGINE_PATTERN = "engine_pattern"

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
    if _BACKREFERENCE.search(pattern.this):  # its group numbers move with the text before
        raise Refused(refusal(dialect))
    written = _past_the_text_before(replacement.this, pattern.this, dialect)
    return exp.RegexpReplace(
        this=value,
        expression=exp.Literal.string(f"(?s)^(?<before>.*?)(?:{pattern.this})(?<after>.*)$"),
        replacement=exp.Literal.string(f"${{before}}{written}${{after}}"),
    )


def _is_empty_text(value: exp.Expr) -> bool:
    return isinstance(value, exp.Literal) and value.is_string and value.this == ""


#: The empty pattern on ClickHouse (REQ-2236): it matches before each character and at the end, so
#: PostgreSQL's first match puts the replacement before the text, and every match puts it before
#: each character and after the last. ClickHouse's replaceRegexpOne and replaceRegexpAll answer the
#: text unchanged (or raise, before 26.x), so the answer is written here. $v is the text, $r the
#: replacement's literal.
_CH_EMPTY_FIRST = "concat($r, $v)"
_CH_EMPTY_EVERY = (
    "if($v IS NULL, NULL, concat(arrayStringConcat(arrayMap("
    "_i -> concat($r, substringUTF8(ifNull($v, ''), _i, 1)), "
    "range(1, toUInt64(lengthUTF8(ifNull($v, ''))) + 1))), $r))"
)


def _ch_empty_pattern(value: exp.Expr, said: exp.Expr, every: bool) -> exp.Expr:
    """regexp_replace of the empty pattern on ClickHouse (_CH_EMPTY_FIRST, _CH_EMPTY_EVERY). A
    replacement that is no literal, or that holds a backslash (an escape PostgreSQL reads), is
    refused by name."""
    if not (isinstance(said, exp.Literal) and said.is_string) or "\\" in said.this:
        raise Refused(refusal("clickhouse"))
    form = _CH_EMPTY_EVERY if every else _CH_EMPTY_FIRST
    return graft(
        replacement(form, "clickhouse", "f($v, $r)"),
        {"v": value, "r": exp.Literal.string(said.this)},
    )


#: A pattern that matches the empty text, every match on ClickHouse (REQ-2236): PostgreSQL's g flag
#: replaces an empty match and copies the next character before it searches again, where
#: ClickHouse's replaceRegexpAll stops at the first empty match (regexp_replace('abc', 'x*', '-',
#: 'g') is -a-b-c- on PostgreSQL, -abc on ClickHouse). Where the pattern is a literal and the
#: replacement a literal that holds no backslash, PostgreSQL's search is written here: at each
#: character from the first, the pattern's match there is replaced, and an empty match copies the
#: character after it; a character where nothing matches is copied. At the first character the
#: pattern is read anchored ($a, $ga); past it, behind the one character before ($b, $gb: the
#: pattern after ^(?s:.)), so a ^ or \A matches only at the text's start or after a newline as
#: the pattern says, and a word boundary sees the character before it. $v is the text, $p the
#: pattern, $e the pattern with its word boundaries matching everywhere (are_re2.Re2.loose), $r
#: the replacement's literal.
_CH_EMPTY_MATCH_EVERY = (
    "if(match('', $e), if($v IS NULL, NULL, tupleElement(arrayFold("
    "(_acc, _i, _l) -> if(_i < tupleElement(_acc, 2), _acc, if(_l < 0, "
    "tuple(concat(tupleElement(_acc, 1), substringUTF8(ifNull($v, ''), _i, 1)), toUInt64(_i + 1)), "
    "tuple(concat(tupleElement(_acc, 1), $r, "
    "if(_l = 0, substringUTF8(ifNull($v, ''), _i, 1), '')), toUInt64(_i + greatest(_l, 1))))), "
    "range(1, toUInt64(lengthUTF8(ifNull($v, ''))) + 2), "
    "arrayMap(_i -> if(if(_i = 1, match(ifNull($v, ''), $a), "
    "match(substringUTF8(ifNull($v, ''), _i - 1), $b)), toInt64(lengthUTF8(if(_i = 1, "
    "extract(ifNull($v, ''), $ga), "
    "extract(substringUTF8(ifNull($v, ''), _i - 1), $gb)))), -1), "
    "range(1, toUInt64(lengthUTF8(ifNull($v, ''))) + 2)), "
    "tuple('', toUInt64(1))), 1)), replaceRegexpAll($v, $p, $r))"
)
#: A pattern that matches the empty text where no form above writes PostgreSQL's search: the
#: pattern is no literal, or the replacement is no literal or holds a backslash. It raises 0A000
#: where the pattern can match the empty text ($e: the pattern, its word boundaries matching
#: everywhere), and is replaceRegexpAll otherwise.
_CH_EMPTY_MATCH_RAISES = (
    "if("
    + ch_fail(
        "0A000",
        "regexp_replace() of a pattern that matches the empty text has no form on clickhouse",
    ).format(condition="match('', $e)")
    + " = 0, replaceRegexpAll($v, $p, $r), NULL)"
)


def _ch_every(value: exp.Expr, pattern: exp.Expr, said: exp.Expr, re2: are_re2.Re2 | None):
    """regexp_replace with the g flag on ClickHouse: replaceRegexpAll, or PostgreSQL's search where
    the pattern matches the empty text (_CH_EMPTY_MATCH_EVERY, else _CH_EMPTY_MATCH_RAISES).
    ``re2`` is the literal pattern as RE2 reads it (are_re2), None for a pattern that is no
    literal, which RE2 reads as it is."""
    plain = isinstance(said, exp.Literal) and said.is_string and "\\" not in said.this
    if re2 is None:
        return graft(
            replacement(_CH_EMPTY_MATCH_RAISES, "clickhouse", "f($v, $p, $r, $e)"),
            {"v": value, "p": pattern, "r": said, "e": pattern},
        )
    written = exp.Literal.string(re2.text)
    held = _ch_replacement(said, re2)
    if not plain:
        return graft(
            replacement(_CH_EMPTY_MATCH_RAISES, "clickhouse", "f($v, $p, $r, $e)"),
            {"v": value, "p": written, "r": held, "e": exp.Literal.string(re2.loose)},
        )
    return graft(
        replacement(_CH_EMPTY_MATCH_EVERY, "clickhouse", "f($v, $p, $e, $r, $a, $b, $ga, $gb)"),
        {
            "v": value,
            "p": written,
            "e": exp.Literal.string(re2.loose),
            "r": said,
            "a": exp.Literal.string(f"^(?:{re2.text})"),
            "b": exp.Literal.string(f"^(?s:.)(?:{re2.text})"),
            "ga": exp.Literal.string(f"^((?:{re2.text}))"),
            "gb": exp.Literal.string(f"^(?s:.)((?:{re2.text}))"),
        },
    )


#: The flags of regexp_replace on ClickHouse: g, and the option letters a pattern reads
#: (are_re2), PostgreSQL's 22023 for any other.
_CH_OPTION_LETTERS = frozenset("ceimnpqstwx")


def _ch_replace_flags(said: exp.Expr | None) -> tuple[bool, str]:
    """Whether the flags say every match, and the option letters they say."""
    if said is None:
        return False, ""
    written = said_text(said)
    if not (isinstance(written, exp.Literal) and written.is_string):
        raise Refused(refusal("clickhouse"))
    every, options = False, ""
    for flag in written.this:
        if flag == _EVERY:
            every = True
        elif flag in _CH_OPTION_LETTERS or flag == "b":
            options += flag
        else:
            raise Refused(
                Refusal("22023", f'invalid regular expression option: "{flag}"'), ruling=True
            )
    return every, options


def ch_translated(function: str, pattern: str, options: str) -> are_re2.Re2:
    """``pattern`` as ClickHouse's RE2 reads it as PostgreSQL does (are_re2): PostgreSQL's error
    where it rejects the pattern, a refusal by name where RE2 cannot express it."""
    try:
        return are_re2.translated(pattern, options)
    except are_re2.AreError as error:
        raise Refused(Refusal(error.sqlstate, error.message), ruling=True) from None
    except are_re2.NoForm as missing:
        raise Refused(_ch_refusal(function, f"of {missing.why}"), ruling=True) from None


def _ch_replacement(said: exp.Expr, re2: are_re2.Re2) -> exp.Expr:
    """PostgreSQL's replacement as ClickHouse's replaceRegexpOne and replaceRegexpAll read it:
    \\1 to \\9 the group (nothing for a group the pattern lacks), \\& the whole match (\\0), \\\\ a
    backslash, and a backslash before anything else itself. A replacement that is no literal is
    read as it is."""
    if not (isinstance(said, exp.Literal) and said.is_string):
        return said
    text, out, at = said.this, [], 0
    while at < len(text):
        c = text[at]
        if c != "\\":
            out.append(c)
            at += 1
            continue
        following = text[at + 1 : at + 2]
        if following and following in "123456789":
            out.append(f"\\{following}" if int(following) <= re2.groups else "")
            at += 2
        elif following == "&":
            out.append("\\0")
            at += 2
        elif following == "\\":
            out.append("\\\\")
            at += 2
        else:
            out.append("\\\\")
            at += 1
    return exp.Literal.string("".join(out))


def _ch_bounded(value: exp.Expr, re2: are_re2.Re2, answer: exp.Expr) -> exp.Expr:
    """``answer``, which is PostgreSQL's on text outside the pattern's bound (are_re2.Re2.bound);
    0A000 on text within it."""
    if re2.bound is None:
        return answer
    text, why = re2.bound
    fail = ch_fail("0A000", f"{why} has no form on clickhouse")
    return graft(
        replacement(
            f"if({fail.format(condition='match($v, $g)')} = 0, $a, NULL)",
            "clickhouse",
            "f($v, $g, $a)",
        ),
        {"v": value, "g": exp.Literal.string(text), "a": answer},
    )


def _ch_replace(value: exp.Expr, pattern: exp.Expr, said: exp.Expr, flags: exp.Expr | None):
    """regexp_replace on ClickHouse: the empty pattern written here (_ch_empty_pattern); a
    literal pattern as RE2 reads it as PostgreSQL does (are_re2), its first match by
    replaceRegexpOne or every match (_ch_every); a pattern that is no literal as RE2 reads it."""
    every, options = _ch_replace_flags(flags)
    if _is_empty_text(pattern):
        return _ch_empty_pattern(value, said, every)
    if not (isinstance(pattern, exp.Literal) and pattern.is_string):
        if set(options) - {"c", "i", "t"}:
            raise Refused(refusal("clickhouse"))
        if [flag for flag in options if flag in "ic"][-1:] == ["i"]:
            pattern = _no_case(pattern)
        if every:
            return _ch_every(value, pattern, said, None)
        raise Refused(refusal("clickhouse"))
    re2 = ch_translated("regexp_replace", pattern.this, options)
    if every:
        answer = _ch_every(value, pattern, said, re2)
    elif not (isinstance(said, exp.Literal) and said.is_string):
        raise Refused(refusal("clickhouse"))
    else:
        answer = exp.Anonymous(
            this="replaceRegexpOne",
            expressions=[value, exp.Literal.string(re2.text), _ch_replacement(said, re2)],
        )
    return _ch_bounded(value, re2, answer)


def flagged(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """regexp_replace with PostgreSQL's flags read: the first match or every one, its case."""
    if not isinstance(node, exp.RegexpReplace):
        return None
    said = node.args.get("modifiers")
    if said is not None and _is_null(said):
        return exp.Cast(this=exp.Null(), to=exp.DataType.build("TEXT"))  # strict
    value = node.this
    pattern = said_text(node.expression)
    replacement = said_text(node.args["replacement"])
    if dialect == "clickhouse":
        return _ch_replace(value, pattern, replacement, said)
    every, no_case = _flags(said, dialect)
    if no_case:
        pattern = _no_case(pattern)
    if every:
        return exp.RegexpReplace(this=value, expression=pattern, replacement=replacement)
    return _first_match(value, pattern, replacement, dialect)


#: What a literal pattern on ClickHouse is answered within: text without the characters a locale
#: classes by the database's locale (are_re2.Re2.bound), each refused at run time by name.
_CH_LOCALE_BOUNDS = (
    Limitation(
        "A locale's class (\\w, \\W, [[:alpha:]] and the like) or a word constraint (\\y, \\Y, "
        "\\m, \\M) reads the ASCII characters as PostgreSQL does in every locale; on text "
        "beyond ASCII the call is refused when it runs (0A000), as PostgreSQL's locale decides.",
        Refusal("0A000", f"{are_re2.LOCALE_CLASS} has no form on clickhouse"),
    ),
    Limitation(
        "A space class (\\s, \\S, [[:space:]], [[:blank:]]) on text holding a space beyond ASCII "
        "is refused when it runs (0A000), as PostgreSQL's locale decides.",
        Refusal("0A000", f"{are_re2.LOCALE_SPACE} has no form on clickhouse"),
    ),
)
#: What RE2 cannot express of a literal pattern, refused by name (are_re2.NoForm).
_CH_NO_RE2 = (
    "A literal pattern is read as PostgreSQL reads it (each escape, class, constraint, director "
    "and embedded option, and the option letters of the flags). A back reference, lookahead and "
    "lookbehind, a basic or extended expression (b, e), a named collating element, \\m or \\M "
    "beside what may not be a word character, and \\S in a complemented bracket expression are "
    "refused by name: RE2 has no form of them."
)

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
    if dialect != "clickhouse"
} | {
    "clickhouse": (
        Limitation(
            "A flag that is no constant, and a first match whose pattern or replacement is no "
            "constant, are refused; so is the empty pattern's replacement that is no literal or "
            "holds a backslash. " + _CH_NO_RE2,
            refusal("clickhouse"),
        ),
        Limitation(
            "With the g flag, a pattern that matches the empty text is searched as PostgreSQL "
            "searches it (each empty match replaced, then the next character copied) where "
            "the pattern is a literal and the replacement a literal holding no backslash. "
            "Otherwise such a pattern raises 0A000 when it runs.",
            Refusal(
                "0A000",
                "regexp_replace() of a pattern that matches the empty text has no form on "
                "clickhouse",
            ),
        ),
        *_CH_LOCALE_BOUNDS,
    )
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


def _ch_refusal(function: str, why: str) -> Refusal:
    return Refusal("0A000", f"{function}() {why} has no form on clickhouse")


def _re2_pattern(function: str, pattern: exp.Expr, options: str) -> are_re2.Re2:
    """A literal pattern, read with the flags' option letters, as RE2 reads it as PostgreSQL does
    (ch_translated). A pattern that is no literal is refused by name."""
    written = said_text(pattern)
    if not (isinstance(written, exp.Literal) and written.is_string):
        raise Refused(_ch_refusal(function, "of a pattern that is no literal"))
    return ch_translated(function, written.this, options)


def _ch_flags(function: str, said: exp.Expr | None) -> str | None:
    """The option letters the flags say (are_re2); None for a NULL (a strict call answers NULL).
    The global flag is PostgreSQL's 22023 for these functions, and so is a letter that is no
    option; flags that are no literal are refused by name."""
    if said is None:
        return ""
    if _is_null(said):
        return None
    written = said_text(said)
    if not (isinstance(written, exp.Literal) and written.is_string):
        raise Refused(_ch_refusal(function, "of flags that are no literal"))
    for flag in written.this:
        if flag == _EVERY and function != "regexp_split_to_array":
            raise Refused(
                Refusal(
                    "22023",
                    f'{function}() does not support the "global" option',
                ),
                ruling=True,
            )
        if flag == _EVERY:
            raise Refused(_ch_refusal(function, f"of the flag {flag!r}"))
        if flag not in _CH_OPTION_LETTERS and flag != "b":
            raise Refused(
                Refusal("22023", f'invalid regular expression option: "{flag}"'), ruling=True
            )
    return written.this


def _invalid(parameter: str) -> str:
    return ch_fail("22023", f'invalid value for parameter "{parameter}"')


#: The text from the start (1 where no start is said).
_FROM = "substringUTF8($s, toInt64($b))"
#: The pattern matching the empty text: a count, a position or a split of it is not RE2's.
_EMPTY_MATCH = ch_fail("0A000", "a pattern that matches the empty text has no form on clickhouse")


def _with_checks(checks: list[str], answer: str) -> str:
    return f"if({' + '.join(checks)} = 0, {answer}, NULL)"


def _quoted(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ch_form(function: str, count: int, re2: are_re2.Re2, empty_pattern: bool) -> str:
    """The ClickHouse form of ``function`` of ``count`` arguments, its holes $s (the text), $b (the
    start), $n (the occurrence), $e (the end option) and $x (the subexpression), of the pattern as
    RE2 reads it, ``re2``; ``empty_pattern`` when the statement's pattern is the empty text. Text
    within the pattern's bound (are_re2.Re2.bound) raises 0A000."""
    form = _ch_form_unbounded(function, count, re2, empty_pattern)
    if re2.bound is None:
        return form
    text, why = re2.bound
    beyond = ch_fail("0A000", f"{why} has no form on clickhouse")
    return _with_checks([beyond.format(condition=f"match($s, {_quoted(text)})")], form)


def _ch_form_unbounded(function: str, count: int, re2: are_re2.Re2, empty_pattern: bool) -> str:
    """_ch_form, on any text."""
    pattern = _quoted(re2.text)
    whole = _quoted(f"({re2.text})")
    # A pattern that can match the empty text anywhere: its word boundaries matching everywhere.
    empty = _EMPTY_MATCH.format(condition=f"match('', {_quoted(re2.loose)})")
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
        if empty_pattern:
            # PostgreSQL's empty pattern matches the empty text before each character from the
            # start and at the end: its nth match is '' where there are n such places. extractAll
            # skips an empty match, so the answer is written here.
            found = "if(toInt64($n) <= toInt64(lengthUTF8($s)) - toInt64($b) + 2, '', NULL)"
            return _with_checks(
                [start, occurrence, subexpression], f"CAST({found} AS Nullable(String))"
            )
        # Any other pattern that matches the empty text: extractAll skips its empty matches, so
        # it raises 0A000, as where it counts, places or splits.
        found = f"if(length({matches}) >= toInt64($n), {matches}[toInt64($n)], NULL)"
        return _with_checks(
            [start, occurrence, subexpression, empty], f"CAST({found} AS Nullable(String))"
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
    if node.meta.get(ENGINE_PATTERN):
        return None  # a pattern in the engine's own flavour (transpile._regex_found)
    if isinstance(node, exp.RegexpILike):
        # ~*: regexp_like case-insensitive, the pattern's embedded options read after it.
        if not _is_literal_text(node.expression):
            return None
        call = (frozenset({"regexp_like"}), [node.this, node.expression, exp.Literal.string("i")])
    else:
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
    if function == "regexp_like" and len(args) == 2 and not _is_literal_text(args[1]):
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
    options = _ch_flags(function, said.get("f"))
    assert options is not None  # a NULL flag is a NULL argument
    subexpression = said.get("x")
    if subexpression is not None and _constant_integer(subexpression) != 0:
        raise Refused(_ch_refusal(function, "of a subexpression"))
    re2 = _re2_pattern(function, args[1], options)
    holes = ("s", "b", "n", "e", "x")
    bindings = {"s": args[0]}
    for hole in holes[1:]:
        bindings[hole] = said.get(hole) or exp.Literal.number(_DEFAULTS[hole])
    form = _ch_form(function, len(args), re2, said_text(args[1]).this == "")
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
                        mentions=r"\bregexp_(?:count|instr|substr|like|split_to_array)\b|~",
                    )
                },
            ),
        ),
        limitations={
            _CH: (
                Limitation(
                    "The pattern and the flags are literals (a ~ or regexp_like of a pattern "
                    "that is no literal is ClickHouse's own match). "
                    + _CH_NO_RE2
                    + " A subexpression other than 0 is refused by name; a pattern "
                    "that matches the empty text raises 0A000 where it counts, places, splits or "
                    "extracts (the empty pattern itself extracts the empty text), and a NULL text "
                    "that is no literal raises 0A000 in regexp_split_to_array "
                    "(ClickHouse holds no NULL of an array).",
                ),
                *_CH_LOCALE_BOUNDS,
            )
        },
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 80
FEATURES: tuple[Feature, ...] = (REGEXP_REPLACE, CH_REGEXPS)
