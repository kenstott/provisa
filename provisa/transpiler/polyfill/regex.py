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

import functools
import re

import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler import are_re2, pg_forms_system
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail, ch_only

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


def ch_leftmost(function: str, re2: are_re2.Re2) -> None:
    """Refuses by name a pattern whose match RE2 may choose otherwise than PostgreSQL
    (are_re2.Re2.leftmost), where ``function`` answers the match itself and not only whether there
    is one."""
    if re2.leftmost is not None:
        raise Refused(_ch_refusal(function, f"of {re2.leftmost}"), ruling=True)


def _quoted(text: str) -> str:
    """``text`` as a ClickHouse string literal."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


# --- PostgreSQL's matches where RE2 chooses another at the same start --------------------------

#: The longest text (in characters) PostgreSQL's matches are found in where RE2 chooses another
#: at the same start (are_re2.Re2.leftmost): each match's length is found by testing every length
#: from its start, a regex call each, so a text costs some n^2 calls; beyond the bound the call
#: raises 0A000 by name (REQ-2236).
LONGEST_BOUND = 256
#: The refusal beyond LONGEST_BOUND, of every function it bounds.
LONGEST_BEYOND = Refusal(
    "0A000",
    f"a pattern RE2 matches otherwise than PostgreSQL, on text of more than {LONGEST_BOUND} "
    "characters, has no form on clickhouse",
)


def _let(bindings: list[tuple[str, str]], body: str) -> str:
    """``body`` with each name bound to its value, read once (a lambda's parameters)."""
    names = ", ".join(name for name, _ in bindings)
    values = ", ".join(f"[{value}]" for _, value in bindings)
    return f"arrayElement(arrayMap(({names}) -> {body}, {values}), 1)"


def _ch_pg_matches(re2: are_re2.Re2, limit: str) -> str:
    """The matches PostgreSQL finds in the text _t (of _len characters) from the start $b, at most
    ``limit`` of them: an Array(Tuple(Int64, Int64)) of each match's start (0 the first character)
    and length. Each is found as PostgreSQL finds it: from where the search is, the leftmost start
    where the pattern matches (RE2's: the text before it the shortest a match follows), and there
    the longest text the whole pattern matches (the shortest, where the pattern is non-greedy,
    are_re2.Re2.shortest), tested a length at a time with the characters on either side as its
    context, from RE2's own match's length (which is one); the search goes on from its end, or one
    character past an empty match (src/backend/utils/adt/regexp.c)."""
    p = re2.text
    ahead = _quoted(f"\\A(?s:(.*?))({p})")
    behind = _quoted(f"\\A(?s:.)(?s:(.*?))({p})")
    alone, before = _quoted(f"\\A{p}\\z"), _quoted(f"\\A{p}(?s:.)\\z")
    after, between = _quoted(f"\\A(?s:.){p}\\z"), _quoted(f"\\A(?s:.){p}(?s:.)\\z")
    fits = (
        f"multiIf(_k = 0 AND _k + _l = _len, match(_t, {alone}), "
        f"_k = 0, match(substringUTF8(_t, 1, _l + 1), {before}), "
        f"_k + _l = _len, match(substringUTF8(_t, _k), {after}), "
        f"match(substringUTF8(_t, _k, _l + 2), {between}))"
    )
    lengths = "range(0, _r + 1)" if re2.shortest else "arrayReverse(range(_r, _len - _k + 1))"
    length = f"arrayFirst(_l -> {fits}, arrayMap(_x -> toInt64(_x), {lengths}))"
    found = f"if(_p = 0, match(_t, {ahead}), match(substringUTF8(_t, _p), {behind}))"
    groups = (
        f"if(_p = 0, extractGroups(_t, {ahead}), extractGroups(substringUTF8(_t, _p), {behind}))"
    )
    kept = "tupleElement(_acc, 2)"
    advanced = _let(
        [("_l", length)],
        f"tuple(if(_l = 0, _k + 1, _k + _l), arrayPushBack({kept}, tuple(_k, _l)), toUInt8(0))",
    )
    at = _let(
        [("_g", groups)],
        _let(
            [
                ("_k", "_p + toInt64(lengthUTF8(_g[1]))"),
                ("_r", "toInt64(lengthUTF8(_g[2]))"),
            ],
            advanced,
        ),
    )
    step = (
        f"(_acc, _j) -> if(tupleElement(_acc, 3) = 1 OR tupleElement(_acc, 1) > _len, "
        f"tuple(tupleElement(_acc, 1), {kept}, toUInt8(1)), "
        + _let(
            [("_p", "tupleElement(_acc, 1)")], f"if({found}, {at}, tuple(_p, {kept}, toUInt8(1)))"
        )
        + ")"
    )
    start = "greatest(toInt64($b), 1) - 1"
    steps = f"range(toUInt64(greatest(least(_len + 2, {limit}), 0)))"
    initial = f"tuple({start}, CAST([], 'Array(Tuple(Int64, Int64))'), toUInt8(0))"
    return f"tupleElement(arrayFold({step}, {steps}, {initial}), 2)"


def _ch_with_pg_matches(re2: are_re2.Re2, limit: str, answer: str) -> list[str]:
    """The check and the answer: ``answer`` of the text _t, its length _len and
    PostgreSQL's matches _m (_ch_pg_matches) in the text $s, NULL where $s is; the check raises
    0A000 by name on text longer than LONGEST_BOUND."""
    beyond = ch_fail(LONGEST_BEYOND.sqlstate, LONGEST_BEYOND.message)
    body = _let(
        [("_t", "ifNull($s, '')"), ("_len", "toInt64(lengthUTF8(ifNull($s, '')))")],
        _let([("_m", _ch_pg_matches(re2, limit))], answer),
    )
    check = beyond.format(condition=f"lengthUTF8($s) > {LONGEST_BOUND}")
    return [check, f"if($s IS NULL, NULL, {body})"]


def _ch_replaced_by(said: str, re2: are_re2.Re2) -> str:
    """PostgreSQL's replacement ``said`` of the match at _k of _l characters of _t: \\& the match,
    \\\\ a backslash, a backslash before anything else itself, and \\N nothing for a group the
    pattern lacks. A group the pattern has is refused by name: PostgreSQL chooses each group's part
    of the match by the group's own preference, which RE2 reads otherwise (are_re2.Re2.leftmost)."""
    parts: list[str] = []
    held: list[str] = []
    at = 0
    while at < len(said):
        c, following = said[at], said[at + 1 : at + 2]
        if c != "\\":
            held.append(c)
            at += 1
        elif following and following in "123456789":
            if int(following) <= re2.groups:
                ch_leftmost("regexp_replace", re2)
            at += 2
        elif following == "&":
            parts.extend([_quoted("".join(held)), "substringUTF8(_t, _k + 1, _l)"])
            held = []
            at += 2
        else:
            held.append("\\")
            at += 2 if following == "\\" else 1
    parts.append(_quoted("".join(held)))
    return f"concat({', '.join(parts)})"


def _ch_replace_longest(
    value: exp.Expr,
    re2: are_re2.Re2,
    said: exp.Expr,
    start: exp.Expr,
    n: exp.Expr | None,
) -> exp.Expr:
    """regexp_replace of a pattern RE2 may match otherwise than PostgreSQL: PostgreSQL's matches
    from ``start`` (_ch_pg_matches), the Nth replaced (``n``; 0 every one), or, where ``n`` is
    None, every one. Text before ``start`` and between the matches is kept. A replacement that is
    no literal is refused by name."""
    if not (isinstance(said, exp.Literal) and said.is_string):
        raise Refused(refusal("clickhouse"))
    replaced = _ch_replaced_by(said.this, re2)
    none = "CAST([] AS Array(Tuple(Int64, Int64)))"
    if n is None:
        limit, chosen, checks = "_len + 2", "_m", []
    else:
        limit = "if(toInt64($n) = 0, _len + 2, toInt64($n))"
        chosen = (
            f"if(toInt64($n) = 0, _m, if(length(_m) >= toInt64($n), [_m[toInt64($n)]], {none}))"
        )
        checks = [
            _invalid("start").format(condition="toInt64($b) < 1"),
            _invalid("n").format(condition="toInt64($n) < 0"),
        ]
    ends = "if(_i = 1, 0, tupleElement(_c[_i - 1], 1) + tupleElement(_c[_i - 1], 2))"
    piece = _let(
        [("_k", "tupleElement(_c[_i], 1)"), ("_l", "tupleElement(_c[_i], 2)"), ("_e", ends)],
        f"concat(substringUTF8(_t, _e + 1, _k - _e), {replaced})",
    )
    last = "if(length(_c) = 0, 0, tupleElement(_c[-1], 1) + tupleElement(_c[-1], 2))"
    answer = _let(
        [("_c", chosen)],
        f"concat(arrayStringConcat(arrayMap(_i -> {piece}, arrayEnumerate(_c))), "
        f"substringUTF8(_t, {last} + 1))",
    )
    beyond, answered = _ch_with_pg_matches(re2, limit, answer)
    form = _with_checks([*checks, beyond], f"CAST({answered} AS Nullable(String))")
    answer_node = graft(
        replacement(form, _CH, "f($s, $b, $n)"),
        {"s": value, "b": start, "n": n if n is not None else exp.Literal.number(0)},
    )
    return _ch_bounded(value, re2, answer_node)


#: Where a pattern's character is read as itself: past no backslash, or an even run of them.
_UNESCAPED = r"(?:^|[^\\])(?:\\\\)*"
#: A locale's classes as RE2 names them in a bracket expression.
_CLASS_NAMES = "alnum|alpha|ascii|blank|cntrl|digit|graph|lower|print|punct|space|upper|word|xdigit"
#: What of a pattern that is no literal RE2 reads otherwise than PostgreSQL, or not at all: an
#: escape other than a punctuation character's, \a \d \D \f \n \r \t \v \A and the class
#: escapes \w \W \s \S (\b is a backspace in PostgreSQL's, \y \m \M \Z PostgreSQL's alone, a
#: digit a back reference ...); a (? other than (?: (lookaround, embedded options); a director; a
#: collating element, an equivalence class or a word constraint in a bracket expression; a bound
#: PostgreSQL reads otherwise ({,n}, past 255).
_OTHERWISE = (
    f"{_UNESCAPED}\\\\(?:[^!-/:-@\\[-`{{-~adDfnrtvAwWsS]|$)"
    f"|{_UNESCAPED}\\((?:\\?[^:]|\\*)"
    "|^\\*\\*\\*"
    "|\\[[^\\]]*\\[[.=]|\\[:[<>]:\\]"
    "|\\{,|\\{[0-9]*[0-9]{3}"
)
#: A class of a locale, which reads text beyond ASCII (and RE2's \\s the vertical tab) otherwise.
_LOCALE_CLASSES = f"{_UNESCAPED}\\\\[wWsS]|\\[:(?:{_CLASS_NAMES}):\\]"
_OTHER_TEXT = "[^\\x{0}-\\x{7F}]|\\x{0B}"
#: What of a pattern makes RE2's leftmost-first match differ from PostgreSQL's longest: an
#: alternative, a non-greedy quantifier, a repeated group (are_re2.Re2.leftmost, read the safe way).
_LEFTMOST = f"{_UNESCAPED}(?:\\||[*+?}}]\\?|\\)[*+?{{])"
_RUNTIME = "a pattern RE2 reads otherwise than PostgreSQL"
_RUNTIME_LEFTMOST = "a pattern of alternatives or quantifiers RE2 matches otherwise than PostgreSQL"


#: Whether a pattern that is no literal, $p, holds what RE2 reads otherwise (_OTHERWISE), or a
#: class RE2 names not.
_READ_OTHERWISE = (
    f"match($p, {_quoted(_OTHERWISE)}) OR countMatches($p, {_quoted('[[]:')}) != "
    f"countMatches($p, {_quoted(f'[[]:(?:{_CLASS_NAMES}):[]]')})"
)


def ch_runtime_checks(function: str, *, leftmost: bool) -> list[str]:
    """The run-time checks of a pattern that is no literal, $p, of the text $s: 0A000 by name where
    the pattern holds what RE2 reads otherwise than PostgreSQL (_OTHERWISE, a class name RE2 has
    not), a locale's class on text beyond ASCII, and (``leftmost``, where the function answers the
    match) what RE2 matches otherwise (_LEFTMOST)."""
    otherwise = ch_fail("0A000", f"{function}() of {_RUNTIME} has no form on clickhouse")
    locale = ch_fail("0A000", f"{are_re2.LOCALE_CLASS} has no form on clickhouse")
    checks = [
        otherwise.format(condition=_READ_OTHERWISE),
        locale.format(
            condition=f"match($p, {_quoted(_LOCALE_CLASSES)}) AND match($s, {_quoted(_OTHER_TEXT)})"
        ),
    ]
    if leftmost:
        fail = ch_fail("0A000", f"{function}() of {_RUNTIME_LEFTMOST} has no form on clickhouse")
        checks.append(fail.format(condition=f"match($p, {_quoted(_LEFTMOST)})"))
    return checks


def _ch_checked(value: exp.Expr, pattern: exp.Expr, checks: list[str], answer: exp.Expr):
    """``answer`` where none of ``checks`` of the text ``value`` and the pattern raises."""
    return graft(
        replacement(_with_checks(checks, "$a"), "clickhouse", "f($s, $p, $a)"),
        {"s": value, "p": pattern, "a": answer},
    )


def _safe(pattern: exp.Expr) -> exp.Expr:
    """A pattern that is no literal, or the empty pattern where it holds what RE2 reads otherwise:
    ClickHouse compiles a constant pattern before its check raises (ch_runtime_checks)."""
    return graft(
        replacement(f"if({_READ_OTHERWISE}, '', $p)", "clickhouse", "f($p)"), {"p": pattern}
    )


def _dot_all(pattern: exp.Expr, flags: str = "s") -> exp.Expr:
    """A pattern that is no literal (_safe) with PostgreSQL's default options as RE2 reads them:
    . and a complemented bracket expression match a newline (RE2's s)."""
    return exp.Anonymous(this="concat", expressions=[exp.Literal.string(f"(?{flags})"), pattern])


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
        if not every:
            raise Refused(refusal("clickhouse"))
        checks = ch_runtime_checks("regexp_replace", leftmost=True)
        safe = _safe(pattern)
        read = _no_case(safe) if [f for f in options if f in "ic"][-1:] == ["i"] else safe
        answer = _ch_every(value, _dot_all(read), said, None)
        return _ch_checked(value, pattern, checks, answer)
    re2 = ch_translated("regexp_replace", pattern.this, options)
    if re2.leftmost is not None:
        n = None if every else exp.Literal.number(1)
        return _ch_replace_longest(value, re2, said, exp.Literal.number(1), n)
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


def _ch_longest_bound() -> Limitation:
    """The bound of PostgreSQL's matches found where RE2 chooses another (_ch_pg_matches)."""
    return Limitation(
        "Where RE2 would choose another match than PostgreSQL at the same start (alternatives, "
        "quantifiers of mixed greediness, a group repeated a varying number of times), each "
        "match is PostgreSQL's: the longest at the leftmost start, or the shortest where the "
        "pattern's first quantifier is non-greedy, found by testing each length from that start. "
        f"That costs some n^2 regex calls on a text of n characters: on text of more than "
        f"{LONGEST_BOUND} characters the call raises 0A000 when it runs.",
        LONGEST_BEYOND,
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
        _ch_longest_bound(),
        Limitation(
            "Where RE2 would choose another match than PostgreSQL, a replacement naming one of "
            "the pattern's groups (\\1 to \\9) is refused by name: PostgreSQL chooses each "
            "group's part of the match by the group's own preference.",
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


def _ch_form_longest(function: str, re2: are_re2.Re2) -> str:
    """_ch_form of a pattern RE2 may match otherwise than PostgreSQL (are_re2.Re2.leftmost):
    PostgreSQL's matches found one by one (_ch_pg_matches), on text of LONGEST_BOUND characters at
    most. An empty match is one of them, as PostgreSQL finds it."""
    start = _invalid("start").format(condition="toInt64($b) < 1")
    occurrence = _invalid("n").format(condition="toInt64($n) < 1")
    subexpression = _invalid("subexpr").format(condition="toInt64($x) < 0")
    nth = "_m[toInt64($n)]"
    if function == "regexp_count":
        checks = [start]
        limit, answer = "_len + 2", "CAST(length(_m) AS Nullable(Int32))"
    elif function == "regexp_substr":
        checks = [start, occurrence, subexpression]
        found = f"substringUTF8(_t, tupleElement({nth}, 1) + 1, tupleElement({nth}, 2))"
        limit = "toInt64($n)"
        answer = f"CAST(if(length(_m) >= toInt64($n), {found}, NULL) AS Nullable(String))"
    else:  # regexp_instr
        end = _invalid("endoption").format(condition="toInt64($e) NOT IN (0, 1)")
        checks = [start, occurrence, end, subexpression]
        past = f"if(toInt64($e) = 1, tupleElement({nth}, 2), 0)"
        position = f"tupleElement({nth}, 1) + 1 + {past}"
        limit = "toInt64($n)"
        answer = f"CAST(if(length(_m) >= toInt64($n), {position}, 0) AS Nullable(Int32))"
    beyond, answered = _ch_with_pg_matches(re2, limit, answer)
    return _with_checks([*checks, beyond], answered)


def _ch_form_unbounded(function: str, count: int, re2: are_re2.Re2, empty_pattern: bool) -> str:
    """_ch_form, on any text."""
    if re2.leftmost is not None and function != "regexp_like":
        return _ch_form_longest(function, re2)
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
            return _ch_like_at_run_time(node.this, node.expression, "si")
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
        if _is_null(args[1]):
            return exp.cast(exp.null(), exp.DataType.build("Nullable(Bool)", dialect=dialect))
        return _ch_like_at_run_time(args[0], args[1], "s")
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
    if function == "regexp_split_to_array":
        ch_leftmost(function, re2)
    holes = ("s", "b", "n", "e", "x")
    bindings = {"s": args[0]}
    for hole in holes[1:]:
        bindings[hole] = said.get(hole) or exp.Literal.number(_DEFAULTS[hole])
    form = _ch_form(function, len(args), re2, said_text(args[1]).this == "")
    pattern = "f(" + ", ".join("$" + h for h in holes) + ")"
    return graft(replacement(form, dialect, pattern), bindings)


def _ch_like_at_run_time(value: exp.Expr, pattern: exp.Expr, flags: str) -> exp.Expr:
    """Whether a pattern that is no literal matches, as RE2 reads it with PostgreSQL's options
    ``flags``, where its checks pass (ch_runtime_checks): whether there is a match is the same
    whichever match RE2 chooses."""
    matched = exp.cast(
        exp.Anonymous(this="match", expressions=[value, _dot_all(_safe(pattern), flags)]),
        exp.DataType.build("Nullable(Bool)", dialect="clickhouse"),
    )
    return _ch_checked(value, pattern, ch_runtime_checks("regexp_like", leftmost=False), matched)


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


# --- substring(text FROM pattern), SIMILAR TO, regexp_replace's Nth match on ClickHouse -------------


def _literal_text(value: exp.Expr) -> str | None:
    """The text of a literal (cast to text or not); None for anything else."""
    written = said_text(value)
    if isinstance(written, exp.Literal) and written.is_string:
        return written.this
    return None


def _said_text_type(value: exp.Expr) -> bool:
    """Whether the statement says ``value`` is a text: a literal, or a cast to a text type."""
    node = value.unnest()
    if isinstance(node, exp.Literal):
        return node.is_string
    return isinstance(node, exp.Cast) and node.to.is_type("text", "varchar", "char")


def _null_of(type_name: str) -> exp.Expr:
    return exp.cast(exp.null(), exp.DataType.build(type_name, dialect=_CH))


def _sql_refusal(subject: str, why: str) -> Refusal:
    return Refusal("0A000", f"{subject} {why} has no form on clickhouse")


def _similar_are(subject: str, pattern: str, escape: str) -> str:
    """PostgreSQL's ARE of a SQL regular expression (are_re2.similar_to), its errors raised."""
    try:
        return are_re2.similar_to(pattern, escape)
    except are_re2.AreError as error:
        raise Refused(Refusal(error.sqlstate, error.message), ruling=True) from None
    except are_re2.NoForm as missing:
        raise Refused(_sql_refusal(subject, f"of {missing.why}"), ruling=True) from None


#: substring of a pattern: the first group where the pattern has one, else the match, as
#: ClickHouse's extract answers; NULL where nothing matches. $s the text, $p the pattern.
_SUBSTRING = "if(match($s, $p), extract($s, $p), NULL)"
#: The same where the first group may take no part in the match: PostgreSQL answers NULL there,
#: and ClickHouse's extract '' as of an empty group, so an empty answer raises 0A000.
_SUBSTRING_OPTIONAL = "substring() of a first group that may take no part in the match"


def _ch_substring_of(value: exp.Expr, re2: are_re2.Re2) -> exp.Expr:
    form = _SUBSTRING
    if re2.first_group_optional:
        fail = ch_fail("0A000", f"{_SUBSTRING_OPTIONAL} has no form on clickhouse")
        condition = "match($s, $p) AND extract($s, $p) = ''"
        form = _with_checks([fail.format(condition=condition)], form)
    answer = graft(
        replacement(f"CAST({form} AS Nullable(String))", _CH, "f($s, $p)"),
        {"s": value, "p": exp.Literal.string(re2.text)},
    )
    return _ch_bounded(value, re2, answer)


def _ch_substring_longest(value: exp.Expr, re2: are_re2.Re2) -> exp.Expr:
    """substring of a pattern of no group that RE2 may match otherwise than PostgreSQL: its first
    match as PostgreSQL finds it (_ch_pg_matches)."""
    first = "substringUTF8(_t, tupleElement(_m[1], 1) + 1, tupleElement(_m[1], 2))"
    answer = f"CAST(if(length(_m) >= 1, {first}, NULL) AS Nullable(String))"
    checks = _ch_with_pg_matches(re2, "1", answer)
    form = _with_checks(checks[:1], checks[1])
    answered = graft(replacement(form, _CH, "f($s, $b)"), {"s": value, "b": exp.Literal.number(1)})
    return _ch_bounded(value, re2, answered)


def _ch_substring(node: exp.Expr) -> exp.Expr | None:
    """substring(text FROM pattern) and substring(text FROM pattern FOR escape) on ClickHouse: a
    start or a length the statement does not say is a text is the substr row's."""
    pattern, escape = node.args.get("start"), node.args.get("length")
    if pattern is None or not _said_text_type(pattern):
        return None
    if escape is not None and not _said_text_type(escape):
        return None
    if _is_null(node.this) or _is_null(pattern) or (escape is not None and _is_null(escape)):
        return _null_of("Nullable(String)")
    written = _literal_text(pattern)
    if written is None:
        raise Refused(_ch_refusal("substring", "of a pattern that is no literal"), ruling=True)
    if escape is None:
        re2 = ch_translated("substring", written, "")
        if re2.leftmost is not None and re2.groups == 0:
            return _ch_substring_longest(node.this, re2)
        # A group's part of the match PostgreSQL chooses by each part's own preference.
        ch_leftmost("substring", re2)
        return _ch_substring_of(node.this, re2)
    said = _literal_text(escape)
    if said is None:
        raise Refused(_ch_refusal("substring", "of an escape that is no literal"), ruling=True)
    are = _similar_are("substring()", written, said)
    if said and f"{said}{are_re2.SEPARATOR}" in written:
        # Its three parts, the first matched as little as it can: RE2 reads no such preference.
        raise Refused(_ch_refusal("substring", "of escape-double-quote separators"), ruling=True)
    # Anchored at both ends, of no group: the text where all of it matches.
    re2 = ch_translated("substring", are, "")
    return _ch_substring_of(node.this, re2)


def _ch_similar(node: exp.Expr) -> exp.Expr | None:
    """text SIMILAR TO pattern [ESCAPE escape] on ClickHouse: whether the text matches the ARE
    PostgreSQL writes of the pattern (are_re2.similar_to)."""
    escape: exp.Expr | None = None
    if isinstance(node, exp.Escape) and isinstance(node.this, exp.SimilarTo):
        escape, node = node.expression, node.this
    if not isinstance(node, exp.SimilarTo):
        return None
    value, pattern = node.this, node.expression
    if any(_is_null(v) for v in (value, pattern, *([] if escape is None else [escape]))):
        return _null_of("Nullable(Bool)")
    written = _literal_text(pattern)
    if written is None:
        raise Refused(_sql_refusal("SIMILAR TO", "of a pattern that is no literal"), ruling=True)
    said = "\\" if escape is None else _literal_text(escape)
    if said is None:
        raise Refused(_sql_refusal("SIMILAR TO", "of an escape that is no literal"), ruling=True)
    are = _similar_are("SIMILAR TO", written, said)
    try:
        re2 = are_re2.translated(are)
    except are_re2.AreError as error:
        raise Refused(Refusal(error.sqlstate, error.message), ruling=True) from None
    except are_re2.NoForm as missing:
        raise Refused(_sql_refusal("SIMILAR TO", f"of {missing.why}"), ruling=True) from None
    matched = graft(
        replacement("CAST(match($s, $p) AS Nullable(Bool))", _CH, "f($s, $p)"),
        {"s": value, "p": exp.Literal.string(re2.text)},
    )
    return _ch_bounded(value, re2, matched)


#: The catalog's functions of the match operators: ~ (regexeq), !~, ~* and !~*, of a text, a
#: name and a bpchar (its text).
_MATCH_FUNCTIONS = {
    f"{kind}{suffix}": (suffix.startswith("ic"), suffix.endswith("ne"), kind == "bpchar")
    for kind in ("text", "name", "bpchar")
    for suffix in ("regexeq", "regexne", "icregexeq", "icregexne")
}


def _ch_similar_escape(name: str, args: list[exp.Expr]) -> exp.Expr | None:
    """similar_to_escape(pattern [, escape]) and similar_escape(pattern, escape) of literals: the
    ARE PostgreSQL writes (are_re2.similar_to). similar_escape reads a NULL escape as the default;
    similar_to_escape is strict. A pattern or escape that is no literal is refused by name."""
    if _is_null(args[0]) or (name == "similar_to_escape" and any(_is_null(a) for a in args)):
        return _null_of("Nullable(String)")
    pattern = _literal_text(args[0])
    escape = "\\" if len(args) == 1 or _is_null(args[1]) else _literal_text(args[1])
    if pattern is None or escape is None:
        raise Refused(_ch_refusal(name, "of a pattern or escape that is no literal"), ruling=True)
    return exp.Literal.string(_similar_are(f"{name}()", pattern, escape))


def _ch_sql_patterns(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """substring of a pattern, and SIMILAR TO, on ClickHouse: its substring reads a position, and
    it has no SIMILAR TO. The catalog's functions of them (textregexsubstr, similar_to_escape,
    similar_escape) and of the match operators (textregexeq ...) are read as the call they name."""
    if isinstance(node, exp.Substring):
        return _ch_substring(node)
    if isinstance(node, (exp.SimilarTo, exp.Escape)):
        return _ch_similar(node)
    if not isinstance(node, exp.Anonymous):
        return None
    name, args = node.name.lower(), list(node.expressions)
    if name == "textregexsubstr" and len(args) == 2:  # noqa: PLR2004 -- text, pattern
        return _ch_substring(exp.Substring(this=args[0], start=args[1]))
    if name in ("similar_to_escape", "similar_escape") and len(args) in (1, 2):
        if name == "similar_escape" and len(args) == 1:
            return None
        return _ch_similar_escape(name, args)
    if name in _MATCH_FUNCTIONS and len(args) == 2:  # noqa: PLR2004 -- value, pattern
        no_case, negated, padded = _MATCH_FUNCTIONS[name]
        value = exp.cast(args[0], "text") if padded else args[0]
        kind = exp.RegexpILike if no_case else exp.RegexpLike
        matched = _ch_regexp(kind(this=value, expression=args[1]), dialect)
        if matched is None:
            return None
        return exp.Not(this=exp.Paren(this=matched)) if negated else matched
    return None


@functools.cache
def _ch_nth_form() -> exp.Expr:
    """regexp_replace's Nth match on ClickHouse, its parameters columns of their names."""
    from provisa.transpiler.function_macros import _thrown

    parsed = sqlglot.parse_one(pg_forms_system.CLICKHOUSE_REGEXP_REPLACE_NTH, read=_CH)
    return parsed.transform(_thrown)


def _ch_nth(value, pattern, said, start, n) -> exp.Expr:
    """The Nth-match form with each parameter its argument."""
    from provisa.transpiler.function_macros import _argument

    values = dict(zip(pg_forms_system.REGEXP_PARAMS, (value, pattern, said, start, n), strict=True))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name]).copy()
        return node

    return _ch_nth_form().copy().transform(bound)


def _ch_replace_nth(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """regexp_replace(text, pattern, replacement, start [, n [, flags]]) on ClickHouse: the Nth
    match's form (pg_forms_system), of a literal pattern read as PostgreSQL reads it (are_re2) with
    the flags' option letters; n 1 where it is not said. A constant pattern that is no literal
    (lower('')) is read as RE2 reads it where its run-time checks pass (ch_runtime_checks); one
    that varies is refused by name before (pg_forms_enum, REQ-2167)."""
    if not isinstance(node, exp.RegexpReplace):
        return None
    value, pattern = node.this, said_text(node.expression)
    said, start = said_text(node.args["replacement"]), node.args["position"]
    n = node.args.get("occurrence") or exp.Literal.number(1)
    flags = node.args.get("modifiers")
    if _is_null(pattern) or (flags is not None and _is_null(flags)):
        return _null_of("Nullable(String)")
    _, options = _ch_replace_flags(flags)  # g beside N changes nothing, as in PostgreSQL
    if not (isinstance(pattern, exp.Literal) and pattern.is_string):
        if set(options) - {"c", "i", "t"}:
            raise Refused(
                _ch_refusal("regexp_replace", "of a pattern that is no literal"), ruling=True
            )
        safe = _safe(pattern)
        read = _no_case(safe) if [f for f in options if f in "ic"][-1:] == ["i"] else safe
        checks = ch_runtime_checks("regexp_replace", leftmost=True)
        return _ch_checked(value, pattern, checks, _ch_nth(value, _dot_all(read), said, start, n))
    re2 = ch_translated("regexp_replace", pattern.this, options)
    if re2.leftmost is not None:
        return _ch_replace_longest(value, re2, said, start, n)
    answer = _ch_nth(value, exp.Literal.string(re2.text), said, start, n)
    return _ch_bounded(value, re2, answer)


_NTH_PATTERNS = (
    "regexp_replace($s, $p, $r, $b::integer)",
    "regexp_replace($s, $p, $r, $b::integer, $n::integer)",
    "regexp_replace($s, $p, $r, $b::integer, $n::integer, $f::text)",
)

CH_NTH_REPLACE = Feature(
    "regexp_replace from a start",
    FAMILY,
    "regexp_replace from a start: its first match, or its Nth (0 every match), with flags or "
    "without.",
    read=Side(
        rules=tuple(Rule(p, ch_only(Rewrite(_ch_replace_nth))) for p in _NTH_PATTERNS),
        limitations={
            _CH: (
                Limitation(
                    "A literal pattern is read as PostgreSQL reads it, with the flags' option "
                    "letters (g beside n changes nothing, as in PostgreSQL). A constant pattern "
                    "that is no literal (lower('...')) is read as RE2 reads it, and raises 0A000 "
                    "when it runs where it holds what RE2 reads otherwise or matches otherwise; a "
                    "pattern that varies and flags that are no literal are refused by name. "
                    + _CH_NO_RE2
                    + " A constraint (\\A, \\y, \\Y, \\m, \\M), a replacement holding \\& or "
                    "\\\\, a start below 1 and a negative n raise 0A000 when the call runs: the Nth "
                    "match is replaced apart from the text around it. Where RE2 would choose "
                    "another match than PostgreSQL, the matches are PostgreSQL's (below), and a "
                    "replacement naming one of the pattern's groups is refused by name.",
                    Refusal("0A000", "regexp_replace does not read this value"),
                ),
                _ch_longest_bound(),
                *_CH_LOCALE_BOUNDS,
            )
        },
    ),
)

SQL_PATTERNS = Feature(
    "substring of a pattern, SIMILAR TO",
    FAMILY,
    "substring(text FROM pattern [FOR escape]) and text SIMILAR TO pattern [ESCAPE escape].",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    _CH: Rewrite(
                        _ch_sql_patterns,
                        mentions=r"\bsimilar\b|\bsimilar_(?:to_)?escape\b|\bsubstring\b"
                        r"|\bsubstr\b|\btextregexsubstr\b|\b(?:text|name|bpchar)(?:ic)?regex(?:eq|ne)\b",
                        first=True,
                    )
                },
            ),
        ),
        limitations={
            _CH: (
                Limitation(
                    "substring's pattern is read as PostgreSQL reads it (see the regular "
                    "expressions on ClickHouse); SIMILAR TO's is the expression PostgreSQL writes "
                    "of it (% any text, _ any character), anchored at both ends. A pattern or an "
                    "escape that is no literal is refused by name, and so is substring's of "
                    "escape-double-quote separators, whose first part PostgreSQL matches as little "
                    "as it can. Where substring's first group may take no part in the match and "
                    "the match leaves it empty, the call raises 0A000 when it runs. Where RE2 "
                    "would choose another match than PostgreSQL, a pattern of no group is "
                    "answered with PostgreSQL's match (below), and one of a group is refused by "
                    "name: PostgreSQL chooses the group's part by its own preference.",
                    Refusal("0A000", f"{_SUBSTRING_OPTIONAL} has no form on clickhouse"),
                ),
                _ch_longest_bound(),
                *_CH_LOCALE_BOUNDS,
            )
        },
    ),
)


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
                    "The pattern and the flags are literals, but for ~, ~* and a two-argument "
                    "regexp_like: a pattern of those that is no literal is read as RE2 reads it, "
                    "and the call raises 0A000 when it runs where the pattern holds what RE2 reads "
                    "otherwise or a locale's class meets text beyond ASCII. Any other function of "
                    "a pattern or flags that is no literal is refused by name. "
                    + _CH_NO_RE2
                    + " A subexpression other than 0 is refused by name; a pattern "
                    "that matches the empty text raises 0A000 where it counts, places, splits or "
                    "extracts (the empty pattern itself extracts the empty text), and a NULL text "
                    "that is no literal raises 0A000 in regexp_split_to_array "
                    "(ClickHouse holds no NULL of an array). Where RE2 would choose another match "
                    "than PostgreSQL, regexp_split_to_array is refused by name; regexp_count, "
                    "regexp_instr and regexp_substr find PostgreSQL's matches (below), empty ones "
                    "too.",
                ),
                _ch_longest_bound(),
                *_CH_LOCALE_BOUNDS,
            )
        },
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 80
FEATURES: tuple[Feature, ...] = (REGEXP_REPLACE, CH_REGEXPS, CH_NTH_REPLACE, SQL_PATTERNS)
