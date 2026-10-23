# Copyright (c) 2026 Kenneth Stott
# Canary: 0c4b7e1d-93a2-4f5e-8b61-d2e7a9c3f014
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The universal regex dialect (REQ-2142): one form for every regex a user gives Provisa (a
mask's pattern and replacement), written once and lowered to each engine's own flavour.

The dialect is the subset every engine reads alike:

- literals, ``.`` (any character but a newline), sets ``[...]`` with ranges and negation;
- quantifiers ``*`` ``+`` ``?`` ``{n}`` ``{n,}`` ``{n,m}`` (greedy only, bounds at most 1000);
- anchors ``^`` and ``$``, which anchor to the whole value;
- groups ``( )`` and ``(?: )``, alternation ``|``;
- ``\\d`` ``\\s`` ``\\w`` (and ``\\D`` ``\\S`` ``\\W`` outside a set) as their ASCII classes;
- ``\\n`` ``\\t`` ``\\r`` ``\\f`` ``\\v`` and a backslash before any punctuation, as literals.

A replacement is literal text with ``$1``-``$9`` (a captured group) and ``$$`` (a dollar sign).

``parse`` and ``parse_replacement`` refuse anything else by name (``NotUniversal``): what the
construct is, where it is, and why it is not universal. ``lower`` writes a pattern and replacement
in one engine's form."""

# Requirements: REQ-2142

from __future__ import annotations

from dataclasses import dataclass, field

MAX_BOUND = 1000  # RE2's largest repeat count, the smallest of the engines'
MAX_GROUP_REF = 9

_DIGIT = (("0", "9"),)
_SPACE = ((" ", " "), ("\t", "\t"), ("\n", "\n"), ("\r", "\r"), ("\f", "\f"), ("\v", "\v"))
_WORD = (("A", "Z"), ("a", "z"), ("0", "9"), ("_", "_"))
_CLASS_ESCAPES = {"d": _DIGIT, "s": _SPACE, "w": _WORD}
_CONTROL_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "f": "\f", "v": "\v"}
_QUANTIFIERS = "*+?{"


class NotUniversal(ValueError):
    """A pattern or replacement outside the universal dialect: ``construct`` (a stable code),
    ``position`` (0-based, in the text given) and ``reason`` (why it is not universal)."""

    def __init__(self, construct: str, position: int, what: str, reason: str) -> None:
        self.construct = construct
        self.position = position
        self.what = what
        self.reason = reason
        super().__init__(f"{what} at position {position} — {reason}")


# --------------------------------------------------------------------------- #
# The parsed form
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Char:
    c: str


@dataclass(frozen=True)
class AnyChar:
    pass


@dataclass(frozen=True)
class Start:
    pass


@dataclass(frozen=True)
class End:
    pass


@dataclass(frozen=True)
class CharSet:
    negated: bool
    ranges: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Group:
    body: "Alt"
    capturing: bool


@dataclass(frozen=True)
class Repeat:
    node: "Atom"
    min: int
    max: int | None  # None: unbounded


Atom = Char | AnyChar | Start | End | CharSet | Group | Repeat


@dataclass(frozen=True)
class Alt:
    branches: tuple[tuple[Atom, ...], ...]
    position: int = 0  # where the alternation starts in the pattern


@dataclass(frozen=True)
class Pattern:
    """A parsed universal pattern: its body, and for each capturing group (1-based, in order of
    its opening parenthesis) its number counting every group, non-capturing ones included."""

    body: Alt
    all_groups_number: tuple[int, ...] = field(default=())

    @property
    def groups(self) -> int:
        return len(self.all_groups_number)


@dataclass(frozen=True)
class GroupRef:
    n: int


Replacement = tuple[str | GroupRef, ...]


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


class _Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.i = 0
        self.groups_seen = 0  # every group, capturing or not
        self.capturing: list[int] = []

    def refuse(self, construct: str, what: str, reason: str, at: int | None = None) -> NotUniversal:
        return NotUniversal(construct, self.i if at is None else at, what, reason)

    def peek(self, k: int = 0) -> str | None:
        j = self.i + k
        return self.text[j] if j < len(self.text) else None

    def parse(self) -> Pattern:
        body = self.alternation()
        if self.i < len(self.text):  # only a ")" stops an alternation early
            raise self.refuse(
                "unbalanced_paren",
                "a ) with no ( before it",
                "the group it closes was never opened",
            )
        return Pattern(body, tuple(self.capturing))

    def alternation(self) -> Alt:
        position = self.i
        branches = [self.sequence()]
        while self.peek() == "|":
            self.i += 1
            branches.append(self.sequence())
        alt = Alt(tuple(branches), position)
        _refuse_prefix_branches(alt)
        return alt

    def sequence(self) -> tuple[Atom, ...]:
        items: list[Atom] = []
        while (c := self.peek()) is not None and c not in "|)":
            if c in _QUANTIFIERS:
                if not items or isinstance(items[-1], (Repeat, Start, End)):
                    raise self.refuse(
                        "nothing_to_repeat",
                        f"the quantifier {c}",
                        "it follows nothing that can repeat (the start, a |, an anchor or another quantifier)",
                    )
                items[-1] = self.quantifier(items[-1])
                continue
            items.append(self.atom())
        return tuple(items)

    def quantifier(self, node: Atom) -> Repeat:
        start = self.i
        c = self.text[self.i]
        self.i += 1
        if c == "*":
            low, high = 0, None
        elif c == "+":
            low, high = 1, None
        elif c == "?":
            low, high = 0, 1
        else:
            low, high = self.bound(start)
        nxt = self.peek()
        if nxt == "?":
            raise self.refuse(
                "lazy_quantifier",
                f"the lazy quantifier {self.text[start : self.i + 1]}",
                "PostgreSQL and Snowflake read a lazy quantifier differently, or not at all",
            )
        if nxt == "+":
            raise self.refuse(
                "possessive_quantifier",
                f"the possessive quantifier {self.text[start : self.i + 1]}",
                "only Java-family engines have possessive quantifiers",
            )
        return Repeat(node, low, high)

    def bound(self, start: int) -> tuple[int, int | None]:
        close = self.text.find("}", self.i)
        body = self.text[self.i : close] if close != -1 else ""
        low_s, comma, high_s = body.partition(",")
        if close == -1 or not low_s.isdigit() or (high_s and not high_s.isdigit()):
            if close != -1 and not low_s and comma and high_s.isdigit():
                raise self.refuse(
                    "open_bound",
                    f"the bound {{{body}}}",
                    "a bound with no lower count is read as literal text by some engines; write {0,"
                    + high_s
                    + "}",
                    at=start,
                )
            raise self.refuse(
                "open_bound",
                "a { that starts no bound",
                "a literal { must be escaped as \\{; engines disagree on a bare one",
                at=start,
            )
        low = int(low_s)
        high = None if comma and not high_s else int(high_s) if comma else low
        for n in (low, high):
            if n is not None and n > MAX_BOUND:
                raise self.refuse(
                    "bound_too_large",
                    f"the bound {{{body}}}",
                    f"RE2-based engines repeat at most {MAX_BOUND} times",
                    at=start,
                )
        if high is not None and high < low:
            raise self.refuse(
                "bad_range", f"the bound {{{body}}}", "its upper count is below its lower", at=start
            )
        self.i = close + 1
        return low, high

    def atom(self) -> Atom:
        c = self.text[self.i]
        if c == "(":
            return self.group()
        if c == "[":
            return self.charset()
        if c == "\\":
            return self.escape(in_set=False)
        self.i += 1
        if c == ".":
            return AnyChar()
        if c == "^":
            return Start()
        if c == "$":
            return End()
        return Char(c)

    def group(self) -> Group:
        start = self.i
        self.i += 1
        capturing = True
        if self.peek() == "?":
            kind = self.text[self.i : self.i + 3]
            if kind.startswith("?:"):
                capturing = False
                self.i += 2
            else:
                raise self.special_group(start, kind)
        self.groups_seen += 1
        if capturing:
            self.capturing.append(self.groups_seen)
        body = self.alternation()
        if self.peek() != ")":
            raise self.refuse(
                "unbalanced_paren", "a ( with no )", "the group is never closed", at=start
            )
        self.i += 1
        return Group(body, capturing)

    def special_group(self, start: int, kind: str) -> NotUniversal:
        if kind.startswith(("?=", "?!")):
            return self.refuse(
                "lookahead",
                f"the lookahead ({kind[:2]}",
                "PostgreSQL's ARE has it but RE2 (DuckDB, BigQuery, ClickHouse) and Snowflake have no lookaround",
                at=start,
            )
        if kind.startswith(("?<=", "?<!")):
            return self.refuse(
                "lookbehind",
                f"the lookbehind ({kind}",
                "RE2-based engines, PostgreSQL and Snowflake have no lookbehind",
                at=start,
            )
        if kind.startswith(("?<", "?P", "?'")):
            return self.refuse(
                "named_group",
                "a named group",
                "engines spell named groups differently and some have none; use ( ) and $1-$9",
                at=start,
            )
        if kind.startswith("?>"):
            return self.refuse(
                "possessive_quantifier",
                "the atomic group (?>",
                "only Java-family engines have atomic groups",
                at=start,
            )
        return self.refuse(
            "inline_flags",
            f"the inline flag group ({kind[:2]}",
            "inline flags are read differently by each engine and not at all by Snowflake",
            at=start,
        )

    def escape(self, *, in_set: bool) -> Atom:
        start = self.i
        nxt = self.peek(1)
        if nxt is None:
            raise self.refuse("unknown_escape", "a trailing \\", "it escapes nothing")
        self.i += 2
        if nxt in _CLASS_ESCAPES:
            return CharSet(False, _CLASS_ESCAPES[nxt])
        if nxt in "DSW":
            if in_set:
                raise self.refuse(
                    "negated_class_escape_in_set",
                    f"\\{nxt} inside a set",
                    "a negated class cannot be written inside [ ] the same way in every engine; "
                    "write the negated set instead",
                    at=start,
                )
            return CharSet(True, _CLASS_ESCAPES[nxt.lower()])
        if nxt in _CONTROL_ESCAPES:
            return Char(_CONTROL_ESCAPES[nxt])
        if nxt.isdigit():
            raise self.refuse(
                "backreference",
                f"the backreference \\{nxt}",
                "RE2-based engines (DuckDB, BigQuery, ClickHouse, Trino's re2j) cannot match a backreference",
                at=start,
            )
        if nxt in "bB":
            raise self.refuse(
                "word_boundary",
                f"\\{nxt}",
                "PostgreSQL writes a word boundary \\y and Snowflake has none",
                at=start,
            )
        if nxt in "pPX":
            raise self.refuse(
                "unicode_class",
                f"the Unicode class \\{nxt}",
                "PostgreSQL and Snowflake have no Unicode property classes",
                at=start,
            )
        if nxt.isalnum():
            raise self.refuse(
                "unknown_escape",
                f"the escape \\{nxt}",
                "it means different things to different engines, or nothing",
                at=start,
            )
        return Char(nxt)

    def charset(self) -> CharSet:
        start = self.i
        self.i += 1
        negated = self.peek() == "^"
        if negated:
            self.i += 1
        ranges: list[tuple[str, str]] = []
        first = True
        while True:
            c = self.peek()
            if c is None:
                raise self.refuse(
                    "unbalanced_bracket", "a [ with no ]", "the set is never closed", at=start
                )
            if c == "]":
                if first:
                    raise self.refuse(
                        "empty_class",
                        "an empty set",
                        "a set needs a member; a literal ] in a set must be escaped as \\]",
                        at=start,
                    )
                self.i += 1
                return CharSet(negated, tuple(ranges))
            first = False
            if c == "[":
                if self.peek(1) in (":", ".", "="):
                    raise self.refuse(
                        "posix_class",
                        f"the POSIX class [{self.peek(1)}",
                        "RE2- and Java-family engines read [: :] differently or not at all; use \\d \\s \\w or a range",
                    )
                raise self.refuse(
                    "unbalanced_bracket",
                    "a [ inside a set",
                    "Java-family engines read it as a nested set; escape it as \\[",
                )
            low = self.set_member()
            if isinstance(low, tuple):
                ranges.extend(low)
                continue
            if self.peek() == "-" and self.peek(1) not in ("]", None):
                at = self.i
                self.i += 1
                high = self.set_member()
                if isinstance(high, tuple):
                    raise self.refuse(
                        "bad_range",
                        "a range ending in a class",
                        "a range runs between two characters",
                        at=at,
                    )
                if high < low:
                    raise self.refuse(
                        "bad_range",
                        f"the range {low}-{high}",
                        "its end comes before its start",
                        at=at,
                    )
                ranges.append((low, high))
            else:
                ranges.append((low, low))

    def set_member(self) -> str | tuple[tuple[str, str], ...]:
        c = self.text[self.i]
        if c == "\\":
            esc = self.escape(in_set=True)
            if isinstance(esc, CharSet):
                return esc.ranges
            assert isinstance(esc, Char)
            return esc.c
        self.i += 1
        return c


# --------------------------------------------------------------------------- #
# Alternation: leftmost-first against leftmost-longest
# --------------------------------------------------------------------------- #
#
# PostgreSQL and Snowflake choose among an alternation's branches by the longest match (POSIX);
# the other engines take the first branch that matches. The two agree unless a branch that comes
# first can match a strict prefix of what a later branch matches. Where that is certain -- an
# earlier branch that is all literal text and is a strict prefix of a later branch's literal
# prefix, as in (a|ab) -- the pattern is refused (alternation_prefix). Where it cannot be decided
# statically, it is not guessed at: the pattern is accepted, and ``warnings`` reports any
# alternation whose branches can begin with the same character (alternation_overlap).


def _literal_prefix(branch: tuple[Atom, ...]) -> tuple[str, bool]:
    """The branch's leading literal text, and whether the branch is that text and nothing else."""
    text = []
    for atom in branch:
        if not isinstance(atom, Char):
            return "".join(text), False
        text.append(atom.c)
    return "".join(text), True


def _refuse_prefix_branches(alt: Alt) -> None:
    prefixes = [_literal_prefix(b) for b in alt.branches]
    for i, (earlier, whole) in enumerate(prefixes):
        if not whole:
            continue
        for later, _ in prefixes[i + 1 :]:
            if later != earlier and later.startswith(earlier):
                raise NotUniversal(
                    "alternation_prefix",
                    alt.position,
                    f"the alternation branch {earlier!r} before {later!r}",
                    "PostgreSQL and Snowflake take the longest branch and the other engines the "
                    "first, so they would match differently; put the longer branch first, or "
                    "anchor the branches",
                )


_ANY = (("\0", "\U0010ffff"),)


def _first_chars(seq: tuple[Atom, ...]) -> tuple[list[tuple[str, str]], bool]:
    """The characters a match of ``seq`` can begin with (as ranges; a negated set or . counted as
    any character), and whether ``seq`` can match the empty string."""
    out: list[tuple[str, str]] = []
    for atom in seq:
        node, optional = (atom.node, atom.min == 0) if isinstance(atom, Repeat) else (atom, False)
        if isinstance(node, (Start, End)):
            continue
        if isinstance(node, Char):
            first, empty = [(node.c, node.c)], False
        elif isinstance(node, CharSet):
            first, empty = (list(_ANY) if node.negated else list(node.ranges)), False
        elif isinstance(node, AnyChar):
            first, empty = list(_ANY), False
        else:
            assert isinstance(node, Group)
            first, empty = [], False
            for branch in node.body.branches:
                f, e = _first_chars(branch)
                first += f
                empty = empty or e
        out += first
        if not (optional or empty):
            return out, False
    return out, True


def _overlap(a: list[tuple[str, str]], b: list[tuple[str, str]]) -> bool:
    return any(lo1 <= hi2 and lo2 <= hi1 for lo1, hi1 in a for lo2, hi2 in b)


def _alternations(alt: Alt) -> list[Alt]:
    found = [alt] if len(alt.branches) > 1 else []
    for branch in alt.branches:
        for atom in branch:
            node = atom.node if isinstance(atom, Repeat) else atom
            if isinstance(node, Group):
                found += _alternations(node.body)
    return found


def warnings(pattern: Pattern) -> list[NotUniversal]:
    """Each alternation of ``pattern`` whose branches can begin with the same character (or match
    nothing): engines may choose between them differently."""
    out = []
    for alt in _alternations(pattern.body):
        firsts = [_first_chars(b) for b in alt.branches]
        if any(
            ea or eb or _overlap(fa, fb)
            for i, (fa, ea) in enumerate(firsts)
            for fb, eb in firsts[i + 1 :]
        ):
            out.append(
                NotUniversal(
                    "alternation_overlap",
                    alt.position,
                    "an alternation whose branches can begin alike",
                    "engines that take the longest branch (PostgreSQL, Snowflake) and those that "
                    "take the first may match differently; check the masked output on each engine, "
                    "or make the branches begin differently",
                )
            )
    return out


def parse(pattern: str) -> Pattern:
    """The universal pattern ``pattern``, or ``NotUniversal`` naming what is not."""
    return _Parser(pattern).parse()


def parse_replacement(replace: str, pattern: Pattern) -> Replacement:
    """The universal replacement ``replace`` for ``pattern``, or ``NotUniversal``."""
    out: list[str | GroupRef] = []
    text: list[str] = []
    i = 0
    while i < len(replace):
        c = replace[i]
        if c != "$":
            text.append(c)
            i += 1
            continue
        nxt = replace[i + 1] if i + 1 < len(replace) else None
        if nxt == "$":
            text.append("$")
            i += 2
            continue
        if nxt == "{":
            raise NotUniversal(
                "replace_named_ref",
                i,
                "a named reference ${",
                "named groups are not universal; refer to a group as $1-$9",
            )
        if nxt is None or not nxt.isdigit() or nxt == "0":
            raise NotUniversal(
                "replace_bad_escape",
                i,
                "a $ that is not $1-$9 or $$",
                "write a literal dollar sign as $$",
            )
        n = int(nxt)
        if n > pattern.groups:
            raise NotUniversal(
                "replace_group_out_of_range",
                i,
                f"${n}",
                f"the pattern captures {pattern.groups} group(s)",
            )
        if pattern.all_groups_number[n - 1] > MAX_GROUP_REF:
            raise NotUniversal(
                "replace_group_out_of_range",
                i,
                f"${n}",
                "counting (?: ) groups it is past the ninth group, and Snowflake numbers every group",
            )
        if text:
            out.append("".join(text))
            text = []
        out.append(GroupRef(n))
        i += 2
    if text:
        out.append("".join(text))
    return tuple(out)


def check(pattern: str, replace: str) -> tuple[Pattern, Replacement]:
    """Both parsed, or the first ``NotUniversal`` (``field`` set to "pattern" or "replace")."""
    try:
        parsed = parse(pattern)
    except NotUniversal as exc:
        exc.field = "pattern"  # type: ignore[attr-defined]
        raise
    try:
        return parsed, parse_replacement(replace, parsed)
    except NotUniversal as exc:
        exc.field = "replace"  # type: ignore[attr-defined]
        raise


# --------------------------------------------------------------------------- #
# Lowering
# --------------------------------------------------------------------------- #

# Each engine's regex flavour (sqlglot dialect name -> flavour).
#   java:   Trino (Joni with Java syntax, or re2j), Databricks and Spark (java.util.regex), MySQL (ICU)
#   pcre:   Exasol
#   re2:    DuckDB, BigQuery, ClickHouse
#   are:    PostgreSQL's advanced regular expressions
#   posix:  POSIX extended syntax with Perl class escapes: Snowflake, Oracle
#   python: Python's re, for rows masked outside a query (change events)
FLAVOURS = {
    "trino": "java",
    "databricks": "java",
    "spark": "java",
    "mysql": "java",
    "exasol": "pcre",
    "duckdb": "re2",
    "bigquery": "re2",
    "clickhouse": "re2",
    "postgres": "are",
    "snowflake": "posix",
    "oracle": "posix",
    "python": "python",
}

_ESCAPED_OUTSIDE = set(".[\\()*+?{|^$")
_ESCAPED_IN_SET = set("]\\^-[&~|")


def _flavour(dialect: str) -> str:
    if dialect not in FLAVOURS:
        raise ValueError(
            f"a regex mask has no lowering for the {dialect} engine; it is lowered for "
            + ", ".join(sorted(d for d in FLAVOURS if d != "python"))
        )
    return FLAVOURS[dialect]


def _set_char(c: str, flavour: str) -> str:
    if flavour == "posix":
        return "\\\\" if c == "\\" else c  # placed by _posix_set; a backslash is doubled
    return "\\" + c if c in _ESCAPED_IN_SET else c


def _render_set(node: CharSet, flavour: str) -> str:
    if flavour == "posix":
        return _posix_set(node)
    body = "".join(
        _set_char(lo, flavour) if lo == hi else f"{_set_char(lo, flavour)}-{_set_char(hi, flavour)}"
        for lo, hi in node.ranges
    )
    return f"[{'^' if node.negated else ''}{body}]"


def _posix_set(node: CharSet) -> str:
    """A set in POSIX bracket form, where a backslash may be literal: ] first, [ and - last, ^ not
    first, so no member needs escaping."""
    special = {"]", "[", "-", "^"}
    singles = {lo for lo, hi in node.ranges if lo == hi}
    ranges = [(lo, hi) for lo, hi in node.ranges if lo != hi]
    for lo, hi in ranges:
        if lo in special | {"\\"} or hi in special | {"\\"}:
            raise ValueError(
                f"a regex mask's range {lo}-{hi} cannot be written for a POSIX engine (Snowflake, Oracle); list its "
                "characters instead"
            )
    body = "]" if "]" in singles else ""
    body += "".join(f"{lo}-{hi}" for lo, hi in ranges)
    body += "".join(_set_char(c, "posix") for c in sorted(singles - special))
    body += "^" if "^" in singles else ""
    body += "[" if "[" in singles else ""
    body += "-" if "-" in singles else ""
    if body == "^":  # a set of ^ alone cannot put it anywhere but first
        body = "\\^"
    return f"[{'^' if node.negated else ''}{body}]"


def _render_atom(node: Atom, flavour: str) -> str:
    if isinstance(node, Char):
        return "\\" + node.c if node.c in _ESCAPED_OUTSIDE else node.c
    if isinstance(node, AnyChar):
        # A newline is outside . in every engine but PostgreSQL's, whose . takes it.
        return "[^\n]" if flavour == "are" else "."
    if isinstance(node, Start):
        # Java-family ^ can mean a line's start (Joni's Ruby heritage); \A is the value's.
        return "\\A" if flavour in ("java", "pcre", "python") else "^"
    if isinstance(node, End):
        # $ also matches before a final newline in Java and Python; \z and \Z are the value's end.
        return {"java": "\\z", "pcre": "\\z", "python": "\\Z"}.get(flavour, "$")
    if isinstance(node, CharSet):
        return _render_set(node, flavour)
    if isinstance(node, Group):
        inner = _render_alt(node.body, flavour)
        open_ = "(" if node.capturing or flavour == "posix" else "(?:"
        return f"{open_}{inner})"
    assert isinstance(node, Repeat)
    inner = _render_atom(node.node, flavour)
    if (node.min, node.max) == (0, None):
        q = "*"
    elif (node.min, node.max) == (1, None):
        q = "+"
    elif (node.min, node.max) == (0, 1):
        q = "?"
    elif node.max is None:
        q = f"{{{node.min},}}"
    elif node.max == node.min:
        q = f"{{{node.min}}}"
    else:
        q = f"{{{node.min},{node.max}}}"
    return inner + q


def _render_alt(node: Alt, flavour: str) -> str:
    return "|".join("".join(_render_atom(a, flavour) for a in branch) for branch in node.branches)


def _render_replacement(parsed: Pattern, replacement: Replacement, flavour: str) -> str:
    out: list[str] = []
    for part in replacement:
        if isinstance(part, GroupRef):
            # A POSIX engine turns (?: ) into ( ), so it numbers a group counting those too.
            n = parsed.all_groups_number[part.n - 1] if flavour == "posix" else part.n
            if flavour == "java":
                out.append(f"${n}")
            elif flavour == "python":
                out.append(f"\\g<{n}>")
            else:
                out.append(f"\\{n}")
            continue
        for i, c in enumerate(part):
            if c == "\\":
                out.append("\\\\")
            elif c == "$" and flavour == "java":
                out.append("\\$")
            elif c.isdigit() and i == 0 and flavour == "java" and out and out[-1].startswith("$"):
                out.append("\\" + c)  # $1 then a digit: Java would read $12
            else:
                out.append(c)
    return "".join(out)


def lower(pattern: str, replace: str, dialect: str) -> tuple[str, str]:
    """``pattern`` and ``replace`` (universal) written for ``dialect``'s regex engine: a sqlglot
    dialect name, or ``python`` for Python's re."""
    flavour = _flavour(dialect)
    parsed, replacement = check(pattern, replace)
    return _render_alt(parsed.body, flavour), _render_replacement(parsed, replacement, flavour)
