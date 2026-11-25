# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c8e42-7a9b-4f36-9e0d-2b6a41c7f835
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's advanced regular expressions (AREs) as RE2 reads them (REQ-2236).

ClickHouse matches by RE2, which reads several of an ARE's escapes otherwise or not at all: \\b is a
backspace in PostgreSQL's and a word boundary in RE2's, \\B a backslash and not a boundary, \\y \\Y
\\m \\M and \\Z are PostgreSQL's alone, \\s holds the vertical tab in PostgreSQL's and not in RE2's,
and a back reference RE2 has none of. A pattern is read here as PostgreSQL's lexer reads it
(src/backend/regex/regc_lex.c) and written as the RE2 pattern that matches what it matches:

- each character-entry escape (\\a \\b \\B \\cX \\e \\f \\n \\r \\t \\v \\uXXXX \\UXXXXXXXX \\xH...
  \\0 and octal) as the character, \\x{...};
- \\d \\s \\w and their complements as the ASCII class, within a bracket expression too;
- \\A as \\A, \\Z as \\z, \\y as \\b and \\Y as \\B; \\m and \\M (and [[:<:]], [[:>:]]) as \\b where
  the character after (\\m) or before (\\M) is a word character, which RE2 needs no lookahead for;
- a bracket expression's classes ([:alpha:] ...), collating elements and equivalence classes of
  one character;
- the director prefixes ***= and ***:, the embedded options (?...) at the start, and the same
  option letters given as a function's flags: i and c the case, n m p s w how a newline is read,
  x the expanded syntax, q a literal, t the tight syntax.

A class of a locale (\\w \\s [:alpha:] ...) and a word boundary read the ASCII characters the same in
every locale, and those beyond ASCII by the database's locale, which RE2 cannot: the answer is
PostgreSQL's on text without them, and each form raises 0A000 on text that holds one (Re2.bound).
What RE2 cannot express at all is refused by name (NoForm): a back reference, lookahead and
lookbehind, a basic or extended expression (the b and e options), a named collating element, \\m
or \\M beside what may not be a word character, and a complemented \\S in a complemented bracket.
A pattern PostgreSQL itself rejects raises PostgreSQL's error (AreError, 2201B)."""

# Requirements: REQ-2236

from __future__ import annotations

from dataclasses import dataclass

#: PostgreSQL's SQLSTATE of an invalid regular expression.
INVALID_REGULAR_EXPRESSION = "2201B"

#: The class of PostgreSQL's [[:space:]] on ASCII: RE2's \\s lacks the vertical tab.
_SPACE = "\\t\\n\\x{0B}\\f\\r "
#: The class of PostgreSQL's [[:word:]] on ASCII, which RE2's \\w is.
_WORD = "0-9A-Za-z_"
_DIGIT = "0-9"

#: Text beyond ASCII: a locale's class reads it by the database's locale.
_BEYOND_ASCII = "[^\\x{0}-\\x{7F}]"
#: The spaces beyond ASCII a locale may class as space (Unicode's Zs, Zl and Zp, and NEL).
_SPACE_BEYOND_ASCII = (
    "[\\x{85}\\x{A0}\\x{1680}\\x{2000}-\\x{200A}\\x{2028}\\x{2029}\\x{202F}\\x{205F}\\x{3000}]"
)
LOCALE_CLASS = "a locale's class or word boundary on text beyond ASCII"
LOCALE_SPACE = "a locale's space class on text holding a space beyond ASCII"

#: A bracket expression's classes: RE2's of the same name reads the ASCII characters as
#: PostgreSQL does; the bound is what text PostgreSQL's locale reads otherwise.
_CLASSES = {
    "alnum": ("[:alnum:]", "ascii"),
    "alpha": ("[:alpha:]", "ascii"),
    "ascii": ("[:ascii:]", None),
    "blank": ("\\t ", "space"),
    "cntrl": ("[:cntrl:]", "ascii"),
    "digit": ("[:digit:]", None),
    "graph": ("[:graph:]", "ascii"),
    "lower": ("[:lower:]", "ascii"),
    "print": ("[:print:]", "ascii"),
    "punct": ("[:punct:]", "ascii"),
    "space": (_SPACE, "space"),
    "upper": ("[:upper:]", "ascii"),
    "word": (_WORD, "ascii"),
    "xdigit": ("[:xdigit:]", None),
}

#: The whitespace the expanded syntax (x) ignores outside a bracket expression.
_EXPANDED_SPACE = frozenset(" \t\n\r\f\v\x85\xa0     　") | frozenset(
    chr(c) for c in range(0x2000, 0x200B)
)

_OPTION_LETTERS = frozenset("bceimnpqstwx")


class AreError(Exception):
    """PostgreSQL's own error of a pattern it rejects (2201B, its message)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.sqlstate = INVALID_REGULAR_EXPRESSION
        self.message = f"invalid regular expression: {message}"


class NoForm(Exception):  # noqa: N818 -- what has no form, as the refusal names it
    """What of a pattern RE2 cannot express: ``why`` completes "<function>() of ...", the
    refusal's text."""

    def __init__(self, why: str) -> None:
        super().__init__(why)
        self.why = why


@dataclass(frozen=True)
class Re2:
    """A pattern as RE2 reads it as PostgreSQL does."""

    #: The RE2 pattern, its flags in a group of its own: it can be placed in another pattern.
    text: str
    #: The capturing groups, numbered as PostgreSQL numbers them.
    groups: int
    #: An RE2 pattern of the text the answer is not PostgreSQL's on, and what that text is; None
    #: where the answer is PostgreSQL's on any text.
    bound: tuple[str, str] | None
    #: Whether the pattern holds a word boundary (RE2's \\b or \\B), which RE2 reads in the text
    #: before a match.
    word_boundary: bool
    #: ``text`` with its word boundaries matching everywhere: it matches the empty text where the
    #: pattern matches the empty text anywhere in a text.
    loose: str
    #: Whether ^ and $ match at a newline (the n, m and w options).
    multiline: bool


@dataclass
class _Options:
    no_case: bool = False
    #: How a newline is read: s (an ordinary character), n, p or w.
    newline: str = "s"
    expanded: bool = False
    literal: bool = False
    #: The syntax: a for advanced, b basic, e extended.
    syntax: str = "a"

    def read(self, letters: str, *, embedded: bool) -> None:
        for letter in letters:
            if letter not in _OPTION_LETTERS:
                if embedded:
                    raise AreError("invalid embedded option")
                raise ValueError(letter)
            if letter in "ic":
                self.no_case = letter == "i"
            elif letter in "mn":
                self.newline = "n"
            elif letter in "psw":
                self.newline = letter
            elif letter == "x":
                self.expanded = True
            elif letter == "t":
                self.expanded = False
            elif letter == "q":
                self.literal = True
            else:  # b or e
                self.syntax = letter


def _char(code: int) -> str:
    """The character of ``code`` as RE2 reads it literally, in or out of a bracket expression."""
    if code > 0x10FFFF or 0xD800 <= code <= 0xDFFF:
        # PostgreSQL: no error, but no text holds it (a UTF-8 database's character-entry escape).
        raise _Nowhere
    return f"\\x{{{code:X}}}"


class _Nowhere(Exception):  # noqa: N818 -- a character no text holds
    pass


#: What RE2 matches nowhere: a class of no character.
NOWHERE = "[^\\x{0}-\\x{10FFFF}]"


def _literal(c: str) -> str:
    """``c`` as RE2 reads it literally."""
    return c if c.isascii() and (c.isalnum() or c == "_") else _char(ord(c))


def _is_word(c: str) -> bool:
    return c.isascii() and (c.isalnum() or c == "_")


#: A word boundary as the reader writes it, RE2's \\b and \\B in Re2.text, nothing in Re2.loose: a
#: literal character is never written raw but as \\x{...}, so no pattern writes these.
_BOUNDARY, _NOT_BOUNDARY = "\x00y", "\x00Y"


class _Reader:
    """One pattern read left to right, written as RE2's."""

    def __init__(self, text: str, options: _Options) -> None:
        self.text = text
        self.at = 0
        self.options = options
        self.out: list[str] = []
        self.opened = 0
        self.closed = 0
        self.open_groups: list[bool] = []  # whether each open group captures
        self.bound: set[str] = set()
        self.word_boundary = False
        #: Whether what was written last is one word character, matched at least once (\\M).
        self.word_before = False

    # -- the whole pattern --------------------------------------------------------------------

    def read(self) -> None:
        text = self.text
        while self.at < len(text):
            c = text[self.at]
            if self.options.expanded and c in _EXPANDED_SPACE:
                self.at += 1
                continue
            if self.options.expanded and c == "#":
                end = text.find("\n", self.at)
                self.at = len(text) if end < 0 else end + 1
                continue
            if c == "\\":
                self._escape()
            elif c == "[":
                self._bracket()
            elif c == "(":
                self._open()
            elif c == ")":
                self._close()
            elif c == ".":
                self._atom("[^\\n]" if self.options.newline in "np" else ".", word=False)
            elif c in "*?":
                self.out.append(c)
                self.at += 1
                self.word_before = False
            elif c == "+":
                self.out.append(c)
                self.at += 1
            elif c == "{":
                self._brace()
            elif c in "|^$":
                self.out.append(c)
                self.at += 1
                self.word_before = False
            else:
                self._atom(_literal(c), word=_is_word(c))

    def _atom(self, written: str, *, word: bool, width: int = 1) -> None:
        self.out.append(written)
        self.at += width
        self.word_before = word

    def _brace(self) -> None:
        """{ opens a bound where a digit follows it, and is an ordinary character otherwise."""
        text = self.text
        following = text[self.at + 1 : self.at + 2]
        if not following.isdigit():
            self._atom("\\{", word=False)
            return
        end = text.find("}", self.at)
        if end < 0:
            raise AreError("braces {} not balanced")
        bound = text[self.at : end + 1]
        self.out.append(bound)
        self.at = end + 1
        if bound[1:].startswith("0"):
            self.word_before = False

    def _open(self) -> None:
        text = self.text
        if text.startswith("(?", self.at):
            kind = text[self.at + 2 : self.at + 3]
            if kind == ":":
                self.out.append("(?:")
                self.open_groups.append(False)
                self.at += 3
            elif kind in ("=", "!") or text.startswith(("(?<=", "(?<!"), self.at):
                raise NoForm("a lookahead or lookbehind constraint")
            elif kind == "#":
                end = text.find(")", self.at)
                if end < 0:
                    raise AreError("parentheses () not balanced")
                self.at = end + 1
                return
            else:
                raise AreError("quantifier operand invalid")
        else:
            self.out.append("(")
            self.opened += 1
            self.open_groups.append(True)
            self.at += 1
        self.word_before = False

    def _close(self) -> None:
        if not self.open_groups:
            raise AreError("parentheses () not balanced")
        if self.open_groups.pop():
            self.closed += 1
        self.out.append(")")
        self.at += 1
        self.word_before = False

    # -- escapes --------------------------------------------------------------------------------

    def _escape(self) -> None:
        text = self.text
        if self.at + 1 >= len(text):
            raise AreError("invalid escape \\ sequence")
        e = text[self.at + 1]
        entered = self._entry(e, in_bracket=False)
        if entered is not None:
            written, width = entered
            self._atom(written, word=False, width=width)
            return
        if e in "dDsSwW":
            self._atom(self._shorthand(e), word=e in "dw", width=2)
        elif e == "A":
            self._atom("\\A", word=False, width=2)
        elif e == "Z":
            self._atom("\\z", word=False, width=2)
        elif e in "yY":
            self.bound.add("ascii")
            self.word_boundary = True
            self._atom(_BOUNDARY if e == "y" else _NOT_BOUNDARY, word=False, width=2)
        elif e == "m":
            self._start_of_word(2)
        elif e == "M":
            self._end_of_word(2)
        elif e.isdigit():
            self._digits()
        elif e.isalnum():
            raise AreError("invalid escape \\ sequence")
        else:
            self._atom(_literal(e), word=False, width=2)

    def _entry(self, e: str, *, in_bracket: bool) -> tuple[str, int] | None:
        """A character-entry escape at ``self.at`` (its letter ``e``): what RE2 reads as the
        character, and the escape's width; None for an escape of another kind."""
        simple = {"a": 0x07, "b": 0x08, "B": 0x5C, "e": 0x1B, "f": 0x0C, "n": 0x0A, "r": 0x0D}
        simple.update({"t": 0x09, "v": 0x0B})
        try:
            if e in simple:
                return _char(simple[e]), 2
            if e == "c":
                if self.at + 2 >= len(self.text):
                    raise AreError("invalid escape \\ sequence")
                return _char(ord(self.text[self.at + 2]) & 0x1F), 3
            if e in "uU":
                count = 4 if e == "u" else 8
                digits = self.text[self.at + 2 : self.at + 2 + count]
                if len(digits) != count or not _all_in(digits, "0123456789abcdefABCDEF"):
                    raise AreError("invalid escape \\ sequence")
                return _char(int(digits, 16)), 2 + count
            if e == "x":
                end = self.at + 2
                while end < len(self.text) and self.text[end] in "0123456789abcdefABCDEF":
                    end += 1
                if end == self.at + 2:
                    raise AreError("invalid escape \\ sequence")
                return _char(int(self.text[self.at + 2 : end], 16)), end - self.at
            if e == "0":
                return self._octal()
        except _Nowhere:
            if in_bracket:
                raise NoForm("a character no text holds in a bracket expression") from None
            return NOWHERE, self._width_of_nowhere(e)
        return None

    def _width_of_nowhere(self, e: str) -> int:
        end = self.at + 2
        if e in "uU":
            return 2 + (4 if e == "u" else 8)
        while end < len(self.text) and self.text[end] in "0123456789abcdefABCDEF":
            end += 1
        return end - self.at

    def _octal(self) -> tuple[str, int]:
        """Up to three octal digits from the escape's first digit."""
        start = self.at + 1
        end = start
        while end < len(self.text) and end - start < 3 and self.text[end] in "01234567":
            end += 1
        if end == start:
            raise AreError("invalid escape \\ sequence")
        return _char(int(self.text[start:end], 8)), end - self.at

    def _digits(self) -> None:
        """\\N: a back reference where PostgreSQL reads one (one nonzero digit, or a number of a
        group closed before it), an octal character otherwise."""
        start = self.at + 1
        end = start
        while end < len(self.text) and self.text[end].isdigit():
            end += 1
        number = int(self.text[start:end])
        if self.text[start] != "0" and (end - start == 1 or 0 < number <= self.closed):
            if number > self.closed:
                raise AreError("invalid backreference number")
            raise NoForm("a back reference")
        written, width = self._octal()
        self._atom(written, word=False, width=width)

    def _shorthand(self, e: str) -> str:
        if e in "dD":
            return f"[{'^' if e == 'D' else ''}{_DIGIT}]"
        if e in "sS":
            self.bound.add("space")
            return f"[{'^' if e == 'S' else ''}{_SPACE}]"
        self.bound.add("ascii")
        return f"[{'^' if e == 'W' else ''}{_WORD}]"

    # -- word constraints -----------------------------------------------------------------------

    def _start_of_word(self, width: int) -> None:
        """\\m: RE2's \\b where a word character follows, matched at least once."""
        start = self.at + width
        following = self.text[start:]
        if following.startswith(("\\w", "\\d")):
            quantified = following[2:]
        elif following[:1] and _is_word(following[0]):
            quantified = following[1:]
        elif following.startswith("[") and not following.startswith(("[[:<:]]", "[[:>:]]")):
            held = set(self.bound)
            _, end, word = self._bracket_at(start)
            self.bound = held
            if not word:
                raise NoForm("\\m beside what may not be a word character")
            quantified = self.text[end:]
        else:
            raise NoForm("\\m beside what may not be a word character")
        after = quantified[:1]
        if after in ("*", "?") or quantified.startswith(("{0", "{,")):
            raise NoForm("\\m beside what may not be a word character")
        self.bound.add("ascii")
        self.word_boundary = True
        self._atom(_BOUNDARY, word=False, width=width)

    def _end_of_word(self, width: int) -> None:
        """\\M: RE2's \\b where a word character precedes, matched at least once."""
        if not self.word_before:
            raise NoForm("\\M beside what may not be a word character")
        self.bound.add("ascii")
        self.word_boundary = True
        self._atom(_BOUNDARY, word=False, width=width)

    # -- bracket expressions --------------------------------------------------------------------

    def _bracket(self) -> None:
        text = self.text
        if text.startswith("[[:<:]]", self.at):
            self._start_of_word(7)
            return
        if text.startswith("[[:>:]]", self.at):
            self._end_of_word(7)
            return
        written, end, word = self._bracket_at(self.at)
        self.out.append(written)
        self.at = end
        self.word_before = word

    def _bracket_at(self, at: int) -> tuple[str, int, bool]:
        """The bracket expression at ``at``: RE2's of it, the position past it, and whether it
        matches word characters alone."""
        text = self.text
        at += 1
        negated = text.startswith("^", at)
        if negated:
            at += 1
        body: list[str] = []
        others: list[str] = []  # complemented classes a bracket can only join by alternation
        items: list[tuple[str, str]] = []  # what decides whether it holds word characters alone
        first = True
        while True:
            if at >= len(text):
                raise AreError("brackets [] not balanced")
            c = text[at]
            if c == "]" and not first:
                at += 1
                break
            first = False
            if c == "[" and text[at + 1 : at + 2] in (":", ".", "="):
                kind = text[at + 1]
                end = text.find(kind + "]", at + 2)
                if end < 0:
                    raise AreError("brackets [] not balanced")
                name = text[at + 2 : end]
                at = end + 2
                if kind == ":":
                    if name not in _CLASSES:
                        raise AreError("invalid character class")
                    written, bound = _CLASSES[name]
                    body.append(written)
                    if bound is not None:
                        self.bound.add(bound)
                    word = name in ("alnum", "alpha", "digit", "lower", "upper", "word")
                    items.append(("class", "w" if word else ""))
                elif len(name) == 1:
                    body.append(_literal(name))
                    items.append(("char", name))
                else:
                    raise NoForm("a named collating element")
            elif c == "\\":
                at, written, complement, word = self._bracket_escape(at, negated)
                if complement is not None:
                    others.append(complement)
                else:
                    body.append(written)
                items.append(("class", "w" if word else ""))
            elif c == "-":
                body.append("-")
                items.append(("dash", "-"))
                at += 1
            else:
                body.append(_literal(c))
                items.append(("char", c))
                at += 1
        if negated and self.options.newline in "np":
            body.append("\\n")
        written = f"[{'^' if negated else ''}{''.join(body)}]"
        if others:
            written = "(?:" + "|".join(([written] if body else []) + others) + ")"
        return written, at, not negated and not others and _word_only(items)

    def _bracket_escape(self, at: int, negated: bool) -> tuple[int, str, str | None, bool]:
        """An escape in a bracket expression at ``at``: the position past it, what the bracket
        holds of it, a complemented class it can join only by alternation (\\S), and whether it
        is word characters alone."""
        text = self.text
        if at + 1 >= len(text):
            raise AreError("invalid escape \\ sequence")
        e = text[at + 1]
        held, self.at = self.at, at
        try:
            entered = self._entry(e, in_bracket=True)
        finally:
            self.at = held
        if entered is not None:
            written, width = entered
            return at + width, written, None, False
        if e == "d":
            return at + 2, _DIGIT, None, True
        if e == "w":
            self.bound.add("ascii")
            return at + 2, _WORD, None, True
        if e == "s":
            self.bound.add("space")
            return at + 2, _SPACE, None, False
        if e in "DW":
            if e == "W":
                self.bound.add("ascii")
            return at + 2, f"\\{e}", None, False
        if e == "S":
            if negated:
                raise NoForm("\\S in a complemented bracket expression")
            self.bound.add("space")
            return at + 2, "", f"[^{_SPACE}]", False
        if e.isdigit():
            raise NoForm("a digit escape in a bracket expression")
        if e.isalnum():
            raise AreError("invalid escape \\ sequence")
        return at + 2, _literal(e), None, False

    # -- the result -----------------------------------------------------------------------------

    def bound_of(self) -> tuple[str, str] | None:
        if "ascii" in self.bound:
            return _BEYOND_ASCII, LOCALE_CLASS
        if "space" in self.bound:
            return _SPACE_BEYOND_ASCII, LOCALE_SPACE
        return None


def _word_only(items: list[tuple[str, str]]) -> bool:
    """Whether a bracket expression's items are word characters alone: a word character, a
    range between two of one kind (a-z, A-Z, 0-9), a class of them."""
    at = 0
    while at < len(items):
        kind, said = items[at]
        if kind == "dash":
            return False
        if kind == "class" and said != "w":
            return False
        if kind == "char" and not _is_word(said):
            return False
        if at + 2 < len(items) and items[at + 1][0] == "dash":
            low, high = said, items[at + 2][1]
            if kind != "char" or items[at + 2][0] != "char" or not _is_word(high):
                return False
            same = any(f(low) and f(high) for f in (str.islower, str.isupper, str.isdigit))
            if not same:
                return False
            at += 3
            continue
        at += 1
    return True


def _all_in(text: str, allowed: str) -> bool:
    return all(c in allowed for c in text)


def translated(pattern: str, flags: str = "") -> Re2:
    """``pattern``, an ARE read with the option letters ``flags`` (a function's flags, less its
    g), as RE2 reads it as PostgreSQL does. Raises AreError where PostgreSQL rejects the pattern,
    NoForm where RE2 cannot express it, and ValueError for a flag that is no option letter."""
    options = _Options()
    options.read(flags, embedded=False)
    text = pattern
    if text.startswith("***="):
        options.literal = True
        text = text[4:]
    elif text.startswith("***:"):
        options.syntax = "a"
        text = text[4:]
    if options.literal:
        body = "".join(_literal(c) for c in text)
        return Re2(_grouped(body, options), 0, None, False, _grouped(body, options), False)
    if options.syntax != "a":
        raise NoForm("a basic or extended regular expression")
    if text.startswith("(?") and text[2:3].isalpha():
        end = 2
        while end < len(text) and text[end].isalpha():
            end += 1
        if not text.startswith(")", end):
            raise AreError("invalid embedded option")
        options.read(text[2:end], embedded=True)
        text = text[end + 1 :]
        if options.syntax != "a":
            raise NoForm("a basic or extended regular expression")
        if options.literal:
            body = "".join(_literal(c) for c in text)
            return Re2(_grouped(body, options), 0, None, False, _grouped(body, options), False)
    reader = _Reader(text, options)
    reader.read()
    if reader.open_groups:
        raise AreError("parentheses () not balanced")
    body = "".join(reader.out)
    return Re2(
        _grouped(body.replace(_BOUNDARY, "\\b").replace(_NOT_BOUNDARY, "\\B"), options),
        reader.opened,
        reader.bound_of(),
        reader.word_boundary,
        _grouped(body.replace(_BOUNDARY, "(?:)").replace(_NOT_BOUNDARY, "(?:)"), options),
        options.newline in "nw",
    )


def _grouped(body: str, options: _Options) -> str:
    flags = "s" + ("m" if options.newline in "nw" else "") + ("i" if options.no_case else "")
    return f"(?{flags}:{body})"
