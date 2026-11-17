# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0b7c1d-2a4f-4d8e-9b63-71c9a0f4e2d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A regular expression an org writes, matched in bounded time (REQ-2087).

An org's redaction pattern runs on that org's log records, spans and client text. Python's ``re``
backtracks, so a pattern such as ``(a+)+$`` takes exponential time on a line of ``a``s, and
nothing can interrupt it once it starts. An org's pattern is therefore never run by ``re``:

- When saved, :func:`compile_bounded` parses it with the standard library's own regex parser and
  refuses, by name, what this matcher does not run (backreferences, lookaround, atomic groups,
  possessive repeats, conditionals) and the shapes that backtrack catastrophically under a
  backtracking engine: a repeat nested in an unbounded repeat, and alternatives in an unbounded
  repeat that can begin with the same character.
- At use, :class:`BoundedPattern` runs it on Provisa's own matcher, a Pike VM (Thompson's NFA
  simulation, as RE2 uses): every position is visited once per search, with every live thread
  advanced together, so a search never backtracks. Each call has a step limit of
  :data:`STEP_FACTOR` x the program's size x (the text's length + 1) thread steps -- several times
  what one scan of the text can take -- and one that would exceed it raises
  :class:`StepLimitExceeded`, so no pattern stalls the thread that logs.

Matches are leftmost-first, as ``re`` finds them, and the flags ``i``, ``m``, ``s``, ``x`` and
``a`` mean what they mean to ``re``.
"""

# Requirements: REQ-2087

from __future__ import annotations

import importlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# The standard library's own regex parser and its constants (``re`` parses with them); typeshed
# ships no stubs for these modules, so they are loaded by name. No third-party engine.
sre: Any = importlib.import_module("re._constants")
sre_parse: Any = importlib.import_module("re._parser")

#: One scan of a text takes at most two steps per instruction per character (each instruction is
#: entered and stepped at most once per position). A call may take this many scans' worth: a
#: pattern that rescans the rest of the text after every match (quadratic) is cut off.
STEP_FACTOR = 8
#: Instructions one compiled pattern may hold: a bounded repeat is unrolled, so ``a{1,5000}``
#: would otherwise compile to a program the matcher steps through at every position.
PROGRAM_LIMIT = 512

#: The characters the alternation check compares first characters over.
_PROBE = [chr(c) for c in range(0x250)]


class PatternRefused(ValueError):
    """A pattern an org's rule may not hold, and why."""


class StepLimitExceeded(RuntimeError):
    """A pattern took more than :data:`STEP_LIMIT` steps on one text."""

    def __init__(self, steps: int, length: int) -> None:  # steps: the limit that was exceeded
        super().__init__(
            f"the pattern exceeded its step limit ({steps} steps) on a text of {length} characters"
        )
        self.steps = steps
        self.length = length


# --- the program --------------------------------------------------------------------------------

_CHAR, _SPLIT, _JMP, _ASSERT, _MATCH = range(5)


@dataclass
class _Inst:
    op: int
    pred: Callable[[str], bool] | None = None
    x: int = 0
    y: int = 0
    at: Callable[[str, int], bool] | None = None


def _is_word(c: str, ascii_only: bool) -> bool:
    if ascii_only and not c.isascii():
        return False
    return c.isalnum() or c == "_"


def _category(code: int, ascii_only: bool) -> Callable[[str], bool]:
    def ascii_ok(c: str) -> bool:
        return c.isascii() or not ascii_only

    if code is sre.CATEGORY_DIGIT:
        return lambda c: c.isdecimal() and ascii_ok(c)
    if code is sre.CATEGORY_NOT_DIGIT:
        return lambda c: not (c.isdecimal() and ascii_ok(c))
    if code is sre.CATEGORY_SPACE:
        return lambda c: c.isspace() and ascii_ok(c)
    if code is sre.CATEGORY_NOT_SPACE:
        return lambda c: not (c.isspace() and ascii_ok(c))
    if code is sre.CATEGORY_WORD:
        return lambda c: _is_word(c, ascii_only)
    if code is sre.CATEGORY_NOT_WORD:
        return lambda c: not _is_word(c, ascii_only)
    raise PatternRefused(f"the class {code} is not available in an org's rule")


def _folded(pred: Callable[[str], bool]) -> Callable[[str], bool]:
    return lambda c: pred(c) or pred(c.lower()) or pred(c.upper())


def _set(items: list, ascii_only: bool) -> Callable[[str], bool]:
    negate = False
    parts: list[Callable[[str], bool]] = []
    for op, av in items:
        if op is sre.NEGATE:
            negate = True
        elif op is sre.LITERAL:
            parts.append(lambda c, ch=chr(av): c == ch)
        elif op is sre.RANGE:
            parts.append(lambda c, lo=av[0], hi=av[1]: lo <= ord(c) <= hi)
        elif op is sre.CATEGORY:
            parts.append(_category(av, ascii_only))
        else:
            raise PatternRefused(f"{op} in a character class is not available in an org's rule")
    if negate:
        return lambda c: not any(p(c) for p in parts)
    return lambda c: any(p(c) for p in parts)


def _at(code: int, flags: int) -> Callable[[str, int], bool]:
    multiline = bool(flags & sre.SRE_FLAG_MULTILINE)
    ascii_only = bool(flags & sre.SRE_FLAG_ASCII)

    def boundary(text: str, i: int) -> bool:
        before = i > 0 and _is_word(text[i - 1], ascii_only)
        after = i < len(text) and _is_word(text[i], ascii_only)
        return before != after

    if code is sre.AT_BEGINNING:
        if multiline:
            return lambda t, i: i == 0 or t[i - 1] == "\n"
        return lambda t, i: i == 0
    if code is sre.AT_BEGINNING_STRING:
        return lambda t, i: i == 0
    if code is sre.AT_END:
        if multiline:
            return lambda t, i: i == len(t) or t[i] == "\n"
        return lambda t, i: i == len(t) or (i == len(t) - 1 and t[i] == "\n")
    if code is sre.AT_END_STRING:
        return lambda t, i: i == len(t)
    if code is sre.AT_BOUNDARY:
        return boundary
    if code is sre.AT_NON_BOUNDARY:
        return lambda t, i: not boundary(t, i)
    raise PatternRefused(f"the anchor {code} is not available in an org's rule")


_REFUSED_OPS = {
    "GROUPREF": "a backreference",
    "GROUPREF_EXISTS": "a conditional group",
    "ASSERT": "a lookahead or lookbehind",
    "ASSERT_NOT": "a negative lookahead or lookbehind",
    "ATOMIC_GROUP": "an atomic group",
    "POSSESSIVE_REPEAT": "a possessive repeat",
}


class _Compiler:
    def __init__(self) -> None:
        self.prog: list[_Inst] = []

    def emit(self, inst: _Inst) -> int:
        if len(self.prog) >= PROGRAM_LIMIT:
            raise PatternRefused(
                f"the pattern is too large (more than {PROGRAM_LIMIT} instructions once repeats are "
                "counted out)"
            )
        self.prog.append(inst)
        return len(self.prog) - 1

    def char(self, pred: Callable[[str], bool], flags: int) -> None:
        self.emit(_Inst(_CHAR, pred=_folded(pred) if flags & sre.SRE_FLAG_IGNORECASE else pred))

    def seq(self, items: list, flags: int) -> None:
        for op, av in items:
            self.node(op, av, flags)

    def node(self, op: int, av: Any, flags: int) -> None:  # noqa: C901 -- one branch per sre op
        name = str(op)
        if name in _REFUSED_OPS:
            raise PatternRefused(f"{_REFUSED_OPS[name]} is not available in an org's rule")
        ascii_only = bool(flags & sre.SRE_FLAG_ASCII)
        if op is sre.LITERAL:
            self.char(lambda c, ch=chr(av): c == ch, flags)
        elif op is sre.NOT_LITERAL:
            ch = chr(av)
            if flags & sre.SRE_FLAG_IGNORECASE:
                self.emit(_Inst(_CHAR, pred=lambda c: c.lower() != ch.lower()))
            else:
                self.emit(_Inst(_CHAR, pred=lambda c: c != ch))
        elif op is sre.ANY:
            if flags & sre.SRE_FLAG_DOTALL:
                self.emit(_Inst(_CHAR, pred=lambda c: True))
            else:
                self.emit(_Inst(_CHAR, pred=lambda c: c != "\n"))
        elif op is sre.IN:
            self.char(_set(av, ascii_only), flags)
        elif op is sre.AT:
            self.emit(_Inst(_ASSERT, at=_at(av, flags)))
        elif op is sre.BRANCH:
            self.branch(av[1], flags)
        elif op is sre.SUBPATTERN:
            _group, add, remove, body = av
            self.seq(body, (flags | add) & ~remove)
        elif op is sre.MAX_REPEAT or op is sre.MIN_REPEAT:
            low, high, body = av
            self.repeat(low, high, body, flags, greedy=op is sre.MAX_REPEAT)
        else:
            raise PatternRefused(f"{name} is not available in an org's rule")

    def branch(self, alternatives: list, flags: int) -> None:
        jumps: list[int] = []
        for k, alt in enumerate(alternatives):
            if k == len(alternatives) - 1:
                self.seq(alt, flags)
                break
            split = self.emit(_Inst(_SPLIT))
            self.prog[split].x = len(self.prog)
            self.seq(alt, flags)
            jumps.append(self.emit(_Inst(_JMP)))
            self.prog[split].y = len(self.prog)
        for j in jumps:
            self.prog[j].x = len(self.prog)

    def _optional(self, body: list, flags: int, greedy: bool) -> int:
        split = self.emit(_Inst(_SPLIT))
        start = len(self.prog)
        self.seq(body, flags)
        end = len(self.prog)
        self.prog[split].x, self.prog[split].y = (start, end) if greedy else (end, start)
        return split

    def repeat(self, low: int, high: int, body: list, flags: int, *, greedy: bool) -> None:
        for _ in range(low):
            self.seq(body, flags)
        if high is sre.MAXREPEAT:
            split = self.emit(_Inst(_SPLIT))
            start = len(self.prog)
            self.seq(body, flags)
            self.emit(_Inst(_JMP, x=split))
            end = len(self.prog)
            self.prog[split].x, self.prog[split].y = (start, end) if greedy else (end, start)
            return
        splits = [self._optional(body, flags, greedy) for _ in range(high - low)]
        # Each optional copy that is skipped skips the rest: x{0,3} is (x(x(x)?)?)?.
        end = len(self.prog)
        for split in splits:
            if greedy:
                self.prog[split].y = end
            else:
                self.prog[split].x = end


def _program(items: list, flags: int) -> list[_Inst]:
    compiler = _Compiler()
    compiler.seq(items, flags)
    compiler.emit(_Inst(_MATCH))
    return compiler.prog


# --- the save-time check ------------------------------------------------------------------------


def _unbounded(high: int) -> bool:
    return high is sre.MAXREPEAT


def _has_repeat(items: list) -> bool:
    for op, av in items:
        if op is sre.MAX_REPEAT or op is sre.MIN_REPEAT:
            _low, high, _body = av
            if high is sre.MAXREPEAT or high > 1:
                return True
        if op is sre.SUBPATTERN and _has_repeat(av[3]):
            return True
        if op is sre.BRANCH and any(_has_repeat(alt) for alt in av[1]):
            return True
    return False


def _first_chars(items: list, flags: int) -> set[str] | None:
    """The probe characters ``items`` can begin with, or None when it can match empty text."""
    prog = _program(items, flags)
    seen: set[int] = set()
    first: set[str] = set()
    stack = [0]
    while stack:
        pc = stack.pop()
        if pc in seen:
            continue
        seen.add(pc)
        inst = prog[pc]
        if inst.op == _MATCH:
            return None
        if inst.op == _CHAR:
            assert inst.pred is not None
            first.update(c for c in _PROBE if inst.pred(c))
        elif inst.op == _SPLIT:
            stack += [inst.x, inst.y]
        elif inst.op == _JMP:
            stack.append(inst.x)
        else:  # an anchor: zero width, so what follows it is what the text can begin with
            stack.append(pc + 1)
    return first


def _alternatives(items: list, flags: int) -> list[list]:
    """The alternatives ``items`` chooses between at its start, a group's included."""
    if len(items) != 1:
        return []
    op, av = items[0]
    if op is sre.BRANCH:
        return list(av[1])
    if op is sre.SUBPATTERN:
        return _alternatives(av[3], (flags | av[1]) & ~av[2])
    return []


def _overlapping(alternatives: list, flags: int) -> bool:
    seen: set[str] = set()
    for alt in alternatives:
        first = _first_chars(alt, flags)
        if first is None or first & seen:
            return True
        seen |= first
    return False


def _check(items: list, flags: int) -> None:
    for op, av in items:
        if op is sre.SUBPATTERN:
            _check(av[3], (flags | av[1]) & ~av[2])
        elif op is sre.BRANCH:
            for alt in av[1]:
                _check(alt, flags)
        elif op is sre.MAX_REPEAT or op is sre.MIN_REPEAT:
            _low, high, body = av
            if _unbounded(high):
                if _has_repeat(body):
                    raise PatternRefused(
                        "the pattern can backtrack catastrophically: a repeat is nested inside "
                        "an unbounded repeat"
                    )
                if _overlapping(_alternatives(body, flags), flags):
                    raise PatternRefused(
                        "the pattern can backtrack catastrophically: alternatives inside an "
                        "unbounded repeat can begin with the same text"
                    )
            _check(body, flags)


# --- the matcher --------------------------------------------------------------------------------


class BoundedPattern:
    """An org's pattern, compiled for the Pike VM: :meth:`sub` as ``re.sub`` with a function."""

    def __init__(self, pattern: str) -> None:
        try:
            parsed = sre_parse.parse(pattern, 0)
        except re.error as exc:
            raise PatternRefused(f"the pattern does not compile: {exc}") from exc
        flags = parsed.state.flags
        if flags & sre.SRE_FLAG_LOCALE:
            raise PatternRefused("the L flag is not available in an org's rule")
        items = list(parsed)
        _check(items, flags)
        self.pattern = pattern
        self._prog = _program(items, flags)
        #: Instructions in the compiled program.
        self.size = len(self._prog)

    def _search(self, text: str, pos: int, budget: list[int], limit: int) -> tuple[int, int] | None:
        prog, n = self._prog, len(text)
        clist: list[tuple[int, int]] = []
        visited: set[int] = set()
        matched: tuple[int, int] | None = None

        def add(threads: list[tuple[int, int]], seen: set[int], pc: int, start: int, i: int):
            stack = [pc]
            while stack:
                pc = stack.pop()
                if pc in seen:
                    continue
                seen.add(pc)
                budget[0] -= 1
                inst = prog[pc]
                if inst.op == _JMP:
                    stack.append(inst.x)
                elif inst.op == _SPLIT:
                    stack += [inst.y, inst.x]  # x is popped first: it has priority
                elif inst.op == _ASSERT:
                    assert inst.at is not None
                    if inst.at(text, i):
                        stack.append(pc + 1)
                else:
                    threads.append((pc, start))

        for i in range(pos, n + 1):
            if matched is None:
                add(clist, visited, 0, i, i)
            if not clist:
                if matched is not None:
                    break
                visited = set()  # nothing lives into the next position: it starts afresh
                continue
            if budget[0] < 0:
                raise StepLimitExceeded(limit, n)
            nlist: list[tuple[int, int]] = []
            nvisited: set[int] = set()
            c = text[i] if i < n else ""
            for pc, start in clist:
                budget[0] -= 1
                inst = prog[pc]
                if inst.op == _MATCH:
                    matched = (start, i)
                    break  # the threads after this one have lower priority
                assert inst.pred is not None
                if i < n and inst.pred(c):
                    add(nlist, nvisited, pc + 1, start, i + 1)
            clist, visited = nlist, nvisited
        if budget[0] < 0:
            raise StepLimitExceeded(limit, n)
        return matched

    def search(self, text: str) -> bool:
        """Whether the pattern matches anywhere in ``text``, as ``re.search``; raises
        :class:`StepLimitExceeded` past its step limit (:data:`STEP_FACTOR`)."""
        limit = STEP_FACTOR * self.size * (len(text) + 1)
        return self._search(text, 0, [limit], limit) is not None

    def sub(self, repl: Callable[[str], str], text: str) -> str:
        """``text`` with each non-overlapping match replaced by ``repl(match text)``, leftmost
        first; raises :class:`StepLimitExceeded` past its step limit (:data:`STEP_FACTOR`)."""
        limit = STEP_FACTOR * self.size * (len(text) + 1)
        budget = [limit]
        out: list[str] = []
        pos = 0
        n = len(text)
        while pos <= n:
            found = self._search(text, pos, budget, limit)
            if found is None:
                break
            start, end = found
            out.append(text[pos:start])
            out.append(repl(text[start:end]))
            if end == start:  # an empty match: keep the next character and go on past it
                out.append(text[start : start + 1])
                pos = start + 1
            else:
                pos = end
        if pos <= n:
            out.append(text[pos:])
        return "".join(out)


def compile_bounded(pattern: str) -> BoundedPattern:
    """``pattern`` compiled for the bounded matcher, or :class:`PatternRefused` saying why not."""
    return BoundedPattern(pattern)
