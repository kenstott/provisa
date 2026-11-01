# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c1e70-6d38-4b52-8f07-c2e5b19d7a36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's tsvector and tsquery values (REQ-2171), as the server reads and writes them: the
input grammar of tsvector_parser.c and tsquery.c, the output of tsvectorout and tsqueryout, and
what is computed of a tsquery alone (tsquery_op.c, tsquery_cleanup.c). pg_forms_textsearch folds a
literal through these; an engine only ever reads a value in the form they write."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
import zlib
from dataclasses import dataclass, field

from provisa.core.pg_error import PostgresError

_MAX_POSITION = 16383
_MAX_DISTANCE = 16384
_MAX_POSITIONS = 256
_WEIGHTS = "DCBA"
_SYNTAX = "42601"
_NOT, _AND, _OR, _PHRASE = "!", "&", "|", "<->"
_PRIORITY = {_OR: 1, _AND: 2, _PHRASE: 3, _NOT: 4}
#: tsquery.h's numbering of the operators, which CompareTSQ orders by.
_OPERATOR_NUMBER = {_NOT: 1, _AND: 2, _OR: 3, _PHRASE: 4}


def _quoted(lexeme: str) -> str:
    return "'" + lexeme.replace("\\", "\\\\").replace("'", "''") + "'"


def _word(text: str, start: int, query: bool) -> tuple[str, int]:
    """The lexeme at ``start`` and where it ends: gettoken_tsvector's states, a quoted lexeme or a
    bare one, with its backslash escapes. A query's bare lexeme also ends at its operators."""
    out: list[str] = []
    at, size = start, len(text)
    if text[at] == "'":
        at += 1
        while True:
            if at >= size:
                raise ValueError("unterminated")
            if text[at] == "\\":
                if at + 1 >= size:
                    raise ValueError("unterminated")
                out.append(text[at + 1])
                at += 2
            elif text[at] == "'":
                if at + 1 < size and text[at + 1] == "'":
                    out.append("'")
                    at += 2
                else:
                    return "".join(out), at + 1
            else:
                out.append(text[at])
                at += 1
    ends = "!|&():<" if query else ":"
    while at < size:
        char = text[at]
        if at == start and char == ":":  # the first character of a word is its own
            out.append(char)
            at += 1
            continue
        if char == "\\":
            if at + 1 >= size:
                raise ValueError("unterminated")
            out.append(text[at + 1])
            at += 2
        elif char.isspace() or char in ends:
            break
        else:
            out.append(char)
            at += 1
    return "".join(out), at


def tsvector_entries(text: str) -> dict[str, list[tuple[int, int]]]:
    """``text``, a tsvector's input, as lexeme -> sorted (position, weight); raises PostgreSQL's
    syntax error."""

    def wrong() -> PostgresError:
        return PostgresError(_SYNTAX, f'syntax error in tsvector: "{text}"')

    entries: dict[str, list[tuple[int, int]]] = {}
    at, size = 0, len(text)
    while at < size:
        if text[at].isspace():
            at += 1
            continue
        try:
            lexeme, at = _word(text, at, False)
        except ValueError:
            raise wrong() from None
        if not lexeme:
            raise wrong()
        entry = entries.setdefault(lexeme, [])
        if at < size and text[at] == ":":
            at += 1
            while True:
                digits = re.compile(r"\d+").match(text, at)
                if digits is None:
                    raise wrong()
                position = min(int(digits.group()), _MAX_POSITION)
                at = digits.end()
                if position == 0:
                    raise PostgresError(_SYNTAX, f'wrong position info in tsvector: "{text}"')
                weight = 0
                if at < size and text[at].upper() in "ABCD":
                    weight = _WEIGHTS.index(text[at].upper())
                    at += 1
                entry.append((position, weight))
                if at < size and text[at] == ",":
                    at += 1
                    continue
                break
            if at < size and not text[at].isspace():
                raise wrong()
    return {k: _unique(v) for k, v in entries.items()}


def _unique(positions: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """uniquePos: each position once, of its heaviest weight, the first 256."""
    best: dict[int, int] = {}
    for position, weight in positions:
        best[position] = max(weight, best.get(position, 0))
    return sorted(best.items())[:_MAX_POSITIONS]


def tsvector_text(entries: dict[str, list[tuple[int, int]]]) -> str:
    """tsvectorout: the lexemes in byte order."""

    def written(lexeme: str) -> str:
        marks = ",".join(f"{p}{_WEIGHTS[w] if w else ''}" for p, w in entries[lexeme])
        return _quoted(lexeme) + (f":{marks}" if marks else "")

    return " ".join(written(k) for k in sorted(entries, key=lambda k: k.encode()))


def tsvector_in(text: str) -> str:
    """A tsvector's input as PostgreSQL writes it back."""
    return tsvector_text(tsvector_entries(text))


@dataclass(frozen=True)
class Node:
    """A tsquery's operator (``children``) or operand (``lexeme``)."""

    op: str = ""
    children: tuple["Node", ...] = ()
    distance: int = 1
    lexeme: str = ""
    weight: str = ""  # of ABCD
    prefix: bool = False


@dataclass
class _Reader:
    text: str
    at: int = 0
    ops: list[tuple[str, int]] = field(default_factory=list)
    out: list[Node] = field(default_factory=list)

    def wrong(self) -> PostgresError:
        return PostgresError(_SYNTAX, f'syntax error in tsquery: "{self.text}"')

    def space(self) -> None:
        while self.at < len(self.text) and self.text[self.at].isspace():
            self.at += 1

    def reduce(self, floor: int) -> None:
        """makepol's cleanOpStack: what the stack holds of at least ``floor``."""
        while self.ops and self.ops[-1][0] != "(" and _PRIORITY[self.ops[-1][0]] >= floor:
            op, distance = self.ops.pop()
            if op == _NOT:
                self.out.append(Node(op, (self.out.pop(),)))
            else:
                right, left = self.out.pop(), self.out.pop()
                self.out.append(Node(op, (left, right), distance))

    def marks(self) -> tuple[str, bool]:
        weight, prefix = "", False
        if self.at < len(self.text) and self.text[self.at] == ":":
            self.at += 1
            while self.at < len(self.text) and self.text[self.at] in "*aAbBcCdD":
                char = self.text[self.at].upper()
                prefix = prefix or char == "*"
                weight += char if char != "*" and char not in weight else ""
                self.at += 1
        return "".join(c for c in "ABCD" if c in weight), prefix

    def distance(self) -> int | None:
        found = re.compile(r"<(-|\d+)>").match(self.text, self.at)
        if found is None:
            return None
        self.at = found.end()
        step = found.group(1)
        distance = 1 if step == "-" else int(step)
        if distance > _MAX_DISTANCE:
            raise PostgresError(
                "22023",
                "distance in phrase operator must be an integer value between zero and 16384 "
                "inclusive",
            )
        return distance

    def operand(self) -> None:
        self.space()
        if self.at >= len(self.text):
            raise PostgresError(_SYNTAX, f'no operand in tsquery: "{self.text}"')
        char = self.text[self.at]
        if char == "!":
            self.at += 1
            self.ops.append((_NOT, 0))
            self.operand()
            return
        if char == "(":
            self.at += 1
            self.ops.append(("(", 0))
            self.operand()
            return
        if char in ":)&|<":
            raise self.wrong()
        try:
            lexeme, self.at = _word(self.text, self.at, True)
        except ValueError:
            raise self.wrong() from None
        if not lexeme:
            raise self.wrong()
        weight, prefix = self.marks()
        self.out.append(Node(lexeme=lexeme, weight=weight, prefix=prefix))
        self.operator()

    def operator(self) -> None:
        while True:
            self.space()
            if self.at >= len(self.text):
                self.reduce(0)
                if self.ops:
                    raise self.wrong()
                return
            char = self.text[self.at]
            if char == ")":
                self.at += 1
                self.reduce(0)
                if not self.ops:
                    raise self.wrong()
                self.ops.pop()
                continue
            distance = self.distance() if char == "<" else None
            if distance is not None:
                op, step = _PHRASE, distance
            elif char in "&|":
                self.at += 1
                op, step = char, 1
            else:
                raise self.wrong()
            self.reduce(_PRIORITY[op])
            self.ops.append((op, step))
            self.operand()
            return


def tsquery_in(text: str) -> Node | None:
    """``text`` as a query, None for a query of no lexemes; raises PostgreSQL's syntax error."""
    reader = _Reader(text)
    reader.space()
    if reader.at >= len(text):
        return None
    reader.operand()
    return reader.out[0]


def tsquery_text(node: Node | None) -> str:
    """tsqueryout's infix, as infix() parenthesizes."""
    return "" if node is None else _infix(node, 0, False)


def _infix(node: Node, parent: int, right_of_phrase: bool) -> str:
    if not node.op:
        marks = ("*" if node.prefix else "") + node.weight
        return _quoted(node.lexeme) + (f":{marks}" if marks else "")
    priority = _PRIORITY[node.op]
    if node.op == _NOT:
        inner = "!" + _infix(node.children[0], priority, False)
        return f"( {inner} )" if priority < parent else inner
    left = _infix(node.children[0], priority, False)
    right = _infix(node.children[1], priority, node.op == _PHRASE)
    spelled = node.op
    if node.op == _PHRASE and node.distance != 1:
        spelled = f"<{node.distance}>"
    written = f"{left} {spelled} {right}"
    return (
        f"( {written} )"
        if priority < parent or (node.op == _PHRASE and right_of_phrase)
        else written
    )


def nodes(node: Node | None) -> int:
    """numnode."""
    return 0 if node is None else 1 + sum(nodes(c) for c in node.children)


def operands(node: Node | None) -> list[Node]:
    if node is None:
        return []
    return [node] if not node.op else [o for c in node.children for o in operands(c)]


def _clean_not(node: Node | None) -> Node | None:
    """clean_NOT: a query with every NOT left out, None where one held the whole of a branch."""
    if node is None or not node.op:
        return node
    if node.op == _NOT:
        return None
    left, right = (_clean_not(c) for c in node.children)
    if left is None or right is None:
        kept = right if left is None else left
        return None if node.op == _OR else kept
    return Node(node.op, (left, right), node.distance)


def querytree(node: Node | None) -> str:
    """querytree: 'T' where NOT leaves nothing to search for."""
    if node is None:
        return ""
    kept = _clean_not(node)
    return "T" if kept is None else tsquery_text(kept)


def joined(op: str, left: Node | None, right: Node | None, distance: int = 1) -> Node | None:
    """tsquery_and, tsquery_or and tsquery_phrase: an empty query gives the other."""
    if left is None or right is None:
        return right if left is None else left
    return Node(op, (left, right), distance)


def negated(node: Node | None) -> Node | None:
    return None if node is None else Node(_NOT, (node,))


def _legacy_crc(lexeme: str) -> int:
    """COMP_LEGACY_CRC32 of a lexeme, as an int32: valcrc. The legacy CRC indexes a reflected
    table from the high byte."""
    crc = 0xFFFFFFFF
    for byte in lexeme.encode():
        crc = (_CRC_TABLE[((crc >> 24) ^ byte) & 0xFF] ^ (crc << 8)) & 0xFFFFFFFF
    crc ^= 0xFFFFFFFF
    return crc - (1 << 32) if crc >= 1 << 31 else crc


_CRC_TABLE = [zlib.crc32(bytes([i ^ 0xFF])) ^ 0xFF000000 for i in range(256)]


def _sizes(node: Node | None) -> tuple[int, int]:
    """A query's item count and the bytes of its operands: what CompareTSQ orders by first."""
    if node is None:
        return (0, 0)
    found = [nodes(node), 0]
    found[1] = sum(len(o.lexeme.encode()) + 1 for o in operands(node))
    return found[0], found[1]


def compare(left: Node | None, right: Node | None) -> int:
    """CompareTSQ."""
    a, b = _sizes(left), _sizes(right)
    if a != b:
        return -1 if a < b else 1
    return 0 if left is None or right is None else _node_compare(left, right)


def _node_compare(a: Node, b: Node) -> int:
    """QTNodeCompare."""
    if bool(a.op) != bool(b.op):
        return -1 if not b.op else 1
    if not a.op:
        crcs = _legacy_crc(a.lexeme), _legacy_crc(b.lexeme)
        if crcs[0] != crcs[1]:
            return -1 if crcs[0] > crcs[1] else 1
        return _string_compare(a.lexeme, b.lexeme)
    if a.op != b.op:
        return -1 if _OPERATOR_NUMBER[a.op] > _OPERATOR_NUMBER[b.op] else 1
    if len(a.children) != len(b.children):
        return -1 if len(a.children) > len(b.children) else 1
    for x, y in zip(a.children, b.children):
        order = _node_compare(x, y)
        if order:
            return order
    if a.distance != b.distance:
        return -1 if a.distance > b.distance else 1
    return 0


def _string_compare(a: str, b: str) -> int:
    x, y = a.encode(), b.encode()
    return (x > y) - (x < y)


def contains(query: Node | None, wanted: Node | None) -> bool:
    """tsq_mcontains: every operand of ``wanted``'s among the query's."""
    held = {_legacy_crc(o.lexeme) for o in operands(query)}
    return all(_legacy_crc(o.lexeme) in held for o in operands(wanted))
