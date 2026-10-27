# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The patterns of a Cypher write the ephemeral layer reads (REQ-2176): a CREATE's (or a
MERGE's) node and relationship patterns, ``(a:A {k: v})-[:T {k: v}]->(b)``, each property's value
kept as the expression it is written as, and the statement split at its writing clause.

The governed compiler's own parser reads only what a read is; this reads what it leaves."""

# Requirements: REQ-2176

from __future__ import annotations

import re
from dataclasses import dataclass, field

from provisa.cypher.parser import CypherParseError

_IDENT = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"


@dataclass
class NodeSpec:
    variable: str | None
    labels: list[str]
    properties: list[tuple[str, str]]  # (key, the value's expression as written)


@dataclass
class RelSpec:
    variable: str | None
    rel_type: str
    properties: list[tuple[str, str]]
    source: int  # the index of its source node among the statement's node occurrences
    target: int


@dataclass
class CreatePattern:
    """A CREATE's patterns: each node occurrence in order, and each relationship between two."""

    nodes: list[NodeSpec] = field(default_factory=list)
    rels: list[RelSpec] = field(default_factory=list)


def _unquote(name: str) -> str:
    return name[1:-1] if name.startswith("`") else name


def _scan(text: str) -> list[tuple[int, str]]:
    """Each character of ``text`` outside a string or backquoted name, with its depth of nesting
    in (), [] and {} before it: (depth, char), the others as (-1, char)."""
    out: list[tuple[int, str]] = []
    depth = 0
    quote: str | None = None
    for i, ch in enumerate(text):
        if quote is not None:
            out.append((-1, ch))
            if ch == quote and text[i - 1] != "\\":
                quote = None
            continue
        if ch in "'\"`":
            quote = ch
            out.append((-1, ch))
            continue
        if ch in ")]}":
            depth -= 1
        out.append((depth, ch))
        if ch in "([{":
            depth += 1
    return out


def split_top(text: str, sep: str = ",") -> list[str]:
    """``text`` split at each ``sep`` outside brackets and strings."""
    parts: list[str] = []
    start = 0
    for i, (depth, ch) in enumerate(_scan(text)):
        if ch == sep and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return [p.strip() for p in parts]


def create_clause(statement: str) -> tuple[str, str] | None:
    """(the statement before its CREATE, its patterns), or None when it creates nothing. A
    statement with a clause after its CREATE patterns is refused by name."""
    scanned = _scan(statement)
    plain = "".join(ch if depth == 0 else " " for depth, ch in scanned)
    found = list(re.finditer(r"\bCREATE\b", plain, re.IGNORECASE))
    if not found:
        return None
    if len(found) > 1:
        raise CypherParseError("A statement creating ephemeral objects has one CREATE clause")
    at = found[0]
    after = plain[at.end() :]
    trailing = re.search(r"\b(RETURN|WITH|SET|DELETE|REMOVE|MERGE|MATCH|UNWIND)\b", after, re.I)
    if trailing is not None:
        raise CypherParseError(
            f"A CREATE of ephemeral objects ends its statement; it is followed by "
            f"{trailing.group(1).upper()}"
        )
    return statement[: at.start()].strip(), statement[at.end() :].strip()


def _map_entries(text: str) -> list[tuple[str, str]]:
    inner = text.strip()[1:-1].strip()
    if not inner:
        return []
    entries = []
    for part in split_top(inner):
        key, sep, value = part.partition(":")
        if not sep or not re.fullmatch(_IDENT, key.strip()):
            raise CypherParseError(f"Cannot read the property {part!r}")
        entries.append((_unquote(key.strip()), value.strip()))
    return entries


_NODE = re.compile(rf"\(\s*({_IDENT})?\s*((?::\s*{_IDENT}\s*)*)(\{{.*\}})?\s*\)\Z", re.S)
_REL = re.compile(rf"\[\s*({_IDENT})?\s*:\s*({_IDENT})\s*(\{{.*\}})?\s*\]\Z", re.S)


def _element(text: str, at: int) -> tuple[str, int]:
    """The bracketed element of ``text`` starting at ``at`` and the index after it."""
    scanned = _scan(text)
    base = scanned[at][0]
    for i in range(at + 1, len(text)):
        depth, ch = scanned[i]
        if depth == base and ch in ")]":
            return text[at : i + 1], i + 1
    raise CypherParseError(f"Unclosed pattern element: {text[at:]!r}")


def _node(text: str) -> NodeSpec:
    m = _NODE.match(text)
    if m is None:
        raise CypherParseError(f"Cannot read the node pattern {text!r}")
    labels = [_unquote(x.strip()) for x in re.findall(_IDENT, m.group(2) or "")]
    props = _map_entries(m.group(3)) if m.group(3) else []
    return NodeSpec(_unquote(m.group(1)) if m.group(1) else None, labels, props)


def parse_patterns(text: str) -> CreatePattern:
    """A CREATE's comma-separated patterns: ``(a:A {k: v})-[:T {k: v}]->(b)``, ..."""
    out = CreatePattern()
    for part in split_top(text):
        i = 0
        prev: int | None = None
        pending: tuple[str, str] | None = None  # (relationship element, direction)
        while i < len(part):
            ch = part[i]
            if ch.isspace():
                i += 1
                continue
            if ch == "(":
                element, i = _element(part, i)
                out.nodes.append(_node(element))
                here = len(out.nodes) - 1
                if pending is not None:
                    assert prev is not None
                    rel_text, direction = pending
                    m = _REL.match(rel_text)
                    if m is None:
                        raise CypherParseError(
                            f"A created relationship has exactly one type: {rel_text!r}"
                        )
                    source, target = (prev, here) if direction == "right" else (here, prev)
                    out.rels.append(
                        RelSpec(
                            _unquote(m.group(1)) if m.group(1) else None,
                            _unquote(m.group(2)),
                            _map_entries(m.group(3)) if m.group(3) else [],
                            source,
                            target,
                        )
                    )
                    pending = None
                prev = here
                continue
            if part.startswith("<-[", i) or part.startswith("-[", i):
                left = part.startswith("<", i)
                element, i = _element(part, part.index("[", i))
                if part.startswith("->", i):
                    if left:
                        raise CypherParseError("A created relationship has one direction")
                    pending, i = (element, "right"), i + 2
                elif part.startswith("-", i) and left:
                    pending, i = (element, "left"), i + 1
                else:
                    raise CypherParseError("A created relationship has a direction")
                continue
            raise CypherParseError(f"Cannot read the pattern {part!r} at {part[i:]!r}")
    return out


def bound(prefix: str) -> set[str]:
    """The variables a write's leading clauses (MATCH, UNWIND, WITH) bind."""
    if not prefix:
        return set()
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.ir import UnwindClause, WithClause

    ast = parse_cypher(prefix + " RETURN 1")
    out: set[str] = set()
    for m in ast.match_clauses:
        for n in getattr(m.pattern, "nodes", []):
            if n.variable:
                out.add(n.variable)
        for r in getattr(m.pattern, "rels", []):
            if r.variable:
                out.add(r.variable)
    for step in ast.pipeline:
        if isinstance(step, UnwindClause):
            out.add(step.variable)
        elif isinstance(step, WithClause):
            out.update(item.alias or item.name for item in step.items)
    return out
