# Copyright (c) 2026 Kenneth Stott
# Canary: 5ab6d152-4c89-405b-a60d-0e3e60213723
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The patterns of a Cypher write the ephemeral layer reads (REQ-2176), from the graph IR
(REQ-2135): a CREATE's node and relationship patterns, ``(a:A {k: v})-[:T {k: v}]->(b)``, each
property's value kept as the expression it is written as, and the statement's leading clauses."""

# Requirements: REQ-2176

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from provisa.cypher.parser import CypherParseError


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


@dataclass(frozen=True)
class CreateOf:
    """A statement's CREATE, as the ephemeral layer reads it from the graph IR: the text of its
    leading clauses, its patterns, the variables those clauses bind (which it reaches back to
    rather than creates), and the clause following it, if any (RETURN, SET, ...)."""

    prefix: str
    pattern: CreatePattern
    bound: set[str]
    trailing: str | None


_TRAILING = {
    "CreateClause": "CREATE",
    "SetClause": "SET",
    "RemoveClause": "REMOVE",
    "DeleteClause": "DELETE",
    "MergeClause": "MERGE",
    "WithClause": "WITH",
    "MatchStep": "MATCH",
    "UnwindClause": "UNWIND",
    "ProcedureCall": "CALL",
}


def create_of(statement: str) -> CreateOf | None:
    """The statement's CREATE read from the graph IR (parse_cypher_statement), or None when it
    creates nothing. A statement of two CREATE clauses is refused by name."""
    from provisa.cypher.parser import parse_cypher_statement, writes_in
    from provisa.gql.ir import CreateClause

    if not writes_in(statement):
        return None
    ast = parse_cypher_statement(statement)
    creates = [i for i, step in enumerate(ast.pipeline) if isinstance(step, CreateClause)]
    if not creates:
        return None
    if len(creates) > 1:
        raise CypherParseError("A statement creating ephemeral objects has one CREATE clause")
    at = creates[0]
    create = ast.pipeline[at]
    assert isinstance(create, CreateClause)
    after = ast.pipeline[at + 1 :]
    trailing = (
        _TRAILING[type(after[0]).__name__]
        if after
        else "RETURN"
        if ast.return_clause is not None
        else None
    )
    return CreateOf(
        statement[: create.start].strip(),
        _pattern(create),
        _bound(ast.pipeline[:at]),
        trailing,
    )


def _pattern(create: Any) -> CreatePattern:
    """A CREATE clause's patterns: each node occurrence in order, each relationship between the
    two it joins, each property's value as written."""
    out = CreatePattern()
    for path in create.patterns:
        base = len(out.nodes)
        for node in path.nodes:
            out.nodes.append(
                NodeSpec(node.variable, list(node.labels), list(node.property_text.items()))
            )
        for i, rel in enumerate(path.rels):
            if len(rel.types) != 1:
                raise CypherParseError(
                    f"A created relationship has exactly one type: {', '.join(rel.types) or 'none'}"
                )
            if rel.direction not in ("right", "left"):
                raise CypherParseError("A created relationship has a direction")
            left, right = base + i, base + i + 1
            source, target = (left, right) if rel.direction == "right" else (right, left)
            out.rels.append(
                RelSpec(rel.variable, rel.types[0], list(rel.property_text.items()), source, target)
            )
    return out


def _bound(steps: list[Any]) -> set[str]:
    """The variables a write's leading clauses (MATCH, UNWIND, WITH) bind."""
    from provisa.gql.ir import MatchStep, UnwindClause, WithClause

    out: set[str] = set()
    for step in steps:
        if isinstance(step, MatchStep):
            for m in step.matches:
                out.update(n.variable for n in getattr(m.pattern, "nodes", []) if n.variable)
                out.update(r.variable for r in getattr(m.pattern, "rels", []) if r.variable)
        elif isinstance(step, UnwindClause):
            out.add(step.variable)
        elif isinstance(step, WithClause):
            out.update(item.alias or item.name for item in step.items)
    return out
