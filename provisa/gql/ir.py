# Copyright (c) 2026 Kenneth Stott
# Canary: 16571769-20f7-4aab-8a21-90b35dab49e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The graph query IR (REQ-2135): what a graph language front end parses a statement into, and what
the back end in this package lowers to semantic SQL.

Today Cypher's front end (provisa.cypher.parser) is the one that produces it. A clause's
expressions (WHERE, RETURN, WITH, UNWIND, ORDER BY) are still held as Cypher expression text, which
the back end lowers through Cypher's expression grammar; they become expression trees next.
"""

# Requirements: REQ-2135, REQ-347, REQ-348, REQ-571

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodePattern:
    variable: str | None
    labels: list[str]
    properties: dict[str, Any]
    label_alternation: bool = False
    #: Each inline property's value as the statement writes it (``'x'``, ``42``, ``$p``), which a
    #: read matches on as ``WHERE var.prop = <value>`` (:func:`with_inline_conditions`).
    property_text: dict[str, str] = field(default_factory=dict)


@dataclass
class RelPattern:
    variable: str | None
    types: list[str]
    min_hops: int | None  # None = exactly 1 unless variable-length
    max_hops: int | None
    direction: str  # "right" | "left" | "none"
    variable_length: bool = False
    #: An inline property map on the relationship, as its node's is (``property_text``).
    properties: dict[str, Any] = field(default_factory=dict)
    property_text: dict[str, str] = field(default_factory=dict)


@dataclass
class PathPattern:
    nodes: list[NodePattern]
    rels: list[RelPattern]


@dataclass
class PathFunction:
    func_name: str  # "shortestPath" | "allShortestPaths"
    pattern: PathPattern


@dataclass
class MatchClause:
    pattern: PathPattern | PathFunction
    variable: str | None  # path variable (p = shortestPath(...))
    optional: bool = False


@dataclass
class WhereClause:
    expression: str  # raw expression text


@dataclass
class ReturnItem:
    expression: str
    alias: str | None = None


@dataclass
class ReturnClause:
    items: list[ReturnItem]
    distinct: bool = False


@dataclass
class WithClause:
    items: list[ReturnItem]
    where: WhereClause | None = None
    # A WITH's own ORDER BY, SKIP and LIMIT pick the rows it carries on, not the statement's.
    order_by: list[OrderItem] = field(default_factory=list)
    skip: int | None = None
    limit: int | None = None


@dataclass
class UnwindClause:
    expression: str  # raw expression text (list literal, param, or property)
    variable: str  # AS variable name


@dataclass
class MatchStep:
    """One or more MATCH/OPTIONAL MATCH clauses and their associated WHERE."""

    matches: list[MatchClause]
    where: WhereClause | None = None


@dataclass
class CallSubquery:
    body: "CypherAST"
    imported_vars: list[str] = field(default_factory=list)  # vars from outer scope via WITH


@dataclass
class OrderItem:
    expression: str
    direction: str = "ASC"


@dataclass
class CypherAST:  # REQ-347, REQ-348, REQ-571
    pipeline: list["MatchStep | WithClause"]
    return_clause: ReturnClause | None
    order_by: list[OrderItem]
    skip: int | None
    limit: int | None
    call_subqueries: list[CallSubquery] = field(default_factory=list)
    # UNION / UNION ALL: list of (sub_ast, is_all)
    union_parts: list[tuple["CypherAST", bool]] = field(default_factory=list)
    comments: list[str] = field(
        default_factory=list
    )  # // comment text (without //) in source order

    @property
    def match_clauses(self) -> list[MatchClause]:
        return [m for s in self.pipeline if isinstance(s, MatchStep) for m in s.matches]

    @property
    def with_clauses(self) -> list[WithClause]:
        return [s for s in self.pipeline if isinstance(s, WithClause)]

    @property
    def where(self) -> WhereClause | None:
        for s in self.pipeline:
            if isinstance(s, MatchStep) and s.where is not None:
                return s.where
        return None
