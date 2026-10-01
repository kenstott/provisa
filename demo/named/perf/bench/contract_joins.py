# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Which joins can extend a request, on which transport, and how far (REQ-1911)."""

from __future__ import annotations

from contract_model import TRANSPORT_LANGUAGE, Join, JoinStep, Setup, Table

_JOIN_KINDS = {"same": "joins_same_source", "cross": "joins_cross_source"}


def _joins_of(setup: Setup, kind: str) -> list[Join]:
    if kind == "cross":
        return list(setup.cross_source_joins)
    return [j for src in setup.sources.values() for j in src.joins]


def expandable_joins(
    setup: Setup, transport: str, members: list[tuple[str, str]], kind: str
) -> list[JoinStep]:
    """Joins of ``kind`` that can add a table to a request whose tables are ``members`` (source,
    table), on ``transport``: the other end must be new, its source must have weight there, and
    the transport's language must be able to spell the join (GraphQL and Cypher only follow the
    declared direction)."""
    language = TRANSPORT_LANGUAGE[transport]
    weights = setup.source_weights(transport)
    tables = {(sid, t.name): t for sid, s in setup.sources.items() for t in s.tables}
    out: list[JoinStep] = []
    for j in _joins_of(setup, kind):
        left, right = (j.left_source, j.left_table), (j.right_source, j.right_table)
        if left == right:
            continue
        for forward in (True, False):
            if not forward and language != "sql":
                continue
            parent, child = (left, right) if forward else (right, left)
            if parent not in members or child in members or weights[child[0]] <= 0:
                continue
            if language == "graphql" and not (j.graphql_field and tables[child].has("graphql")):
                continue
            if language == "cypher" and not (j.cypher_rel and tables[child].has("cypher")):
                continue
            pc, cc = (j.left_column, j.right_column) if forward else (j.right_column, j.left_column)
            out.append(
                JoinStep(
                    members.index(parent),
                    kind,
                    child[0],
                    child[1],
                    pc,
                    cc,
                    forward,
                    j.graphql_field,
                    j.cypher_rel,
                )
            )
    return out


def reachable_joins(setup: Setup, transport: str, source: str, table: str, kind: str) -> int:
    """How many joins of ``kind`` a request starting at ``table`` can carry on ``transport``."""
    members = [(source, table)]
    while True:
        fresh = {}
        for step in expandable_joins(setup, transport, members, kind):
            fresh.setdefault((step.child_source, step.child_table), step)
        if not fresh:
            return len(members) - 1
        members.extend(fresh)


def transport_targets(setup: Setup, transport: str) -> list[tuple[str, Table]]:
    """(source, table) a request on ``transport`` can start at, in declaration order."""
    weights = setup.source_weights(transport)
    return [
        (sid, t)
        for sid, src in setup.sources.items()
        if weights[sid] > 0
        for t in src.tables
        if t.weight > 0
    ]
