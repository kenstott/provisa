# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wrap a Neo4j-backed table's registered ``query_template`` with a keyed WHERE filter.

Every Cypher statement actually executed against a live Neo4j instance must be built by wrapping
the table's own registered ``query_template`` text -- the one artifact anyone has verified against
the real graph (fragment.yaml's hand-authored MATCH/RETURN Cypher) -- never by inventing a label or
property spelling from the compiler's OWN internal SQL-facing property dict (``NodeMapping.properties``),
which is scoped to the cypher-over-relational surface and has no relationship to what the live
Neo4j instance actually calls its properties. A filter can only ever bind to a property the
template *already projects* (its own ``RETURN`` clause) -- never an invented or guessed one.

Shared by the row-level materializer's keyed fetch (``SourceRowLoader.load_keys``,
``provisa/events/source_loader.py``) and, eventually, the DIRECT route's single-table Cypher
execution (issue #119 / REQ-1863's counterpart) -- one capability, multiple callers.
"""

from __future__ import annotations

import re

from provisa.cypher.parser import MatchStep, WithClause, parse_cypher


class ProjectionFilterError(ValueError):
    """The query_template cannot be wrapped with a keyed filter -- never guessed, always loud."""


def resolve_projected_property(query_template: str, column: str) -> str:
    """Return the real Cypher property expression ``query_template``'s RETURN clause projects
    under ``column`` (its ``RETURN ... AS column`` alias, or the bare expression when unaliased).

    This is the only verified-correct name for anything actually stored in the live graph --
    ``fragment.yaml``'s query_templates are hand-authored against the real seeded data, unlike the
    compiler's own CQL naming convention. Raises when ``column`` is not among the projected
    elements: the filter can only ever bind to something already projected, never an invented name.
    """
    ast = parse_cypher(query_template)
    if ast.return_clause is None:
        raise ProjectionFilterError(f"query_template has no RETURN clause: {query_template!r}")
    for item in ast.return_clause.items:
        if item.alias == column or (item.alias is None and item.expression == column):
            # The parser reconstructs expression text by joining tokens with a single space
            # (token-for-token, not a verbatim substring of the source) -- tidy "o . prop" back to
            # "o.prop" so the wrapped Cypher this feeds into reads and executes identically to
            # hand-written text, never leaving a token-join artifact in what's sent to Neo4j.
            return re.sub(r"\s*\.\s*", ".", item.expression)
    projected = [i.alias or i.expression for i in ast.return_clause.items]
    raise ProjectionFilterError(
        f"column {column!r} is not among query_template's projected elements {projected!r} -- "
        "a keyed filter can only bind to something the projection already returns"
    )


def inject_keys_filter(query_template: str, pk_column: str, *, placeholder: str = "$keys") -> str:
    """Return ``query_template`` with an added ``WHERE <projected_pk_property> IN <placeholder>``.

    Single-column PK only -- composite PK is unimplemented (raise loud rather than guess an
    untested multi-property IN-shape with no live source to verify it against; Cypher has no
    tuple-IN syntax the way SQL does, so a composite key needs its own, deliberately designed,
    ``ANY(k IN $keys WHERE ...)`` shape, not a naive extension of this one).

    A template that already has a WHERE clause is unsupported for now: the parser reconstructs
    ``WhereClause.expression`` by re-joining tokens, so it is not a verbatim substring of the
    original text and cannot be safely located by a naive string search. None of today's
    registered templates have one; add safe splicing when one actually needs it.
    """
    real_prop = resolve_projected_property(query_template, pk_column)

    ast = parse_cypher(query_template)
    match_steps = [s for s in ast.pipeline if isinstance(s, MatchStep)]
    with_steps = [s for s in ast.pipeline if isinstance(s, WithClause)]
    if len(match_steps) != 1 or with_steps or ast.union_parts or ast.call_subqueries:
        raise ProjectionFilterError(
            "inject_keys_filter only supports a single-MATCH, non-WITH, non-UNION, "
            "non-CALL query_template"
        )
    if match_steps[0].where is not None:
        raise ProjectionFilterError(
            "inject_keys_filter does not yet support a query_template with an existing WHERE "
            "clause (none of today's registered templates need it; add safe splicing when one "
            "does, using clause source positions rather than the reconstructed expression text)"
        )
    m = re.search(r"\bRETURN\b", query_template, flags=re.IGNORECASE)
    if m is None:
        raise ProjectionFilterError(f"query_template has no RETURN keyword: {query_template!r}")
    return (
        f"{query_template[: m.start()]}WHERE {real_prop} IN {placeholder}\n"
        f"{query_template[m.start() :]}"
    )
