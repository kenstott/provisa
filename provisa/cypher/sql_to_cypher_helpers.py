# Copyright (c) 2026 Kenneth Stott
# Canary: 7643abaf-8a1a-4212-b72d-3c2e57fa4e9f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Leaf helpers for the SQL→Cypher translator.

Pure functions shared by sql_to_cypher and sql_to_cypher_agg: label resolution,
JOIN-ON parsing, and minimal SQL-expression→Cypher rewriting. No dependency on
either caller module.
"""

from __future__ import annotations

import re

import sqlglot.expressions as exp

from provisa.cypher.label_map import RelationshipMapping


def _resolve_label(
    tbl: exp.Table,
    domain_to_label: dict[tuple[str, str], str],
) -> str | None:
    """Map a sqlglot Table node to a Cypher node label using the domain lookup."""
    db = tbl.db or ""
    name = tbl.name or ""
    return domain_to_label.get((db, name)) or domain_to_label.get(("", name))


def _rel_type_from_on(
    on_expr: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    join_to_rel: dict[tuple[str, str, str], RelationshipMapping | None],
    tgt_label: str | None = None,
) -> str | None:
    """Extract Cypher relationship type from a JOIN ON condition."""
    if on_expr is None:
        return None
    for eq in on_expr.find_all(exp.EQ):
        left, right = eq.this, eq.expression
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            lc = left.name
            rc = right.name
            rel = (
                (join_to_rel.get((lc, rc, tgt_label)) or join_to_rel.get((rc, lc, tgt_label)))
                if tgt_label is not None
                else None
            )
            if rel:
                return rel.rel_type
    return None


def rel_pattern(  # REQ-464
    src_label: str | None,
    tgt_label: str | None,
    src_tgt_to_rel: dict[tuple[str, str], str | None],
    *,
    rel_type: str | None = None,
) -> str:
    """The Cypher relationship pattern joining ``src_label`` to ``tgt_label``.

    ``rel_type``, when given, is a type already resolved from the JOIN ON columns — the most
    precise source there is, and oriented source→target by construction.

    Otherwise the ORDERED PAIR decides. Keying on the target label alone (which is what the
    translator used to fall back on) has no single answer once two relationships land on the same
    label — a junction table, or two foreign keys onto one table — and the last-writer-wins map
    answered with a real-but-wrong type: a playgroup-membership join came out as
    ``(:Playgroups)-[:SHARES_ENCLOSURE]->(:Pets)``. When the pair resolves only in reverse the
    arrow flips; when it does not resolve, or the pair itself is ambiguous, the pattern is
    anonymous and undirected, which still matches the intended edge instead of naming another one.
    """
    if rel_type is not None:
        return f"-[:{rel_type}]->"
    if src_label is not None and tgt_label is not None:
        forward = src_tgt_to_rel.get((src_label, tgt_label))
        if forward is not None:
            return f"-[:{forward}]->"
        reverse = src_tgt_to_rel.get((tgt_label, src_label))
        if reverse is not None:
            return f"<-[:{reverse}]-"
    return "-[]-"


def _src_alias_from_on(
    on_expr: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    tgt_sql_alias: str,
    default_alias: str,
    *,
    also_target: str | None = None,
) -> str:
    """Return the source table alias from a JOIN ON condition.

    Looks for column references whose table qualifier is not the join target —
    that's the source side of the relationship.  Falls back to default_alias.

    ``also_target`` names a second qualifier that is still the target side: a LATERAL subquery's
    inner WHERE qualifies its own columns by the inner TABLE name, not by the subquery alias, so
    without it the target's own name is read as the source.
    """
    if on_expr is None:
        return default_alias
    targets = {tgt_sql_alias, also_target}
    for eq in on_expr.find_all(exp.EQ):
        for col in (eq.this, eq.expression):
            if isinstance(col, exp.Column) and col.table and col.table not in targets:
                return col.table
    return default_alias


def _sql_to_cypher_expr(sql_expr: str) -> str:
    """Minimally rewrite a SQL expression fragment to Cypher syntax."""
    # Remove double-quote wrapping from identifiers (sqlglot emits them)
    result = re.sub(r'"(\w+)"', r"\1", sql_expr)
    result = result.replace(" ILIKE ", " =~ ")
    result = result.replace("TRUE", "true").replace("FALSE", "false")
    return result
