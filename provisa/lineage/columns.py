# Copyright (c) 2026 Kenneth Stott
# Canary: df2f32d4-e8d8-42e6-a069-e2bf00b38011
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column-level lineage resolution from a view SELECT (REQ-862).

For each output column of an MV's ``view_sql``, resolve the upstream source
column(s) it derives from and the transform expression that produces it. This is
the pure, reusable core of the column-level trace instrumentation: the MV refresh
path (``provisa/mv/refresh.py``) emits these derivations as OTel span attributes,
and users DERIVE point-in-time lineage by querying the resulting trace records —
the platform does not run a lineage-resolution engine at query time.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage


@dataclass(frozen=True)
class ColumnDerivation:  # REQ-862
    """One output column's resolved derivation."""

    output: str  # output column name (alias or projected name)
    sources: tuple[str, ...]  # upstream leaf source columns (e.g. "orders.amount")
    transform: str  # the projection expression that produces the output


def resolve_column_lineage(sql: str, dialect: str | None = None) -> list[ColumnDerivation]:
    """Resolve per-output-column derivations for a SELECT statement.

    Returns one :class:`ColumnDerivation` per projected output column, each naming its
    upstream leaf source columns and the transform expression. ``SELECT *`` and columns
    the parser cannot resolve yield an empty ``sources`` tuple (still recorded, so the
    output column is never silently dropped from the lineage trace). Raises SqlglotError
    only when the SQL itself cannot be parsed.
    """
    expr = sqlglot.parse_one(sql, dialect=dialect)
    selects = list(getattr(expr, "selects", []) or [])
    derivations: list[ColumnDerivation] = []
    for projection in selects:
        output = projection.alias_or_name
        transform = projection.sql(dialect=dialect)
        sources: tuple[str, ...] = ()
        # A projection with no column references is a pure literal/constant — no
        # upstream source column (sqlglot's lineage would otherwise leaf on the alias).
        if output and output != "*" and projection.find(exp.Column) is not None:
            try:
                node = lineage(output, sql, dialect=dialect)
                sources = tuple(
                    sorted({n.name for n in node.walk() if not n.downstream and n.name})
                )
            except (SqlglotError, KeyError, ValueError):
                sources = ()  # unresolvable column — recorded with no sources, not dropped
        derivations.append(ColumnDerivation(output=output, sources=sources, transform=transform))
    return derivations


def lineage_span_attributes(derivations: list[ColumnDerivation]) -> dict[str, str]:
    """Flatten derivations into OTel span attributes (REQ-862).

    Emits ``lineage.column.<output>.sources`` (comma-joined) and
    ``lineage.column.<output>.transform`` per column, plus a ``lineage.columns`` roster.
    """
    attrs: dict[str, str] = {"lineage.columns": ",".join(d.output for d in derivations)}
    for d in derivations:
        attrs[f"lineage.column.{d.output}.sources"] = ",".join(d.sources)
        attrs[f"lineage.column.{d.output}.transform"] = d.transform
    return attrs
