# Copyright (c) 2026 Kenneth Stott
# Canary: 7b90e724-f4a4-459b-9905-94ec3fbcb12e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Metric-composed view compilation over the live registries (REQ-1318).

The admin mutation path's counterpart to the config-loader pass in
``provisa.core.config_loader._upsert_tables``: a ``ViewMetricsSpec`` compiles to its
SELECT against the REPOSITORY-backed metric/table/relationship registries, and every
persisted spec regenerates when a referenced metric changes — the stored view SQL can
never drift from the governed metric definition.
"""

# Requirements: REQ-1318

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

from sqlalchemy import select, update

from provisa.core.models import Metric, ViewMetricsSpec
from provisa.core.schema_org import registered_tables

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def _load_registries(
    conn: "Connection",
) -> tuple[dict[str, Metric], dict[str, Any], Sequence[Mapping[str, Any]]]:
    """The metric/table/relationship registries in the shapes generate_view_metrics_sql takes.

    Tables are keyed by table_name with ``id`` = the registered_tables row id, matching the
    integer table ids the relationship rows carry (same shape as the pgwire pipeline's
    metric-query expansion)."""
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.repositories import relationship as relationship_repo
    from provisa.core.repositories import table as table_repo

    metric_rows = await metric_repo.list_all(conn)
    metrics = {
        r["name"]: Metric(
            name=r["name"],
            expression=r["expression"],
            datatype=r["datatype"],
            description=r["description"],
            ai_context=r["ai_context"],
            visible_to=list(r["visible_to"]),
            from_fact=r["from_fact"],
        )
        for r in metric_rows
    }
    tables = {
        t["table_name"]: {
            "id": t["id"],
            "columns": [c["column_name"] for c in t["columns"]],
        }
        for t in await table_repo.list_all(conn)
    }
    relationships = await relationship_repo.list_all(conn)
    return metrics, tables, relationships


async def compile_view_metrics_sql(conn: "Connection", spec: ViewMetricsSpec) -> str:
    """Compile a metric-composed view spec into its SELECT (hard error on any unknown ref)."""
    from provisa.compiler.metric_expand import generate_view_metrics_sql

    metrics, tables, relationships = await _load_registries(conn)
    return generate_view_metrics_sql(spec, metrics, tables, relationships)


async def regenerate_metric_views(conn: "Connection", metric_name: str) -> list[str]:
    """Regenerate the stored view_sql of every registered view whose spec references the metric.

    Called after a successful metric upsert: each dependent ``view_metrics`` spec recompiles
    against the UPDATED metric set and the fresh SQL is persisted. Returns the regenerated
    table names. Free-hand ``view_sql`` born from inline ``metric()`` calls carries no stored
    provenance, so it is not regenerated here (config-path views regenerate on config reload).
    """
    result = await conn.execute_core(
        select(
            registered_tables.c.id,
            registered_tables.c.table_name,
            registered_tables.c.view_metrics,
        ).where(registered_tables.c.view_metrics.is_not(None))
    )
    rows = [dict(r._mapping) for r in result.fetchall()]
    dependents = [
        (row, ViewMetricsSpec(**row["view_metrics"]))
        for row in rows
        # SQLAlchemy JSON stores Python None as JSON 'null' (not SQL NULL), so specless
        # rows can survive the SQL filter — the Python check is the authoritative one.
        if row["view_metrics"] and metric_name in row["view_metrics"]["metrics"]
    ]
    if not dependents:
        return []

    from provisa.compiler.metric_expand import generate_view_metrics_sql

    metrics, tables, relationships = await _load_registries(conn)
    regenerated: list[str] = []
    for row, spec in dependents:
        new_sql = generate_view_metrics_sql(spec, metrics, tables, relationships)
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == row["id"])
            .values(view_sql=new_sql)
        )
        regenerated.append(row["table_name"])
    return regenerated
