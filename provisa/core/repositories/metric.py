# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c9a72-8b64-4e05-9d21-c47a5e80f6b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Metric repository — CRUD for governed metric definitions in the control plane (REQ-1317).

A metric is a named aggregate expression with no grain of its own; ``from_fact`` marks one
auto-registered from a fact spec's measure (REQ-1320). Every write path validates the
expression here — an unparsable or non-aggregate expression is a hard error, never stored.
"""

# Requirements: REQ-1317, REQ-1319, REQ-1320

from typing import TYPE_CHECKING

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError
from sqlalchemy import delete as _delete, select

from provisa.core.models import Metric
from provisa.core.schema_org import metrics

if TYPE_CHECKING:
    from provisa.core.database import Connection


def validate_expression(expression: str) -> None:  # REQ-1317
    """A metric expression must parse as SQL and contain at least one aggregate function."""
    try:
        tree = sqlglot.parse_one(expression)
    except SqlglotError as e:
        raise ValueError(f"metric expression {expression!r} does not parse: {e}") from e
    if not any(isinstance(node, exp.AggFunc) for node in tree.walk()):
        raise ValueError(
            f"metric expression {expression!r} must contain at least one aggregate function"
        )


async def upsert(conn: "Connection", metric: Metric) -> None:  # REQ-1317, REQ-1320
    """Upsert a metric by name. The expression is validated on every write (hard error)."""
    validate_expression(metric.expression)
    vals = {
        "name": metric.name,
        "expression": metric.expression,
        "datatype": metric.datatype,
        "description": metric.description,
        "ai_context": metric.ai_context,  # REQ-1319
        "visible_to": list(metric.visible_to),
        "from_fact": metric.from_fact,  # REQ-1320
    }
    await conn.upsert(
        metrics,
        vals,
        index_elements=["name"],
        update_columns=[
            "expression",
            "datatype",
            "description",
            "ai_context",
            "visible_to",
            "from_fact",
        ],
    )


async def get(conn: "Connection", name: str) -> dict | None:  # REQ-1317
    result = await conn.execute_core(select(metrics).where(metrics.c.name == name))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-1317
    result = await conn.execute_core(select(metrics).order_by(metrics.c.name))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", name: str) -> bool:  # REQ-1317
    result = await conn.execute_core(_delete(metrics).where(metrics.c.name == name))
    return (result.rowcount or 0) > 0
