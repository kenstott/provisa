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
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.repositories.origin import require as require_origin
from provisa.core.repositories.origin import take_over
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


async def upsert(  # REQ-1317, REQ-1320, REQ-1919
    conn: "Connection", metric: Metric, *, origin: str
) -> None:
    """Upsert a metric by name. The expression is validated on every write (hard error).
    ``origin`` says where it comes from (``repositories.origin``): written when the metric is
    CREATED and left alone after, except that a config load takes over an admin-made one."""
    require_origin(origin)
    validate_expression(metric.expression)
    vals = {
        "name": metric.name,
        "expression": metric.expression,
        "datatype": metric.datatype,
        "description": metric.description,
        "ai_context": metric.ai_context,  # REQ-1319
        "visible_to": list(metric.visible_to),
        "from_fact": metric.from_fact,  # REQ-1320
        "origin": origin,  # REQ-1919: on INSERT only
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
    await take_over(
        conn,
        metrics,
        (metrics.c.name == metric.name,),
        kind="metric",
        ident=metric.name,
        origin=origin,
    )


async def get(conn: "Connection", name: str) -> dict | None:  # REQ-1317
    result = await conn.execute_core(select(metrics).where(metrics.c.name == name))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-1317
    result = await conn.execute_core(select(metrics).order_by(metrics.c.name))
    return [dict(r._mapping) for r in result.fetchall()]


class MetricDeleteRefused(Exception):
    """A metric that may not be deleted because views use it; ``dependents`` lists them."""

    def __init__(self, name: str, dependents: list[Dependent]) -> None:
        self.name = name
        self.dependents = dependents
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"Metric {name!r} is still used by: {named}")


async def delete(conn: "Connection", name: str) -> bool:  # REQ-1317, REQ-1918
    """Delete one metric: THE delete, for every surface. False when there is no such metric.

    Refused (:class:`MetricDeleteRefused`), naming each, while a view is composed from it (its
    ``view_metrics`` lists the metric) or a view's SQL reads it as ``metrics.<name>``. One
    transaction; no database cascade is relied on."""
    ref = ObjectRef("metric", name)
    async with conn.transaction():
        if await get(conn, name) is None:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise MetricDeleteRefused(name, blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(metrics).where(metrics.c.name == name))
    return True


async def remove_where(conn: "Connection", *where) -> None:
    """Remove every metric matching ``where`` (clauses on ``metrics``) — for the config loader,
    which declares metrics as a set and replaces that set. It is not a deletion of one object
    and does not ask the dependency guard."""
    await conn.execute_core(_delete(metrics).where(*where))
