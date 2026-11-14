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

from provisa.core import model_change
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError
from sqlalchemy import delete as _delete, select, update

from provisa.core.models import Metric
from provisa.core.repositories import retirement
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard
from provisa.core.schema_org import glossary_terms, metrics

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
    conn: "Connection", metric: Metric
) -> None:
    """Upsert a metric by name. The expression is validated on every write (hard error)."""
    model_change.name("upsert", "metric", metric.name)  # REQ-1524
    validate_expression(metric.expression)
    # REQ-2248: a retired metric keeps its name; a save under it would bring it back.
    held = (
        await conn.execute_core(select(metrics.c.retired).where(metrics.c.name == metric.name))
    ).fetchone()
    if held is not None and held.retired:
        raise retirement.RetiredNameTaken("metric", metric.name)
    term_id = await _term_id(conn, metric) if metric.term is not None else None
    vals = {
        "name": metric.name,
        "expression": metric.expression,
        "datatype": metric.datatype,
        "description": metric.description,
        "ai_context": metric.ai_context,  # REQ-1319
        "visible_to": list(metric.visible_to),
        "from_fact": metric.from_fact,  # REQ-1320
        "term_id": term_id,  # REQ-2243
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
            "term_id",
        ],
    )


async def _term_id(conn: "Connection", metric: Metric) -> int:  # REQ-2243
    """The id of the glossary term ``metric`` names; a name no term holds is refused."""
    row = (
        await conn.execute_core(
            select(glossary_terms.c.id).where(glossary_terms.c.name == metric.term)
        )
    ).fetchone()
    if row is None:
        raise ValueError(
            f"metric {metric.name!r} names the glossary term {metric.term!r}, which does not exist"
        )
    return row.id


def _with_term():
    """Each metric with the name of the glossary term it names (REQ-2243), None when none."""
    return select(metrics, glossary_terms.c.name.label("term")).outerjoin(
        glossary_terms, glossary_terms.c.id == metrics.c.term_id
    )


async def get(conn: "Connection", name: str) -> dict | None:  # REQ-1317
    result = await conn.execute_core(_with_term().where(metrics.c.name == name))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-1317
    result = await conn.execute_core(_with_term().order_by(metrics.c.name))
    return [dict(r._mapping) for r in result.fetchall()]


class MetricDeleteRefused(Exception):
    """A metric that may not be deleted because views use it; ``dependents`` lists them."""

    def __init__(self, name: str, dependents: list[Dependent]) -> None:
        self.name = name
        self.dependents = dependents
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = f"Metric {name!r} is still used by"
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"{self.lead}: {named}")


async def delete(  # REQ-1317, REQ-1918, REQ-2248
    conn: "Connection", name: str, *, reason: str | None = None
) -> bool:
    """Delete one metric: THE delete, for every surface. It RETIRES the metric (REQ-2248): the
    row, its name and its tag assignments are kept, the metric marked retired with ``reason``,
    the acting person and the time. False when there is no such metric, or it is retired already.

    Refused (:class:`MetricDeleteRefused`), naming each, while a view is composed from it (its
    ``view_metrics`` lists the metric) or a view's SQL reads it as ``metrics.<name>``. One
    transaction."""
    model_change.name("retire", "metric", name)  # REQ-1524
    ref = ObjectRef("metric", name)
    async with conn.transaction():
        held = await get(conn, name)
        if held is None or held["retired"]:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise MetricDeleteRefused(name, blocking)
        await conn.execute_core(
            update(metrics).where(metrics.c.name == name).values(**retirement.values(reason))
        )
    return True


async def remove_where(conn: "Connection", *where) -> None:
    """Remove every metric matching ``where`` (clauses on ``metrics``) — for the config loader,
    which declares metrics as a set and replaces that set. It is not a deletion of one object
    and does not ask the dependency guard."""
    await conn.execute_core(_delete(metrics).where(*where))
