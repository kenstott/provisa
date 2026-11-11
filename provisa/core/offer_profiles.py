# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A product's profiles as its reader sees the data (REQ-2180, A PROFILE SHOWN IS SANITIZED).

Each dataset's latest profile is read as the profiler wrote it and sanitized here, on the server,
for the reader's roles, by the rule the profile runs view applies (provisa.profiler.governance
``safe_run``): a row speaking of a column the reader is not served is left out, and a value-bearing
row speaking of a column masked to them or tagged PII is left out (no frequent values, quantiles,
histograms or samples). Of such a column's ``columns`` row the reader sees its type, null rate and
distinct count, and no value (no minimum, maximum or moments).
"""

# Requirements: REQ-2180

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.profiler.governance import ColumnRule

#: What a masked or PII-tagged column's ``columns`` row shows besides what ``safe_run`` keeps.
COUNTS = ("distinct_count",)


@dataclass(frozen=True)
class Profile:
    """A dataset's latest profile as one reader sees it: its run (a ProfileRun) and its results
    by kind (ProfileRunResults)."""

    run: dict[str, Any]
    results: dict[str, list[dict[str, Any]]]


def sanitized(
    results: dict[str, list[dict[str, Any]]], rules: "list[ColumnRule]", roles: frozenset[str]
) -> dict[str, list[dict[str, Any]]]:
    """``results`` as ``roles`` may see them: ``safe_run``'s view, a restricted column's
    ``columns`` row keeping its distinct count."""
    from provisa.profiler.governance import safe_run

    safe = safe_run(results, rules, roles)
    restricted = {r.exposed for r in rules if r.seen_by(roles) and r.restricted_for(roles)}
    counts = {
        row["column_name"]: {k: row.get(k) for k in COUNTS} for row in results.get("columns", [])
    }
    if "columns" in safe:
        safe["columns"] = [
            {**row, **counts[row["column_name"]]} if row["column_name"] in restricted else row
            for row in safe["columns"]
        ]
    return safe


def _jsonable(row: Any) -> dict[str, Any]:
    return {
        k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(row._mapping).items()
    }


async def latest(
    conn: "Connection", state: Any, table_id: int, roles: frozenset[str]
) -> Profile | None:
    """``table_id``'s latest successful measured profile, sanitized for ``roles``; None when the
    dataset has no profiler or no such run."""
    from provisa.core.schema_org import registered_tables
    from provisa.profiler.governance import column_rules
    from provisa.profiler.history import measured, relation
    from provisa.profiler.schema import RESULT_KINDS

    member = (
        await conn.execute_core(
            select(registered_tables.c.table_name, registered_tables.c.profiler_source_id).where(
                registered_tables.c.id == table_id
            )
        )
    ).fetchone()
    if member is None or member.profiler_source_id is None:
        return None
    runs = await relation(conn, member.table_name, table_id, "runs")
    run = (
        await conn.execute_core(
            select(runs)
            .where(runs.c.status == "succeeded", measured(runs))
            .order_by(runs.c.run_time.desc())
            .limit(1)
        )
    ).fetchone()
    if run is None:
        return None
    run_id = run._mapping["run_id"]
    results: dict[str, list[dict[str, Any]]] = {}
    for kind in RESULT_KINDS:
        rel = await relation(conn, member.table_name, table_id, kind)
        rows = await conn.execute_core(select(rel).where(rel.c.run_id == run_id))
        results[kind] = [_jsonable(r) for r in rows.fetchall()]
    rules = await column_rules(conn, state, table_id)
    return Profile(run=_jsonable(run), results=sanitized(results, rules, roles))
