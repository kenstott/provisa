# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The evidence a judgment was made on (REQ-2094): the table's latest measured profile run and a
digest of it -- for a column, its ``data_type``, ``family``, ``null_share``, ``distinct_ratio``,
``plausible_type`` and confidence and its most frequent shape; for the table, its column list.
The digest keeps the comparison possible after old runs are pruned. A table no profiler profiles,
or one with no run yet, gives no evidence, and drift is then read from the first run after the
decision."""

# Requirements: REQ-2094

from __future__ import annotations

from typing import Any

from sqlalchemy import select

#: The digest's column facts, from the run's ``columns`` and ``plausible_type`` results.
COLUMN_FACTS = ("data_type", "family", "null_share", "distinct_ratio")


def _at(table: Any, schema: str | None) -> Any:
    """``table`` addressed in ``schema``; as it is (the connection's search path) when None."""
    import sqlalchemy as sa

    return table if schema is None else table.to_metadata(sa.MetaData(), schema=schema)


async def latest_run(conn: Any, schema: str | None, reg: dict) -> tuple[int, str] | None:
    """(the table's id in ``schema``, its latest measured run id), or None when no profiler
    holds the table there (its result relations exist only then) or it has no run."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler.declared import measured
    from provisa.profiler.schema import result_sa_table

    q = _at(rt, schema)
    row = (
        await conn.execute_core(
            select(q.c.id, q.c.profiler_source_id).where(
                q.c.source_id == reg["source_id"],
                q.c.schema_name == reg["schema_name"],
                q.c.table_name == reg["table_name"],
            )
        )
    ).fetchone()
    if row is None or row[1] is None:
        return None
    tid = int(row[0])
    runs = _at(result_sa_table(reg["table_name"], tid, "runs"), schema)
    found = (
        await conn.execute_core(
            select(runs.c.run_id)
            .where(runs.c.status == "succeeded", measured(runs))
            .order_by(runs.c.run_time.desc())
            .limit(1)
        )
    ).fetchone()
    return None if found is None else (tid, found[0])


async def _rows(
    conn: Any, schema: str | None, reg: dict, tid: int, kind: str, run_id: str
) -> list[dict]:
    from provisa.profiler.schema import result_sa_table

    table = _at(result_sa_table(reg["table_name"], tid, kind), schema)
    result = await conn.execute_core(select(table).where(table.c.run_id == run_id))
    return [dict(r._mapping) for r in result.fetchall()]


async def digest(conn: Any, schema: str | None, reg: dict, tid: int, run_id: str) -> dict[str, Any]:
    """Run ``run_id``'s digest of the table: its column list, and each column's facts."""
    columns = await _rows(conn, schema, reg, tid, "columns", run_id)
    plausible = {
        p["column_name"]: p for p in await _rows(conn, schema, reg, tid, "plausible_type", run_id)
    }
    shapes = {
        s["column_name"]: s["shape"]
        for s in await _rows(conn, schema, reg, tid, "shapes", run_id)
        if s["rank"] == 1
    }
    facts: dict[str, dict[str, Any]] = {}
    for c in columns:
        name = c["column_name"]
        p = plausible.get(name, {})
        facts[name] = {
            **{f: c.get(f) for f in COLUMN_FACTS},
            "plausible_type": p.get("plausible_type"),
            "confidence": p.get("confidence"),
            "top_shape": shapes.get(name),
        }
    return {"columns": sorted(facts), "facts": facts}


async def evidence(
    conn: Any, schema: str | None, reg: dict, column: str | None = None
) -> dict[str, Any]:
    """The evidence a judgment on ``reg`` (``column`` of it, when given) is made on now: the run
    id and that run's digest -- the column's facts and the table's column list, or the column
    list alone for a table-level judgment. Empty when there is no run."""
    found = await latest_run(conn, schema, reg)
    if found is None:
        return {}
    tid, run_id = found
    d = await digest(conn, schema, reg, tid, run_id)
    out: dict[str, Any] = {"run_id": run_id, "columns": d["columns"]}
    if column is not None:
        out["column"] = d["facts"].get(column)
    return out
