# Copyright (c) 2026 Kenneth Stott
# Canary: 35a64d3e-5482-44b2-93d3-41778cbc1b8b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Measured values and shares for faked reads (REQ-1494, MEASURED FROM THE PROFILE, ELSE FROM THE
TABLE). A fake that takes what it shows from measurement -- categories(), bool() with no share,
profile(), pattern(), and after, before, greater_than and less_than with no distance -- reads it
from the column's latest profile run, else from the table itself, read as the organisation's
administrator through the governed pipeline as a profile is. The measurements are taken when the
model is built and bound to the column's fake masking rule; a faked read computes from them.

A measurement that cannot be taken is held as a refusal, named, and a faked read of that column
refuses with it.
"""

# Requirements: REQ-1494

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from provisa.fakes.measurement import Measured
from provisa.fakes.kinds import Bool, Categories, FakeKind, Ordered, Pattern, Profile, kind_name

log = logging.getLogger(__name__)

#: The quantiles a measured difference is read at, for after, before, greater_than and less_than.
DISTANCE_POINTS: tuple[float, ...] = tuple(round(i / 20, 2) for i in range(21))


def needs(kind: FakeKind) -> bool:
    """Whether ``kind`` computes from measurement."""
    if isinstance(kind, Bool):
        return kind.share is None
    if isinstance(kind, Categories):
        return kind.values is None
    if isinstance(kind, Ordered):
        return kind.distance is None
    return isinstance(kind, (Profile, Pattern))


def _shares(counts: list[tuple[str | None, int]]) -> tuple[tuple[str, float], ...]:
    """Non-null values and their shares of the non-null rows, most frequent first."""
    held = sorted(((v, n) for v, n in counts if v is not None and n > 0), key=lambda x: -x[1])
    total = sum(n for _, n in held)
    return tuple((v, n / total) for v, n in held)


def _true_share(counts: list[tuple[str | None, int]]) -> float:
    held = [(v, n) for v, n in counts if v is not None]
    total = sum(n for _, n in held)
    if total == 0:
        return 0.5  # REQ-1494: bool() "is even on an empty table"
    return sum(n for v, n in held if v.lower() in ("true", "t", "1")) / total


def from_counts(column: str, kind: FakeKind, counts: list[tuple[str | None, int]]) -> Measured:
    """bool() or categories() measured from the column's values and their counts."""
    if isinstance(kind, Bool):
        return Measured(true_share=_true_share(counts))
    assert isinstance(kind, Categories)  # the only other kind measured by counting values
    values = _shares(counts)
    if not values:
        return Measured(
            refused=f"{column}: categories() takes its values from the table, which holds none; "
            f"declare them, as categories((a, b))"
        )
    return Measured(values=values)


def from_profile(column: str, kind: FakeKind, profiled: Any, run_id: str) -> Measured:
    """bool(), categories(), profile() or pattern() from a profile run's column (a
    provisa.synthetic.plan.ProfiledColumn). Returns None-free refusals where the run lacks what the
    kind reads."""
    freq = list(profiled.frequencies)
    if isinstance(kind, (Bool, Categories)):
        return replace(from_counts(column, kind, freq), run_id=run_id)
    if isinstance(kind, Pattern):
        shapes = _shares([(s, n) for s, n in profiled.shapes])
        if not shapes:
            return Measured(refused=f"{column}: profile run {run_id} recorded no value shapes")
        return Measured(run_id=run_id, shapes=shapes)
    assert isinstance(kind, Profile)  # the only other kind a profile run measures
    # REQ-1494: "A column with few distinct values draws from the profile's value frequencies
    # rather than its sketch" -- the run records the full frequency table only for those.
    if freq:
        values = _shares(freq)
        if values:
            return Measured(run_id=run_id, values=values)
    if profiled.sketch is None:
        return Measured(
            refused=f"{column}: profile run {run_id} recorded no distribution of the column"
        )
    n = len(profiled.sketch) - 1
    return Measured(
        run_id=run_id, points=tuple((i / n, float(v)) for i, v in enumerate(profiled.sketch))
    )


def from_distance(column: str, kind: Ordered, quantiles: list[float | None] | None) -> Measured:
    """after/before/greater_than/less_than with the measured difference's quantiles."""
    if quantiles is None or any(q is None for q in quantiles):
        return Measured(
            refused=f"{column}: {kind.kind}({kind.column}) takes the measured difference between "
            f"the two columns, and the table holds no row with both; declare a distance"
        )
    return Measured(
        points=tuple(zip(DISTANCE_POINTS, (float(q) for q in quantiles if q is not None)))
    )


# -- measuring the model ------------------------------------------------------------------------


def _epoch(expr: str, family: str) -> str:
    if family in ("date", "timestamp"):
        return f"EXTRACT(EPOCH FROM CAST({expr} AS TIMESTAMP))"
    return f"CAST({expr} AS DOUBLE PRECISION)"


async def _latest_run(conn: Any, schema: str, table_name: str, tid: int) -> str | None:
    from sqlalchemy import select

    from provisa.profiler.schema import result_sa_table
    from provisa.synthetic.run import _qualified

    from provisa.profiler.declared import measured

    runs = _qualified(result_sa_table(table_name, tid, "runs"), schema)
    row = (
        await conn.execute_core(
            select(runs.c.run_id)
            .where(runs.c.status == "succeeded", measured(runs))  # REQ-1942: what the data holds
            .order_by(runs.c.run_time.desc())
            .limit(1)
        )
    ).fetchone()
    return None if row is None else row[0]


async def _profiled_table_id(conn: Any, schema: str, reg: dict) -> int | None:
    """The table's id in ``schema`` when it is a profiler's member there (its result relations
    exist only then, provisa.profiler.registration), else None."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables as rt
    from provisa.synthetic.run import _qualified

    q = _qualified(rt, schema)
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
    return int(row[0])


async def _measure_table(
    state: Any, conn: Any, org_id: str, reg: dict, faked: dict[str, tuple[FakeKind, str]]
) -> dict[str, Measured]:
    """Each of ``faked`` (column -> (kind, family)) of table ``reg``, measured."""
    from provisa.core.environments import org_schema
    from provisa.core.request_context import current_env
    from provisa.profiler.run import PROFILE_ROLE, _governed, resolve_target
    from provisa.profiler.statement import _ident, qualified
    from provisa.synthetic.plan import DatasetRefused
    from provisa.synthetic.run import read_profile

    out: dict[str, Measured] = {}
    env = current_env.get()
    schema = org_schema(org_id, env)
    target = resolve_target(state, reg["id"], reg["table_name"], {})
    exposed = {c.physical: c.name for c in target.columns}
    table = qualified(target.pgwire_name)

    runs: dict[str | None, Any] = {}

    async def profile(run_id: str | None) -> Any:
        """The run ``run_id`` (the latest when None), or None when there is none."""
        if run_id not in runs:
            # REQ-1494: with no profile run, the values come from the table itself.
            tid = await _profiled_table_id(conn, schema, reg)
            if tid is None:  # not a profiler's member in this environment: it holds no run of it
                runs[run_id] = None
                return None
            rid = (
                run_id
                if run_id is not None
                else await _latest_run(conn, schema, reg["table_name"], tid)
            )
            if rid is None:
                runs[run_id] = None
            else:
                try:
                    runs[run_id] = await read_profile(
                        conn, org_id=org_id, env=env, reg=reg, run_id=rid
                    )
                except DatasetRefused:  # the pinned run is not one this environment holds
                    runs[run_id] = None
        return runs[run_id]

    async def profiled_column(run_id: str | None, column: str) -> tuple[str, Any] | None:
        """(run id, the run's ProfiledColumn) of ``column``, or None when no run holds it."""
        run = await profile(run_id)
        if run is None or column not in run.columns:
            return None
        return run.run_id, run.columns[column]

    for column, (kind, _family) in faked.items():
        name = exposed.get(column)
        if name is None:
            out[column] = Measured(
                refused=f"{column}: not readable by {PROFILE_ROLE}, so it cannot be measured"
            )
            continue
        if isinstance(kind, (Profile, Pattern)):
            pinned = kind.run if isinstance(kind, Profile) else None
            found = await profiled_column(pinned, column)
            if found is None:
                what = f"profile run {pinned}" if pinned else "a profile run"
                out[column] = Measured(
                    refused=f"{column}: {kind_name(kind)}() reads {what} of the column, and this "
                    f"environment holds none"
                )
            else:
                out[column] = from_profile(column, kind, found[1], found[0])
            continue
        if isinstance(kind, (Bool, Categories)):
            found = await profiled_column(None, column)
            # The profile's full value-frequency table, else the table itself (REQ-1494).
            if found is not None and found[1].frequencies:
                out[column] = from_profile(column, kind, found[1], found[0])
                continue
            value = f"CAST(x.{_ident(name)} AS TEXT)"
            _names, rows = await _governed(
                f"SELECT {value} AS v, COUNT(*) AS n FROM {table} x GROUP BY {value}"
            )
            out[column] = from_counts(column, kind, [(v, int(n)) for v, n in rows])
            continue
        assert isinstance(kind, Ordered)  # the last kind that is measured
        other = exposed.get(kind.column)
        if other is None:
            out[column] = Measured(
                refused=f"{column}: {kind.column} is not readable by {PROFILE_ROLE}, so the "
                f"difference cannot be measured"
            )
            continue
        quantiles = await difference_quantiles(table, name, other, faked[column][1])
        out[column] = from_distance(column, kind, quantiles)
    return out


async def difference_quantiles(
    table: str, column: str, other: str, family: str
) -> list[float | None] | None:
    """The quantiles (:data:`DISTANCE_POINTS`) of ``column`` less ``other`` (in seconds for dates
    and times) over the rows holding both, read as the org admin through the governed pipeline;
    None where no row holds both. ``table`` is qualified, the columns as the org admin reads them."""
    from provisa.profiler.run import _governed
    from provisa.profiler.statement import _ident

    d = f"({_epoch(f'x.{_ident(column)}', family)} - {_epoch(f'x.{_ident(other)}', family)})"
    points = "ARRAY[" + ", ".join(f"{q:.2f}" for q in DISTANCE_POINTS) + "]"
    _names, rows = await _governed(
        f"SELECT PERCENTILE_CONT({points}) WITHIN GROUP (ORDER BY {d}) AS q FROM {table} x "
        f"WHERE {d} IS NOT NULL"
    )
    quantiles = rows[0][0] if rows else None
    return None if quantiles is None else list(quantiles)


async def measure_model(state: Any) -> None:
    """Measure every faked column whose fake computes from measurement, and publish the masking
    rules with the measurements bound, in one assignment (REQ-1914)."""
    from provisa.fakes.checks import family
    from provisa.fakes.kinds import parse
    from provisa.security.masking import MaskType

    wanted: dict[int, dict[str, tuple[FakeKind, str]]] = {}
    for (table_id, _role), col_map in state.masking_rules.items():
        for column, (rule, dtype) in col_map.items():
            if rule.mask_type != MaskType.fake:
                continue
            kind = parse(rule.fake)
            if needs(kind):
                wanted.setdefault(table_id, {})[column] = (kind, family(dtype))
    if not wanted:
        return
    from provisa.fakes.measurement import MEASURING

    org_id = state.org_id  # the deployment's org, whose model this is
    measured: dict[tuple[int, str], Measured] = {}
    token = MEASURING.set(True)
    try:
        await _measure_wanted(state, org_id, wanted, measured)
    finally:
        MEASURING.reset(token)
    state.masking_rules = {
        key: {
            column: (
                (replace(rule, fake_measured=measured[(key[0], column)]), dtype)
                if (key[0], column) in measured
                else (rule, dtype)
            )
            for column, (rule, dtype) in col_map.items()
        }
        for key, col_map in state.masking_rules.items()
    }


async def _measure_wanted(
    state: Any,
    org_id: str,
    wanted: dict[int, dict[str, tuple[FakeKind, str]]],
    measured: dict[tuple[int, str], Measured],
) -> None:
    from provisa.api.admin.db_queries import fetch_tables

    async with state.model_db.acquire() as conn:
        regs = {t["id"]: t for t in await fetch_tables(conn)}
        for table_id, faked in wanted.items():
            try:
                taken = await _measure_table(state, conn, org_id, regs[table_id], faked)
            except Exception as e:  # noqa: BLE001 -- held as each column's named refusal
                log.warning("fake measurement of table %s failed: %s", table_id, e)
                taken = {c: Measured(refused=f"{c}: could not be measured: {e}") for c in faked}
            for column, m in taken.items():
                measured[(table_id, column)] = m
