# Copyright (c) 2026 Kenneth Stott
# Canary: 2a9f5e13-c76b-48d1-9e3a-b4f0d8c2a671
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One profile run of one member table (REQ-1934).

The run reads AS THE ORG ADMIN through the one governed pipeline (``_govern_and_route`` then
``_execute_plan``): the rules that bind the org admin -- its masks, its row rules, a region rule once
regions exist -- apply to what is profiled, and no reader's rules do. The profile is what the org
admin reads; who reads the profile is decided by the result tables' own grants once registered.

Two governed statements per run: the table's row count, which sizes the sample, and the one profile
statement (``statement.profile_sql``). Everything else is computed from its aggregates
(``measures``, ``plausible``) and appended to the member table's result relations (``schema``),
written straight into the org's control-plane schema as an ingest table's rows are (REQ-1771).

REQ-1921 (regions) is not on this branch: when it lands, the region of the node running the profile
is passed to ``_govern_and_route`` as the org admin's region attribute in :func:`_governed`, and
recorded in the runs row's ``region``, which is NULL until then.
"""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.schema import CreateTable

from provisa.profiler import measures, plausible
from provisa.profiler.source import ProfilerSettings
from provisa.profiler.schema import (
    RESULT_KINDS,
    field_names,
    result_sa_table,
)
from provisa.profiler.statement import (
    QUANTILE_POINTS,
    TOP_N,
    ColumnSpec,
    FanoutSpec,
    KeySpec,
    ProfileAggregates,
    Sample,
    count_sql,
    family_of,
    is_integer_key,
    key_bounds_sql,
    key_ranges,
    parse_profile_result,
    profile_sql,
)

log = logging.getLogger(__name__)

# The role every profile is read as (REQ-1934 amendment ONE READER: THE ORG ADMIN).
PROFILE_ROLE = "org_admin"


class ProfileError(Exception):
    """A profile run could not read or profile its table."""


@dataclass(frozen=True)
class Target:
    table_id: int
    table_name: str  # the registered name the result relations are named after
    pgwire_name: str  # domain.table as the org admin reads it
    columns: list[ColumnSpec]
    fanouts: list[FanoutSpec]
    tags: dict[str, set[str]]  # column -> base tag ids
    meta: Any  # the org admin's TableMeta: source, physical address
    key: str | None  # the single-column integer primary key, as published; None when there is none
    keys: list[KeySpec]  # declared primary and unique keys whose columns the org admin reads


@dataclass(frozen=True)
class RunOutcome:
    run_id: str
    run_time: datetime
    status: str
    error: str | None
    row_count: int | None
    profiled_rows: int | None


def resolve_target(state: Any, table_id: int, table_name: str, tags: dict[str, set[str]]) -> Target:
    """The member table as the org admin's compiled context publishes it."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    ctx = state.contexts.get(PROFILE_ROLE)
    if ctx is None:
        raise ProfileError(f"no compiled schema for role {PROFILE_ROLE!r}; cannot profile")
    metas = {tm.table_id: tm for tm in ctx.tables.values()}
    tm = metas.get(table_id)
    if tm is None:
        raise ProfileError(
            f"table {table_name!r} (id {table_id}) is not readable by {PROFILE_ROLE}; nothing "
            f"to profile"
        )

    def _name(meta: Any) -> str:
        domain = domain_to_sql_name(meta.domain_id or meta.schema_name or "public")
        return f"{domain}.{semantic_table_name(meta)}"

    p2s: dict = ctx.physical_to_sql
    col_types = state.schema_build_cache["column_types"][table_id]
    columns: list[ColumnSpec] = []
    exposed_by_phys: dict[str, str] = {}
    for col in col_types:
        exposed = p2s.get((table_id, col.column_name))
        if exposed is None:
            continue  # a column the org admin cannot see is not part of what it reads
        exposed_by_phys[col.column_name] = exposed
        columns.append(
            ColumnSpec(exposed, col.data_type, family_of(col.data_type), col.column_name)
        )

    fanouts: list[FanoutSpec] = []
    for (type_name, field_name), jm in ctx.joins.items():
        if type_name != tm.type_name or jm.cardinality != "one-to-many" or jm.via is not None:
            continue
        if jm.source_expr or jm.target_expr or jm.source_constant is not None or jm.source_json_key:
            continue  # computed edges have no child key column to count by
        parent_key = exposed_by_phys.get(jm.source_column)
        child_key = p2s.get((jm.target.table_id, jm.target_column))
        if parent_key is None or child_key is None:
            continue  # the org admin cannot read the join's keys
        fanouts.append(FanoutSpec(field_name, _name(jm.target), parent_key, child_key))
    pk = ctx.pk_columns.get(table_id, [])
    key_spec = next((c for c in columns if c.physical == pk[0]), None) if len(pk) == 1 else None
    declared = ([("primary key", pk)] if pk else []) + [
        (name, cols) for name, cols in ctx.unique_constraints.get(table_id, [])
    ]
    keys = []
    for name, cols in declared:
        exposed = [exposed_by_phys.get(c) for c in cols]
        if all(e is not None for e in exposed):  # a key the org admin cannot read is not counted
            keys.append(KeySpec(name, tuple(e for e in exposed if e is not None)))
    return Target(
        table_id=table_id,
        table_name=table_name,
        pgwire_name=_name(tm),
        columns=columns,
        fanouts=fanouts,
        tags={exposed_by_phys[c]: t for c, t in tags.items() if c in exposed_by_phys},
        meta=tm,
        key=key_spec.name if key_spec is not None and is_integer_key(key_spec) else None,
        keys=keys,
    )


async def _route(sql: str) -> Any:
    """The governed, routed plan of ``sql`` as the org admin -- the one pipeline's first half."""
    from provisa.pgwire._pipeline import _govern_and_route

    return await _govern_and_route(sql, PROFILE_ROLE)


async def _execute(plan: Any) -> tuple[list[str], list[tuple]]:
    from provisa.api.app import state
    from provisa.pgwire._pipeline import _execute_plan

    result = await _execute_plan(plan, state)
    return list(result.column_names), [tuple(r) for r in result.rows]


async def _governed(sql: str) -> tuple[list[str], list[tuple]]:
    return await _execute(await _route(sql))


def _executed_sql(plan: Any) -> str:
    """The text the plan's terminal runs: engine-physical on the ENGINE route, else the source's."""
    from provisa.transpiler.router import Route

    return plan.physical_sql if plan.route == Route.ENGINE else plan.sql


def sample_fraction(
    row_count: int, column_count: int, sample_above_cells: int | None
) -> float | None:
    """The fraction to sample, or None for the whole table. The budget is in cells -- rows times
    profiled columns -- because the profile's work grows with both, so a wide table samples fewer
    rows than a narrow one under the same budget."""
    cells = row_count * column_count
    if sample_above_cells is None or cells <= sample_above_cells:
        return None
    return sample_above_cells / cells


def result_rows(
    target: Target,
    agg: ProfileAggregates,
    run_id: str,
    run_time: datetime,
    low_cardinality_max: int,
) -> dict[str, list[dict]]:
    """Every result relation's rows for one run, except ``runs``."""
    key = {"run_id": run_id, "run_time": run_time}
    # ``runs`` and ``drift`` are written by profile_table: the run's own row, and its comparison
    # with the runs before it.
    out: dict[str, list[dict]] = {
        kind: [] for kind in RESULT_KINDS if kind not in ("runs", "drift")
    }
    rows = agg.profiled_rows
    for col in agg.columns:
        name = col.spec.name
        m = measures.moments(col)
        out["columns"].append(
            {
                **key,
                "column_name": name,
                "physical_column": col.spec.physical,
                "data_type": col.spec.data_type,
                "family": col.spec.family,
                "row_count": rows,
                "null_count": rows - col.non_null,
                "null_share": (rows - col.non_null) / rows if rows else None,
                "distinct_count": col.distinct,
                "distinct_ratio": col.distinct / rows if rows else None,
                "min_value": col.min_text,
                "max_value": col.max_text,
                "mean": m.mean,
                "stddev": m.stddev,
                "variance": m.variance,
                "skewness": m.skewness,
                "kurtosis": m.kurtosis,
                "log_mean": m.log_mean,
                "log_variance": m.log_variance,
                "integer_only": m.integer_only,
                "zero_share": m.zero_share,
                "length_min": col.length_min,
                "length_max": col.length_max,
            }
        )
        for measure, sketch in (("value", col.quantiles), ("length", col.length_quantiles)):
            if sketch is None:
                continue
            out["quantiles"] += [
                {**key, "column_name": name, "measure": measure, "q": q, "value": v}
                for q, v in zip(QUANTILE_POINTS, sketch)
            ]
        if col.quantiles is not None:
            out["histogram"] += [
                {**key, "column_name": name, "bucket": b, "lo": lo, "hi": hi, "row_count": n}
                for b, lo, hi, n in measures.histogram(col.quantiles, col.non_null)
            ]
        out["top_values"] += [
            {**key, "column_name": name, "kind": "top", "rank": r, "value": v, "row_count": n}
            for r, (v, n) in enumerate(col.values[:TOP_N], 1)
        ]
        if col.distinct + (1 if col.non_null < rows else 0) <= low_cardinality_max:
            out["top_values"] += [
                {
                    **key,
                    "column_name": name,
                    "kind": "frequency",
                    "rank": r,
                    "value": v,
                    "row_count": n,
                }
                for r, (v, n) in enumerate(col.values, 1)
            ]
        out["shapes"] += [
            {**key, "column_name": name, "rank": r, "shape": s, "row_count": n}
            for r, (s, n) in enumerate(col.shapes[:TOP_N], 1)
        ]
        for rank, fit in enumerate(measures.fits(col, m), 1):
            out["fits"] += [
                {**key, "column_name": name, "family": fit.family, "param": p, "value": v}
                for p, v in fit.params.items()
            ]
            out["fit_quality"].append(
                {
                    **key,
                    "column_name": name,
                    "family": fit.family,
                    "ks_stat": fit.ks_stat,
                    "rank": rank,
                }
            )
        label = plausible.infer(col, target.tags.get(name, set()), low_cardinality_max)
        out["plausible_type"].append(
            {
                **key,
                "column_name": name,
                "plausible_type": label.plausible_type,
                "confidence": label.confidence,
                "evidence": label.evidence,
            }
        )
    for dups in [agg.rows, *agg.keys]:
        out["duplicates"].append(
            {
                **key,
                "subject": "row" if dups.key is None else "key",
                "key_name": None if dups.key is None else dups.key.name,
                "involved_columns": json.dumps([] if dups.key is None else list(dups.key.columns)),
                "repeated_values": dups.repeated,
                "extra_rows": dups.extra,
                "extra_share": dups.extra / rows if rows else None,
            }
        )
    out["repeats"] += [
        {**key, "rank": r, "row_count": n} for r, n in enumerate(agg.rows.top_counts, 1)
    ]
    for fan in agg.fanouts:
        out["fanout_runs"].append(
            {
                **key,
                "relationship": fan.spec.relationship,
                "child_table": fan.spec.child_table,
                "parents": fan.parents,
                "mean": fan.mean,
                "max": fan.max,
                "childless_share": fan.childless / fan.parents if fan.parents else None,
            }
        )
        if fan.quantiles is not None:
            out["fanout"] += [
                {**key, "relationship": fan.spec.relationship, "q": q, "value": v}
                for q, v in zip(QUANTILE_POINTS, fan.quantiles)
            ]
    for kind, kind_rows in out.items():
        names = field_names(kind)
        for r in kind_rows:
            assert tuple(r) == names, f"{kind} row drifted from the shipped schema"
    return out


async def write_results(
    conn: Any, table_name: str, table_id: int, rows: dict[str, list[dict]]
) -> None:
    """Append one run's rows, creating any result relation not yet there."""
    for kind in RESULT_KINDS:
        table = result_sa_table(table_name, table_id, kind)
        await conn.execute_core(CreateTable(table, if_not_exists=True))
        await conn.execute_core_many(table.insert(), rows.get(kind, []))


async def column_tags(conn: Any, table_id: int) -> dict[str, set[str]]:
    from provisa.core.schema_org import tag_assignments as ta

    result = await conn.execute_core(
        select(ta.c.column_name, ta.c.base_tag_id).where(
            ta.c.object_type == "column", ta.c.table_id == table_id
        )
    )
    tags: dict[str, set[str]] = {}
    for column_name, base_tag_id in result.fetchall():
        tags.setdefault(column_name, set()).add(base_tag_id)
    return tags


async def _comparison(
    conn: Any, table_name: str, table_id: int, run_time: datetime, results: dict[str, list[dict]]
) -> list[dict]:
    """This run's drift rows: its comparison with the table's previous successful run (REQ-1934),
    recorded with it as a measure of the run. Sets the runs row's ``previous_run_id``."""
    from provisa.profiler import compare
    from provisa.profiler.history import MEASURE_KINDS, previous_runs, run_results

    (run,) = results["runs"]
    found = await previous_runs(conn, table_name, table_id, run_time, 1)
    previous = None
    if found:
        prev_id = found[0][0]
        stored = await run_results(conn, table_name, table_id, [prev_id], MEASURE_KINDS)
        previous = compare.measures_of(stored[prev_id])
        run["previous_run_id"] = prev_id
    rows = compare.compare(compare.measures_of(results), previous)
    key = {"run_id": run["run_id"], "run_time": run["run_time"]}
    return [{**key, "previous_run_id": run["previous_run_id"], **r} for r in rows]


@dataclass(frozen=True)
class ProfileRead:
    """What the profile statement read and how (REQ-1934)."""

    agg: ProfileAggregates
    method: str  # statement.SAMPLE_METHODS
    attempts: list[dict]  # each sample read: {"percent": ..., "rows": ...}; empty when read whole


async def _run_sample(
    sql: str, sample: Sample, route: Any, reach: Any, target: Target
) -> ProfileAggregates:
    """Route, check and run one sampled profile statement."""
    from provisa.profiler.sampling import SampleClauseLost, require_sample_clause

    plan = await _route(sql)
    if plan.route != route:
        # The method was chosen for the reach of the first route; a sampled statement reads the
        # same tables, so a different route means the choice no longer holds.
        raise ProfileError(
            f"the {sample.method} sample of {target.table_name!r} routed {plan.route} where the "
            f"method was chosen for {route} ({reach.where})"
        )
    try:
        require_sample_clause(
            sample.method, _executed_sql(plan), plan.dialect, reach.where, len(sample.ranges)
        )
    except SampleClauseLost as exc:
        raise ProfileError(f"profile of {target.table_name!r}: {exc}") from exc
    names, rows = await _execute(plan)
    return parse_profile_result(names, rows, target.columns, target.fanouts, target.keys)


async def read_profile(
    state: Any,
    target: Target,
    row_count: int,
    fraction: float | None,
    low_cardinality_max: int,
    rng: Any,
) -> ProfileRead:
    """Read ``target``'s profile: whole when ``fraction`` is None, else sampled by the first
    method its reach offers (``provisa.profiler.sampling``)."""
    from provisa.profiler.sampling import choose_method, sample_reach

    def sql(sample: Sample) -> str:
        return profile_sql(
            target.pgwire_name,
            target.columns,
            target.fanouts,
            sample,
            low_cardinality_max,
            target.keys,
        )

    def parse(result: tuple[list[str], list[tuple]]) -> ProfileAggregates:
        return parse_profile_result(
            result[0], result[1], target.columns, target.fanouts, target.keys
        )

    if fraction is None:
        return ProfileRead(parse(await _governed(sql(Sample("whole")))), "whole", [])
    random_sample = Sample("random", fraction)
    random_plan = await _route(sql(random_sample))
    reach = sample_reach(state, random_plan.route, target.meta)
    method = choose_method(reach, target.key is not None)
    if method == "random":
        agg = parse(await _execute(random_plan))
        return ProfileRead(
            agg, method, [{"percent": random_sample.percent, "rows": agg.profiled_rows}]
        )
    if method == "key_range":
        assert target.key is not None  # choose_method picks key_range only for a keyed table
        _, bounds = await _governed(key_bounds_sql(target.pgwire_name, target.key))
        lo, hi = bounds[0]
        if lo is None or hi is None:
            raise ProfileError(
                f"table {target.table_name!r} counted {row_count} rows but its key "
                f"{target.key!r} has no extremes"
            )
        sample = Sample(
            "key_range", fraction, target.key, key_ranges(int(lo), int(hi), fraction, rng)
        )
        agg = await _run_sample(sql(sample), sample, random_plan.route, reach, target)
        return ProfileRead(agg, method, [{"percent": sample.percent, "rows": agg.profiled_rows}])
    # Block: the source's block (a page, a vector, a file split) is the sampling unit, so a small
    # sample of a table with few blocks can come back far under its target -- or empty. Read again
    # at four times the percentage while it is under half its target (maintainer ruling, REQ-1934),
    # up to the whole table.
    attempts: list[dict] = []
    asked = fraction
    while True:
        sample = Sample("block", asked)
        agg = await _run_sample(sql(sample), sample, random_plan.route, reach, target)
        attempts.append({"percent": sample.percent, "rows": agg.profiled_rows})
        if agg.profiled_rows * 2 >= fraction * row_count or sample.percent >= 100.0:
            break
        asked = min(1.0, asked * 4)
    if agg.profiled_rows == 0 and row_count > 0:
        raise ProfileError(
            f"the block sample of {target.table_name!r} ({reach.where}) came back empty at "
            f"{sample.percent}% though the table counted {row_count} rows"
        )
    return ProfileRead(agg, method, attempts)


async def profile_table(
    state: Any, *, table_id: int, table_name: str, settings: ProfilerSettings
) -> RunOutcome:
    """Run the profile of one member table and append it to its history. A failure is recorded as
    a failed run and re-raised as :class:`ProfileError`."""
    run_id = str(uuid.uuid4())
    run_time = datetime.now(UTC)
    started = time.monotonic()
    row_count: int | None = None
    fraction: float | None = None
    read: ProfileRead | None = None
    try:
        async with state.model_db.acquire() as conn:
            tags = await column_tags(conn, table_id)
        target = resolve_target(state, table_id, table_name, tags)
        _, count_rows = await _governed(count_sql(target.pgwire_name))
        row_count = int(count_rows[0][0])
        fraction = sample_fraction(row_count, len(target.columns), settings.sample_above_cells)
        read = await read_profile(
            state, target, row_count, fraction, settings.low_cardinality_max, random.Random()
        )
        results = result_rows(target, read.agg, run_id, run_time, settings.low_cardinality_max)
        status, error = "succeeded", None
    except Exception as exc:  # recorded as the run's outcome, then re-raised below
        results = {}
        status, error = "failed", f"{type(exc).__name__}: {exc}"
        log.exception("profile run %s of table %s failed", run_id, table_name)
    profiled = None if read is None else read.agg.profiled_rows
    counted = row_count is not None
    dups = None if read is None else read.agg.rows
    keys = None if read is None else read.agg.keys
    results["runs"] = [
        {
            "run_id": run_id,
            "run_time": run_time,
            "region": None,  # REQ-1921 not on this branch; see the module docstring
            "profiled_table": table_name,
            "row_count": row_count,
            # Unknown when the run failed before the table was counted.
            "sampled": fraction is not None if counted else None,
            # How the rows were read; unknown when the run failed before reading them.
            "sample_method": None if read is None else read.method,
            "target_fraction": (1.0 if fraction is None else fraction) if counted else None,
            # The realised share: what the source returned, not what was asked for.
            "sample_fraction": None if profiled is None or not row_count else profiled / row_count,
            "sample_attempts": None if read is None else json.dumps(read.attempts),
            "profiled_rows": profiled,
            "previous_run_id": None,  # set by the comparison below, once the run succeeded
            "duplicate_rows": None if dups is None else dups.extra,
            "duplicate_share": None if dups is None or not profiled else dups.extra / profiled,
            # Over every declared key; none declared, nothing to count.
            "key_duplicates": sum(k.repeated for k in keys) if keys else None,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "status": status,
            "error": error,
        }
    ]
    async with state.model_db.acquire() as conn:
        if status == "succeeded":
            try:
                results["drift"] = await _comparison(conn, table_name, table_id, run_time, results)
            except Exception as exc:  # recorded as the run's outcome, then re-raised below
                status, error = "failed", f"{type(exc).__name__}: {exc}"
                log.exception("comparing profile run %s of table %s failed", run_id, table_name)
                results = {"runs": results["runs"]}
                results["runs"][0].update(status=status, error=error, previous_run_id=None)
        await write_results(conn, table_name, table_id, results)
    outcome = RunOutcome(run_id, run_time, status, error, row_count, profiled)
    if error is not None:
        raise ProfileError(f"profile of table {table_name!r} failed: {error}")
    return outcome
