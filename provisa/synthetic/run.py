# Copyright (c) 2026 Kenneth Stott
# Canary: 5a9d2f13-e46c-4b87-a0b1-7c3e8d2f6a50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generating, reporting and dropping a synthetic dataset (REQ-1939).

Generation happens in the environment the dataset belongs to, with that environment bound. Each
table's rows come from its generation statement (``generate.generation_sql``), which the engine
runs and streams as Arrow batches into the environment's store through the replica write face, in
the dataset's own schema. Nothing is read from any source; the profile runs are read from the
control plane of the environment that holds them.

Once every table stands, the dataset is marked generated, which routes those tables' reads to
their copies in this environment (``replica_routing``). Each copy is then profiled through the
governed pipeline and compared with the profile it was drawn from (the report).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import delete, insert, select

from provisa.synthetic import datasets
from provisa.synthetic.generate import generation_sql
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    PlannedTable,
    ProfiledColumn,
    ProfiledFanout,
    ProfiledTable,
    edges_of,
    plan_tables,
    to_measure,
)

log = logging.getLogger(__name__)

# The report compares sketches, shares and fan-out, and no value-frequency table, so its profile
# statement keeps only the most frequent values.
REPORT_LOW_CARDINALITY = 1


def _env() -> str | None:
    from provisa.core.request_context import current_env

    return current_env.get()


def _require_non_prod(env: str | None) -> str:
    from provisa.core.environments import PROD

    if env is None or env == PROD:
        raise DatasetRefused(
            "the production environment cannot hold a synthetic dataset; select another environment"
        )
    return env


def store_schema(dataset_id: str) -> str:
    from provisa.core.request_context import require_current_org
    from provisa.federation.replica_address import synthetic_schema

    return synthetic_schema(require_current_org(), _require_non_prod(_env()), dataset_id)


# -- reading a profile run held by an environment ------------------------------------------------


def _qualified(table: sa.Table, schema: str) -> sa.Table:
    return table.to_metadata(sa.MetaData(), schema=schema)


async def _profile_table_id(conn: Any, schema: str, reg: dict) -> int:
    from provisa.core.schema_org import registered_tables as rt

    q = _qualified(rt, schema)
    row = (
        await conn.execute_core(
            select(q.c.id).where(
                q.c.source_id == reg["source_id"],
                q.c.schema_name == reg["schema_name"],
                q.c.table_name == reg["table_name"],
            )
        )
    ).fetchone()
    if row is None:
        raise DatasetRefused(
            f"table {reg['table_name']!r} is not registered in the environment holding its profile"
        )
    return int(row[0])


async def read_profile(
    conn: Any, *, org_id: str, env: str | None, reg: dict, run_id: str
) -> ProfiledTable:
    """The profile run ``run_id`` of ``reg`` as environment ``env`` holds it (None: prod, as
    provisa.core.environments.org_schema reads it)."""
    from provisa.core.environments import org_schema
    from provisa.profiler.schema import result_sa_table

    schema = org_schema(org_id, env)
    tid = await _profile_table_id(conn, schema, reg)

    async def rows(kind: str) -> list[dict]:
        rel = _qualified(result_sa_table(reg["table_name"], tid, kind), schema)
        result = await conn.execute_core(select(rel).where(rel.c.run_id == run_id))
        return [dict(r._mapping) for r in result.fetchall()]

    runs = await rows("runs")
    if not runs or runs[0]["status"] != "succeeded":
        raise DatasetRefused(
            f"table {reg['table_name']!r} has no succeeded profile run {run_id!r} in environment "
            f"{env!r}"
        )
    quantiles: dict[str, list[tuple[float, float]]] = {}
    for q in await rows("quantiles"):
        if q["measure"] == "value":
            quantiles.setdefault(q["column_name"], []).append((q["q"], q["value"]))
    top: dict[str, list[tuple[int, str | None, int]]] = {}
    freq: dict[str, list[tuple[int, str | None, int]]] = {}
    for v in await rows("top_values"):
        (top if v["kind"] == "top" else freq).setdefault(v["column_name"], []).append(
            (v["rank"], v["value"], v["row_count"])
        )
    shapes: dict[str, list[tuple[int, str, int]]] = {}
    for s in await rows("shapes"):
        shapes.setdefault(s["column_name"], []).append((s["rank"], s["shape"], s["row_count"]))
    columns: dict[str, ProfiledColumn] = {}
    for c in await rows("columns"):
        name = c["column_name"]
        sketch = sorted(quantiles.get(name, []))
        columns[c["physical_column"]] = ProfiledColumn(
            physical=c["physical_column"],
            family=c["family"],
            null_count=c["null_count"],
            distinct_count=c["distinct_count"],
            distinct_ratio=c["distinct_ratio"],
            integer_only=c["integer_only"],
            min_value=c["min_value"],
            sketch=tuple(v for _, v in sketch) if len(sketch) == 101 else None,
            top=tuple((v, n) for _, v, n in sorted(top.get(name, []))),
            frequencies=tuple((v, n) for _, v, n in sorted(freq.get(name, []))),
            shapes=tuple((s, n) for _, s, n in sorted(shapes.get(name, []))),
        )
    fan_q: dict[str, list[tuple[float, float]]] = {}
    for f in await rows("fanout"):
        fan_q.setdefault(f["relationship"], []).append((f["q"], f["value"]))
    fanouts = tuple(
        ProfiledFanout(
            child_table=f["child_table"],
            parents=f["parents"],
            sketch=tuple(v for _, v in sorted(fan_q[f["relationship"]])),
        )
        for f in await rows("fanout_runs")
        if len(fan_q.get(f["relationship"], [])) == 101
    )
    return ProfiledTable(run_id, runs[0]["row_count"], runs[0]["profiled_rows"], columns, fanouts)


# -- generating ---------------------------------------------------------------------------------


class _GeneratedRows:
    """The engine runs a table's generation statement and streams its rows as Arrow batches."""

    def __init__(self, engine_runtime: Any, sql: str, label: str) -> None:
        from provisa.federation.data_replicator import SourceCaps, SourceRead

        self._engine = engine_runtime
        self._sql = sql
        self._label = label
        self.caps = SourceCaps(frozenset({SourceRead.ARROW_STREAM}))

    async def batches(self, batch_rows: int) -> Any:
        from provisa.federation.execution_auth import SystemAuth, mint_system_token
        from provisa.federation.replica_source import _bounded

        # The system's own statement: it reads nothing, it generates (REQ-1760 pins it).
        _schema, stream = self._engine.execute_engine_stream(
            self._sql,
            authorization=SystemAuth(
                mint_system_token(), reason=f"synthetic:{self._label}", expected_sql=self._sql
            ),
        )
        try:
            for batch in stream:
                for part in _bounded(batch, batch_rows):
                    yield part
        finally:
            stream.close()


def _count_rows(state: Any, plan: Any) -> int:
    """How many rows a table generated by a relationship has, counted by the engine."""
    from provisa.federation.execution_auth import SystemAuth, mint_system_token
    from provisa.synthetic.generate import children_count_sql

    engine_rt = state.federation_engine
    sql = children_count_sql(plan, engine_rt.engine.name)
    _schema, stream = engine_rt.execute_engine_stream(
        sql,
        authorization=SystemAuth(
            mint_system_token(), reason=f"synthetic:{plan.name}:count", expected_sql=sql
        ),
    )
    try:
        values = [v for batch in stream for v in batch.column(0).to_pylist()]
    finally:
        stream.close()
    assert len(values) == 1, f"a count statement answered {len(values)} rows"
    return int(values[0])


async def _measure(state: Any, t: DatasetTable, column: str) -> list[tuple[str | None, int]]:
    """``column``'s values and their counts, nulls included, read from its table: one GROUP BY as
    the org admin through the governed pipeline, as the profiler reads (REQ-1494, MEASURED FROM
    THE PROFILE, ELSE FROM THE TABLE)."""
    from provisa.profiler.run import PROFILE_ROLE, _governed
    from provisa.profiler.statement import _ident, qualified

    exposed = state.contexts[PROFILE_ROLE].physical_to_sql.get((t.table_id, column))
    if exposed is None:
        raise DatasetRefused(f"{t.name}.{column} is not readable by {PROFILE_ROLE}")
    value = f"CAST(x.{_ident(exposed)} AS TEXT)"
    _names, rows = await _governed(
        f"SELECT {value} AS v, COUNT(*) AS n FROM {qualified(t.pgwire_name)} x GROUP BY {value}"
    )
    return [(v, int(n)) for v, n in rows]


async def _write_table(state: Any, schema: str, planned: PlannedTable, reg: dict) -> int:
    from provisa.federation.data_replicator import data_replicator
    from provisa.federation.replica_address import ReplicaAddress, replica_table_name
    from provisa.federation.replica_builds import _model_row
    from provisa.federation.replica_parties import StoreReadingEngine
    from provisa.federation.replica_source import BATCH_ROWS
    from provisa.federation.residency import resolve_landing_args

    engine_rt = state.federation_engine
    backend = engine_rt.engine.backend
    source, table, _ = await _model_row(
        state, (reg["source_id"], reg["schema_name"], reg["table_name"])
    )
    args = resolve_landing_args(source, table, platform=backend.dialect)
    address = ReplicaAddress(
        schema, replica_table_name(source.id, reg["schema_name"], reg["table_name"])
    )
    engine_party = StoreReadingEngine(backend, state)
    target = backend.replica_target(state, address=address, args=args, engine=engine_party)
    reader = _GeneratedRows(
        engine_rt, generation_sql(planned.plan, engine_rt.engine.name), planned.plan.name
    )
    copied = 0

    async def progress(rows: int) -> None:
        nonlocal copied
        copied = rows

    job = data_replicator(reader, target, engine_party, batch_rows=BATCH_ROWS)
    outcome = await job.run(progress)
    return outcome.rows_copied


async def _dataset_tables(
    state: Any, row: datasets.DatasetRow
) -> tuple[list[DatasetTable], list[dict], dict[int, dict]]:
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name
    from provisa.core.ir_types import to_ir
    from provisa.core.request_context import require_current_org
    from provisa.fakes.kinds import parse as parse_fake
    from provisa.profiler.run import PROFILE_ROLE, column_tags

    org_id = require_current_org()
    async with state.model_db.acquire() as conn:
        registered = {t["id"]: t for t in await fetch_tables(conn)}
        relationships = await fetch_relationships(conn)
        ctx = state.contexts[PROFILE_ROLE]
        metas = {tm.table_id: tm for tm in ctx.tables.values()}
        out = []
        for t in row.tables:
            reg = registered.get(t.table_id)
            if reg is None:
                raise DatasetRefused(f"table {t.table_id} is no longer registered")
            tm = metas.get(t.table_id)
            if tm is None:
                raise DatasetRefused(
                    f"table {reg['table_name']!r} is not readable by {PROFILE_ROLE}"
                )
            profile = await read_profile(
                conn, org_id=org_id, env=t.profile_env, reg=reg, run_id=t.run_id
            )
            tags = await column_tags(conn, t.table_id)
            dialect = state.federation_engine.engine.backend.dialect
            out.append(
                DatasetTable(
                    table_id=t.table_id,
                    name=reg["table_name"],
                    pgwire_name=(
                        f"{domain_to_sql_name(tm.domain_id or tm.schema_name or 'public')}."
                        f"{semantic_table_name(tm)}"
                    ),
                    columns=tuple(
                        (c["column_name"], to_ir(c["data_type"], dialect), c["is_primary_key"])
                        for c in reg["columns"]
                        if c.get("native_filter_type") is None
                    ),
                    scale=t.scale if t.scale is not None else row.scale,
                    profile=profile,
                    pii=frozenset(c for c, tagged in tags.items() if "pii" in tagged),
                    # REQ-1494, REQ-1939: what generates each column -- its synthetic rule, else
                    # its fake.
                    fakes={
                        c["column_name"]: (
                            parse_fake(c["synthetic_rule"], rule=True)
                            if c.get("synthetic_rule") is not None
                            else parse_fake(c["fake"])
                        )
                        for c in reg["columns"]
                        if c.get("synthetic_rule") is not None or c.get("fake") is not None
                    },
                )
            )
    return out, relationships, registered


async def generate(state: Any, dataset_id: str) -> None:
    """Generate (or regenerate) ``dataset_id`` in the bound environment, then report on it."""
    from provisa.api.app import _rebuild_schemas

    _require_non_prod(_env())
    async with state.model_db.acquire() as conn:
        row = await datasets.get_dataset(conn, dataset_id)
        await datasets.set_status(conn, dataset_id, "generating", error=None)
    try:
        tables, relationships, registered = await _dataset_tables(state, row)
        edges = edges_of(relationships)
        measured = {
            (t.table_id, c): await _measure(state, t, c) for t, c in to_measure(tables, edges)
        }
        planned = plan_tables(
            tables,
            edges,
            seed=row.seed,
            names={tid: reg["table_name"] for tid, reg in registered.items()},
            count_rows=lambda plan: _count_rows(state, plan),
            measure=lambda t, c: measured[(t.table_id, c)],
        )
        for p in planned:
            await _write_table(state, row.store_schema, p, registered[p.table.table_id])
    except Exception as exc:
        async with state.model_db.acquire() as conn:
            await datasets.set_status(
                conn, dataset_id, "failed", error=f"{type(exc).__name__}: {exc}"
            )
        raise
    async with state.model_db.acquire() as conn:
        await datasets.set_status(conn, dataset_id, "generated", generated_at=datetime.now(UTC))
    # Its tables now read their copies here: the routes are republished with the model.
    await _rebuild_schemas()
    await report(state, dataset_id, planned)


async def drop(state: Any, dataset_id: str) -> None:
    """Drop the dataset: its tables read their bindings again and its schema is removed."""
    from provisa.api.app import _rebuild_schemas
    from provisa.federation.store_scope import drop_synthetic_schema

    async with state.model_db.acquire() as conn:
        row = await datasets.get_dataset(conn, dataset_id)
        await datasets.drop(conn, dataset_id)
    await _rebuild_schemas()
    await drop_synthetic_schema(
        state.federation_engine.engine.materialize_store(), row.store_schema
    )


# -- the report -------------------------------------------------------------------------------


async def report(state: Any, dataset_id: str, planned: list[PlannedTable]) -> None:
    """Profile each generated table as the org admin and compare it with its source profile."""
    from provisa.core.schema_org import synthetic_report
    from provisa.profiler import measures
    from provisa.profiler.run import _governed, column_tags, resolve_target
    from provisa.profiler.statement import Sample, parse_profile_result, profile_sql
    from provisa.synthetic.report import compare

    entries: list[dict] = []
    for p in planned:
        async with state.model_db.acquire() as conn:
            tags = await column_tags(conn, p.table.table_id)
        target = resolve_target(state, p.table.table_id, p.table.name, tags)
        names, rows = await _governed(
            profile_sql(
                target.pgwire_name,
                target.columns,
                target.fanouts,
                Sample("whole"),
                REPORT_LOW_CARDINALITY,
            )
        )
        synthetic = parse_profile_result(names, rows, target.columns, target.fanouts)
        entries += compare(p, synthetic, measures)
    async with state.model_db.acquire() as conn:
        async with conn.transaction():
            await conn.execute_core(
                delete(synthetic_report).where(synthetic_report.c.dataset_id == dataset_id)
            )
            for e in entries:
                await conn.execute_core(insert(synthetic_report).values(dataset_id=dataset_id, **e))


# -- defining -----------------------------------------------------------------------------------


async def check_closure_of(conn: Any, table_ids: list[int]) -> None:
    """Refuse naming a table without the parent of one of its relationships (REQ-1939)."""
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.synthetic.plan import check_closure

    names = {t["id"]: t["table_name"] for t in await fetch_tables(conn)}
    missing = [t for t in table_ids if t not in names]
    if missing:
        raise DatasetRefused(f"table {missing[0]} is not registered in this environment")
    check_closure(
        {t: names[t] for t in table_ids}, names, edges_of(await fetch_relationships(conn))
    )


async def profile_runs_in(conn: Any, env: str, tables: list[dict]) -> list[dict]:
    """The succeeded profile runs environment ``env`` holds of each of ``tables``, newest first."""
    from provisa.core.environments import org_schema
    from provisa.core.request_context import require_current_org
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler.schema import result_table_name

    schema = org_schema(require_current_org(), env)
    q = _qualified(rt, schema)
    held = {
        (r[1], r[2], r[3]): r[0]
        for r in (
            await conn.execute_core(
                select(q.c.id, q.c.source_id, q.c.schema_name, q.c.table_name).where(
                    q.c.profiler_source_id.is_not(None)
                )
            )
        ).fetchall()
    }
    existing = {
        r[0]
        for r in (
            await conn.execute_core(
                sa.text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = :s"
                ).bindparams(s=schema)
            )
        ).fetchall()
    }
    out = []
    for t in tables:
        tid = held.get((t["source_id"], t["schema_name"], t["table_name"]))
        if tid is None or result_table_name(t["table_name"], tid, "runs") not in existing:
            continue
        from provisa.profiler.schema import result_sa_table

        runs = _qualified(result_sa_table(t["table_name"], tid, "runs"), schema)
        rows = (
            await conn.execute_core(
                select(runs.c.run_id, runs.c.run_time, runs.c.row_count)
                .where(runs.c.status == "succeeded")
                .order_by(runs.c.run_time.desc())
            )
        ).fetchall()
        if rows:
            out.append(
                {
                    "tableId": t["id"],
                    "tableName": t["table_name"],
                    "runs": [
                        {"runId": r[0], "runTime": r[1].isoformat(), "rowCount": r[2]} for r in rows
                    ],
                }
            )
    return out


# -- bootstrapping an environment ---------------------------------------------------------------


async def bootstrap(
    state: Any,
    *,
    org_id: str,
    env: str,
    dataset_id: str,
    profile_env: str,
    scale: float,
    seed: int,
    table_names: list[str],
) -> None:
    """Seed the newly created environment ``env`` with a synthetic dataset (REQ-1939, BOOTSTRAPPING
    AN ENVIRONMENT): ``table_names`` from their latest profile runs in ``profile_env``, generated."""
    from provisa.api.admin.db_queries import fetch_tables
    from provisa.api.app import ensure_org_runtime
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.synthetic.datasets import DatasetTableRow, check_name, define

    check_name(dataset_id)
    await ensure_org_runtime(org_id, env)
    org_token = set_current_org(org_id)
    env_token = set_current_env(env)
    try:
        async with state.model_db.acquire() as conn:
            tables = await fetch_tables(conn)
            runs = {r["tableName"]: r for r in await profile_runs_in(conn, profile_env, tables)}
            by_name: dict[str, list[dict]] = {}
            for t in tables:
                by_name.setdefault(t["table_name"], []).append(t)
            rows = []
            for name in table_names:
                found = by_name.get(name, [])
                if len(found) != 1:
                    raise DatasetRefused(
                        f"table {name!r} is "
                        + ("not registered" if not found else "registered more than once")
                        + f" in environment {env!r}"
                    )
                if name not in runs:
                    raise DatasetRefused(
                        f"table {name!r} has no succeeded profile run in environment "
                        f"{profile_env!r} to generate it from"
                    )
                rows.append(
                    DatasetTableRow(
                        found[0]["id"], profile_env, runs[name]["runs"][0]["runId"], None
                    )
                )
            await check_closure_of(conn, [r.table_id for r in rows])
            await define(
                conn,
                dataset_id=dataset_id,
                seed=seed,
                scale=scale,
                store_schema=store_schema(dataset_id),
                tables=rows,
            )
        await generate(state, dataset_id)
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)
