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
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import delete, insert, select

from provisa.synthetic import datasets
from provisa.synthetic import closeness
from provisa.synthetic.generate import Closeness, closeness_sql, generation_sql
from provisa.synthetic.group import reads_children
from provisa.synthetic.privacy import report_entries as privacy_entries
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    PlannedTable,
    ProfiledColumn,
    ProfiledFanout,
    ProfiledTable,
    distances_to_measure,
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


def _parent_resolver(ctx: Any, tm: Any) -> Any:
    """``<relationship>.<column>`` -- a parent table's column as a profile run names it -- to a
    dependence Parent: the parent's registered column, reached through this table's referring
    column; None where the name reaches no such column."""
    from provisa.synthetic.dependence import Parent

    by_sql = {(tid, sql): phys for (tid, phys), sql in ctx.physical_to_sql.items()}

    def resolve(name: str) -> Any:
        field, _, column = name.partition(".")
        jm = ctx.joins.get((tm.type_name, field))
        if jm is None or jm.cardinality != "many-to-one":
            return None
        phys = by_sql.get((jm.target.table_id, column))
        return None if phys is None else Parent(phys, via=jm.source_column)

    return resolve


def _published(column_rows: list[dict]) -> dict[str, Any]:
    from types import SimpleNamespace

    return {c["column_name"]: SimpleNamespace(physical=c["physical_column"]) for c in column_rows}


async def _dependence(rows: Any, physical: dict[str, str], parent_of: Any) -> Any:
    """The run's rank correlations between the table's own columns, its dependency network and
    the network's joint counts, by registered column name (REQ-1939, DEPENDENCE KEPT). A parent
    table's column the generator cannot reach (``parent_of`` gives None) leaves its child out of
    the network: it is drawn on its own, and the report names it."""
    from provisa.synthetic.dependence import Dependence, Joint, Parent

    spearman = {}
    for r in await rows("correlations"):
        a, b = physical.get(r["column_name"]), physical.get(r["other_column"])
        if r["measure"] == "spearman" and a and b and r["value"] is not None:
            spearman[(a, b)] = float(r["value"])

    def parent(name: str | None) -> Any:
        if name is None or name == "":
            return None
        if name in physical:
            return Parent(physical[name])
        return parent_of(name) if parent_of is not None else None

    network: dict[str, tuple[Any, ...]] = {}
    unreached: set[str] = set()
    for r in await rows("dependencies"):
        if not r["in_network"]:
            continue
        target = physical.get(r["column_name"])
        if target is None:
            continue
        named = [n for n in (r["parent_1"], r["parent_2"]) if n]
        parents = [parent(n) for n in named]
        if any(p is None for p in parents):
            unreached.add(target)
            continue
        network[target] = tuple(parents)
    joints: dict[str, list[Any]] = {}
    for r in await rows("joint_counts"):
        target = physical.get(r["column_name"])
        if target not in network:
            continue

        def state(value: Any, bucket: Any) -> Any:
            return int(bucket) if bucket is not None else value

        parents = [state(r["parent_1_value"], r["parent_1_bucket"])]
        if r["parent_2"]:
            parents.append(state(r["parent_2_value"], r["parent_2_bucket"]))
        joints.setdefault(target, []).append(
            Joint(state(r["target_value"], r["target_bucket"]), tuple(parents), int(r["row_count"]))
        )
    return Dependence(
        spearman,
        {t: p for t, p in network.items() if t in joints},
        {t: tuple(j) for t, j in joints.items()},
        frozenset(unreached),
    )


async def read_profile(
    conn: Any,
    *,
    org_id: str,
    env: str | None,
    reg: dict,
    run_id: str,
    parent_of: Any = None,
) -> ProfiledTable:
    """The profile run ``run_id`` of ``reg`` as environment ``env`` holds it (None: prod, as
    provisa.core.environments.org_schema reads it). ``parent_of`` resolves a parent table's
    column as the run names it (``<relationship>.<column>``) to a dependence Parent, or None."""
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
    physical = {c: p.physical for c, p in _published(await rows("columns")).items()}
    return ProfiledTable(
        run_id,
        runs[0]["row_count"],
        runs[0]["profiled_rows"],
        columns,
        fanouts,
        await _dependence(rows, physical, parent_of),
    )


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


async def _generated_addresses(
    state: Any, schema: str, planned: list[PlannedTable], registered: dict[int, dict]
) -> dict[str, str]:
    """Each generated table's address on the engine, by its dataset name: the dataset's schema
    in the store the engine reads replicas from (as provisa.federation.replica_routing routes
    the dataset's tables to their copies)."""
    import asyncio

    from provisa.federation.replica_address import replica_table_name

    backend = state.federation_engine.engine.backend
    catalog = await asyncio.to_thread(backend.replica_read_catalog, state)
    out = {}
    for p in planned:
        reg = registered[p.table.table_id]
        parts = [
            catalog,
            schema,
            replica_table_name(reg["source_id"], reg["schema_name"], reg["table_name"]),
        ]
        out[p.table.name] = ".".join('"' + x.replace('"', '""') + '"' for x in parts if x)
    return out


def _children_first(planned: list[PlannedTable]) -> list[PlannedTable]:
    """The second pass's tables, each after the regenerated tables its rules read."""
    names = {p.table.name for p in planned}
    done: list[PlannedTable] = []
    placed: set[str] = set()
    pending = list(planned)
    while pending:
        ready = [
            p
            for p in pending
            if {link.child_table for g in p.plan.group for link in g.children} & names
            <= placed | {p.table.name}
        ]
        if not ready:
            raise DatasetRefused(
                "the rules of " + ", ".join(p.table.name for p in pending) + " read one another"
            )
        for p in ready:
            done.append(p)
            placed.add(p.table.name)
        pending = [p for p in pending if p.table.name not in placed]
    return done


def _spans_rows(kind: Any) -> bool:
    from provisa.fakes.kinds import Sequence, Sql

    return isinstance(kind, Sequence) or (isinstance(kind, Sql) and kind.group)


def _child_links(table_id: int, registered: dict[int, dict], relationships: list[dict]) -> dict:
    """The children a rule of ``table_id`` reads, by the name it reads them by: a one-to-many
    relationship's field name on the parent, or, for a many-to-one relationship declared on the
    child, the child table's name (as provisa.api.admin._fake_guard names them)."""
    from provisa.synthetic.group import ChildLink

    out: dict[str, ChildLink] = {}
    for r in relationships:
        if r.get("target_table_id") is None or r.get("via_table_id") is not None:
            continue
        if r["cardinality"] == "one-to-many" and r["source_table_id"] == table_id:
            child = registered.get(r["target_table_id"])
            if child is not None:
                out[r["graphql_alias"]] = ChildLink(
                    r["graphql_alias"], child["table_name"], r["source_column"], r["target_column"]
                )
        elif r["cardinality"] == "many-to-one" and r["target_table_id"] == table_id:
            child = registered.get(r["source_table_id"])
            if child is not None:
                out[child["table_name"]] = ChildLink(
                    child["table_name"], child["table_name"], r["target_column"], r["source_column"]
                )
    return out


async def _measure_difference(
    state: Any, t: DatasetTable, column: str, other: str
) -> list[float | None] | None:
    """The measured difference of ``column`` less ``other``, read from the table as the org admin
    (REQ-1494, MEASURED FROM THE PROFILE, ELSE FROM THE TABLE)."""
    from provisa.fakes.checks import family
    from provisa.fakes.measured import difference_quantiles
    from provisa.profiler.run import PROFILE_ROLE
    from provisa.profiler.statement import qualified

    p2s = state.contexts[PROFILE_ROLE].physical_to_sql
    exposed = {c: p2s.get((t.table_id, c)) for c in (column, other)}
    hidden = [c for c, e in exposed.items() if e is None]
    if hidden:
        raise DatasetRefused(f"{t.name}.{hidden[0]} is not readable by {PROFILE_ROLE}")
    ir_type = next(ir for name, ir, _ in t.columns if name == column)
    return await difference_quantiles(
        qualified(t.pgwire_name), exposed[column], exposed[other], family(ir_type)
    )


async def _pinned_runs(
    state: Any, row: Any, tables: list[DatasetTable], registered: dict[int, dict]
) -> dict[tuple[int, str], ProfiledTable]:
    """The profile runs a stable profile() fake pins, beside the dataset's own run of the table."""
    from provisa.core.request_context import require_current_org
    from provisa.fakes.kinds import Profile

    env_of = {t.table_id: t.profile_env for t in row.tables}
    out: dict[tuple[int, str], ProfiledTable] = {}
    async with state.model_db.acquire() as conn:
        for t in tables:
            for fake in t.fakes.values():
                if isinstance(fake, Profile) and fake.run not in (None, t.profile.run_id):
                    assert fake.run is not None
                    out[(t.table_id, fake.run)] = await read_profile(
                        conn,
                        org_id=require_current_org(),
                        env=env_of[t.table_id],
                        reg=registered[t.table_id],
                        run_id=fake.run,
                    )
    return out


async def _write_table(
    state: Any,
    schema: str,
    planned: PlannedTable,
    reg: dict,
    child_tables: dict[str, str] | None = None,
    close: Closeness | None = None,
) -> int:
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
    dialect = engine_rt.engine.name
    reader = _GeneratedRows(
        engine_rt,
        generation_sql(planned.plan, dialect, child_tables)
        if close is None
        else closeness_sql(planned.plan, dialect, close, child_tables),
        planned.plan.name,
    )
    copied = 0

    async def progress(rows: int) -> None:
        nonlocal copied
        copied = rows

    job = data_replicator(reader, target, engine_party, batch_rows=BATCH_ROWS)
    outcome = await job.run(progress)
    return outcome.rows_copied


# -- not too close to a real row (REQ-1939; maintainer rulings W1, Z2, C1) -----------------------


def _engine_rows(state: Any, sql: str, label: str) -> list[tuple]:
    """The rows of the system's own statement over the dataset's store tables."""
    from provisa.federation.execution_auth import SystemAuth, mint_system_token

    engine_rt = state.federation_engine
    _schema, stream = engine_rt.execute_engine_stream(
        sql,
        authorization=SystemAuth(
            mint_system_token(), reason=f"synthetic:{label}", expected_sql=sql
        ),
    )
    try:
        return [tuple(r.values()) for b in stream for r in b.to_pylist()]
    finally:
        stream.close()


class _LandedRows:
    """One Arrow batch, read as a replica source."""

    def __init__(self, batch: Any) -> None:
        from provisa.federation.data_replicator import SourceCaps, SourceRead

        self._batch = batch
        self.caps = SourceCaps(frozenset({SourceRead.ARROW_STREAM}))

    async def batches(self, batch_rows: int) -> Any:
        from provisa.federation.replica_source import _bounded

        for part in _bounded(self._batch, batch_rows):
            yield part


def sample_name(reg: dict) -> str:
    """The store name of a table's real sample, beside its generated table and never registered."""
    from provisa.federation.replica_address import replica_table_name

    return replica_table_name("__closeness", reg["schema_name"], reg["table_name"])


async def _land_sample(
    state: Any, schema: str, p: PlannedTable, reg: dict, cols: list
) -> tuple[str, list]:
    """Up to SAMPLE_ROWS real rows of ``p``'s table, each column as the distance reads it, read
    as the org admin through the governed pipeline in an order drawn from their values, landed as
    an unregistered table of the dataset's store schema (W1); its address, and its columns as
    :class:`provisa.synthetic.generate.DistanceColumn`."""
    import asyncio

    import pyarrow as pa

    from provisa.federation.data_replicator import data_replicator
    from provisa.federation.replica_address import ReplicaAddress
    from provisa.federation.replica_builds import _model_row
    from provisa.federation.replica_parties import StoreReadingEngine
    from provisa.federation.replica_source import BATCH_ROWS
    from provisa.federation.residency import resolve_landing_args
    from provisa.profiler.run import _governed
    from provisa.profiler.statement import _ident, qualified
    from provisa.synthetic.generate import SAMPLE_ID, DistanceColumn

    exposed = _exposer(state)
    numbers = [c.family in ("numeric", "temporal") for c in cols]
    values = []
    for c, number in zip(cols, numbers):
        x = f"x.{_ident(exposed(p.table, c.name))}"
        if c.family == "temporal":
            values.append(f"CAST(EXTRACT(EPOCH FROM {x}) AS DOUBLE PRECISION)")
        elif number:
            values.append(f"CAST({x} AS DOUBLE PRECISION)")
        else:
            values.append(f"CAST({x} AS TEXT)")
    text = " || '|' || ".join(f"COALESCE(CAST({v} AS TEXT), '')" for v in values)
    named = ", ".join(f"{v} AS d{i}" for i, v in enumerate(values))
    _names, rows = await _governed(
        f"SELECT {named} FROM {qualified(p.table.pgwire_name)} x ORDER BY MD5({text}) "
        f"LIMIT {closeness.SAMPLE_ROWS}"
    )
    if len(rows) < 2:
        raise DatasetRefused(
            f"{p.table.name}: the closeness check compares with at least two real rows; the "
            f"table has {len(rows)}"
        )
    arrays = {SAMPLE_ID: pa.array(range(len(rows)), pa.int64())}
    distance_cols = []
    for i, (c, number) in enumerate(zip(cols, numbers)):
        column = [r[i] for r in rows]
        if number:
            floats = [None if v is None else float(v) for v in column]
            arrays[f"__d{i}"] = pa.array(floats, pa.float64())
            scale = closeness.scale_of([v for v in floats if v is not None])
        else:
            arrays[f"__d{i}"] = pa.array(column, pa.string())
            scale = None
        distance_cols.append(DistanceColumn(c.name, c.family, scale, c.sql_type))
    batch = pa.RecordBatch.from_pydict(arrays)

    engine_rt = state.federation_engine
    backend = engine_rt.engine.backend
    source, table, _ = await _model_row(
        state, (reg["source_id"], reg["schema_name"], reg["table_name"])
    )
    args = replace(
        resolve_landing_args(source, table, platform=backend.dialect),
        columns=[(SAMPLE_ID, "bigint")]
        + [(f"__d{i}", "double" if n else "text") for i, n in enumerate(numbers)],
        pk_columns=[SAMPLE_ID],
        watermark_column=None,
    )
    engine_party = StoreReadingEngine(backend, state)
    name = sample_name(reg)
    target = backend.replica_target(
        state, address=ReplicaAddress(schema, name), args=args, engine=engine_party
    )

    async def progress(rows: int) -> None:
        del rows  # a sample's landing reports nothing

    await data_replicator(_LandedRows(batch), target, engine_party, batch_rows=BATCH_ROWS).run(
        progress
    )
    catalog = await asyncio.to_thread(backend.replica_read_catalog, state)
    address = ".".join('"' + x.replace('"', '""') + '"' for x in (catalog, schema, name) if x)
    return address, distance_cols


async def _closeness_of(
    state: Any,
    row: datasets.DatasetRow,
    planned: list[PlannedTable],
    registered: dict[int, dict],
    addresses: dict[str, str],
    landed: list[str],
) -> tuple[dict[str, Closeness], dict[str, list[tuple[float, float]]], list[dict]]:
    """Each table's closeness: its real sample landed (its name added to ``landed``, to be
    dropped), the threshold measured over it, the parents its rows drop without; with each
    sample row's two nearest others, and the report's rows for a table not checked."""
    from provisa.synthetic.generate import distance_columns, sample_nearest_sql

    assert row.closeness_threshold is not None and row.closeness_draws is not None
    out: dict[str, Closeness] = {}
    real: dict[str, list[tuple[float, float]]] = {}
    unchecked: list[dict] = []
    for p in planned:
        reg = registered[p.table.table_id]
        # A row's parents among the dataset's other tables (C1); a row referring to its own
        # table is not dropped with its referent, which is generated beside it in one statement.
        parents = tuple(
            (c.name, addresses[c.foreign_key.parent_table], c.foreign_key.parent_key.name)
            for c in p.plan.columns
            if c.foreign_key is not None and c.foreign_key.parent_table != p.plan.name
        )
        cols = distance_columns(p.plan)
        if not cols:
            out[p.plan.name] = Closeness("", 0.0, 1, (), parents)
            unchecked += closeness.unchecked_table(p.plan.name)
            continue
        landed.append(sample_name(reg))
        address, distance_cols = await _land_sample(state, row.store_schema, p, reg, cols)
        close = Closeness(address, 0.0, row.closeness_draws, tuple(distance_cols), parents)
        pairs = [
            (float(a), float(b))
            for _id, a, b in _engine_rows(
                state, sample_nearest_sql(close), f"{p.plan.name}:closeness"
            )
        ]
        real[p.plan.name] = pairs
        limit = closeness.threshold(row.closeness_threshold, [a for a, _ in pairs])
        out[p.plan.name] = Closeness(
            address, limit, row.closeness_draws, tuple(distance_cols), parents
        )
    return out, real, unchecked


def _closeness_entries(
    state: Any,
    row: datasets.DatasetRow,
    planned: list[PlannedTable],
    closes: dict[str, Closeness],
    real: dict[str, list[tuple[float, float]]],
    addresses: dict[str, str],
) -> list[dict]:
    """The report's closeness rows of each checked table, measured while its sample is landed."""
    from provisa.synthetic.generate import closeness_counts_sql, generated_nearest_sql

    assert row.closeness_threshold is not None and row.closeness_draws is not None
    dialect = state.federation_engine.engine.name
    out = []
    for p in planned:
        close = closes[p.plan.name]
        ((redrawn, dropped, cascaded),) = _engine_rows(
            state, closeness_counts_sql(p.plan, dialect, close), f"{p.plan.name}:closeness"
        )
        if not close.columns:
            out += closeness.cascaded_only(p.plan.name, int(cascaded))
            continue
        generated = [
            (float(a), float(b))
            for _id, a, b in _engine_rows(
                state,
                generated_nearest_sql(
                    p.plan,
                    dialect,
                    close,
                    addresses[p.plan.name],
                    closeness.REPORT_ROWS,
                    row.seed,
                ),
                f"{p.plan.name}:closeness",
            )
        ]
        out += closeness.report_entries(
            p.plan.name,
            share=row.closeness_threshold,
            limit=close.threshold,
            draws=row.closeness_draws,
            counts=(int(redrawn), int(dropped), int(cascaded)),
            real=real[p.plan.name],
            generated=generated,
        )
    return out


async def _drop_samples(state: Any, schema: str, landed: list[str]) -> None:
    from provisa.federation.store_scope import drop_synthetic_table

    for name in landed:
        await drop_synthetic_table(state.federation_engine.engine.materialize_store(), schema, name)


async def _dataset_tables(
    state: Any, row: datasets.DatasetRow
) -> tuple[list[DatasetTable], list[dict], dict[int, dict]]:
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name
    from provisa.core.ir_types import to_ir
    from provisa.core.request_context import require_current_org
    from provisa.fakes.kinds import parse as parse_fake
    from provisa.profiler.constraints import accepted_constraints
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
                conn,
                org_id=org_id,
                env=t.profile_env,
                reg=reg,
                run_id=t.run_id,
                parent_of=_parent_resolver(ctx, tm),
            )
            tags = await column_tags(conn, t.table_id)
            # REQ-1939, ACCEPTED CONSTRAINTS BIND GENERATION.
            constraints = tuple(await accepted_constraints(conn, t.table_id))
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
                    # A stable fake generates by the portable definition version it is pinned to;
                    # a synthetic rule takes the fake's place and is never stable.
                    stable={
                        c["column_name"]: c["fake_stable_version"]
                        for c in reg["columns"]
                        if c.get("synthetic_rule") is None and c.get("fake_stable")
                    },
                    constraints=constraints,
                    self_fakes={
                        c["column_name"]: parse_fake(c["fake"])
                        for c in reg["columns"]
                        if c.get("synthetic_rule") is not None
                        and c.get("fake") is not None
                        and _spans_rows(parse_fake(c["synthetic_rule"], rule=True))
                    },
                    children=_child_links(t.table_id, registered, relationships),
                )
            )
    return out, relationships, registered


def _quantiles(histogram: dict[int, int], points: tuple[float, ...]) -> tuple[float, ...]:
    """PERCENTILE_CONT at each point over values given as {value: how many}."""
    values = sorted(histogram)
    total = sum(histogram.values())
    out = []
    for q in points:
        pos = q * (total - 1)
        lo, frac = int(pos), pos - int(pos)

        def at(i: int) -> float:
            seen = 0
            for v in values:
                seen += histogram[v]
                if i < seen:
                    return float(v)
            return float(values[-1])

        a = at(lo)
        out.append(a + frac * (at(min(lo + 1, total - 1)) - a))
    return tuple(out)


async def _measure_condition(
    state: Any, by_id: dict[int, DatasetTable], edges: list[Any], c: Any
) -> tuple[tuple[float, ...], int] | None:
    """The fan-out sketch of the real parents meeting ``c``'s condition: each such parent's child
    count, zero included, read as the org admin through the governed pipeline (REQ-1939,
    CONDITIONAL FAN-OUT), with how many parents meet it; None where none does."""
    import sqlglot
    from sqlglot import exp

    from provisa.profiler.run import PROFILE_ROLE, _governed
    from provisa.profiler.statement import QUANTILE_POINTS, _ident, qualified

    edge = next(e for e in edges if e.relationship == c.relationship)
    parent, child = by_id[edge.parent_id], by_id[edge.child_id]
    p2s = state.contexts[PROFILE_ROLE].physical_to_sql

    def exposed(t: DatasetTable, column: str) -> str:
        name = p2s.get((t.table_id, column))
        if name is None:
            raise DatasetRefused(f"{t.name}.{column} is not readable by {PROFILE_ROLE}")
        return name

    tree = sqlglot.parse_one(c.condition, read="postgres")
    for col in list(tree.find_all(exp.Column)):
        col.replace(exp.column(exposed(parent, col.name), table="x", quoted=True))
    cond = tree.sql(dialect="postgres")
    key = _ident(exposed(parent, edge.parent_column))
    fk = _ident(exposed(child, edge.child_column))
    ptable, ctable = qualified(parent.pgwire_name), qualified(child.pgwire_name)
    _n, rows = await _governed(f"SELECT COUNT(*) AS n FROM {ptable} x WHERE {cond}")
    parents = int(rows[0][0])
    if parents == 0:
        return None
    _n, rows = await _governed(
        f"SELECT n, COUNT(*) AS m FROM (SELECT c.{fk} AS k, COUNT(*) AS n FROM {ctable} c "
        f"WHERE c.{fk} IN (SELECT x.{key} FROM {ptable} x WHERE {cond}) GROUP BY c.{fk}) s "
        f"GROUP BY n"
    )
    histogram = {int(n): int(m) for n, m in rows}
    with_children = sum(histogram.values())
    if parents > with_children:
        histogram[0] = histogram.get(0, 0) + parents - with_children
    return _quantiles(histogram, QUANTILE_POINTS), parents


async def _dependence_entries(
    state: Any, planned: list[PlannedTable], *, private: bool
) -> list[dict]:
    """For the report (REQ-1939, DEPENDENCE KEPT): each pair of numbers the copula draws, its
    measured rank correlation beside the generated rows' own; each column that keeps its own
    draw though the run ties it to others; and, for a private dataset, that the network is not
    kept, its structure being chosen from the data."""
    from provisa.profiler.run import _governed
    from provisa.profiler.statement import _ident, qualified

    def row(t: str, column: str | None, measure: str, source: Any, synthetic: Any, note: str):
        delta = None if source is None or synthetic is None else synthetic - source
        return {
            "table_name": t,
            "column_name": column,
            "measure": measure,
            "source_value": source,
            "synthetic_value": synthetic,
            "delta": delta,
            "note": note,
        }

    exposed = _exposer(state)
    out = []
    for p in planned:
        dep = p.plan.dependence
        if dep is None:
            continue
        nodes = dep.nodes
        measured = p.table.profile.dependence.spearman
        numbers = [c for c in dep.copula if nodes[c].kind == "number"]
        for i, a in enumerate(numbers):
            for b in numbers[i + 1 :]:
                rho = measured.get((a, b), measured.get((b, a)))
                if rho is None:
                    continue
                ca, cb = _ident(exposed(p.table, a)), _ident(exposed(p.table, b))
                _n, rows = await _governed(
                    f"SELECT CORR(ra, rb) FROM (SELECT PERCENT_RANK() OVER (ORDER BY x.{ca}) AS ra, "
                    f"PERCENT_RANK() OVER (ORDER BY x.{cb}) AS rb FROM "
                    f"{qualified(p.table.pgwire_name)} x WHERE x.{ca} IS NOT NULL "
                    f"AND x.{cb} IS NOT NULL) s"
                )
                got = rows[0][0] if rows else None
                out.append(
                    row(
                        p.plan.name,
                        f"{a}~{b}",
                        "dependence_spearman",
                        rho,
                        None if got is None else float(got),
                        "rank correlation, measured beside generated",
                    )
                )
        if dep.copula:
            out.append(
                row(
                    p.plan.name,
                    None,
                    "dependence_copula_shrink",
                    None,
                    dep.shrink,
                    "the measured correlations taken toward independence by this share so that "
                    "they can be drawn together (0: as measured); see each pair beside",
                )
            )
        for c in dep.kept_own:
            out.append(
                row(
                    p.plan.name,
                    c,
                    "dependence_kept_own",
                    None,
                    None,
                    "the profile ties it to other columns; its draw is its own",
                )
            )
        if private:
            out.append(
                row(
                    p.plan.name,
                    None,
                    "dependence_network_not_kept",
                    None,
                    None,
                    "a private dataset keeps rank correlations measured under ε, not the "
                    "dependency network, whose structure is chosen from the data",
                )
            )
    return out


def _condition_entries(state: Any, planned: list[PlannedTable]) -> list[dict]:
    """Each conditional fan-out's parents and the children they were generated with, for the
    report (REQ-1939), counted by the engine as the generation statement counts them."""
    from provisa.federation.execution_auth import SystemAuth, mint_system_token
    from provisa.synthetic.generate import condition_counts_sql

    out = []
    engine_rt = state.federation_engine
    for p in planned:
        if not p.plan.conditions:
            continue
        sql = condition_counts_sql(p.plan, engine_rt.engine.name)
        _schema, stream = engine_rt.execute_engine_stream(
            sql,
            authorization=SystemAuth(
                mint_system_token(), reason=f"synthetic:{p.plan.name}:conditions", expected_sql=sql
            ),
        )
        try:
            rows = sorted(tuple(r.values()) for b in stream for r in b.to_pylist())
        finally:
            stream.close()
        for k, parents, children in rows:
            condition = p.plan.conditions[int(k)].condition
            for measure, value in (
                ("conditional_parents", parents),
                ("conditional_children", children),
            ):
                out.append(
                    {
                        "table_name": p.plan.name,
                        "column_name": None,
                        "measure": measure,
                        "source_value": None,
                        "synthetic_value": float(value),
                        "delta": None,
                        "note": condition,
                    }
                )
    return out


async def _assertion_entries(assertions: tuple[str, ...]) -> list[dict]:
    """Each assertion run over the generated tables as the org admin, with its result: true,
    false, or the error it ran into. An assertion never rejects, regenerates or alters the data
    (REQ-1939, ASSERTIONS); its failure to run is its reported result."""
    from provisa.profiler.run import _governed

    out = []
    for statement in assertions:
        value: float | None
        try:
            _names, rows = await _governed(statement)
            result = rows[0][0] if rows and rows[0] else None
            value = None if result is None else (1.0 if bool(result) else 0.0)
            note = statement if result is not None else f"{statement} -- returned no value"
        except Exception as exc:  # noqa: BLE001 -- REQ-1939: the statement's error is its result
            value, note = None, f"{statement} -- {type(exc).__name__}: {exc}"
        out.append(
            {
                "table_name": "",
                "column_name": None,
                "measure": "assertion",
                "source_value": None,
                "synthetic_value": value,
                "delta": None,
                "note": note,
            }
        )
    return out


def _exposer(state: Any) -> Any:
    from provisa.profiler.run import PROFILE_ROLE

    p2s = state.contexts[PROFILE_ROLE].physical_to_sql

    def exposed(t: DatasetTable, column: str) -> str:
        name = p2s.get((t.table_id, column))
        if name is None:
            raise DatasetRefused(f"{t.name}.{column} is not readable by {PROFILE_ROLE}")
        return name

    return exposed


async def _private_tables(state: Any, tables: list, edges: list, row: Any) -> tuple:
    from provisa.profiler.run import _governed
    from provisa.profiler.statement import qualified
    from provisa.synthetic.private_run import private_tables

    return await private_tables(
        tables,
        edges,
        epsilon=row.private_epsilon,
        seed=row.seed,
        governed=_governed,
        exposed=_exposer(state),
        qualified=qualified,
        conditions=row.fanout_conditions,
    )


async def _private_condition(
    state: Any, measurer: Any, by_id: dict, edges: list, c: Any
) -> tuple[float, ...] | None:
    """A measured conditional fan-out, under ε (REQ-1939, DIFFERENTIAL PRIVACY)."""
    import sqlglot
    from sqlglot import exp

    from provisa.profiler.statement import qualified
    from provisa.synthetic.private_run import fanout

    exposed = _exposer(state)
    edge = next(e for e in edges if e.relationship == c.relationship)
    parent, child = by_id[edge.parent_id], by_id[edge.child_id]
    tree = sqlglot.parse_one(c.condition, read="postgres")
    for col in list(tree.find_all(exp.Column)):
        col.replace(exp.column(exposed(parent, col.name), table="x", quoted=True))
    cond = tree.sql(dialect="postgres")
    sketch = await fanout(
        measurer, parent, child, edge, exposed, qualified, parent.profile.row_count, cond
    )
    return None if all(v == 0 for v in sketch) else sketch


async def generate(state: Any, dataset_id: str) -> None:
    """Generate (or regenerate) ``dataset_id`` in the bound environment, then report on it."""
    from provisa.api.app import _rebuild_schemas

    _require_non_prod(_env())
    async with state.model_db.acquire() as conn:
        row = await datasets.get_dataset(conn, dataset_id)
        await datasets.set_status(conn, dataset_id, "generating", error=None)
    landed: list[str] = []  # the real samples landed for the closeness check, dropped below (W1)
    try:
        tables, relationships, registered = await _dataset_tables(state, row)
        edges = edges_of(relationships)
        measured_conditions = [c for c in row.fanout_conditions if c.count == {"measured": True}]
        budget = measurer = None
        private_differences: dict = {}
        if row.private_epsilon is not None:
            # REQ-1939, DIFFERENTIAL PRIVACY: every statistic generation draws from is measured
            # from the tables under ε; nothing is read from the profile runs' recorded values.
            tables, budget, measurer, private_differences = await _private_tables(
                state, tables, edges, row
            )
        measured = {
            (t.table_id, c): await _measure(state, t, c) for t, c in to_measure(tables, edges)
        }
        distances = (
            private_differences
            if budget is not None
            else {
                (t.table_id, c): await _measure_difference(state, t, c, other)
                for t, c, other in distances_to_measure(tables, edges)
            }
        )
        pinned = await _pinned_runs(state, row, tables, registered)
        by_id = {t.table_id: t for t in tables}
        condition_sketches: dict[tuple[str | None, str], tuple[float, ...] | None] = {}
        for c in measured_conditions:
            if measurer is not None:
                condition_sketches[(c.relationship, c.condition)] = await _private_condition(
                    state, measurer, by_id, edges, c
                )
            else:
                found = await _measure_condition(state, by_id, edges, c)
                condition_sketches[(c.relationship, c.condition)] = (
                    None if found is None else found[0]
                )
        if budget is not None and abs(budget.charged - budget.epsilon) > 1e-9 * budget.epsilon:
            # Composition: the statistics planned are the statistics measured.
            raise RuntimeError(
                f"the private dataset's statistics were charged ε {budget.charged!r}, not its "
                f"ε {budget.epsilon!r}"
            )
        planned = plan_tables(
            tables,
            edges,
            seed=row.seed,
            names={tid: reg["table_name"] for tid, reg in registered.items()},
            count_rows=lambda plan: _count_rows(state, plan),
            measure=lambda t, c: measured[(t.table_id, c)],
            distance=lambda t, c: distances[(t.table_id, c)],
            pinned_run=lambda t, run_id: pinned[(t.table_id, run_id)],
            conditions=row.fanout_conditions,
            measure_condition=lambda e, cond: condition_sketches[(e.relationship, cond)],
        )
        addresses = await _generated_addresses(state, row.store_schema, planned, registered)
        closes: dict[str, Closeness] = {}
        real: dict[str, list[tuple[float, float]]] = {}
        close_extra: list[dict] = []
        if row.closeness_threshold is not None:
            fixed = closeness.fixed_columns(planned)
            planned = [
                replace(p, plan=replace(p.plan, fixed=fixed.get(p.plan.name, frozenset())))
                for p in planned
            ]
            closes, real, close_extra = await _closeness_of(
                state, row, planned, registered, addresses, landed
            )
        for p in planned:
            await _write_table(
                state,
                row.store_schema,
                p,
                registered[p.table.table_id],
                close=closes.get(p.plan.name),
            )
        # The second pass (REQ-1939, GENERATION IN PASSES): a table whose rules read its children
        # is generated again, the same rows, its rules now computed over the generated children.
        second = [p for p in planned if any(reads_children(g) for g in p.plan.group)]
        for p in _children_first(second):
            await _write_table(
                state,
                row.store_schema,
                p,
                registered[p.table.table_id],
                addresses,
                close=closes.get(p.plan.name),
            )
        if row.closeness_threshold is not None:
            close_extra += _closeness_entries(state, row, planned, closes, real, addresses)
        else:
            close_extra = closeness.not_checked()
    except Exception as exc:
        async with state.model_db.acquire() as conn:
            await datasets.set_status(
                conn, dataset_id, "failed", error=f"{type(exc).__name__}: {exc}"
            )
        raise
    finally:
        await _drop_samples(state, row.store_schema, landed)
    async with state.model_db.acquire() as conn:
        await datasets.set_status(conn, dataset_id, "generated", generated_at=datetime.now(UTC))
    # Its tables now read their copies here: the routes are republished with the model.
    await _rebuild_schemas()
    extra = (
        await _dependence_entries(state, planned, private=row.private_epsilon is not None)
        + _condition_entries(state, planned)
        + await _assertion_entries(row.assertions)
        + privacy_entries(budget)
        + close_extra
    )
    await report(state, dataset_id, planned, extra)


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


async def report(
    state: Any, dataset_id: str, planned: list[PlannedTable], extra: list[dict] | None = None
) -> None:
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
                target.keys,
                target.checks,
            )
        )
        synthetic = parse_profile_result(
            names, rows, target.columns, target.fanouts, target.keys, target.checks
        )
        entries += compare(p, synthetic, measures)
    entries += extra or []
    async with state.model_db.acquire() as conn:
        async with conn.transaction():
            await conn.execute_core(
                delete(synthetic_report).where(synthetic_report.c.dataset_id == dataset_id)
            )
            for e in entries:
                await conn.execute_core(insert(synthetic_report).values(dataset_id=dataset_id, **e))


# -- defining -----------------------------------------------------------------------------------


async def check_conditions_of(conn: Any, table_ids: list[int], conditions: list[Any]) -> None:
    """Refuse a conditional fan-out whose relationship is not one the dataset generates its
    child's rows by, or whose condition names a column its parent does not hold (REQ-1939)."""
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.fakes.sql_subset import read

    if not conditions:
        return
    tables = {t["id"]: t for t in await fetch_tables(conn)}
    rels = {r["id"]: r for r in await fetch_relationships(conn)}
    for c in conditions:
        r = rels.get(c.relationship)
        if r is None:
            raise DatasetRefused(
                f"conditional fan-out names relationship {c.relationship!r}, which does not exist"
            )
        edge = edges_of([r])
        if not edge or not {edge[0].parent_id, edge[0].child_id} <= set(table_ids):
            raise DatasetRefused(
                f"conditional fan-out names relationship {c.relationship!r}, whose parent and child "
                f"the dataset does not both generate"
            )
        parent = tables[edge[0].parent_id]
        held = {col["column_name"] for col in parent["columns"]}
        missing = sorted(read(c.condition, group=False).columns - held)
        if missing:
            raise DatasetRefused(
                f"conditional fan-out of {c.relationship!r}: its condition names {missing[0]!r}, "
                f"which {parent['table_name']!r} does not hold"
            )


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
