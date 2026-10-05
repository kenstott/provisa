# Copyright (c) 2026 Kenneth Stott
# Canary: 354f3b98-34d9-4130-b6ff-8351723f551f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Kafka table on a SingleStore store lands through a continuous SingleStore PIPELINE (REQ-990,
REQ-848 PIPELINE_LAND) instead of a Provisa listener relaying its messages.

- :func:`ensure` creates the table's pipeline, or leaves it running when its definition is
  unchanged (it keeps its offsets). A changed definition gets a new pipeline and the old one is
  dropped. A new pipeline starts at the topic's latest offsets, as the relay consumer does.
- :func:`sweep` drops this org and region's Kafka pipelines whose table is no longer wired: the
  table was set as draft, deleted or retired. App shutdown never stops a pipeline. It lives in the
  database and serves every worker.
- :func:`ripple_new_batches` is the region's scheduled poll. It reads each pipeline's new successful
  batches from SingleStore's batch metadata (no rows) and ripples the table's change to the views
  that read it, and its freshness, through the one ripple helper.

The functions that touch the store are synchronous and run on a worker thread.
"""

from __future__ import annotations

from typing import Any

from provisa.federation import singlestore_pipeline as sp


def _execute(conn: Any, statements: list[str], *, tolerate: tuple[int, ...] = ()) -> None:
    raw = conn.connection.driver_connection
    cursor = raw.cursor()
    try:
        for sql in statements:
            try:
                cursor.execute(sql)
            except Exception as exc:  # noqa: BLE001 — only the named error numbers pass
                if getattr(exc, "errno", None) not in tolerate:
                    raise
    finally:
        cursor.close()
    raw.commit()


def _query(conn: Any, sql: str) -> list[tuple]:
    raw = conn.connection.driver_connection
    cursor = raw.cursor()
    try:
        cursor.execute(sql)
        return list(cursor.fetchall())
    finally:
        cursor.close()


def _running(conn: Any, schema: str) -> dict[str, str]:
    """This schema's Kafka pipelines and their state."""
    return {name: state for name, state in _query(conn, sp.kafka_pipelines_query(schema))}


def _drop(conn: Any, schema: str, pipeline: str) -> None:
    _execute(conn, sp.teardown(schema, pipeline, procedure=sp.kafka_procedure_for(pipeline)))


def ensure(
    sa_engine: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    origin: sp.LandOrigin,
    bootstrap: str,
    field_mapping: dict[str, str] | None = None,
    schema_registry: str | None = None,
) -> str:
    """Make ``schema.table`` land from ``origin`` through its continuous pipeline, and return the
    pipeline's name. ``columns`` are (name, IR type) pairs, keyed by ``pk_columns``."""
    from provisa.federation.materialize_exec import build_table
    from provisa.federation.sqlalchemy_runtime import _ensure_schema, _ensure_table

    definition = sp.kafka_definition(
        origin, columns, pk_columns, field_mapping, schema_registry, bootstrap
    )
    name = sp.kafka_pipeline_name(schema, table, definition)
    prefix = sp.kafka_table_prefix(schema, table)
    keyed = build_table(schema, table, columns, tuple(pk_columns), dialect_name="singlestoredb")
    store_types = [
        (column.name, column.type.compile(dialect=sa_engine.dialect)) for column in keyed.columns
    ]
    with sa_engine.connect() as conn:
        _ensure_schema(conn, schema)
        _ensure_table(conn, keyed)
        conn.commit()
        running = _running(conn, schema)
        for old in [n for n in running if n.startswith(prefix) and n != name]:
            _drop(conn, schema, old)  # the table's definition changed
        if name not in running:
            statements = sp.kafka_pipeline_ddl(
                schema=schema,
                pipeline=name,
                procedure=sp.kafka_procedure_for(name),
                bootstrap=bootstrap,
                origin=origin,
                table=table,
                columns=store_types,
                pk_columns=pk_columns,
                field_mapping=field_mapping,
                schema_registry=schema_registry,
            )
            try:
                _execute(conn, [*statements, sp.offsets_latest(schema, name)])
            except Exception:
                # Another worker wiring the same table may have created it first.
                if name not in _running(conn, schema):
                    raise
        _execute(conn, [sp.start(schema, name)], tolerate=(sp.ALREADY_RUNNING,))
    return name


def sweep(
    sa_engine: Any,
    *,
    schema: str,
    keep: set[str],
    keep_prefixes: frozenset[str] | set[str] = frozenset(),
) -> list[str]:
    """Drop every Kafka pipeline in ``schema`` (this org and region's replicas schema) not in
    ``keep`` and not of a table in ``keep_prefixes`` (a wired table whose pipeline could not be
    recreated this pass keeps the one it has), with its procedure. Returns the names dropped."""
    with sa_engine.connect() as conn:
        dropped = [
            name
            for name in _running(conn, schema)
            if name not in keep and not name.startswith(tuple(keep_prefixes))
        ]
        for name in dropped:
            _drop(conn, schema, name)
    return dropped


def new_batches(
    sa_engine: Any, *, schema: str, since: dict[str, int]
) -> dict[str, tuple[int, int]]:
    """For each pipeline in ``since`` (name → last batch id already seen), the newest successful
    batch id after it and the rows those batches changed. Pipelines with nothing new are absent."""
    if not since:
        return {}
    out: dict[str, tuple[int, int]] = {}
    with sa_engine.connect() as conn:
        rows = _query(conn, sp.batches_since_query(schema, since))
    for name, batch_id, inserted, updated, deleted in rows:
        _, changed = out.get(name, (0, 0))
        out[name] = (
            int(batch_id),
            changed + int(inserted or 0) + int(updated or 0) + int(deleted or 0),
        )
    return out


async def ripple_new_batches(state: Any, sa_engine: Any, *, schema: str) -> int:
    """The region's scheduled poll: ripple each pipeline-landed table whose pipeline has new
    successful batches that changed rows. The last batch seen is kept in the node's own state
    (``probe_token``), so a new holder of the region's claim carries on where the last left off.
    Returns the number of tables rippled."""
    import asyncio

    from provisa.events import queue
    from provisa.events.ripple import ripple

    pipelines: dict[str, str] = {
        node: name for node, name in (getattr(state, "kafka_pipelines", {}) or {}).items() if name
    }
    if not pipelines:
        return 0
    since: dict[str, int] = {}
    async with state.tenant_db.acquire() as conn:
        for node, name in pipelines.items():
            node_state = await queue.get_node_state(conn, node)
            token = node_state.get("probe_token") if node_state else None
            since[name] = int(token) if token else -1
    found = await asyncio.to_thread(new_batches, sa_engine, schema=schema, since=since)
    rippled = 0
    for node, name in pipelines.items():
        if name not in found:
            continue
        last_batch, changed = found[name]
        if changed:
            await ripple(
                state,
                node,
                event_type="delta",
                payload={"pipeline": name, "batch": last_batch, "rows": changed},
            )
            rippled += 1
        async with state.tenant_db.acquire() as conn:
            await queue.set_node_state(conn, node, probe_token=str(last_batch))
    return rippled
