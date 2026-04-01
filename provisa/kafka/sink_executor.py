# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka sink executor (REQ-176 through REQ-181).

When a dataset change event fires, finds approved queries with change_event
sinks targeting the changed table, re-executes them, and publishes results
to the configured Kafka topic.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from decimal import Decimal

log = logging.getLogger(__name__)


class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


async def trigger_sinks_for_table(table_name: str, state) -> int:
    """Find and execute sinks triggered by a change to the given table.

    Returns the number of sinks triggered.
    """
    if state.pg_pool is None:
        return 0

    async with state.pg_pool.acquire() as conn:
        # Find approved queries with change_event sinks that target this table
        rows = await conn.fetch(
            """
            SELECT pq.id, pq.stable_id, pq.query_text, pq.sink_topic,
                   pq.sink_key_column
            FROM persisted_queries pq
            WHERE pq.status = 'approved'
              AND pq.sink_trigger = 'change_event'
              AND pq.sink_topic IS NOT NULL
              AND $1 = ANY(
                  SELECT rt.table_name FROM registered_tables rt
                  WHERE rt.id = ANY(pq.target_tables)
              )
            """,
            table_name,
        )

    if not rows:
        return 0

    triggered = 0
    for row in rows:
        try:
            await _execute_and_publish(
                query_text=row["query_text"],
                sink_topic=row["sink_topic"],
                key_column=row["sink_key_column"],
                stable_id=row["stable_id"],
                state=state,
            )
            triggered += 1
            log.info(
                "Sink triggered: query %s → topic %s",
                row["stable_id"], row["sink_topic"],
            )
        except Exception:
            log.exception(
                "Sink execution failed for query %s", row["stable_id"],
            )

    return triggered


async def _execute_and_publish(
    query_text: str,
    sink_topic: str,
    key_column: str | None,
    stable_id: str,
    state,
) -> None:
    """Execute a query and publish results to Kafka."""
    from provisa.compiler.parser import parse_query
    from provisa.compiler.rls import RLSContext, inject_rls
    from provisa.compiler.sql_gen import compile_query
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino

    # Execute as admin role (sink runs server-side)
    role_id = "admin"
    if role_id not in state.schemas:
        log.warning("Admin role not available for sink execution")
        return

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    document = parse_query(schema, query_text)
    compiled_queries = compile_query(document, ctx)
    if not compiled_queries:
        return

    compiled = compiled_queries[0]
    compiled = inject_rls(compiled, ctx, rls)

    # Always route through Trino for sinks (catalog-qualified)
    compiled = compile_query(document, ctx, use_catalog=True)[0]
    compiled = inject_rls(compiled, ctx, rls)
    trino_sql = transpile_to_trino(compiled.sql)

    if state.trino_conn is None:
        log.warning("Trino not connected — cannot execute sink")
        return

    result = execute_trino(state.trino_conn, trino_sql, compiled.params)

    # Publish to Kafka
    bootstrap = os.environ.get(
        "PROVISA_CHANGE_EVENT_BOOTSTRAP",
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
    )
    if not bootstrap:
        log.warning("No Kafka bootstrap servers for sink publishing")
        return

    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": bootstrap})

    for row in result.rows:
        obj = {result.column_names[i]: v for i, v in enumerate(row)}
        key = None
        if key_column and key_column in obj:
            key = str(obj[key_column]).encode()
        producer.produce(
            sink_topic,
            key=key,
            value=json.dumps(obj, cls=_Encoder).encode(),
        )

    producer.flush(timeout=10)
    log.info(
        "Sink published %d rows to %s (query %s)",
        len(result.rows), sink_topic, stable_id,
    )
