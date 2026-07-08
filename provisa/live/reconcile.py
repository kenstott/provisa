# Copyright (c) 2026 Kenneth Stott
# Canary: ce341f4e-0d36-4279-aec3-075183c061ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LiveEngine reconciliation from persisted per-table live config (REQ-565, REQ-813)."""

from __future__ import annotations

import asyncpg

from provisa.core.change_signal import is_poll, resolve_effective  # REQ-932
from provisa.live.engine import LiveSpec


async def reconcile_live_engine(conn: asyncpg.Connection, engine) -> None:  # REQ-565, REQ-932
    """Rebuild the engine's poll jobs from registered_tables.

    A table drives a watermark poll job when its effective change_signal is a poll signal
    (ttl/probe/ttl_probe) and it has a watermark_column (the append/subscribable gate). The
    signal resolves table.change_signal → legacy live.strategy → default; the watermark resolves
    the top-level Table.watermark_column → legacy live.watermark_column (REQ-932).
    """
    if engine is None:
        return

    rows = await conn.fetch(
        "SELECT source_id, schema_name, table_name, live, change_signal, watermark_column "
        "FROM registered_tables WHERE live IS NOT NULL"
    )
    specs: list[LiveSpec] = []
    for row in rows:
        live = row["live"]
        if not live:
            continue
        sig = resolve_effective(row["change_signal"], None, live.get("strategy"))
        if not is_poll(sig):
            continue
        watermark = row["watermark_column"] or live.get("watermark_column")
        if not watermark:
            continue
        catalog = row["source_id"].replace("-", "_")
        sql = f'SELECT * FROM {catalog}."{row["schema_name"]}"."{row["table_name"]}"'
        kafka_outputs = [
            {
                "bootstrap_servers": o["bootstrap_servers"],
                "topic": o["topic"],
                "key_column": o.get("key_column"),
            }
            for o in live.get("outputs", [])
            if o.get("type") == "kafka" and o.get("topic") and o.get("bootstrap_servers")
        ]
        specs.append(
            LiveSpec(
                query_id=f"{row['source_id']}.{row['table_name']}",
                sql=sql,
                watermark_column=watermark,
                poll_interval=int(live.get("poll_interval", 10)),
                kafka_outputs=kafka_outputs,
            )
        )
    engine.reconcile(specs)
