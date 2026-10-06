# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4e1f27-3a5d-4b96-9e02-6d7b1a8c3f54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's Kafka sources and their topics, in its model store (REQ-147, REQ-1919).

A configuration seeds them once; from then on the process reads them from here (the topic
tables, windows and discriminators, the engine's Kafka catalogs), never from the file. The
source is held as its configuration wrote it (``spec``, a credential staying its reference),
with its topics as ``kafka_topics`` rows.
"""

# Requirements: REQ-147, REQ-1919

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core import model_change
from provisa.core.schema_org import kafka_sources, kafka_topics

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _topic_table_name(topic: dict[str, Any]) -> str:
    """The name the topic's table is registered under (config_loader.kafka_topics_as_tables)."""
    return topic.get("table_name") or topic.get("id", "").replace("-", "_")


async def upsert(conn: "Connection", spec: dict[str, Any]) -> None:
    """Add the Kafka source and its topics, or replace their definition. A topic the spec does not
    name is left as it is (REQ-1919: nothing is removed)."""
    model_change.name("upsert", "kafka source", spec["id"])  # REQ-1524
    auth = spec.get("auth") or {}
    await conn.upsert(
        kafka_sources,
        {
            "id": spec["id"],
            "bootstrap_servers": spec.get("bootstrap_servers", ""),
            "schema_registry_url": spec.get("schema_registry_url"),
            "auth_type": auth.get("type") if isinstance(auth, dict) else None,
            "spec": spec,
        },
        index_elements=["id"],
        update_columns=["bootstrap_servers", "schema_registry_url", "auth_type", "spec"],
    )
    for topic in spec.get("topics", []):
        values = {
            "source_id": spec["id"],
            "topic": topic["topic"],
            "table_name": _topic_table_name(topic),
            "schema_source": topic.get("schema_source", "registry"),
            "value_format": topic.get("value_format", "json"),
            "columns": list(topic.get("columns", [])),
        }
        await conn.upsert(
            kafka_topics,
            values,
            index_elements=["source_id", "topic"],
            update_columns=["table_name", "schema_source", "value_format", "columns"],
        )


async def list_specs(conn: "Connection") -> list[dict[str, Any]]:
    """Every Kafka source a configuration seeded, as it was written, by id."""
    rows = (
        await conn.execute_core(
            select(kafka_sources.c.spec)
            .where(kafka_sources.c.spec.is_not(None))
            .order_by(kafka_sources.c.id)
        )
    ).fetchall()
    return [dict(r.spec) for r in rows]
