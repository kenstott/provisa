# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka time-window and discriminator injection.

Auto-injects WHERE clauses for Kafka-backed tables:
- Time window: bounds queries to a recent period (prevents unbounded reads)
- Discriminator: filters a shared topic to a single message type
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from provisa.compiler.sql_gen import CompiledQuery, CompilationContext


@dataclass(frozen=True)
class KafkaTableConfig:
    """Per-table Kafka config for injection."""

    window: str | None = None  # e.g. "1h"
    discriminator_field: str | None = None
    discriminator_value: str | None = None


def _parse_window(window: str) -> str:
    """Parse a human-friendly window like '1h', '30m', '24h' to a Trino INTERVAL.

    Returns a SQL fragment like "INTERVAL '1' HOUR".
    """
    match = re.match(r"^(\d+)\s*(h|m|d|s)$", window.strip().lower())
    if not match:
        raise ValueError(f"Invalid window format: {window!r}. Use e.g. '1h', '30m', '24h'.")

    value = match.group(1)
    unit = match.group(2)
    unit_map = {"h": "HOUR", "m": "MINUTE", "d": "DAY", "s": "SECOND"}
    return f"INTERVAL '{value}' {unit_map[unit]}"


def _inject_filter(sql: str, filter_expr: str) -> str:
    """Inject a filter expression into a SQL string."""
    where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
    if where_match:
        insert_pos = where_match.end()
        return sql[:insert_pos] + f" ({filter_expr}) AND" + sql[insert_pos:]
    else:
        insert_re = re.search(r"\b(ORDER\s+BY|LIMIT|OFFSET|$)", sql, re.IGNORECASE)
        insert_pos = insert_re.start() if insert_re else len(sql)
        return sql[:insert_pos] + f" WHERE {filter_expr} " + sql[insert_pos:]


def inject_kafka_filters(
    compiled: CompiledQuery,
    ctx: CompilationContext,
    source_types: dict[str, str],
    kafka_configs: dict[str, KafkaTableConfig],
) -> CompiledQuery:
    """Inject time-window and discriminator WHERE clauses for Kafka-backed tables.

    Args:
        compiled: The compiled query.
        ctx: Compilation context with table metadata.
        source_types: {source_id: source_type} map.
        kafka_configs: {table_name: KafkaTableConfig} map.

    Returns:
        Modified CompiledQuery with injected filters, or original if not Kafka.
    """
    root_table = ctx.tables.get(compiled.root_field)
    if not root_table:
        return compiled

    source_type = source_types.get(root_table.source_id, "")
    if source_type != "kafka":
        return compiled

    config = kafka_configs.get(root_table.table_name)
    if not config:
        return compiled

    sql = compiled.sql

    # Inject discriminator filter
    if config.discriminator_field and config.discriminator_value:
        safe_value = config.discriminator_value.replace("'", "''")
        discriminator_filter = (
            f'"{config.discriminator_field}" = \'{safe_value}\''
        )
        sql = _inject_filter(sql, discriminator_filter)

    # Inject time-window filter (skip if client already filters on _timestamp)
    if config.window and "_timestamp" not in sql.lower():
        interval = _parse_window(config.window)
        time_filter = f'"_timestamp" >= CURRENT_TIMESTAMP - {interval}'
        sql = _inject_filter(sql, time_filter)

    if sql == compiled.sql:
        return compiled

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=compiled.sources,
    )


# Backward-compatible alias
def inject_kafka_window(
    compiled: CompiledQuery,
    ctx: CompilationContext,
    source_types: dict[str, str],
    kafka_windows: dict[str, str],
) -> CompiledQuery:
    """Legacy wrapper — converts old-style kafka_windows to KafkaTableConfig."""
    root_table = ctx.tables.get(compiled.root_field)
    if not root_table:
        return compiled
    window = kafka_windows.get(root_table.source_id)
    if not window:
        return compiled
    configs = {root_table.table_name: KafkaTableConfig(window=window)}
    return inject_kafka_filters(compiled, ctx, source_types, configs)
