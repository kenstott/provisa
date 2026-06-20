# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SLA monitoring for enterprise tier (REQ-074)."""

from __future__ import annotations

import asyncpg

SLA_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS query_sla_log (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID,
    duration_ms INT NOT NULL,
    status_code INT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sla_tenant_window ON query_sla_log (tenant_id, window_start DESC);
"""

_SLA_P99_LIMIT_MS = 5000
_SLA_AVAILABILITY_MIN = 99.9


async def record_query_sla(
    conn: asyncpg.Connection,
    duration_ms: int,
    status_code: int,
    tenant_id: str | None,
) -> None:
    await conn.execute(
        "INSERT INTO query_sla_log (tenant_id, duration_ms, status_code, window_start)"
        " VALUES ($1, $2, $3, date_trunc('minute', now()))",
        tenant_id,
        duration_ms,
        status_code,
    )


async def get_sla_summary(
    conn: asyncpg.Connection,
    tenant_id: str | None,
    hours: int = 24,
) -> dict:
    row = await conn.fetchrow(
        """
        SELECT
            avg(duration_ms)::float                                         AS avg_ms,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms)::float AS p50_ms,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::float AS p95_ms,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms)::float AS p99_ms,
            100.0 * sum(CASE WHEN status_code < 500 THEN 1 ELSE 0 END)::float
                / nullif(count(*), 0)                                       AS availability_pct
        FROM query_sla_log
        WHERE tenant_id = $1
          AND recorded_at >= now() - ($2 * interval '1 hour')
        """,
        tenant_id,
        hours,
    )
    return dict(row)


def check_sla_breach(summary: dict) -> bool:
    p99 = summary["p99_ms"]
    availability = summary["availability_pct"]
    return p99 > _SLA_P99_LIMIT_MS or availability < _SLA_AVAILABILITY_MIN
