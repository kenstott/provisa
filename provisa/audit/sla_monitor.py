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

import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import insert, select

from provisa.core.schema_org import query_sla_log

if TYPE_CHECKING:
    from provisa.core.database import Connection

# Requirements: REQ-074, REQ-506

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
# A request counts as "available" when its status is below the HTTP 5xx server-error band.
_HTTP_SERVER_ERROR = 500


def _percentile_cont(sorted_vals: list[float], p: float) -> float:
    """Linear-interpolated continuous percentile, matching Postgres percentile_cont."""
    n = len(sorted_vals)
    rank = p * (n - 1)
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


async def record_query_sla(  # REQ-074, REQ-506
    conn: "Connection",
    duration_ms: int,
    status_code: int,
    tenant_id: str | None,
) -> None:
    # date_trunc('minute', now()) has no SQLite equivalent — truncate in Python for portability.
    window_start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    await conn.execute_core(
        insert(query_sla_log).values(
            tenant_id=uuid.UUID(tenant_id) if tenant_id is not None else None,
            duration_ms=duration_ms,
            status_code=status_code,
            window_start=window_start,
        )
    )


async def get_sla_summary(  # REQ-074, REQ-506
    conn: "Connection",
    tenant_id: str | None,
    hours: int = 24,
) -> dict:
    # percentile_cont / interval arithmetic have no SQLite equivalent — aggregate in Python.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    tid = uuid.UUID(tenant_id) if tenant_id is not None else None
    result = await conn.execute_core(
        select(query_sla_log.c.duration_ms, query_sla_log.c.status_code).where(
            query_sla_log.c.tenant_id == tid,
            query_sla_log.c.recorded_at >= cutoff,
        )
    )
    rows = result.fetchall()
    count = len(rows)
    if count == 0:
        # Empty window: the SQL aggregate yields NULLs (avg/percentile of no rows, and the
        # nullif(count,0) availability divisor) — preserve that shape.
        return {
            "avg_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "availability_pct": None,
        }
    durations = sorted(float(r.duration_ms) for r in rows)
    available = sum(1 for r in rows if r.status_code < _HTTP_SERVER_ERROR)
    return {
        "avg_ms": sum(durations) / count,
        "p50_ms": _percentile_cont(durations, 0.50),
        "p95_ms": _percentile_cont(durations, 0.95),
        "p99_ms": _percentile_cont(durations, 0.99),
        "availability_pct": 100.0 * available / count,
    }


def check_sla_breach(summary: dict) -> bool:  # REQ-074, REQ-506
    p99 = summary["p99_ms"]
    availability = summary["availability_pct"]
    return p99 > _SLA_P99_LIMIT_MS or availability < _SLA_AVAILABILITY_MIN
