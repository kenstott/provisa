# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compliance reporting for enterprise tier (REQ-074)."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime

import asyncpg

_AUDIT_COLUMNS = (
    "id",
    "tenant_id",
    "user_id",
    "role_id",
    "query_hash",
    "table_ids",
    "source",
    "status_code",
    "duration_ms",
    "logged_at",
)


def _row_to_dict(row: asyncpg.Record) -> dict:
    d = dict(row)
    # Serialize non-JSON-native types
    if isinstance(d.get("logged_at"), datetime):
        d["logged_at"] = d["logged_at"].isoformat()
    if isinstance(d.get("tenant_id"), object) and not isinstance(
        d.get("tenant_id"), (str, type(None))
    ):
        d["tenant_id"] = str(d["tenant_id"])
    if isinstance(d.get("table_ids"), list):
        d["table_ids"] = list(d["table_ids"])
    if isinstance(d.get("id"), int):
        d["id"] = d["id"]
    return d


async def export_audit_log(
    conn: asyncpg.Connection,
    tenant_id: str | None,
    start_ts: datetime,
    end_ts: datetime,
    format: str = "json",
) -> str:
    rows = await conn.fetch(
        "SELECT id, tenant_id, user_id, role_id, query_hash, table_ids,"
        "       source, status_code, duration_ms, logged_at"
        " FROM query_audit_log"
        " WHERE tenant_id = $1"
        "   AND logged_at >= $2"
        "   AND logged_at <= $3"
        " ORDER BY logged_at ASC",
        tenant_id,
        start_ts,
        end_ts,
    )

    records = [_row_to_dict(r) for r in rows]

    if format == "json":
        return json.dumps(records, default=str)

    if format == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(_AUDIT_COLUMNS))
        writer.writeheader()
        writer.writerows(records)
        return buf.getvalue()

    raise ValueError(f"Unsupported format: {format!r}")
