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

# Requirements: REQ-074

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_org import query_audit_log

if TYPE_CHECKING:
    from provisa.core.database import Connection

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


def _row_to_dict(row: dict) -> dict:
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


async def export_audit_log(  # REQ-074
    conn: "Connection",
    tenant_id: str | None,
    start_ts: datetime,
    end_ts: datetime,
    format: str = "json",
) -> str:
    result = await conn.execute_core(
        select(
            query_audit_log.c.id,
            query_audit_log.c.tenant_id,
            query_audit_log.c.user_id,
            query_audit_log.c.role_id,
            query_audit_log.c.query_hash,
            query_audit_log.c.table_ids,
            query_audit_log.c.source,
            query_audit_log.c.status_code,
            query_audit_log.c.duration_ms,
            query_audit_log.c.logged_at,
        )
        .where(query_audit_log.c.tenant_id == tenant_id)
        .where(query_audit_log.c.logged_at >= start_ts)
        .where(query_audit_log.c.logged_at <= end_ts)
        .order_by(query_audit_log.c.logged_at.asc())
    )

    records = [_row_to_dict(dict(r._mapping)) for r in result.fetchall()]

    if format == "json":
        return json.dumps(records, default=str)

    if format == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(_AUDIT_COLUMNS))
        writer.writeheader()
        writer.writerows(records)
        return buf.getvalue()

    raise ValueError(f"Unsupported format: {format!r}")
