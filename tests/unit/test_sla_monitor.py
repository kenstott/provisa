# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for SLA monitor and compliance reporter (REQ-074)."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.audit.compliance_reporter import export_audit_log
from provisa.audit.sla_monitor import check_sla_breach


# ---------------------------------------------------------------------------
# check_sla_breach
# ---------------------------------------------------------------------------


def test_check_sla_breach_p99_over_limit():
    summary = {"p99_ms": 5001.0, "availability_pct": 100.0}
    assert check_sla_breach(summary) is True


def test_check_sla_breach_p99_at_limit_passes():
    summary = {"p99_ms": 5000.0, "availability_pct": 100.0}
    assert check_sla_breach(summary) is False


def test_check_sla_breach_availability_below_threshold():
    summary = {"p99_ms": 100.0, "availability_pct": 99.8}
    assert check_sla_breach(summary) is True


def test_check_sla_breach_within_targets():
    summary = {"p99_ms": 200.0, "availability_pct": 99.95}
    assert check_sla_breach(summary) is False


def test_check_sla_breach_both_violated():
    summary = {"p99_ms": 9999.0, "availability_pct": 50.0}
    assert check_sla_breach(summary) is True


# ---------------------------------------------------------------------------
# export_audit_log — JSON
# ---------------------------------------------------------------------------

_TENANT_ID = "11111111-1111-1111-1111-111111111111"
_NOW = datetime(2026, 6, 20, 12, 0, 0, tzinfo=timezone.utc)
_START = datetime(2026, 6, 19, 0, 0, 0, tzinfo=timezone.utc)
_END = datetime(2026, 6, 20, 23, 59, 59, tzinfo=timezone.utc)

_FAKE_ROWS = [
    {
        "id": 1,
        "tenant_id": _TENANT_ID,
        "user_id": "alice",
        "role_id": "analyst",
        "query_hash": "abc123",
        "table_ids": ["orders", "customers"],
        "source": "graphql",
        "status_code": 200,
        "duration_ms": 42,
        "logged_at": _NOW,
    }
]


def _make_mock_conn(rows: list[dict]) -> AsyncMock:
    records = []
    for row in rows:
        rec = MagicMock()
        rec.__iter__ = MagicMock(return_value=iter(row.items()))
        rec.keys = MagicMock(return_value=list(row.keys()))
        rec.__getitem__ = MagicMock(side_effect=row.__getitem__)
        # Make dict(rec) work by supporting mapping protocol
        rec.items = MagicMock(return_value=row.items())
        records.append(rec)

    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=records)
    return conn


@pytest.mark.asyncio
async def test_export_audit_log_json_valid():
    conn = _make_mock_conn(_FAKE_ROWS)
    result = await export_audit_log(conn, _TENANT_ID, _START, _END, format="json")
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    record = parsed[0]
    assert record["user_id"] == "alice"
    assert record["status_code"] == 200
    assert "logged_at" in record
    assert "query_hash" in record


@pytest.mark.asyncio
async def test_export_audit_log_csv_valid():
    conn = _make_mock_conn(_FAKE_ROWS)
    result = await export_audit_log(conn, _TENANT_ID, _START, _END, format="csv")
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["user_id"] == "alice"
    assert rows[0]["status_code"] == "200"


@pytest.mark.asyncio
async def test_export_audit_log_csv_has_header():
    conn = _make_mock_conn(_FAKE_ROWS)
    result = await export_audit_log(conn, _TENANT_ID, _START, _END, format="csv")
    first_line = result.splitlines()[0]
    assert "user_id" in first_line
    assert "tenant_id" in first_line
    assert "logged_at" in first_line


@pytest.mark.asyncio
async def test_export_audit_log_unsupported_format():
    conn = _make_mock_conn([])
    with pytest.raises(ValueError, match="Unsupported format"):
        await export_audit_log(conn, _TENANT_ID, _START, _END, format="xml")
