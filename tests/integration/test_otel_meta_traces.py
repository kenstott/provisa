# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: OTel traces gathered and related back via _meta._traces/_queries.

Strategy: bypass the S3/otlp2parquet pipeline by inserting a synthetic trace row
directly into otel.signals.traces via trino_conn, then verify that a GraphQL
query with _meta { _traces { ... } _queries { ... } } returns that row.

This tests the full path:
  Trino Iceberg insert → otel.signals.traces → _meta._traces resolver
  otel.signals.queries view   → _meta._queries resolver
"""

from __future__ import annotations

import time
import uuid

import pytest

pytestmark = [pytest.mark.integration]

TABLE_NAME = "pets"
SERVICE_NAME = "provisa"
SPAN_NAME = "provisa.query.trino"


def _insert_test_trace(trino_conn, table_name: str, trace_id: str, span_id: str) -> None:
    """Insert one synthetic span row into otel.signals.traces."""
    cur = trino_conn.cursor()
    # Use microseconds since epoch as timestamp (Iceberg BIGINT convention)
    ts_us = int(time.time() * 1_000_000)
    cur.execute(
        """
        INSERT INTO otel.signals.traces (
            trace_id, span_id, span_name, service_name,
            "timestamp", table_name, domain_id, role_id, _date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
        """,
        [trace_id, span_id, SPAN_NAME, SERVICE_NAME, ts_us, table_name, "pet-store", "admin"],
    )


def _trace_exists(trino_conn, trace_id: str) -> bool:
    """Return True when the trace row is visible in otel.signals.traces."""
    cur = trino_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM otel.signals.traces WHERE trace_id = ?",
        [trace_id],
    )
    row = cur.fetchone()
    return bool(row and row[0] > 0)


def _query_exists(trino_conn, trace_id: str) -> bool:
    """Return True when the trace row is visible via otel.signals.queries view."""
    cur = trino_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM otel.signals.queries WHERE trace_id = ?",
        [trace_id],
    )
    row = cur.fetchone()
    return bool(row and row[0] > 0)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestOtelMetaTraces:
    """Verify _meta._traces and _meta._queries return data linked to table_name."""

    async def test_inserted_trace_visible_in_trino(self, trino_conn):
        """Synthetic trace row is readable from otel.signals.traces."""
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        _insert_test_trace(trino_conn, TABLE_NAME, trace_id, span_id)
        assert _trace_exists(trino_conn, trace_id), (
            f"trace_id {trace_id!r} not found in otel.signals.traces after insert"
        )

    async def test_inserted_trace_visible_via_queries_view(self, trino_conn):
        """Synthetic trace row (span_name LIKE 'provisa.query%') is in queries view."""
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        _insert_test_trace(trino_conn, TABLE_NAME, trace_id, span_id)
        assert _query_exists(trino_conn, trace_id), (
            f"trace_id {trace_id!r} not found in otel.signals.queries after insert"
        )

    async def test_meta_traces_returns_rows_for_table(self, graphql_client, trino_conn):
        """_meta._traces returns at least one row after a trace is inserted for the table."""
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        _insert_test_trace(trino_conn, TABLE_NAME, trace_id, span_id)

        resp = await graphql_client.post(
            "/data/graphql",
            json={
                "query": """
                query TestMetaTraces @noCache {
                  ps__pets(limit: 1) {
                    id
                    _meta {
                      tableName
                      _traces(limit: 5) {
                        spanName
                        serviceName
                        tableName
                        timestamp
                      }
                    }
                  }
                }
                """
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, f"GraphQL errors: {body.get('errors')}"

        pets = body["data"]["ps__pets"]
        assert pets, "ps__pets returned no rows"

        traces = pets[0]["_meta"]["_traces"]
        assert traces, (
            "_meta._traces returned no rows. "
            "The OTel trace was inserted but the resolver returned nothing. "
            "Check that otel.signals.traces table_name = 'pets' and the join is correct."
        )
        span_names = [t["spanName"] for t in traces]
        assert any("provisa.query" in sn for sn in span_names), (
            f"Expected a provisa.query* span. Got: {span_names}"
        )

    async def test_meta_queries_returns_rows_for_table(self, graphql_client, trino_conn):
        """_meta._queries returns at least one row via the queries view."""
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        _insert_test_trace(trino_conn, TABLE_NAME, trace_id, span_id)

        resp = await graphql_client.post(
            "/data/graphql",
            json={
                "query": """
                query TestMetaQueries @noCache {
                  ps__pets(limit: 1) {
                    id
                    _meta {
                      tableName
                      _queries(limit: 5) {
                        spanName
                        serviceName
                        tableName
                        timestamp
                      }
                    }
                  }
                }
                """
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, f"GraphQL errors: {body.get('errors')}"

        pets = body["data"]["ps__pets"]
        assert pets, "ps__pets returned no rows"

        queries = pets[0]["_meta"]["_queries"]
        assert queries, (
            "_meta._queries returned no rows. "
            "Check that otel.signals.queries view exists and filters span_name LIKE 'provisa.query%'."
        )
        assert all(q["tableName"] == TABLE_NAME for q in queries), (
            f"Expected all _queries rows to have tableName={TABLE_NAME!r}. Got: {queries}"
        )
