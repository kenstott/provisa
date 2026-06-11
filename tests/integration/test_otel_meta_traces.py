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

import os
import time
import uuid

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration]

TABLE_NAME = "pets"
SERVICE_NAME = "provisa"
SPAN_NAME = "provisa.query.trino"

QUERY_TEXT = "{ ps__pets(limit: 1) { id } }"

_BASE_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


def _admin_gql(query: str) -> dict:
    resp = httpx.post(f"{_BASE_URL}/admin/graphql", json={"query": query}, timeout=120)
    resp.raise_for_status()
    return resp.json()


@pytest_asyncio.fixture(scope="module", autouse=True)
async def _ensure_pets_registered():
    """Register source/domain/table via admin mutations so _rebuild_schemas runs."""
    src_result = _admin_gql(
        'mutation { createSource(input: {id: "pet-store-pg", type: "postgresql"}) { success } }'
    )
    inserted_source = src_result.get("data", {}).get("createSource", {}).get("success", False)

    dom_result = _admin_gql(
        'mutation { createDomain(input: {id: "pet-store", description: "Pet store"}) { success } }'
    )
    inserted_domain = dom_result.get("data", {}).get("createDomain", {}).get("success", False)

    tbl_result = _admin_gql("""
        mutation {
            registerTable(input: {
                sourceId: "pet-store-pg", domainId: "pet-store",
                schemaName: "pet_store", tableName: "pets",
                governance: "pre-approved",
                columns: [
                    {name: "id", isPrimaryKey: true, visibleTo: ["admin"], writableBy: [], unmaskedTo: []},
                    {name: "name", visibleTo: ["admin"], writableBy: [], unmaskedTo: []},
                    {name: "species", visibleTo: ["admin"], writableBy: [], unmaskedTo: []}
                ]
            }) { success message }
        }
    """)
    tbl_msg = tbl_result.get("data", {}).get("registerTable", {}).get("message", "")
    table_id: int | None = None
    if "id=" in tbl_msg:
        try:
            table_id = int(tbl_msg.split("id=")[1].rstrip(")"))
        except (ValueError, IndexError):
            pass

    yield

    if table_id is not None:
        _admin_gql(f'mutation {{ deleteTable(id: {table_id}) {{ success }} }}')
    if inserted_domain:
        _admin_gql('mutation { deleteDomain(id: "pet-store") { success } }')
    if inserted_source:
        _admin_gql('mutation { deleteSource(id: "pet-store-pg") { success } }')


def _insert_test_trace(trino_conn, table_name: str, trace_id: str, span_id: str) -> None:
    """Insert one synthetic span row into otel.signals.traces."""
    cur = trino_conn.cursor()
    # Use microseconds since epoch as timestamp (Iceberg BIGINT convention)
    ts_us = int(time.time() * 1_000_000)
    cur.execute(
        """
        INSERT INTO otel.signals.traces (
            trace_id, span_id, span_name, service_name,
            "timestamp", table_name, domain_id, role_id, query_text, _date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
        """,
        [
            trace_id,
            span_id,
            SPAN_NAME,
            SERVICE_NAME,
            ts_us,
            table_name,
            "pet-store",
            "admin",
            QUERY_TEXT,
        ],
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

    async def test_meta_traces_returns_rows_for_table(self, live_client, trino_conn):
        """_meta._traces returns at least one row after a trace is inserted for the table."""
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        _insert_test_trace(trino_conn, TABLE_NAME, trace_id, span_id)

        resp = await live_client.post(
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
        assert resp.status_code == 200, f"HTTP {resp.status_code}: {resp.text[:500]}"
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

    async def test_meta_queries_returns_rows_for_table(self, live_client, trino_conn):
        """_meta._queries returns at least one row via the queries view."""
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        _insert_test_trace(trino_conn, TABLE_NAME, trace_id, span_id)

        resp = await live_client.post(
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
                        queryText
                      }
                    }
                  }
                }
                """
            },
        )
        assert resp.status_code == 200, f"HTTP {resp.status_code}: {resp.text[:500]}"
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
        assert any(q.get("queryText") for q in queries), (
            f"Expected at least one _queries row with non-blank queryText. Got: {queries}"
        )
