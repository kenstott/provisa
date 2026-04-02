# Copyright (c) 2025 Kenneth Stott
# Canary: 9f34945d-17bb-442d-8b45-7a90982a3b0e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for output format content negotiation."""

import io
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


QUERY = json.dumps({"query": "{ sales_analytics__orders(limit: 3) { id amount } }", "role": "admin"})


class TestJSONDefault:
    async def test_default_json(self, client):
        resp = await client.post(
            "/data/graphql",
            content=QUERY,
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "sales_analytics__orders" in data["data"]


class TestNDJSON:
    async def test_ndjson(self, client):
        resp = await client.post(
            "/data/graphql",
            content=QUERY,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/x-ndjson",
            },
        )
        assert resp.status_code == 200
        assert "ndjson" in resp.headers.get("content-type", "")
        lines = resp.text.strip().split("\n")
        assert len(lines) <= 3
        first = json.loads(lines[0])
        assert "id" in first
        assert "amount" in first


class TestCSV:
    async def test_csv(self, client):
        resp = await client.post(
            "/data/graphql",
            content=QUERY,
            headers={
                "Content-Type": "application/json",
                "Accept": "text/csv",
            },
        )
        assert resp.status_code == 200
        assert "csv" in resp.headers.get("content-type", "")
        lines = [l.strip() for l in resp.text.strip().splitlines()]
        assert lines[0] == "id,amount"
        assert len(lines) >= 2  # header + at least 1 row


class TestParquet:
    async def test_parquet(self, client):
        resp = await client.post(
            "/data/graphql",
            content=QUERY,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/vnd.apache.parquet",
            },
        )
        assert resp.status_code == 200
        assert "parquet" in resp.headers.get("content-type", "")
        table = pq.read_table(io.BytesIO(resp.content))
        assert table.num_rows <= 3
        assert "id" in table.column_names


class TestArrowIPC:
    async def test_arrow(self, client):
        resp = await client.post(
            "/data/graphql",
            content=QUERY,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/vnd.apache.arrow.stream",
            },
        )
        assert resp.status_code == 200
        assert "arrow" in resp.headers.get("content-type", "")
        reader = pa.ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows <= 3
        assert "id" in table.column_names
