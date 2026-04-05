# Copyright (c) 2026 Kenneth Stott
# Canary: 7a8b9c0d-1e2f-3456-789a-bcdef0123456
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for MongoDB schema discovery endpoint (issue #13).

Bug: POST /admin/schema-discovery/discover/{source_id} for a MongoDB
source silently returns empty columns when no live connection exists in
source_pools, causing the UI to display "No columns discovered. The source
may need live connection data" instead of a proper HTTP 503 error.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source_row(source_id: str = "mongo-src", source_type: str = "mongodb") -> dict:
    """Return a minimal fake DB row for a MongoDB source."""
    return {
        "id": source_id,
        "type": source_type,
        "host": "mongo",
        "port": 27017,
        "database": "mydb",
        "username": "user",
        "dialect": None,
        "cache_enabled": True,
        "cache_ttl": None,
        "naming_convention": None,
    }


# ---------------------------------------------------------------------------
# _call_discover: MongoDB with no live connection must raise 503
# ---------------------------------------------------------------------------

class TestCallDiscoverMongoDBNoPool:
    """_call_discover for MongoDB must raise 503 when no pool entry exists."""

    def test_raises_503_when_pool_has_no_entry(self):
        """Regression test for issue #13: empty sample docs were silently
        returned instead of a 503 error when no live connection is available.
        """
        from fastapi import HTTPException
        from provisa.api.admin.discovery_schema import _call_discover, DiscoverRequest
        from provisa.source_adapters.registry import get_adapter

        adapter = get_adapter("mongodb")
        row = _make_source_row()
        hints = DiscoverRequest(collection="products")

        empty_pool = MagicMock()
        empty_pool.has.return_value = False

        with patch("provisa.api.admin.discovery_schema._get_source_pool", return_value=empty_pool):
            with pytest.raises(HTTPException) as exc_info:
                _call_discover(adapter, "mongodb", row, hints)

        assert exc_info.value.status_code == 503
        assert "no live connection" in exc_info.value.detail.lower()

    def test_returns_columns_when_pool_has_live_entry(self):
        """When a live pool entry exists with sample_documents, columns are returned."""
        from provisa.api.admin.discovery_schema import _call_discover, DiscoverRequest
        from provisa.source_adapters.registry import get_adapter

        adapter = get_adapter("mongodb")
        row = _make_source_row()
        hints = DiscoverRequest(collection="products", sample_limit=10)

        sample_docs = [
            {"name": "Widget", "price": 9.99},
            {"name": "Gadget", "count": 42},
        ]

        mock_driver = MagicMock()
        mock_driver.sample_documents.return_value = sample_docs

        live_pool = MagicMock()
        live_pool.has.return_value = True
        live_pool.get.return_value = mock_driver

        with patch("provisa.api.admin.discovery_schema._get_source_pool", return_value=live_pool):
            cols = _call_discover(adapter, "mongodb", row, hints)

        assert len(cols) > 0
        names = {c["name"] for c in cols}
        assert "name" in names
        assert "price" in names
        assert "count" in names


# ---------------------------------------------------------------------------
# discover_source_schema endpoint: issue #13 regression
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestDiscoverSourceSchemaMongoIssue13:
    """End-to-end test of the FastAPI handler for issue #13."""

    async def test_endpoint_returns_503_not_empty_columns_for_mongo(self):
        """Before the fix: endpoint returned HTTP 200 with empty columns.
        After the fix: endpoint must return HTTP 503 with 'no live connection'.

        This is the exact bug from issue #13.
        """
        from fastapi import HTTPException
        from provisa.api.admin.discovery_schema import discover_source_schema, DiscoverRequest

        source_id = "mongo-src"
        row = _make_source_row(source_id)

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=row)

        mock_pool_ctx = AsyncMock()
        mock_pool_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_pg_pool = MagicMock()
        mock_pg_pool.acquire.return_value = mock_pool_ctx

        empty_source_pools = MagicMock()
        empty_source_pools.has.return_value = False

        mock_state = MagicMock()
        mock_state.pg_pool = mock_pg_pool

        with (
            patch("provisa.api.admin.discovery_schema._get_source_pool", return_value=empty_source_pools),
            patch("provisa.api.app.state", mock_state),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await discover_source_schema(
                    source_id, DiscoverRequest(collection="products")
                )

        assert exc_info.value.status_code == 503
        assert "no live connection" in exc_info.value.detail.lower()

    async def test_endpoint_returns_columns_with_live_pool(self):
        """When source_pools has a live connection for the MongoDB source,
        the endpoint returns discovered columns.
        """
        from provisa.api.admin.discovery_schema import discover_source_schema, DiscoverRequest

        source_id = "mongo-src"
        row = _make_source_row(source_id)
        sample_docs = [{"title": "Book", "year": 2020}]

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=row)

        mock_pool_ctx = AsyncMock()
        mock_pool_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_ctx.__aexit__ = AsyncMock(return_value=None)

        mock_pg_pool = MagicMock()
        mock_pg_pool.acquire.return_value = mock_pool_ctx

        mock_driver = MagicMock()
        mock_driver.sample_documents.return_value = sample_docs

        live_source_pools = MagicMock()
        live_source_pools.has.return_value = True
        live_source_pools.get.return_value = mock_driver

        mock_state = MagicMock()
        mock_state.pg_pool = mock_pg_pool

        with (
            patch("provisa.api.admin.discovery_schema._get_source_pool", return_value=live_source_pools),
            patch("provisa.api.app.state", mock_state),
        ):
            response = await discover_source_schema(
                source_id, DiscoverRequest(collection="books", sample_limit=10)
            )

        assert response.source_id == source_id
        assert response.source_type == "mongodb"
        assert len(response.columns) > 0
        col_names = {c.name for c in response.columns}
        assert "title" in col_names
        assert "year" in col_names
