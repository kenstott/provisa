# Copyright (c) 2025 Kenneth Stott
# Canary: d6e56d31-3293-4be6-b0d0-2129510227d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for relationship candidate storage and lifecycle.

Uses mocked asyncpg connection to test store/retrieve/accept/reject
without requiring a live PG instance.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.discovery.analyzer import RelationshipCandidate
from provisa.discovery.candidates import accept, list_pending, reject, store_candidates


def _candidate(
    src_table=1, src_col="customer_id",
    tgt_table=2, tgt_col="id",
    cardinality="many-to-one",
    confidence=0.9,
    reasoning="FK pattern",
):
    return RelationshipCandidate(
        source_table_id=src_table,
        source_column=src_col,
        target_table_id=tgt_table,
        target_column=tgt_col,
        cardinality=cardinality,
        confidence=confidence,
        reasoning=reasoning,
    )


class TestStoreCandidates:
    @pytest.mark.asyncio
    async def test_store_single_candidate(self):
        conn = AsyncMock()
        conn.fetchval.return_value = 1
        c = _candidate()
        ids = await store_candidates(conn, [c], "table")
        assert ids == [1]
        conn.fetchval.assert_called_once()
        # Verify the INSERT SQL structure
        call_args = conn.fetchval.call_args
        sql = call_args[0][0]
        assert "INSERT INTO relationship_candidates" in sql
        assert "ON CONFLICT" in sql

    @pytest.mark.asyncio
    async def test_store_multiple_candidates(self):
        conn = AsyncMock()
        conn.fetchval.side_effect = [1, 2, 3]
        candidates = [
            _candidate(src_col="customer_id"),
            _candidate(src_col="product_id", tgt_table=3),
            _candidate(src_col="status_id", tgt_table=4),
        ]
        ids = await store_candidates(conn, candidates, "domain")
        assert ids == [1, 2, 3]
        assert conn.fetchval.call_count == 3

    @pytest.mark.asyncio
    async def test_store_passes_scope(self):
        conn = AsyncMock()
        conn.fetchval.return_value = 1
        c = _candidate()
        await store_candidates(conn, [c], "cross-domain")
        call_args = conn.fetchval.call_args
        # scope is the 8th positional arg
        assert call_args[0][8] == "cross-domain"

    @pytest.mark.asyncio
    async def test_store_empty_list(self):
        conn = AsyncMock()
        ids = await store_candidates(conn, [], "table")
        assert ids == []
        conn.fetchval.assert_not_called()


class TestListPending:
    @pytest.mark.asyncio
    async def test_list_returns_suggested_candidates(self):
        mock_row1 = {"id": 1, "source_table_id": 1, "confidence": 0.95, "status": "suggested"}
        mock_row2 = {"id": 2, "source_table_id": 1, "confidence": 0.8, "status": "suggested"}
        conn = AsyncMock()
        conn.fetch.return_value = [MagicMock(**{"__iter__": lambda s: iter(mock_row1.items()), "keys": lambda: mock_row1.keys()})]
        # Simpler: use real Record-like dicts
        conn.fetch.return_value = [MockRecord(mock_row1), MockRecord(mock_row2)]
        result = await list_pending(conn)
        assert len(result) == 2
        conn.fetch.assert_called_once()
        sql = conn.fetch.call_args[0][0]
        assert "status = 'suggested'" in sql
        assert "ORDER BY confidence DESC" in sql

    @pytest.mark.asyncio
    async def test_list_empty(self):
        conn = AsyncMock()
        conn.fetch.return_value = []
        result = await list_pending(conn)
        assert result == []


class TestAcceptCandidate:
    @pytest.mark.asyncio
    async def test_accept_creates_relationship(self):
        row = MockRecord({
            "id": 1,
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
            "confidence": 0.95,
        })
        conn = AsyncMock()
        conn.fetchrow.return_value = row
        result = await accept(conn, 1)

        # Should have updated candidate status
        conn.fetchrow.assert_called_once()
        update_sql = conn.fetchrow.call_args[0][0]
        assert "status = 'accepted'" in update_sql

        # Should have inserted relationship
        conn.execute.assert_called_once()
        insert_sql = conn.execute.call_args[0][0]
        assert "INSERT INTO relationships" in insert_sql

        assert result["source_column"] == "customer_id"
        assert result["target_column"] == "id"
        assert "relationship_id" in result

    @pytest.mark.asyncio
    async def test_accept_nonexistent_raises(self):
        conn = AsyncMock()
        conn.fetchrow.return_value = None
        with pytest.raises(ValueError, match="not found"):
            await accept(conn, 999)


class TestRejectCandidate:
    @pytest.mark.asyncio
    async def test_reject_records_reason(self):
        conn = AsyncMock()
        conn.execute.return_value = "UPDATE 1"
        await reject(conn, 1, "Not a real FK")
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "status = 'rejected'" in sql
        assert "rejection_reason" in sql
        assert conn.execute.call_args[0][2] == "Not a real FK"

    @pytest.mark.asyncio
    async def test_reject_nonexistent_raises(self):
        conn = AsyncMock()
        conn.execute.return_value = "UPDATE 0"
        with pytest.raises(ValueError, match="not found"):
            await reject(conn, 999, "reason")


class MockRecord(dict):
    """Dict subclass that mimics asyncpg.Record for dict() conversion."""

    def __init__(self, data):
        super().__init__(data)