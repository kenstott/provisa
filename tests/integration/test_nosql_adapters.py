# Copyright (c) 2026 Kenneth Stott
# Canary: af6b7c8d-9e0f-1234-5678-901234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for NoSQL source adapters (MongoDB, Elasticsearch).

Pure-logic and registry tests (no real connections) have been moved to
tests/unit/test_source_adapters.py.

Only live-service tests requiring a running docker-compose service remain here.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# MongoDB live tests (requires running MongoDB)
# ---------------------------------------------------------------------------

class TestMongoDBAdapterLive:
    @pytest.mark.requires_mongodb
    async def test_mongodb_adapter_query_returns_documents(self):
        """MongoDB adapter connects and queries when server is available."""
        import motor.motor_asyncio  # noqa: PLC0415

        host = os.environ.get("MONGO_HOST", "localhost")
        port = int(os.environ.get("MONGO_PORT", "27017"))
        client = motor.motor_asyncio.AsyncIOMotorClient(
            host=host, port=port, serverSelectionTimeoutMS=3000,
        )
        db = client["provisa_test"]
        col = db["test_collection"]

        # Seed test data
        await col.delete_many({})
        await col.insert_many([
            {"item": "alpha", "qty": 10},
            {"item": "beta", "qty": 25},
        ])

        docs = await col.find({"qty": {"$gt": 5}}).to_list(length=10)
        assert len(docs) == 2
        items = {d["item"] for d in docs}
        assert "alpha" in items
        assert "beta" in items

        await col.delete_many({})
        client.close()


# ---------------------------------------------------------------------------
# Elasticsearch live tests (requires running Elasticsearch)
# ---------------------------------------------------------------------------

class TestElasticsearchAdapterLive:
    @pytest.mark.requires_elasticsearch
    async def test_elasticsearch_adapter_live_query(self):
        """ES adapter query returns documents when server is available."""
        from elasticsearch import AsyncElasticsearch  # noqa: PLC0415

        host = os.environ.get("ES_HOST", "localhost")
        port = int(os.environ.get("ES_PORT", "9200"))
        client = AsyncElasticsearch(hosts=[{"host": host, "port": port, "scheme": "http"}])
        index = "provisa_test_index"

        try:
            await client.indices.create(index=index, ignore=400)
            await client.index(index=index, document={"level": "INFO", "msg": "hello"})
            await client.indices.refresh(index=index)

            result = await client.search(
                index=index, query={"match": {"level": "INFO"}}
            )
            hits = result["hits"]["hits"]
            assert len(hits) >= 1
            assert hits[0]["_source"]["msg"] == "hello"
        finally:
            await client.indices.delete(index=index, ignore=404)
            await client.close()
