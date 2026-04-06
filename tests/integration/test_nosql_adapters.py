# Copyright (c) 2026 Kenneth Stott
# Canary: af6b7c8d-9e0f-1234-5678-901234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for NoSQL source adapters (MongoDB, Elasticsearch, registry)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from provisa.source_adapters.registry import (
    get_adapter,
    register_adapter,
    registered_types,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Registry tests (no real connections required)
# ---------------------------------------------------------------------------

class TestAdapterRegistry:
    async def test_adapter_registry_instantiates_correct_adapter(self):
        """get_adapter('mongodb') returns the mongodb source module."""
        mod = get_adapter("mongodb")
        assert hasattr(mod, "generate_catalog_properties")
        assert hasattr(mod, "generate_table_definitions")
        assert mod.__name__ == "provisa.mongodb.source"

    async def test_elasticsearch_adapter_registered(self):
        """get_adapter('elasticsearch') returns the es source module."""
        mod = get_adapter("elasticsearch")
        assert hasattr(mod, "generate_catalog_properties")
        assert hasattr(mod, "discover_schema")
        assert mod.__name__ == "provisa.elasticsearch.source"

    async def test_unknown_adapter_raises(self):
        """get_adapter('nonexistent') raises KeyError."""
        with pytest.raises(KeyError, match="nonexistent"):
            get_adapter("nonexistent")

    async def test_registered_types_returns_sorted_list(self):
        """registered_types() returns a sorted list of known source types."""
        types = registered_types()
        assert isinstance(types, list)
        assert types == sorted(types)
        assert "mongodb" in types
        assert "elasticsearch" in types
        assert "redis" in types

    async def test_register_custom_adapter(self):
        """register_adapter adds a new type to the registry."""
        # Use a stdlib module as a stand-in
        register_adapter("_test_custom_src", "json")
        assert "_test_custom_src" in registered_types()
        mod = get_adapter("_test_custom_src")
        assert mod.__name__ == "json"

    async def test_re_register_adapter_clears_cache(self):
        """Re-registering a source type clears the loaded cache."""
        # Register then immediately re-register
        register_adapter("_test_cache_src", "json")
        first = get_adapter("_test_cache_src")

        register_adapter("_test_cache_src", "os.path")
        second = get_adapter("_test_cache_src")

        assert second.__name__ != first.__name__


# ---------------------------------------------------------------------------
# MongoDB module unit tests (no real MongoDB connection)
# ---------------------------------------------------------------------------

class TestMongoDBAdapter:
    async def test_mongodb_adapter_introspects_collection(self):
        """discover_schema infers column definitions from sample documents."""
        from provisa.mongodb.source import discover_schema  # noqa: PLC0415

        sample_docs = [
            {"name": "Alice", "age": 30, "score": 9.5, "active": True},
            {"name": "Bob", "age": 25, "score": 8.0, "tags": ["python", "kafka"]},
            {"name": "Carol", "age": 35, "score": 9.9, "address": {"city": "NYC"}},
        ]
        columns = discover_schema(sample_docs, "test_collection")

        col_names = {c["name"] for c in columns}
        assert "name" in col_names
        assert "age" in col_names
        assert "score" in col_names
        assert "active" in col_names

        # Nested address.city should be flattened
        assert any("address" in n for n in col_names)

    async def test_mongodb_adapter_generates_catalog_properties(self):
        """generate_catalog_properties returns correct connector config."""
        from provisa.mongodb.source import MongoSourceConfig, generate_catalog_properties  # noqa: PLC0415

        config = MongoSourceConfig(
            id="mongo-prod",
            connection_url="mongodb://admin:pass@mongo.prod:27017/",
            database="analytics",
        )
        props = generate_catalog_properties(config)
        assert props["connector.name"] == "mongodb"
        assert "mongodb.connection-url" in props
        assert "mongo.prod" in props["mongodb.connection-url"]

    async def test_mongodb_adapter_generates_table_definitions(self):
        """generate_table_definitions returns one entry per configured collection."""
        from provisa.mongodb.source import (  # noqa: PLC0415
            MongoColumn,
            MongoSourceConfig,
            MongoTableConfig,
            generate_table_definitions,
        )

        config = MongoSourceConfig(
            id="mongo-test",
            database="mydb",
            tables=[
                MongoTableConfig(
                    name="events",
                    collection="raw_events",
                    discover=True,
                    columns=[
                        MongoColumn(name="event_id", data_type="VARCHAR"),
                        MongoColumn(name="ts", data_type="TIMESTAMP", path="timestamp"),
                    ],
                )
            ],
        )
        defs = generate_table_definitions(config)
        assert len(defs) == 1
        entry = defs[0]
        assert entry["tableName"] == "events"
        assert entry["collection"] == "raw_events"
        assert entry["discover"] is True
        col_names = [c["name"] for c in entry["columns"]]
        assert "event_id" in col_names
        assert "ts" in col_names

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
# Elasticsearch adapter unit tests
# ---------------------------------------------------------------------------

class TestElasticsearchAdapter:
    async def test_elasticsearch_adapter_introspects_index(self):
        """discover_schema converts ES index mapping to column definitions."""
        from provisa.elasticsearch.source import discover_schema  # noqa: PLC0415

        mapping = {
            "timestamp": {"type": "date"},
            "message": {"type": "text"},
            "level": {"type": "keyword"},
            "http": {
                "properties": {
                    "method": {"type": "keyword"},
                    "status": {"type": "integer"},
                }
            },
        }
        columns = discover_schema(mapping)
        col_names = {c["name"] for c in columns}

        assert "timestamp" in col_names
        assert "message" in col_names
        assert "level" in col_names
        # Nested fields flattened
        assert any("method" in n for n in col_names)
        assert any("status" in n for n in col_names)

    async def test_elasticsearch_type_mapping(self):
        """ES type aliases map correctly to Trino types."""
        from provisa.elasticsearch.source import ES_TYPE_TO_TRINO, discover_schema  # noqa: PLC0415

        mapping = {
            "price": {"type": "double"},
            "count": {"type": "long"},
            "active": {"type": "boolean"},
        }
        columns = discover_schema(mapping)
        type_map = {c["name"]: c["type"] for c in columns}
        assert type_map["price"] == ES_TYPE_TO_TRINO["double"]
        assert type_map["count"] == ES_TYPE_TO_TRINO["long"]
        assert type_map["active"] == ES_TYPE_TO_TRINO["boolean"]

    async def test_elasticsearch_adapter_catalog_properties(self):
        """generate_catalog_properties emits correct Trino ES connector config."""
        from provisa.elasticsearch.source import ESSourceConfig, generate_catalog_properties  # noqa: PLC0415

        config = ESSourceConfig(
            id="es-logs",
            host="es.example.com",
            port=9200,
            tls=True,
            auth_user="elastic",
            auth_password="changeme",
        )
        props = generate_catalog_properties(config)
        assert props["connector.name"] == "elasticsearch"
        assert props["elasticsearch.host"] == "es.example.com"
        assert props.get("elasticsearch.tls.enabled") == "true"
        assert props.get("elasticsearch.auth.user") == "elastic"

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
