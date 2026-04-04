# Copyright (c) 2025 Kenneth Stott
# Canary: b4c5d6e7-f8a9-0123-4567-890123b0123c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Phase AI NoSQL source adapters."""

import pytest

from provisa.source_adapters.registry import (
    get_adapter,
    registered_types,
    register_adapter,
)


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_registered_types_includes_all_six(self):
        types = registered_types()
        assert "redis" in types
        assert "mongodb" in types
        assert "elasticsearch" in types
        assert "cassandra" in types
        assert "prometheus" in types
        assert "accumulo" in types

    def test_get_adapter_returns_module(self):
        mod = get_adapter("redis")
        assert hasattr(mod, "generate_catalog_properties")
        assert hasattr(mod, "generate_table_definitions")

    def test_get_adapter_unknown_raises(self):
        with pytest.raises(KeyError, match="Unknown source type"):
            get_adapter("nonexistent")

    def test_register_custom_adapter(self):
        register_adapter("custom", "provisa.redis.source")
        mod = get_adapter("custom")
        assert hasattr(mod, "generate_catalog_properties")


# ---------------------------------------------------------------------------
# Redis tests
# ---------------------------------------------------------------------------


class TestRedisAdapter:
    def _make_config(self):
        from provisa.redis.source import (
            RedisColumn,
            RedisSourceConfig,
            RedisTableConfig,
        )

        return RedisSourceConfig(
            id="session-cache",
            host="redis",
            port=6379,
            tables=[
                RedisTableConfig(
                    name="user_sessions",
                    key_pattern="session:*",
                    key_column="session_id",
                    value_type="hash",
                    columns=[
                        RedisColumn(name="user_id", data_type="INTEGER", field="user_id"),
                        RedisColumn(name="email", data_type="VARCHAR", field="email"),
                    ],
                )
            ],
        )

    def test_catalog_properties(self):
        from provisa.redis.source import generate_catalog_properties

        props = generate_catalog_properties(self._make_config())
        assert props["connector.name"] == "redis"
        assert props["redis.nodes"] == "redis:6379"
        assert "user_sessions" in props["redis.table-names"]

    def test_table_definitions(self):
        from provisa.redis.source import generate_table_definitions

        defs = generate_table_definitions(self._make_config())
        assert len(defs) == 1
        td = defs[0]
        assert td["tableName"] == "user_sessions"
        assert td["key"]["fields"][0]["name"] == "session_id"
        assert len(td["value"]["fields"]) == 2

    def test_table_json_generation(self):
        from provisa.redis.source import generate_table_json

        result = generate_table_json(self._make_config())
        assert "user_sessions.json" in result

    def test_password_in_properties(self):
        from provisa.redis.source import RedisSourceConfig, generate_catalog_properties

        config = RedisSourceConfig(id="r", host="h", password="secret")
        props = generate_catalog_properties(config)
        assert props["redis.password"] == "secret"


# ---------------------------------------------------------------------------
# MongoDB tests
# ---------------------------------------------------------------------------


class TestMongoDBAdapter:
    def _make_config(self):
        from provisa.mongodb.source import (
            MongoColumn,
            MongoSourceConfig,
            MongoTableConfig,
        )

        return MongoSourceConfig(
            id="product-mongo",
            connection_url="mongodb://mongo:27017/",
            database="products_db",
            tables=[
                MongoTableConfig(
                    name="products",
                    collection="products",
                    discover=True,
                    columns=[
                        MongoColumn(
                            name="metadata.category",
                            data_type="VARCHAR",
                            alias="category",
                        ),
                    ],
                )
            ],
        )

    def test_catalog_properties(self):
        from provisa.mongodb.source import generate_catalog_properties

        props = generate_catalog_properties(self._make_config())
        assert props["connector.name"] == "mongodb"
        assert props["mongodb.connection-url"] == "mongodb://mongo:27017/"

    def test_table_definitions(self):
        from provisa.mongodb.source import generate_table_definitions

        defs = generate_table_definitions(self._make_config())
        assert len(defs) == 1
        assert defs[0]["collection"] == "products"
        assert defs[0]["discover"] is True
        assert defs[0]["columns"][0]["name"] == "category"

    def test_discover_schema(self):
        from provisa.mongodb.source import discover_schema

        docs = [
            {"name": "Widget", "price": 9.99, "tags": ["a", "b"]},
            {"name": "Gadget", "count": 42, "active": True},
        ]
        cols = discover_schema(docs, "products")
        names = {c["name"] for c in cols}
        assert "name" in names
        assert "price" in names
        assert "count" in names
        assert "active" in names
        assert "tags" in names

    def test_discover_nested(self):
        from provisa.mongodb.source import discover_schema

        docs = [{"meta": {"category": "A", "rating": 4.5}}]
        cols = discover_schema(docs, "items")
        paths = {c["sourcePath"] for c in cols}
        assert "meta.category" in paths
        assert "meta.rating" in paths


# ---------------------------------------------------------------------------
# Elasticsearch tests
# ---------------------------------------------------------------------------


class TestElasticsearchAdapter:
    def _make_config(self):
        from provisa.elasticsearch.source import (
            ESColumn,
            ESSourceConfig,
            ESTableConfig,
        )

        return ESSourceConfig(
            id="logs-es",
            host="elasticsearch",
            port=9200,
            tables=[
                ESTableConfig(
                    name="access_logs",
                    index="nginx-access-*",
                    discover=True,
                    columns=[
                        ESColumn(name="timestamp", data_type="TIMESTAMP", path="@timestamp"),
                        ESColumn(name="method", data_type="VARCHAR", path="request.method"),
                    ],
                )
            ],
        )

    def test_catalog_properties(self):
        from provisa.elasticsearch.source import generate_catalog_properties

        props = generate_catalog_properties(self._make_config())
        assert props["connector.name"] == "elasticsearch"
        assert props["elasticsearch.host"] == "elasticsearch"

    def test_table_definitions(self):
        from provisa.elasticsearch.source import generate_table_definitions

        defs = generate_table_definitions(self._make_config())
        assert len(defs) == 1
        assert defs[0]["index"] == "nginx-access-*"
        assert defs[0]["columns"][1]["sourcePath"] == "request.method"

    def test_discover_schema_flat(self):
        from provisa.elasticsearch.source import discover_schema

        mapping = {
            "status": {"type": "integer"},
            "message": {"type": "text"},
        }
        cols = discover_schema(mapping)
        names = {c["name"] for c in cols}
        assert "status" in names
        assert "message" in names

    def test_discover_schema_nested(self):
        from provisa.elasticsearch.source import discover_schema

        mapping = {
            "request": {
                "properties": {
                    "method": {"type": "keyword"},
                    "url": {"type": "text"},
                }
            },
            "status": {"type": "integer"},
        }
        cols = discover_schema(mapping)
        paths = {c["sourcePath"] for c in cols}
        assert "request.method" in paths
        assert "request.url" in paths
        assert "status" in paths

    def test_tls_properties(self):
        from provisa.elasticsearch.source import ESSourceConfig, generate_catalog_properties

        config = ESSourceConfig(id="es", host="es", tls=True, auth_user="u", auth_password="p")
        props = generate_catalog_properties(config)
        assert props["elasticsearch.tls.enabled"] == "true"
        assert props["elasticsearch.auth.user"] == "u"


# ---------------------------------------------------------------------------
# Cassandra tests
# ---------------------------------------------------------------------------


class TestCassandraAdapter:
    def _make_config(self):
        from provisa.cassandra.source import CassandraSourceConfig, CassandraTableConfig

        return CassandraSourceConfig(
            id="events-cassandra",
            contact_points="cassandra",
            port=9042,
            tables=[
                CassandraTableConfig(
                    name="user_events",
                    keyspace="analytics",
                    table="user_events",
                    discover=True,
                )
            ],
        )

    def test_catalog_properties(self):
        from provisa.cassandra.source import generate_catalog_properties

        props = generate_catalog_properties(self._make_config())
        assert props["connector.name"] == "cassandra"
        assert props["cassandra.contact-points"] == "cassandra"

    def test_table_definitions(self):
        from provisa.cassandra.source import generate_table_definitions

        defs = generate_table_definitions(self._make_config())
        assert len(defs) == 1
        assert defs[0]["keyspace"] == "analytics"

    def test_discover_schema(self):
        from provisa.cassandra.source import discover_schema

        meta = {
            "columns": [
                {"name": "user_id", "type": "uuid"},
                {"name": "event_time", "type": "timestamp"},
                {"name": "data", "type": "text"},
            ],
            "partition_keys": ["user_id"],
            "clustering_keys": ["event_time"],
        }
        cols = discover_schema(meta)
        assert len(cols) == 3
        pk = next(c for c in cols if c["name"] == "user_id")
        assert pk["partitionKey"] is True
        ck = next(c for c in cols if c["name"] == "event_time")
        assert ck["clusteringKey"] is True

    def test_discover_collection_type(self):
        from provisa.cassandra.source import discover_schema

        meta = {
            "columns": [{"name": "tags", "type": "list<text>"}],
            "partition_keys": [],
            "clustering_keys": [],
        }
        cols = discover_schema(meta)
        assert cols[0]["type"] == "VARCHAR"

    def test_auth_properties(self):
        from provisa.cassandra.source import CassandraSourceConfig, generate_catalog_properties

        config = CassandraSourceConfig(id="c", username="u", password="p")
        props = generate_catalog_properties(config)
        assert props["cassandra.username"] == "u"


# ---------------------------------------------------------------------------
# Prometheus tests
# ---------------------------------------------------------------------------


class TestPrometheusAdapter:
    def _make_config(self):
        from provisa.prometheus.source import PrometheusSourceConfig, PrometheusTableConfig

        return PrometheusSourceConfig(
            id="metrics",
            url="http://prometheus:9090",
            tables=[
                PrometheusTableConfig(
                    name="api_latency",
                    metric="http_request_duration_seconds",
                    labels_as_columns=["method", "endpoint", "status"],
                    value_column="duration_ms",
                    default_range="1h",
                )
            ],
        )

    def test_catalog_properties(self):
        from provisa.prometheus.source import generate_catalog_properties

        props = generate_catalog_properties(self._make_config())
        assert props["connector.name"] == "prometheus"
        assert props["prometheus.uri"] == "http://prometheus:9090"

    def test_table_definitions(self):
        from provisa.prometheus.source import generate_table_definitions

        defs = generate_table_definitions(self._make_config())
        assert len(defs) == 1
        td = defs[0]
        assert td["metric"] == "http_request_duration_seconds"
        assert td["defaultRange"] == "1h"
        col_names = [c["name"] for c in td["columns"]]
        assert "timestamp" in col_names
        assert "duration_ms" in col_names
        assert "method" in col_names
        assert "endpoint" in col_names
        assert "status" in col_names

    def test_discover_schema_gauge(self):
        from provisa.prometheus.source import discover_schema

        meta = {"labels": ["instance", "job", "__name__"], "type": "gauge"}
        cols = discover_schema(meta, "up")
        names = [c["name"] for c in cols]
        assert "timestamp" in names
        assert "value" in names
        assert "instance" in names
        assert "job" in names
        assert "__name__" not in names

    def test_discover_schema_histogram(self):
        from provisa.prometheus.source import discover_schema

        meta = {"labels": ["method"], "type": "histogram"}
        cols = discover_schema(meta, "duration")
        names = [c["name"] for c in cols]
        assert "le" in names


# ---------------------------------------------------------------------------
# Accumulo tests
# ---------------------------------------------------------------------------


class TestAccumuloAdapter:
    def _make_config(self):
        from provisa.accumulo.source import (
            AccumuloColumn,
            AccumuloSourceConfig,
            AccumuloTableConfig,
        )

        return AccumuloSourceConfig(
            id="graph-accumulo",
            instance="accumulo",
            zookeepers="zookeeper:2181",
            tables=[
                AccumuloTableConfig(
                    name="edges",
                    accumulo_table="graph_edges",
                    columns=[
                        AccumuloColumn(
                            name="src_vertex",
                            data_type="VARCHAR",
                            family="edge",
                            qualifier="src",
                        ),
                        AccumuloColumn(
                            name="dst_vertex",
                            data_type="VARCHAR",
                            family="edge",
                            qualifier="dst",
                        ),
                    ],
                )
            ],
        )

    def test_catalog_properties(self):
        from provisa.accumulo.source import generate_catalog_properties

        props = generate_catalog_properties(self._make_config())
        assert props["connector.name"] == "accumulo"
        assert props["accumulo.instance"] == "accumulo"
        assert props["accumulo.zookeepers"] == "zookeeper:2181"

    def test_table_definitions(self):
        from provisa.accumulo.source import generate_table_definitions

        defs = generate_table_definitions(self._make_config())
        assert len(defs) == 1
        td = defs[0]
        assert td["accumuloTable"] == "graph_edges"
        assert len(td["columns"]) == 2
        assert td["columns"][0]["family"] == "edge"
        assert td["columns"][0]["qualifier"] == "src"

    def test_auth_properties(self):
        from provisa.accumulo.source import AccumuloSourceConfig, generate_catalog_properties

        config = AccumuloSourceConfig(id="a", username="root", password="secret")
        props = generate_catalog_properties(config)
        assert props["accumulo.username"] == "root"
        assert props["accumulo.password"] == "secret"


# ---------------------------------------------------------------------------
# Cross-adapter via registry
# ---------------------------------------------------------------------------


class TestAdapterViaRegistry:
    @pytest.mark.parametrize("source_type", [
        "redis", "mongodb", "elasticsearch",
        "cassandra", "prometheus", "accumulo",
    ])
    def test_all_adapters_have_required_interface(self, source_type):
        mod = get_adapter(source_type)
        assert callable(getattr(mod, "generate_catalog_properties", None))
        assert callable(getattr(mod, "generate_table_definitions", None))
