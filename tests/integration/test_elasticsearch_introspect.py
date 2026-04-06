# Copyright (c) 2026 Kenneth Stott
# Canary: c1d3e5f7-a9b1-4c2d-8e6f-0a1b2c3d4e5f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Elasticsearch source mapping (Phase AI, REQ-250–253).

Pure-Python tests cover discover_schema, generate_catalog_properties, and
generate_table_definitions. Live-service tests (pytest.mark.requires_elasticsearch)
verify against a running ES instance in Docker Compose.

To run live tests:
    docker compose up elasticsearch
    pytest tests/integration/test_elasticsearch_introspect.py -m requires_elasticsearch
"""

from __future__ import annotations

import pytest

from provisa.elasticsearch.source import (
    ES_TYPE_TO_TRINO,
    ESColumn,
    ESSourceConfig,
    ESTableConfig,
    _flatten_mapping,
    discover_schema,
    generate_catalog_properties,
    generate_table_definitions,
)

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# discover_schema — pure Python
# ---------------------------------------------------------------------------

class TestDiscoverSchema:
    def test_single_field(self):
        mapping = {"name": {"type": "keyword"}}
        cols = discover_schema(mapping)
        assert len(cols) == 1
        assert cols[0]["name"] == "name"
        assert cols[0]["type"] == "VARCHAR"

    def test_multiple_fields(self):
        mapping = {
            "id": {"type": "long"},
            "title": {"type": "text"},
            "active": {"type": "boolean"},
        }
        cols = discover_schema(mapping)
        names = {c["name"] for c in cols}
        assert "id" in names
        assert "title" in names
        assert "active" in names

    def test_nested_fields_flattened(self):
        mapping = {
            "address": {
                "properties": {
                    "city": {"type": "keyword"},
                    "zip": {"type": "keyword"},
                }
            }
        }
        cols = discover_schema(mapping)
        names = {c["name"] for c in cols}
        assert "address_city" in names
        assert "address_zip" in names

    def test_deeply_nested_flattened(self):
        mapping = {
            "request": {
                "properties": {
                    "http": {
                        "properties": {
                            "method": {"type": "keyword"},
                            "status_code": {"type": "integer"},
                        }
                    }
                }
            }
        }
        cols = discover_schema(mapping)
        names = {c["name"] for c in cols}
        assert "request_http_method" in names
        assert "request_http_status_code" in names

    def test_source_path_preserved_for_nested(self):
        mapping = {
            "geo": {
                "properties": {
                    "lat": {"type": "double"},
                }
            }
        }
        cols = discover_schema(mapping)
        geo_lat = next(c for c in cols if c["name"] == "geo_lat")
        assert geo_lat["sourcePath"] == "geo.lat"

    def test_source_path_for_top_level_field(self):
        mapping = {"score": {"type": "float"}}
        cols = discover_schema(mapping)
        assert cols[0]["sourcePath"] == "score"

    def test_empty_mapping_returns_empty(self):
        assert discover_schema({}) == []

    def test_object_type_without_properties_is_varchar(self):
        mapping = {"metadata": {"type": "object"}}
        cols = discover_schema(mapping)
        assert cols[0]["type"] == "VARCHAR"

    def test_nested_type_mapped_to_varchar(self):
        """Nested type at field level (no sub-properties) → VARCHAR."""
        mapping = {"tags": {"type": "nested"}}
        cols = discover_schema(mapping)
        assert cols[0]["type"] == "VARCHAR"

    def test_unknown_type_defaults_to_varchar(self):
        mapping = {"custom": {"type": "some_unknown_type"}}
        cols = discover_schema(mapping)
        assert cols[0]["type"] == "VARCHAR"


class TestESTypeMappings:
    """Verify ES type → Trino type mappings."""

    @pytest.mark.parametrize("es_type,expected_trino", [
        ("text", "VARCHAR"),
        ("keyword", "VARCHAR"),
        ("long", "BIGINT"),
        ("integer", "INTEGER"),
        ("short", "SMALLINT"),
        ("byte", "TINYINT"),
        ("double", "DOUBLE"),
        ("float", "REAL"),
        ("half_float", "REAL"),
        ("scaled_float", "DOUBLE"),
        ("boolean", "BOOLEAN"),
        ("date", "TIMESTAMP"),
        ("ip", "VARCHAR"),
        ("binary", "VARBINARY"),
        ("geo_point", "VARCHAR"),
    ])
    def test_type_mapping(self, es_type, expected_trino):
        assert ES_TYPE_TO_TRINO[es_type] == expected_trino

    def test_all_mapped_types_produce_columns(self):
        mapping = {es_t: {"type": es_t} for es_t in ES_TYPE_TO_TRINO}
        cols = discover_schema(mapping)
        assert len(cols) == len(ES_TYPE_TO_TRINO)


# ---------------------------------------------------------------------------
# generate_catalog_properties — pure Python
# ---------------------------------------------------------------------------

class TestGenerateCatalogProperties:
    def test_connector_name(self):
        cfg = ESSourceConfig(id="es-1", host="localhost", port=9200)
        props = generate_catalog_properties(cfg)
        assert props["connector.name"] == "elasticsearch"

    def test_host_and_port(self):
        cfg = ESSourceConfig(id="es-1", host="es-host", port=9201)
        props = generate_catalog_properties(cfg)
        assert props["elasticsearch.host"] == "es-host"
        assert props["elasticsearch.port"] == "9201"

    def test_default_schema_name(self):
        cfg = ESSourceConfig(id="es-1")
        props = generate_catalog_properties(cfg)
        assert props["elasticsearch.default-schema-name"] == "default"

    def test_tls_enabled(self):
        cfg = ESSourceConfig(id="es-1", tls=True)
        props = generate_catalog_properties(cfg)
        assert props["elasticsearch.tls.enabled"] == "true"

    def test_tls_not_set_when_false(self):
        cfg = ESSourceConfig(id="es-1", tls=False)
        props = generate_catalog_properties(cfg)
        assert "elasticsearch.tls.enabled" not in props

    def test_auth_credentials(self):
        cfg = ESSourceConfig(id="es-1", auth_user="elastic", auth_password="secret")
        props = generate_catalog_properties(cfg)
        assert props["elasticsearch.auth.user"] == "elastic"
        assert props["elasticsearch.auth.password"] == "secret"

    def test_no_auth_by_default(self):
        cfg = ESSourceConfig(id="es-1")
        props = generate_catalog_properties(cfg)
        assert "elasticsearch.auth.user" not in props
        assert "elasticsearch.auth.password" not in props


# ---------------------------------------------------------------------------
# generate_table_definitions — pure Python
# ---------------------------------------------------------------------------

class TestGenerateTableDefinitions:
    def test_empty_tables(self):
        cfg = ESSourceConfig(id="es-1", tables=[])
        assert generate_table_definitions(cfg) == []

    def test_single_table_definition(self):
        table = ESTableConfig(
            name="nginx_access",
            index="nginx-access-*",
            columns=[
                ESColumn(name="status_code", data_type="INTEGER"),
                ESColumn(name="method", data_type="VARCHAR"),
            ],
        )
        cfg = ESSourceConfig(id="es-1", tables=[table])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 1
        assert defs[0]["tableName"] == "nginx_access"
        assert defs[0]["index"] == "nginx-access-*"

    def test_discover_flag_passed(self):
        table = ESTableConfig(name="t", index="t-*", discover=True)
        cfg = ESSourceConfig(id="es-1", tables=[table])
        defs = generate_table_definitions(cfg)
        assert defs[0]["discover"] is True

    def test_column_definitions(self):
        table = ESTableConfig(
            name="logs",
            index="logs-*",
            columns=[
                ESColumn(name="timestamp", data_type="TIMESTAMP"),
                ESColumn(name="level", data_type="VARCHAR"),
            ],
        )
        cfg = ESSourceConfig(id="es-1", tables=[table])
        defs = generate_table_definitions(cfg)
        cols = {c["name"]: c for c in defs[0]["columns"]}
        assert cols["timestamp"]["type"] == "TIMESTAMP"
        assert cols["level"]["type"] == "VARCHAR"

    def test_column_source_path_uses_path_when_set(self):
        table = ESTableConfig(
            name="events",
            index="events",
            columns=[ESColumn(name="city", data_type="VARCHAR", path="address.city")],
        )
        cfg = ESSourceConfig(id="es-1", tables=[table])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"][0]["sourcePath"] == "address.city"

    def test_column_source_path_defaults_to_name(self):
        table = ESTableConfig(
            name="events",
            index="events",
            columns=[ESColumn(name="status", data_type="VARCHAR")],
        )
        cfg = ESSourceConfig(id="es-1", tables=[table])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"][0]["sourcePath"] == "status"

    def test_multiple_tables(self):
        cfg = ESSourceConfig(id="es-1", tables=[
            ESTableConfig(name="t1", index="i1"),
            ESTableConfig(name="t2", index="i2"),
        ])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 2
        names = {d["tableName"] for d in defs}
        assert names == {"t1", "t2"}


# ---------------------------------------------------------------------------
# Live-service tests (requires running Elasticsearch instance)
# ---------------------------------------------------------------------------

@pytest.mark.requires_elasticsearch
class TestLiveElasticsearchIntrospect:
    """Require Docker Compose elasticsearch service:
        docker compose up elasticsearch
    """

    ES_HOST = "localhost"
    ES_PORT = 9200

    async def test_index_mapping_discovery(self):
        """GET /<index>/_mapping returns a mapping that discover_schema can parse."""
        import httpx

        index = "provisa-test-index"

        # Create a test index with known mapping
        async with httpx.AsyncClient() as client:
            # Ensure index exists with a simple mapping
            await client.put(
                f"http://{self.ES_HOST}:{self.ES_PORT}/{index}",
                json={
                    "mappings": {
                        "properties": {
                            "user_id": {"type": "long"},
                            "event_type": {"type": "keyword"},
                            "ts": {"type": "date"},
                        }
                    }
                },
            )

            # Fetch the mapping
            resp = await client.get(
                f"http://{self.ES_HOST}:{self.ES_PORT}/{index}/_mapping"
            )
            resp.raise_for_status()
            data = resp.json()

        properties = data[index]["mappings"]["properties"]
        cols = discover_schema(properties)
        names = {c["name"] for c in cols}
        assert "user_id" in names
        assert "event_type" in names
        assert "ts" in names

    async def test_catalog_properties_connect_to_live_es(self):
        """generate_catalog_properties produces a connector.name that Trino accepts."""
        import httpx

        cfg = ESSourceConfig(id="es-live", host=self.ES_HOST, port=self.ES_PORT)
        props = generate_catalog_properties(cfg)
        assert props["connector.name"] == "elasticsearch"

        # Sanity check: ES is actually running
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"http://{self.ES_HOST}:{self.ES_PORT}/_cluster/health",
                timeout=5.0,
            )
            assert resp.status_code == 200
