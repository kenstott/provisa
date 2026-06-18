# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-250/251: Trino catalog + table-description generation from the mapping DSL."""

from __future__ import annotations

import json

from provisa.core import trino_catalog_files as tcf
from provisa.core.models import Source, SourceType


def _redis_source() -> Source:
    return Source(
        id="redis1",
        type=SourceType.redis,
        host="redis-host",
        port=6379,
        mapping={
            "tables": [
                {
                    "name": "users",
                    "key_pattern": "user:*",
                    "key_column": "id",
                    "value_type": "hash",
                    "columns": [{"name": "email", "data_type": "VARCHAR", "field": "email"}],
                }
            ]
        },
    )


class TestRedis:
    def test_catalog_properties(self):
        props = tcf.catalog_properties_for(_redis_source(), "")
        assert props is not None
        # connector.name is set by USING <connector>, not the WITH clause.
        assert "connector.name" not in props
        assert props["redis.nodes"] == "redis-host:6379"
        assert props["redis.table-names"] == "users"

    def test_table_definition_files_written(self, tmp_path):
        written = tcf.write_table_definitions(_redis_source(), "", tmp_path)
        assert [p.name for p in written] == ["users.json"]
        doc = json.loads(written[0].read_text())
        assert doc["tableName"] == "users"
        assert doc["key"]["fields"][0]["mapping"] == "key"

    def test_is_mapping_dsl_source(self):
        assert tcf.is_mapping_dsl_source(_redis_source()) is True


class TestElasticsearch:
    def test_catalog_properties_and_files(self, tmp_path):
        src = Source(
            id="es1",
            type=SourceType.elasticsearch,
            host="es-host",
            port=9200,
            mapping={
                "tls": True,
                "tables": [
                    {"name": "logs", "index": "nginx-*", "columns": [{"name": "method", "data_type": "VARCHAR", "path": "request.method"}]}
                ],
            },
        )
        props = tcf.catalog_properties_for(src, "")
        assert props is not None
        assert "connector.name" not in props
        assert props["elasticsearch.host"] == "es-host"
        assert props["elasticsearch.tls.enabled"] == "true"
        written = tcf.write_table_definitions(src, "", tmp_path)
        doc = json.loads(written[0].read_text())
        assert doc["index"] == "nginx-*"
        assert doc["columns"][0]["sourcePath"] == "request.method"


class TestPrometheus:
    def test_catalog_properties(self):
        src = Source(
            id="prom1",
            type=SourceType.prometheus,
            mapping={"url": "http://prom:9090", "tables": [{"name": "cpu", "metric": "node_cpu"}]},
        )
        props = tcf.catalog_properties_for(src, "")
        assert props == {"prometheus.uri": "http://prom:9090"}


class TestKafka:
    def test_catalog_file_no_auth(self, tmp_path):
        written = tcf.write_kafka_catalog_files(
            {"id": "events-bus", "bootstrap_servers": "kafka:9092", "schema_registry_url": "http://sr:8081"},
            tmp_path,
        )
        assert [p.name for p in written] == ["events_bus.properties"]
        content = written[0].read_text()
        assert "connector.name=kafka" in content
        assert "kafka.nodes=kafka:9092" in content
        assert "kafka.confluent-schema-registry-url=http://sr:8081" in content

    def test_catalog_file_with_sasl_auth_writes_client_props(self, tmp_path):
        written = tcf.write_kafka_catalog_files(
            {
                "id": "secure",
                "bootstrap_servers": "kafka:9092",
                "auth": {"type": "sasl_plain", "username": "u", "password": "p"},
            },
            tmp_path,
        )
        names = {p.name for p in written}
        assert "secure.properties" in names
        assert "kafka-client.properties" in names
        client = next(p for p in written if p.name == "kafka-client.properties").read_text()
        assert "SASL_PLAINTEXT" in client
        assert "sasl.mechanism=PLAIN" in client


class TestNonMappingSource:
    def test_returns_none_for_jdbc_source(self):
        src = Source(id="pg", type=SourceType.postgresql, host="h", port=5432)
        assert tcf.catalog_properties_for(src, "secret") is None
        assert tcf.is_mapping_dsl_source(src) is False
