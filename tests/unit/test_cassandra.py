# Copyright (c) 2026 Kenneth Stott
# Canary: a4192999-8b71-43dd-b294-aeda3f5daba8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the Cassandra source adapter (provisa/cassandra/source.py)."""

from __future__ import annotations

import pytest

from provisa.cassandra.source import (
    CQL_TYPE_TO_TRINO,
    CassandraSourceConfig,
    CassandraTableConfig,
    discover_schema,
    generate_catalog_properties,
    generate_table_definitions,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    *,
    contact_points: str = "10.0.0.1",
    port: int = 9042,
    username: str | None = None,
    password: str | None = None,
    tables: list[CassandraTableConfig] | None = None,
) -> CassandraSourceConfig:
    return CassandraSourceConfig(
        id="cass-prod",
        contact_points=contact_points,
        port=port,
        username=username,
        password=password,
        tables=tables or [],
    )


def _make_table(name: str = "user_activity", keyspace: str = "analytics") -> CassandraTableConfig:
    return CassandraTableConfig(
        name=name,
        keyspace=keyspace,
        table=name,
        discover=False,
    )


def _make_keyspace_metadata(
    columns: list[dict],
    partition_keys: list[str] | None = None,
    clustering_keys: list[str] | None = None,
) -> dict:
    return {
        "columns": columns,
        "partition_keys": partition_keys or [],
        "clustering_keys": clustering_keys or [],
    }


# ---------------------------------------------------------------------------
# TestCQLTypeToTrinoMapping
# ---------------------------------------------------------------------------


class TestCQLTypeToTrinoMapping:
    def test_text_maps_to_varchar(self):
        assert CQL_TYPE_TO_TRINO["text"] == "VARCHAR"

    def test_ascii_maps_to_varchar(self):
        assert CQL_TYPE_TO_TRINO["ascii"] == "VARCHAR"

    def test_bigint_maps_to_bigint(self):
        assert CQL_TYPE_TO_TRINO["bigint"] == "BIGINT"

    def test_int_maps_to_integer(self):
        assert CQL_TYPE_TO_TRINO["int"] == "INTEGER"

    def test_boolean_maps_to_boolean(self):
        assert CQL_TYPE_TO_TRINO["boolean"] == "BOOLEAN"

    def test_double_maps_to_double(self):
        assert CQL_TYPE_TO_TRINO["double"] == "DOUBLE"

    def test_float_maps_to_real(self):
        assert CQL_TYPE_TO_TRINO["float"] == "REAL"

    def test_timestamp_maps_to_timestamp(self):
        assert CQL_TYPE_TO_TRINO["timestamp"] == "TIMESTAMP"

    def test_date_maps_to_date(self):
        assert CQL_TYPE_TO_TRINO["date"] == "DATE"

    def test_uuid_maps_to_uuid(self):
        assert CQL_TYPE_TO_TRINO["uuid"] == "UUID"

    def test_timeuuid_maps_to_uuid(self):
        assert CQL_TYPE_TO_TRINO["timeuuid"] == "UUID"

    def test_blob_maps_to_varbinary(self):
        assert CQL_TYPE_TO_TRINO["blob"] == "VARBINARY"

    def test_inet_maps_to_varchar(self):
        assert CQL_TYPE_TO_TRINO["inet"] == "VARCHAR"

    def test_counter_maps_to_bigint(self):
        assert CQL_TYPE_TO_TRINO["counter"] == "BIGINT"

    def test_varint_maps_to_bigint(self):
        assert CQL_TYPE_TO_TRINO["varint"] == "BIGINT"

    def test_decimal_maps_to_decimal(self):
        assert CQL_TYPE_TO_TRINO["decimal"] == "DECIMAL"

    def test_smallint_maps_to_smallint(self):
        assert CQL_TYPE_TO_TRINO["smallint"] == "SMALLINT"

    def test_tinyint_maps_to_tinyint(self):
        assert CQL_TYPE_TO_TRINO["tinyint"] == "TINYINT"

    def test_list_maps_to_varchar(self):
        assert CQL_TYPE_TO_TRINO["list"] == "VARCHAR"

    def test_map_maps_to_varchar(self):
        assert CQL_TYPE_TO_TRINO["map"] == "VARCHAR"

    def test_set_maps_to_varchar(self):
        assert CQL_TYPE_TO_TRINO["set"] == "VARCHAR"

    def test_time_maps_to_time(self):
        assert CQL_TYPE_TO_TRINO["time"] == "TIME"


# ---------------------------------------------------------------------------
# TestCassandraSourceConfig
# ---------------------------------------------------------------------------


class TestCassandraSourceConfig:
    def test_defaults(self):
        cfg = CassandraSourceConfig(id="test-cass")
        assert cfg.contact_points == "localhost"
        assert cfg.port == 9042
        assert cfg.username is None
        assert cfg.password is None
        assert cfg.tables == []

    def test_custom_values(self):
        cfg = _make_config(
            contact_points="10.1.2.3,10.1.2.4",
            port=9043,
            username="admin",
            password="secret",
        )
        assert cfg.contact_points == "10.1.2.3,10.1.2.4"
        assert cfg.port == 9043
        assert cfg.username == "admin"
        assert cfg.password == "secret"

    def test_tables_attached(self):
        table = _make_table()
        cfg = _make_config(tables=[table])
        assert len(cfg.tables) == 1
        assert cfg.tables[0].name == "user_activity"


# ---------------------------------------------------------------------------
# TestCassandraTableConfig
# ---------------------------------------------------------------------------


class TestCassandraTableConfig:
    def test_discover_defaults_false(self):
        tbl = _make_table()
        assert tbl.discover is False

    def test_discover_enabled(self):
        tbl = CassandraTableConfig(
            name="events", keyspace="logs", table="events", discover=True,
        )
        assert tbl.discover is True

    def test_name_keyspace_table_fields(self):
        tbl = _make_table("pageviews", "web")
        assert tbl.name == "pageviews"
        assert tbl.keyspace == "web"
        assert tbl.table == "pageviews"


# ---------------------------------------------------------------------------
# TestGenerateCatalogProperties
# ---------------------------------------------------------------------------


class TestGenerateCatalogProperties:
    def test_required_keys_always_present(self):
        cfg = _make_config()
        props = generate_catalog_properties(cfg)
        assert props["connector.name"] == "cassandra"
        assert "cassandra.contact-points" in props
        assert "cassandra.native-protocol-port" in props

    def test_contact_points(self):
        cfg = _make_config(contact_points="host1,host2")
        props = generate_catalog_properties(cfg)
        assert props["cassandra.contact-points"] == "host1,host2"

    def test_port_as_string(self):
        cfg = _make_config(port=9043)
        props = generate_catalog_properties(cfg)
        assert props["cassandra.native-protocol-port"] == "9043"

    def test_no_credentials_when_absent(self):
        cfg = _make_config()
        props = generate_catalog_properties(cfg)
        assert "cassandra.username" not in props
        assert "cassandra.password" not in props

    def test_credentials_included_when_set(self):
        cfg = _make_config(username="user", password="pass")
        props = generate_catalog_properties(cfg)
        assert props["cassandra.username"] == "user"
        assert props["cassandra.password"] == "pass"

    def test_username_without_password(self):
        cfg = _make_config(username="readonly")
        props = generate_catalog_properties(cfg)
        assert props["cassandra.username"] == "readonly"
        assert "cassandra.password" not in props

    def test_returns_dict_of_strings(self):
        cfg = _make_config()
        props = generate_catalog_properties(cfg)
        assert isinstance(props, dict)
        for k, v in props.items():
            assert isinstance(k, str)
            assert isinstance(v, str)


# ---------------------------------------------------------------------------
# TestGenerateTableDefinitions
# ---------------------------------------------------------------------------


class TestGenerateTableDefinitions:
    def test_empty_tables(self):
        cfg = _make_config(tables=[])
        defs = generate_table_definitions(cfg)
        assert defs == []

    def test_single_table(self):
        tbl = _make_table("events", "logs")
        cfg = _make_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 1
        assert defs[0]["tableName"] == "events"
        assert defs[0]["keyspace"] == "logs"
        assert defs[0]["table"] == "events"
        assert defs[0]["discover"] is False

    def test_multiple_tables(self):
        tables = [
            _make_table("pageviews", "web"),
            _make_table("sessions", "web"),
        ]
        cfg = _make_config(tables=tables)
        defs = generate_table_definitions(cfg)
        assert len(defs) == 2
        names = {d["tableName"] for d in defs}
        assert names == {"pageviews", "sessions"}

    def test_discover_true_preserved(self):
        tbl = CassandraTableConfig(
            name="raw_events", keyspace="ingestion", table="raw_events", discover=True,
        )
        cfg = _make_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        assert defs[0]["discover"] is True

    def test_each_entry_has_required_keys(self):
        tbl = _make_table()
        cfg = _make_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        required = {"tableName", "keyspace", "table", "discover"}
        assert required.issubset(defs[0].keys())


# ---------------------------------------------------------------------------
# TestDiscoverSchema — schema introspection
# ---------------------------------------------------------------------------


class TestDiscoverSchema:
    def test_empty_metadata_returns_empty(self):
        result = discover_schema({})
        assert result == []

    def test_empty_columns_list(self):
        meta = _make_keyspace_metadata(columns=[])
        result = discover_schema(meta)
        assert result == []

    def test_basic_column_mapping(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "user_id", "type": "uuid"},
                {"name": "score", "type": "int"},
                {"name": "label", "type": "text"},
            ],
        )
        result = discover_schema(meta)
        assert len(result) == 3

        names = {col["name"] for col in result}
        assert names == {"user_id", "score", "label"}

        by_name = {col["name"]: col for col in result}
        assert by_name["user_id"]["type"] == "UUID"
        assert by_name["score"]["type"] == "INTEGER"
        assert by_name["label"]["type"] == "VARCHAR"

    def test_cql_type_preserved_in_output(self):
        meta = _make_keyspace_metadata(
            columns=[{"name": "ts", "type": "timestamp"}],
        )
        result = discover_schema(meta)
        assert result[0]["cqlType"] == "timestamp"

    def test_partition_key_annotation(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "tenant_id", "type": "uuid"},
                {"name": "record_id", "type": "bigint"},
            ],
            partition_keys=["tenant_id"],
        )
        result = discover_schema(meta)
        by_name = {col["name"]: col for col in result}
        assert by_name["tenant_id"].get("partitionKey") is True
        assert "partitionKey" not in by_name["record_id"]

    def test_clustering_key_annotation(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "bucket", "type": "int"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "payload", "type": "text"},
            ],
            clustering_keys=["created_at"],
        )
        result = discover_schema(meta)
        by_name = {col["name"]: col for col in result}
        assert by_name["created_at"].get("clusteringKey") is True
        assert "clusteringKey" not in by_name["bucket"]
        assert "clusteringKey" not in by_name["payload"]

    def test_partition_and_clustering_keys_together(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "org_id", "type": "uuid"},
                {"name": "event_time", "type": "timestamp"},
                {"name": "value", "type": "double"},
            ],
            partition_keys=["org_id"],
            clustering_keys=["event_time"],
        )
        result = discover_schema(meta)
        by_name = {col["name"]: col for col in result}
        assert by_name["org_id"].get("partitionKey") is True
        assert by_name["event_time"].get("clusteringKey") is True
        assert "partitionKey" not in by_name["value"]
        assert "clusteringKey" not in by_name["value"]

    def test_collection_type_stripped_to_base(self):
        """list<text>, map<text,int>, set<uuid> should strip the wrapper."""
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "tags", "type": "list<text>"},
                {"name": "meta", "type": "map<text, int>"},
                {"name": "flags", "type": "set<uuid>"},
            ],
        )
        result = discover_schema(meta)
        by_name = {col["name"]: col for col in result}
        # All collection types map to VARCHAR after stripping
        assert by_name["tags"]["type"] == "VARCHAR"
        assert by_name["meta"]["type"] == "VARCHAR"
        assert by_name["flags"]["type"] == "VARCHAR"
        # Raw CQL type should be preserved
        assert "list" in by_name["tags"]["cqlType"]
        assert "map" in by_name["meta"]["cqlType"]

    def test_unknown_cql_type_defaults_to_varchar(self):
        meta = _make_keyspace_metadata(
            columns=[{"name": "custom_col", "type": "custom_type"}],
        )
        result = discover_schema(meta)
        assert result[0]["type"] == "VARCHAR"

    def test_missing_type_key_defaults_gracefully(self):
        meta = _make_keyspace_metadata(
            columns=[{"name": "col_no_type"}],
        )
        result = discover_schema(meta)
        assert len(result) == 1
        assert result[0]["name"] == "col_no_type"
        # "text" is the default CQL type when absent
        assert result[0]["type"] == "VARCHAR"

    def test_non_key_columns_have_no_key_annotations(self):
        meta = _make_keyspace_metadata(
            columns=[{"name": "regular_col", "type": "text"}],
            partition_keys=[],
            clustering_keys=[],
        )
        result = discover_schema(meta)
        col = result[0]
        assert "partitionKey" not in col
        assert "clusteringKey" not in col

    def test_all_cql_types_mapped(self):
        """Every key in CQL_TYPE_TO_TRINO should survive discover_schema."""
        base_types = [k for k in CQL_TYPE_TO_TRINO if "<" not in k]
        columns = [{"name": t, "type": t} for t in base_types]
        meta = _make_keyspace_metadata(columns=columns)
        result = discover_schema(meta)
        assert len(result) == len(base_types)
        by_name = {col["name"]: col for col in result}
        for t in base_types:
            assert by_name[t]["type"] == CQL_TYPE_TO_TRINO[t]


# ---------------------------------------------------------------------------
# TestGraphQLTypeMapping (NoSQL → GraphQL type reasoning)
# ---------------------------------------------------------------------------


class TestGraphQLTypeMapping:
    """Verify that Cassandra column types map to appropriate Trino types
    which downstream schema generation can convert to GraphQL scalars."""

    def test_string_types_become_varchar(self):
        """text, ascii, varchar, inet, list, map, set all become VARCHAR."""
        string_cql = ["text", "ascii", "varchar", "inet", "list", "map", "set"]
        for cql in string_cql:
            assert CQL_TYPE_TO_TRINO[cql] == "VARCHAR", f"Expected VARCHAR for {cql}"

    def test_integer_types_are_numeric(self):
        integer_cql = ["bigint", "int", "smallint", "tinyint", "varint", "counter"]
        numeric_trino = {"BIGINT", "INTEGER", "SMALLINT", "TINYINT"}
        for cql in integer_cql:
            assert CQL_TYPE_TO_TRINO[cql] in numeric_trino, (
                f"Expected numeric type for {cql}, got {CQL_TYPE_TO_TRINO[cql]}"
            )

    def test_id_types_become_uuid(self):
        assert CQL_TYPE_TO_TRINO["uuid"] == "UUID"
        assert CQL_TYPE_TO_TRINO["timeuuid"] == "UUID"

    def test_temporal_types_preserved(self):
        assert CQL_TYPE_TO_TRINO["timestamp"] == "TIMESTAMP"
        assert CQL_TYPE_TO_TRINO["date"] == "DATE"
        assert CQL_TYPE_TO_TRINO["time"] == "TIME"


# ---------------------------------------------------------------------------
# TestPagination (partition key awareness for cursor-based paging)
# ---------------------------------------------------------------------------


class TestPagination:
    """Cassandra pagination requires knowledge of partition and clustering keys.
    discover_schema annotates them so upper layers can build page tokens."""

    def test_partition_key_marked_for_pagination(self):
        """Partition key columns are required for building Cassandra page tokens."""
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "customer_id", "type": "uuid"},
                {"name": "order_date", "type": "date"},
                {"name": "total", "type": "decimal"},
            ],
            partition_keys=["customer_id"],
            clustering_keys=["order_date"],
        )
        result = discover_schema(meta)
        pk_cols = [col for col in result if col.get("partitionKey")]
        ck_cols = [col for col in result if col.get("clusteringKey")]

        assert len(pk_cols) == 1
        assert pk_cols[0]["name"] == "customer_id"

        assert len(ck_cols) == 1
        assert ck_cols[0]["name"] == "order_date"

    def test_composite_partition_key(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "region", "type": "text"},
                {"name": "bucket", "type": "int"},
                {"name": "event_id", "type": "uuid"},
            ],
            partition_keys=["region", "bucket"],
        )
        result = discover_schema(meta)
        pk_cols = [col["name"] for col in result if col.get("partitionKey")]
        assert set(pk_cols) == {"region", "bucket"}

    def test_compound_clustering_key(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "user_id", "type": "uuid"},
                {"name": "year", "type": "int"},
                {"name": "month", "type": "int"},
                {"name": "payload", "type": "text"},
            ],
            partition_keys=["user_id"],
            clustering_keys=["year", "month"],
        )
        result = discover_schema(meta)
        ck_cols = [col["name"] for col in result if col.get("clusteringKey")]
        assert set(ck_cols) == {"year", "month"}

    def test_non_key_columns_not_annotated_for_pagination(self):
        meta = _make_keyspace_metadata(
            columns=[
                {"name": "pk", "type": "uuid"},
                {"name": "data", "type": "text"},
            ],
            partition_keys=["pk"],
        )
        result = discover_schema(meta)
        data_col = next(col for col in result if col["name"] == "data")
        assert "partitionKey" not in data_col
        assert "clusteringKey" not in data_col
