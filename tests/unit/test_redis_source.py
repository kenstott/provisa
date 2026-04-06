# Copyright (c) 2026 Kenneth Stott
# Canary: f3a91d2e-07bc-4e85-b6d4-c29f5a810e3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Redis source mapping (REQ-250)."""

from __future__ import annotations

import json

import pytest

from provisa.redis.source import (
    RedisColumn,
    RedisSourceConfig,
    RedisTableConfig,
    ValueType,
    _data_format_for,
    generate_catalog_properties,
    generate_table_definitions,
    generate_table_json,
)


# --------------------------------------------------------------------------- #
# TestRedisDataFormat                                                          #
# --------------------------------------------------------------------------- #


class TestRedisDataFormat:
    def test_hash_maps_to_hash(self):
        assert _data_format_for("hash") == "hash"

    def test_string_maps_to_raw(self):
        assert _data_format_for("string") == "raw"

    def test_zset_maps_to_json(self):
        assert _data_format_for("zset") == "json"

    def test_list_maps_to_json(self):
        assert _data_format_for("list") == "json"

    def test_unknown_value_type_falls_back_to_raw(self):
        assert _data_format_for("stream") == "raw"
        assert _data_format_for("") == "raw"
        assert _data_format_for("SET") == "raw"


# --------------------------------------------------------------------------- #
# TestGenerateCatalogProperties                                                #
# --------------------------------------------------------------------------- #


def _simple_config(**kwargs) -> RedisSourceConfig:
    """Build a minimal RedisSourceConfig for catalog-property tests."""
    defaults = dict(
        id="redis-1",
        host="redis.example.com",
        port=6379,
        password=None,
        tables=[
            RedisTableConfig(
                name="orders",
                key_pattern="order:*",
                key_column="order_key",
            ),
            RedisTableConfig(
                name="users",
                key_pattern="user:*",
                key_column="user_key",
            ),
        ],
    )
    defaults.update(kwargs)
    return RedisSourceConfig(**defaults)


class TestGenerateCatalogProperties:
    def test_connector_name_is_redis(self):
        props = generate_catalog_properties(_simple_config())
        assert props["connector.name"] == "redis"

    def test_redis_nodes_host_port(self):
        props = generate_catalog_properties(_simple_config(host="cache.internal", port=6380))
        assert props["redis.nodes"] == "cache.internal:6380"

    def test_table_names_comma_separated(self):
        props = generate_catalog_properties(_simple_config())
        names = props["redis.table-names"].split(",")
        assert set(names) == {"orders", "users"}

    def test_key_delimiter_is_colon(self):
        props = generate_catalog_properties(_simple_config())
        assert props["redis.key-delimiter"] == ":"

    def test_table_description_dir_present(self):
        props = generate_catalog_properties(_simple_config())
        assert "redis.table-description-dir" in props
        assert props["redis.table-description-dir"]  # non-empty

    def test_no_password_key_absent(self):
        props = generate_catalog_properties(_simple_config(password=None))
        assert "redis.password" not in props

    def test_with_password_key_present(self):
        props = generate_catalog_properties(_simple_config(password="s3cr3t"))
        assert props["redis.password"] == "s3cr3t"


# --------------------------------------------------------------------------- #
# TestGenerateTableDefinitions                                                 #
# --------------------------------------------------------------------------- #


def _hash_table(
    name: str = "orders",
    key_column: str = "order_key",
    key_pattern: str = "order:*",
    value_type: str = ValueType.HASH,
    columns: list[RedisColumn] | None = None,
) -> RedisTableConfig:
    return RedisTableConfig(
        name=name,
        key_pattern=key_pattern,
        key_column=key_column,
        value_type=value_type,
        columns=columns or [],
    )


def _config_with_tables(*tables: RedisTableConfig) -> RedisSourceConfig:
    return RedisSourceConfig(id="r1", host="localhost", port=6379, tables=list(tables))


class TestGenerateTableDefinitions:
    def test_single_hash_table_structure(self):
        col = RedisColumn(name="amount", data_type="BIGINT")
        cfg = _config_with_tables(_hash_table(columns=[col]))
        defs = generate_table_definitions(cfg)
        assert len(defs) == 1
        td = defs[0]
        assert td["tableName"] == "orders"
        assert "key" in td
        assert "value" in td

    def test_key_column_in_key_section_with_mapping_key(self):
        cfg = _config_with_tables(_hash_table(key_column="order_key"))
        defs = generate_table_definitions(cfg)
        key_fields = defs[0]["key"]["fields"]
        assert len(key_fields) == 1
        key_field = key_fields[0]
        assert key_field["name"] == "order_key"
        assert key_field["mapping"] == "key"
        assert key_field["type"] == "VARCHAR"

    def test_each_column_in_value_section(self):
        cols = [
            RedisColumn(name="amount", data_type="BIGINT"),
            RedisColumn(name="status", data_type="VARCHAR"),
        ]
        cfg = _config_with_tables(_hash_table(columns=cols))
        defs = generate_table_definitions(cfg)
        value_fields = defs[0]["value"]["fields"]
        assert len(value_fields) == 2
        names = {f["name"] for f in value_fields}
        assert names == {"amount", "status"}

    def test_column_with_explicit_field_uses_field_as_mapping(self):
        col = RedisColumn(name="order_id", data_type="INTEGER", field="oid")
        cfg = _config_with_tables(_hash_table(columns=[col]))
        defs = generate_table_definitions(cfg)
        value_fields = defs[0]["value"]["fields"]
        field_def = next(f for f in value_fields if f["name"] == "order_id")
        assert field_def["mapping"] == "oid"

    def test_column_without_field_uses_column_name_as_mapping(self):
        col = RedisColumn(name="status", data_type="VARCHAR")
        cfg = _config_with_tables(_hash_table(columns=[col]))
        defs = generate_table_definitions(cfg)
        value_fields = defs[0]["value"]["fields"]
        field_def = next(f for f in value_fields if f["name"] == "status")
        assert field_def["mapping"] == "status"

    def test_value_type_hash_produces_data_format_hash(self):
        cfg = _config_with_tables(_hash_table(value_type=ValueType.HASH))
        defs = generate_table_definitions(cfg)
        assert defs[0]["value"]["dataFormat"] == "hash"

    def test_value_type_string_produces_data_format_raw(self):
        cfg = _config_with_tables(_hash_table(value_type=ValueType.STRING))
        defs = generate_table_definitions(cfg)
        assert defs[0]["value"]["dataFormat"] == "raw"

    def test_value_type_zset_produces_data_format_json(self):
        cfg = _config_with_tables(_hash_table(value_type=ValueType.ZSET))
        defs = generate_table_definitions(cfg)
        assert defs[0]["value"]["dataFormat"] == "json"

    def test_value_type_list_produces_data_format_json(self):
        cfg = _config_with_tables(_hash_table(value_type=ValueType.LIST))
        defs = generate_table_definitions(cfg)
        assert defs[0]["value"]["dataFormat"] == "json"

    def test_empty_columns_key_field_only_in_value_section(self):
        cfg = _config_with_tables(_hash_table(columns=[]))
        defs = generate_table_definitions(cfg)
        # key section has the key column; value section is empty
        assert defs[0]["key"]["fields"][0]["name"] == "order_key"
        assert defs[0]["value"]["fields"] == []

    def test_multiple_tables_produce_multiple_definitions(self):
        t1 = _hash_table(name="orders", key_column="order_key", key_pattern="order:*")
        t2 = _hash_table(name="sessions", key_column="session_key", key_pattern="session:*")
        cfg = _config_with_tables(t1, t2)
        defs = generate_table_definitions(cfg)
        assert len(defs) == 2
        names = {d["tableName"] for d in defs}
        assert names == {"orders", "sessions"}


# --------------------------------------------------------------------------- #
# TestGenerateTableJson                                                        #
# --------------------------------------------------------------------------- #


class TestGenerateTableJson:
    def test_keyed_by_table_name_dot_json(self):
        col = RedisColumn(name="amount", data_type="BIGINT")
        cfg = _config_with_tables(_hash_table(name="orders", columns=[col]))
        result = generate_table_json(cfg)
        assert "orders.json" in result

    def test_values_are_valid_json_strings(self):
        col = RedisColumn(name="amount", data_type="BIGINT")
        cfg = _config_with_tables(_hash_table(name="orders", columns=[col]))
        result = generate_table_json(cfg)
        for content in result.values():
            parsed = json.loads(content)  # must not raise
            assert isinstance(parsed, dict)

    def test_json_parses_to_expected_structure(self):
        col = RedisColumn(name="amount", data_type="BIGINT", field="amt")
        cfg = _config_with_tables(_hash_table(name="orders", columns=[col]))
        result = generate_table_json(cfg)
        parsed = json.loads(result["orders.json"])
        assert parsed["tableName"] == "orders"
        assert parsed["key"]["fields"][0]["mapping"] == "key"
        value_field = parsed["value"]["fields"][0]
        assert value_field["name"] == "amount"
        assert value_field["mapping"] == "amt"

    def test_multiple_tables_produce_multiple_json_files(self):
        t1 = _hash_table(name="orders", key_column="order_key", key_pattern="order:*")
        t2 = _hash_table(name="sessions", key_column="session_key", key_pattern="session:*")
        cfg = _config_with_tables(t1, t2)
        result = generate_table_json(cfg)
        assert "orders.json" in result
        assert "sessions.json" in result
        assert len(result) == 2


# --------------------------------------------------------------------------- #
# TestRedisColumnConfig                                                        #
# --------------------------------------------------------------------------- #


class TestRedisColumnConfig:
    def test_redis_column_without_field(self):
        col = RedisColumn(name="status", data_type="VARCHAR")
        assert col.name == "status"
        assert col.data_type == "VARCHAR"
        assert col.field is None

    def test_redis_column_with_explicit_field(self):
        col = RedisColumn(name="order_id", data_type="INTEGER", field="oid")
        assert col.field == "oid"

    def test_redis_table_config_fields(self):
        tbl = RedisTableConfig(
            name="orders",
            key_column="order_key",
            key_pattern="order:*",
            value_type=ValueType.HASH,
        )
        assert tbl.key_column == "order_key"
        assert tbl.key_pattern == "order:*"
        assert tbl.value_type == ValueType.HASH
        assert tbl.columns == []

    def test_redis_source_config_fields(self):
        cfg = RedisSourceConfig(
            id="redis-prod",
            host="redis.prod.internal",
            port=6380,
            password="hunter2",
            tables=[],
        )
        assert cfg.host == "redis.prod.internal"
        assert cfg.port == 6380
        assert cfg.password == "hunter2"
        assert cfg.tables == []
