# Copyright (c) 2026 Kenneth Stott
# Canary: fb064706-57b2-4db0-9e18-3a8730a9c846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Accumulo source mapping (REQ-250)."""

from __future__ import annotations

import pytest

from provisa.accumulo.source import (
    AccumuloColumn,
    AccumuloSourceConfig,
    AccumuloTableConfig,
    generate_catalog_properties,
    generate_table_definitions,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _config(
    *,
    instance: str = "accumulo",
    zookeepers: str = "zoo1:2181",
    username: str | None = None,
    password: str | None = None,
    tables: list[AccumuloTableConfig] | None = None,
) -> AccumuloSourceConfig:
    return AccumuloSourceConfig(
        id="acc-1",
        instance=instance,
        zookeepers=zookeepers,
        username=username,
        password=password,
        tables=tables or [],
    )


def _column(
    name: str = "col1",
    data_type: str = "VARCHAR",
    family: str = "cf",
    qualifier: str = "cq",
) -> AccumuloColumn:
    return AccumuloColumn(name=name, data_type=data_type, family=family, qualifier=qualifier)


def _table(
    name: str = "mytable",
    accumulo_table: str = "ns.mytable",
    columns: list[AccumuloColumn] | None = None,
) -> AccumuloTableConfig:
    return AccumuloTableConfig(name=name, accumulo_table=accumulo_table, columns=columns or [])


# --------------------------------------------------------------------------- #
# TestGenerateCatalogProperties                                                #
# --------------------------------------------------------------------------- #


class TestGenerateCatalogProperties:
    def test_connector_name_is_accumulo(self):
        props = generate_catalog_properties(_config())
        assert props["connector.name"] == "accumulo"

    def test_instance_propagated(self):
        props = generate_catalog_properties(_config(instance="myinstance"))
        assert props["accumulo.instance"] == "myinstance"

    def test_zookeepers_propagated(self):
        props = generate_catalog_properties(_config(zookeepers="zoo1:2181,zoo2:2181"))
        assert props["accumulo.zookeepers"] == "zoo1:2181,zoo2:2181"

    def test_username_included_when_set(self):
        props = generate_catalog_properties(_config(username="root"))
        assert props["accumulo.username"] == "root"

    def test_username_absent_when_none(self):
        props = generate_catalog_properties(_config(username=None))
        assert "accumulo.username" not in props

    def test_password_included_when_set(self):
        props = generate_catalog_properties(_config(username="root", password="secret"))
        assert props["accumulo.password"] == "secret"

    def test_password_absent_when_none(self):
        props = generate_catalog_properties(_config(password=None))
        assert "accumulo.password" not in props

    def test_no_extra_keys_without_auth(self):
        props = generate_catalog_properties(_config())
        assert set(props.keys()) == {"connector.name", "accumulo.instance", "accumulo.zookeepers"}

    def test_all_keys_present_with_full_auth(self):
        props = generate_catalog_properties(_config(username="root", password="secret"))
        assert "accumulo.username" in props
        assert "accumulo.password" in props


# --------------------------------------------------------------------------- #
# TestGenerateTableDefinitions                                                 #
# --------------------------------------------------------------------------- #


class TestGenerateTableDefinitions:
    def test_empty_tables_returns_empty_list(self):
        cfg = _config(tables=[])
        assert generate_table_definitions(cfg) == []

    def test_single_table_produces_one_entry(self):
        cfg = _config(tables=[_table()])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 1

    def test_table_name_in_definition(self):
        cfg = _config(tables=[_table(name="orders")])
        defs = generate_table_definitions(cfg)
        assert defs[0]["tableName"] == "orders"

    def test_accumulo_table_in_definition(self):
        cfg = _config(tables=[_table(accumulo_table="ns.orders")])
        defs = generate_table_definitions(cfg)
        assert defs[0]["accumuloTable"] == "ns.orders"

    def test_columns_key_present(self):
        cfg = _config(tables=[_table()])
        defs = generate_table_definitions(cfg)
        assert "columns" in defs[0]

    def test_no_columns_produces_empty_columns_list(self):
        cfg = _config(tables=[_table(columns=[])])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"] == []

    def test_column_name_mapped(self):
        col = _column(name="order_id")
        cfg = _config(tables=[_table(columns=[col])])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"][0]["name"] == "order_id"

    def test_column_type_preserved(self):
        col = _column(data_type="BIGINT")
        cfg = _config(tables=[_table(columns=[col])])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"][0]["type"] == "BIGINT"

    def test_column_family_mapped(self):
        col = _column(family="metadata")
        cfg = _config(tables=[_table(columns=[col])])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"][0]["family"] == "metadata"

    def test_column_qualifier_mapped(self):
        col = _column(qualifier="amount")
        cfg = _config(tables=[_table(columns=[col])])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"][0]["qualifier"] == "amount"

    def test_multiple_columns_all_present(self):
        cols = [
            _column(name="id", data_type="INTEGER", family="row", qualifier="id"),
            _column(name="price", data_type="DOUBLE", family="data", qualifier="price"),
            _column(name="label", data_type="VARCHAR", family="meta", qualifier="label"),
        ]
        cfg = _config(tables=[_table(columns=cols)])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert col_names == ["id", "price", "label"]

    def test_multiple_tables_produce_multiple_definitions(self):
        t1 = _table(name="orders", accumulo_table="ns.orders")
        t2 = _table(name="users", accumulo_table="ns.users")
        cfg = _config(tables=[t1, t2])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 2
        table_names = {d["tableName"] for d in defs}
        assert table_names == {"orders", "users"}

    def test_column_types_integer_and_double(self):
        cols = [
            _column(name="count", data_type="INTEGER", family="f", qualifier="cnt"),
            _column(name="ratio", data_type="DOUBLE", family="f", qualifier="rat"),
        ]
        cfg = _config(tables=[_table(columns=cols)])
        defs = generate_table_definitions(cfg)
        type_map = {c["name"]: c["type"] for c in defs[0]["columns"]}
        assert type_map["count"] == "INTEGER"
        assert type_map["ratio"] == "DOUBLE"

    def test_column_family_qualifier_independent_per_column(self):
        cols = [
            _column(name="a", family="cf1", qualifier="q1"),
            _column(name="b", family="cf2", qualifier="q2"),
        ]
        cfg = _config(tables=[_table(columns=cols)])
        defs = generate_table_definitions(cfg)
        by_name = {c["name"]: c for c in defs[0]["columns"]}
        assert by_name["a"]["family"] == "cf1"
        assert by_name["a"]["qualifier"] == "q1"
        assert by_name["b"]["family"] == "cf2"
        assert by_name["b"]["qualifier"] == "q2"

    def test_definition_has_exactly_three_keys(self):
        cfg = _config(tables=[_table()])
        defs = generate_table_definitions(cfg)
        assert set(defs[0].keys()) == {"tableName", "accumuloTable", "columns"}
