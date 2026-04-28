# Copyright (c) 2026 Kenneth Stott
# Canary: a59d52d1-15d2-40b1-89c7-e75fd4df7a86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Prometheus source mapping (REQ-250)."""

from __future__ import annotations

import pytest

from provisa.prometheus.source import (
    PrometheusSourceConfig,
    PrometheusTableConfig,
    discover_schema,
    generate_catalog_properties,
    generate_table_definitions,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _source_config(
    *,
    url: str = "http://localhost:9090",
    tables: list[PrometheusTableConfig] | None = None,
) -> PrometheusSourceConfig:
    return PrometheusSourceConfig(id="prom-1", url=url, tables=tables or [])


def _table_config(
    name: str = "http_requests",
    metric: str = "http_requests_total",
    labels_as_columns: list[str] | None = None,
    value_column: str = "value",
    default_range: str = "1h",
) -> PrometheusTableConfig:
    return PrometheusTableConfig(
        name=name,
        metric=metric,
        labels_as_columns=labels_as_columns or [],
        value_column=value_column,
        default_range=default_range,
    )


# --------------------------------------------------------------------------- #
# TestGenerateCatalogProperties                                                #
# --------------------------------------------------------------------------- #


class TestGenerateCatalogProperties:
    def test_connector_name_is_prometheus(self):
        props = generate_catalog_properties(_source_config())
        assert props["connector.name"] == "prometheus"

    def test_default_url_is_localhost_9090(self):
        props = generate_catalog_properties(_source_config())
        assert props["prometheus.uri"] == "http://localhost:9090"

    def test_custom_url_propagated(self):
        props = generate_catalog_properties(_source_config(url="http://prom.internal:9090"))
        assert props["prometheus.uri"] == "http://prom.internal:9090"

    def test_only_two_keys_returned(self):
        props = generate_catalog_properties(_source_config())
        assert set(props.keys()) == {"connector.name", "prometheus.uri"}


# --------------------------------------------------------------------------- #
# TestGenerateTableDefinitions                                                 #
# --------------------------------------------------------------------------- #


class TestGenerateTableDefinitions:
    def test_empty_tables_returns_empty_list(self):
        cfg = _source_config(tables=[])
        assert generate_table_definitions(cfg) == []

    def test_single_table_produces_one_entry(self):
        cfg = _source_config(tables=[_table_config()])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 1

    def test_table_name_in_definition(self):
        cfg = _source_config(tables=[_table_config(name="cpu_usage")])
        defs = generate_table_definitions(cfg)
        assert defs[0]["tableName"] == "cpu_usage"

    def test_metric_name_in_definition(self):
        cfg = _source_config(tables=[_table_config(metric="node_cpu_seconds_total")])
        defs = generate_table_definitions(cfg)
        assert defs[0]["metric"] == "node_cpu_seconds_total"

    def test_timestamp_column_always_present(self):
        cfg = _source_config(tables=[_table_config()])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert "timestamp" in col_names

    def test_timestamp_column_is_timestamp_type(self):
        cfg = _source_config(tables=[_table_config()])
        defs = generate_table_definitions(cfg)
        ts_col = next(c for c in defs[0]["columns"] if c["name"] == "timestamp")
        assert ts_col["type"] == "TIMESTAMP"

    def test_value_column_always_present(self):
        cfg = _source_config(tables=[_table_config(value_column="value")])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert "value" in col_names

    def test_value_column_is_double_type(self):
        cfg = _source_config(tables=[_table_config(value_column="value")])
        defs = generate_table_definitions(cfg)
        val_col = next(c for c in defs[0]["columns"] if c["name"] == "value")
        assert val_col["type"] == "DOUBLE"

    def test_custom_value_column_name_used(self):
        cfg = _source_config(tables=[_table_config(value_column="rate")])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert "rate" in col_names
        assert "value" not in col_names

    def test_labels_as_columns_included(self):
        tbl = _table_config(labels_as_columns=["instance", "job"])
        cfg = _source_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert "instance" in col_names
        assert "job" in col_names

    def test_label_columns_are_varchar_type(self):
        tbl = _table_config(labels_as_columns=["instance"])
        cfg = _source_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        label_col = next(c for c in defs[0]["columns"] if c["name"] == "instance")
        assert label_col["type"] == "VARCHAR"

    def test_no_labels_only_timestamp_and_value(self):
        tbl = _table_config(labels_as_columns=[])
        cfg = _source_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert col_names == ["timestamp", "value"]

    def test_default_range_applied(self):
        tbl = _table_config(default_range="5m")
        cfg = _source_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        assert defs[0]["defaultRange"] == "5m"

    def test_default_range_default_is_1h(self):
        cfg = _source_config(tables=[_table_config()])
        defs = generate_table_definitions(cfg)
        assert defs[0]["defaultRange"] == "1h"

    def test_column_order_timestamp_value_then_labels(self):
        tbl = _table_config(labels_as_columns=["job", "instance"])
        cfg = _source_config(tables=[tbl])
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert col_names[0] == "timestamp"
        assert col_names[1] == "value"

    def test_multiple_tables_produce_multiple_definitions(self):
        t1 = _table_config(name="cpu", metric="node_cpu_seconds_total")
        t2 = _table_config(name="mem", metric="node_memory_bytes")
        cfg = _source_config(tables=[t1, t2])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 2
        names = {d["tableName"] for d in defs}
        assert names == {"cpu", "mem"}

    def test_definition_has_expected_keys(self):
        cfg = _source_config(tables=[_table_config()])
        defs = generate_table_definitions(cfg)
        assert set(defs[0].keys()) == {"tableName", "metric", "defaultRange", "columns"}


# --------------------------------------------------------------------------- #
# TestDiscoverSchema                                                           #
# --------------------------------------------------------------------------- #


class TestDiscoverSchema:
    def test_counter_metric_has_timestamp_and_value(self):
        metadata = {"labels": [], "type": "counter"}
        cols = discover_schema(metadata, "http_requests_total")
        col_names = [c["name"] for c in cols]
        assert "timestamp" in col_names
        assert "value" in col_names

    def test_gauge_metric_has_timestamp_and_value(self):
        metadata = {"labels": [], "type": "gauge"}
        cols = discover_schema(metadata, "node_memory_bytes")
        col_names = [c["name"] for c in cols]
        assert "timestamp" in col_names
        assert "value" in col_names

    def test_timestamp_is_timestamp_type(self):
        metadata = {"labels": [], "type": "gauge"}
        cols = discover_schema(metadata, "some_metric")
        ts_col = next(c for c in cols if c["name"] == "timestamp")
        assert ts_col["type"] == "TIMESTAMP"

    def test_value_is_double_type(self):
        metadata = {"labels": [], "type": "gauge"}
        cols = discover_schema(metadata, "some_metric")
        val_col = next(c for c in cols if c["name"] == "value")
        assert val_col["type"] == "DOUBLE"

    def test_labels_appear_as_varchar_columns(self):
        metadata = {"labels": ["instance", "job"], "type": "counter"}
        cols = discover_schema(metadata, "http_requests_total")
        col_names = [c["name"] for c in cols]
        assert "instance" in col_names
        assert "job" in col_names
        for col in cols:
            if col["name"] in ("instance", "job"):
                assert col["type"] == "VARCHAR"

    def test_dunder_name_label_excluded(self):
        metadata = {"labels": ["__name__", "instance"], "type": "gauge"}
        cols = discover_schema(metadata, "some_metric")
        col_names = [c["name"] for c in cols]
        assert "__name__" not in col_names

    def test_labels_sorted_alphabetically(self):
        metadata = {"labels": ["zone", "instance", "job"], "type": "gauge"}
        cols = discover_schema(metadata, "some_metric")
        label_cols = [c["name"] for c in cols if c["name"] not in ("timestamp", "value")]
        assert label_cols == sorted(label_cols)

    def test_histogram_adds_le_column(self):
        metadata = {"labels": [], "type": "histogram"}
        cols = discover_schema(metadata, "http_request_duration_seconds")
        col_names = [c["name"] for c in cols]
        assert "le" in col_names

    def test_histogram_le_is_varchar_type(self):
        metadata = {"labels": [], "type": "histogram"}
        cols = discover_schema(metadata, "http_request_duration_seconds")
        le_col = next(c for c in cols if c["name"] == "le")
        assert le_col["type"] == "VARCHAR"

    def test_histogram_does_not_add_quantile(self):
        metadata = {"labels": [], "type": "histogram"}
        cols = discover_schema(metadata, "http_request_duration_seconds")
        col_names = [c["name"] for c in cols]
        assert "quantile" not in col_names

    def test_summary_adds_quantile_column(self):
        metadata = {"labels": [], "type": "summary"}
        cols = discover_schema(metadata, "rpc_duration_seconds")
        col_names = [c["name"] for c in cols]
        assert "quantile" in col_names

    def test_summary_quantile_is_varchar_type(self):
        metadata = {"labels": [], "type": "summary"}
        cols = discover_schema(metadata, "rpc_duration_seconds")
        q_col = next(c for c in cols if c["name"] == "quantile")
        assert q_col["type"] == "VARCHAR"

    def test_summary_does_not_add_le(self):
        metadata = {"labels": [], "type": "summary"}
        cols = discover_schema(metadata, "rpc_duration_seconds")
        col_names = [c["name"] for c in cols]
        assert "le" not in col_names

    def test_missing_labels_key_defaults_to_empty(self):
        metadata = {"type": "gauge"}
        cols = discover_schema(metadata, "some_metric")
        col_names = [c["name"] for c in cols]
        assert col_names == ["timestamp", "value"]

    def test_missing_type_key_defaults_to_gauge_behaviour(self):
        metadata = {"labels": []}
        cols = discover_schema(metadata, "some_metric")
        col_names = [c["name"] for c in cols]
        assert "le" not in col_names
        assert "quantile" not in col_names

    def test_histogram_with_labels_includes_both(self):
        metadata = {"labels": ["handler"], "type": "histogram"}
        cols = discover_schema(metadata, "http_request_duration_seconds")
        col_names = [c["name"] for c in cols]
        assert "handler" in col_names
        assert "le" in col_names
