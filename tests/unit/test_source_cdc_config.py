# Copyright (c) 2026 Kenneth Stott
# Canary: 59fd04d2-a497-47b7-8b40-cce3cd670782
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for REQ-824: source-level CDC transport (Debezium/Kafka).

Debezium/Kafka delta-transport (bootstrap_servers, topic_prefix, ...) is entered
once on the source, never per-table. Per-table live config only picks
strategy=debezium/kafka; the runtime inherits the transport from the source.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data.subscribe import (
    _build_cdc_config,
    _resolve_provider_type,
    _CDC_DEBEZIUM_SOURCE_TYPES,
)
from provisa.core.config_loader import _validate_change_signal, _validate_table_live_delivery
from provisa.core.models import Source, SourceCdcConfig, SourceType


def _source_with_cdc(stype="mysql", **cdc_over):
    cdc = SourceCdcConfig(
        bootstrap_servers=cdc_over.get("bootstrap_servers", "broker:9092"),
        topic_prefix=cdc_over.get("topic_prefix", "dbserver1"),
        schema_registry_url=cdc_over.get("schema_registry_url"),
        consumer_group_id=cdc_over.get("consumer_group_id", "provisa-debezium"),
    )
    return Source(id="s1", type=SourceType(stype), database="app", cdc=cdc)


class TestSourceCdcModel:
    def test_defaults(self):
        c = SourceCdcConfig(bootstrap_servers="b:9092", topic_prefix="dbserver1")
        assert c.schema_registry_url is None
        # REQ-931: consumer_group_id is a receiver-side setting; None = inherit the Provisa-level
        # cdc_consumer_group_id. Only set per-source for deliberate offset isolation.
        assert c.consumer_group_id is None

    def test_source_cdc_defaults_none(self):
        s = Source(id="s1", type=SourceType.mysql)
        assert s.cdc is None

    def test_source_carries_cdc(self):
        s = _source_with_cdc()
        assert s.cdc is not None
        assert s.cdc.bootstrap_servers == "broker:9092"
        assert s.cdc.topic_prefix == "dbserver1"


class TestCdcValidation:
    def _cfg(self, source, *, strategy="debezium", watermark_column=None, kafka=None):
        live = SimpleNamespace(strategy=strategy, watermark_column=watermark_column, kafka=kafka)
        table = SimpleNamespace(table_name="orders", source_id=source.id, live=live)
        return SimpleNamespace(tables=[table], sources=[source])

    def test_non_pg_rdbms_with_cdc_block_allows_debezium(self):
        _validate_table_live_delivery(self._cfg(_source_with_cdc("mysql")))

    @pytest.mark.parametrize("stype", ["mysql", "oracle", "sqlserver", "mariadb"])
    def test_non_pg_rdbms_without_cdc_block_rejects_debezium(self, stype):
        src = Source(id="s1", type=SourceType(stype))
        with pytest.raises(ValueError, match="requires source-level cdc transport"):
            _validate_table_live_delivery(self._cfg(src))

    def test_cdc_block_on_non_capable_source_rejected(self):
        # A warehouse cannot host a Debezium transport block.
        src = _source_with_cdc("snowflake")
        with pytest.raises(ValueError, match="cdc transport config not supported"):
            _validate_table_live_delivery(self._cfg(src, strategy="poll", watermark_column="ts"))

    def test_postgres_debezium_needs_cdc_block(self):
        # PostgreSQL debezium still requires a source-level cdc transport block.
        src = Source(id="s1", type=SourceType.postgresql)
        with pytest.raises(ValueError, match="requires source-level cdc transport"):
            _validate_table_live_delivery(self._cfg(src))

    def test_postgres_native_needs_no_block(self):
        # postgresql supports strategy=native (LISTEN/NOTIFY) without a cdc block.
        src = Source(id="s1", type=SourceType.postgresql)
        _validate_table_live_delivery(self._cfg(src, strategy="native"))


class TestProviderRouting:
    @staticmethod
    def _tbl(strategy=None):
        live = SimpleNamespace(strategy=strategy) if strategy is not None else None
        return SimpleNamespace(live=live)

    # REQ-932: _resolve_provider_type resolves source-level change_signal inherit from
    # state.config.sources; an empty list means no source override (table/legacy signal wins).
    _EMPTY_CFG = SimpleNamespace(sources=[])

    def test_strategy_debezium_routes_to_debezium(self):
        state = SimpleNamespace(
            cdc_sources={"s1": _source_with_cdc("mysql")}, config=self._EMPTY_CFG
        )
        assert _resolve_provider_type("mysql", "s1", self._tbl("debezium"), state) == "debezium"

    def test_strategy_kafka_routes_to_kafka(self):
        state = SimpleNamespace(cdc_sources={}, config=self._EMPTY_CFG)
        assert _resolve_provider_type("postgresql", "s1", self._tbl("kafka"), state) == "kafka"

    def test_strategy_native_on_pg_routes_to_source_type(self):
        state = SimpleNamespace(cdc_sources={}, config=self._EMPTY_CFG)
        assert (
            _resolve_provider_type("postgresql", "s1", self._tbl("native"), state) == "postgresql"
        )

    def test_strategy_poll_routes_to_source_type(self):
        state = SimpleNamespace(cdc_sources={}, config=self._EMPTY_CFG)
        assert _resolve_provider_type("mysql", "s1", self._tbl("poll"), state) == "mysql"

    def test_no_strategy_falls_back_to_cdc_heuristic(self):
        # REQ-824: no explicit strategy, but a registered non-PG RDBMS cdc source.
        state = SimpleNamespace(cdc_sources={"s1": _source_with_cdc("mysql")})
        assert _resolve_provider_type("mysql", "s1", self._tbl(), state) == "debezium"

    def test_no_strategy_without_registration_stays_source_type(self):
        state = SimpleNamespace(cdc_sources={})
        assert _resolve_provider_type("mysql", "s1", self._tbl(), state) == "mysql"

    def test_postgres_no_strategy_never_routes_to_debezium(self):
        state = SimpleNamespace(cdc_sources={"s1": _source_with_cdc("postgresql")})
        assert _resolve_provider_type("postgresql", "s1", self._tbl(), state) == "postgresql"
        assert "postgresql" not in _CDC_DEBEZIUM_SOURCE_TYPES

    def test_build_cdc_config_reads_source_transport(self):
        state = SimpleNamespace(
            cdc_sources={"s1": _source_with_cdc("mysql", schema_registry_url="http://sr:8081")}
        )
        cfg = _build_cdc_config(state, "s1")
        assert cfg == {
            "bootstrap_servers": "broker:9092",
            "topic_prefix": "dbserver1",
            "schema_registry_url": "http://sr:8081",
            "consumer_group_id": "provisa-debezium",
            "database": "app",
            "source_type": "mysql",
        }

    def test_build_cdc_config_fails_loud_without_block(self):
        state = SimpleNamespace(cdc_sources={})
        with pytest.raises(ValueError, match="no source-level"):
            _build_cdc_config(state, "s1")


class TestChangeSignalValidation:
    """REQ-932: change_signal capability gate (push transports need the source's cdc block)."""

    @staticmethod
    def _cfg(source, table_signal):
        table = SimpleNamespace(
            source_id=source.id, table_name="orders", change_signal=table_signal
        )
        return SimpleNamespace(sources=[source], tables=[table])

    def test_debezium_requires_cdc_block(self):
        src = Source(id="s1", type=SourceType.mysql, database="app")  # no cdc block
        with pytest.raises(ValueError, match="requires source-level cdc"):
            _validate_change_signal(self._cfg(src, "debezium"))

    def test_debezium_with_cdc_block_ok(self):
        _validate_change_signal(self._cfg(_source_with_cdc("mysql"), "debezium"))

    def test_kafka_source_type_needs_no_block(self):
        src = Source(id="s1", type=SourceType.kafka, database="app")
        _validate_change_signal(self._cfg(src, "kafka"))

    def test_poll_signal_unrestricted(self):
        src = Source(id="s1", type=SourceType.mysql, database="app")
        _validate_change_signal(self._cfg(src, "ttl"))

    def test_table_inherits_source_signal(self):
        # No table override; source declares debezium but has no cdc block → still rejected.
        src = Source(id="s1", type=SourceType.mysql, database="app", change_signal="debezium")
        with pytest.raises(ValueError, match="requires source-level cdc"):
            _validate_change_signal(self._cfg(src, None))
