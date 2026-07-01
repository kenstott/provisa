# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for REQ-824: source-level CDC transport (Debezium/Kafka).

Debezium/Kafka delta-transport (bootstrap_servers, topic_prefix, ...) is entered
once on the source, never per-table. Per-table live config only picks delivery=cdc;
the runtime inherits the transport from the source.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data.subscribe import (
    _build_cdc_config,
    _resolve_provider_type,
    _CDC_DEBEZIUM_SOURCE_TYPES,
)
from provisa.core.config_loader import _validate_table_live_delivery
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
        assert c.consumer_group_id == "provisa-debezium"

    def test_source_cdc_defaults_none(self):
        s = Source(id="s1", type=SourceType.mysql)
        assert s.cdc is None

    def test_source_carries_cdc(self):
        s = _source_with_cdc()
        assert s.cdc.bootstrap_servers == "broker:9092"
        assert s.cdc.topic_prefix == "dbserver1"


class TestCdcValidation:
    def _cfg(self, source, *, delivery="cdc", watermark_column=None):
        live = SimpleNamespace(delivery=delivery, watermark_column=watermark_column)
        table = SimpleNamespace(table_name="orders", source_id=source.id, live=live)
        return SimpleNamespace(tables=[table], sources=[source])

    def test_non_pg_rdbms_with_cdc_block_allows_cdc(self):
        _validate_table_live_delivery(self._cfg(_source_with_cdc("mysql")))

    @pytest.mark.parametrize("stype", ["mysql", "oracle", "sqlserver", "mariadb"])
    def test_non_pg_rdbms_without_cdc_block_rejects_cdc(self, stype):
        src = Source(id="s1", type=SourceType(stype))
        with pytest.raises(ValueError, match="cdc not supported"):
            _validate_table_live_delivery(self._cfg(src))

    def test_cdc_block_on_non_capable_source_rejected(self):
        # A warehouse cannot host a Debezium transport block.
        src = _source_with_cdc("snowflake")
        with pytest.raises(ValueError, match="cdc transport config not supported"):
            _validate_table_live_delivery(self._cfg(src, delivery="poll", watermark_column="ts"))

    def test_postgres_cdc_needs_no_block(self):
        src = Source(id="s1", type=SourceType.postgresql)
        _validate_table_live_delivery(self._cfg(src))


class TestProviderRouting:
    def test_non_pg_rdbms_with_cdc_routes_to_debezium(self):
        state = SimpleNamespace(cdc_sources={"s1": _source_with_cdc("mysql")})
        assert _resolve_provider_type("mysql", "s1", state) == "debezium"

    def test_non_pg_rdbms_without_registration_stays_source_type(self):
        state = SimpleNamespace(cdc_sources={})
        assert _resolve_provider_type("mysql", "s1", state) == "mysql"

    def test_postgres_never_routes_to_debezium(self):
        state = SimpleNamespace(cdc_sources={"s1": _source_with_cdc("postgresql")})
        assert _resolve_provider_type("postgresql", "s1", state) == "postgresql"
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
