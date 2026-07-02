# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-824 — Source-level CDC transport configuration."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.api.data.subscribe import (
    _build_cdc_config,
    _resolve_provider_type,
    _CDC_DEBEZIUM_SOURCE_TYPES,
)
from provisa.core.config_loader import _validate_table_live_delivery
from provisa.core.models import Source, SourceCdcConfig, SourceType


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------


def _make_source_with_cdc(stype: str, **cdc_kwargs) -> Source:
    cdc = SourceCdcConfig(
        bootstrap_servers=cdc_kwargs.get("bootstrap_servers", "broker:9092"),
        topic_prefix=cdc_kwargs.get("topic_prefix", "dbserver1"),
        schema_registry_url=cdc_kwargs.get("schema_registry_url", None),
        consumer_group_id=cdc_kwargs.get("consumer_group_id", "provisa-debezium"),
    )
    return Source(id="s1", type=SourceType(stype), database="app", cdc=cdc)


def _make_config(source: Source, delivery: str = "cdc", watermark_column: str | None = None):
    live = SimpleNamespace(delivery=delivery, watermark_column=watermark_column)
    table = SimpleNamespace(table_name="orders", source_id=source.id, live=live)
    return SimpleNamespace(tables=[table], sources=[source])


# ---------------------------------------------------------------------------
# Scenario 1 — MySQL WITH cdc block → delivery=cdc passes, routes to Debezium
# ---------------------------------------------------------------------------


@given("a MySQL source with a source-level cdc block (bootstrap_servers, topic_prefix)")
def given_mysql_source_with_cdc_block(shared_data):
    source = _make_source_with_cdc("mysql", bootstrap_servers="broker:9092", topic_prefix="dbserver1")
    shared_data["source"] = source
    assert source.cdc is not None
    assert source.cdc.bootstrap_servers == "broker:9092"
    assert source.cdc.topic_prefix == "dbserver1"


@when("a table from that source sets live.delivery=cdc")
def when_table_sets_delivery_cdc(shared_data):
    source = shared_data["source"]
    config = _make_config(source, delivery="cdc")
    shared_data["config"] = config
    shared_data["delivery"] = "cdc"


@then(
    "validation passes and the runtime routes the subscription to the Debezium provider"
    " using the source's transport"
)
def then_validation_passes_and_routes_to_debezium(shared_data):
    # Validation must not raise
    config = shared_data["config"]
    _validate_table_live_delivery(config)

    # Provider dispatch must return "debezium" for a non-PG RDBMS source with a cdc block
    source: Source = shared_data["source"]
    state = SimpleNamespace(cdc_sources={source.id: source})
    provider = _resolve_provider_type(source.type.value, source.id, state)
    assert provider == "debezium", f"Expected 'debezium', got {provider!r}"

    # _build_cdc_config must read the transport from the source, not the table
    cdc_cfg = _build_cdc_config(source.id, state)
    assert cdc_cfg["bootstrap_servers"] == source.cdc.bootstrap_servers
    assert cdc_cfg["topic_prefix"] == source.cdc.topic_prefix


# ---------------------------------------------------------------------------
# Scenario 2 — MySQL WITHOUT cdc block → delivery=cdc is rejected
# ---------------------------------------------------------------------------


@given("a MySQL source WITHOUT a cdc block")
def given_mysql_source_without_cdc_block(shared_data):
    source = Source(id="s2", type=SourceType.mysql, database="app")
    shared_data["source"] = source
    assert source.cdc is None


@when("a table sets live.delivery=cdc")
def when_table_sets_cdc_no_block(shared_data):
    source = shared_data["source"]
    config = _make_config(source, delivery="cdc")
    shared_data["config"] = config


@then("config validation rejects it (no push mechanism)")
def then_validation_rejects_missing_cdc_block(shared_data):
    config = shared_data["config"]
    with pytest.raises(ValueError, match="cdc not supported"):
        _validate_table_live_delivery(config)


# ---------------------------------------------------------------------------
# Scenario 3 — Warehouse source (snowflake) → cdc block is rejected
# ---------------------------------------------------------------------------


@given("a warehouse source (e.g. snowflake)")
def given_warehouse_source(shared_data):
    # Build the source model; the cdc block is what we are testing
    source = _make_source_with_cdc("snowflake")
    shared_data["source"] = source
    assert source.type == SourceType.snowflake
    assert source.cdc is not None  # block exists — validation should reject it


@when("a cdc block is set on it")
def when_cdc_block_set_on_warehouse(shared_data):
    # State already captured in given; nothing additional to do — the cdc block
    # was placed on the source during construction.
    source = shared_data["source"]
    assert source.cdc is not None, "CDC block must be present for this scenario"


@then("validation rejects the cdc block as unsupported for that source type")
def then_validation_rejects_warehouse_cdc_block(shared_data):
    source = shared_data["source"]
    # Use a non-cdc delivery (poll) so the only rejection reason is the invalid cdc block
    config = _make_config(source, delivery="poll", watermark_column="updated_at")
    with pytest.raises(ValueError, match="cdc transport config not supported"):
        _validate_table_live_delivery(config)


# ---------------------------------------------------------------------------
# Additional invariant assertions (standalone unit tests, not scenario steps)
# ---------------------------------------------------------------------------


def test_cdc_debezium_source_types_does_not_contain_postgresql():
    """postgresql uses native LISTEN/NOTIFY — must never be routed to Debezium."""
    assert "postgresql" not in _CDC_DEBEZIUM_SOURCE_TYPES


def test_source_cdc_config_defaults():
    c = SourceCdcConfig(bootstrap_servers="b:9092", topic_prefix="pfx")
    assert c.schema_registry_url is None
    assert c.consumer_group_id == "provisa-debezium"


def test_source_cdc_field_defaults_none():
    s = Source(id="s99", type=SourceType.mysql)
    assert s.cdc is None


@pytest.mark.parametrize("stype", ["mysql", "oracle", "sqlserver", "mariadb"])
def test_non_pg_rdbms_without_cdc_block_rejects_delivery_cdc(stype):
    src = Source(id="s1", type=SourceType(stype))
    config = _make_config(src, delivery="cdc")
    with pytest.raises(ValueError, match="cdc not supported"):
        _validate_table_live_delivery(config)


def test_postgres_cdc_needs_no_transport_block():
    """postgresql supports delivery=cdc via LISTEN/NOTIFY without a cdc block."""
    src = Source(id="s1", type=SourceType.postgresql)
    config = _make_config(src, delivery="cdc")
    # Must not raise
    _validate_table_live_delivery(config)


def test_postgres_never_routes_to_debezium():
    src = _make_source_with_cdc("postgresql")
    state = SimpleNamespace(cdc_sources={src.id: src})
    provider = _resolve_provider_type("postgresql", src.id, state)
    assert provider == "postgresql"


def test_non_pg_rdbms_without_registered_cdc_returns_source_type():
    state = SimpleNamespace(cdc_sources={})
    provider = _resolve_provider_type("mysql", "s_unknown", state)
    assert provider == "mysql"


def test_build_cdc_config_reads_source_transport():
    src = _make_source_with_cdc(
        "mysql",
        bootstrap_servers="kafka:9092",
        topic_prefix="myprefix",
        schema_registry_url="http://registry:8081",
    )
    state = SimpleNamespace(cdc_sources={src.id: src})
    cfg = _build_cdc_config(src.id, state)
    assert cfg["bootstrap_servers"] == "kafka:9092"
    assert cfg["topic_prefix"] == "myprefix"
    assert cfg["schema_registry_url"] == "http://registry:8081"
