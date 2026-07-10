# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-824 — Source-level CDC transport configuration."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, scenarios, then, when

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


def _tbl_meta(strategy: str | None = "debezium"):
    return SimpleNamespace(live=SimpleNamespace(strategy=strategy))


def _make_config(
    source: Source,
    strategy: str = "debezium",
    watermark_column: str | None = None,
    kafka=None,
):
    live = SimpleNamespace(strategy=strategy, watermark_column=watermark_column, kafka=kafka)
    table = SimpleNamespace(table_name="orders", source_id=source.id, live=live)
    return SimpleNamespace(tables=[table], sources=[source])


# ---------------------------------------------------------------------------
# Scenario 1 — MySQL WITH cdc block → strategy=debezium passes, routes to Debezium
# ---------------------------------------------------------------------------


@given("a MySQL source with a source-level cdc block (bootstrap_servers, topic_prefix)")
def given_mysql_source_with_cdc_block(shared_data):
    source = _make_source_with_cdc(
        "mysql", bootstrap_servers="broker:9092", topic_prefix="dbserver1"
    )
    shared_data["source"] = source
    assert source.cdc is not None
    assert source.cdc.bootstrap_servers == "broker:9092"
    assert source.cdc.topic_prefix == "dbserver1"


@when("a table from that source sets live.delivery=cdc")
def when_table_sets_delivery_cdc(shared_data):
    source = shared_data["source"]
    config = _make_config(source, strategy="debezium")
    shared_data["config"] = config
    shared_data["strategy"] = "debezium"


@then(
    "validation passes and the runtime routes the subscription to the Debezium provider"
    " using the source's transport"
)
def then_validation_passes_and_routes_to_debezium(shared_data):
    # Validation must not raise
    config = shared_data["config"]
    _validate_table_live_delivery(config)

    # Provider dispatch must return "debezium" for a strategy=debezium table
    source: Source = shared_data["source"]
    assert source.cdc is not None
    # REQ-931: legacy strategy resolution reads state.config.sources for the source's change_signal.
    state = SimpleNamespace(
        cdc_sources={source.id: source}, config=SimpleNamespace(sources=[source])
    )
    provider = _resolve_provider_type(source.type.value, source.id, _tbl_meta("debezium"), state)
    assert provider == "debezium", f"Expected 'debezium', got {provider!r}"

    # _build_cdc_config must read the transport from the source, not the table
    cdc_cfg = _build_cdc_config(state, source.id)
    assert cdc_cfg["bootstrap_servers"] == source.cdc.bootstrap_servers
    assert cdc_cfg["topic_prefix"] == source.cdc.topic_prefix


# ---------------------------------------------------------------------------
# Scenario 2 — MySQL WITHOUT cdc block → strategy=debezium is rejected
# ---------------------------------------------------------------------------


@given("a MySQL source WITHOUT a cdc block")
def given_mysql_source_without_cdc_block(shared_data):
    source = Source(id="s2", type=SourceType.mysql, database="app")
    shared_data["source"] = source
    assert source.cdc is None


@when("a table sets live.delivery=cdc")
def when_table_sets_cdc_no_block(shared_data):
    source = shared_data["source"]
    config = _make_config(source, strategy="debezium")
    shared_data["config"] = config


@then("config validation rejects it (no push mechanism)")
def then_validation_rejects_missing_cdc_block(shared_data):
    config = shared_data["config"]
    with pytest.raises(ValueError, match="requires source-level cdc transport"):
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
    # Use strategy=poll so the only rejection reason is the invalid cdc block
    config = _make_config(source, strategy="poll", watermark_column="updated_at")
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
    # REQ-931: consumer group identity is unified at the global level
    # (ProvisaConfig.cdc_consumer_group_id="provisa-debezium"); a per-source
    # consumer_group_id defaults to None and is set only for deliberate offset isolation.
    assert c.consumer_group_id is None


def test_source_cdc_field_defaults_none():
    s = Source(id="s99", type=SourceType.mysql)
    assert s.cdc is None


@pytest.mark.parametrize("stype", ["mysql", "oracle", "sqlserver", "mariadb"])
def test_non_pg_rdbms_without_cdc_block_rejects_debezium(stype):
    src = Source(id="s1", type=SourceType(stype))
    config = _make_config(src, strategy="debezium")
    with pytest.raises(ValueError, match="requires source-level cdc transport"):
        _validate_table_live_delivery(config)


def test_postgres_native_needs_no_transport_block():
    """postgresql supports strategy=native via LISTEN/NOTIFY without a cdc block."""
    src = Source(id="s1", type=SourceType.postgresql)
    config = _make_config(src, strategy="native")
    # Must not raise
    _validate_table_live_delivery(config)


def test_postgres_no_strategy_never_routes_to_debezium():
    src = _make_source_with_cdc("postgresql")
    state = SimpleNamespace(cdc_sources={src.id: src})
    provider = _resolve_provider_type("postgresql", src.id, _tbl_meta(None), state)
    assert provider == "postgresql"


def test_non_pg_rdbms_without_registered_cdc_returns_source_type():
    state = SimpleNamespace(cdc_sources={})
    provider = _resolve_provider_type("mysql", "s_unknown", _tbl_meta(None), state)
    assert provider == "mysql"


def test_build_cdc_config_reads_source_transport():
    src = _make_source_with_cdc(
        "mysql",
        bootstrap_servers="kafka:9092",
        topic_prefix="myprefix",
        schema_registry_url="http://registry:8081",
    )
    state = SimpleNamespace(cdc_sources={src.id: src})
    cfg = _build_cdc_config(state, src.id)
    assert cfg["bootstrap_servers"] == "kafka:9092"
    assert cfg["topic_prefix"] == "myprefix"
    assert cfg["schema_registry_url"] == "http://registry:8081"


def test_source_cdc_config_full_round_trip():
    """All four transport fields survive model construction and attribute access."""
    cdc = SourceCdcConfig(
        bootstrap_servers="kafka-broker:9092",
        topic_prefix="mydb",
        schema_registry_url="http://schema-registry:8081",
        consumer_group_id="my-consumer-group",
    )
    assert cdc.bootstrap_servers == "kafka-broker:9092"
    assert cdc.topic_prefix == "mydb"
    assert cdc.schema_registry_url == "http://schema-registry:8081"
    assert cdc.consumer_group_id == "my-consumer-group"


def test_source_cdc_attached_to_source_model():
    """Source.cdc field carries the SourceCdcConfig and persists all transport values."""
    cdc = SourceCdcConfig(
        bootstrap_servers="b:9092",
        topic_prefix="prefix1",
        schema_registry_url="http://sr:8081",
        consumer_group_id="grp",
    )
    src = Source(id="src-mysql", type=SourceType.mysql, database="mydb", cdc=cdc)
    assert src.cdc is not None
    assert src.cdc.bootstrap_servers == "b:9092"
    assert src.cdc.topic_prefix == "prefix1"
    assert src.cdc.schema_registry_url == "http://sr:8081"
    assert src.cdc.consumer_group_id == "grp"


@pytest.mark.parametrize("stype", ["mysql", "mariadb", "sqlserver", "oracle"])
def test_debezium_capable_sources_route_to_debezium_via_strategy(stype):
    """Every Debezium-capable RDBMS type routes to 'debezium' when strategy=debezium."""
    src = _make_source_with_cdc(stype)
    # REQ-931: legacy strategy resolution reads state.config.sources for the source's change_signal.
    state = SimpleNamespace(cdc_sources={src.id: src}, config=SimpleNamespace(sources=[src]))
    provider = _resolve_provider_type(src.type.value, src.id, _tbl_meta("debezium"), state)
    assert provider == "debezium", (
        f"Expected 'debezium' for {stype} with strategy=debezium, got {provider!r}"
    )


@pytest.mark.parametrize("stype", ["mysql", "mariadb", "sqlserver", "oracle"])
def test_debezium_capable_sources_with_cdc_block_pass_validation(stype):
    """validation accepts strategy=debezium for Debezium-capable sources with a cdc block."""
    src = _make_source_with_cdc(stype)
    config = _make_config(src, strategy="debezium")
    # Must not raise
    _validate_table_live_delivery(config)


@pytest.mark.parametrize(
    "warehouse_type",
    ["snowflake", "bigquery", "redshift", "databricks"],
)
def test_warehouse_sources_reject_cdc_block(warehouse_type):
    """Warehouse sources must never accept a cdc transport block."""
    src = _make_source_with_cdc(warehouse_type)
    config = _make_config(src, strategy="poll", watermark_column="updated_at")
    with pytest.raises(ValueError, match="cdc transport config not supported"):
        _validate_table_live_delivery(config)


def test_build_cdc_config_raises_for_unregistered_source():
    """_build_cdc_config fails loud when source not in cdc_sources."""
    state = SimpleNamespace(cdc_sources={})
    with pytest.raises(ValueError, match="no source-level"):
        _build_cdc_config(state, "nonexistent-source")


def test_cdc_block_not_duplicated_per_table():
    """Transport details live on the source; per-table config should not need to repeat them.

    This test verifies that a config with two tables sharing the same source's cdc block
    validates correctly without any per-table transport fields.
    """
    src = _make_source_with_cdc("mysql")
    live1 = SimpleNamespace(strategy="debezium", watermark_column=None, kafka=None)
    live2 = SimpleNamespace(strategy="debezium", watermark_column=None, kafka=None)
    table1 = SimpleNamespace(table_name="orders", source_id=src.id, live=live1)
    table2 = SimpleNamespace(table_name="customers", source_id=src.id, live=live2)
    config = SimpleNamespace(tables=[table1, table2], sources=[src])
    # Both tables share the single source-level cdc block — validation must pass
    _validate_table_live_delivery(config)


def test_polling_config_independent_of_cdc_block():
    """Poll strategy (watermark_column, poll_interval) is per-table and independent of cdc block."""
    src = _make_source_with_cdc("mysql")
    # Even with a cdc block on the source, a table may still use poll strategy
    config = _make_config(src, strategy="poll", watermark_column="updated_at")
    # Must not raise — poll is always valid for RDBMS sources
    _validate_table_live_delivery(config)


scenarios("../features/REQ-824.feature")
