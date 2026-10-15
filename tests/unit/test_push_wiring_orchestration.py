# Copyright (c) 2026 Kenneth Stott
# Canary: dc9ae060-9dc0-4641-a4eb-b5e49bd046f5
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for wire_push_listeners's orchestration (REQ-1733): which tables get a listener,
idempotent re-wire, and clean shutdown — with every collaborator mocked (no real DB, no real
provider connection; consume_cdc_into_store itself is separately tested in
test_cdc_landing_debounce.py)."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.models import Source, SourceType
from provisa.events.push_wiring import shutdown_push_listeners, wire_push_listeners
from provisa.federation.runtime import EngineRuntime


def _kafka_table(table_name: str = "orders", with_pk: bool = True) -> dict:
    return {
        "id": 1,
        "source_id": "kafka_src",
        "schema_name": "s",
        "table_name": table_name,
        "live": {"kafka": {"topic": "orders-topic"}},
        "change_signal": None,
        "cache_ttl": None,
        "push_debounce_quiet": 0.5,
        "push_debounce_max_delay": 5.0,
        "columns": [
            {
                "column_name": "id",
                "data_type": "integer",
                "is_primary_key": with_pk,
                "native_filter_type": None,
            },
            {
                "column_name": "amount",
                "data_type": "double",
                "is_primary_key": False,
                "native_filter_type": None,
            },
        ],
    }


def _key(node: str) -> tuple:
    """A listener's key: the org and environment it lands for (the bound ones), and its node."""
    from provisa.events.push_wiring import _listener_key

    return _listener_key(node)


class _FakeState:
    def __init__(self) -> None:
        self.push_listener_disconnects: dict = {}
        self.push_listener_tasks: list = []
        self.push_listener_handles: dict = {}
        self.tenant_db = MagicMock()
        self.model_db = self.tenant_db
        self.tenant_db.acquire = MagicMock(
            return_value=MagicMock(
                __aenter__=AsyncMock(return_value=MagicMock()),
                __aexit__=AsyncMock(return_value=False),
            )
        )
        # spec=EngineRuntime (REQ-1733/REQ-989 postmortem): a bare MagicMock() silently
        # auto-creates ANY attribute accessed on it, including ones the real EngineRuntime class
        # does not have (e.g. a stray `.materialize_store()`/`.backend` this fake used to carry) —
        # which is exactly how wire_push_listeners shipped calling two nonexistent attributes on
        # the real object and every push listener silently never started. Spec'ing to the real
        # class makes an attribute typo/API drift fail the test instead of masking it.
        self.federation_engine = MagicMock(spec=EngineRuntime)
        # EngineRuntime.engine is an instance attribute (set in __init__), invisible to a class
        # spec: the bound FederationEngine, here one whose store is not SingleStore (REQ-990).
        self.federation_engine.engine = MagicMock(native_store=None)
        self.federation_engine.materialize_store_dsn = MagicMock(return_value="duckdb:///x")
        self.federation_engine.landing_target = MagicMock(return_value=("mat", "orders__x"))


@pytest.mark.asyncio
async def test_wires_a_listener_for_a_valid_kafka_table():
    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker:9092")

    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[_kafka_table()])
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        patch("provisa.subscriptions.registry.get_provider", return_value=MagicMock()),
        patch(
            "provisa.events.push_wiring._run_listener", AsyncMock()
        ),  # never actually connect anywhere
    ):
        tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))

    assert len(tasks) == 1
    assert _key("kafka_src/s.orders") in state.push_listener_disconnects  # the table's node
    await shutdown_push_listeners(state)
    for t in tasks:
        assert t.done()


@pytest.mark.asyncio
async def test_reentrant_wire_skips_already_running_node():
    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker:9092")

    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[_kafka_table()])
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        patch("provisa.subscriptions.registry.get_provider", return_value=MagicMock()),
        patch("provisa.events.push_wiring._run_listener", AsyncMock()),
    ):
        first = await wire_push_listeners(state=state, log=logging.getLogger("test"))
        second = await wire_push_listeners(state=state, log=logging.getLogger("test"))

    assert len(first) == 1
    assert len(second) == 0  # already wired — no duplicate listener for the same node
    await shutdown_push_listeners(state)


@pytest.mark.asyncio
async def test_table_with_no_primary_key_is_skipped(caplog):
    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker:9092")

    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables",
            AsyncMock(return_value=[_kafka_table(with_pk=False)]),
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        caplog.at_level(logging.WARNING),
    ):
        tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))

    assert tasks == []
    assert "no primary key" in caplog.text


@pytest.mark.asyncio
async def test_non_push_source_types_are_ignored():
    state = _FakeState()
    src = Source(id="pg_src", type=SourceType.postgresql, host="db", port=5432)
    tbl = _kafka_table()
    tbl["source_id"] = "pg_src"

    with (
        patch("provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[tbl])),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
    ):
        tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))

    assert tasks == []


@pytest.mark.asyncio
async def test_no_materialize_store_configured_returns_empty():
    from provisa.federation.engine import MaterializeStoreUnconfigured

    state = _FakeState()
    state.federation_engine.materialize_store_dsn = MagicMock(
        side_effect=MaterializeStoreUnconfigured("duckdb")
    )
    tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))
    assert tasks == []


@pytest.mark.asyncio
async def test_a_listener_whose_table_goes_draft_is_stopped_on_the_next_wire():
    """REQ-1921: a drafted (or deleted) table leaves fetch_tables, and the rebuild's re-wire stops
    the listener that was landing it; a table that stays keeps its listener."""
    import asyncio

    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker:9092")

    async def _runs_until_stopped(**kwargs):
        await asyncio.sleep(3600)

    tables = [_kafka_table("orders"), _kafka_table("refunds")]
    fetch = AsyncMock(side_effect=[tables, tables[1:]])  # "orders" goes draft before the 2nd wire
    with (
        patch("provisa.api.admin.db_queries.fetch_tables", fetch),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        patch("provisa.subscriptions.registry.get_provider", return_value=MagicMock()),
        patch("provisa.events.push_wiring._run_listener", _runs_until_stopped),
    ):
        started = await wire_push_listeners(state=state, log=logging.getLogger("test"))
        orders = state.push_listener_handles[_key("kafka_src/s.orders")]
        refunds = state.push_listener_handles[_key("kafka_src/s.refunds")]
        orders_disconnect = state.push_listener_disconnects[_key("kafka_src/s.orders")]

        again = await wire_push_listeners(state=state, log=logging.getLogger("test"))

    assert len(started) == 2 and again == []
    assert orders.done() and orders_disconnect.is_set()
    assert _key("kafka_src/s.orders") not in state.push_listener_disconnects
    assert _key("kafka_src/s.orders") not in state.push_listener_handles
    assert orders not in state.push_listener_tasks
    assert not refunds.done() and _key("kafka_src/s.refunds") in state.push_listener_disconnects
    await shutdown_push_listeners(state)
    assert refunds.done()


def _captured_table(signal: str | None) -> dict:
    """A table of a PostgreSQL source whose changes Debezium captures (REQ-261, REQ-824)."""
    table = _kafka_table("orders")
    table.update(source_id="pg_src", schema_name="sales", live=None, change_signal=signal)
    return table


def _captured_source(signal: str = "ttl") -> Source:
    from provisa.core.models import SourceCdcConfig

    return Source(
        id="pg_src",
        type=SourceType.postgresql,
        host="db",
        port=5432,
        database="shop",
        username="reader",
        change_signal=signal,
        cdc=SourceCdcConfig(bootstrap_servers="broker:9092", topic_prefix="shopdb"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("table_signal", "source_signal"), [("debezium", "ttl"), (None, "debezium")]
)
async def test_a_debezium_captured_table_lands_its_topic_into_its_replica(
    table_signal, source_signal
):
    """A table whose signal is debezium, its own or its source's, gets a listener: the source's
    CDC transport, the topic its connector writes the table to, landed by key into the replica."""
    state = _FakeState()
    state.config = MagicMock(cdc_consumer_group_id="provisa-debezium")
    run = AsyncMock()
    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables",
            AsyncMock(return_value=[_captured_table(table_signal)]),
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_captured_source(source_signal)]),
        ),
        patch("provisa.events.push_wiring._run_listener", run),
    ):
        tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))
        await shutdown_push_listeners(state)

    assert len(tasks) == 1
    kwargs = run.call_args.kwargs
    provider = kwargs["provider"]
    assert provider._build_topic(kwargs["watch_target"]) == "shopdb.sales.orders"
    # The operator's group, made the bound org's own: each org reads the whole topic.
    org, _env, _ = _key("")
    assert provider._consumer_group_id == f"provisa-debezium-{org}"
    assert kwargs["pk_columns"] == ["id"]


@pytest.mark.asyncio
async def test_a_postgresql_table_on_a_polling_signal_gets_no_listener():
    state = _FakeState()
    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables",
            AsyncMock(return_value=[_captured_table("ttl")]),
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_captured_source()]),
        ),
        patch("provisa.events.push_wiring._run_listener", AsyncMock()),
    ):
        tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))
    assert tasks == [] and state.push_listener_disconnects == {}


@pytest.mark.asyncio
async def test_a_kafka_signal_table_of_a_database_lands_its_delta_topic():
    """A database table on the kafka signal reads the delta topic it names, over the source's
    CDC transport, landed by key into its replica like a Debezium-captured one."""
    state = _FakeState()
    state.config = MagicMock(cdc_consumer_group_id="provisa-debezium")
    table = _captured_table("kafka")
    table["live"] = {"kafka": {"topic": "orders.deltas"}}
    run = AsyncMock()
    built: dict = {}

    def _get_provider(kind, config):
        built.update(kind=kind, **config)
        return MagicMock()

    with (
        patch("provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[table])),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[_captured_source()]),
        ),
        patch("provisa.subscriptions.registry.get_provider", _get_provider),
        patch("provisa.events.push_wiring._run_listener", run),
        patch("provisa.events.push_wiring._drop_slot_of", lambda src: False),
    ):
        tasks = await wire_push_listeners(state=state, log=logging.getLogger("test"))
        await shutdown_push_listeners(state)

    assert len(tasks) == 1
    assert run.call_args.kwargs["watch_target"] == "orders.deltas"
    assert built == {
        "kind": "kafka",
        "bootstrap_servers": "broker:9092",
        "group_id": f"provisa-debezium-{_key('')[0]}",  # the operator's group, the org's own
        "registry": None,  # the source's CDC block names no schema registry
        "field_mapping": {},
    }


@pytest.mark.unbound
def test_a_cdc_consumer_group_is_each_org_s_and_each_environment_s():
    """Two orgs (or an org's environment) landing one CDC topic each read every message: the
    operator's configured group is the base, and each reads under its own."""
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.events.push_wiring import _consumer_group

    def _group(org: str, env: str | None) -> str:
        org_token, env_token = set_current_org(org), set_current_env(env)
        try:
            return _consumer_group("cdc-readers")
        finally:
            reset_current_env(env_token)
            reset_current_org(org_token)

    assert _group("acme", None) == "cdc-readers-acme"
    assert _group("globex", None) == "cdc-readers-globex"
    assert _group("acme", "dev") == "cdc-readers-acme-dev"
