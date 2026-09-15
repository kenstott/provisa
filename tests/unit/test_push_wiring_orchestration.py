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


class _FakeState:
    def __init__(self) -> None:
        self.push_listener_disconnects: dict = {}
        self.push_listener_tasks: list = []
        self.tenant_db = MagicMock()
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
    assert "s.orders" in state.push_listener_disconnects
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
