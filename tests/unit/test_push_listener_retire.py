# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A push listener lives as long as the runtime it lands for (REQ-1733).

What failed before: a sandbox org's runtime was retired while its orders listener ran. The
listener's next landing met RuntimeNotBuilt and the listener died with a traceback, and its key,
left in ``push_listener_disconnects``, kept every later build of the org from wiring it again, so
nothing of the topic ever landed for that org again."""

# Requirements: REQ-1733

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.subscriptions.base import ChangeEvent

_NODE = "kafka_src/s.orders"


async def _wire(state, org: str) -> list:
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.events.push_wiring import wire_push_listeners

    token = set_current_org(org)
    try:
        return await wire_push_listeners(state=state, log=logging.getLogger("test"))
    finally:
        reset_current_org(token)


@pytest.mark.unbound
async def test_a_retired_runtime_s_listeners_stop_and_its_next_build_wires_them_again():
    import provisa.api.app as appmod
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime
    from provisa.core.models import Source, SourceType
    from provisa.events.push_wiring import shutdown_push_listeners
    from tests.unit.test_push_wiring_orchestration import _FakeState, _kafka_table

    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker", port=9092)

    async def _runs_until_stopped(**kwargs):
        await asyncio.sleep(3600)

    registry = OrgRegistry()
    registry.set("acme", OrgRuntime(org_id="acme"))
    registry.set("globex", OrgRuntime(org_id="globex"))
    with (
        patch.object(appmod, "state", state),
        patch(
            "provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[_kafka_table()])
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        patch("provisa.subscriptions.registry.get_provider", lambda kind, config: MagicMock()),
        patch("provisa.events.push_wiring._run_listener", _runs_until_stopped),
    ):
        assert len(await _wire(state, "acme")) == 1
        assert len(await _wire(state, "globex")) == 1
        acme = state.push_listener_handles[("acme", None, _NODE)]
        acme_stop = state.push_listener_disconnects[("acme", None, _NODE)]

        registry.invalidate("acme")  # the org's runtime is retired

        # Its listener is stopped and forgotten; the other org's runs on.
        assert acme_stop.is_set()
        assert await acme.wait(5)
        assert acme not in state.push_listener_tasks
        assert set(state.push_listener_disconnects) == {("globex", None, _NODE)}
        assert not state.push_listener_handles[("globex", None, _NODE)].done()

        # The org's next build wires its listener again.
        registry.set("acme", OrgRuntime(org_id="acme"))
        assert len(await _wire(state, "acme")) == 1
        assert ("acme", None, _NODE) in state.push_listener_disconnects
        assert not state.push_listener_handles[("acme", None, _NODE)].done()
    await shutdown_push_listeners(state)


@pytest.mark.unbound
async def test_a_replaced_runtime_s_listeners_stop_too():
    """A rebuild replaces the runtime: the replaced runtime's listener lands into the replaced
    engine, so it stops, and the rebuild's own wiring starts the new one."""
    import provisa.api.app as appmod
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    stopped = threading.Event()
    handle = MagicMock()
    state = SimpleNamespace(
        push_listener_disconnects={("acme", None, _NODE): stopped},
        push_listener_handles={("acme", None, _NODE): handle},
        push_listener_tasks=[handle],
    )
    registry = OrgRegistry()
    registry.set("acme", OrgRuntime(org_id="acme"))
    with patch.object(appmod, "state", state):
        registry.set("acme", OrgRuntime(org_id="acme"))
    assert stopped.is_set()
    handle.cancel.assert_called_once()
    assert state.push_listener_disconnects == {}
    assert state.push_listener_handles == {}
    assert state.push_listener_tasks == []


def test_only_the_retired_runtime_s_environment_is_stopped():
    from provisa.events.push_wiring import stop_runtime_listeners

    keys = [("acme", None, _NODE), ("acme", "dev", _NODE), ("globex", None, _NODE)]
    state = SimpleNamespace(
        push_listener_disconnects={key: threading.Event() for key in keys},
        push_listener_handles={key: MagicMock() for key in keys},
        push_listener_tasks=[],
    )
    stop_runtime_listeners(state, "acme", "prod")
    assert set(state.push_listener_disconnects) == {("acme", "dev", _NODE), ("globex", None, _NODE)}
    stop_runtime_listeners(state, "acme", "dev")
    assert set(state.push_listener_disconnects) == {("globex", None, _NODE)}


async def test_a_landing_after_its_runtime_is_retired_ends_the_listener_quietly(caplog):
    from provisa.core.runtime_gone import RuntimeNotBuilt
    from provisa.events.push_wiring import _run_listener

    class _Engine:
        async def apply_cdc_events(self, **kwargs):
            raise RuntimeNotBuilt("acme", None, "no runtime built for org 'acme'")

    class _Provider:
        acked: list = []
        closed = False

        async def watch(self, target):
            yield ChangeEvent(
                operation="insert",
                table="orders",
                row={"id": "o-1"},
                timestamp=datetime.now(timezone.utc),
                ack_token=("orders", 0, 0),
            )
            await asyncio.sleep(3600)

        async def ack(self, events):
            self.acked.extend(e.ack_token for e in events)

        async def close(self):
            self.closed = True

    provider = _Provider()
    caplog.set_level(logging.ERROR)
    await asyncio.wait_for(
        _run_listener(
            state=SimpleNamespace(),
            engine=_Engine(),
            provider=provider,
            watch_target="orders",
            land_schema="s",
            land_table="orders",
            columns=[("id", "varchar")],
            pk_columns=["id"],
            disconnect=threading.Event(),
            debounce_quiet=0.0,
            debounce_max_delay=5.0,
            node=_NODE,
            log=logging.getLogger("test"),
            key=("kafka_src", "s", "orders"),
        ),
        timeout=5,
    )
    # Not landed, so not acknowledged: the next build's listener reads it again.
    assert provider.acked == []
    assert provider.closed
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
