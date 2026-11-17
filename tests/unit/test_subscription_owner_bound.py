# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7d4b19-8c3e-4a61-b05d-e94a1c6f3b72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-812 (amended 2026-11-16): a client-started Kafka sink and an open subscription stream end
when their owner no longer holds the role they were started under, and one principal holds at most
the operator's cap of running sinks (``subscriptions.max_sinks_per_principal``, default 5)."""

# Requirements: REQ-812

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.responses import StreamingResponse

import pytest

from provisa.api.data.subscription_sse import handle_subscription_sse
from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity, RoleAssignment
from provisa.core import settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.platform_state import sinks as running
from provisa.core.schema_admin import running_sinks
from provisa.subscriptions import owner as owner_mod
from provisa.subscriptions.owner import OwnerRoleLost, bounded_by_owner

pytestmark = pytest.mark.unit


@pytest.fixture
def sink_db(tmp_path) -> Database:
    engine = create_engine_from_url(f"sqlite:///{tmp_path / 'ps.db'}", pool_size=5, max_overflow=0)
    running_sinks.create(engine)
    return Database(engine, name="platform-state", holds="platform_state")


def _identity(user_id: str = "alice") -> AuthIdentity:
    return AuthIdentity(
        user_id=user_id, email=None, display_name=user_id, roles=["analyst"], raw_claims={}
    )


class _Held:
    """The roles the control plane says the owner holds, changed by a test mid-run."""

    def __init__(self, *roles: str) -> None:
        self.roles = set(roles)

    async def __call__(self, state, identity, org_id):
        return [RoleAssignment(role_id=r, domain_id="*") for r in sorted(self.roles)]


class _Changes:
    """A change feed: one change now, then one each time ``more`` is set; never ends itself."""

    def __init__(self) -> None:
        self.closed = False

    async def watch(self, table):
        yield {"op": "insert"}
        while True:
            await asyncio.sleep(3600)
            yield {"op": "insert"}  # pragma: no cover - the test ends it first

    async def close(self):
        self.closed = True


def _document(field_name: str = "orders"):
    from graphql.language.ast import (
        DocumentNode,
        FieldNode,
        NameNode,
        OperationDefinitionNode,
        SelectionSetNode,
    )

    field_node = FieldNode(name=NameNode(value=field_name), selection_set=None)
    op = OperationDefinitionNode(
        operation=None,  # type: ignore[arg-type]
        name=None,
        variable_definitions=[],
        directives=[],
        selection_set=SelectionSetNode(selections=[field_node]),
    )
    return DocumentNode(definitions=[op])


def _state(sink_db: Database):
    table_meta = MagicMock()
    table_meta.table_name = "orders"
    table_meta.type_name = "Orders"
    table_meta.source_id = "pg1"
    table_meta.catalog_name = None
    table_meta.schema_name = None
    ctx = MagicMock()
    ctx.tables = {"orders": table_meta}
    ctx.joins = {}
    state = MagicMock()
    state.source_types = {"pg1": "postgresql"}
    state.tenant_db = None
    state.pg_notify_tables = set()
    state.table_watermarks = {}
    state.source_pools = {}
    state.platform_state_db = sink_db
    state.auth_config = {"assignments_source": "provisa", "default_role": "analyst"}
    return state, ctx


def _request(headers: dict[str, str], user_id: str = "alice"):
    req = MagicMock()
    req.headers = MagicMock()
    req.headers.get = lambda key, default="": headers.get(key.lower(), default)
    req.state = SimpleNamespace(identity=_identity(user_id), active_org_id="org1")

    async def _connected():
        return False

    req.is_disconnected = _connected
    return req


def _no_directives():
    directives = MagicMock()
    directives.sink_topic = None
    directives.sink_broker = None
    directives.watermark_column = None
    return directives


async def _subscribe(state, ctx, request):
    return await handle_subscription_sse(
        document=_document(),
        ctx=ctx,
        rls=MagicMock(),
        state=state,
        variables=None,
        role="analyst",
        role_id="analyst",
        raw_request=request,
        directives=_no_directives(),
    )


async def _query_result(*_args, **_kwargs):
    return {"data": {"orders": []}}


# --- the check, on any change feed -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_feed_with_no_changes_ends_when_the_role_is_lost():
    """The check runs while no change arrives, so an idle sink or stream ends too."""

    async def silent():
        await asyncio.sleep(3600)
        yield None  # pragma: no cover

    async def lost():
        raise OwnerRoleLost(("analyst",))

    with pytest.raises(OwnerRoleLost):
        async with asyncio.timeout(5):
            async for _ in bounded_by_owner(silent(), lost, every=0.05):
                pass


@pytest.mark.asyncio
async def test_changes_pass_while_the_role_is_held():
    async def three():
        for i in range(3):
            yield i

    checks = []

    async def held():
        checks.append(1)

    got = [i async for i in bounded_by_owner(three(), held, every=0.0)]
    assert got == [0, 1, 2]
    assert checks  # asked before changes were passed on


@pytest.mark.asyncio
async def test_holding_is_read_from_the_principals_assignments():
    held = _Held("analyst")
    owner = owner_mod.Owner(identity=_identity(), org_id="org1", roles=("analyst",))
    state = SimpleNamespace(auth_config={"assignments_source": "provisa"})
    with patch("provisa.auth.held_assignments.held_assignments", held):
        assert await owner_mod.holds(state, owner)
        held.roles.clear()
        assert not await owner_mod.holds(state, owner)


@pytest.mark.asyncio
async def test_a_meta_role_needs_every_member_held():
    held = _Held("analyst", "finance")
    owner = owner_mod.Owner(identity=_identity(), org_id="org1", roles=("analyst", "finance"))
    state = SimpleNamespace(auth_config={"assignments_source": "provisa"})
    with patch("provisa.auth.held_assignments.held_assignments", held):
        assert await owner_mod.holds(state, owner)
        held.roles.discard("finance")
        assert not await owner_mod.holds(state, owner)


# --- the cap ---------------------------------------------------------------------------------------


def test_the_cap_is_an_operator_setting_defaulting_to_five():
    s = settings_registry.setting(running.CAP_SETTING)
    assert s.default == 5
    assert s.effect == "live"


@pytest.mark.asyncio
async def test_the_sixth_sink_is_refused_by_name(sink_db):
    for i in range(5):
        await running.claim(
            sink_db,
            user_id="alice",
            org_id="org1",
            roles=("analyst",),
            topic=f"t{i}",
            table_name="orders",
            cap=5,
        )
    with pytest.raises(running.SinkCapReached) as refused:
        await running.claim(
            sink_db,
            user_id="alice",
            org_id="org1",
            roles=("analyst",),
            topic="t6",
            table_name="orders",
            cap=5,
        )
    assert refused.value.code == "subscribe.sink_cap_reached"
    assert running.CAP_SETTING in str(refused.value)
    # Another principal has places of its own.
    await running.claim(
        sink_db,
        user_id="bob",
        org_id="org1",
        roles=("analyst",),
        topic="t",
        table_name="orders",
        cap=5,
    )


@pytest.mark.asyncio
async def test_a_sink_that_stopped_beating_frees_its_place(sink_db):
    ids = [
        await running.claim(
            sink_db,
            user_id="alice",
            org_id="org1",
            roles=("analyst",),
            topic=f"t{i}",
            table_name="orders",
            cap=5,
        )
        for i in range(5)
    ]
    stale = datetime.now(UTC) - running.STALE_AFTER - timedelta(seconds=1)
    async with sink_db.acquire() as conn:
        await conn.execute_core(
            running_sinks.update().where(running_sinks.c.sink_id == ids[0]).values(last_seen=stale)
        )
    await running.claim(
        sink_db,
        user_id="alice",
        org_id="org1",
        roles=("analyst",),
        topic="t5",
        table_name="orders",
        cap=5,
    )
    assert await running.held_by(sink_db, "alice") == 5


@pytest.mark.asyncio
async def test_a_request_past_the_cap_is_refused_with_its_code(sink_db, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092")
    monkeypatch.setenv("PROVISA_MAX_SINKS_PER_PRINCIPAL", "5")
    state, ctx = _state(sink_db)
    request = _request({"x-provisa-sink": "kafka://broker1:9092/orders-topic"})

    def _spawn(coro, *, name=None):
        coro.close()

    with patch("provisa.core.connection_loop.spawn_long_lived", _spawn):
        for _ in range(5):
            response = await _subscribe(state, ctx, request)
            assert response.status_code == 202
        with pytest.raises(ApiError) as refused:
            await _subscribe(state, ctx, request)
    assert refused.value.status_code == 429
    assert refused.value.code == "subscribe.sink_cap_reached"
    assert refused.value.params["cap"] == 5


# --- close on revoke -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoking_the_role_ends_an_open_sink_and_frees_its_place(sink_db, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092")
    monkeypatch.setattr(running, "HEARTBEAT", timedelta(seconds=0.05))
    state, ctx = _state(sink_db)
    request = _request({"x-provisa-sink": "kafka://broker1:9092/orders-topic"})
    held = _Held("analyst")
    feed = _Changes()
    spawned: list = []
    producer = MagicMock()

    async def _publish(*_a, **_k):
        return None

    producer.publish_rows = _publish

    with (
        patch(
            "provisa.core.connection_loop.spawn_long_lived",
            lambda c, *, name=None: spawned.append(c),
        ),
        patch("provisa.subscriptions.registry.get_provider", lambda *_a, **_k: feed),
        patch("provisa.kafka.sink.KafkaProducer", lambda **_k: producer),
        patch("provisa.api.data.endpoint._handle_query", _query_result),
        patch("provisa.compiler.parser.parse_query", MagicMock()),
        patch("provisa.auth.held_assignments.held_assignments", held),
    ):
        response = await _subscribe(state, ctx, request)
        assert response.status_code == 202
        assert await running.held_by(sink_db, "alice") == 1
        sink = asyncio.ensure_future(spawned[0])
        await asyncio.sleep(0.2)
        assert not sink.done()  # held: it runs
        held.roles.clear()  # the role is revoked in the control plane
        async with asyncio.timeout(5):
            await sink
    assert feed.closed
    producer.close.assert_called_once()
    assert await running.held_by(sink_db, "alice") == 0


@pytest.mark.asyncio
async def test_revoking_the_role_ends_an_open_stream_with_a_last_frame(sink_db, monkeypatch):
    monkeypatch.setattr(owner_mod, "RECHECK", 0.05)
    state, ctx = _state(sink_db)
    request = _request({})
    held = _Held("analyst")
    feed = _Changes()

    with (
        patch("provisa.subscriptions.registry.get_provider", lambda *_a, **_k: feed),
        patch("provisa.api.data.endpoint._handle_query", _query_result),
        patch("provisa.compiler.parser.parse_query", MagicMock()),
        patch("provisa.auth.held_assignments.held_assignments", held),
        patch("provisa.api.request_timeout.outlive_deadline", lambda: None),
    ):
        response = await _subscribe(state, ctx, request)
        assert isinstance(response, StreamingResponse)
        frames = response.body_iterator.__aiter__()
        first = str(await frames.__anext__())
        assert json.loads(first.removeprefix("data: "))["data"] == {"orders": []}
        second = str(await frames.__anext__())  # the one change, while the role is held
        assert "orders" in second
        held.roles.clear()
        async with asyncio.timeout(5):
            last = str(await frames.__anext__())
            with pytest.raises(StopAsyncIteration):
                await frames.__anext__()
    error = json.loads(last.removeprefix("data: "))["errors"][0]
    assert error["extensions"]["code"] == "subscribe.owner_role_lost"
    assert error["extensions"]["params"] == {"roles": "analyst"}
    assert feed.closed


# --- the refusals and the setting are told in every locale -----------------------------------------


def test_the_refusals_and_the_setting_are_catalogued_in_every_locale():
    from pathlib import Path

    locales = Path(__file__).resolve().parents[2] / "provisa-ui/src/i18n/locales"
    languages = sorted(p for p in locales.iterdir() if p.is_dir())
    assert len(languages) == 13
    for lang in languages:
        refusals = json.loads((lang / "serverErrors.json").read_text(encoding="utf-8"))
        subscribe = refusals["serverErrors"]["subscribe"]
        assert "{{roles}}" in subscribe["owner_role_lost"], lang
        assert "{{cap}}" in subscribe["sink_cap_reached"], lang
        assert "{{setting}}" in subscribe["sink_cap_reached"], lang
        settings = json.loads((lang / "settings.json").read_text(encoding="utf-8"))
        field = settings["settings"]["subscriptions"]["max_sinks_per_principal"]
        assert field["label"] and field["help"], lang
