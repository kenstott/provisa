# Copyright (c) 2026 Kenneth Stott
# Canary: d4b2bfa1-6e3a-4f44-83b2-422de6f4471a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1861: a table that follows its MongoDB source's change feed.

- Its server must serve change streams: a standalone server is refused by name at save and at
  load; a server that cannot be reached is the source's ordinary unreachable failure.
- While the feed's listener is down, the table's replica status says since when and why; it
  clears once the listener is watching again.

The deployment check itself runs against real servers in
tests/integration/test_mongodb_change_feed_deployment.py; here it is stood in for."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.api.admin._replica_builds import build_view
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, registered_tables, sources
from provisa.core.schema_org import replica_state as replica_state_table
from provisa.federation import replica_state
from provisa.mongodb.change_feed import ChangeStreamsUnavailable, follows_change_feed

pytestmark = pytest.mark.unit

KEY = ("orders_db", "shop", "orders")
NOW = datetime(2026, 10, 3, 8, 0, tzinfo=UTC)


@pytest.fixture
async def db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state_table])
    return Database(engine, "test")


def _checks(monkeypatch, outcome):
    """Stand in for the server's answer: None passes, an exception is raised."""
    seen: list[str] = []

    async def _require(source):
        seen.append(source.id)
        if outcome is not None:
            raise outcome

    monkeypatch.setattr("provisa.mongodb.change_feed.require_change_feed", _require)
    monkeypatch.setattr("provisa.api.admin._change_feed.require_change_feed", _require)
    return seen


# -- which tables follow a change feed ---------------------------------------------------------


@pytest.mark.parametrize(
    ("source_type", "table_signal", "source_signal", "follows"),
    [
        ("mongodb", "native", None, True),
        ("mongodb", None, "native", True),
        ("mongodb", "poll", "native", False),  # the table's own signal wins
        ("mongodb", None, "poll", False),
        ("postgresql", "native", None, False),  # change streams are MongoDB's
    ],
)
def test_a_table_follows_the_change_feed_only_with_the_native_signal_on_mongodb(
    source_type, table_signal, source_signal, follows
):
    assert follows_change_feed(source_type, table_signal, source_signal) is follows


# -- the listener's state on the replica record ------------------------------------------------


async def test_a_down_listener_is_recorded_with_its_reason_and_cleared_when_it_watches(db):
    async with db.acquire() as conn:
        await replica_state.request_build(conn, KEY, replica_state.REASON_MODEL)
        await replica_state.record_feed(conn, KEY, error="connection refused", now=NOW)
        await replica_state.record_feed(
            conn, KEY, error="no route to host", now=NOW + timedelta(minutes=5)
        )
        down = await replica_state.read(conn, KEY)
        await replica_state.record_feed(conn, KEY, error=None, now=NOW + timedelta(minutes=9))
        watching = await replica_state.read(conn, KEY)
    assert down is not None and watching is not None
    # The time it went down is kept while it stays down; the reason is the latest.
    assert (down.feed_down_since, down.feed_error) == (NOW, "no route to host")
    assert (watching.feed_down_since, watching.feed_error) == (None, None)


async def test_the_replica_status_shows_the_down_listener(db):
    async with db.acquire() as conn:
        await replica_state.request_build(conn, KEY, replica_state.REASON_MODEL)
        await replica_state.record_feed(conn, KEY, error="connection refused", now=NOW)
        record = await replica_state.read(conn, KEY)
    view = build_view(record, NOW)
    assert view["feed_down_since"] == NOW.isoformat()
    assert view["feed_error"] == "connection refused"


# -- the load ------------------------------------------------------------------------------------


def _config(table_signal, source_signal=None, source_type="mongodb"):
    from provisa.core.models import SourceType

    source = SimpleNamespace(
        id="orders_db",
        type=SourceType(source_type),
        change_signal=source_signal,
        host="mongo.test",
        port=27017,
        username=None,
        password=None,
    )
    table = SimpleNamespace(source_id="orders_db", change_signal=table_signal)
    return SimpleNamespace(sources=[source], tables=[table])


async def test_the_load_refuses_a_standalone_server_by_name(monkeypatch):
    from provisa.core.config_loader import _check_change_feeds

    _checks(monkeypatch, ChangeStreamsUnavailable("orders_db"))
    with pytest.raises(ChangeStreamsUnavailable, match="replica set.*poll"):
        await _check_change_feeds(_config("native"))


async def test_the_load_reports_an_unreachable_server_as_that_sources_failure(monkeypatch):
    from provisa.core.config_loader import _check_change_feeds

    _checks(monkeypatch, ConnectionError("connection refused"))
    assert await _check_change_feeds(_config(None, source_signal="native")) == ["orders_db"]


async def test_the_load_checks_only_sources_with_a_table_on_the_change_feed(monkeypatch):
    from provisa.core.config_loader import _check_change_feeds

    seen = _checks(monkeypatch, None)
    assert await _check_change_feeds(_config("poll")) == []
    assert await _check_change_feeds(_config("native", source_type="postgresql")) == []
    assert seen == []
    assert await _check_change_feeds(_config("native")) == []
    assert seen == ["orders_db"]


# -- the save ------------------------------------------------------------------------------------


@pytest.fixture
async def plane(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'plane.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[sources, registered_tables])
        raw.execute(
            sources.insert().values(
                id="orders_db",
                type="mongodb",
                host="mongo.test",
                port=27017,
                database="shop",
                username="",
                password_ref="",
                change_signal="native",
            )
        )
    return Database(engine, "test")


async def test_saving_a_table_on_the_change_feed_refuses_a_standalone_server_by_name(
    plane, monkeypatch
):
    from provisa.api.admin._change_feed import table_change_feed_refusal

    _checks(monkeypatch, ChangeStreamsUnavailable("orders_db"))
    async with plane.acquire() as conn:
        refused = await table_change_feed_refusal(conn, "orders_db", None)
        polled = await table_change_feed_refusal(conn, "orders_db", "poll")
    assert refused is not None and refused.success is False
    assert refused.code == "schema.change_streams_unavailable"
    assert refused.params == {"source": "orders_db"}
    assert polled is None  # a polled table needs no change streams


async def test_saving_against_an_unreachable_server_is_the_ordinary_connection_failure(
    plane, monkeypatch
):
    from provisa.api.admin._change_feed import table_change_feed_refusal

    _checks(monkeypatch, ConnectionError("connection refused"))
    async with plane.acquire() as conn:
        refused = await table_change_feed_refusal(conn, "orders_db", "native")
    assert refused is not None
    assert refused.code == "schema.source_connection_failed"


async def test_saving_a_source_on_the_change_feed_refuses_a_standalone_server(plane, monkeypatch):
    from provisa.api.admin._change_feed import source_change_feed_refusal

    seen = _checks(monkeypatch, ChangeStreamsUnavailable("orders_db"))
    source = SimpleNamespace(
        id="orders_db",
        type="mongodb",
        change_signal="native",
        host="mongo.test",
        port=27017,
        username="",
        password="",
    )
    async with plane.acquire() as conn:
        refused = await source_change_feed_refusal(conn, source)
        polled = await source_change_feed_refusal(
            conn, SimpleNamespace(**{**vars(source), "change_signal": "poll"})
        )
    assert refused is not None and refused.code == "schema.change_streams_unavailable"
    assert polled is None
    assert seen == ["orders_db"]
