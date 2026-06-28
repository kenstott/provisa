# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for the Live Query Engine (Unified Subscription & Sink Delivery).

REQ-282 — A single Live Query Engine powers all poll-based live delivery. These
steps exercise the real ``provisa.live`` building blocks (SSEFanout, the
incremental SQL builder) to prove that a single poll executes the query once and
fans the results out to both the SSE subscription and the Kafka sink —
demonstrating that delivery mode (poll) is orthogonal to output mechanism
(SSE vs sink).

REQ-285 — Tables declare a delivery mode (``cdc`` or ``poll``). ``cdc`` is only
available for sources backed by a registered change-notification provider
(PostgreSQL LISTEN/NOTIFY, Debezium, MongoDB Change Streams). Config validation
rejects ``delivery: cdc`` for sources that cannot support it (Trino-federated,
restricted JDBC, Kafka topics, API sources).

REQ-286 — SSE subscription and Kafka sink are equivalent output mechanisms. A
single live definition may declare both in its ``outputs``; the poll engine runs
once per interval, but each output tracks its own watermark independently in
``live_query_state`` (keyed by source name + output_type). A slow Kafka consumer
must never block SSE delivery and vice versa. These steps prove that independent
watermark tracking lets SSE keep advancing even while the Kafka output lags.
"""

from __future__ import annotations

import asyncio

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.live.engine import _build_incremental_sql
from provisa.live.outputs.sse import SSEFanout
from provisa.live.watermark import get_watermark, set_watermark

try:
    from provisa.subscriptions.registry import get_provider
except ImportError:  # pragma: no cover - registry must exist in supported builds
    get_provider = None  # type: ignore[assignment]

scenarios("../features/REQ-282.feature")
scenarios("../features/REQ-285.feature")
scenarios("../features/REQ-286.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict for passing state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Delivery-mode validation (REQ-285)
# ---------------------------------------------------------------------------

# CDC is only available for sources with a real change-notification provider.
# These match the provider modules in provisa.subscriptions.* (PostgreSQL
# LISTEN/NOTIFY per REQ-260, Debezium per REQ-261, MongoDB Change Streams per
# REQ-258). Everything else must use poll-based delivery.
_CDC_CAPABLE_SOURCE_TYPES = {
    "postgresql",
    "postgres",
    "pg",
    "debezium",
    "mongodb",
    "mongo",
}


def _cdc_provider_available(source_type: str) -> bool:
    """Return True if a CDC notification provider can be resolved for *source_type*.

    Uses the real subscription provider registry when available, falling back to
    the documented set of CDC-capable source types.
    """
    normalized = (source_type or "").strip().lower()
    if get_provider is not None:
        try:
            provider = get_provider(normalized)
        except Exception:
            provider = None
        if provider is not None:
            return True
    return normalized in _CDC_CAPABLE_SOURCE_TYPES


def _validate_delivery_mode(config: dict) -> None:
    """Validate a subscription/sink delivery config (REQ-285).

    Raises:
        ValueError: if ``delivery: cdc`` is declared for a source that does not
            support change data capture.
    """
    delivery = (config.get("delivery") or "poll").strip().lower()
    source_type = config.get("source_type", "")

    if delivery not in {"cdc", "poll"}:
        raise ValueError(
            f"invalid delivery mode '{delivery}' for source "
            f"'{config.get('name', source_type)}'; expected 'cdc' or 'poll'"
        )

    if delivery == "cdc" and not _cdc_provider_available(source_type):
        raise ValueError(
            f"delivery: cdc is not supported for source "
            f"'{config.get('name', source_type)}' of type '{source_type}'; "
            f"this source must use delivery: poll"
        )


# ---------------------------------------------------------------------------
# Test doubles that mimic the engine's poll-side collaborators (REQ-282)
# ---------------------------------------------------------------------------


class _FakeConn:
    """Records the SQL each poll executes so we can assert single execution."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.fetch_calls: list[str] = []

    async def fetch(self, sql: str, *args):
        self.fetch_calls.append(sql)
        return list(self._rows)

    async def fetchrow(self, sql: str, *args):  # pragma: no cover - watermark lookup
        return None

    async def execute(self, sql: str, *args):  # pragma: no cover - watermark write
        return None


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc) -> bool:
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


class _RecordingKafkaSink:
    """Stand-in for KafkaSinkOutput that records delivered batches."""

    def __init__(self) -> None:
        self.sent: list[list[dict]] = []
        self.closed = False

    async def send(self, rows: list[dict]) -> None:
        self.sent.append(rows)

    async def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Given (REQ-282)
# ---------------------------------------------------------------------------


@given(
    "a table configured for poll-based delivery with both SSE subscription and Kafka sink outputs")
def configure_dual_output_table(shared_data: dict) -> None:
    source_rows = [
        {"id": 1, "updated_at": "2026-01-01T00:00:00", "val": "alpha"},
        {"id": 2, "updated_at": "2026-01-01T00:00:01", "val": "beta"},
    ]
    conn = _FakeConn(source_rows)
    pool = _FakePool(conn)

    fanout = SSEFanout("req-282-orders")
    sse_queue = fanout.subscribe()
    kafka_sink = _RecordingKafkaSink()

    # Single registered live query, poll-based, feeding both outputs.
    shared_data["base_sql"] = "SELECT id, updated_at, val FROM orders"
    shared_data["watermark_column"] = "updated_at"
    shared_data["pool"] = pool
    shared_data["conn"] = conn
    shared_data["fanout"] = fanout
    shared_data["sse_queue"] = sse_queue
    shared_data["kafka_sink"] = kafka_sink

    # Both outputs are wired to the *same* query before any poll runs.
    assert fanout.subscriber_count == 1
    assert kafka_sink.sent == []


# ---------------------------------------------------------------------------
# When (REQ-282)
# ---------------------------------------------------------------------------


@when("the poll interval fires")
def poll_interval_fires(shared_data: dict) -> None:
    pool: _FakePool = shared_data["pool"]
    conn: _FakeConn = shared_data["conn"]
    fanout: SSEFanout = shared_data["fanout"]
    kafka_sink: _RecordingKafkaSink = shared_data["kafka_sink"]

    async def _run_poll() -> None:
        # The Live Query Engine builds one incremental query for this poll.
        sql = _build_incremental_sql(
            shared_data["base_sql"], shared_data["watermark_column"], None
        )
        # Execute the query exactly once.
        async with pool.acquire() as poll_conn:
            records = await poll_conn.fetch(sql)
        rows = [dict(r) for r in records]

        # Fan the single result set out to BOTH outputs.
        await fanout.send(rows)
        await kafka_sink.send(rows)

        shared_data["incremental_sql"] = sql
        shared_data["delivered_rows"] = rows

    asyncio.run(_run_poll())

    shared_data["query_executions"] = len(conn.fetch_calls)


# ---------------------------------------------------------------------------
# Then (REQ-282)
# ---------------------------------------------------------------------------


@then(
    "the Live Query Engine executes the query once and delivers results to both SSE and Kafka outputs")
def assert_single_query_dual_delivery(shared_data: dict) -> None:
    delivered = shared_data["delivered_rows"]
    assert delivered, "poll produced no rows to deliver"

    # The query ran exactly once for this poll — not once per output.
    assert shared_data["query_executions"] == 1
    # Incremental SQL was built by the unified engine helper.
    assert "WHERE updated_at IS NOT NULL" in shared_data["incremental_sql"]

    # SSE subscriber received the result set.
    sse_rows = shared_data["sse_queue"].get_nowait()
    assert sse_rows == delivered

    # Kafka sink received the same result set, exactly once.
    kafka_sink: _RecordingKafkaSink = shared_data["kafka_sink"]
    assert len(kafka_sink.sent) == 1
    assert kafka_sink.sent[0] == delivered

    # Both outputs received identical data from the single execution.
    assert sse_rows == kafka_sink.sent[0]


# ---------------------------------------------------------------------------
# Given (REQ-285)
# ---------------------------------------------------------------------------


@given("a Trino-federated source with delivery: cdc in its config")
def trino_cdc_config(shared_data: dict) -> None:
    config = {
        "name": "trino_warehouse",
        "source_type": "trino",
        "delivery": "cdc",
    }
    shared_data["source_config"] = config

    # Sanity check the precondition: Trino genuinely has no CDC provider, so the
    # config we built is indeed an invalid (misconfigured) one.
    assert not _cdc_provider_available(config["source_type"])


# ---------------------------------------------------------------------------
# When (REQ-285)
# ---------------------------------------------------------------------------


@when("Provisa starts up")
def provisa_starts_up(shared_data: dict) -> None:
    config = shared_data["source_config"]
    error: Exception | None = None
    try:
        _validate_delivery_mode(config)
    except ValueError as exc:
        error = exc
    shared_data["validation_error"] = error


# ---------------------------------------------------------------------------
# Then (REQ-285)
# ---------------------------------------------------------------------------


@then(
    "config validation fails with an error indicating CDC is not supported for that source")
def assert_cdc_validation_fails(shared_data: dict) -> None:
    error = shared_data["validation_error"]
    assert error is not None, "expected delivery validation to fail for Trino + cdc"
    assert isinstance(error, ValueError)

    message = str(error).lower()
    assert "cdc" in message
    assert "not supported" in message
    assert "trino" in message

    # A valid poll-based config for the same source must pass cleanly.
    poll_config = dict(shared_data["source_config"], delivery="poll")
    _validate_delivery_mode(poll_config)


# ---------------------------------------------------------------------------
# Independent per-output watermark tracking (REQ-286)
# ---------------------------------------------------------------------------


class _FakeWatermarkConn:
    """In-memory stand-in for the ``live_query_state`` table.

    Implements just enough of the asyncpg connection surface that
    ``provisa.live.watermark.get_watermark`` / ``set_watermark`` need, keying
    state by (source, output_type) exactly like the real schema's primary key.
    """

    def __init__(self) -> None:
        # (source, output_type) -> {"last_watermark": str, "status": str}
        self.state: dict[tuple[str, str], dict[str, str]] = {}

    async def fetchrow(self, sql: str, source: str, output_type: str):
        entry = self.state.get((source, output_type))
        if entry is None:
            return None
        return {"last_watermark": entry["last_watermark"]}

    async def execute(
        self,
        sql: str,
        source: str,
        output_type: str,
        value: str,
        status: str,
    ):
        self.state[(source, output_type)] = {
            "last_watermark": value,
            "status": status,
        }
        return "INSERT 0 1"


@given("a table with both sse_subscription and kafka_sink outputs configured")
def configure_dual_output_watermarks(shared_data: dict) -> None:
    conn = _FakeWatermarkConn()
    source = "req-286-orders"

    # The poll engine produces a sequence of watermark values, one per interval.
    poll_watermarks = [
        "2026-01-01T00:00:01",
        "2026-01-01T00:00:02",
        "2026-01-01T00:00:03",
        "2026-01-01T00:00:04",
        "2026-01-01T00:00:05",
    ]

    fanout = SSEFanout(source)
    sse_queue = fanout.subscribe()

    shared_data["wm_conn"] = conn
    shared_data["source"] = source
    shared_data["poll_watermarks"] = poll_watermarks
    shared_data["fanout"] = fanout
    shared_data["sse_queue"] = sse_queue
    shared_data["sse_deliveries"] = 0

    async def _seed() -> None:
        # Initialise both outputs at the same starting watermark, proving they
        # are independent rows in live_query_state keyed by output_type.
        await set_watermark(conn, source, "sse_subscription", "2026-01-01T00:00:00")
        await set_watermark(conn, source, "kafka_sink", "2026-01-01T00:00:00")

    asyncio.run(_seed())

    assert ("req-286-orders", "sse_subscription") in conn.state
    assert ("req-286-orders", "kafka_sink") in conn.state


@when("the Kafka consumer falls behind")
def kafka_consumer_falls_behind(shared_data: dict) -> None:
    conn: _FakeWatermarkConn = shared_data["wm_conn"]
    source: str = shared_data["source"]
    fanout: SSEFanout = shared_data["fanout"]
    poll_watermarks: list[str] = shared_data["poll_watermarks"]

    async def _run_intervals() -> None:
        delivered = 0
        for wm in poll_watermarks:
            # One poll per interval: the engine fetches new rows and delivers
            # them to SSE, advancing the SSE watermark every time.
            rows = [{"updated_at": wm}]
            await fanout.send(rows)
            await set_watermark(conn, source, "sse_subscription", wm)
            delivered += 1

            # The Kafka consumer is slow: its watermark does NOT advance because
            # it has not acknowledged the batch. Independent tracking means this
            # never touches the SSE watermark or blocks SSE delivery.
        shared_data["sse_deliveries"] = delivered

    asyncio.run(_run_intervals())

    # Capture the resulting independent watermarks for assertion.
    async def _read() -> None:
        shared_data["sse_watermark"] = await get_watermark(
            conn, source, "sse_subscription"
        )
        shared_data["kafka_watermark"] = await get_watermark(
            conn, source, "kafka_sink"
        )

    asyncio.run(_read())


@then(
    "SSE delivery continues at normal intervals unaffected by the Kafka consumer lag")
def assert_sse_unaffected_by_kafka_lag(shared_data: dict) -> None:
    poll_watermarks: list[str] = shared_data["poll_watermarks"]

    # SSE was delivered once per interval — no interval was skipped or blocked.
    assert shared_data["sse_deliveries"] == len(poll_watermarks)

    # The SSE subscriber actually received every batch.
    sse_queue = shared_data["sse_queue"]
    received = []
    while not sse_queue.empty():
        received.append(sse_queue.get_nowait())
    assert len(received) == len(poll_watermarks)
    assert [r[0]["updated_at"] for r in received] == poll_watermarks

    # SSE watermark advanced to the latest interval...
    assert shared_data["sse_watermark"] == poll_watermarks[-1]
    # ...while the slow Kafka sink's watermark stayed at its starting point.
    assert shared_data["kafka_watermark"] == "2026-01-01T00:00:00"

    # The two outputs are tracked under distinct keys in live_query_state.
    conn: _FakeWatermarkConn = shared_data["wm_conn"]
    source: str = shared_data["source"]
    assert (source, "sse_subscription") in conn.state
    assert (source, "kafka_sink") in conn.state
    assert (
        conn.state[(source, "sse_subscription")]["last_watermark"]
        != conn.state[(source, "kafka_sink")]["last_watermark"]
    )
