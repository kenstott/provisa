# Copyright (c) 2026 Kenneth Stott
# Canary: 1607842a-f9f7-4465-acd1-2bb02117faec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The egress byte meter (REQ-1452, REQ-1455).

A reported byte is metered exactly once, or still pending after a failed drain. Never both,
never neither.
"""

import asyncio

import pytest

from provisa.core import egress


@pytest.fixture(autouse=True)
def _empty_buffer():
    """The module buffer is process-global."""
    egress.take_pending()
    yield
    egress.take_pending()


class TestReport:
    def test_reports_for_one_org_accumulate(self):
        egress.report("acme", 100)
        egress.report("acme", 250)
        assert egress.take_pending() == {"acme": 350}

    def test_orgs_are_counted_separately(self):
        egress.report("acme", 100)
        egress.report("globex", 7)
        assert egress.take_pending() == {"acme": 100, "globex": 7}

    def test_unattributable_bytes_are_dropped_not_defaulted(self):
        egress.report(None, 5000)
        assert egress.take_pending() == {}

    def test_a_zero_length_body_is_not_a_report(self):
        egress.report("acme", 0)
        assert egress.take_pending() == {}

    def test_taking_pending_empties_the_buffer(self):
        egress.report("acme", 100)
        assert egress.take_pending() == {"acme": 100}
        assert egress.take_pending() == {}

    def test_reports_from_many_threads_lose_nothing(self):
        # pgwire writes from socketserver threads.
        import threading

        def hammer():
            for _ in range(500):
                egress.report("acme", 1)

        threads = [threading.Thread(target=hammer) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert egress.take_pending() == {"acme": 4000}


class TestDrain:
    def test_drain_writes_every_org_to_the_meter(self, monkeypatch):
        written = []

        async def fake_meter(pool, org_id, n_bytes):  # noqa: ARG001
            written.append((org_id, n_bytes))

        monkeypatch.setattr("provisa.core.commerce.meter_egress", fake_meter)
        egress.report("acme", 100)
        egress.report("globex", 200)
        asyncio.run(egress.drain(object()))
        assert sorted(written) == [("acme", 100), ("globex", 200)]
        assert egress.take_pending() == {}

    def test_an_empty_buffer_never_touches_the_meter(self, monkeypatch):
        called = []

        async def fake_meter(pool, org_id, n_bytes):  # noqa: ARG001
            called.append(org_id)

        monkeypatch.setattr("provisa.core.commerce.meter_egress", fake_meter)
        asyncio.run(egress.drain(object()))
        assert called == []

    def test_a_failed_drain_restores_what_it_had_not_written(self, monkeypatch):
        async def fake_meter(pool, org_id, n_bytes):  # noqa: ARG001
            if org_id == "globex":
                raise RuntimeError("control plane unreachable")

        monkeypatch.setattr("provisa.core.commerce.meter_egress", fake_meter)
        egress.report("acme", 100)
        egress.report("globex", 200)
        with pytest.raises(RuntimeError):
            asyncio.run(egress.drain(object()))
        assert egress.take_pending() == {"globex": 200}

    def test_bytes_reported_during_a_failed_drain_survive_the_restore(self, monkeypatch):
        async def fake_meter(pool, org_id, n_bytes):  # noqa: ARG001
            egress.report("acme", 5)  # a live request writing mid-drain
            raise RuntimeError("boom")

        monkeypatch.setattr("provisa.core.commerce.meter_egress", fake_meter)
        egress.report("acme", 100)
        with pytest.raises(RuntimeError):
            asyncio.run(egress.drain(object()))
        assert egress.take_pending() == {"acme": 105}


class TestHTTPMiddleware:
    def _run(self, scope, chunks):
        async def app(scope, receive, send):  # noqa: ARG001
            await send({"type": "http.response.start", "status": 200, "headers": []})
            for body in chunks:
                await send({"type": "http.response.body", "body": body, "more_body": True})

        async def send(message):
            return None

        asyncio.run(egress.EgressMeterMiddleware(app)(scope, None, send))

    def test_response_body_bytes_are_metered_to_the_scope_org(self):
        self._run({"type": "http", "state": {"active_org_id": "acme"}}, [b"abcd", b"ef"])
        assert egress.take_pending() == {"acme": 6}

    def test_a_request_with_no_org_meters_nothing(self):
        self._run({"type": "http", "state": {}}, [b"abcd"])
        assert egress.take_pending() == {}

    def test_the_response_start_frame_is_not_a_body(self):
        self._run({"type": "http", "state": {"active_org_id": "acme"}}, [])
        assert egress.take_pending() == {}

    def test_a_websocket_scope_passes_straight_through(self):
        seen = []

        async def app(scope, receive, send):  # noqa: ARG001
            seen.append(scope["type"])

        async def send(message):
            return None

        asyncio.run(
            egress.EgressMeterMiddleware(app)({"type": "websocket", "state": {}}, None, send)
        )
        assert seen == ["websocket"]
        assert egress.take_pending() == {}

    def test_the_org_is_read_at_flush_time_not_entry_time(self):
        state: dict = {}

        async def app(scope, receive, send):  # noqa: ARG001
            state["active_org_id"] = "acme"  # what the auth middleware does
            await send({"type": "http.response.body", "body": b"xyz"})

        async def send(message):
            return None

        asyncio.run(egress.EgressMeterMiddleware(app)({"type": "http", "state": state}, None, send))
        assert egress.take_pending() == {"acme": 3}


class TestCountingWriter:
    class _Sink:
        def __init__(self):
            self.buf = b""
            self.flushed = 0

        def write(self, data):
            self.buf += data
            return len(data)

        def flush(self):
            self.flushed += 1

    def test_bytes_written_are_metered_and_still_reach_the_socket(self):
        sink = self._Sink()
        writer = egress.CountingWriter(sink, "acme")
        assert writer.write(b"hello") == 5
        assert sink.buf == b"hello"
        assert egress.take_pending() == {"acme": 5}

    def test_writes_before_authentication_are_not_attributed(self):
        sink = self._Sink()
        writer = egress.CountingWriter(sink, None)
        writer.write(b"handshake")
        assert egress.take_pending() == {}

    def test_binding_the_org_attributes_only_subsequent_writes(self):
        sink = self._Sink()
        writer = egress.CountingWriter(sink, None)
        writer.write(b"handshake")
        writer.bind_org("acme")
        writer.write(b"rows")
        assert egress.take_pending() == {"acme": 4}

    def test_everything_else_delegates_to_the_wrapped_writer(self):
        sink = self._Sink()
        writer = egress.CountingWriter(sink, "acme")
        writer.flush()
        assert sink.flushed == 1
