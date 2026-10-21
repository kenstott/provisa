# Copyright (c) 2026 Kenneth Stott
# Canary: f11b1b2f-5ff1-46fa-9b22-64edb8c04b26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's worker (REQ-1984, REQ-1993): the protocol every language's host speaks, against
a stand-in host written in Python. Rows come batch by batch, a command's value whole, the code's
own error by its message, a crash names the source and the export, a deadline cancels the call in
the host and releases the waiting thread, and a pool starts a new process after a crash."""

# Requirements: REQ-1984, REQ-1993, REQ-1882
from __future__ import annotations

import sys
import textwrap
import time
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import schema_text
from provisa.core import request_deadline

pytestmark = pytest.mark.unit

# The stand-in host: the protocol of provisa/node/host.mjs, in Python. It records each cancel it
# is sent in a file beside it, so a test can see the call was stopped in the host.
_HOST = textwrap.dedent(
    """
    import base64, json, socket, struct, sys, threading, time
    import pyarrow as pa
    path, log = sys.argv[1], sys.argv[2]
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(path)
    lock = threading.Lock()
    cancelled = set()

    def send(m):
        body = b"J" + json.dumps(m).encode()
        with lock:
            conn.sendall(struct.pack(">I", len(body)) + body)

    def send_rows(i, schema, rows):
        # Protocol 2: rows go as one Arrow IPC stream, against the call's schema.
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as stream:
            stream.write_batch(pa.RecordBatch.from_pylist(rows, schema=schema))
        body = b"A" + struct.pack(">Q", i) + sink.getvalue().to_pybytes()
        with lock:
            conn.sendall(struct.pack(">I", len(body)) + body)

    def recv():
        head = b""
        while len(head) < 4:
            chunk = conn.recv(4 - len(head))
            if not chunk:
                return None
            head += chunk
        size, body = struct.unpack(">I", head)[0], b""
        while len(body) < size:
            body += conn.recv(size - len(body))
        return json.loads(body[1:])

    def serve(m):
        # Protocol 3: a table's several argument sets come as one call's `calls`, served in order.
        sets = [c["args"] for c in m["calls"]] if "calls" in m else [m["args"]]
        for args in sets:
            if serve_one(m, args) is False:
                return
        send({"op": "end", "id": m["id"]})

    def serve_one(m, args):
        i, name = m["id"], m["export"]
        schema = m["schema"] and pa.ipc.open_stream(pa.py_buffer(base64.b64decode(m["schema"]))).schema
        if name == "many":
            for start in range(0, args[0], 1000):
                send_rows(i, schema, [{"n": n} for n in range(start, min(start + 1000, args[0]))])
        elif name == "echo":
            # The arguments as they came, wire-encoded: a json field's text is the wire's JSON.
            send_rows(i, schema, [{"args": json.dumps(args)}])
        elif name == "total":
            send({"op": "value", "id": i, "value": {"$decimal": "12.50"}})
        elif name == "boom":
            send({"op": "error", "id": i, "message": "no such customer", "stack": "at boom"})
            return False
        elif name == "ticks":
            n = 0
            while i not in cancelled:
                send_rows(i, schema, [{"tick": n}])
                n += 1
                time.sleep(0.01)
            with open(log, "a") as record:
                record.write(f"cancelled {i}\\n")
            return False
        elif name == "slow":
            while i not in cancelled:
                time.sleep(0.01)
            with open(log, "a") as record:
                record.write(f"cancelled {i}\\n")
            return False
        elif name == "die":
            sys.stderr.write("the code fell over\\n")
            sys.stderr.flush()
            import os
            os._exit(3)

    import os
    hello = {"op": "hello", "protocol": int(os.environ.get("TEST_PROTOCOL", "3")),
             "code_hash": os.environ["PROVISA_CODE_HASH"],
             "exports": ["many", "echo", "total", "boom", "slow", "ticks", "die"]}
    send(hello)
    while (m := recv()) is not None:
        if m["op"] == "ping":
            send({"op": "pong", "id": m["id"]})
        elif m["op"] == "cancel":
            cancelled.add(m["id"])
        elif m["op"] == "call":
            threading.Thread(target=serve, args=(m,), daemon=True).start()
    """
)


_JSON = {b"provisa.ir": b"json"}
MANY = schema_text(pa.schema([("n", pa.int64())]))
ECHO = schema_text(pa.schema([pa.field("args", pa.utf8(), metadata=_JSON)]))
TICKS = schema_text(pa.schema([("tick", pa.int64())]))
ANY = MANY  # an export that sends no rows: any schema


@pytest.fixture
def command(tmp_path):
    host = tmp_path / "host.py"
    host.write_text(_HOST)
    log = tmp_path / "cancels.log"
    return w.HostCommand(
        argv=lambda address: [sys.executable, str(host), address, str(log)],
        env=w.private_environment(),
        cwd=tmp_path,
    ), log


@pytest.fixture
def worker(command):
    started = w.Worker("orders-src", "h1", command[0])
    started.start()
    yield started
    started.stop()


def test_the_host_says_which_exports_it_has(worker):
    assert worker.exports == ["many", "echo", "total", "boom", "slow", "ticks", "die"]
    assert worker.ping()


def test_a_tables_rows_come_batch_by_batch(worker):
    batches = list(worker.rows("many", "table", [2500], schema=MANY))
    assert [len(b) for b in batches] == [1000, 1000, 500]
    assert batches[2][-1] == {"n": 2499}


def test_values_cross_the_wire_in_their_own_types(worker):
    placed = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    [[row]] = list(
        worker.rows("echo", "table", [placed, 2**60, Decimal("1.10"), None, "x"], schema=ECHO)
    )
    assert row == {"args": [placed, 2**60, Decimal("1.10"), None, "x"]}


def test_a_tables_argument_sets_go_in_one_call_and_come_back_in_order(worker):
    """REQ-2099: every argument set of a read goes to the host in one call, which runs the
    function per set; their rows come back in the sets' order, under the one call."""
    calls = [([n], ["n"]) for n in (1, 2, 3)]
    rows = [row for batch in worker.rows_each("echo", calls, schema=ECHO) for row in batch]
    assert rows == [{"args": [1]}, {"args": [2]}, {"args": [3]}]


def test_a_tables_argument_sets_are_one_round_trip(worker, monkeypatch):
    """REQ-2099: n argument sets are one request to the host, not n."""
    sent = []
    original = w.Worker._send

    def counting(self, message):
        sent.append(message.get("op"))
        return original(self, message)

    monkeypatch.setattr(w.Worker, "_send", counting)
    calls = [([n], ["n"]) for n in range(50)]
    rows = [row for batch in worker.rows_each("echo", calls, schema=ECHO) for row in batch]
    assert len(rows) == 50
    assert sent.count("call") == 1


def test_a_commands_value_comes_whole(worker):
    assert worker.command("total", []) == Decimal("12.50")


def test_the_codes_own_error_is_named_by_source_and_export(worker):
    with pytest.raises(w.CodeFailed) as failed:
        list(worker.rows("boom", "table", [], schema=ANY))
    assert str(failed.value) == "source 'orders-src', export 'boom': no such customer"
    assert failed.value.stack == "at boom"
    assert worker.ping()  # the worker serves on


def test_a_deadline_cancels_the_call_in_the_host_and_releases_the_thread(worker, command):
    _, log = command
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        with request_deadline.within(0.3):
            list(worker.rows("slow", "table", [], schema=ANY))
    assert time.monotonic() - started < 5.0
    # The host records the cancel once the call stops; wait for the record, not only the file.
    until = time.monotonic() + 5.0
    while not (log.exists() and log.read_text()) and time.monotonic() < until:
        time.sleep(0.02)
    assert log.read_text().startswith("cancelled ")
    assert worker.ping()


def test_closing_a_stream_early_cancels_it_in_the_host(worker, command):
    _, log = command
    rows = worker.rows("ticks", "stream", [], schema=TICKS)
    assert [next(rows), next(rows)] == [[{"tick": 0}], [{"tick": 1}]]
    rows.close()  # the subscriber left
    until = time.monotonic() + 5.0
    while not log.exists() and time.monotonic() < until:
        time.sleep(0.02)
    assert log.exists()


def test_closing_a_stream_early_ends_its_call_while_its_batches_are_still_referenced(
    worker, command, monkeypatch
):
    # rows() reads through batches(); closing rows must close the call itself, not leave it to
    # the collector, or the call's span stays current for whatever runs next on the thread.
    _, log = command
    held = []
    batches = worker.batches

    def holding(*args, **kwargs):
        held.append(batches(*args, **kwargs))
        return held[-1]

    monkeypatch.setattr(worker, "batches", holding)
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    monkeypatch.setattr(w, "_tracer", TracerProvider().get_tracer(__name__))
    before = trace.get_current_span()
    rows = worker.rows("ticks", "stream", [], schema=TICKS)
    assert [next(rows), next(rows)] == [[{"tick": 0}], [{"tick": 1}]]
    rows.close()
    assert trace.get_current_span() is before
    until = time.monotonic() + 5.0
    while not log.exists() and time.monotonic() < until:
        time.sleep(0.02)
    assert log.exists()


def test_a_crash_fails_the_call_naming_the_source_the_export_and_what_the_host_said(worker):
    with pytest.raises(w.WorkerFailed) as failed:
        list(worker.rows("die", "table", [], schema=ANY))
    message = str(failed.value)
    assert message.startswith("source 'orders-src', export 'die': its host ended (exit 3)")
    assert "the code fell over" in message
    assert not worker.alive()


def test_a_host_that_never_connects_is_named(tmp_path):
    never = w.HostCommand(
        argv=lambda _socket: [
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('no'); sys.exit(2)",
        ],
        env=w.private_environment(),
        cwd=tmp_path,
    )
    worker = w.Worker("broken-src", "h", never)
    with pytest.raises(w.WorkerFailed) as failed:
        worker.start(timeout=2.0)
    assert str(failed.value).startswith(
        "source 'broken-src': its host ended before it connected (exit 2)"
    )


def test_the_pool_starts_a_new_worker_after_a_crash_and_stops_a_sources_workers(command):
    pool = w.WorkerPool()
    first = pool.get("orders-src", "h1", lambda: command[0])
    assert pool.get("orders-src", "h1", lambda: command[0]) is first
    with pytest.raises(w.WorkerFailed):
        first.command("die", [])
    second = pool.get("orders-src", "h1", lambda: command[0])
    assert second is not first and second.ping()
    other = pool.get("orders-src", "h2", lambda: command[0])
    pool.stop_source("orders-src")
    assert not second.alive() and not other.alive()
    pool.stop_all()


def test_a_worker_gets_only_path_from_the_environment(monkeypatch):
    monkeypatch.setenv("PROVISA_SECRET", "s3cret")
    assert set(w.private_environment(NO_COLOR="1")) == {"PATH", "NO_COLOR"}


def test_a_value_with_no_wire_form_is_refused():
    with pytest.raises(TypeError):
        w.encode(object())


def test_a_host_that_neither_connects_nor_exits_is_stopped_at_the_start_timeout(tmp_path):
    silent = w.HostCommand(
        argv=lambda _socket: [sys.executable, "-c", "import time; time.sleep(60)"],
        env=w.private_environment(),
        cwd=tmp_path,
    )
    worker = w.Worker("silent-src", "h", silent)
    with pytest.raises(w.WorkerFailed) as failed:
        worker.start(timeout=0.5)
    assert str(failed.value).startswith("source 'silent-src': its host did not connect within 0.5s")
    assert not worker.alive()


def test_a_host_speaking_another_protocol_is_refused_by_name(command, tmp_path):
    """REQ-2098: a host built for another Provisa version is refused, and told why."""
    base = command[0]
    other = w.HostCommand(argv=base.argv, env={**base.env, "TEST_PROTOCOL": "1"}, cwd=base.cwd)
    worker = w.Worker("orders-src", "h1", other)
    with pytest.raises(w.WorkerFailed) as failed:
        worker.start()
    assert str(failed.value) == (
        "source 'orders-src': its host speaks protocol 1, and this Provisa speaks 3: "
        "run a host built for this Provisa version"
    )
    assert not worker.alive()


def test_a_host_running_other_code_than_the_source_registered_is_refused_by_name():
    """REQ-2098: the hello's code hash is the source's, or the host is refused."""
    hello = {"op": "hello", "protocol": w.PROTOCOL, "code_hash": "abc", "exports": []}
    assert w.refusal(hello, "abc") is None
    assert w.refusal(hello, "def") == "its host runs code abc, not the def the source registered"
