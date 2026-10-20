# Copyright (c) 2026 Kenneth Stott
# Canary: 0d939eb4-d302-40ef-81a4-ff9b67cd9319
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Node source's worker under each real runtime (REQ-1984): the pinned runtime is fetched as a
source's would be and runs Provisa's host on a bundle. Rows, timestamps and big integers, a
command's value, the code's own error, a stream stopped by its subscriber, and a deadline that
stops a call, alike on Node, Bun and Deno; Deno reads nothing outside the bundle."""

# Requirements: REQ-1984, REQ-1882
from __future__ import annotations

import json
import socket
import threading
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import schema_text
from provisa.core import request_deadline
from provisa.node.worker import host_command
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

pytestmark = [pytest.mark.integration]

BUNDLE = """
export async function orders(customerId, since) {
  return [{ id: 1, customer: customerId, placed: new Date("2026-01-02T03:04:05.000Z"),
            since: since ?? null, big: 9007199254740993n, note: undefined }];
}
export function many(count) {
  return Array.from({ length: count }, (_, i) => ({ i }));
}
export async function* ticks() {
  for (let n = 0; ; n++) {
    yield { tick: n };
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
export async function* forever() {
  await new Promise(() => {});
}
export async function cancelOrder(id) {
  return { cancelled: id };
}
export async function boom() {
  throw new Error("no such customer");
}
export async function fetched(url) {
  const response = await fetch(url);
  return [{ status: response.status, body: await response.text() }];
}
export async function direct(host, port) {
  const net = await import("node:net");
  await new Promise((resolve, reject) => {
    const socket = net.connect({ host, port }, () => { socket.destroy(); resolve(); });
    socket.on("error", reject);
  });
  return [{ connected: true }];
}
export async function outside() {
  const fs = await import("node:fs");
  return [{ text: fs.readFileSync("/etc/hosts", "utf8").length }];
}
"""


# Each export's rows' schema, as a call gives it (REQ-2098): the bundle is untyped JavaScript.
_UTC = pa.timestamp("us", tz="UTC")
SCHEMAS = {
    "orders": [
        ("id", pa.int64()),
        ("customer", pa.utf8()),
        ("placed", _UTC),
        ("since", _UTC),
        ("big", pa.int64()),
        ("note", pa.utf8()),
    ],
    "many": [("i", pa.int64())],
    "ticks": [("tick", pa.int64())],
    "forever": [("tick", pa.int64())],
    "boom": [("x", pa.utf8())],
    "fetched": [("status", pa.int64()), ("body", pa.utf8())],
    "direct": [("connected", pa.bool_())],
    "outside": [("text", pa.int64())],
}


def _schema(export: str) -> str:
    return schema_text(pa.schema(SCHEMAS[export]))


def _host_lib(cache):
    return CodeRuntimeResolver(cache_root=cache).resolve("apache-arrow").parent.parent


@pytest.fixture(scope="module")
def cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture
def bundle(tmp_path):
    (tmp_path / "package.json").write_text(json.dumps({"type": "module", "main": "orders.js"}))
    (tmp_path / "orders.js").write_text(BUNDLE)
    return tmp_path


def _start(runtime, cache, bundle, egress=()):
    executable = CodeRuntimeResolver(cache_root=cache).resolve(runtime)
    command = host_command(
        runtime=runtime,
        executable=executable,
        bundle=bundle,
        egress=list(egress),
        host_lib=_host_lib(cache),
    )
    started = w.Worker(f"{runtime}-src", "h", command)
    started.start()
    return started


@pytest.fixture(params=["node", "bun", "deno"])
def worker(request, cache, bundle):
    started = _start(request.param, cache, bundle)
    yield request.param, started
    started.stop()


def test_rows_and_their_values_come_back_in_their_own_types(worker):
    _, started = worker
    assert set(started.exports) >= {"orders", "many", "ticks", "cancelOrder", "boom"}
    since = datetime(2025, 12, 1, tzinfo=UTC)
    [[row]] = list(started.rows("orders", "table", ["acme", since], schema=_schema("orders")))
    assert row == {
        "id": 1,
        "customer": "acme",
        "placed": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        "since": since,
        "big": 9007199254740993,
        "note": None,
    }


def test_a_large_table_comes_in_batches(worker):
    _, started = worker
    assert [len(b) for b in started.rows("many", "table", [2500], schema=_schema("many"))] == [
        1000,
        1000,
        500,
    ]


def test_a_commands_value_and_the_codes_own_error(worker):
    _, started = worker
    assert started.command("cancelOrder", [7]) == {"cancelled": 7}
    with pytest.raises(w.CodeFailed, match="no such customer"):
        list(started.rows("boom", "table", [], schema=_schema("boom")))


def test_a_stream_its_subscriber_leaves_is_stopped_and_the_worker_serves_on(worker):
    _, started = worker
    rows = started.rows("ticks", "stream", [], schema=_schema("ticks"))
    assert [next(rows), next(rows), next(rows)] == [[{"tick": 0}], [{"tick": 1}], [{"tick": 2}]]
    rows.close()
    assert started.ping()


def test_a_deadline_stops_a_call_that_never_answers(worker):
    _, started = worker
    with pytest.raises(TimeoutError):
        with request_deadline.within(0.5):
            list(started.rows("forever", "stream", [], schema=_schema("forever")))
    assert started.ping()


@pytest.mark.parametrize("runtime", ["node", "bun", "deno"])
def test_no_runtime_reads_outside_the_bundle(runtime, cache, bundle):
    started = _start(runtime, cache, bundle)
    try:
        with pytest.raises(w.CodeFailed):
            list(started.rows("outside", "table", [], schema=_schema("outside")))
    finally:
        started.stop()


def _outward_address() -> str:
    """This machine's address on its default route (loopback is never reachable from a code
    source, so it cannot stand in for a host on the allow-list)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("192.0.2.1", 9))  # TEST-NET-1: routed, never answered
        return probe.getsockname()[0]


@pytest.fixture
def upstream():
    """An HTTP server on this machine's outward address, answering every request "ok"."""
    server = socket.socket()
    server.bind((_outward_address(), 0))
    server.listen(8)

    def _serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                conn.recv(65536)
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")

    threading.Thread(target=_serve, daemon=True).start()
    yield server.getsockname()
    server.close()


@pytest.mark.parametrize("runtime", ["node", "bun", "deno"])
def test_a_host_on_the_allow_list_is_reached_and_no_other(runtime, cache, bundle, upstream):
    host, port = upstream
    started = _start(runtime, cache, bundle, egress=[f"{host}:{port}"])
    try:
        [[row]] = list(
            started.rows(
                "fetched", "table", [f"http://{host}:{port}/rates"], schema=_schema("fetched")
            )
        )
        assert row == {"status": 200, "body": "ok"}
        # Off the list: refused before it leaves. A runtime that tunnels plain HTTP (Node) sees
        # the refusal as an error; one that sends it to the proxy as a request (Bun) gets the
        # proxy's 403, naming the host; Deno's own permission refuses it.
        try:
            [[refused]] = list(
                started.rows(
                    "fetched", "table", [f"http://{host}:{port + 1}/x"], schema=_schema("fetched")
                )
            )
        except w.CodeFailed:
            return
        assert refused["status"] == 403, refused
        assert "not on the egress allow-list" in refused["body"], refused
    finally:
        started.stop()


@pytest.mark.parametrize("runtime", ["node", "bun"])
def test_a_connection_that_does_not_go_through_the_proxy_is_refused(
    runtime, cache, bundle, upstream
):
    host, port = upstream
    started = _start(runtime, cache, bundle, egress=[f"{host}:{port}"])
    try:
        with pytest.raises(w.CodeFailed):
            list(started.rows("direct", "table", [host, port], schema=_schema("direct")))
    finally:
        started.stop()


@pytest.mark.parametrize("runtime", ["node", "bun"])
def test_a_host_serves_over_a_socket_it_inherits(runtime, cache, bundle):
    """The Linux sandbox hands the host its socket already connected (it may open no socket but
    TCP); each runtime's host serves over it as over one it connected itself."""
    from provisa.node.worker import HOST, entry_module

    executable = CodeRuntimeResolver(cache_root=cache).resolve(runtime)
    entry = entry_module(bundle)
    command = w.HostCommand(
        argv=lambda address: [str(executable), str(HOST), address, str(entry)],
        env=w.private_environment(PROVISA_HOST_LIB=str(_host_lib(cache))),
        cwd=bundle,
        inherit_socket=True,
    )
    started = w.Worker(f"{runtime}-src", "h", command)
    started.start()
    try:
        assert [len(b) for b in started.rows("many", "table", [1500], schema=_schema("many"))] == [
            1000,
            500,
        ]
    finally:
        started.stop()
