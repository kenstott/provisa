# Copyright (c) 2026 Kenneth Stott
# Canary: 0f5c7a39-8e21-4d64-b7f0-6a3d9c2e1b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow Flight compresses its streams as the operator sets ``server.flight_compression``.

Shipped off. Set through the settings catalog, the next stream is compressed — seen where it
matters, in the bytes the server puts on the wire — and a client reads the same rows with no
setting of its own."""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import urllib.request

import pyarrow.flight as fl
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROWS = 200_000
_KEY = "server.flight_compression"
CATALOG = "/admin/settings/catalog"


class _CountingProxy:
    """A local TCP relay to the server's Flight port that counts the bytes the server sends."""

    def __init__(self, upstream_port: int) -> None:
        self._upstream = ("127.0.0.1", upstream_port)
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self.from_server = 0
        self._lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            upstream = socket.create_connection(self._upstream)
            threading.Thread(target=self._copy, args=(client, upstream, False), daemon=True).start()
            threading.Thread(target=self._copy, args=(upstream, client, True), daemon=True).start()

    def _copy(self, src: socket.socket, dst: socket.socket, counted: bool) -> None:
        try:
            while data := src.recv(1 << 16):
                if counted:
                    with self._lock:
                        self.from_server += len(data)
                dst.sendall(data)
        except OSError:
            pass  # either side closed: the relayed connection is over
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  # already closed by the other direction

    def close(self) -> None:
        self._listener.close()


def _catalog(boot) -> dict[str, dict]:
    with urllib.request.urlopen(f"http://127.0.0.1:{boot.ports['http']}{CATALOG}", timeout=30) as r:
        payload = json.loads(r.read())
    return {s["key"]: s for card in payload["cards"] for s in card["settings"]}


def _set(boot, codec: str) -> None:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{CATALOG}",
        data=json.dumps({"values": {_KEY: codec}, "confirm": []}).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        assert resp.status == 200, resp.read()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _catalog(boot)[_KEY]["value"] == codec:
            return
        time.sleep(0.25)
    raise AssertionError(f"{_KEY} did not become {codec!r}")


def _scan(proxy: _CountingProxy) -> tuple[int, int, int]:
    """(rows, sum of ids, bytes the server sent) for one scan of the table over Flight."""
    before = proxy.from_server
    client = fl.connect(f"grpc://127.0.0.1:{proxy.port}")
    try:
        ticket = fl.Ticket(
            json.dumps({"query": "SELECT id, region FROM sales.wide", "role": "org_admin"}).encode()
        )
        table = client.do_get(ticket).read_all()
    finally:
        client.close()
    time.sleep(0.2)  # the relay's last bytes
    return table.num_rows, sum(table.column("id").to_pylist()), proxy.from_server - before


def test_flight_streams_are_compressed_when_the_operator_sets_it():
    orders = _config(_PG_HOST, _PG_PORT, "unused")["tables"][0]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={"tables": [orders, {**orders, "table": "wide"}]},
    )
    boot.create_database()
    proxy = None
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.wide AS SELECT g AS id, "
                    "'a region name that repeats on every row' AS region "
                    f"FROM generate_series(1, {_ROWS}) g"
                )
            )
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        proxy = _CountingProxy(boot.ports["flight"])
        expected_sum = _ROWS * (_ROWS + 1) // 2

        entry = _catalog(boot)[_KEY]
        assert entry["value"] == "none" and entry["default"] == "none"
        assert entry["choices"] == ["none", "lz4", "zstd"]

        rows, total, plain = _scan(proxy)
        assert (rows, total) == (_ROWS, expected_sum)

        sizes = {"none": plain}
        for codec in ("lz4", "zstd"):
            _set(boot, codec)
            rows, total, sizes[codec] = _scan(proxy)
            assert (rows, total) == (_ROWS, expected_sum), codec  # the client needs no setting
            assert sizes[codec] < plain / 3, sizes

        _set(boot, "none")
        rows, total, again = _scan(proxy)
        assert (rows, total) == (_ROWS, expected_sum)
        assert again > plain * 0.8, {"none": plain, "none again": again, **sizes}
    finally:
        if proxy is not None:
            proxy.close()
        boot.cleanup()
