# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f9e05-c4d1-4a86-8e3f-6d0a1b5c9e72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source served inside the worker's sandbox (REQ-2200): its host, compiled by the pinned Go
with the code linked in, serves rows sandboxed; it reads nothing outside itself and reaches the
network only through the egress proxy, to the hosts on the allow-list."""

# Requirements: REQ-2200
from __future__ import annotations

import socket
import threading
from pathlib import Path

import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.manifest import Manifest
from provisa.golang.build import build_host
from provisa.golang.reader import ensure_reader, read_code
from provisa.golang.source import host_command
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

pytestmark = [pytest.mark.integration]

FIXTURE = Path(__file__).parent.parent / "fixtures" / "go_source"
PROXY = "https://proxy.golang.org"
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Manifest, Path]:
    """The fixture's manifest, and its host compiled by the pinned Go."""
    cache = tmp_path_factory.mktemp("cache")
    go = CodeRuntimeResolver(cache_root=cache).resolve("go")
    reader = ensure_reader(go, RUNTIMES["go"].version, cache, PROXY)
    manifest = read_code(
        reader=reader, go=go, code=FIXTURE, package=".", gomodcache=cache / "mod", scratch=cache
    )
    binary = build_host(
        go=go,
        code=FIXTURE,
        package=".",
        manifest=manifest,
        gomodcache=cache / "mod",
        proxy=PROXY,
        dest=tmp_path_factory.mktemp("bin") / "host",
    )
    return manifest, binary


def _start(binary: Path, egress=()) -> w.Worker:
    started = w.Worker("go-src", "h", host_command(binary=binary, egress=list(egress)))
    started.start()
    return started


def _rows(started: w.Worker, manifest: Manifest, export: str, args: list) -> list[list[dict]]:
    table = manifest.export(export)
    names = [p.name for p in table.parameters]
    given = [*args, *([w.ABSENT] * (len(names) - len(args)))]
    schema = schema_text(row_schema(manifest, table))
    return list(
        started.rows(export, "table", given, names, schema=schema, takes=table.takes, context=EMPTY)
    )


def test_rows_are_served_inside_the_sandbox(built):
    manifest, binary = built
    started = _start(binary)
    try:
        [[row]] = _rows(started, manifest, "Orders", ["acme"])
        assert row["lines"] == [{"sku": "acme", "qty": 2}]
        assert [len(b) for b in _rows(started, manifest, "Many", [1500])] == [1000, 500]
    finally:
        started.stop()


def test_the_code_reads_nothing_outside_it(built):
    manifest, binary = built
    started = _start(binary)
    try:
        with pytest.raises(w.CodeFailed, match="permission denied|operation not permitted"):
            _rows(started, manifest, "Outside", [])
    finally:
        started.stop()


def _outward_address() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("192.0.2.1", 9))  # TEST-NET-1: routed, never answered
        return probe.getsockname()[0]


@pytest.fixture
def upstream():
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


def test_a_host_on_the_allow_list_is_reached_through_the_proxy_and_no_other(built, upstream):
    manifest, binary = built
    host, port = upstream
    started = _start(binary, egress=[f"{host}:{port}"])
    try:
        [[reached]] = _rows(started, manifest, "Fetched", [f"http://{host}:{port}/rates"])
        assert reached == {"status": 200, "body": "ok"}
        [[refused]] = _rows(started, manifest, "Fetched", [f"http://{host}:{port + 1}/x"])
        assert refused["status"] == 403
        assert "not on the egress allow-list" in refused["body"]
        with pytest.raises(w.CodeFailed):
            _rows(started, manifest, "Direct", [host, port])  # around the proxy
    finally:
        started.stop()
