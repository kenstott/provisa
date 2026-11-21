# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4e1c7b-2d95-4f36-b0e8-6c3f9a2d5b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A client that closes its connection mid-request stops the statement on the engine (REQ-1905).

One server, one table whose scan the test can make slow (a view over ``pg_sleep``: 30 s), and the
shipped 60 s request timeout, so nothing but the client leaving can end the scan early. On each
HTTP data transport the request is sent over a raw socket; once its statement is running on the
source the socket is closed (what the UI's Stop does by aborting its fetch), and the statement is
gone from the source well before its 30 s were up. The next request on that transport is served."""

# Requirements: REQ-1905, REQ-1882

from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLE = "org_admin"
_SCAN_S = 30
# The statement is gone this soon after the client leaves, against its 30 s.
_GONE_WITHIN_S = 8.0

# transport -> (method, path, body) of a request that scans ``{table}``
_REQUESTS: dict[str, tuple[str, str, dict | None]] = {
    "graphql": ("POST", "/data/graphql", {"query": "{{ s__{table} {{ id }} }}"}),
    "sql_http": ("POST", "/data/sql", {"sql": "SELECT id FROM sales.{table}"}),
    "rest": ("GET", "/data/rest/sales/{table}", None),
    "jsonapi": ("GET", "/data/jsonapi/sales/{table}", None),
    "cypher_http": ("POST", "/data/cypher", {"query": "MATCH (n:{Table}) RETURN n.id AS id"}),
}


def _request(transport: str, table: str) -> tuple[str, str, bytes | None]:
    method, path, body = _REQUESTS[transport]
    fill = {"table": table, "Table": table.capitalize()}
    raw = None
    if body is not None:
        raw = json.dumps({k: v.format(**fill) for k, v in body.items()}).encode()
    return method, path.format(**fill), raw


def _http(boot, transport: str, table: str) -> tuple[int, str]:
    method, path, body = _request(transport, table)
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=body,
        headers={"Content-Type": "application/json", "x-provisa-role": _ROLE},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _open(boot, transport: str, table: str) -> socket.socket:
    """Send the request over a socket of its own, and leave the answer unread."""
    method, path, body = _request(transport, table)
    payload = body or b""
    head = (
        f"{method} {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{boot.ports['http']}\r\n"
        f"Content-Type: application/json\r\n"
        f"x-provisa-role: {_ROLE}\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "\r\n"
    ).encode()
    sock = socket.create_connection(("127.0.0.1", boot.ports["http"]), timeout=30)
    sock.sendall(head + payload)
    return sock


def _scans(engine: sa.Engine) -> list[str]:
    """The statements reading the slow view that are running on the source now."""
    query = sa.text(
        "SELECT left(query, 120) FROM pg_stat_activity "
        "WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' "
        "AND query ILIKE '%slow%' AND query NOT ILIKE '%pg_stat_activity%'"
    )
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(query).fetchall()]


def _until(check, timeout: float) -> float | None:
    """Seconds until ``check()`` holds, or None when it did not within ``timeout``."""
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if check():
            return time.monotonic() - started
        time.sleep(0.1)
    return None


@pytest.fixture(scope="module")
def server():
    orders = _config(_PG_HOST, _PG_PORT, "unused")["tables"][0]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={"tables": [orders, {**orders, "table": "slow"}]},
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            # A scan of ``slow`` takes 30 s on the source once the switch is on; until then it
            # is three rows at once, so the server's own start-up reads of it are not slowed.
            conn.execute(sa.text("CREATE TABLE public.slow_switch (seconds integer)"))
            conn.execute(sa.text("INSERT INTO public.slow_switch VALUES (0)"))
            conn.execute(
                sa.text(
                    "CREATE VIEW public.slow AS SELECT g AS id, 'r'::text AS region "
                    "FROM generate_series(1, 3) g, "
                    "(SELECT pg_sleep((SELECT seconds FROM public.slow_switch))) s"
                )
            )
        boot.start()
        boot.wait_all_ready(timeout=300)
        with own.connect() as conn:
            conn.execute(sa.text(f"UPDATE public.slow_switch SET seconds = {_SCAN_S}"))
        yield boot, own
    finally:
        boot.cleanup()


@pytest.mark.parametrize("transport", list(_REQUESTS))
def test_closing_the_connection_stops_the_statement_on_the_engine(server, transport):
    boot, own = server
    assert _until(lambda: not _scans(own), 60) is not None, "a scan was left from before"

    sock = _open(boot, transport, "slow")
    try:
        running = _until(lambda: bool(_scans(own)), 30)
        assert running is not None, f"{transport}: the scan never reached the source"
    finally:
        sock.close()  # the client leaves (the UI's Stop aborts its fetch)

    gone = _until(lambda: not _scans(own), _SCAN_S)
    assert gone is not None and gone < _GONE_WITHIN_S, (
        f"{transport}: the statement ran on after the client left "
        f"({'to its end' if gone is None else f'{gone:.1f}s'}): {_scans(own)}"
    )

    # The transport keeps serving: the next request on it is answered.
    status, body = _http(boot, transport, "orders")
    assert status == 200, f"{transport}: the request after the disconnect failed: {status} {body}"
