# Copyright (c) 2026 Kenneth Stott
# Canary: c47e1b93-2d6a-48f5-a0b7-5e9d13f86a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source's pgwire sidecar (REQ-2267 part 3): Provisa launches pgfront as a
process of its own in front of the source's local worker, and a PostgreSQL client reads the
code's tables through it -- its catalog, and a read that is the function called on that worker.
The sidecar is started once, health-checked and stopped with its port freed; forgetting the
source stops it, and it ends when Provisa's connections to it close."""

# Requirements: REQ-2267
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import textwrap
import time
from types import SimpleNamespace

import psycopg
import pytest

from provisa.codesource import pgwire_sidecar
from provisa.codesource.manifest import PYTHON_RULES, read_manifest
from provisa.codesource.names import name_tables
from provisa.codesource.worker import HostCommand, Worker
from provisa.federation.pgwire_replica import PortPair, ServerLifecycleError, _port_is_free
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

BOUNDS = {"memory_mb": 256, "threads": 2, "spill_cap_mb": 1024}

_CODE = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_services import ReadContext


    @dataclass
    class Order:
        id: int
        customerName: str
        asked: str


    @dataclass
    class OrderInputs:
        n: int = 3


    def orders(inputs: OrderInputs, context: ReadContext) -> list[Order]:
        """Orders.

        @table
        """
        filters = ",".join(f"{f.column} {f.op} {f.value}" for f in context.filters)
        asked = f"n={inputs.n};filters={filters}"
        return [Order(i, "acme" if i % 2 else "zeta", asked) for i in range(inputs.n)]


    @dataclass
    class Broken:
        id: int


    def broken() -> list[Broken]:
        """Always fails.

        @table
        """
        raise RuntimeError("the warehouse is closed")
    '''
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def code(tmp_path_factory):
    """The code, its manifest, and its local worker, as the pool runs a dev-mode source's."""
    root = tmp_path_factory.mktemp("sidecar")
    code = root / "code"
    code.mkdir()
    (code / "shop.py").write_text(_CODE)
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), "shop"], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    manifest = read_manifest(json.loads(done.stdout), PYTHON_RULES)
    worker = Worker(
        "shop-src",
        "h1",
        HostCommand(
            argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "shop"],
            env=dict(os.environ),
            cwd=code,
        ),
    )
    worker.start()
    yield SimpleNamespace(root=root, manifest=manifest, worker=worker)
    worker.stop()


def _sidecar(code, port: int) -> pgwire_sidecar.Sidecar:
    return pgwire_sidecar.Sidecar(
        source_id="shop-src",
        content_hash="h1",
        names=name_tables(code.manifest),
        worker=lambda: code.worker,
        ports=PortPair(port, "127.0.0.1", 0),
        bounds=BOUNDS,
        spill_dir=code.root / "spill",
    )


@pytest.fixture(scope="module")
def sidecar(code):
    running = _sidecar(code, _free_port())
    running.start()
    yield running
    running.stop()


def _dsn(port: int) -> str:
    return f"host=127.0.0.1 port={port} dbname=code user=provisa sslmode=disable"


def _rows(conn, sql: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_the_sidecar_answers_a_catalog_query_with_the_codes_tables(sidecar):
    with psycopg.connect(_dsn(sidecar.ports.pgwire_port), autocommit=True) as conn:
        assert _rows(
            conn,
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'orders' ORDER BY ordinal_position",
        ) == [("id", "bigint"), ("customerName", "text"), ("asked", "text"), ("_nf_n", "bigint")]
        assert _rows(
            conn,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
            "ORDER BY 1",
        ) == [("broken",), ("orders",)]


def test_a_select_calls_the_function_on_the_local_worker(sidecar):
    with psycopg.connect(_dsn(sidecar.ports.pgwire_port), autocommit=True) as conn:
        assert _rows(
            conn,
            "SELECT id, asked FROM public.orders WHERE _nf_n = 5 AND \"customerName\" = 'acme' "
            "ORDER BY id DESC",
        ) == [(3, "n=5;filters=customerName eq acme"), (1, "n=5;filters=customerName eq acme")]
        # The function's own default, when the read gives no value for its parameter.
        assert _rows(conn, "SELECT count(*) FROM orders") == [(3,)]


def test_the_codes_failure_is_the_statements_error(sidecar):
    with psycopg.connect(_dsn(sidecar.ports.pgwire_port), autocommit=True) as conn:
        with pytest.raises(psycopg.Error, match="the warehouse is closed"):
            _rows(conn, "SELECT id FROM broken")
        # The connection still serves the next statement.
        assert _rows(conn, "SELECT count(*) FROM orders WHERE _nf_n = 2") == [(2,)]


def test_a_cursor_left_before_its_end_frees_the_workers_connection(sidecar, code):
    with psycopg.connect(_dsn(sidecar.ports.pgwire_port), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("START TRANSACTION")
            cur.execute("DECLARE c1 CURSOR FOR SELECT id FROM orders WHERE _nf_n = 50000")
            cur.execute("FETCH 2 FROM c1")
            assert cur.fetchall() == [(0,), (1,)]
            cur.execute("CLOSE c1")
            cur.execute("COMMIT")
    deadline = time.monotonic() + 10
    while code.worker.idle_since() is None:
        assert time.monotonic() < deadline, "the worker's connection is still leased"
        time.sleep(0.05)


def test_duckdb_attaches_the_sidecar_and_reads_a_table(sidecar):
    import duckdb

    db = duckdb.connect()
    db.execute("LOAD postgres")
    db.execute(f"ATTACH '{_dsn(sidecar.ports.pgwire_port)}' AS shop (TYPE postgres, READ_ONLY)")
    assert db.execute(
        'SELECT id, "customerName" FROM shop.public.orders WHERE _nf_n = 3 ORDER BY id'
    ).fetchall() == [(0, "zeta"), (1, "acme"), (2, "zeta")]


def test_the_lifecycle_starts_checks_health_and_stops_with_the_port_freed(code):
    port = _free_port()
    running = _sidecar(code, port)
    with pytest.raises(ServerLifecycleError):
        running.health()  # not started
    running.start()
    try:
        assert running.health()
        assert not _port_is_free(port)
        with pytest.raises(ServerLifecycleError):
            running.start()  # already running
    finally:
        running.stop()
    assert not running.alive()
    assert _port_is_free(port)
    running.stop()  # stopping a stopped sidecar does nothing
    # Started again, it listens on its port as before.
    running.start()
    try:
        with psycopg.connect(_dsn(port), autocommit=True) as conn:
            assert _rows(conn, "SELECT count(*) FROM orders WHERE _nf_n = 4") == [(4,)]
    finally:
        running.stop()
    assert _port_is_free(port)


def test_the_sidecar_ends_when_provisas_connections_to_it_close(code):
    running = _sidecar(code, _free_port())
    running.start()
    process = running._process
    assert process is not None
    for bridge in running._bridges:
        bridge.close()
    try:
        assert process.wait(timeout=10) != 0
    finally:
        running.stop()
    assert _port_is_free(running.ports.pgwire_port)


def test_a_sidecar_that_cannot_listen_is_refused_with_what_it_said(code):
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        running = _sidecar(code, port)
        with pytest.raises(pgwire_sidecar.ServerExited, match="Address already in use"):
            running.start()
    assert not running.alive()


def _prepared(code, *, content_hash: str = "h1", remote: dict | None = None):
    return SimpleNamespace(
        org_id="acme",
        source_id="shop-src",
        content_hash=content_hash,
        manifest=code.manifest,
        remote=remote,
        worker=lambda: code.worker,
    )


@pytest.fixture
def registry(monkeypatch, code):
    monkeypatch.setattr(pgwire_sidecar, "_bounds", lambda: BOUNDS)
    monkeypatch.setattr(pgwire_sidecar, "_spill_dir", lambda _org, _source: code.root / "spill")
    monkeypatch.setattr(pgwire_sidecar, "_PORTS", pgwire_sidecar.PortAllocator())
    monkeypatch.setattr(pgwire_sidecar, "_sidecars", {})
    yield
    pgwire_sidecar.stop_all_sidecars()


def test_a_dev_mode_sources_sidecar_is_started_once_and_stopped_when_it_is_forgotten(
    registry, code
):
    from provisa.codesource.source import forget

    first = pgwire_sidecar.ensure_sidecar(_prepared(code))  # type: ignore[arg-type]
    assert first.host == "127.0.0.1"
    held = pgwire_sidecar._sidecars[("acme", "shop-src")]
    assert pgwire_sidecar.ensure_sidecar(_prepared(code)) == first  # type: ignore[arg-type]
    assert pgwire_sidecar._sidecars[("acme", "shop-src")] is held
    with psycopg.connect(_dsn(first.port), autocommit=True) as conn:
        assert _rows(conn, "SELECT count(*) FROM orders WHERE _nf_n = 2") == [(2,)]
    # Its code changed: a sidecar of the new code, where the engine already dials.
    again = pgwire_sidecar.ensure_sidecar(_prepared(code, content_hash="h2"))  # type: ignore[arg-type]
    assert again == first
    assert pgwire_sidecar._sidecars[("acme", "shop-src")] is not held
    assert not held.alive()
    forget("shop-src")  # edited, switched to built, or deleted
    assert ("acme", "shop-src") not in pgwire_sidecar._sidecars
    assert _port_is_free(first.port)


def test_a_built_source_has_no_sidecar(registry, code):
    built = _prepared(code, remote={"host": "10.0.0.5", "port": 7443})
    with pytest.raises(ValueError, match="is built"):
        pgwire_sidecar.ensure_sidecar(built)  # type: ignore[arg-type]
    assert pgwire_sidecar._sidecars == {}
