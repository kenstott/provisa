# Copyright (c) 2026 Kenneth Stott
# Canary: c41e9a06-5d7b-4f83-b2a9-0e6f1d3c8b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The once-per-launch half of the boot runs once; every worker does its own half (REQ-1900).

``uvicorn --workers N`` used to run the whole boot in every worker, one at a time under the boot
lock: time-to-all-ready was N times one boot. The control plane now records which launch's
once-per-launch work is complete, so the first worker does it and the rest go straight to the
per-worker half — at the same time."""

# Requirements: REQ-1900

from __future__ import annotations

import os
import signal
import threading
import time
import uuid

import pytest
import sqlalchemy as sa

from provisa.core.boot_lock import boot_generation, control_plane_boot_lock
from tests.integration.worker_boot_harness import WorkerBoot, requests_served

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_USER = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_BASE = f"postgresql+psycopg://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}"
_ADMIN_URL = f"{_BASE}/{os.environ.get('PG_DATABASE', 'provisa')}"
_WORKERS = 4


@pytest.fixture
def fresh_database():
    name = f"boot_gen_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    try:
        yield f"{_BASE}/{name}"
    finally:
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def _boot_together(url: str, generations: list[str | None]) -> list[str]:
    """Run one simulated boot per generation, all released together; return who did the
    once-per-launch work."""
    start = threading.Barrier(len(generations))
    applied: list[str] = []
    failures: list[BaseException] = []

    def _boot(generation: str | None) -> None:
        try:
            start.wait(timeout=30)
            with control_plane_boot_lock(url) as lock:
                if not lock.completed("default", generation):
                    time.sleep(0.05)  # the once-per-launch work
                    applied.append(str(generation))
                    lock.mark_completed("default", generation)
        except BaseException as exc:  # collected and asserted on below
            failures.append(exc)

    threads = [threading.Thread(target=_boot, args=(g,)) for g in generations]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not failures, failures
    return applied


def test_workers_of_one_launch_do_the_once_work_once(fresh_database):
    generation = boot_generation("launch-a", config="c1", schema="s1")
    assert _boot_together(fresh_database, [generation] * 12) == [generation]
    # A worker that boots later in the same launch (a respawn) finds the work done.
    assert _boot_together(fresh_database, [generation]) == []


def test_a_new_launch_or_a_changed_config_is_a_new_generation(fresh_database):
    first = boot_generation("launch-a", config="c1", schema="s1")
    assert _boot_together(fresh_database, [first] * 3) == [first]
    relaunch = boot_generation("launch-b", config="c1", schema="s1")
    assert _boot_together(fresh_database, [relaunch] * 3) == [relaunch]
    reconfigured = boot_generation("launch-b", config="c2", schema="s1")
    assert _boot_together(fresh_database, [reconfigured] * 3) == [reconfigured]


def test_a_process_outside_a_launch_always_does_the_whole_boot(fresh_database):
    assert _boot_together(fresh_database, [None] * 3) == ["None"] * 3


def test_a_file_control_plane_records_no_generation(tmp_path):
    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    generation = boot_generation("launch-a", config="c1")
    with control_plane_boot_lock(url) as lock:
        assert not lock.completed("default", generation)
        lock.mark_completed("default", generation)
    with control_plane_boot_lock(url) as lock:
        assert not lock.completed("default", generation)


@pytest.fixture
def four_workers():
    boot = WorkerBoot(_WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _error_lines(log: str) -> list[str]:
    return [ln for ln in log.splitlines() if "ERROR" in ln or "Traceback" in ln]


def test_four_workers_boot_together_on_a_fresh_control_plane(four_workers):
    boot = four_workers
    log = boot.log_text()
    assert _error_lines(log) == []
    assert log.count("startup phase once-per-launch      applied") == 1
    assert log.count("startup phase once-per-launch      found complete") == _WORKERS - 1
    # Only the applying worker runs the control-plane DDL + seed phase; the rest skip it.
    seeded = [pid for pid, rows in boot.phases().items() if dict(rows).get("pg+schema+seed")]
    assert len(seeded) == 1
    health = boot.health()
    assert health is not None
    assert health["workers"] == {"ready": _WORKERS, "expected": _WORKERS}


def test_a_respawned_worker_skips_the_once_work(four_workers):
    boot = four_workers
    victim = sorted(boot.ready_pids())[0]
    os.kill(victim, signal.SIGKILL)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if len(boot.ready_pids()) == _WORKERS + 1:
            break
        time.sleep(0.2)
    else:
        raise AssertionError(f"no worker replaced {victim}:\n{boot.log_text()[-3000:]}")
    log = boot.log_text()
    assert log.count("startup phase once-per-launch      applied") == 1
    assert log.count("startup phase once-per-launch      found complete") == _WORKERS
    assert _error_lines(log) == []
    health = boot.health()
    assert health is not None
    assert health["workers"] == {"ready": _WORKERS, "expected": _WORKERS}


def test_every_worker_serves_flight_on_the_one_advertised_port(four_workers):
    """pyarrow's Flight server cannot share a port, so each worker binds the advertised port
    itself and relays to its own server on loopback: the advertised port reaches every worker,
    and no worker exposes a second Flight port."""
    boot = four_workers
    workers = set(boot.worker_pids())
    flight = boot.ports["flight"]
    assert boot.listeners(flight) == workers
    exposed = set(boot.ports.values())
    for pid in workers:
        assert boot.wildcard_listening_ports(pid) <= exposed, pid
    # Direct do_get with a ticket — the call Flight clients make — on the advertised port.
    assert requests_served("flight", flight, 50) == (50, "")
    # The airport Flight service (PROVISA_AIRPORT_PORT) is a second pyarrow server with the same
    # limitation and the same relay: before it, every worker but the first failed to bind it.
    airport = boot.ports["airport"]
    assert boot.listeners(airport) == workers
    assert requests_served("airport", airport, 20) == (20, "")
    assert _error_lines(boot.log_text()) == []


def _running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_stopping_a_launch_ends_its_workers_even_when_the_supervisor_was_killed():
    """A uvicorn worker does not exit when its supervisor dies. A stop that killed only the
    supervisor (what it did when the supervisor outlasted its wait) therefore left every worker
    running, retrying a database that was then dropped, for as long as the machine stayed up —
    and dialling a port later sessions were leased for something else. stop() ends the launch's
    whole process group."""
    boot = WorkerBoot(2, pg_host=_PG_HOST, pg_port=_PG_PORT)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        workers = boot.worker_pids()
        assert len(workers) == 2
        assert boot._proc is not None
        os.kill(boot._proc.pid, signal.SIGKILL)  # the supervisor alone
        boot._proc.wait(timeout=20)
        assert [pid for pid in workers if _running(pid)] == workers  # they outlive it

        boot.stop()

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and any(_running(pid) for pid in workers):
            time.sleep(0.1)
        assert [pid for pid in workers if _running(pid)] == []
    finally:
        boot.cleanup()


def _rows_on_fresh_connections(boot, n: int) -> list[int]:
    """Row counts of a GraphQL query with no limit, run as a role the default row limit applies
    to (no ``full_results``), each over a NEW connection so the requests are spread over the
    workers."""
    import json
    import urllib.request

    counts = []
    for _ in range(n):
        req = urllib.request.Request(
            f"http://127.0.0.1:{boot.ports['http']}/data/graphql",
            data=json.dumps({"query": "{ s__orders { id } }"}).encode(),
            headers={
                "Content-Type": "application/json",
                "Connection": "close",
                "x-provisa-role": "analyst",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            counts.append(len(json.loads(resp.read())["data"]["s__orders"]))
    return counts


def test_a_setting_changed_on_one_worker_holds_on_every_worker_and_after_a_restart(four_workers):
    import json
    import urllib.request

    boot = four_workers
    assert set(_rows_on_fresh_connections(boot, 8)) == {2}  # the table's two rows, no cap hit

    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/admin/settings",
        data=json.dumps({"limits": {"default_row_limit": 1}}).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        assert json.loads(resp.read())["updated"] == ["limits.default_row_limit"]

    time.sleep(6)  # past every worker's snapshot TTL (5s)
    assert set(_rows_on_fresh_connections(boot, 40)) == {1}

    # A new launch on the same control plane keeps the setting.
    boot.stop()
    again = WorkerBoot(
        _WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT, database=boot.database, data_dir=boot.data_dir
    )
    try:
        again.start()
        again.wait_all_ready(timeout=300)
        assert set(_rows_on_fresh_connections(again, 20)) == {1}
    finally:
        again.stop()


def _put_settings(boot, body: dict) -> dict:
    import json
    import urllib.request

    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/admin/settings",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _get_limits(boot) -> dict:
    import json
    import urllib.request

    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/admin/settings", headers={"Connection": "close"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["limits"]


def test_the_request_timeout_is_each_transports_own():
    """REQ-1905. On one server with a 6M-row table:

    * as shipped, Flight has 3600 s and pgwire 300 s; everything else the 60 s default;
    * with the DEFAULT lowered to 2 s, a GraphQL scan is cut at 2 s naming graphql and the
      setting, while the same scan over Flight — which has its own value — completes;
    * with Flight's own value set to 2 s, the Flight scan is cut at 2 s naming Flight's setting,
      and the server keeps serving."""
    import json
    import urllib.error
    import urllib.request

    import pyarrow.flight as fl

    from tests.integration.worker_boot_harness import _config

    orders = _config(_PG_HOST, _PG_PORT, "unused")["tables"][0]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={"tables": [orders, {**orders, "table": "big"}]},
        # No large-result redirect: this test is about a query that runs past its timeout, not
        # about where a large result is delivered.
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.big AS SELECT g AS id, 'region-' || (g % 50) AS region "
                    "FROM generate_series(1, 6000000) g"
                )
            )
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)

        shipped = _get_limits(boot)
        assert shipped["request_timeout"] == 60.0
        assert shipped["request_timeouts"]["flight"] == 3600.0
        assert shipped["request_timeouts"]["pgwire"] == 300.0
        assert shipped["request_timeouts"]["graphql"] is None

        client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
        try:

            def _flight(sql: str):
                ticket = fl.Ticket(json.dumps({"query": sql, "role": "org_admin"}).encode())
                return client.do_get(ticket).read_all()

            def _graphql_big() -> tuple[int, str, float]:
                req = urllib.request.Request(
                    f"http://127.0.0.1:{boot.ports['http']}/data/graphql",
                    data=json.dumps({"query": "{ s__big { id region } }"}).encode(),
                    headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
                )
                started = time.monotonic()
                try:
                    with urllib.request.urlopen(req, timeout=120) as resp:
                        return resp.status, resp.read()[:200].decode(), time.monotonic() - started
                except urllib.error.HTTPError as exc:
                    return exc.code, exc.read().decode(), time.monotonic() - started

            # The default at 2 s: GraphQL is cut, Flight (its own 3600 s) is not.
            assert _put_settings(boot, {"limits": {"request_timeout": 2}})["updated"] == [
                "limits.request_timeout"
            ]
            status, body, elapsed = _graphql_big()
            assert status == 504, (status, f"{elapsed:.1f}s", body)
            assert "graphql" in body and "limits.request_timeout" in body, body
            assert 1.5 < elapsed < 8.0
            assert _flight("SELECT id, region FROM sales.big").num_rows == 6_000_000

            # Flight's own value at 2 s: the Flight scan is cut, naming Flight's setting.
            _put_settings(boot, {"limits": {"request_timeouts": {"flight": 2}}})
            started = time.monotonic()
            with pytest.raises(fl.FlightServerError) as raised:
                _flight("SELECT id, region FROM sales.big")
            elapsed = time.monotonic() - started
            assert "flight request exceeded its 2s request deadline" in str(raised.value)
            assert "limits.request_timeouts.flight" in str(raised.value)
            assert 1.5 < elapsed < 8.0

            # The server is still serving, and the cut-off stream gave its slot back.
            assert _flight("SELECT id, region FROM sales.orders").num_rows == 2
        finally:
            client.close()
    finally:
        boot.cleanup()


def test_a_transports_timeout_set_on_one_worker_holds_on_every_worker_and_after_a_restart(
    four_workers,
):
    boot = four_workers
    _put_settings(
        boot, {"limits": {"request_timeout": 45, "request_timeouts": {"grpc": 7, "pgwire": None}}}
    )
    time.sleep(6)  # past every worker's snapshot TTL (5s)
    seen = [_get_limits(boot) for _ in range(40)]  # fresh connections: spread over the workers
    assert {s["request_timeout"] for s in seen} == {45.0}
    assert {s["request_timeouts"]["grpc"] for s in seen} == {7.0}
    assert {s["request_timeouts"]["pgwire"] for s in seen} == {300.0}  # cleared: what shipped
    assert {s["request_timeouts"]["flight"] for s in seen} == {3600.0}

    boot.stop()
    again = WorkerBoot(
        _WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT, database=boot.database, data_dir=boot.data_dir
    )
    try:
        again.start()
        again.wait_all_ready(timeout=300)
        limits_ = _get_limits(again)
        assert limits_["request_timeout"] == 45.0
        assert limits_["request_timeouts"]["grpc"] == 7.0
    finally:
        again.stop()


def test_each_worker_serves_http_on_its_own_socket_on_the_public_port():
    """REQ-1900: launched as for several workers on Linux — uvicorn's supervisor on a unix socket,
    each worker on its own SO_REUSEPORT socket. Every worker listens on the public port, /health
    still reports the roll call, queries are answered, and shutdown is clean. (How connections
    then spread is the kernel's: tests/integration/http_listener_spread_check.py measures it on
    Linux; darwin hands them all to one listener.)"""
    boot = WorkerBoot(_WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT, per_worker_http=True)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        assert boot.listeners(boot.ports["http"]) == set(boot.worker_pids())
        health = boot.health()
        assert health is not None
        assert health["workers"] == {"ready": _WORKERS, "expected": _WORKERS}
        assert set(_rows_on_fresh_connections(boot, 20)) == {2}
        assert requests_served("http", boot.ports["http"], 50) == (50, "")
        boot.stop()
        assert _error_lines(boot.log_text()) == []
        assert boot.listeners(boot.ports["http"]) == set()
    finally:
        boot.cleanup()
