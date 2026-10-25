# Copyright (c) 2026 Kenneth Stott
# Canary: 25c97f45-eba4-45e4-885b-a09da287b615
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2149: the leak test reads the process through /debug/leak-probe, which exists only when
PROVISA_LEAK_PROBE=1 -- no deployment sets it, and without it the route is not there at all."""

# Requirements: REQ-2149

from __future__ import annotations

import tracemalloc

from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api import leak_probe
from provisa.core import org_forget


def _app() -> FastAPI:
    app = FastAPI()
    leak_probe.mount(app)
    return app


def test_without_the_flag_there_is_no_probe(monkeypatch):
    monkeypatch.delenv(leak_probe.ENV, raising=False)
    assert TestClient(_app()).get("/debug/leak-probe").status_code == 404
    monkeypatch.setenv(leak_probe.ENV, "true")  # only "1" mounts it
    assert TestClient(_app()).get("/debug/leak-probe").status_code == 404


def test_the_app_mounts_the_probe_only_through_the_flag():
    from pathlib import Path

    source = Path(leak_probe.__file__).with_name("app.py").read_text()
    assert "_mount_leak_probe(app)" in source
    assert "/debug/leak-probe" not in source  # the route is the probe module's alone


def test_with_the_flag_it_answers_the_processes_counts_and_what_grew(monkeypatch):
    monkeypatch.setenv(leak_probe.ENV, "1")
    kept: dict[str, int] = {}
    monkeypatch.setattr(org_forget, "_sizes", {"m._forget": lambda: len(kept)})
    client = TestClient(_app())
    tracemalloc.start(5)
    try:
        first = client.get("/debug/leak-probe").json()
        kept["acme"] = 1
        second = client.get("/debug/leak-probe").json()
    finally:
        tracemalloc.stop()
    assert first["org_maps"] == {"m._forget": 0} and second["org_maps"] == {"m._forget": 1}
    assert first["tracing"] and first["grew"] == []  # nothing to compare the first snapshot with
    assert {"tasks", "loops", "threads"} <= set(second)
    assert all(g["size_diff"] > 0 for g in second["grew"]) and second["traced_bytes"] > 0


def test_it_answers_each_process_wide_maps_entries_and_each_pools_connections(monkeypatch):
    from provisa.core import held_sizes
    from provisa.core.sync_pool import BlockingPool

    monkeypatch.setenv(leak_probe.ENV, "1")
    roles: dict[str, int] = {"analyst": 1}
    monkeypatch.setattr(held_sizes, "_sizes", {"nl.schema_matcher": lambda: len(roles)})
    pool = BlockingPool(object, lambda conn: None, minsize=2, maxsize=4, wait_s=1, name="pg-a")
    taken = pool.getconn()
    answer = TestClient(_app()).get("/debug/leak-probe").json()
    pool.putconn(taken)
    pool.closeall()
    assert answer["maps"] == {"nl.schema_matcher": 1}
    assert {"name": "pg-a", "open": 2, "checked_out": 1, "maxsize": 4} in answer["pools"]


def test_the_process_wide_maps_are_registered_at_their_modules_import():
    from provisa.core import held_sizes
    import provisa.core.egress  # noqa: F401
    import provisa.discovery.catalog_cache  # noqa: F401
    import provisa.federation.pgwire_replica  # noqa: F401
    import provisa.nl.schema_matcher  # noqa: F401

    assert {
        "provisa.core.egress._PENDING",
        "provisa.discovery.catalog_cache._COLUMN_FILLS",
        "provisa.federation.pgwire_replica._ENDPOINTS",
        "provisa.federation.pgwire_replica._ENDPOINT_USERS",
        "provisa.nl.schema_matcher._CACHE",
    } <= set(held_sizes.sizes())


def test_it_answers_live_threads_by_name_without_a_workers_number(monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setenv(leak_probe.ENV, "1")
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="probe-test") as pool:
        started = threading.Barrier(3)
        for _ in range(2):
            pool.submit(started.wait)
        started.wait()
        answer = TestClient(_app()).get("/debug/leak-probe").json()
    assert answer["thread_names"]["probe-test"] == 2
    assert sum(answer["thread_names"].values()) == answer["threads"]


def test_a_named_background_thread_keeps_its_name(monkeypatch):
    import threading

    monkeypatch.setenv(leak_probe.ENV, "1")
    release = threading.Event()
    named = threading.Thread(target=release.wait, name="provisa-bg:probe-test-task", daemon=True)
    named.start()
    try:
        answer = TestClient(_app()).get("/debug/leak-probe").json()
    finally:
        release.set()
        named.join()
    assert answer["thread_names"]["provisa-bg:probe-test-task"] == 1


def test_with_stacks_it_answers_what_each_thread_is_waiting_on(monkeypatch):
    import threading

    monkeypatch.setenv(leak_probe.ENV, "1")
    release = threading.Event()
    waiting = threading.Thread(target=release.wait, name="probe-stack-test", daemon=True)
    waiting.start()
    try:
        answer = TestClient(_app()).get("/debug/leak-probe", params={"stacks": "true"}).json()
    finally:
        release.set()
        waiting.join()
    mine = [t for t in answer["stacks"] if t["name"] == "probe-stack-test"]
    assert len(mine) == 1 and "wait" in mine[0]["stack"][-1]
