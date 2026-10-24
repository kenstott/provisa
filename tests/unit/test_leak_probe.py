# Copyright (c) 2026 Kenneth Stott
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
