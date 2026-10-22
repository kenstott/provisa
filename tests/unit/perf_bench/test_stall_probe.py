# Copyright (c) 2026 Kenneth Stott
# Canary: 00a73cae-4a28-4834-be91-1660c836ead5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: when the server and the client stalled, each record with its wall-clock end, so a
slow request is put beside what was happening on both sides at the time."""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import host_agent  # noqa: E402
import stall_probe  # noqa: E402


def _until(records: list, kind: str, timeout: float = 5.0) -> list:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = [r for r in records if r["kind"] == kind]
        if found:
            return found
        time.sleep(0.05)
    return []


def test_a_process_holding_the_gil_is_recorded_as_a_stall() -> None:
    records: list = []
    stall_probe.install(records.append)
    values = [random.random() for _ in range(3_000_000)]
    started = time.time()
    values.sort()  # one C call: the GIL is held throughout
    [stall] = _until(records, "stall")[:1]
    assert stall["t"] >= started and stall["ms"] >= stall_probe.LATE_S * 1000
    assert stall["pid"] == os.getpid()


def test_a_long_collection_is_recorded_with_its_generation() -> None:
    records: list = []
    stall_probe.install(records.append)
    import gc

    held = [[i] for i in range(2_000_000)]
    gc.collect()
    del held
    collections = _until(records, "gc")
    assert any(r["gen"] == 2 and r["ms"] >= stall_probe.GC_S * 1000 for r in collections)


def test_a_server_process_writes_its_records_and_the_agent_serves_a_window(tmp_path) -> None:
    """The server's processes load the probe from stallprobe/ on their PYTHONPATH."""
    started = time.time()
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import gc; held = [[i] for i in range(2_000_000)]; gc.collect()",
        ],
        env={
            **os.environ,
            "PYTHONPATH": str(BENCH / "stallprobe"),
            "PROVISA_BENCH_STALL_DIR": str(tmp_path),
        },
        check=True,
    )
    [written] = tmp_path.glob("stall-*.jsonl")
    records = [json.loads(line) for line in written.read_text().splitlines()]
    assert any(r["kind"] == "gc" and r["gen"] == 2 for r in records)

    server = host_agent.serve(
        ("127.0.0.1", 0), pid=None, facts={}, rtt_targets={}, stall_dir=tmp_path
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/stalls"
        inside = httpx.get(url, params={"since": started, "until": time.time()}).json()
        assert inside == sorted(records, key=lambda r: r["t"])
        before = httpx.get(url, params={"since": 0, "until": started - 1}).json()
        assert before == []
    finally:
        server.shutdown()
        server.server_close()
