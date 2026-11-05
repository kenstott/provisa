# Copyright (c) 2026 Kenneth Stott
# Canary: 3e396c16-b939-4b08-93a2-41fb38b2b5a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the soak's samples of the server, read from /proc, the leak probe and Redis INFO, and
the report that fits each series' slope after the warm-up and flags a series that keeps rising."""

# Requirements: REQ-1911

from __future__ import annotations

import sys
from pathlib import Path

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import process_sample  # noqa: E402
import soak  # noqa: E402


def _proc(root: Path) -> Path:
    # 10 is the server, 11 its worker; 20 is another process
    for pid, kids, rss_kb, threads, fds in (
        (10, [11], 1000, 4, ["pipe:[1]", "socket:[7]", "/var/log/x"]),
        (11, [], 3000, 9, ["socket:[8]", "socket:[9]"]),
        (20, [], 99999, 99, ["socket:[1]"]),
    ):
        task = root / str(pid) / "task" / str(pid)
        task.mkdir(parents=True)
        (task / "children").write_text(" ".join(map(str, kids)))
        (root / str(pid) / "status").write_text(
            f"Name:\tx\nVmRSS:\t   {rss_kb} kB\nThreads:\t{threads}\n"
        )
        fd = root / str(pid) / "fd"
        fd.mkdir()
        for i, target in enumerate(fds):
            (fd / str(i)).symlink_to(target)
    return root


def test_a_sample_is_the_process_trees_memory_fds_sockets_threads_and_spill_size(tmp_path):
    spill = tmp_path / "spill"
    (spill / "q1").mkdir(parents=True)
    (spill / "q1" / "part").write_bytes(b"x" * 300)
    (spill / "top").write_bytes(b"x" * 20)
    got = process_sample.tree(10, [spill], _proc(tmp_path / "proc"))
    assert got == {
        "processes": 2,
        "rss_kb": 4000,
        "threads": 13,
        "fds": 5,
        "sockets": 3,
        "spill_bytes": {str(spill): 320},
    }


def test_a_host_without_proc_reports_why_never_a_figure(tmp_path):
    got = process_sample.tree(10, [], tmp_path / "no-proc")
    assert set(got) == {"error"} and "FileNotFoundError" in got["error"]


def test_redis_info_is_read_into_its_memory_and_keys():
    text = "# Memory\r\nused_memory:2048\r\nused_memory_rss:4096\r\n# Keyspace\r\n"
    text += "db0:keys=12,expires=10,avg_ttl=0\r\ndb1:keys=3,expires=0,avg_ttl=0\r\n"
    assert process_sample.parse_redis_info(text) == {
        "used_memory": 2048,
        "used_memory_rss": 4096,
        "keys": 15,
    }


def test_a_sample_is_flattened_into_one_number_per_series():
    sample = {
        "process": {"rss_kb": 10, "spill_bytes": {"/s": 5}},
        "probe": {
            "tasks": 3,
            "tracing": True,
            "org_maps": {"m.f": 2},
            "maps": {"n._C": 1},
            "pools": [{"name": "pg", "open": 2, "checked_out": 1, "maxsize": 4}],
            "grew": [{"site": "a.py:1", "size_diff": 9}],
        },
        "redis": {"keys": 7},
    }
    assert soak.flatten(sample) == {
        "process.rss_kb": 10,
        "process.spill_bytes./s": 5,
        "probe.tasks": 3,
        "probe.org_maps.m.f": 2,
        "probe.maps.n._C": 1,
        "probe.pools.pg.open": 2,
        "probe.pools.pg.checked_out": 1,
        "redis.keys": 7,
    }


def test_a_series_that_rises_in_each_third_after_the_warm_up_is_flagged():
    t = list(range(0, 3600, 30))
    rising = [100 + 2 * s / 30 for s in t]
    level = [100 + (5 if i % 2 else -5) for i in range(len(t))]
    # rose during warm-up, flat after: not flagged
    warmed = [100 + min(s, 600) for s in t]
    samples = [
        {"t": s, "series": {"rss": r, "fds": f, "tasks": w}}
        for s, r, f, w in zip(t, rising, level, warmed, strict=True)
    ]
    report = soak.report(samples, warmup_s=600)
    assert report["rising"] == ["rss"]
    assert report["series"]["rss"]["slope_per_hour"] == pytest.approx(240.0)
    assert report["series"]["tasks"]["slope_per_hour"] == pytest.approx(0.0)
    assert report["series"]["fds"]["thirds"][0] == pytest.approx(100, abs=1)


def test_a_report_without_samples_after_the_warm_up_is_refused():
    with pytest.raises(ValueError, match="after the warm-up"):
        soak.report([{"t": 0, "series": {"rss": 1}}], warmup_s=600)


def test_the_agent_answers_the_tree_the_servers_probe_and_redis_in_one_sample():
    import json
    import os
    import socket
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    import host_agent
    import httpx

    class _Probe(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            body = json.dumps({"tasks": 4, "maps": {"m._C": 2}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: str) -> None:
            return

    redis = socket.create_server(("127.0.0.1", 0))

    def answer_info() -> None:
        conn, _ = redis.accept()
        with conn:
            assert conn.recv(64) == b"INFO\r\n"
            text = b"used_memory:10\r\nused_memory_rss:20\r\ndb0:keys=5,expires=0\r\n"
            conn.sendall(b"$%d\r\n%s\r\n" % (len(text), text))

    probe = ThreadingHTTPServer(("127.0.0.1", 0), _Probe)
    agent = host_agent.serve(
        ("127.0.0.1", 0),
        pid=os.getpid(),
        facts={},
        rtt_targets={},
        leak_probe_url=f"http://127.0.0.1:{probe.server_address[1]}/debug/leak-probe",
        redis=redis.getsockname(),
    )
    threads = [
        threading.Thread(target=probe.serve_forever, daemon=True),
        threading.Thread(target=agent.serve_forever, daemon=True),
        threading.Thread(target=answer_info, daemon=True),
    ]
    for t in threads:
        t.start()
    try:
        got = httpx.get(f"http://127.0.0.1:{agent.server_address[1]}/process", timeout=30).json()
    finally:
        agent.shutdown()
        probe.shutdown()
        redis.close()
    assert got["probe"] == {"tasks": 4, "maps": {"m._C": 2}}
    assert got["redis"] == {"used_memory": 10, "used_memory_rss": 20, "keys": 5}
    # /proc is Linux's: elsewhere the tree says why it has no figures
    assert set(got["process"]) in ({"error"}, {*process_sample.tree(os.getpid(), [])})
    assert soak.flatten(got)["probe.maps.m._C"] == 2
