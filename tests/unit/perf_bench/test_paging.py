# Copyright (c) 2026 Kenneth Stott
# Canary: 05bee0e4-9415-4037-a37a-976f24f5afad
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2092: a step's paging (major faults, swap in and out, each process class's major faults,
available memory at both ends) read from /proc, beside its CPU per request."""

# Requirements: REQ-2092

from __future__ import annotations

import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import optimistic_load as ol  # noqa: E402
import paging  # noqa: E402


def _stat(pid: int, name: str, majflt: int, cmajflt: int) -> str:
    # fields 1-2, then 3 (state) .. 13 (cmajflt): ppid pgrp session tty tpgid flags minflt cminflt
    return f"{pid} ({name}) S 1 1 1 0 -1 4194304 100 0 {majflt} {cmajflt} 5 6\n"


def _proc(
    root: Path, *, pgmajfault: int, pswpin: int, pswpout: int, avail_kb: int, faults: dict
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "vmstat").write_text(
        f"pgpgin 1\npgmajfault {pgmajfault}\npswpin {pswpin}\npswpout {pswpout}\n"
    )
    (root / "meminfo").write_text(f"MemTotal: 4000000 kB\nMemAvailable: {avail_kb} kB\n")
    # 10 is the server, 11 and 12 its workers (12 a child of 11); 20 is another process
    tree = {10: [11], 11: [12], 12: [], 20: []}
    for pid, kids in tree.items():
        task = root / str(pid) / "task" / str(pid)
        task.mkdir(parents=True, exist_ok=True)
        (task / "children").write_text(" ".join(map(str, kids)))
        majflt, cmajflt = faults[pid]
        (root / str(pid) / "stat").write_text(_stat(pid, "a name) with (spaces", majflt, cmajflt))
    return root


def test_a_step_s_paging_is_the_counters_over_it_and_the_memory_at_both_ends(
    tmp_path: Path,
) -> None:
    classes = {"provisa": 10, "load-generator": 20}
    before = paging.snapshot(
        classes,
        _proc(
            tmp_path / "a",
            pgmajfault=100,
            pswpin=4,
            pswpout=9,
            avail_kb=2048000,
            faults={10: (1, 2), 11: (3, 0), 12: (5, 0), 20: (0, 0)},
        ),
    )
    after = paging.snapshot(
        classes,
        _proc(
            tmp_path / "b",
            pgmajfault=160,
            pswpin=4,
            pswpout=30,
            avail_kb=1024000,
            faults={10: (1, 2), 11: (13, 0), 12: (25, 0), 20: (4, 0)},
        ),
    )
    # the server's class is it, its workers and their workers, with what each has reaped
    assert before["majflt"] == {"provisa": 11, "load-generator": 0}
    assert paging.delta(before, after) == {
        "pgmajfault": 60,
        "pswpin": 0,
        "pswpout": 21,
        "majflt": {"provisa": 30, "load-generator": 4},
        "mem_available_mb_start": 2000,
        "mem_available_mb_end": 1000,
    }


def test_a_host_without_proc_says_why_and_reports_no_figure(tmp_path: Path) -> None:
    missing = paging.snapshot({"provisa": 10}, tmp_path / "no-proc")
    assert set(missing) == {"error"} and "vmstat" in missing["error"]
    read = {"pgmajfault": 0, "pswpin": 0, "pswpout": 0, "mem_available_kb": 0, "majflt": {}}
    assert paging.delta(missing, read) == {"error": missing["error"]}
    assert paging.delta(read, missing) == {"error": missing["error"]}


def test_the_step_line_shows_each_host_s_paging() -> None:
    text = ol._paging_text(
        {
            "client": {"error": "FileNotFoundError: /proc/vmstat"},
            "server": {
                "pgmajfault": 60,
                "pswpin": 0,
                "pswpout": 21,
                "majflt": {"provisa": 30},
                "mem_available_mb_start": 2000,
                "mem_available_mb_end": 1000,
            },
        }
    )
    assert text == (
        "paging[client: paging not read (FileNotFoundError: /proc/vmstat); "
        "server: majflt=60 (provisa=30) swap_in=0 swap_out=21 avail=2000->1000MB]"
    )


def test_a_step_reads_each_device_s_paging(monkeypatch) -> None:
    seen: list[str] = []

    class _Resp:
        def __init__(self, url: str) -> None:
            seen.append(url)

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"error": "remote"}

    import httpx

    monkeypatch.setattr(httpx, "get", lambda url, timeout: _Resp(url))
    monkeypatch.setattr(paging, "snapshot", lambda classes: {"classes": dict(classes)})
    out = ol._paging_snapshots(4321, (("server", "http://s:9555"), ("backends", "http://b:9555")))
    assert seen == ["http://s:9555/paging", "http://b:9555/paging"]
    assert out["client"]["classes"]["provisa"] == 4321
    assert set(out["client"]["classes"]) == {"load-generator", "provisa"}
    assert out["server"] == out["backends"] == {"error": "remote"}
    # a server on another host is read through its agent, not as a local class
    remote = ol._paging_snapshots("http://s:9555/cpu", ())
    assert set(remote) == {"client"} and set(remote["client"]["classes"]) == {"load-generator"}
