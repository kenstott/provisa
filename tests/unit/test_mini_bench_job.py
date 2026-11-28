# Copyright (c) 2026 Kenneth Stott
# Canary: be0534bf-6107-4b2c-9cf4-1b9ca2afbe67
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The mini benchmark's step plan (demo/named/perf/bench/mini.sh, mini-job.sh).

mini.sh is run with a stub spot-bench.sh and gcloud, so no VM is made; the job it would send is
then run with a stub step.sh that records each step and runs only the compare step, against
reports written here, with the real bench_sanity.py.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_BENCH = _REPO / "demo" / "named" / "perf" / "bench"

# Records "NAME ARGS" per step in ~/plan; runs only the compare step, whose rc it keeps.
_STEP_SH = """OUT=~/out; mkdir -p "$OUT"
step() {
  local name=$1; shift
  echo "$name $*" >> ~/plan
  rc=0
  if [ "$name" = compare ]; then ( "$@" ) > "$OUT/$name.log" 2>&1; rc=$?; fi
  [ "$name" = setup ] && rc=${SETUP_RC:-0}
  echo "$name rc=$rc end" >> "$OUT/rc"
}
finish() { echo finish >> ~/plan; }
"""


def _send(tmp: Path, *args: str) -> Path:
    """The job mini.sh would send to the VM, from a run with stubbed spot-bench.sh and gcloud."""
    stubs = tmp / "stubs"
    stubs.mkdir()
    spot = stubs / "spot-bench.sh"
    spot.write_text(
        '#!/bin/bash\nwhile [ $# -gt 0 ]; do [ "$1" = --script ] && cp "$2" "$SENT"; shift; done\n'
    )
    gcloud = stubs / "gcloud"
    gcloud.write_text("#!/bin/bash\necho us-east1-b\n")
    for f in (spot, gcloud):
        f.chmod(0o755)
    sent = tmp / "sent-job.sh"
    env = {
        **os.environ,
        "PATH": f"{stubs}:{os.environ['PATH']}",
        "SPOT_BENCH": str(spot),
        "SENT": str(sent),
        "OUT": str(tmp / "out"),
    }
    subprocess.run(
        ["bash", str(_BENCH / "mini.sh"), *args], env=env, check=True, capture_output=True
    )
    return sent


def _report(home: Path, run: str, engine: str, p50: float) -> None:
    d = home / "out" / run / engine
    d.mkdir(parents=True)
    report = {
        "engine": engine,
        "host": {"instance_id": "42", "cpu_platform": "Intel Cascade Lake"},
        "results": [
            {
                "query_id": "point_lookup",
                "transport": "http",
                "dialect": "sql",
                "cache_pass": "nocache",
                "latency_ms_p50": p50,
            }
        ],
    }
    (d / "20261127-120000.json").write_text(json.dumps(report))


def _run_job(tmp: Path, job: Path, **env: str) -> tuple[list[str], Path]:
    home = tmp / "home"
    (home / "src" / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
    (home / "src" / ".venv" / "bin" / "python").symlink_to(sys.executable)
    (home / "src" / "demo" / "named" / "perf").mkdir(parents=True)
    (home / "src" / "demo" / "named" / "perf" / "bench").symlink_to(_BENCH)
    (home / "step.sh").write_text(_STEP_SH)
    subprocess.run(
        ["bash", str(job)],
        env={**os.environ, "HOME": str(home), "REF": "cafe", **env},
        check=True,
        capture_output=True,
    )
    plan = (home / "plan").read_text().replace(str(home), "~").splitlines()
    return plan, home


def test_without_base_the_plan_is_unchanged(tmp_path: Path) -> None:
    job = _send(tmp_path, "--rounds", "2")
    assert job.read_text().splitlines()[:3] == ["ROUNDS=2", "BASE_REF=''", "STAGE_SPANS=''"]
    plan, _ = _run_job(tmp_path, job)
    assert plan == [
        "setup setup",
        "round1 round ~/src round1",
        "round2 round ~/src round2",
        "finish",
    ]


def test_base_rounds_alternate_with_candidate_rounds_then_compare(tmp_path: Path) -> None:
    job = _send(tmp_path, "--base", "v0.1.0-alpha.512", "--rounds", "2")
    lines = job.read_text().splitlines()
    assert lines[:3] == ["ROUNDS=2", "BASE_REF=v0.1.0-alpha.512", "STAGE_SPANS=''"]
    assert "\n".join(lines[3:]) + "\n" == (_BENCH / "mini-job.sh").read_text()
    home = tmp_path / "home"
    for r, (base_ms, cand_ms) in enumerate([(10.0, 11.0), (10.0, 25.0)], start=1):
        for engine in ("duckdb", "pg", "trino"):
            _report(home, f"base{r}", engine, base_ms)
            _report(home, f"round{r}", engine, cand_ms)
    plan, home = _run_job(tmp_path, job)
    assert plan == [
        "setup setup",
        "base1 round ~/src-base base1",
        "round1 round ~/src round1",
        "base2 round ~/src-base base2",
        "round2 round ~/src round2",
        "compare compare",
        "finish",
    ]
    assert "compare rc=0" in (home / "out" / "rc").read_text()
    for engine in ("duckdb", "pg", "trino"):
        first = json.loads((home / "out/compare/round1" / f"{engine}-vs-base.json").read_text())
        second = json.loads((home / "out/compare/round2" / f"{engine}-vs-base.json").read_text())
        assert first["comparison"] == second["comparison"] == "same VM"
        assert [f["kind"] for f in first["flags"]] == []
        assert [(f["kind"], f["ratio"]) for f in second["flags"]] == [("over_2x", 2.5)]


def test_stage_spans_reaches_the_job_header(tmp_path: Path) -> None:
    job = _send(tmp_path, "--rounds", "1", "--stage-spans")
    assert job.read_text().splitlines()[:3] == ["ROUNDS=1", "BASE_REF=''", "STAGE_SPANS=1"]


def test_compare_fails_when_a_round_wrote_no_report(tmp_path: Path) -> None:
    job = _send(tmp_path, "--base", "v0.1.0-alpha.512", "--rounds", "1")
    home = tmp_path / "home"
    for engine in ("duckdb", "pg"):
        _report(home, "base1", engine, 10.0)
        _report(home, "round1", engine, 10.0)
    _report(home, "base1", "trino", 10.0)
    _, home = _run_job(tmp_path, job)
    assert "compare rc=1" in (home / "out" / "rc").read_text()
    assert "round1 trino: no report" in (home / "out" / "compare.log").read_text()
    assert (home / "out/compare/round1/pg-vs-base.json").exists()


def test_failed_setup_runs_no_round(tmp_path: Path) -> None:
    job = _send(tmp_path, "--base", "v0.1.0-alpha.512", "--rounds", "2")
    plan, _ = _run_job(tmp_path, job, SETUP_RC="1")
    assert plan == ["setup setup", "finish"]
