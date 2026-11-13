# Copyright (c) 2026 Kenneth Stott
# Canary: e0187e0b-a6d1-40fd-a031-bd5ce45b88bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fold a suite run's measured file times into lane_file_seconds.json, which plans the core lane's
shards (run_lane.py planned_shards).

Each planned lane shard uploads, in its lane-<name> artifact, its junit.xml and beside it
shard_files.json (run_lane.py write_shard_files): the files it ran and whether its budget let it
start every test. Download a run's and fold them in:

    gh run download <run id> -p 'lane-core*' -D /path/to/artifacts
    python3 scripts/ci/lane_durations.py fold /path/to/artifacts

A file's seconds are the sum of its test cases' junit times (setup, call and teardown: a session
stack booted by its first test counts to that file). Each file a shard measured takes the new
value; a file a complete shard ran with no test case of the lane takes 0; a file of a shard its
budget stopped, never reached, keeps what it had; a file no longer on disk is dropped. Commit the
result: the next plan reads it, and its unit tests (test_ci_core_shards.py) check it still fits.
"""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import run_lane  # noqa: E402 - beside this script

REPO = run_lane.REPO


def _junit_file(case: ET.Element) -> str:
    """The test file of a junit test case: its classname (``tests.integration.test_x.TestY``), or
    for a collection error, which has none, its name."""
    dotted = case.get("classname") or case.get("name", "")
    parts = dotted.split(".")
    for end in range(len(parts), 0, -1):
        candidate = "/".join(parts[:end]) + ".py"
        if (REPO / candidate).is_file():
            return candidate
    raise ValueError(f"no test file for junit case {dotted!r}")


def shard_seconds(junit: Path) -> dict[str, float]:
    seconds: defaultdict[str, float] = defaultdict(float)
    for case in ET.parse(junit).getroot().iter("testcase"):
        seconds[_junit_file(case)] += float(case.get("time", 0))
    return dict(seconds)


def fold(recorded: dict[str, dict[str, float]], dirs: list[Path]) -> dict[str, dict[str, float]]:
    """``recorded`` with every shard under ``dirs`` folded in."""
    out = {lane: dict(files) for lane, files in recorded.items()}
    found = sorted(p for d in dirs for p in d.rglob(run_lane.SHARD_FILES))
    if not found:
        raise ValueError(f"no {run_lane.SHARD_FILES} under {', '.join(map(str, dirs))}")
    for record_path in found:
        record = json.loads(record_path.read_text())
        junit = record_path.with_name("junit.xml")
        if not junit.is_file():
            raise ValueError(f"{record_path}: no junit.xml beside it")
        lane = out.setdefault(run_lane.MEASURED_AS[record["lane"]], {})
        measured = shard_seconds(junit)
        for f, s in measured.items():
            lane[f] = round(s, 1)
        if record["complete"]:
            for f in record["files"]:
                if f not in measured:
                    lane[f] = 0.0
    # A file since deleted or renamed is dropped: its successor is measured on its first run.
    return {
        lane: {f: s for f, s in sorted(files.items()) if (REPO / f).is_file()}
        for lane, files in sorted(out.items())
    }


def main(argv: list[str]) -> int:
    if argv[:1] != ["fold"] or len(argv) < 2:
        print("usage: lane_durations.py fold ARTIFACT_DIR...", file=sys.stderr)
        return 2
    recorded = json.loads(run_lane.DURATIONS.read_text())
    folded = fold(recorded, [Path(d) for d in argv[1:]])
    run_lane.DURATIONS.write_text(json.dumps(folded, indent=1) + "\n")
    for lane, files in folded.items():
        print(f"{lane}: {len(files)} files, {sum(files.values()) / 60:.0f} minutes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
