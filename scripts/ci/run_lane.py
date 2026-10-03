# Copyright (c) 2026 Kenneth Stott
# Canary: 141cb354-2666-496e-b9cf-511cf6417d5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Run one CI lane, or one shard of it, of the container-backed suite.

The lanes are scripts/test-all's: each holds the tests whose session-provisioned services can live
together in one Docker host's memory. The large lanes are cut into file shards so each job fits a
runner and finishes in reasonable time. A shard is every Nth test file of the lane's paths, in
sorted order, so the shards partition the lane: each file runs in exactly one shard.

Usage: run_lane.py <lane> [--shard K/N] [extra pytest args...]
       run_lane.py --matrix <all | lane,lane,...>   (the CI suite matrix, as JSON)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# The selectors scripts/test-all uses, by name. The base exclusions are folded in because a CLI -m
# replaces the ini addopts -m.
_BASE = "not packaging and not cluster and not isolated and not requires_warehouse"
_KAFKA = "requires_kafka or requires_debezium"
_DATASTORE = "requires_elasticsearch or requires_sparql or requires_mongodb or requires_prometheus"
_HIVE_S3 = "requires_hive_s3"
# Container-backed paths. tests/unit runs in unit-tests.yml and is not repeated here.
_SUITE = ("tests/integration", "tests/steps")


@dataclass(frozen=True)
class Lane:
    paths: tuple[str, ...]
    marker: str
    clear_addopts: bool = False
    ignore: tuple[str, ...] = ()


LANES: dict[str, Lane] = {
    "core": Lane(
        _SUITE, f"{_BASE} and not ({_KAFKA} or requires_neo4j or {_DATASTORE}) and not e2e"
    ),
    "app": Lane(
        _SUITE,
        f"{_BASE} and not ({_KAFKA} or requires_neo4j or {_DATASTORE} or {_HIVE_S3}) and e2e",
    ),
    "hive_s3": Lane(_SUITE, f"{_BASE} and ({_HIVE_S3}) and e2e"),
    "kafka": Lane(_SUITE, f"{_BASE} and ({_KAFKA})"),
    "neo4j": Lane(_SUITE, f"{_BASE} and requires_neo4j and not ({_KAFKA})"),
    "datastores": Lane(_SUITE, f"{_BASE} and ({_DATASTORE}) and not ({_KAFKA} or requires_neo4j)"),
    # Every directory but tests/e2e, as test-all runs it: the isolated tests under tests/unit are
    # deselected by unit-tests.yml's addopts and run only here.
    "isolated": Lane(("tests",), "isolated", clear_addopts=True, ignore=("tests/e2e",)),
    "e2e": Lane(("tests/e2e",), "e2e and not cluster", clear_addopts=True),
    "cluster": Lane(("tests/e2e/test_helm_minikube.py",), "cluster", clear_addopts=True),
    # Live cloud warehouses: nightly and on demand, with the credentials from repo secrets.
    "warehouse": Lane(
        ("tests/integration", "tests/steps"), "requires_warehouse", clear_addopts=True
    ),
}


# The suite job's matrix: lane -> (file shards, timeout minutes). cluster and warehouse are their
# own jobs, not matrix entries.
SUITE: dict[str, tuple[int, int]] = {
    "core": (6, 150),
    "app": (3, 120),
    "hive_s3": (1, 90),
    "kafka": (1, 90),
    "neo4j": (1, 90),
    "datastores": (1, 90),
    "isolated": (1, 90),
    "e2e": (1, 120),
}


def matrix(selection: str) -> dict:
    """The suite matrix for ``selection``: "all", or comma-separated lane names. A name that is
    no lane is refused, so a typo cannot dispatch an empty run that reports green."""
    names = [n.strip() for n in selection.split(",") if n.strip()]
    if names == ["all"]:
        names = list(SUITE)
    unknown = [n for n in names if n not in LANES]
    if unknown:
        raise ValueError(f"no such lane: {', '.join(unknown)}; lanes: {', '.join(sorted(LANES))}")
    include = []
    for name in names:
        if name not in SUITE:
            continue
        shards, timeout = SUITE[name]
        if shards == 1:
            include.append({"lane": name, "shard": "", "timeout": timeout})
        else:
            include += [
                {"lane": name, "shard": f"{k}/{shards}", "timeout": timeout}
                for k in range(1, shards + 1)
            ]
    return {"include": include}


def test_files(paths: tuple[str, ...]) -> list[str]:
    """Every test module under ``paths``, sorted (pytest's own file patterns)."""
    found: set[str] = set()
    for path in paths:
        root = REPO / path
        if root.is_file():
            found.add(path)
            continue
        for pattern in ("test_*.py", "steps_*.py"):
            for f in root.rglob(pattern):
                found.add(str(f.relative_to(REPO)))
    return sorted(found)


def shard(files: list[str], index: int, total: int) -> list[str]:
    """Shard ``index`` (1-based) of ``total``: every total-th file starting at index-1."""
    if not 1 <= index <= total:
        raise ValueError(f"shard {index}/{total} is out of range")
    return files[index - 1 :: total]


def command(lane_name: str, shard_spec: str | None, extra: list[str]) -> list[str]:
    lane = LANES[lane_name]
    targets = list(lane.paths)
    if shard_spec is not None:
        index, total = (int(x) for x in shard_spec.split("/"))
        targets = shard(test_files(lane.paths), index, total)
    cmd = [sys.executable, "-m", "pytest", *targets, "-m", lane.marker]
    if lane.clear_addopts:
        cmd += ["-o", "addopts="]
    cmd += [f"--ignore={path}" for path in lane.ignore]
    return cmd + ["-ra", "-p", "no:cacheprovider", "--durations=50", *extra]


def main(argv: list[str]) -> int:
    if argv[:1] == ["--matrix"]:
        import json

        print(json.dumps(matrix(argv[1] if len(argv) > 1 else "all")))
        return 0
    parser = argparse.ArgumentParser()
    parser.add_argument("lane", choices=sorted(LANES))
    parser.add_argument("--shard", help="K/N: the Kth of N file shards of the lane")
    args, extra = parser.parse_known_args(argv)
    cmd = command(args.lane, args.shard, extra)
    print("+", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=REPO, env=os.environ.copy())  # noqa: S603 - fixed argv


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
