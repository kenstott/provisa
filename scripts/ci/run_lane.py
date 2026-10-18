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
_BASE = (
    "not packaging and not cluster and not isolated and not requires_warehouse "
    "and not requires_vector"
)
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
    # REQ-2016: the tests that embed with a local sentence-transformers model (the vector extra,
    # with CPU-only torch): their own job installs it (scripts/ci/lanes/suite-prepare.sh vector), so no
    # other lane pays for torch.
    "vector": Lane(
        _SUITE,
        "requires_vector and not packaging and not cluster and not isolated "
        "and not requires_warehouse",
    ),
    "cluster": Lane(("tests/e2e/test_helm_minikube.py",), "cluster", clear_addopts=True),
    # Live cloud services: nightly and on demand, with the credentials from repo secrets. A live
    # test runs in CI only in a lane whose job declares its secrets and checks they are present
    # (integration-suite-lanes.yml), so what a lane runs is decided here, by marker, never by a
    # test skipping itself. The live Salesforce test is in no nightly lane: a cold run costs
    # about 2,450 API calls of an org allowed 15,000 a day. The live Microsoft 365 test is in no
    # CI lane at all: its tenant is the maintainer's own and no job declares M365 secrets.
    # Nor is the live Google Workspace test: its domain is the maintainer's own too.
    "warehouse": Lane(
        ("tests/integration", "tests/steps"),
        "requires_warehouse and not requires_salesforce and not requires_athena and not requires_servicenow "
        "and not requires_microsoft_365 and not requires_google_workspace and not requires_polly_bakeoff",
        clear_addopts=True,
    ),
    # The live Salesforce org: on demand only (workflow_dispatch, include_salesforce), its own job.
    "salesforce": Lane(
        ("tests/integration", "tests/steps"), "requires_salesforce", clear_addopts=True
    ),
    # The live Amazon Athena account (REQ-1981): on demand only (include_athena), its own job.
    "athena": Lane(("tests/integration", "tests/steps"), "requires_athena", clear_addopts=True),
    # The live ServiceNow instance (REQ-1954): on demand only (include_servicenow), its own job.
    "servicenow": Lane(
        ("tests/integration", "tests/steps"), "requires_servicenow", clear_addopts=True
    ),
    # REQ-2012: Polly's model bake-off asks paid models real questions; on demand only, its own
    # workflow (polly-bakeoff.yml).
    "polly_bakeoff": Lane(
        ("tests/integration/test_polly_bakeoff_e2e.py",),
        "requires_polly_bakeoff",
        clear_addopts=True,
    ),
}


# The longest one test may run in a lane. Generous: a heavy engine's provisioning happens inside its
# first test's setup.
TEST_TIMEOUT_S = 900
# How long before that bound a still-running test's stacks are written to the log.
STACKS_BEFORE_BOUND_S = 60

# The suite job's matrix: lane -> (file shards, timeout minutes). cluster, warehouse and salesforce
# are their own jobs, not matrix entries.
SUITE: dict[str, tuple[int, int]] = {
    "core": (6, 150),
    "app": (3, 120),
    "hive_s3": (1, 90),
    "kafka": (1, 90),
    "neo4j": (1, 90),
    "datastores": (1, 90),
    "isolated": (1, 90),
    "e2e": (1, 120),
    "vector": (1, 90),
}


def named_tests() -> list[str]:
    """The tests a targeted run names (PROVISA_LANE_TESTS: space-separated pytest node ids or
    files), or none for an ordinary run. A named file that does not exist is refused, so a typo
    cannot pass by running nothing."""
    named = os.environ.get("PROVISA_LANE_TESTS", "").split()
    missing = [t for t in named if not (REPO / t.split("::", 1)[0]).is_file()]
    if missing:
        raise ValueError(f"no such test file: {', '.join(missing)}")
    return named


def matrix(selection: str) -> dict:
    """The suite matrix for ``selection``: "all", or comma-separated lane names. A name that is
    no lane is refused, so a typo cannot dispatch an empty run that reports green. A targeted run
    (named_tests) runs each lane as one job: its few tests are not sharded."""
    names = [n.strip() for n in selection.split(",") if n.strip()]
    if names == ["all"]:
        names = list(SUITE)
    unknown = [n for n in names if n not in LANES]
    if unknown:
        raise ValueError(f"no such lane: {', '.join(unknown)}; lanes: {', '.join(sorted(LANES))}")
    include = []
    targeted = bool(named_tests())
    for name in names:
        if name not in SUITE:
            continue
        # A targeted run plans only the lanes that hold a named test: a lane with none would
        # collect nothing, write no results and fail its required results upload.
        if targeted and not targets(name, None):
            continue
        shards, timeout = SUITE[name]
        if shards == 1 or targeted:
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


def targets(lane_name: str, shard_spec: str | None) -> list[str]:
    """What the lane (or its shard) hands pytest: the lane's paths, or the shard's files."""
    lane = LANES[lane_name]
    named = named_tests()
    if named:
        # A targeted run: only the named tests that live under this lane's paths.
        return [t for t in named if _under(t.split("::", 1)[0], lane.paths)]
    if shard_spec is None:
        return list(lane.paths)
    index, total = (int(x) for x in shard_spec.split("/"))
    return shard(test_files(lane.paths), index, total)


def _under(path: str, roots: tuple[str, ...]) -> bool:
    return any(path == r or path.startswith(r.rstrip("/") + "/") for r in roots)


def command(lane_name: str, shard_spec: str | None, extra: list[str]) -> list[str]:
    lane = LANES[lane_name]
    cmd = [sys.executable, "-m", "pytest", *targets(lane_name, shard_spec), "-m", lane.marker]
    if lane.clear_addopts:
        cmd += ["-o", "addopts="]
    cmd += [f"--ignore={path}" for path in lane.ignore]
    # Every test is bounded: one that waits past this fails by name with every thread's stack,
    # instead of holding the shard until the job's own timeout cancels it and its log is lost.
    bound = ["--timeout", str(TEST_TIMEOUT_S), "--timeout-method", "signal"]
    # The bound's own stacks are printed in pytest's final report, which a lane cancelled by its
    # job limit never reaches: the e2e lane spent seven 15-minute bounds in module teardowns and
    # was cancelled with no stack in its log. A test phase still running a minute before its
    # bound has every thread's stack written to the log at that moment.
    stacks = ["-o", f"faulthandler_timeout={TEST_TIMEOUT_S - STACKS_BEFORE_BOUND_S}"]
    return cmd + ["-ra", "-p", "no:cacheprovider", "--durations=50", *bound, *stacks, *extra]


def main(argv: list[str]) -> int:
    if argv[:1] == ["--matrix"]:
        import json

        print(json.dumps(matrix(argv[1] if len(argv) > 1 else "all")))
        return 0
    if argv[:1] == ["--has-targets"]:
        # A job of its own (warehouse, salesforce, athena, servicenow) runs when the run names no
        # tests (the whole lane) or names one under the lane's paths; otherwise it would collect
        # nothing, write no results and fail its required results upload.
        print("true" if not named_tests() or targets(argv[1], None) else "false")
        return 0
    parser = argparse.ArgumentParser()
    parser.add_argument("lane", choices=sorted(LANES))
    parser.add_argument("--shard", help="K/N: the Kth of N file shards of the lane")
    args, extra = parser.parse_known_args(argv)
    if named_tests() and not targets(args.lane, args.shard):
        print(f"targeted run: no named test is in lane {args.lane}; nothing to run", flush=True)
        return 0
    cmd = command(args.lane, args.shard, extra)
    print("+", " ".join(cmd), flush=True)
    code = subprocess.call(cmd, cwd=REPO, env=os.environ.copy())  # noqa: S603 - fixed argv
    if code == 5 and named_tests():
        # Lanes share paths and differ by marker: a named test runs in its own lane, and every
        # other lane of a targeted run deselects it. Collecting nothing there is not a failure.
        print(f"targeted run: lane {args.lane} selects none of the named tests", flush=True)
        return 0
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
