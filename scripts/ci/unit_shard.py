# Copyright (c) 2026 Kenneth Stott
# Canary: 9e384508-c270-4752-83e6-1f3ddfae8b8b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The unit suite's shards (unit-tests.yml), and the check that they run every test once.

A run's ``plan`` job takes the seconds each test file took in the last successful run on main
(``timings OUT JUNIT...`` over that run's unit-junit-N artifacts) and hands them to every shard, so
the times are never older than main's last green run and no branch edits a record. ``files K/N
SECONDS`` prints the test files of shard K of N: the files are dealt out longest first, each to the
shard with the least time so far. A file costs its tests' seconds plus FILE_SECONDS for collecting
it; a file the run on main did not have counts as the mean file.

Loaded as a pytest plugin (``-p unit_shard``): with PROVISA_UNIT_SHARD (K/N) and
PROVISA_UNIT_SECONDS set it leaves out of ``pytest tests/unit`` every test file of another shard.
The files are not passed as arguments: 1,285 file arguments took a shard 8:36 to collect, where
the whole directory collects in 1:35. With PROVISA_COLLECTED_IDS set it writes the ids pytest
collected and kept after deselection, one per line. ``check FULL SHARD...`` compares the shards'
ids with a full collection's and fails naming any test missing from every shard or in more than
one. ``budget SECONDS`` fails when a shard's tests took longer than BUDGET_SECONDS.

``check-vitest FILES SHARD...`` does the same for ui-unit-tests.yml's vitest shards. Vitest shards
by file and runs a file's tests together, so every test runs once exactly when every test file
(``vitest list --filesOnly --json``) is in one shard's results (its json reporter's output).
"""

from __future__ import annotations

import json
import os
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ROOT = "tests/unit"
# pyproject.toml [tool.pytest.ini_options] python_files.
PATTERNS = ("test_*.py", "*_test.py", "steps_*.py")
# What a file adds to its shard beyond its tests' seconds: every xdist worker imports and collects
# it (95 s for 2,453 files in one process, in four workers). It also spreads the files whose tests
# take no measurable time, which a split by test seconds alone dealt to one shard: 1,285 files.
FILE_SECONDS = 0.2
# A shard's tests, collection included: 15 minutes is the target and warns, 20 fails the shard by
# name. The job's limit (timeout-minutes in unit-tests.yml) is 30, of which up to four pass before
# pytest starts; a job that reaches it is cancelled with no test named.
TARGET_SECONDS = 15 * 60
BUDGET_SECONDS = 20 * 60
SHARD_ENV = "PROVISA_UNIT_SHARD"
SECONDS_ENV = "PROVISA_UNIT_SECONDS"


def test_files() -> list[str]:
    found = {
        str(f.relative_to(REPO)) for p in PATTERNS for f in (REPO / ROOT).rglob(p) if f.is_file()
    }
    return sorted(found)


def assign(paths: list[str], seconds: dict[str, float], total: int) -> dict[str, int]:
    """The 1-based shard of ``total`` for each path: longest first to the shard with the least
    time so far (ties to the lower shard, equal times in path order). A path with no recorded
    seconds counts as the mean of the recorded ones."""
    known = [seconds[p] for p in paths if p in seconds]
    mean = sum(known) / len(known) if known else 0.0
    cost = {p: seconds.get(p, mean) + FILE_SECONDS for p in paths}
    load = [0.0] * total
    out: dict[str, int] = {}
    for path in sorted(paths, key=lambda p: (-cost[p], p)):
        shard = min(range(total), key=lambda i: (load[i], i))
        load[shard] += cost[path]
        out[path] = shard + 1
    return out


def files(spec: str, seconds: dict[str, float]) -> list[str]:
    index, total = (int(x) for x in spec.split("/"))
    if not 1 <= index <= total:
        raise ValueError(f"shard {spec} is out of range")
    paths = test_files()
    placed = assign(paths, seconds, total)
    return [f for f in paths if placed[f] == index]


def _junit_file(classname: str) -> str | None:
    """The test file a junit classname (``tests.unit.sub.test_x.TestY``) names."""
    parts = classname.split(".")
    for end in range(len(parts), 0, -1):
        candidate = "/".join(parts[:end]) + ".py"
        if (REPO / candidate).is_file():
            return candidate
    return None


def timings(junits: list[Path]) -> tuple[dict[str, float], int]:
    """Seconds per test file over the junit files, and the number of tests left out: the junit
    files are another commit's, so a test of a file this checkout does not have has no shard here."""
    seconds: Counter[str] = Counter()
    absent = 0
    for junit in junits:
        for case in ET.parse(junit).getroot().iter("testcase"):
            file = _junit_file(case.get("classname", ""))
            if file is None:
                absent += 1
                continue
            seconds[file] += float(case.get("time", 0))
    return {f: round(t, 1) for f, t in sorted(seconds.items())}, absent


_kept: tuple[frozenset[Path], frozenset[Path]] | None = None


def pytest_ignore_collect(collection_path: Path, config) -> bool | None:
    """Leave out the test files of the other shards."""
    global _kept
    spec = os.environ.get(SHARD_ENV)
    if not spec or collection_path.suffix != ".py":
        return None
    if _kept is None:
        seconds = json.loads(Path(os.environ[SECONDS_ENV]).read_text())
        _kept = (
            frozenset(REPO / f for f in test_files()),
            frozenset(REPO / f for f in files(spec, seconds)),
        )
    every, mine = _kept
    path = collection_path.resolve()
    return True if path in every and path not in mine else None


def budget(taken: int) -> tuple[bool, str]:
    """Whether a shard whose tests took ``taken`` seconds is within the budget, and the line that
    says so."""
    spent = f"{taken // 60}:{taken % 60:02d}"
    if taken > BUDGET_SECONDS:
        return False, (
            f"this shard's tests took {spent}, over the {BUDGET_SECONDS // 60}-minute budget; the "
            "job is cancelled at its timeout-minutes. The split already follows the times of "
            "main's last green run, so the suite has outgrown the shards: add one to the matrix "
            "in .github/workflows/unit-tests.yml."
        )
    if taken > TARGET_SECONDS:
        return True, (
            f"this shard's tests took {spent}, over the {TARGET_SECONDS // 60}-minute target and "
            f"failing above {BUDGET_SECONDS // 60}: add a shard to the matrix in "
            ".github/workflows/unit-tests.yml."
        )
    return True, f"this shard's tests took {spent}; the target is {TARGET_SECONDS // 60}:00."


def pytest_collection_finish(session) -> None:
    """Write the collected ids. Under xdist the controller collects nothing and every worker
    collects the same set, so the first worker writes them."""
    out = os.environ.get("PROVISA_COLLECTED_IDS")
    worker = getattr(session.config, "workerinput", {}).get("workerid")
    if not out or worker not in (None, "gw0"):
        return
    Path(out).write_text("".join(f"{item.nodeid}\n" for item in session.items))


def check(full: Path, shards: list[Path]) -> list[str]:
    """What is wrong with the shards against the full collection: each test missing from every
    shard, run by more than one, or run by a shard but absent from the full collection."""
    expected = set(full.read_text().split("\n")) - {""}
    ran = Counter(i for s in shards for i in s.read_text().split("\n") if i)
    problems = [f"missing from every shard: {i}" for i in sorted(expected - set(ran))]
    problems += [f"run by {n} shards: {i}" for i, n in sorted(ran.items()) if n > 1]
    problems += [f"not in the full collection: {i}" for i in sorted(set(ran) - expected)]
    return problems


def _ui_path(path: str) -> str:
    return path.split("/provisa-ui/", 1)[-1]


def check_vitest(full: Path, shards: list[Path]) -> tuple[int, list[str]]:
    """The test-file count, and what is wrong: each file in no shard's results, in more than
    one, or in a shard's results but not listed."""
    expected = {_ui_path(f["file"]) for f in json.loads(full.read_text())}
    ran = Counter(
        _ui_path(r["name"]) for s in shards for r in json.loads(s.read_text())["testResults"]
    )
    problems = [f"in no shard: {f}" for f in sorted(expected - set(ran))]
    problems += [f"in {n} shards: {f}" for f, n in sorted(ran.items()) if n > 1]
    problems += [f"not listed: {f}" for f in sorted(set(ran) - expected)]
    return len(expected), problems


def main(argv: list[str]) -> int:
    if argv[:1] == ["files"] and len(argv) == 3:
        seconds = json.loads(Path(argv[2]).read_text())
        mine = files(argv[1], seconds)
        print("\n".join(mine))
        recorded = sum(seconds[f] for f in mine if f in seconds)
        unrecorded = sum(1 for f in mine if f not in seconds)
        print(
            f"shard {argv[1]}: {len(mine)} files, {recorded:.0f} s recorded, "
            f"{unrecorded} files with no record",
            file=sys.stderr,
        )
        return 0
    if argv[:1] == ["timings"] and len(argv) >= 3:
        result, absent = timings([Path(p) for p in argv[2:]])
        Path(argv[1]).write_text(json.dumps(result, indent=1) + "\n")
        print(
            f"{len(result)} files, {sum(result.values()):.0f} s; "
            f"{absent} tests of files not in this checkout"
        )
        return 0
    if argv[:1] == ["budget"] and len(argv) == 2:
        ok, line = budget(int(argv[1]))
        if ok and int(argv[1]) > TARGET_SECONDS:
            line = f"::warning::{line}"
        print(line if ok else f"::error::{line}")
        return 0 if ok else 1
    if argv[:1] == ["check"] and len(argv) >= 3:
        full, shards = Path(argv[1]), [Path(p) for p in argv[2:]]
        problems = check(full, shards)
        for p in problems:
            print(p)
        count = len(set(full.read_text().split("\n")) - {""})
        print(f"{count} tests collected; {len(shards)} shards; {len(problems)} problems")
        return 1 if problems else 0
    if argv[:1] == ["check-vitest"] and len(argv) >= 3:
        count, problems = check_vitest(Path(argv[1]), [Path(p) for p in argv[2:]])
        for p in problems:
            print(p)
        print(f"{count} test files listed; {len(argv) - 2} shards; {len(problems)} problems")
        return 1 if problems else 0
    print(
        "usage: unit_shard.py files K/N SECONDS | timings OUT JUNIT... | budget SECONDS | "
        "check FULL SHARD... | check-vitest FILES SHARD...",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
