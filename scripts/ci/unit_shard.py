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

``files K/N`` prints the test files of shard K of N. Files with a recorded time
(unit_file_seconds.json) are dealt out longest first, each to the shard with the least time so far,
so the shards take about the same time; a hash split left one shard with nearly three times the
work of another. A file with no recorded time goes to the shard its path's hash names, so a file
added moves no other file. ``timings JUNIT...`` rewrites the recorded times from shards' junit
files (the unit-junit-N artifacts of a run). Loaded as a pytest plugin
(``-p unit_shard``) with PROVISA_COLLECTED_IDS set, it writes the ids pytest collected and kept
after deselection, one per line. ``check FULL SHARD...`` compares the shards' ids with a full
collection's and fails naming any test missing from every shard or in more than one.

``check-vitest FILES SHARD...`` does the same for ui-unit-tests.yml's vitest shards. Vitest shards
by file and runs a file's tests together, so every test runs once exactly when every test file
(``vitest list --filesOnly --json``) is in one shard's results (its json reporter's output).
"""

from __future__ import annotations

import hashlib
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
# Seconds of test time per file, summed over a run's tests (``timings``).
TIMINGS = Path(__file__).with_name("unit_file_seconds.json")


def test_files() -> list[str]:
    found = {
        str(f.relative_to(REPO)) for p in PATTERNS for f in (REPO / ROOT).rglob(p) if f.is_file()
    }
    return sorted(found)


def shard_of(path: str, total: int) -> int:
    """The 1-based shard of ``total`` that runs ``path``."""
    return int(hashlib.sha256(path.encode()).hexdigest(), 16) % total + 1


def recorded_seconds() -> dict[str, float]:
    return json.loads(TIMINGS.read_text())


def assign(paths: list[str], seconds: dict[str, float], total: int) -> dict[str, int]:
    """The 1-based shard of ``total`` for each path: the timed ones longest first to the shard with
    the least time so far (ties to the lower shard, equal times in path order), the rest by hash."""
    load = [0.0] * total
    out: dict[str, int] = {}
    for path in sorted((p for p in paths if p in seconds), key=lambda p: (-seconds[p], p)):
        shard = min(range(total), key=lambda i: (load[i], i))
        load[shard] += seconds[path]
        out[path] = shard + 1
    for path in paths:
        out.setdefault(path, shard_of(path, total))
    return out


def files(spec: str) -> list[str]:
    index, total = (int(x) for x in spec.split("/"))
    if not 1 <= index <= total:
        raise ValueError(f"shard {spec} is out of range")
    paths = test_files()
    placed = assign(paths, recorded_seconds(), total)
    return [f for f in paths if placed[f] == index]


def _junit_file(classname: str) -> str | None:
    """The test file a junit classname (``tests.unit.sub.test_x.TestY``) names."""
    parts = classname.split(".")
    for end in range(len(parts), 0, -1):
        candidate = "/".join(parts[:end]) + ".py"
        if (REPO / candidate).is_file():
            return candidate
    return None


def timings(junits: list[Path]) -> dict[str, float]:
    """Seconds per test file over the junit files; a test whose file is not found fails loudly."""
    seconds: Counter[str] = Counter()
    for junit in junits:
        for case in ET.parse(junit).getroot().iter("testcase"):
            file = _junit_file(case.get("classname", ""))
            if file is None:
                raise ValueError(f"{junit}: no test file for {case.get('classname')!r}")
            seconds[file] += float(case.get("time", 0))
    return {f: round(t, 1) for f, t in sorted(seconds.items())}


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
    if argv[:1] == ["files"] and len(argv) == 2:
        print("\n".join(files(argv[1])))
        return 0
    if argv[:1] == ["timings"] and len(argv) >= 2:
        result = timings([Path(p) for p in argv[1:]])
        TIMINGS.write_text(json.dumps(result, indent=1) + "\n")
        print(f"{len(result)} files, {sum(result.values()):.0f} s")
        return 0
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
        "usage: unit_shard.py files K/N | timings JUNIT... | check FULL SHARD... | check-vitest FILES SHARD...",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
