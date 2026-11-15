#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: fe597230-25d7-460b-9173-5be21d7c8ab7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Whether a conformance run fails its job, judged against its suite's committed expected-failures
list, ``expected/<surface>.json`` (REQ-2167). The last step of every conformance workflow but the
drivers' (whose driver_suite.py judges its own per-test list, REQ-2203).

A run fails when a case fails and the list does not hold it, and when a listed case now passes
("fixed but still listed": the change that fixes it takes it off, so the list only shrinks). Only
a full run is judged: a run filtered to some cases, or with fewer cases than the suite's committed
``total`` (a crash midway, a step's bound reached), fails as not a full run. A missing report or
list fails; neither is ever read as empty.

    {"total": 3880,                       # every case of the suite at its pinned version
     "expected": [                        # [] when nothing fails
       {"case": "Delete6 - ...::[5] ...", # <feature>::<scenario>, as the report names it
        "job": "J1275",                   # the queue job that will fix it (required)
        "since": "2026-11-13",            # the day it was listed
        "reason": "..."}]}                # optional: what the run said

A surface at its baseline (by_design.AT_BASELINE) is judged by its list like any other: its
summary script already fails on the part it gates, and the list holds the rest.

``functions`` judges function_support.json per engine against ``expected/functions.json``: every
engine of the list has a full result; a wrong answer is listed (as ``<engine>::<fn>(<overload>)``)
or fails; ``passed`` does not fall below the committed count; engine errors and refusals by name
do not rise above theirs. A count that moves the good way is said, so the file can be lowered.

Usage: baseline_gate.py cases <surface> <report.json>
       baseline_gate.py functions <function_support.json>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

EXPECTED = Path(__file__).resolve().parent / "expected"

#: pgwire's report keeps its failures in its parts (pgwire_catalog_types.py PARTS).
PGWIRE_PARTS = ("pgspec", "catalog_relations", "catalog_functions", "type_conversions")

ENTRY_FIELDS = {"case", "job", "since", "reason"}
JOB = re.compile(r"J\d+[a-z]?")
SINCE = re.compile(r"\d{4}-\d{2}-\d{2}")

#: The function counts held to the committed file: (count, whether it may only rise).
FUNCTION_COUNTS = {
    "passed": True,
    "differs_engine_error": False,
    "refused_by_name": False,
}


class Refused(ValueError):
    """The run cannot be judged: what it, or the committed list, lacks."""


@dataclass
class Expected:
    path: Path
    total: int
    cases: dict[str, dict]


def _read(path: Path, what: str) -> dict:
    if not path.is_file():
        raise Refused(f"no {what}: {path}")
    return json.loads(path.read_text())


def _entries(path: Path, entries: list[dict] | None) -> dict[str, dict]:
    if not isinstance(entries, list):
        raise Refused(f"{path.name}: expected is not a list")
    out: dict[str, dict] = {}
    for i, entry in enumerate(entries):
        where = f"{path.name} entry {i}"
        if not isinstance(entry, dict):
            raise Refused(f"{where} is not an object")
        unknown = sorted(set(entry) - ENTRY_FIELDS)
        if unknown:
            raise Refused(f"{where} has unknown field(s) {unknown}")
        for name in ("case", "job", "since"):
            if not entry.get(name):
                raise Refused(f"{where} has no {name}: every listed failure names its {name}")
        if not JOB.fullmatch(str(entry["job"])):
            raise Refused(f"{where}: job {entry['job']!r} is not a queue job (J<n>)")
        if not SINCE.fullmatch(str(entry["since"])):
            raise Refused(f"{where}: since {entry['since']!r} is not a date (YYYY-MM-DD)")
        if entry["case"] in out:
            raise Refused(f"{path.name}: {entry['case']} is listed twice")
        out[entry["case"]] = entry
    return out


def load_expected(surface: str) -> Expected:
    path = EXPECTED / f"{surface}.json"
    doc = _read(path, "expected-failures file")
    total = doc.get("total")
    if not isinstance(total, int) or total <= 0:
        raise Refused(f"{path.name}: total is not the suite's case count")
    return Expected(path, total, _entries(path, doc.get("expected")))


def load_functions_expected() -> tuple[Path, dict[str, dict[str, int]], dict[str, dict]]:
    path = EXPECTED / "functions.json"
    doc = _read(path, "expected-failures file")
    engines = doc.get("engines")
    if not isinstance(engines, dict) or not engines:
        raise Refused(f"{path.name}: no engines")
    for engine, counts in engines.items():
        missing = [k for k in ("total", *FUNCTION_COUNTS) if not isinstance(counts.get(k), int)]
        if missing:
            raise Refused(f"{path.name}: engine {engine} has no {missing}")
    return path, engines, _entries(path, doc.get("expected"))


def _failures(surface: str, doc: dict) -> list[dict]:
    if surface != "pgwire":
        return list(doc["failures"])
    absent = [p for p in PGWIRE_PARTS if not isinstance(doc.get(p), dict)]
    if absent:
        raise Refused(f"pgwire: the run has no {absent}: not a full run")
    return [f for p in PGWIRE_PARTS for f in doc[p]["failures"]]


def judge_cases(surface: str, report: Path) -> list[str]:
    """The run's errors against the surface's list; none when it passes."""
    expected = load_expected(surface)
    doc = _read(report, "report")
    if doc.get("filter"):
        raise Refused(
            f"{surface}: a run filtered to {doc['filter']!r} is not a full run; the gate judges "
            "only a full run"
        )
    if doc.get("total") != expected.total:
        raise Refused(
            f"{surface}: the run has {doc.get('total')} of the suite's {expected.total} cases: "
            f"not a full run (a changed suite version changes total in {expected.path.name})"
        )
    failures = _failures(surface, doc)
    if len(failures) != doc["failed"]:
        raise Refused(
            f"{surface}: the report lists {len(failures)} failure(s) but counts {doc['failed']}"
        )
    failed = {f"{f['feature']}::{f['scenario']}": f for f in failures}
    errors = [
        f"{surface}: {case} failed and is not on the expected-failures list "
        f"({expected.path.name}): {failed[case]['reason']}"
        for case in sorted(set(failed) - set(expected.cases))
    ]
    errors += [
        f"{surface}: {case} fixed but still listed ({expected.cases[case]['job']}): take it off "
        f"{expected.path.name}"
        for case in sorted(set(expected.cases) - set(failed))
    ]
    return errors


def judge_functions(report: Path) -> list[str]:
    path, engines, listed = load_functions_expected()
    doc = _read(report, "report")
    unjudged = sorted(set(doc["engines_without_a_result"]) | set(doc["engines_incomplete"]))
    if unjudged:
        raise Refused(f"functions: no full result on {unjudged}: not a full run")
    absent = sorted(set(engines) - set(doc["by_engine"]))
    if absent:
        raise Refused(f"functions: the run has no result on {absent}: not a full run")
    errors: list[str] = []
    for engine, committed in sorted(engines.items()):
        got = doc["by_engine"][engine]
        if got["total"] != committed["total"]:
            raise Refused(
                f"functions on {engine}: {got['total']} of the committed {committed['total']} "
                "overloads: not a full run"
            )
        for count, rises in FUNCTION_COUNTS.items():
            now, then = got[count], committed[count]
            worse = now < then if rises else now > then
            if worse:
                side = "below" if rises else "above"
                errors.append(
                    f"functions on {engine}: {count} {now}, {side} the committed {then} "
                    f"({path.name})"
                )
            elif now != then:
                print(
                    f"::notice::functions on {engine}: {count} {now} (committed {then}): "
                    f"update {path.name}",
                    file=sys.stderr,
                )
    wrong = {
        f"{engine}::{fn['function']}({fn['overload']})"
        for fn in doc["functions"]
        for engine, outcome in fn["outcomes"].items()
        if engine in engines and outcome["outcome"] == "wrong_answer"
    }
    errors += [
        f"functions: {case} answers wrong and is not on the expected-failures list ({path.name})"
        for case in sorted(wrong - set(listed))
    ]
    errors += [
        f"functions: {case} fixed but still listed ({listed[case]['job']}): take it off {path.name}"
        for case in sorted(set(listed) - wrong)
    ]
    return errors


def main(argv: list[str]) -> int:
    command, *args = argv
    try:
        if command == "cases":
            surface, report = args
            errors = judge_cases(surface, Path(report))
        elif command == "functions":
            (report,) = args
            errors = judge_functions(Path(report))
        else:
            raise Refused(f"no command {command!r}: cases or functions")
    except Refused as refused:
        errors = [str(refused)]
    for error in errors:
        print(f"::error::{error}", file=sys.stderr)
    if not errors:
        print("conformance gate: every failure is on the expected-failures list", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
