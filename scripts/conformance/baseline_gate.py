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

A documented limit (REQ-2167, amended 2026-11-23: a case that fails exactly as a stated limitation
of that feature on that engine says it will) is judged as a case skipped by design is: it is no
failure, so it is never listed, and a listed case that becomes one is fixed but still listed.

A surface at its baseline (by_design.AT_BASELINE) is judged by its list like any other: its
summary script already fails on the part it gates, and the list holds the rest.

``functions`` judges function_support.json per engine against ``expected/functions.json``, which
functions_baseline.py writes from a run's merged result and nobody types. An overload's base calls
(its NULL call and its samples) and its edge calls (REQ-2236, function_edges.py) are held apart: an
edge is tried only when the base calls are right, so an overload failing at an edge still passes
its base calls.

    {"measured": {"run": "43861326336", "sha": "426be55f44"},
     "engines": {"trino": {
       "total": 3319,
       "base": {"passed": 1768,           # base calls right; may not fall
                "wrong_answer": 0,        # as many as the list holds at "base"
                "engine_error": 0,
                "refused_by_name": 625},  # may not rise
       "edge": {"served": 1296,           # base calls and every edge call right, or within bound
                "wrong_answer": 203,      # as many as the list holds at "edge"
                "engine_error": 269,
                "documented_limit": 0}}},  # base calls right, an edge as a stated limitation says
     "expected": [
       {"case": "trino::abs(bigint)",     # <engine>::<function>(<overload>)
        "at": "edge",                     # or "base": a base call fails
        "kind": "wrong_answer",           # or "engine_error"
        "calls": 1,                       # the edge calls failing so (an edge entry's)
        "statement": "SELECT ...",        # the first failing call
        "edge": "bigint least",           # its edge value (an edge entry's)
        "said": "...",                    # a wrong answer: the engine's answer and PostgreSQL's;
                                          # an engine error: the engine's error
        "sqlstate": "22003",              # an engine error's, where it has one
        "postgres": "22003 ERROR: ...",   # an engine error's: PostgreSQL's error, or "answers"
        "job": "J1388b", "since": "2026-11-16"}]}

Every wrong answer and every engine error is listed, by engine and overload, with where it fails
and how. The run fails on one not listed; on a listed one failing otherwise than listed (an edge
entry whose base call now fails, an engine error now a wrong answer); on a listed edge entry with
more failing edge calls than listed; on a listed one now right ("fixed but still listed"); on base
calls right below the committed count; and on refusals by name above theirs. A count that moves the
good way is said, so the file can be regenerated. A run that passes prints its baseline.

Usage: baseline_gate.py cases <surface> <report.json>
       baseline_gate.py functions <function_support.json>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

EXPECTED = Path(__file__).resolve().parent / "expected"

#: pgwire's report keeps its failures in its parts (pgwire_catalog_types.py PARTS).
PGWIRE_PARTS = ("pgspec", "catalog_relations", "catalog_functions", "type_conversions")

ENTRY_FIELDS = {"case", "job", "since", "reason"}
#: A queue job: numbered (J1275, J1388b) or named (SPARQL-G1, POLYFILL-G3B2).
JOB = re.compile(r"J\d+[a-z]?|[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+")
SINCE = re.compile(r"\d{4}-\d{2}-\d{2}")

#: Where an overload fails and how: an entry of functions.json's list.
AT = ("base", "edge")
KINDS = {"wrong_answer": "wrong_answer", "error": "engine_error"}
FUNCTION_ENTRY_FIELDS = {
    "case",
    "at",
    "kind",
    "calls",
    "statement",
    "edge",
    "said",
    "sqlstate",
    "postgres",
    "job",
    "since",
    "reason",
}
BASE_COUNTS = ("passed", "wrong_answer", "engine_error", "refused_by_name")
EDGE_COUNTS = ("served", "wrong_answer", "engine_error", "documented_limit")


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


def _entries(
    path: Path, entries: list[dict] | None, fields: frozenset[str] | set[str] = ENTRY_FIELDS
) -> dict[str, dict]:
    if not isinstance(entries, list):
        raise Refused(f"{path.name}: expected is not a list")
    out: dict[str, dict] = {}
    for i, entry in enumerate(entries):
        where = f"{path.name} entry {i}"
        if not isinstance(entry, dict):
            raise Refused(f"{where} is not an object")
        unknown = sorted(set(entry) - fields)
        if unknown:
            raise Refused(f"{where} has unknown field(s) {unknown}")
        for name in ("case", "job", "since"):
            if not entry.get(name):
                raise Refused(f"{where} has no {name}: every listed failure names its {name}")
        if not JOB.fullmatch(str(entry["job"])):
            raise Refused(f"{where}: job {entry['job']!r} is not a queue job (J<n> or NAME-N)")
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


def load_functions_expected() -> tuple[Path, dict[str, dict], dict[str, dict]]:
    """The committed functions file: its path, each engine's counts and its list by case. Refused
    when an entry does not say where and how its overload fails, or when the counts are not the
    list's own: the file is written from a run (functions_baseline.py), never by hand."""
    path = EXPECTED / "functions.json"
    doc = _read(path, "expected-failures file")
    engines = doc.get("engines")
    if not isinstance(engines, dict) or not engines:
        raise Refused(f"{path.name}: no engines")
    for engine, counts in engines.items():
        if not isinstance(counts.get("total"), int):
            raise Refused(f"{path.name}: engine {engine} has no total")
        for part, names in (("base", BASE_COUNTS), ("edge", EDGE_COUNTS)):
            held = counts.get(part)
            missing = [
                k for k in names if not isinstance(held, dict) or not isinstance(held.get(k), int)
            ]
            if missing:
                raise Refused(f"{path.name}: engine {engine} has no {part} {missing}")
    listed = _entries(path, doc.get("expected"), FUNCTION_ENTRY_FIELDS)
    tally: dict[tuple[str, str, str], int] = {}
    for case, entry in listed.items():
        engine = case.split("::", 1)[0]
        if engine not in engines:
            raise Refused(f"{path.name}: {case} is of an engine the file does not hold")
        if entry.get("at") not in AT or entry.get("kind") not in KINDS.values():
            raise Refused(f"{path.name}: {case} does not say where (at) and how (kind) it fails")
        if not entry.get("statement") or not entry.get("said"):
            raise Refused(f"{path.name}: {case} does not name its failing call and what it said")
        at_edge = entry["at"] == "edge"
        if at_edge != isinstance(entry.get("calls"), int) or at_edge != bool(entry.get("edge")):
            raise Refused(f"{path.name}: {case}: an edge entry, and no other, has calls and edge")
        key = (engine, entry["at"], entry["kind"])
        tally[key] = tally.get(key, 0) + 1
    for engine, counts in engines.items():
        for at in AT:
            for kind in KINDS.values():
                if counts[at][kind] != tally.get((engine, at, kind), 0):
                    raise Refused(
                        f"{path.name}: {engine} counts {counts[at][kind]} {at} {kind} and lists "
                        f"{tally.get((engine, at, kind), 0)}: regenerate it (functions_baseline.py)"
                    )
    return path, engines, listed


def function_counts(doc: dict, engine: str) -> dict:
    """An engine's counts of a merged run, base calls and edge calls apart. An edge is tried only
    when the base calls are right, so what fails at an edge passes its base calls."""
    got = doc["by_engine"][engine]
    wrong_at_edge, error_at_edge = got["differs_wrong_at_edge"], got["differs_error_at_edge"]
    # A documented limit is counted in no refused or differs key (function_report.result_counts):
    # one found at an edge passed its base calls.
    return {
        "total": got["total"],
        "base": {
            "passed": got["passed"] + wrong_at_edge + error_at_edge + got["documented_at_edge"],
            "wrong_answer": got["differs_wrong_answer"] - wrong_at_edge,
            "engine_error": got["differs_engine_error"] - error_at_edge,
            "refused_by_name": got["refused_by_name"],
        },
        "edge": {
            "served": got["passed"],
            "wrong_answer": wrong_at_edge,
            "engine_error": error_at_edge,
            "documented_limit": got["documented_at_edge"],
        },
    }


def function_failures(doc: dict, engines: Iterable[str]) -> dict[str, dict]:
    """Every wrong answer and engine error of a merged run on ``engines`` that is not a documented
    limit, by case: where it fails (``at``), how (``kind``), and its first failing call as the run
    said it."""
    held = set(engines)
    out: dict[str, dict] = {}
    for fn in doc["functions"]:
        for engine, outcome in fn["outcomes"].items():
            if engine not in held or outcome["outcome"] not in KINDS:
                continue
            if outcome.get("documented_limit"):
                # A documented limit is no failure, as skipped by design is (REQ-2167, 2026-11-23).
                continue
            kind = KINDS[outcome["outcome"]]
            at_edge = bool(outcome.get("wrong_at_edge") or outcome.get("error_at_edge"))
            detail = outcome["detail"]
            entry = {
                "case": f"{engine}::{fn['function']}({fn['overload']})",
                "at": "edge" if at_edge else "base",
                "kind": kind,
            }
            if at_edge:
                edges = outcome["edges"]
                entry["calls"] = (
                    edges["wrong"] if kind == "wrong_answer" else edges["raised"] + edges["errors"]
                )
            entry["statement"] = detail["statement"]
            if at_edge:
                entry["edge"] = detail["edge"]
            entry["said"] = detail["difference"] if kind == "wrong_answer" else detail["error"]
            if kind == "engine_error":
                if detail.get("sqlstate"):
                    entry["sqlstate"] = detail["sqlstate"]
                if at_edge:
                    # _measure_edges: a raise with no PostgreSQL error is one where it answered.
                    entry["postgres"] = detail.get("postgres", "answers")
            out[entry["case"]] = entry
    return out


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


def _state(entry: dict) -> str:
    kind = "a wrong answer" if entry["kind"] == "wrong_answer" else "an engine error"
    return f"{kind} {'at an edge value' if entry['at'] == 'edge' else 'on a base call'}"


def baseline_lines(engines: dict[str, dict]) -> list[str]:
    """Each engine's counts as one line, base calls and edge calls apart."""
    return [
        f"functions on {engine}: {c['total']} overloads; base calls: {c['base']['passed']} right, "
        f"{c['base']['wrong_answer']} wrong, {c['base']['engine_error']} engine errors, "
        f"{c['base']['refused_by_name']} refused by name; edge calls: {c['edge']['served']} "
        f"served, {c['edge']['wrong_answer']} wrong, {c['edge']['engine_error']} engine errors, "
        f"{c['edge']['documented_limit']} documented limits"
        for engine, c in sorted(engines.items())
    ]


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
    measured = {engine: function_counts(doc, engine) for engine in engines}
    for engine, committed in sorted(engines.items()):
        got = measured[engine]
        if got["total"] != committed["total"]:
            raise Refused(
                f"functions on {engine}: {got['total']} of the committed {committed['total']} "
                "overloads: not a full run"
            )
        for count, rises in (("passed", True), ("refused_by_name", False)):
            now, then = got["base"][count], committed["base"][count]
            worse = now < then if rises else now > then
            if worse:
                side = "below" if rises else "above"
                errors.append(
                    f"functions on {engine}: base {count} {now}, {side} the committed {then} "
                    f"({path.name})"
                )
            elif now != then:
                print(
                    f"::notice::functions on {engine}: base {count} {now} (committed {then}): "
                    f"regenerate {path.name}",
                    file=sys.stderr,
                )
    found = function_failures(doc, engines)
    for case in sorted(set(found) - set(listed)):
        now = found[case]
        errors.append(
            f"functions: {case} is {_state(now)} and is not on the expected-failures list "
            f"({path.name}): {now['statement']}: {now['said']}"
        )
    for case in sorted(set(found) & set(listed)):
        now, then = found[case], listed[case]
        if (now["at"], now["kind"]) != (then["at"], then["kind"]):
            errors.append(
                f"functions: {case} is listed as {_state(then)} ({then['job']}) and is now "
                f"{_state(now)}: {now['statement']}: {now['said']}"
            )
        elif now["at"] == "edge" and now["calls"] > then["calls"]:
            errors.append(
                f"functions: {case} fails {now['calls']} edge calls, above the listed "
                f"{then['calls']} ({then['job']}): a new {_state(now)}"
            )
        elif now["at"] == "edge" and now["calls"] < then["calls"]:
            print(
                f"::notice::functions: {case} fails {now['calls']} edge calls (listed "
                f"{then['calls']}): regenerate {path.name}",
                file=sys.stderr,
            )
    errors += [
        f"functions: {case} fixed but still listed ({listed[case]['job']}): take it off {path.name}"
        for case in sorted(set(listed) - set(found))
    ]
    if not errors:
        print("\n".join(baseline_lines(measured)))
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
