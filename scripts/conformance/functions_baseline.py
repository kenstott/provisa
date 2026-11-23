#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f4a61-5c0e-4b7a-9e13-6f0a2c7d9b54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Writes ``expected/functions.json``, the functions measure's expected-failures file, from one
run's merged result (function_support.json, the "function-support" artifact of Conformance
(functions)). The file is what baseline_gate.py judges a run against (REQ-2167), and is never typed
by hand: its counts are the run's, base calls and edge calls apart, and its list holds every wrong
answer and engine error of the run, each naming its engine, overload, where it fails (a base call
or an edge value), its first failing call and what the engine and PostgreSQL said of it.

An entry the file already holds, failing in the same place and way, keeps its job and the day it
was listed; every other entry is new, and takes ``--job`` and ``--since``.

Usage: functions_baseline.py <function_support.json> --job J<n> --since YYYY-MM-DD
           [--run <run id>] [--sha <commit>] [--breakdown <out.md>]

``--breakdown`` also writes the list by engine and function family, for cutting the jobs that
shorten it.
"""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import baseline_gate

#: The longest text an entry keeps of what a call said: a set of rows can run to pages.
SAID_MAX = 300


def _clipped(text: str) -> str:
    return text if len(text) <= SAID_MAX else text[: SAID_MAX - 1] + "…"


def baseline(doc: dict, kept: dict[str, dict], job: str, since: str, measured: dict) -> dict:
    """The expected-failures file of the merged run ``doc``. ``kept``: the entries of the file it
    replaces, by case."""
    unjudged = sorted(set(doc["engines_without_a_result"]) | set(doc["engines_incomplete"]))
    if unjudged:
        raise baseline_gate.Refused(f"no full result on {unjudged}: a baseline is of a full run")
    engines = sorted(doc["engines"])
    entries = []
    for case, found in sorted(baseline_gate.function_failures(doc, engines).items()):
        before = kept.get(case, {})
        same = (before.get("at"), before.get("kind")) == (found["at"], found["kind"])
        entry = {**found, "said": _clipped(found["said"])}
        if "postgres" in entry:
            entry["postgres"] = _clipped(entry["postgres"])
        entry["job"] = before["job"] if same else job
        entry["since"] = before["since"] if same else since
        entries.append(entry)
    return {
        "measured": measured,
        "engines": {engine: baseline_gate.function_counts(doc, engine) for engine in engines},
        "expected": entries,
    }


def written(made: dict) -> str:
    """The file's text: its counts indented, and its list one entry a line, so a change to one
    overload is one line of a diff."""
    head = json.dumps({k: v for k, v in made.items() if k != "expected"}, indent=1)
    rows = ",\n".join("  " + json.dumps(e, ensure_ascii=False) for e in made["expected"])
    return head[:-2] + ',\n "expected": [\n' + rows + "\n ]\n}\n"


def breakdown(doc: dict, entries: list[dict]) -> str:
    """The list by engine and function family, wrong answers first, with each cell's commonest
    edge values: what the jobs that shorten it are cut from."""
    family = {f"{f['function']}({f['overload']})": f["family"] for f in doc["functions"]}
    cells: dict[tuple[str, str, str, str], list[dict]] = {}
    for entry in entries:
        engine, overload = entry["case"].split("::", 1)
        cells.setdefault((engine, entry["at"], entry["kind"], family[overload]), []).append(entry)
    lines = [
        "| engine | where | how | family | overloads | commonest edge values |",
        "|---|---|---|---|---:|---|",
    ]
    for (engine, at, kind, fam), held in sorted(
        cells.items(), key=lambda item: (item[0][0], item[0][1], item[0][2], -len(item[1]))
    ):
        edges = Counter(e["edge"] for e in held if "edge" in e).most_common(4)
        said = ", ".join(f"{edge} ({n})" for edge, n in edges)
        lines.append(f"| {engine} | {at} | {kind} | {fam} | {len(held)} | {said} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Write expected/functions.json from a run.")
    parser.add_argument("report", type=Path)
    parser.add_argument("--job", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--run", default="")
    parser.add_argument("--sha", default="")
    parser.add_argument("--breakdown", type=Path)
    args = parser.parse_args(argv)
    out = baseline_gate.EXPECTED / "functions.json"
    try:
        if not baseline_gate.JOB.fullmatch(args.job):
            raise baseline_gate.Refused(f"job {args.job!r} is not a queue job (J<n> or NAME-N)")
        if not baseline_gate.SINCE.fullmatch(args.since):
            raise baseline_gate.Refused(f"since {args.since!r} is not a date (YYYY-MM-DD)")
        if not args.report.is_file():
            raise baseline_gate.Refused(f"no report: {args.report}")
        doc = json.loads(args.report.read_text())
        before = json.loads(out.read_text()) if out.is_file() else {}
        kept = {e["case"]: e for e in before.get("expected", ())}
        measured = {"run": args.run, "sha": args.sha}
        made = baseline(doc, kept, args.job, args.since, measured)
    except baseline_gate.Refused as refused:
        print(f"::error::{refused}", file=sys.stderr)
        return 1
    out.write_text(written(made))
    print("\n".join(baseline_gate.baseline_lines(made["engines"])))
    print(f"{out}: {len(made['expected'])} listed")
    if args.breakdown:
        args.breakdown.write_text(breakdown(doc, made["expected"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
