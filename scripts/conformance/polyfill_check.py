#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e8a3f-7b21-4d96-a1e4-93f2c6d8b157
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The polyfill grid checked against a Conformance (functions) run (REQ-2236): the conformance runs
check the grid.

Reads function_support.json, the merged result of scripts/conformance/function_support.py, whose
``functions`` hold each overload's outcome on each engine of the run. Each overload is looked up in
the grid as the translator looks up a call of it (``GRID.read`` of the call with each argument of
its type), on the dialect of each engine, and its outcome is held to what the row says there:

  holds       the row's native or polyfilled form answered as PostgreSQL does; a refusal the row
              (or one of its limitations) states, raised with the row's message and SQLSTATE; or
              a difference a limitation of the row states
  fails       a feature the grid marks native or polyfilled that answers otherwise than PostgreSQL
              (a wrong answer, an error, or a refusal the grid does not state), or a refusal row
              the run did not raise as the row says
  unverified  the run compared nothing of it: not checked, skipped by design, not callable, or
              not reached

A function the grid has no row of is not the grid's, and not read. The workflow reports the check
and does not gate on it yet.

Usage:
  python scripts/conformance/polyfill_check.py --in function_support.json \\
      --out-json polyfill-check.json --out-md polyfill-check.md
"""

# Requirements: REQ-2236

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import sqlglot.expressions as exp

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from provisa.transpiler.polyfill import GRID, Applied, Feature, Grid, Refusal  # noqa: E402
from provisa.transpiler.polyfill.grid import DIALECTS  # noqa: E402
from provisa.transpiler.polyfill.notation import Form  # noqa: E402

#: The dialect of each engine a Conformance (functions) run measures, by the name its leg has.
ENGINE_DIALECTS = {
    "duckdb": "duckdb",
    "pg": "postgres",
    "clickhouse": "clickhouse",
    "trino": "trino",
}

HOLDS = "holds"
FAILS = "fails"
UNVERIFIED = "unverified"

#: The outcomes of function_support.py that compared nothing.
_NOTHING_COMPARED = frozenset({"not_checked", "by_design", "not_callable", "not_reached"})


@dataclass(frozen=True)
class Finding:
    """One overload of a grid row on one engine of the run, and what the check made of it."""

    feature: str
    function: str
    overload: str
    engine: str
    dialect: str
    #: What the grid says of it there: native, polyfilled or refused.
    grid: str
    outcome: str
    verdict: str
    why: str
    statement: str = ""


def call_of(function: str, overload: str) -> exp.Expr:
    """A call of ``function`` with one argument of each type ``overload`` lists, as the run calls it
    (pg_catalog."f"(NULL::t, ...)): what the grid reads of it is what it reads of the run's."""
    types = [t.strip() for t in overload.split(",")] if overload.strip() else []
    return exp.Anonymous(
        this=function,
        expressions=[
            exp.Cast(this=exp.Null(), to=exp.DataType.build(t, dialect="postgres", udt=True))
            for t in types
        ],
    )


def _said(form: Form | None) -> str:
    if form is None:
        return "native"
    return "refused" if isinstance(form, Refusal) else "polyfilled"


def _error(outcome: dict[str, Any]) -> tuple[str, str]:
    """The primary message and the SQLSTATE of a failed call's detail."""
    detail = outcome.get("detail")
    if not isinstance(detail, dict):
        return "", ""
    error = str(detail.get("error", "")).split("\n", 1)[0]
    return error.removeprefix("ERROR:").strip(), str(detail.get("sqlstate", ""))


def _raised(refusal: Refusal, outcome: dict[str, Any]) -> bool:
    """Whether the run's refusal is ``refusal``: its message, and its SQLSTATE where the run kept
    one."""
    message, sqlstate = _error(outcome)
    return message == refusal.message and sqlstate in ("", refusal.sqlstate)


def _stated_refusals(feature: Feature, dialect: str) -> list[Refusal]:
    return [
        limitation.refusal
        for limitation in feature.read.limitations.get(dialect, ())
        if limitation.refusal is not None
    ]


def _stated_differences(feature: Feature, dialect: str) -> bool:
    return any(lim.refusal is None for lim in feature.read.limitations.get(dialect, ()))


def judged(applied: Applied, dialect: str, outcome: dict[str, Any]) -> tuple[str, str]:
    """The verdict on one outcome of a grid row on ``dialect``, and why."""
    kind = outcome["outcome"]
    form = applied.form
    feature = applied.feature
    if kind in _NOTHING_COMPARED:
        return UNVERIFIED, f"the run compared nothing ({kind})"
    if isinstance(form, Refusal):
        if kind == "refused" and _raised(form, outcome):
            return HOLDS, "refused as its row says"
        if kind == "pass":
            return FAILS, "its row refuses it, and it answered as PostgreSQL does"
        message, sqlstate = _error(outcome)
        return FAILS, (
            f"its row refuses it with {form.sqlstate} {form.message!r}; the run: {kind}"
            + (f" {sqlstate} {message!r}" if message else "")
        )
    if kind == "pass":
        return HOLDS, "answered as PostgreSQL does"
    if kind == "refused":
        if any(_raised(refusal, outcome) for refusal in _stated_refusals(feature, dialect)):
            return HOLDS, "a refusal a limitation of its row states"
        message, sqlstate = _error(outcome)
        return (
            FAILS,
            f"a refusal the grid does not state: {sqlstate or '(no SQLSTATE)'} {message!r}",
        )
    if kind == "wrong_answer" and _stated_differences(feature, dialect):
        return HOLDS, "a difference a limitation of its row states"
    if kind == "wrong_answer":
        return FAILS, f"marked {_said(form)}, it answers otherwise than PostgreSQL"
    message, _ = _error(outcome)
    return FAILS, f"marked {_said(form)}, it fails: {message!r}"


def _statement(outcome: dict[str, Any]) -> str:
    detail = outcome.get("detail")
    return str(detail.get("statement", "")) if isinstance(detail, dict) else ""


def checked(run: dict[str, Any], grid: Grid = GRID) -> list[Finding]:
    """Every overload of a grid row in ``run`` on each of its engines, judged."""
    unknown = sorted(set(run["engines"]) - set(ENGINE_DIALECTS))
    if unknown:
        raise SystemExit(f"engines the check has no dialect of: {unknown}")
    findings: list[Finding] = []
    for entry in run["functions"]:
        node = call_of(entry["function"], entry["overload"])
        for engine, outcome in sorted(entry["outcomes"].items()):
            dialect = ENGINE_DIALECTS[engine]
            assert dialect in DIALECTS, dialect
            applied = grid.read(node, dialect)
            if applied is None:
                continue
            verdict, why = judged(applied, dialect, outcome)
            findings.append(
                Finding(
                    feature=applied.feature.name,
                    function=entry["function"],
                    overload=entry["overload"],
                    engine=engine,
                    dialect=dialect,
                    grid=_said(applied.form),
                    outcome=outcome["outcome"],
                    verdict=verdict,
                    why=why,
                    statement=_statement(outcome),
                )
            )
    return findings


def _features_unmeasured(findings: Iterable[Finding], grid: Grid) -> list[str]:
    measured = {f.feature for f in findings}
    return sorted(f.name for f in grid.features if f.name not in measured)


def report(findings: list[Finding], grid: Grid = GRID) -> dict[str, Any]:
    by_engine: dict[str, Counter[str]] = {}
    for finding in findings:
        by_engine.setdefault(finding.engine, Counter())[finding.verdict] += 1
    return {
        "counts": {engine: dict(sorted(c.items())) for engine, c in sorted(by_engine.items())},
        "failures": sum(f.verdict == FAILS for f in findings),
        "features_not_in_the_run": _features_unmeasured(findings, grid),
        "findings": [asdict(f) for f in findings],
    }


def _cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def markdown(doc: dict[str, Any]) -> str:
    lines = [
        "## Polyfill grid against the run (REQ-2236)",
        "",
        "Each overload of a grid row, on each engine of the run, held to what its row says"
        " there. Reported, not a gate yet.",
        "",
        "| Engine | holds | fails | unverified |",
        "| --- | ---: | ---: | ---: |",
    ]
    for engine, counts in doc["counts"].items():
        lines.append(
            f"| {engine} | {counts.get(HOLDS, 0)} | {counts.get(FAILS, 0)} |"
            f" {counts.get(UNVERIFIED, 0)} |"
        )
    failures = [f for f in doc["findings"] if f["verdict"] == FAILS]
    lines += ["", f"Failures: {len(failures)}.", ""]
    if failures:
        lines += [
            "| Feature | Overload | Engine | Grid | Outcome | Why | Statement |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        lines += [
            f"| `{f['feature']}` | `{f['function']}({f['overload']})` | {f['engine']} |"
            f" {f['grid']} | {f['outcome']} | {_cell(f['why'])} | `{_cell(f['statement'])}` |"
            for f in failures
        ]
    if doc["features_not_in_the_run"]:
        lines += [
            "",
            "Features of the grid with no overload in the run: "
            + ", ".join(f"`{name}`" for name in doc["features_not_in_the_run"])
            + ".",
        ]
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="The polyfill grid checked against a run.")
    parser.add_argument("--in", dest="infile", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args(argv[1:])
    doc = report(checked(json.loads(args.infile.read_text())))
    args.out_json.write_text(json.dumps(doc, indent=1) + "\n")
    text = markdown(doc)
    args.out_md.write_text(text)
    print(text)
    return 1 if doc["failures"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
