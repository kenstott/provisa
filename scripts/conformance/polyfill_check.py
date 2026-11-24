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
              (or one of its limitations) states, raised with the row's message and SQLSTATE; a
              difference a limitation of the row states (one stating no error: a limitation that
              states an error excuses that error only; one naming edge values states only a wrong
              answer at those values); or a form served within its bound, each of its edge raises
              an error a limitation of its row states, its SQLSTATE and, where the limitation gives
              it, its message, or one a limitation of the grid states with its message on the
              dialect (a literal's own reading) (REQ-2236, amended 2026-11-15 and 2026-11-22)
  fails       a feature the grid marks native or polyfilled that answers otherwise than PostgreSQL
              (a wrong answer, an error, an edge raise no limitation states, or a refusal the grid
              does not state), a refusal row the run did not raise as the row says, or a bounded
              row (a limitation says what its form raises) whose limitation no edge call of any
              overload of the feature on that engine raised: a bound is credited per feature and
              engine, and a limit no edge call can reach stands as a statement of record the check
              exempts (function_edges.STATEMENTS_OF_RECORD)
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
import re
import sys
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from provisa.transpiler.polyfill import GRID, Applied, Feature, Grid, Refusal  # noqa: E402
from provisa.transpiler.polyfill.grid import Limitation, column_of  # noqa: E402
from provisa.transpiler.polyfill.notation import Form, Refused  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

import function_edges  # noqa: E402
import function_legs  # noqa: E402
import pgwire_function_probe as function_probe  # noqa: E402

#: The dialect Provisa transpiles the SQL of each engine whose backend names its own dialect to
#: (provisa/federation/backend.py EngineBackend.dialect), by the name its leg has (function_legs.yaml).
_FIXED_DIALECTS = {
    "duckdb": "duckdb",
    "motherduck": "duckdb",
    "pg": "postgres",
    "clickhouse": "clickhouse",
    "clickhouse-server": "clickhouse",
    "trino": "trino",
    "snowflake": "snowflake",
    "databricks": "databricks",
    "bigquery": "bigquery",
    "fabric": "tsql",
    "synapse": "tsql",
}


def _engine_dialects() -> dict[str, str]:
    """The dialect Provisa transpiles each engine's SQL to, by the name its leg has: a fixed
    builder's own (``_FIXED_DIALECTS``), and a URL-driven engine's the one its backend reads off
    its URL (``function_legs.url_dialect``, the lookup ``function_legs.refused`` shares). A URL-driven
    engine with no sqlglot dialect (firebird, monetdb) is one ``build_engine`` refuses by name; it is
    named by its native store, which is no grid column (column_of): the grid states nothing of it.
    A unit test holds every leg to an entry."""
    dialects = dict(_FIXED_DIALECTS)
    kinds = function_legs.url_engine_kinds()
    for leg in function_legs.load()["legs"]:
        if leg["engine"] in kinds:
            dialect = function_legs.url_dialect(leg)
            dialects[leg["engine"]] = (
                dialect if dialect is not None else function_legs.native_store(leg)
            )
    return dialects


ENGINE_DIALECTS = _engine_dialects()

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


def _argument_type(argument: str) -> tuple[str, bool]:
    """The type of one argument of an identity argument list (pg_get_function_identity_arguments:
    ``[VARIADIC ][name ]type``), and whether it is VARIADIC. A name is the first word of an
    argument that does not read whole as a type: a type of several words (``timestamp with time
    zone``) reads whole, a named one (``from_json json``) does not."""
    variadic = argument.startswith("VARIADIC ")
    argument = argument.removeprefix("VARIADIC ").strip()
    words = argument.split(" ", 1)
    if len(words) == 2:
        try:
            exp.DataType.build(argument, dialect="postgres")
        except ParseError:
            argument = words[1].strip()
    return argument, variadic


def _cast_null(argtype: str) -> exp.Expr:
    concrete = function_probe.CONCRETE.get(argtype, argtype)
    return exp.Cast(this=exp.Null(), to=exp.DataType.build(concrete, dialect="postgres", udt=True))


def call_of(function: str, overload: str) -> exp.Expr:
    """The run's NULL call of ``function`` (pgwire_function_probe.call): the arguments it gives the
    overload (``ARGUMENTS``), else a NULL of each argument type, a pseudo-type cast to its concrete
    type (``CONCRETE``), and a VARIADIC array passed VARIADIC (a VARIADIC ``"any"`` is one argument
    of its own, pg_proc's provariadic). What the grid reads of it is what it reads of the run's."""
    given = function_probe.ARGUMENTS.get(f"{function}({overload})")
    if given is not None:
        args = [sqlglot.parse_one(arg, read="postgres") for arg in given]
        return exp.Anonymous(this=function, expressions=args)
    arguments = [_argument_type(a.strip()) for a in overload.split(",")] if overload.strip() else []
    args = []
    for argtype, variadic in arguments:
        arg = _cast_null(argtype)
        args.append(exp.Variadic(this=arg) if variadic and argtype != '"any"' else arg)
    return exp.Anonymous(this=function, expressions=args)


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


#: A limitation that states an error its form raises, as gen_measured.bounds writes one ("Raises
#: 22003 at an edge value ...") or a row does ("raises PostgreSQL's 22003"), in either case.
_STATES_A_RAISE = re.compile(r"\braises?\s+(?:PostgreSQL's\s+)?[0-9A-Z]{5}\b", re.IGNORECASE)

#: A limitation that names the edge values its form answers otherwise at (gen_measured.bounds).
_ANSWERS_AT = re.compile(r"Answers otherwise than PostgreSQL at an edge value: (?P<values>.*)\.\Z")


def _differences(feature: Feature, dialect: str) -> list[Limitation]:
    """The limitations of the row on ``dialect`` that state a difference: neither a refusal nor an
    error its form raises. A limitation stating an error excuses that error only, never a wrong
    answer (REQ-2236; REQ-2167, amended 2026-11-23: a limitation excuses only the failure it
    states)."""
    return [
        lim
        for lim in feature.read.limitations.get(dialect, ())
        if lim.refusal is None and _STATES_A_RAISE.search(lim.text) is None
    ]


def _names(values: str, value: str) -> bool:
    return f", {value}, " in f", {values}, "


def _difference_stated(feature: Feature, dialect: str, outcome: dict[str, Any]) -> bool:
    """Whether a limitation of the row on ``dialect`` states the wrong answer of ``outcome``. One
    that states a difference of no particular value states any; one that names edge values states
    only a wrong answer at an edge each of whose wrong calls (``edge_wrong``) is at one of them: a
    limitation excuses only its own values."""
    differences = _differences(feature, dialect)
    if any(_ANSWERS_AT.fullmatch(lim.text) is None for lim in differences):
        return True
    if not differences or not outcome.get("wrong_at_edge"):
        return False
    stated = [match["values"] for lim in differences if (match := _ANSWERS_AT.fullmatch(lim.text))]
    return all(
        any(
            _names(values, function_edges.edge_value(w["edge"], w["statement"]))
            for values in stated
        )
        for w in outcome["edge_wrong"]
    )


@dataclass(frozen=True)
class Bound:
    """What the run held of a row's stated bounds on one engine: the errors any limitation of the
    grid states on its dialect with a message (``function_edges.stated_by_dialect``), and the
    bounded limitations of the row no edge call of any overload of the feature raised, but for
    statements of record (``function_edges.STATEMENTS_OF_RECORD``)."""

    elsewhere: dict[tuple[str, str], str]
    unheld: tuple[Limitation, ...] = ()


_NO_BOUND = Bound({})


def _unstated_raises(
    feature: Feature, dialect: str, outcome: dict[str, Any], bound: Bound
) -> list[dict[str, Any]]:
    """The edge raises of ``outcome`` no limitation of its row on ``dialect`` states, nor any of
    the grid's on ``dialect`` with its message (a literal's own reading)."""
    limitations = feature.read.limitations.get(dialect, ())
    return [
        raised
        for raised in outcome.get("edge_raised", ())
        if not any(function_edges.excuses(lim, raised) for lim in limitations)
        and function_edges.raised_error(raised) not in bound.elsewhere
    ]


def judged(
    applied: Applied, dialect: str, outcome: dict[str, Any], bound: Bound = _NO_BOUND
) -> tuple[str, str]:
    """The verdict on one outcome of a grid row on ``dialect``, and why."""
    kind = outcome["outcome"]
    form = applied.form
    feature = applied.feature
    if kind in _NOTHING_COMPARED:
        return UNVERIFIED, f"the run compared nothing ({kind})"
    if bound.unheld and "edges" in outcome and kind in ("pass", "within_bound"):
        stated = sorted(
            {e.sqlstate for lim in bound.unheld for e in function_edges.bound_errors(lim)}
        )
        return FAILS, (
            f"its row states it raises {', '.join(stated)} outside its bound; no edge call of "
            f"any overload of {feature.name} raised it"
        )
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
    if kind == "within_bound":
        unstated = _unstated_raises(feature, dialect, outcome, bound)
        if unstated:
            sqlstate, message = function_edges.raised_error(unstated[0])
            return FAILS, (
                f"raises at an edge value an error no limitation of its row states: "
                f"{sqlstate or '(no SQLSTATE)'} {message!r}"
            )
        return HOLDS, "raises at an edge value, as a limitation of its row states"
    if kind == "refused":
        if any(_raised(refusal, outcome) for refusal in _stated_refusals(feature, dialect)):
            return HOLDS, "a refusal a limitation of its row states"
        message, sqlstate = _error(outcome)
        return (
            FAILS,
            f"a refusal the grid does not state: {sqlstate or '(no SQLSTATE)'} {message!r}",
        )
    if kind == "wrong_answer" and _difference_stated(feature, dialect, outcome):
        return HOLDS, "a difference a limitation of its row states"
    if kind == "wrong_answer":
        return FAILS, f"marked {_said(form)}, it answers otherwise than PostgreSQL"
    message, _ = _error(outcome)
    return FAILS, f"marked {_said(form)}, it fails: {message!r}"


def _statement(outcome: dict[str, Any]) -> str:
    detail = outcome.get("detail")
    return str(detail.get("statement", "")) if isinstance(detail, dict) else ""


def checked(run: dict[str, Any], grid: Grid = GRID) -> list[Finding]:
    """Every overload of a grid row in ``run`` on each of its engines, judged. A row's stated bound
    is credited per feature and engine (``_held``)."""
    unknown = sorted(set(run["engines"]) - set(ENGINE_DIALECTS))
    if unknown:
        raise SystemExit(f"engines the check has no dialect of: {unknown}")
    read: list[tuple[dict[str, Any], str, str, dict[str, Any], Applied | Refused]] = []
    for entry in run["functions"]:
        node = call_of(entry["function"], entry["overload"])
        for engine, outcome in sorted(entry["outcomes"].items()):
            dialect = ENGINE_DIALECTS[engine]
            if column_of(dialect) is None:
                # No column of the grid: it states nothing of the engine to hold it to.
                continue
            try:
                applied = grid.read(node, dialect)
            except Refused as refused:
                # A rewrite function refusing the call for its values (notation.Refused): the row
                # is not named, its refusal is what it states of the call.
                read.append((entry, engine, dialect, outcome, refused))
                continue
            if applied is not None:
                read.append((entry, engine, dialect, outcome, applied))
    elsewhere = {
        dialect: function_edges.stated_by_dialect(grid, dialect) for _, _, dialect, _, _ in read
    }
    held = _held(read)
    findings: list[Finding] = []
    for entry, engine, dialect, outcome, applied in read:
        if isinstance(applied, Refused):
            findings.append(_refused_by_rewrite(entry, engine, dialect, outcome, applied))
            continue
        feature = applied.feature
        unheld = tuple(
            lim
            for lim in feature.read.limitations.get(dialect, ())
            if function_edges.bound_errors(lim)
            and not function_edges.of_record(feature.name, dialect, lim)
            and (feature.name, engine, lim) not in held
        )
        verdict, why = judged(applied, dialect, outcome, Bound(elsewhere[dialect], unheld))
        findings.append(
            Finding(
                feature=feature.name,
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


#: The outcomes of function_support.py that fail (function_report.verdict): a failure the grid
#: states is a documented limit.
_FAILING = frozenset({"refused", "wrong_answer", "error"})


def documented_limits(run: dict[str, Any], grid: Grid = GRID) -> dict[tuple[str, str, str], str]:
    """The documented limits of ``run`` (REQ-2167, amended 2026-11-23): each failing outcome that
    fails exactly as a stated limitation of its feature on its engine says it will (REQ-2236), the
    check holding it (``judged``): a refusal the row or one of its limitations states with its
    message and SQLSTATE, a refusal a rewrite function states for those values, or a wrong answer a
    limitation stating a difference covers. By (function, overload, engine), with why."""
    return {
        (f.function, f.overload, f.engine): f.why
        for f in checked(run, grid)
        if f.verdict == HOLDS and f.outcome in _FAILING
    }


def _held(
    read: Iterable[tuple[dict[str, Any], str, str, dict[str, Any], Applied | Refused]],
) -> set[tuple[str, str, Limitation]]:
    """The bounded limitations each feature held on each engine: (feature, engine, limitation) for
    each one any overload of the feature raised an error of at an edge (REQ-2236, amended
    2026-11-22: a stated limit is credited per feature and engine)."""
    held: set[tuple[str, str, Limitation]] = set()
    for _, engine, dialect, outcome, applied in read:
        if isinstance(applied, Refused):
            continue
        feature = applied.feature
        for lim in feature.read.limitations.get(dialect, ()):
            if any(function_edges.bound_excuses(lim, r) for r in outcome.get("edge_raised", ())):
                held.add((feature.name, engine, lim))
    return held


#: The feature a finding names when a rewrite function refused the call before naming its row.
REWRITE_REFUSAL = "(a rewrite function's refusal)"


def _refused_by_rewrite(
    entry: dict[str, Any], engine: str, dialect: str, outcome: dict[str, Any], refused: Refused
) -> Finding:
    """The verdict on an overload a rewrite function refuses as the run calls it: it holds where
    the run raised that refusal, and fails where the run did anything else it compared."""
    kind = outcome["outcome"]
    if kind in _NOTHING_COMPARED:
        verdict, why = UNVERIFIED, f"the run compared nothing ({kind})"
    elif kind == "refused" and _raised(refused.refusal, outcome):
        verdict, why = HOLDS, "refused as its rewrite function says"
    else:
        message, sqlstate = _error(outcome)
        verdict, why = (
            FAILS,
            (
                f"a rewrite function refuses it with {refused.refusal.sqlstate} "
                f"{refused.refusal.message!r}; the run: {kind}"
                + (f" {sqlstate} {message!r}" if message else "")
            ),
        )
    return Finding(
        feature=REWRITE_REFUSAL,
        function=entry["function"],
        overload=entry["overload"],
        engine=engine,
        dialect=dialect,
        grid="refused",
        outcome=kind,
        verdict=verdict,
        why=why,
        statement=_statement(outcome),
    )


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
