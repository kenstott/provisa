# Copyright (c) 2026 Kenneth Stott
# Canary: 7d40b3a2-5e16-49c8-a3f7-c29e16d8f054
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The W3C SPARQL 1.1 test suite against Provisa's /data/sparql (REQ-2167).

Reads the suite's manifests (sparql_w3c_manifest, fetched at a pinned commit, never vendored), and
for each case sends its own query or update, as the job user acting as the role of the case's
dataset (sparql_w3c_fixtures, the data file's objects declared in the fixture model), and checks
the answer:

* a query evaluation test or result format test: the answer, mapped back to the dataset's terms,
  is the suite's expected result (sparql_w3c_results);
* a positive syntax test: 200. A refusal of a legal statement is a failure, whether the parser or
  the lowering refused it;
* a negative syntax test: 400 with the parser's refusal;
* an update evaluation test: the update is accepted and the graph afterwards is the expected one.

A dataset the model cannot state fails its cases with the reason, so no case silently leaves the
count. Every case is passed, skipped by design (by_design/sparql.yaml) or failed.

The results are written as in cypher_tck.py, with ``groups``: the three counts per test group.
The exit status is non-zero only when the suite did not run, until SPARQL is at its baseline
(by_design.AT_BASELINE).

Usage: sparql_w3c.py <sparql/sparql11> <version> <base url> <out.json> <out.md>
       (CONFORMANCE_AUTHORIZATION is the job user's; CONFORMANCE_GROUPS, comma-separated group
       globs, narrows the run)
"""

# Requirements: REQ-2167

from __future__ import annotations

import fnmatch
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import by_design
import sparql_w3c_fixtures as fixtures
import sparql_w3c_manifest as manifest
import sparql_w3c_results as results

#: How the endpoint says a statement is not SPARQL (provisa/sparql_query/compiler.py).
NOT_SPARQL = "is not SPARQL this endpoint reads"
_TIMEOUT = 120


@dataclass
class Outcome:
    feature: str
    scenario: str
    status: str
    reason: str = ""


def post(
    base: str, body: bytes, content_type: str, role: str, accept: str
) -> tuple[int, str, bytes]:
    """(status, content type, body) of one request to the endpoint."""
    request = urllib.request.Request(
        f"{base}/data/sparql",
        data=body,
        method="POST",
        headers={
            "Content-Type": content_type,
            "Accept": accept,
            "X-Provisa-Role": role,
            "Authorization": os.environ["CONFORMANCE_AUTHORIZATION"],
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
            return response.status, response.headers.get("content-type", ""), response.read()
    except urllib.error.HTTPError as err:
        return err.code, err.headers.get("content-type", ""), err.read()


def _refusal(status: int, body: bytes) -> str:
    text = body.decode("utf-8", "replace")
    try:
        doc = json.loads(text)
        text = str(doc.get("detail") or doc.get("reason") or doc.get("error") or doc)
        if isinstance(doc.get("error"), dict):
            text = str(doc["error"].get("reason") or doc["error"].get("message") or text)
    except (ValueError, AttributeError):
        pass
    return f"{status}: {' '.join(text.split())[:400]}"


def adapted_query(text: str, uri: str, fixture: fixtures.Fixture) -> str:
    """The case's query as it runs on the fixture: its constants mapped, its open patterns
    guarded."""
    mapped = results.map_query(text, uri, fixture.term_map())
    return results.guard_open_patterns(
        mapped, fixture.key_columns(), fixture.untyped_classes(), typed=bool(fixture.classes)
    )


def run_query(case: manifest.Case, fixture: fixtures.Fixture, root: Path, base: str) -> str | None:
    """None when the case's query answers its expected result, else why not."""
    assert case.result is not None
    text = case.action.read_text(encoding="utf-8")
    uri = fixtures.data_uri(case.action, root)
    query = adapted_query(text, uri, fixture)
    accept = results.accept_of(case.result)
    expected = results.expected_of(case.result, fixtures.data_uri(case.result, root))
    if expected[0] == "boolean":
        accept = "application/sparql-results+json"
    if expected[0] == "graph":
        accept = "text/turtle"
    status, media, body = post(
        base, query.encode("utf-8"), "application/sparql-query", fixture.role, accept
    )
    if status != 200:
        return _refusal(status, body)
    return results.compare(
        results.answer(body, media, fixture.reverse_map(), fixture.scaffolding()), expected
    )


def run_syntax(case: manifest.Case, role: str, base: str) -> str | None:
    update = case.kind.endswith("update")
    content_type = "application/sparql-update" if update else "application/sparql-query"
    body = case.action.read_bytes()
    status, _media, answer = post(
        base, body, content_type, role, "application/sparql-results+json, text/turtle"
    )
    if case.kind.startswith("positive"):
        return None if status == 200 else _refusal(status, answer)
    if status == 400 and NOT_SPARQL in answer.decode("utf-8", "replace"):
        return None
    return f"a statement that is not SPARQL answered {_refusal(status, answer)}"


def run_update(case: manifest.Case, role: str, base: str) -> str | None:
    status, _media, answer = post(
        base, case.action.read_bytes(), "application/sparql-update", role, "*/*"
    )
    if status not in (200, 204):
        return _refusal(status, answer)
    return "the update was accepted, and this runner has no graph to compare it on"


def run(
    case: manifest.Case,
    fixture_of: dict[tuple[Path, ...], fixtures.Fixture],
    root: Path,
    base: str,
) -> Outcome:
    empty = fixture_of[()]
    try:
        if case.kind == "query":
            fixture = fixture_of.get(case.data, empty)
            if fixture.refusal is not None:
                return Outcome(case.group, case.name, "failed", fixture.refusal)
            why = run_query(case, fixture, root, base)
        elif case.kind == "update":
            why = run_update(case, empty.role, base)
        else:
            why = run_syntax(case, empty.role, base)
    except Exception as exc:  # noqa: BLE001 -- a case the runner cannot finish is failed, by name
        why = f"runner: {type(exc).__name__}: {exc}"
    return Outcome(case.group, case.name, "failed" if why else "passed", why or "")


def report(outcomes: list[Outcome], version: str, spec: str) -> dict:
    declared = by_design.groups("sparql")
    failed = [
        {"feature": o.feature, "scenario": o.scenario, "reason": o.reason}
        for o in outcomes
        if o.status == "failed"
    ]
    failures, excluded = by_design.classify(failed, declared)
    designed = {c["case"] for g in excluded for c in g["cases"]}
    tally: dict[str, dict[str, int]] = {}
    for o in outcomes:
        row = tally.setdefault(o.feature, {"passed": 0, "by_design": 0, "failed": 0})
        if o.status == "passed":
            row["passed"] += 1
        elif f"{o.feature}::{o.scenario}" in designed:
            row["by_design"] += 1
        else:
            row["failed"] += 1
    passed = sum(o.status == "passed" for o in outcomes)
    ran = [
        {"feature": o.feature, "scenario": o.scenario, "reason": o.reason, "outcome": o.status}
        for o in outcomes
        if f"{o.feature}::{o.scenario}" not in designed
    ]
    doc = {
        "surface": "sparql",
        "suite": "W3C SPARQL 1.1 test suite",
        "version": version,
        "tests": "query, update and result formats",
        **by_design.outcomes(passed, excluded, failures),
        "groups": [{"group": g, **c} for g, c in tally.items()],
        "adapted": by_design.adapted(ran, declared),
    }
    if spec.strip():
        doc["filter"] = spec
    return doc


def markdown(doc: dict) -> str:
    lines = [
        f"## SPARQL conformance: {doc['suite']} {doc['version']}",
        "",
        *([f"Filtered to groups: `{doc['filter']}`", ""] if "filter" in doc else []),
        by_design.counts_line(doc),
        "",
        *by_design.group_table(doc["groups"]),
        "",
        "### Skipped by design",
        "",
        *by_design.table(doc["by_design_groups"], requirement=True),
        "",
        "### Failed",
        "",
        "| Group | Test | Reason |",
        "| --- | --- | --- |",
        *(
            f"| {f['feature']} | {f['scenario']} | {f['reason'].replace('|', '/')} |"
            for f in doc["failures"]
        ),
    ]
    return "\n".join(lines) + "\n"


def select(cases: list[manifest.Case], spec: str) -> list[manifest.Case]:
    """The cases of the groups ``spec`` names; all of them when empty. A spec naming none is
    refused, so a typo is never a green run of nothing."""
    globs = [g.strip() for g in spec.split(",") if g.strip()]
    if not globs:
        return cases
    chosen = [c for c in cases if any(fnmatch.fnmatch(c.group, g) for g in globs)]
    if not chosen:
        raise SystemExit(f"no test group matches {spec!r}")
    return chosen


def main(argv: list[str]) -> int:
    suite, version, base, out_json, out_md = argv
    root = Path(suite).resolve()
    cases = manifest.read(root)
    fixture_of = fixtures.fixtures(root, cases)
    spec = os.environ.get("CONFORMANCE_GROUPS", "")
    outcomes = [run(c, fixture_of, root, base) for c in select(cases, spec)]
    doc = report(outcomes, version, spec)
    Path(out_json).write_text(json.dumps(doc, indent=2) + "\n")
    Path(out_md).write_text(markdown(doc))
    print("\n".join(by_design.log_lines(doc)), flush=True)
    if not doc["total"]:
        return 1
    return 1 if by_design.baseline_failed("sparql", doc["failed"]) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
