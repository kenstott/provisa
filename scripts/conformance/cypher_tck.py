# Copyright (c) 2026 Kenneth Stott
# Canary: 4e9c1a73-2d58-4b06-8f21-7a3c5e9d1b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The openCypher TCK against Provisa's Cypher over HTTP (REQ-2167).

Reads the TCK's feature files (fetched at a pinned tag, never vendored), runs each scenario's query
against ``POST /data/cypher`` and compares its answer with the scenario's expectation. Writes:

* a JSON document -- suite, version, totals, pass rate and every failing scenario with its reason
  -- uploaded as the run's ``conformance-cypher.json``, and read by
  scripts/conformance/fill_docs.py into docs/conformance/cypher.md;
* the same as Markdown, for the job summary.

A scenario whose setup builds a graph (``having executed`` a CREATE, a named graph, a test
procedure) cannot run: a Provisa graph's labels and relationship types are the model's, fixed
when it is declared, not created by a statement. Those are counted apart, as not runnable by
design, and named with the setup that needs it. Every other scenario passes or fails; a failure
is data. An error the scenario expects (a SyntaxError, a TypeError, ...) passes when the query is
refused; the error's type is not compared, since Provisa names its own.

The exit status is non-zero only when the suite did not run (no scenario passed or failed).

Usage: cypher_tck.py <features dir> <version> <base url> <role> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import math
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import by_design

# --- the feature files -------------------------------------------------------------------------


@dataclass
class Step:
    keyword: str
    text: str
    doc: str | None = None
    table: list[list[str]] | None = None


@dataclass
class Scenario:
    feature: str
    name: str
    steps: list[Step] = field(default_factory=list)


_STEP = re.compile(r"^\s*(Given|When|Then|And|But)\s+(.*?)\s*:?\s*$")


def _cells(line: str) -> list[str]:
    inner = line.strip()[1:-1]
    cells, cur, quote = [], [], None
    for ch in inner:
        if quote:
            cur.append(ch)
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
            cur.append(ch)
        elif ch == "|":
            cells.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    cells.append("".join(cur).strip())
    return cells


def parse_feature(path: Path) -> list[Scenario]:
    lines = path.read_text().splitlines()
    feature = ""
    out: list[Scenario] = []
    outline: Scenario | None = None
    current: Scenario | None = None
    current_steps: list[Step] = []
    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.strip()
        if line.startswith("Feature:"):
            feature = line[len("Feature:") :].strip()
        elif line.startswith("Scenario Outline:") or line.startswith("Scenario:"):
            name = line.split(":", 1)[1].strip()
            current = Scenario(feature, name)
            if line.startswith("Scenario Outline:"):
                outline, current = current, None
                current_steps = outline.steps
            else:
                outline = None
                out.append(current)
                current_steps = current.steps
        elif line.startswith("Examples:"):
            i += 1
            rows: list[list[str]] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(_cells(lines[i]))
                i += 1
            assert outline is not None
            header, *values = rows
            for n, row in enumerate(values):
                bound = dict(zip(header, row, strict=True))

                def put(text: str | None, bound=bound) -> str | None:
                    if text is None:
                        return None
                    for k, v in bound.items():
                        text = text.replace(f"<{k}>", v)
                    return text

                out.append(
                    Scenario(
                        feature,
                        f"{outline.name} #{n + 1}",
                        [
                            Step(
                                s.keyword,
                                put(s.text) or "",
                                put(s.doc),
                                [[put(c) or "" for c in r] for r in s.table] if s.table else None,
                            )
                            for s in outline.steps
                        ],
                    )
                )
            continue
        elif (m := _STEP.match(raw)) and (current is not None or outline is not None):
            step = Step(m.group(1), m.group(2))
            if i + 1 < len(lines) and lines[i + 1].strip() == '"""':
                i += 2
                doc: list[str] = []
                while lines[i].strip() != '"""':
                    doc.append(lines[i])
                    i += 1
                step.doc = "\n".join(doc).strip()
            elif i + 1 < len(lines) and lines[i + 1].strip().startswith("|"):
                rows = []
                while i + 1 < len(lines) and lines[i + 1].strip().startswith("|"):
                    i += 1
                    rows.append(_cells(lines[i]))
                step.table = rows
            current_steps.append(step)
        i += 1
    return out


# --- TCK values --------------------------------------------------------------------------------


class _Value:
    """A reader of the TCK's value notation: Cypher literals, nodes, relationships and paths."""

    def __init__(self, text: str) -> None:
        self.s = text
        self.i = 0

    def ws(self) -> None:
        while self.i < len(self.s) and self.s[self.i].isspace():
            self.i += 1

    def peek(self) -> str:
        self.ws()
        return self.s[self.i] if self.i < len(self.s) else ""

    def eat(self, ch: str) -> None:
        if self.peek() != ch:
            raise ValueError(f"expected {ch!r} at {self.i} in {self.s!r}")
        self.i += 1

    def read(self) -> Any:
        ch = self.peek()
        if ch == "[":
            return self._list_or_relationship()
        if ch == "{":
            return self._map()
        if ch == "(":
            return ("node", self._element(")"))
        if ch == "<":
            self.i += 1
            depth = 1
            start = self.i
            while depth:
                depth += {"<": 1, ">": -1}.get(self.s[self.i], 0)
                self.i += 1
            return ("path", self.s[start : self.i - 1])
        if ch in "'\"":
            return self._string(ch)
        word = re.match(r"-?[A-Za-z0-9_.+]+(?:[eE][-+]?\d+)?", self.s[self.i :])
        if word is None:
            raise ValueError(f"unreadable value at {self.i} in {self.s!r}")
        tok = word.group(0)
        self.i += len(tok)
        low = tok.lower()
        if low == "null":
            return None
        if low in ("true", "false"):
            return low == "true"
        if low in ("nan",):
            return math.nan
        if low in ("inf", "infinity"):
            return math.inf
        if low in ("-inf", "-infinity"):
            return -math.inf
        if re.fullmatch(r"-?\d+", tok):
            return int(tok)
        if re.fullmatch(r"-?0x[0-9a-fA-F]+", tok):
            return int(tok, 16)
        return float(tok)

    def _string(self, quote: str) -> str:
        self.i += 1
        out = []
        while self.s[self.i] != quote:
            if self.s[self.i] == "\\":
                self.i += 1
                out.append({"n": "\n", "t": "\t"}.get(self.s[self.i], self.s[self.i]))
            else:
                out.append(self.s[self.i])
            self.i += 1
        self.i += 1
        return "".join(out)

    def _list_or_relationship(self) -> Any:
        self.eat("[")
        if self.peek() == ":":
            self.i -= 1
            return ("relationship", self._element("]"))
        items = []
        while self.peek() != "]":
            items.append(self.read())
            if self.peek() == ",":
                self.i += 1
        self.eat("]")
        return items

    def _map(self) -> dict:
        self.eat("{")
        out = {}
        while self.peek() != "}":
            key = re.match(r"\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s*:", self.s[self.i :])
            if key is None:
                raise ValueError(f"unreadable map key at {self.i} in {self.s!r}")
            self.i += key.end()
            out[key.group(1)] = self.read()
            if self.peek() == ",":
                self.i += 1
        self.eat("}")
        return out

    def _element(self, close: str) -> str:
        start = self.i
        depth = 0
        while True:
            ch = self.s[self.i]
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
                if depth == 0 and ch == close:
                    self.i += 1
                    return self.s[start : self.i]
            self.i += 1


def tck_value(text: str) -> Any:
    reader = _Value(text)
    value = reader.read()
    reader.ws()
    if reader.i != len(reader.s):
        raise ValueError(f"trailing text in {text!r}")
    return value


def _same(expected: Any, got: Any, ignore_list_order: bool) -> bool:
    if isinstance(expected, tuple):
        return False  # a node, relationship or path: not compared (see the module docstring)
    if expected is None or isinstance(expected, bool):
        return got is expected or got == expected and type(got) is type(expected)
    if isinstance(expected, int):
        return type(got) is int and got == expected
    if isinstance(expected, float):
        if not isinstance(got, (int, float)) or isinstance(got, bool):
            return False
        if math.isnan(expected):
            return isinstance(got, float) and math.isnan(got)
        return math.isclose(float(got), expected, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(expected, str):
        return got == expected
    if isinstance(expected, list):
        if not isinstance(got, list) or len(got) != len(expected):
            return False
        if ignore_list_order:
            rest = list(got)
            for e in expected:
                hit = next((n for n, g in enumerate(rest) if _same(e, g, True)), None)
                if hit is None:
                    return False
                rest.pop(hit)
            return True
        return all(_same(e, g, False) for e, g in zip(expected, got, strict=True))
    if isinstance(expected, dict):
        return (
            isinstance(got, dict)
            and set(got) == set(expected)
            and all(_same(v, got[k], ignore_list_order) for k, v in expected.items())
        )
    return False


# --- running a scenario ------------------------------------------------------------------------

#: A setup that builds a graph: Provisa's graph is the model's, so the scenario cannot run.
_NOT_RUNNABLE = (
    ("having executed", "builds its graph with a statement"),
    ("there exists a procedure", "declares a test procedure"),
)


@dataclass
class Outcome:
    feature: str
    scenario: str
    status: str  # passed | failed | not_runnable
    reason: str = ""


#: The credential every request presents (main sets it from its argument): the job user's
#: "Bearer <token>", as the instance's password auth takes it.
_CREDENTIAL: dict[str, str] = {}


def _post(base: str, role: str, query: str, params: dict) -> tuple[int, Any]:
    req = urllib.request.Request(
        f"{base}/data/cypher",
        data=json.dumps({"query": query, "params": params}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role, **_CREDENTIAL},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"null")


def _rows(body: Any) -> tuple[list[str], list[list[Any]]]:
    columns = list(body.get("columns") or [])
    rows = body.get("rows") or []
    return columns, [[r.get(c) for c in columns] if isinstance(r, dict) else r for r in rows]


def run(scenario: Scenario, base: str, role: str) -> Outcome:
    def out(status: str, reason: str = "") -> Outcome:
        return Outcome(scenario.feature, scenario.name, status, reason)

    params: dict = {}
    query: str | None = None
    for step in scenario.steps:
        text = step.text
        if step.keyword == "Given" or (step.keyword == "And" and query is None):
            if text in ("an empty graph", "any graph"):
                continue
            if text == "parameters are" and step.table:
                params = {r[0]: tck_value(r[1]) for r in step.table}
                continue
            why = next((w for marker, w in _NOT_RUNNABLE if text.startswith(marker)), None)
            return out("not_runnable", f"its setup {why or 'uses a named TCK graph'}")
        if text.startswith("executing query"):
            query = step.doc or ""
            continue
        assert query is not None
        if text.startswith("executing control query"):
            return out("not_runnable", "checks state with a control query")
        if "should be raised" in text:
            status, body = _post(base, role, query, params)
            if status >= 400:
                return out("passed")
            return out("failed", f"expected {text.split(' should')[0][2:]}; the query ran")
        if text.startswith("the result should be"):
            status, body = _post(base, role, query, params)
            if status >= 400:
                detail = (body or {}).get("detail") if isinstance(body, dict) else body
                return out("failed", f"refused ({status}): {str(detail)[:200]}")
            columns, rows = _rows(body)
            if text == "the result should be empty":
                return out("passed") if not rows else out("failed", f"{len(rows)} row(s) returned")
            assert step.table is not None
            header, *expected_raw = step.table
            if columns != header:
                return out("failed", f"columns {columns} != {header}")
            try:
                expected = [[tck_value(c) for c in r] for r in expected_raw]
            except ValueError as exc:
                return out("failed", f"expected value unreadable: {exc}")
            if any(isinstance(v, tuple) for r in expected for v in r):
                return out("failed", "returns nodes, relationships or paths (not compared)")
            ignore = "ignoring element order" in text
            ordered = "in order" in text
            if len(rows) != len(expected):
                return out("failed", f"{len(rows)} row(s), expected {len(expected)}")
            if ordered:
                good = all(
                    all(_same(e, g, ignore) for e, g in zip(er, gr, strict=True))
                    for er, gr in zip(expected, rows, strict=True)
                )
            else:
                rest = list(rows)
                good = True
                for er in expected:
                    hit = next(
                        (
                            n
                            for n, gr in enumerate(rest)
                            if all(_same(e, g, ignore) for e, g in zip(er, gr, strict=True))
                        ),
                        None,
                    )
                    if hit is None:
                        good = False
                        break
                    rest.pop(hit)
            if good:
                return out("passed")
            if _below_the_microsecond(header, expected_raw):
                return out(
                    "failed", f"expects digits below the microsecond; rows differ: got {rows[:3]}"
                )
            return out("failed", f"rows differ: got {rows[:3]}")
    return out("failed", "the scenario states no expectation this runner reads")


_SUB_MICROSECOND = re.compile(r"\.\d{6}\d*[1-9]")


def _below_the_microsecond(header: list[str], rows: list[list[str]]) -> bool:
    """Whether an expected result writes digits below the microsecond: a fraction of a second
    past six digits, or a nanosecond count that is not a whole number of microseconds."""
    for row in rows:
        for column, cell in zip(header, row, strict=True):
            if _SUB_MICROSECOND.search(cell):
                return True
            if "nanosecond" in column.lower() and cell.lstrip("-").isdigit() and int(cell) % 1000:
                return True
    return False


# --- the report --------------------------------------------------------------------------------


def report(outcomes: list[Outcome], version: str) -> dict:
    """The run as the conformance page reads it (REQ-2167, amended 2026-10-27): a scenario not
    run, or failed, because of a designed limitation is not runnable by design in a group that
    names its decision (by_design/cypher.yaml) and is out of the pass rate; one with no decision
    behind it is a failure."""
    passed = sum(o.status == "passed" for o in outcomes)
    unmet = [
        {"feature": o.feature, "scenario": o.scenario, "reason": o.reason}
        for o in outcomes
        if o.status in ("failed", "not_runnable")
    ]
    failures, excluded = by_design.classify(unmet, by_design.groups("cypher"))
    ran = passed + len(failures)
    return {
        "surface": "cypher",
        "suite": "openCypher TCK",
        "version": version,
        "total": len(outcomes),
        "ran": ran,
        "passed": passed,
        "failed": len(failures),
        "not_runnable": sum(g["count"] for g in excluded),
        "by_design": excluded,
        "pass_rate": round(passed / ran, 4) if ran else None,
        "failures": failures,
    }


def markdown(doc: dict) -> str:
    rate = "n/a" if doc["pass_rate"] is None else f"{doc['pass_rate']:.1%}"
    lines = [
        f"## Cypher conformance: {doc['suite']} {doc['version']}",
        "",
        f"Passed {doc['passed']} of {doc['ran']} scenarios run ({rate}); "
        f"{doc['not_runnable']} not runnable by design.",
        "",
        *by_design.table(doc["by_design"]),
        "",
        "| Feature | Scenario | Reason |",
        "| --- | --- | --- |",
        *(
            f"| {f['feature']} | {f['scenario']} | {f['reason'].replace('|', '/')} |"
            for f in doc["failures"]
        ),
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    features, version, base, role, authorization, out_json, out_md = argv
    _CREDENTIAL["Authorization"] = authorization
    scenarios = [s for p in sorted(Path(features).rglob("*.feature")) for s in parse_feature(p)]
    outcomes = []
    for s in scenarios:
        try:
            outcomes.append(run(s, base, role))
        except (OSError, ValueError, KeyError) as exc:
            outcomes.append(Outcome(s.feature, s.name, "failed", f"runner: {exc}"))
    doc = report(outcomes, version)
    Path(out_json).write_text(json.dumps(doc, indent=2) + "\n")
    Path(out_md).write_text(markdown(doc))
    return 0 if doc["ran"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
