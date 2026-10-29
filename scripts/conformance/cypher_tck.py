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

A scenario whose setup builds a graph (``having executed`` a CREATE, a named graph) runs its
setup and its query in one Bolt session (cypher_tck_bolt), a named graph's setup being its CREATE
script from the TCK's graphs directory; one that declares a test procedure has the procedure
declared as a command of the fixture model (cypher_tck_procedures) and calls it. Without a Bolt
session such a setup cannot run: a Provisa graph's labels and relationship types are the model's,
fixed when it is declared, not created by a statement. Every other scenario passes or fails; a
failure is data. An error the scenario expects (a SyntaxError, a TypeError, ...) passes when the query is
refused; the error's type is not compared, since Provisa names its own. A scenario run in a Bolt
session has its side effects (``the side effects should be``, ``no side effects``) compared
exactly with the summary counters of its query.

The exit status is non-zero only when the suite did not run (no scenario passed or failed).

Usage: cypher_tck.py <features dir> <version> <base url> <role> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import math
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import by_design
import cypher_tck_bolt as bolt
import cypher_tck_procedures as procedures

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


#: Gherkin's escapes in a table cell: \\ for a backslash, \| for a bar, \n for a newline.
_CELL_ESCAPES = {"\\": "\\", "|": "|", "n": "\n"}


def _cells(line: str) -> list[str]:
    inner = line.strip()[1:-1]
    cells, cur, quote = [], [], None
    chars = iter(inner)
    for ch in chars:
        if ch == "\\":
            # A cell's own escape, undone before the value is read (a Cypher string's escapes
            # are tck_value's): \\\\ in a cell is the two characters \\ of the value's text.
            nxt = next(chars, "")
            cur.append(_CELL_ESCAPES.get(nxt, ch + nxt))
            continue
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
            # A path: to the > after its last node, outside any (, [ or { (an arrow's > is in
            # none, but follows a ] or -, never a node's closing ")").
            self.i += 1
            start = self.i
            depth = 0
            while True:
                c = self.s[self.i]
                if c in "([{":
                    depth += 1
                elif c in ")]}":
                    depth -= 1
                elif c == ">" and depth == 0 and self.s[start : self.i].rstrip().endswith(")"):
                    break
                self.i += 1
            self.i += 1
            return ("path", self.s[start : self.i - 1])
        if ch in "'\"":
            return self._string(ch)
        # A number (1e-305 and .5 included) before a word, or the word's e reads as the number's.
        word = re.match(
            r"-?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?(?![A-Za-z_])|-?[A-Za-z0-9_.+]+",
            self.s[self.i :],
        )
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
                code = self.s[self.i]
                if code in "uU":
                    width = 4 if code == "u" else 8
                    out.append(chr(int(self.s[self.i + 1 : self.i + 1 + width], 16)))
                    self.i += width
                else:
                    escapes = {"n": "\n", "t": "\t", "b": "\b", "f": "\f", "r": "\r"}
                    out.append(escapes.get(code, code))
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
        # A node, relationship or path: as a session's Bolt answer gives it (cypher_tck_bolt).
        kind, text = expected
        return bolt.same_element(bolt.expected_element(kind, text, tck_value), got, _same)
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

#: A setup that builds a graph: Provisa's graph is the model's, so the scenario cannot run --
#: unless a Bolt session is configured, in which a setup's statements (or a named graph's script)
#: build the session's own graph (REQ-2167, amended 2026-10-27).
_NOT_RUN = (("having executed", "builds its graph with a statement"),)

#: What a session scenario's outcome is marked with, for its adapted group (by_design/cypher.yaml).
IN_A_SESSION = "[in a session]"
EMPTY_GRAPH = "[in a session, empty graph]"
NAMED_GRAPH = "[in a session, named graph]"
#: What a scenario whose test procedure became a command is marked with, before a session's mark.
AS_A_COMMAND = "[procedure as a command]"

_PROCEDURE = "there exists a procedure "
_NAMED = re.compile(r"^the ([\w-]+) graph$")

#: The TCK's graphs directory, beside its features (main sets it): a named graph's CREATE script
#: is graphs/<name>/<name>.cypher.
_GRAPHS: dict[str, Path] = {}


def named_graph(text: str) -> str | None:
    """The CREATE script of the named graph a ``the <name> graph`` step names, as a session runs
    it (its closing semicolon is the script's, no part of the statement); None for any other
    step."""
    m = _NAMED.match(text)
    if m is None:
        return None
    if "graphs" not in _GRAPHS:
        raise ValueError(f"no TCK graphs directory to read the {m.group(1)} graph from")
    script = _GRAPHS["graphs"] / m.group(1) / f"{m.group(1)}.cypher"
    if not script.is_file():
        raise ValueError(f"the TCK has no named graph {m.group(1)} ({script})")
    return script.read_text().strip().rstrip(";").rstrip()


def procedure(scenario: Scenario) -> procedures.Procedure | None:
    """The test procedure a scenario's setup declares, or None."""
    for step in _setup_steps(scenario):
        if step.text.startswith(_PROCEDURE):
            return procedures.parse(step.text[len(_PROCEDURE) :], step.table, tck_value)
    return None


#: A refusal of a setup's write that waits on another piece of work, named so.
_WAITS = (
    ("has one CREATE clause", "waits on step 2c (multi-CREATE)"),
    ("MERGE", "waits on step 4a landing (MERGE)"),
    ("DETACH", "waits on write form"),
    ("REMOVE", "waits on write form"),
    ("a SET must say", "waits on write form"),
    ("Cannot parse SET assignment", "waits on write form"),
)


def _setup_steps(scenario: Scenario) -> list[Step]:
    """The Given/And steps before ``executing query``: an ``And`` after it (``And the side effects
    should be``, ``And no side effects``) checks the query's answer, not its graph."""
    out: list[Step] = []
    for step in scenario.steps:
        if step.text.startswith("executing query"):
            break
        if step.keyword in ("Given", "And"):
            out.append(step)
    return out


#: A TCK side-effect counter and the Bolt summary's counter for it; None for one Bolt's summary
#: has no counter for (Neo4j counts a removed property as a property set).
_SIDE_EFFECTS: dict[str, str | None] = {
    "+nodes": "nodes-created",
    "-nodes": "nodes-deleted",
    "+relationships": "relationships-created",
    "-relationships": "relationships-deleted",
    "+properties": "properties-set",
    "-properties": None,
    "+labels": "labels-added",
    "-labels": "labels-removed",
}


def side_effects(expected: dict[str, int], stats: dict[str, int]) -> str | None:
    """None when a query's summary counters are exactly the side effects ``expected`` (each TCK
    counter not named is zero); else what differs. A Bolt summary leaves out a counter at zero,
    so an absent Bolt counter reads as zero; a TCK counter with no Bolt counter is a failure
    when it is expected, naming it."""
    differs: list[str] = []
    for counter, key in _SIDE_EFFECTS.items():
        want = expected.get(counter, 0)
        if key is None:
            if want:
                differs.append(f"{counter} expected {want}, the summary has no counter for it")
            continue
        got = stats.get(key, 0)
        if got != want:
            differs.append(f"{counter} {got}, expected {want}")
    unknown = sorted(set(expected) - set(_SIDE_EFFECTS))
    differs += [f"{counter}: no such side effect" for counter in unknown]
    return "; ".join(differs) or None


def _session_scenario(scenario: Scenario) -> str | None:
    """How a scenario runs in a Bolt session, when one is configured: NAMED_GRAPH for a named
    TCK graph, IN_A_SESSION for a setup that builds its graph, EMPTY_GRAPH for an empty graph its
    query matches a pattern on (the session's graph alone is empty); None to run it over HTTP. A
    test procedure is the fixture model's command, called over either."""
    if not bolt.configured():
        return None
    texts = [s.text for s in _setup_steps(scenario)]
    if any(_NAMED.match(t) for t in texts):
        return NAMED_GRAPH
    if any(t.startswith("having executed") for t in texts):
        return IN_A_SESSION
    query = next((s.doc or "" for s in scenario.steps if s.text.startswith("executing query")), "")
    if "an empty graph" in texts and re.search(r"\bMATCH\b", query, re.I):
        return EMPTY_GRAPH
    return None


def _waiting(reason: str) -> str:
    for marker, waits in _WAITS:
        if marker in reason and reason.startswith(("refused (400): setup:", "refused (400):")):
            return f"{waits}: {reason}"
    return reason


@dataclass
class Outcome:
    feature: str
    scenario: str
    status: str  # passed | failed | not_run (skipped by design if a group names it, else failed)
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


def _clear(base: str, authorization: str) -> None:
    """Empty the graph session of the principal ``authorization`` signs in: a principal has one
    graph session on every client (REQ-2176), so a scenario starts from the TCK's empty graph only
    once what earlier scenarios made in it is dropped."""
    req = urllib.request.Request(
        f"{base}/data/cypher/session", headers={"Authorization": authorization}, method="DELETE"
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        resp.read()


def _rows(body: Any) -> tuple[list[str], list[list[Any]]]:
    columns = list(body.get("columns") or [])
    rows = body.get("rows") or []
    return columns, [[r.get(c) for c in columns] if isinstance(r, dict) else r for r in rows]


def run(scenario: Scenario, base: str, role: str) -> Outcome:
    session = _session_scenario(scenario)
    setups: list[str] = []
    declared = False

    def out(status: str, reason: str = "") -> Outcome:
        marks = [AS_A_COMMAND] if declared else []
        if session is not None:
            reason = _waiting(reason) if status == "failed" else reason
            marks.append(session)
        return Outcome(scenario.feature, scenario.name, status, " ".join([reason, *marks]).strip())

    def ask(query: str, params: dict) -> tuple[int, Any]:
        if session is None:
            return _post(base, role, query, params)
        return bolt.run(setups, query, params)

    _clear(base, _CREDENTIAL["Authorization"] if session is None else bolt.authorization())
    params: dict = {}
    query: str | None = None
    answer: tuple[int, Any] | None = None
    checked = False
    for step in scenario.steps:
        text = step.text
        if step.keyword == "Given" or (step.keyword == "And" and query is None):
            if text in ("an empty graph", "any graph"):
                continue
            if session is not None and text.startswith("having executed"):
                setups.append(step.doc or "")
                continue
            if session is not None and (script := named_graph(text)) is not None:
                setups.append(script)
                continue
            if text.startswith(_PROCEDURE):
                try:
                    procedures.declare(
                        procedures.parse(text[len(_PROCEDURE) :], step.table, tck_value)
                    )
                except procedures.NotACommand as exc:
                    return out("failed", f"its test procedure is no command: {exc}")
                declared = True
                continue
            if text == "parameters are" and step.table:
                params = {r[0]: tck_value(r[1]) for r in step.table}
                continue
            why = next((w for marker, w in _NOT_RUN if text.startswith(marker)), None)
            return out("not_run", f"its setup {why or 'uses a named TCK graph'}")
        if text.startswith("executing query"):
            query = step.doc or ""
            continue
        assert query is not None
        if text.startswith("executing control query"):
            return out("not_run", "checks state with a control query")
        if answer is None:
            answer = ask(query, params)
        status, body = answer
        if "should be raised" in text:
            if status >= 400:
                return out("passed")
            return out("failed", f"expected {text.split(' should')[0][2:]}; the query ran")
        if text == "no side effects" or text.startswith("the side effects should be"):
            if session is None:
                # An HTTP answer carries no summary counters; only a Bolt run's are compared.
                continue
            if status >= 400:
                return out("failed", f"refused ({status}): {str(body)[:200]}")
            expected = {r[0]: int(r[1]) for r in step.table or []}
            differs = side_effects(expected, body["stats"])
            if differs:
                return out("failed", f"side effects differ: {differs}")
            checked = True
            continue
        if text.startswith("the result should be"):
            if status >= 400:
                detail = (body or {}).get("detail") if isinstance(body, dict) else body
                return out("failed", f"refused ({status}): {str(detail)[:200]}")
            columns, rows = _rows(body)
            if text == "the result should be empty":
                if rows:
                    return out("failed", f"{len(rows)} row(s) returned")
                checked = True
                continue
            assert step.table is not None
            header, *expected_raw = step.table
            if columns != header:
                return out("failed", f"columns {columns} != {header}")
            try:
                expected = [[tck_value(c) for c in r] for r in expected_raw]
            except ValueError as exc:
                return out("failed", f"expected value unreadable: {exc}")
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
                checked = True
                continue
            if _below_the_microsecond(header, expected_raw):
                return out(
                    "failed", f"expects digits below the microsecond; rows differ: got {rows[:3]}"
                )
            return out("failed", f"rows differ: got {rows[:3]}")
    if checked:
        return out("passed")
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


def in_running_order(scenarios: list[Scenario]) -> list[int]:
    """The scenarios' indexes in the order they run: every scenario that declares no test
    procedure first, as read, so one that calls an unknown procedure meets none; then those that
    do, each procedure's together, so the model is put once per procedure declared."""
    first: dict[Any, int] = {}
    keyed: list[tuple[int, int, int]] = []
    for n, s in enumerate(scenarios):
        try:
            proc = procedure(s)
        except procedures.NotACommand:
            proc = ("unread", n)  # its run says why, wherever it runs
        if proc is None:
            keyed.append((0, n, n))
        else:
            keyed.append((1, first.setdefault(proc, n), n))
    return [n for *_, n in sorted(keyed)]


# --- the report --------------------------------------------------------------------------------


def report(outcomes: list[Outcome], version: str) -> dict:
    """The run as the conformance page reads it (REQ-2167, amended 2026-10-27 and 2026-10-28):
    each scenario is passed, skipped by design or failed. One not run, or failed, because of a
    designed limitation is skipped by design in a group that names its decision
    (by_design/cypher.yaml) and is out of the pass rate; one with no decision behind it is
    failed."""
    passed = sum(o.status == "passed" for o in outcomes)
    unmet = [
        {"feature": o.feature, "scenario": o.scenario, "reason": o.reason}
        for o in outcomes
        if o.status in ("failed", "not_run")
    ]
    declared = by_design.groups("cypher")
    failures, excluded = by_design.classify(unmet, declared)
    # A scenario run in a session is adapted: its group says what its setup became.
    adapted = by_design.adapted(
        [
            {
                "feature": o.feature,
                "scenario": o.scenario,
                "reason": o.reason,
                "outcome": o.status,
            }
            for o in outcomes
            if o.status in ("passed", "failed")
        ],
        declared,
    )
    return {
        "surface": "cypher",
        "suite": "openCypher TCK",
        "version": version,
        **by_design.outcomes(passed, excluded, failures),
        "adapted": adapted,
    }


def markdown(doc: dict) -> str:
    lines = [
        f"## Cypher conformance: {doc['suite']} {doc['version']}",
        "",
        by_design.counts_line(doc, "scenarios"),
        "",
        "### Skipped by design",
        "",
        *by_design.table(doc["by_design_groups"], requirement=True),
        "",
        *by_design.cases_lines(doc["by_design_groups"]),
        "",
        "### Adapted",
        "",
        *by_design.adapted_lines(doc["adapted"]),
        "",
        "### Failed",
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
    # The named graphs' CREATE scripts, beside the features (tck/graphs).
    _GRAPHS["graphs"] = Path(features).resolve().parent / "graphs"
    # A Bolt session for the scenarios whose setup builds their graph (REQ-2167, amended
    # 2026-10-27): the job's Bolt endpoint and the TCK session user's token, from the job's env.
    if os.environ.get("CONFORMANCE_BOLT_URI"):
        bolt.configure(os.environ["CONFORMANCE_BOLT_URI"], os.environ["CONFORMANCE_SESSION_TOKEN"])
    # The fixture model a test procedure is declared in, as a command, and the break-glass
    # account that puts it (REQ-2167, amended 2026-10-27).
    if os.environ.get("CONFORMANCE_MODEL"):
        procedures.configure(
            base,
            Path(os.environ["CONFORMANCE_MODEL"]),
            os.environ["PROVISA_SUPERUSER_USERNAME"],
            os.environ["PROVISA_SUPERUSER_PASSWORD"],
        )
    scenarios = [s for p in sorted(Path(features).rglob("*.feature")) for s in parse_feature(p)]
    outcomes: list[Outcome | None] = [None] * len(scenarios)
    for n in in_running_order(scenarios):
        s = scenarios[n]
        try:
            outcomes[n] = run(s, base, role)
        except (OSError, ValueError, KeyError) as exc:
            outcomes[n] = Outcome(s.feature, s.name, "failed", f"runner: {exc}")
    doc = report([o for o in outcomes if o is not None], version)
    Path(out_json).write_text(json.dumps(doc, indent=2) + "\n")
    Path(out_md).write_text(markdown(doc))
    if not doc["total"]:
        return 1
    # REQ-2167: at its baseline, a failed scenario fails the job.
    return 1 if by_design.baseline_failed("cypher", doc["failed"]) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
