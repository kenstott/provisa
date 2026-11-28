# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6d1a2e-8c47-4b19-9e05-7a2b4c6d8e10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No regular expression applied to a statement's text can backtrack catastrophically.

A statement opening with several ``--`` comment lines (two queries with provisa-params comments,
the first commented out) once held a worker at 100% CPU inside the regex engine on
``POST /data/sql/explain``: ``_OPENING_WRITE_RE`` nested ``\\s+`` and ``--[^\\n]*\\n?`` inside a
``*`` repeat, so each comment line could be split two ways and a statement that is not a write
(the verb never follows) tried every split. Every regex on the request paths is run here against
adversarial texts in a child process with a hard time bound."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from provisa.pgwire._pipeline import _OPENING_WRITE_RE, opening_write_verb

_REPO = Path(__file__).resolve().parents[2]

# The directories whose regexes read request text: the SQL, explain, params and directive paths.
_SWEPT = (
    "provisa/compiler",
    "provisa/api/data",
    "provisa/api/flight",
    "provisa/transpiler",
    "provisa/core",
    "provisa/pgwire",
)
# Modules whose compiled patterns are built from non-literal text (joined fragments, f-strings),
# which the literal sweep cannot read: their module-level patterns are collected by import.
_IMPORTED = (
    "provisa.pgwire._pipeline",
    "provisa.compiler.definitions",
    "provisa.compiler.temp_tables",
    "provisa.compiler.params",
    "provisa.compiler.directives",
    "provisa.compiler.view_expand",
    "provisa.compiler.sql_rewrite",
    "provisa.compiler.hints",
    "provisa.api.flight.server",
    "provisa.transpiler.transpile",
)
_FUNCS = {"compile", "sub", "subn", "match", "search", "findall", "finditer", "fullmatch", "split"}
_FLAG_POS = {
    "compile": 1,
    "match": 2,
    "search": 2,
    "fullmatch": 2,
    "findall": 2,
    "finditer": 2,
    "split": 3,
    "sub": 4,
    "subn": 4,
}
# Seconds one pattern may take over every adversarial text. A linear pattern takes milliseconds;
# the exponential one this file was written for did not finish in minutes.
_PER_PATTERN_BOUND = 2.0


def _two_statements() -> str:
    first = [
        "-- provisa-params: $1='alpha' $2=42 $3=NULL",
        "-- provisa-params: $4='b''c' $5=TRUE",
        "-- @provisa cache=true",
        "SELECT a, b, c",
        "FROM orders o",
        "WHERE o.a = $1 AND o.b = $2 -- trailing",
        "  AND o.c IS $3;",
    ]
    second = [
        "-- provisa-params: $1='x' $2=7.5",
        "-- provisa-params: $3=-1",
        "",
        "   ",
        "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3",
    ]
    # The first statement commented out line by line, as the SQL editor's toggle does.
    return "\n".join(["-- " + line for line in first] + ["", "", "  "] + second)


def _adversarial_texts() -> dict[str, str]:
    two = _two_statements()
    blanks = "\n".join(["   ", "", "\t ", "-- note", "  -- another", ""] * 40)
    mixed = "\n".join(["-- a", "/* b */", "  ", "-- c /* d", "/* e -- f */", "\t"] * 40)
    return {
        "two_statements": two,
        "two_statements_commented_again": "\n".join("-- " + ln for ln in two.split("\n"))
        + "\n"
        + two,
        "blank_and_comment_runs": blanks + "\nSELECT 1",
        "blank_runs_unterminated_block": blanks + "\n/* never closed",
        "unterminated_block": "  \n" * 60 + "/* " + "x " * 200,
        "mixed_comments": mixed + "\nSELECT 1",
        "mixed_comments_unterminated": mixed + "\n/*",
        "comment_lines": "\n".join(["-- provisa-params: $1='a' -- b"] * 60) + "\nSELECT 1",
        "spaces_then_bang": " " * 1000 + "!",
        "newlines_then_bang": "\n" * 1000 + "!",
        "dashes": "-" * 1000,
        "bare_comment_lines": "\n".join(["--"] * 500),
        "quotes": "'" + "''" * 500 + " $1 " + "x" * 50,
        "dollars": "$" * 1000,
        "block_openers": "/*" * 500,
    }


def _literal(node: ast.AST, consts: dict[str, str]) -> str | None:
    try:
        value = ast.literal_eval(node)
    except ValueError:
        value = None
    if isinstance(value, str):
        return value
    if isinstance(node, ast.Name):
        return consts.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _literal(node.left, consts), _literal(node.right, consts)
        if left is not None and right is not None:
            return left + right
    return None


def _flags(node: ast.AST | None) -> int:
    if node is None:
        return 0
    names = {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)}
    names |= {n.id for n in ast.walk(node) if isinstance(n, ast.Name)} - {"re", "_re"}
    value = 0
    for name in names:
        flag = getattr(re, name, None)
        if isinstance(flag, re.RegexFlag):
            value |= flag
    return value


def _literal_patterns() -> list[tuple[str, str, int]]:
    """Every regex under :data:`_SWEPT` written as a literal: ``(where, pattern, flags)``."""
    found: list[tuple[str, str, int]] = []
    for root in _SWEPT:
        for path in sorted((_REPO / root).rglob("*.py")):
            tree = ast.parse(path.read_text())
            consts: dict[str, str] = {}
            for stmt in tree.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                ):
                    value = _literal(stmt.value, consts)
                    if value is not None:
                        consts[stmt.targets[0].id] = value
            for node in ast.walk(tree):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in _FUNCS
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id in ("re", "_re")
                    and node.args
                ):
                    continue
                pattern = _literal(node.args[0], consts)
                if pattern is None:
                    continue
                flags_node = next((k.value for k in node.keywords if k.arg == "flags"), None)
                pos = _FLAG_POS[node.func.attr]
                if flags_node is None and len(node.args) > pos:
                    flags_node = node.args[pos]
                where = f"{path.relative_to(_REPO)}:{node.lineno}"
                found.append((where, pattern, _flags(flags_node)))
    return found


def _imported_patterns() -> list[tuple[str, str, int]]:
    import importlib

    found: list[tuple[str, str, int]] = []
    for name in _IMPORTED:
        module = importlib.import_module(name)
        for attr, value in vars(module).items():
            if isinstance(value, re.Pattern):
                found.append((f"{name}.{attr}", value.pattern, value.flags))
    return found


# Runs in a child process: one line per pattern before it starts and after it ends, so a pattern
# that never returns is named by the last line the parent read.
_CHILD = r"""
import json, re, sys, time
items, texts = json.load(sys.stdin)
for where, pattern, flags in items:
    try:
        rx = re.compile(pattern, flags)
    except re.error:
        continue
    print("start", where, flush=True)
    t0 = time.perf_counter()
    for text in texts.values():
        rx.search(text)
        rx.match(text)
        for _ in rx.finditer(text):
            pass
    print("done", round(time.perf_counter() - t0, 4), where, flush=True)
"""


def _run_bounded(
    items: list[tuple[str, str, int]], total_timeout: float
) -> tuple[list[str], str | None]:
    """Run ``items`` against the adversarial texts in a child; return (slow patterns, hung one)."""
    payload = json.dumps([items, _adversarial_texts()])
    proc = subprocess.Popen(
        [sys.executable, "-c", _CHILD],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        out, _ = proc.communicate(payload, timeout=total_timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    lines = out.splitlines()
    slow = []
    started = None
    for line in lines:
        kind, _, rest = line.partition(" ")
        if kind == "start":
            started = rest
        elif kind == "done":
            seconds, _, where = rest.partition(" ")
            started = None
            if float(seconds) > _PER_PATTERN_BOUND:
                slow.append(f"{where}: {seconds}s")
    return slow, started


def _unique(items: list[tuple[str, str, int]]) -> list[tuple[str, str, int]]:
    seen: set[tuple[str, int]] = set()
    out = []
    for where, pattern, flags in items:
        if (pattern, flags) not in seen:
            seen.add((pattern, flags))
            out.append((where, pattern, flags))
    return out


def test_opening_write_verb_is_bounded_on_commented_statements():
    """The reproduction: the explain path's write check on two statements, the first commented
    out, which is not a write. It must answer at once."""
    slow, hung = _run_bounded(
        [
            (
                "provisa.pgwire._pipeline._OPENING_WRITE_RE",
                _OPENING_WRITE_RE.pattern,
                _OPENING_WRITE_RE.flags,
            )
        ],
        total_timeout=10,
    )
    assert hung is None, f"{hung} did not finish within 10s"
    assert not slow


# The pattern as it was before it was made linear: the reference its matches are kept against.
_ORIGINAL_OPENING_WRITE = re.compile(
    r"(?:\s+|--[^\n]*\n?|/\*.*?\*/)*(?P<verb>INSERT|UPDATE|DELETE|MERGE)\b",
    re.IGNORECASE | re.DOTALL,
)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "insert into t values (1)",
        "  \n\tUPDATE t SET a = 1",
        "DELETE FROM t",
        "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE",
        "-- provisa-params: $1=5\nINSERT INTO t VALUES ($1)",
        "-- a\n-- b\n\n  UPDATE t SET a = 1",
        "/* a */ DELETE FROM t",
        "/* a\n b */\n-- c\nMERGE INTO t USING s ON true WHEN MATCHED THEN DELETE",
        "-- last line without newline then nothing",
        "SELECT 1",
        "-- provisa-params: $1=5\nSELECT a FROM t WHERE a = $1",
        "INSERTED",
        "UPDATES",
        "/* unterminated INSERT",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "",
    ],
)
def test_opening_write_verb_keeps_its_matches(sql):
    original = _ORIGINAL_OPENING_WRITE.match(sql)
    expected = original.group("verb").upper() if original else None
    assert opening_write_verb(sql) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "-- INSERT INTO t VALUES (1)\nSELECT 1",
        "--DELETE FROM t\nSELECT 1",
        "-- note: we UPDATE nothing\nSELECT 1",
    ],
)
def test_a_write_verb_inside_a_comment_is_not_the_opening_verb(sql):
    """The old pattern could backtrack into a comment and read a word there as the statement's
    verb, refusing to explain a read whose comment mentions a write."""
    assert opening_write_verb(sql) is None


def test_every_request_path_regex_is_bounded():
    items = _unique(_literal_patterns() + _imported_patterns())
    assert len(items) > 300  # the sweep reads the request paths, not an empty set
    slow, hung = _run_bounded(items, total_timeout=240)
    assert hung is None, f"{hung} did not finish: catastrophic backtracking"
    assert not slow, f"patterns over {_PER_PATTERN_BOUND}s: {slow}"
