# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A surface at its conformance baseline fails its job on a failure no group excludes (REQ-2167,
amended 2026-10-27): each summary script's exit status, with the surface at and not at its
baseline (by_design.AT_BASELINE)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

CONFORMANCE = Path(__file__).resolve().parents[2] / "scripts" / "conformance"
sys.path.insert(0, str(CONFORMANCE))  # the scripts' sibling import of by_design


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"baseline_{name}", CONFORMANCE / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # a dataclass resolves its module while being defined
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def at_baseline(monkeypatch):
    """Put a surface at (or take it off) its baseline, as the scripts read it."""
    import by_design

    def put(surface: str, at: bool) -> None:
        listed = set(by_design.AT_BASELINE) - {surface}
        monkeypatch.setattr(
            by_design, "AT_BASELINE", frozenset(listed | ({surface} if at else set()))
        )

    return put


def test_the_surfaces_at_baseline_are_pgwire_and_graphql():
    import by_design

    assert by_design.AT_BASELINE == {"pgwire", "graphql"}


def test_pgspec_summary_fails_on_a_failure_only_at_baseline(tmp_path, at_baseline):
    m = _load("pgspec_summary")
    src, results = tmp_path / "pgspec_test.go", tmp_path / "r.jsonl"
    src.write_text("func TestLone(t *testing.T) {\n}\n")
    results.write_text(json.dumps({"Action": "fail", "Test": "TestLone"}))
    argv = ["x", str(src), str(results), str(tmp_path / "o.json"), str(tmp_path / "o.md")]
    at_baseline("pgwire", False)
    assert m.main(argv) == 0
    at_baseline("pgwire", True)
    assert m.main(argv) == 1


def test_junit_summary_fails_on_a_failure_only_at_baseline(tmp_path, at_baseline):
    m = _load("junit_summary")
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuite><testcase classname="t.T" name="test_ok"/>'
        '<testcase classname="t.T" name="test_bad"><failure message="boom">t</failure>'
        "</testcase></testsuite>"
    )
    argv = [
        str(junit),
        "bolt",
        "suite",
        "1",
        "tests",
        str(tmp_path / "o.json"),
        str(tmp_path / "o.md"),
    ]
    at_baseline("bolt", False)
    assert m.main(argv) == 0
    at_baseline("bolt", True)
    assert m.main(argv) == 1


def test_cypher_tck_fails_on_a_failure_only_at_baseline(tmp_path, at_baseline, monkeypatch):
    m = _load("cypher_tck")
    features = tmp_path / "features"
    features.mkdir()
    (features / "a.feature").write_text("Feature: A\n")
    scenario = m.Scenario("A", "one")
    monkeypatch.setattr(m, "parse_feature", lambda path: [scenario])
    monkeypatch.setattr(m, "run", lambda s, base, role: m.Outcome(s.feature, s.name, "failed", "x"))
    argv = [
        str(features),
        "1",
        "http://x",
        "r",
        "Bearer t",
        str(tmp_path / "o.json"),
        str(tmp_path / "o.md"),
    ]
    at_baseline("cypher", False)
    assert m.main(argv) == 0
    at_baseline("cypher", True)
    assert m.main(argv) == 1


def test_the_graphql_gate_fails_on_a_failed_must_audit_only_at_baseline(tmp_path, at_baseline):
    m = _load("baseline_gate")
    report = tmp_path / "conformance-graphql.json"
    report.write_text(json.dumps({"surface": "graphql", "ran": 47, "passed": 46, "failed": 1}))
    at_baseline("graphql", False)
    assert m.main(["graphql", str(report)]) == 0
    at_baseline("graphql", True)
    assert m.main(["graphql", str(report)]) == 1
    report.write_text(json.dumps({"surface": "graphql", "ran": 47, "passed": 47, "failed": 0}))
    assert m.main(["graphql", str(report)]) == 0
