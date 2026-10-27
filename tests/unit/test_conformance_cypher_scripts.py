# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7e5c19-6d82-4b04-9f31-8c2e4a6d1b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Cypher conformance job's scripts (REQ-2167): the openCypher TCK's features read and run
against a Cypher endpoint, the report, and a conformance page filled from an artifact."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(
        name, REPO / "scripts" / "conformance" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


by_design = _load("by_design")  # the scripts import it as their sibling
tck = _load("cypher_tck")
fill_docs = _load("fill_docs")

_FEATURE = '''
Feature: Literals1 - Booleans

  Scenario: [1] Return a boolean
    Given any graph
    When executing query:
      """
      RETURN true AS literal
      """
    Then the result should be, in any order:
      | literal |
      | true    |
    And no side effects

  Scenario Outline: [2] Add numbers
    Given any graph
    When executing query:
      """
      RETURN <a> + 1 AS n
      """
    Then the result should be, in any order:
      | n   |
      | <b> |

    Examples:
      | a | b |
      | 1 | 2 |
      | 2 | 3 |

  Scenario: [3] Needs a graph
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (a) RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | (:A) |

  Scenario: [4] A syntax error
    Given any graph
    When executing query:
      """
      RETURN 1 +
      """
    Then a SyntaxError should be raised at compile time: InvalidExpression
'''


@pytest.fixture
def scenarios(tmp_path):
    path = tmp_path / "literals.feature"
    path.write_text(_FEATURE)
    return tck.parse_feature(path)


def test_scenarios_and_each_example_of_an_outline_are_read(scenarios):
    assert [s.name for s in scenarios] == [
        "[1] Return a boolean",
        "[2] Add numbers #1",
        "[2] Add numbers #2",
        "[3] Needs a graph",
        "[4] A syntax error",
    ]
    assert scenarios[2].steps[1].doc == "RETURN 2 + 1 AS n"
    assert scenarios[2].steps[2].table == [["n"], ["3"]]


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("null", None),
        ("-12", -12),
        ("1.5e3", 1500.0),
        ("'it\\'s'", "it's"),
        ("[1, 'a', [true]]", [1, "a", [True]]),
        ("{a: 1, `b`: [null]}", {"a": 1, "b": [None]}),
        ("(:A {name: 'x'})", ("node", "(:A {name: 'x'})")),
        ("[:T]", ("relationship", "[:T]")),
    ],
)
def test_tck_values_are_read(text, value):
    assert tck.tck_value(text) == value


def test_nan_is_its_own_match():
    assert tck._same(math.nan, math.nan, False)
    assert not tck._same(1, 1.0, False)  # an integer is not a float
    assert tck._same([1, 2], [2, 1], True)


def test_each_scenario_passes_fails_or_is_not_runnable(scenarios, monkeypatch):
    answers = {
        "RETURN true AS literal": (200, {"columns": ["literal"], "rows": [[True]]}),
        "RETURN 1 + 1 AS n": (200, {"columns": ["n"], "rows": [[2]]}),
        "RETURN 2 + 1 AS n": (200, {"columns": ["n"], "rows": [[4]]}),
        "RETURN 1 +": (400, {"detail": "parse error"}),
    }
    monkeypatch.setattr(tck, "_post", lambda _b, _r, query, _p: answers[query])
    outcomes = [tck.run(s, "http://x", "org_admin") for s in scenarios]
    assert [o.status for o in outcomes] == [
        "passed",
        "passed",
        "failed",
        "not_runnable",
        "passed",
    ]
    doc = tck.report(outcomes, "2024.3")
    # [3] builds its graph only as setup: adapted, not excluded, so a failure until it is.
    assert (doc["ran"], doc["passed"], doc["not_runnable"]) == (5, 3, 0)
    assert doc["pass_rate"] == 0.6
    assert doc["failures"][0]["scenario"] == "[2] Add numbers #2"


def test_a_page_is_filled_from_the_artifact_and_refilled_in_place():
    page = "# Cypher Conformance\n\n**Suite:** TBD.\n\n**Pass rate:** TBD.\n\n## Deliberate\n"
    artifact = {
        "surface": "cypher",
        "suite": "openCypher TCK",
        "version": "2024.3",
        "ran": 4,
        "passed": 3,
        "not_runnable": 1,
        "failures": [{"feature": "Literals1", "scenario": "[2] #2", "reason": "rows differ"}],
    }
    surface, suite, rate, cases = fill_docs.read(artifact)
    filled = fill_docs.fill(page, suite, rate, cases)
    assert surface == "cypher"
    assert "**Suite:** openCypher TCK 2024.3." in filled
    assert "**Pass rate:** 75.0% (3 of 4); 1 cases not runnable by design." in filled
    assert "| [2] #2 | rows differ |" in filled
    assert filled.index("conformance-results:begin") < filled.index("## Deliberate")
    again = fill_docs.fill(filled, suite, rate, cases)
    assert again == filled


junit = _load("junit_summary")


def test_a_driver_s_junit_results_are_a_conformance_report(tmp_path):
    xml = tmp_path / "junit.xml"
    xml.write_text(
        '<testsuites><testsuite name="pytest">'
        '<testcase classname="tests.integration.test_bolt_driver" name="test_ok"/>'
        '<testcase classname="tests.integration.test_bolt_driver" name="test_bad">'
        '<failure message="ClientError: Unknown label">trace</failure></testcase>'
        '<testcase classname="tests.integration.examples" name="test_skip">'
        '<skipped message="needs enterprise"/></testcase>'
        "</testsuite></testsuites>"
    )
    doc = junit.summarise(
        str(xml), "bolt", "neo4j-python-driver integration tests", "6.2.0", "tests/integration"
    )
    assert (doc["ran"], doc["passed"], doc["failed"], doc["skipped"]) == (2, 1, 1, 1)
    assert doc["failures"] == [
        {
            "feature": "tests.integration.test_bolt_driver",
            "scenario": "test_bad",
            "reason": "ClientError: Unknown label",
        }
    ]
    _, suite, rate, _ = fill_docs.read(doc)
    assert suite == "neo4j-python-driver integration tests 6.2.0 (tests/integration)"
    # A test the suite skips never ran: counted apart, not as by design (no decision names it).
    assert rate == "50.0% (1 of 2); 1 skipped by the suite"


def test_a_failure_a_design_decision_excludes_is_not_runnable_by_design(tmp_path):
    """REQ-2167 (amended 2026-10-27): it is counted in its group, with its requirement, and left
    out of the pass rate; a failure no group names stays a failure."""
    xml = tmp_path / "junit.xml"
    xml.write_text(
        '<testsuites><testsuite name="pytest">'
        '<testcase classname="tests.integration.examples" name="test_ok"/>'
        '<testcase classname="tests.integration.examples" name="test_detach">'
        '<failure message="CypherSyntaxError: Cypher write patterns cannot target relationships">'
        "t</failure></testcase>"
        '<testcase classname="tests.integration.examples" name="test_bookmark">'
        '<failure message="assert Bookmarks">t</failure></testcase>'
        "</testsuite></testsuites>"
    )
    doc = junit.summarise(str(xml), "bolt", "driver tests", "6.2.0", "tests/integration")
    assert (doc["ran"], doc["passed"], doc["failed"], doc["not_runnable"]) == (2, 1, 1, 1)
    assert doc["by_design"] == [
        {
            "group": "Writes to relationships",
            "requirement": "REQ-665",
            "decision": '"CREATE, SET, and DELETE cannot target relationship entities. '
            'Attempting to write a relationship is a hard error at translation time."',
            "count": 1,
        }
    ]
    assert [f["scenario"] for f in doc["failures"]] == ["test_bookmark"]
    _, _, rate, cases = fill_docs.read(doc)
    assert rate == "50.0% (1 of 2); 1 cases not runnable by design"
    assert "| Writes to relationships | 1 |" in cases
    assert "| REQ-665 |" in cases


def test_a_group_with_no_requirement_is_refused(tmp_path, monkeypatch):
    (tmp_path / "x.yaml").write_text("- group: Unexplained\n  decision: none\n  reason: x\n")
    monkeypatch.setattr(by_design, "RULES", tmp_path)
    with pytest.raises(ValueError, match="names no requirement"):
        by_design.groups("x")


def test_a_scenario_not_run_with_no_decision_behind_it_is_a_failure():
    """A setup that builds its graph is adapted, not excluded (REQ-2167, amended 2026-10-27):
    until it is, it is a failure, as a scenario with no decision behind it is."""
    outcomes = [
        tck.Outcome("F", "built", "not_runnable", "its setup builds its graph with a statement"),
        tck.Outcome("F", "control", "not_runnable", "checks state with a control query"),
        tck.Outcome("F", "label", "failed", "refused (400): There is no label :Person."),
        tck.Outcome("F", "ok", "passed"),
    ]
    doc = tck.report(outcomes, "2024.3")
    assert (doc["passed"], doc["failed"], doc["not_runnable"]) == (1, 2, 1)
    assert [g["requirement"] for g in doc["by_design"]] == ["REQ-778"]
    assert [f["scenario"] for f in doc["failures"]] == ["built", "control"]


def test_an_expected_value_below_the_microsecond_is_named():
    assert tck._below_the_microsecond(["t"], [["'12:31:14.645876123'"]])
    assert tck._below_the_microsecond(["d.nanosecond"], [["645876123"]])
    assert not tck._below_the_microsecond(["t"], [["'12:31:14.645876'"]])
    assert not tck._below_the_microsecond(["d.nanosecond"], [["645876000"]])
