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
bolt = _load("cypher_tck_bolt")  # cypher_tck's sibling too
procedures = _load("cypher_tck_procedures")  # and this
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


@pytest.fixture(autouse=True)
def cleared(monkeypatch):
    """The credentials whose graph session the runner emptied, in turn (none is reached)."""
    emptied: list[str] = []
    monkeypatch.setattr(tck, "_clear", lambda _base, authorization: emptied.append(authorization))
    monkeypatch.setitem(tck._CREDENTIAL, "Authorization", "Bearer admin")
    return emptied


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
        '<testcase classname="tests.integration.examples.test_temporal_types_example" '
        'name="test_time"><failure message="AssertionError: assert '
        "'07:47:00.000004000-04:00' == '07:47:00.000004123-04:00'\">"
        "t</failure></testcase>"
        # Another difference than the digits below the microsecond is no part of the group.
        '<testcase classname="tests.integration.examples.test_temporal_types_example" '
        'name="test_local_time"><failure message="AssertionError: assert '
        "'07:47:00.000004000+00:00' == '07:47:00.000004123'\">"
        "t</failure></testcase>"
        '<testcase classname="tests.integration.examples" name="test_bookmark">'
        '<failure message="assert Bookmarks">t</failure></testcase>'
        "</testsuite></testsuites>"
    )
    doc = junit.summarise(str(xml), "bolt", "driver tests", "6.2.0", "tests/integration")
    assert (doc["ran"], doc["passed"], doc["failed"], doc["not_runnable"]) == (3, 1, 2, 1)
    assert doc["by_design"] == [
        {
            "group": "Digits below the microsecond",
            "requirement": "REQ-2173",
            "decision": '"Every temporal value, read from a column or built in a query, has '
            "microsecond precision, what the engines and their drivers hold; its nanosecond "
            'property is its microseconds times 1000."',
            "count": 1,
        }
    ]
    assert [f["scenario"] for f in doc["failures"]] == ["test_local_time", "test_bookmark"]
    _, _, rate, cases = fill_docs.read(doc)
    assert rate == "33.3% (1 of 3); 1 cases not runnable by design"
    assert "| Digits below the microsecond | 1 |" in cases
    assert "REQ-" not in cases  # the requirement is the artifact's; a published page names none


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


def test_a_cell_s_own_escapes_are_undone_before_its_value_is_read():
    # Gherkin's \\ is a backslash and \| a bar in a table cell (Literals6 [5]): the cell
    # 'a\\\\b' is the Cypher string 'a\\b', whose value is a\b.
    (cell,) = tck._cells(r"| 'a\\\\b\'c' |")
    assert tck.tck_value(cell) == "a\\b'c"
    assert tck._cells(r"| 'x\|y' | 2 |") == ["'x|y'", "2"]
    assert tck.tck_value("'\\u01FF'") == "ǿ"


@pytest.mark.parametrize(
    ("text", "value"), [("1e-305", 1e-305), ("-1e-305", -1e-305), (".5", 0.5), ("0x1F", 31)]
)
def test_a_number_s_exponent_is_its_own(text, value):
    # Literals5: 1e-305 is a number, not the word 1e and a stray -305.
    assert tck.tck_value(text) == value


def test_a_setup_that_builds_its_graph_runs_with_its_query_in_one_session(scenarios, monkeypatch):
    # REQ-2167, amended 2026-10-27: with a Bolt session configured, [3]'s setup and its query run
    # on one connection (REQ-2176), its answer's node compared, and it is marked adapted.
    sent = []

    def session(setups, query, params):
        sent.append((list(setups), query))
        return 200, {"columns": ["a"], "rows": [[("node", frozenset({"A"}), {})]]}

    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    monkeypatch.setattr(bolt, "run", session)
    outcome = tck.run(scenarios[3], "http://x", "org_admin")
    assert sent == [(["CREATE (:A)"], "MATCH (a) RETURN a")]
    assert (outcome.status, outcome.reason) == ("passed", tck.IN_A_SESSION)
    doc = tck.report([outcome], "2024.3")
    assert doc["adapted"][0]["group"] == "Setups that build their graph"


def test_each_scenario_starts_from_an_empty_graph_session(scenarios, monkeypatch, cleared):
    # REQ-2176: a principal has one graph session on every client, so what a scenario made is
    # dropped before the next, over HTTP as the job's user and over Bolt as the TCK session user.
    monkeypatch.setattr(tck, "_post", lambda *_a: (200, {"columns": ["literal"], "rows": []}))
    tck.run(scenarios[0], "http://x", "org_admin")
    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    monkeypatch.setattr(bolt, "run", lambda *_a: (200, {"columns": ["a"], "rows": []}))
    tck.run(scenarios[3], "http://x", "org_admin")
    assert cleared == ["Bearer admin", "Bearer t"]


def test_a_setup_waiting_on_a_write_form_is_named_so(scenarios, monkeypatch):
    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    monkeypatch.setattr(
        bolt,
        "run",
        lambda *_a: (
            400,
            {"detail": "setup: A statement creating ephemeral objects has one CREATE clause"},
        ),
    )
    outcome = tck.run(scenarios[3], "http://x", "org_admin")
    assert outcome.status == "failed"
    assert outcome.reason.startswith("waits on step 2c (multi-CREATE): refused (400): setup:")


@pytest.mark.parametrize(
    ("text", "got", "same"),
    [
        ("(:A:B {name: 'x'})", ("node", frozenset({"A", "B"}), {"name": "x"}), True),
        ("(:A {name: 'x'})", ("node", frozenset({"A"}), {"name": "y"}), False),
        ("[:T {k: 1}]", ("relationship", "T", {"k": 1}), True),
        (
            "<(:A)-[:T]->(:B)>",
            (
                "path",
                [
                    ("node", frozenset({"A"}), {}),
                    ("relationship", "T", {}),
                    ("node", frozenset({"B"}), {}),
                ],
            ),
            True,
        ),
    ],
)
def test_a_node_relationship_or_path_is_compared(text, got, same):
    # A session's Bolt answer gives graph values; a path's arrows are no nesting (<- and ->).
    assert tck._same(tck.tck_value(text), got, False) is same


def test_a_temporal_value_from_bolt_is_written_as_opencypher_writes_it():
    import datetime

    from neo4j import time

    assert bolt.value(time.Time(10, 35)) == "10:35"
    assert bolt.value(time.Time(12, 31, 14, 645876000)) == "12:31:14.645876"
    assert (
        bolt.value(time.Time(12, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-1))))
        == "12:00-01:00"
    )
    assert bolt.value(time.Date(1984, 10, 11)) == "1984-10-11"
    assert bolt.value(time.DateTime(1984, 10, 11, 12, 31, tzinfo=datetime.timezone.utc)) == (
        "1984-10-11T12:31Z"
    )
    assert bolt.value(time.Duration(months=14, days=3, seconds=3700)) == "P1Y2M3DT1H1M40S"
    assert bolt.value(time.Duration()) == "PT0S"


_SIDE_EFFECTS_FEATURE = '''
Feature: Remove3 - Removing a label

  Scenario: [1] Remove a label
    Given an empty graph
    And having executed:
      """
      CREATE (:L1:L2)
      """
    When executing query:
      """
      MATCH (n) REMOVE n:L2 RETURN n
      """
    Then the result should be, in any order:
      | n     |
      | (:L1) |
    And the side effects should be:
      | -labels | 1 |
'''


@pytest.fixture
def side_effect_scenario(tmp_path):
    path = tmp_path / "remove3.feature"
    path.write_text(_SIDE_EFFECTS_FEATURE)
    (scenario,) = tck.parse_feature(path)
    return scenario


def test_the_setup_is_only_the_steps_before_the_query(side_effect_scenario, monkeypatch):
    # The post-query `And the side effects should be` step starts with "the ", the mark of a named
    # graph; it is not setup, so the scenario's `having executed` runs in a Bolt session.
    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    assert [s.text for s in tck._setup_steps(side_effect_scenario)] == [
        "an empty graph",
        "having executed",
    ]
    assert tck._session_scenario(side_effect_scenario) == tck.IN_A_SESSION


@pytest.mark.parametrize(
    ("effects", "status", "reason"),
    [
        ({"-labels": 1}, "passed", tck.IN_A_SESSION),
        (
            {"-labels": 1, "+nodes": 1},
            "failed",
            f"side effects differ: +nodes 1, expected 0 {tck.IN_A_SESSION}",
        ),
        ({}, "failed", f"side effects differ: -labels 0, expected 1 {tck.IN_A_SESSION}"),
    ],
)
def test_a_session_scenario_s_side_effects_are_what_the_tck_observes(
    side_effect_scenario, monkeypatch, effects, status, reason
):
    sent = []

    def session(setups, query, params):
        sent.append((list(setups), query))
        rows = [[("node", frozenset({"L1"}), {})]]
        return 200, {"columns": ["n"], "rows": rows, "effects": effects}

    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    monkeypatch.setattr(bolt, "run", session)
    outcome = tck.run(side_effect_scenario, "http://x", "org_admin")
    assert sent == [(["CREATE (:L1:L2)"], "MATCH (n) REMOVE n:L2 RETURN n")]
    assert (outcome.status, outcome.reason) == (status, reason)


def test_side_effects_are_compared_exactly():
    assert tck.side_effects({}, {}) is None
    assert (
        tck.side_effects({"+nodes": 2, "+properties": 1}, {"+nodes": 2, "+properties": 1}) is None
    )
    assert tck.side_effects({"+nodes": 2}, {"+nodes": 1}) == "+nodes 1, expected 2"
    assert tck.side_effects({}, {"-relationships": 3}) == "-relationships 3, expected 0"
    assert tck.side_effects({"+nodes": 1}, {"+nodes": 1, "-nodes": 0}) is None
    assert tck.side_effects({"+widgets": 1}, {}) == "+widgets: no such side effect"


def test_a_removed_property_is_a_side_effect_like_any_other():
    # The TCK's -properties is a triple the graph held before and not after (README), which a
    # summary's counters have no name for; observed, it is compared as every side effect is.
    assert tck.side_effects({"+properties": 1, "-properties": 1}, {"+properties": 1}) == (
        "-properties 0, expected 1"
    )
    assert tck.side_effects({"-properties": 1}, {"-properties": 1}) is None
    assert tck.side_effects({"-properties": 0}, {}) is None


def _observed(nodes=(), rels=(), props=(), labels=()):
    return bolt.Observed(frozenset(nodes), frozenset(rels), frozenset(props), frozenset(labels))


def test_side_effects_are_the_difference_of_two_observations():
    before = _observed(["n:e:1"], [], [("n:e:1", "name", "'A'")], ["A"])
    after = _observed(
        ["n:e:1", "n:e:2"],
        ["r:7"],
        [("n:e:1", "name", "'B'"), ("n:e:2", "k", "1")],
        ["A", "B"],
    )
    assert bolt.effects(before, after) == {
        "+nodes": 1,
        "-nodes": 0,
        "+relationships": 1,
        "-relationships": 0,
        # A changed value is one property removed and one added (README: an (entity, key,
        # value) triple).
        "+properties": 2,
        "-properties": 1,
        "+labels": 1,
        "-labels": 0,
    }


def test_labels_are_observed_as_the_distinct_labels_the_graph_holds():
    # Create1 [6]: CREATE (:B:A:D), (:B:C), (:D:E:B) adds eight node labels, five labels.
    before = _observed()
    after = _observed(["n:e:1", "n:e:2", "n:e:3"], labels=["A", "B", "C", "D", "E"])
    assert bolt.effects(before, after)["+labels"] == 5


class _Node(dict):
    def __init__(self, labels, props):
        super().__init__(props)
        self.labels = frozenset(labels)


class _Rel(dict):
    def __init__(self, element_id, props):
        super().__init__(props)
        self.element_id = element_id


class _Session:
    def __init__(self, nodes, rels):
        self.nodes, self.rels = nodes, rels
        self.ran = []

    def run(self, query):
        self.ran.append(query)
        if "-[r]->" in query:
            if isinstance(self.rels, Exception):
                raise self.rels
            return [{"r": r} for r in self.rels]
        return [{"id": i, "n": n} for i, n in self.nodes]


def test_the_graph_is_observed_by_node_identity_whatever_label_reads_it():
    # A node with no label in the pattern is read as Label|key, its label whichever of its own
    # read it: its identity is its key, so a label added to it is no node removed and added.
    before = bolt.observe(_Session([("A|e:1", _Node(["A"], {"k": 1}))], []))
    after = bolt.observe(_Session([("Z|e:1", _Node(["A", "Z"], {"k": 1}))], [_Rel("9", {"w": 2})]))
    assert before.nodes == after.nodes == frozenset({"n:e:1"})
    assert bolt.effects(before, after) == {
        "+nodes": 0,
        "-nodes": 0,
        "+relationships": 1,
        "-relationships": 0,
        "+properties": 1,
        "-properties": 0,
        "+labels": 1,
        "-labels": 0,
    }


def test_a_graph_with_no_relationship_type_is_observed_holding_no_relationship():
    from neo4j.exceptions import Neo4jError

    refused = Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Statement.SyntaxError",
        message="No relationship types found in schema",
    )
    observed = bolt.observe(_Session([("|e:1", _Node([], {}))], refused))
    assert observed.relationships == frozenset()
    other = Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Statement.SyntaxError", message="something else"
    )
    with pytest.raises(Neo4jError):
        bolt.observe(_Session([], other))


_MERGE_ON_EMPTY = '''
Feature: Merge1 - Merge node

  Scenario: [1] Merge node when no nodes exist
    Given an empty graph
    When executing query:
      """
      MERGE (a)
      RETURN count(*) AS n
      """
    Then the result should be, in any order:
      | n |
      | 1 |
'''


def test_a_merge_on_an_empty_graph_runs_in_an_empty_session(tmp_path, monkeypatch):
    # A MERGE matches its pattern before it makes it: on the TCK's empty graph it must match the
    # session's empty graph, never the model's.
    path = tmp_path / "merge1.feature"
    path.write_text(_MERGE_ON_EMPTY)
    (scenario,) = tck.parse_feature(path)
    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    assert tck._session_scenario(scenario) == tck.EMPTY_GRAPH


# --- named TCK graphs ---------------------------------------------------------------------------

_NAMED_FEATURE = '''
Feature: TriadicSelection1 - Query three related nodes on binary-tree graphs

  Scenario: [1] Handling triadic friend of a friend
    Given the binary-tree-1 graph
    When executing query:
      """
      MATCH (a:A)-[:KNOWS]->(b)-->(c)
      RETURN c.name
      """
    Then the result should be, in any order:
      | c.name |
      | 'b2'   |
'''


@pytest.fixture
def named_graph_scenario(tmp_path, monkeypatch):
    features = tmp_path / "tck" / "features"
    features.mkdir(parents=True)
    graph = tmp_path / "tck" / "graphs" / "binary-tree-1"
    graph.mkdir(parents=True)
    (graph / "binary-tree-1.cypher").write_text(
        "CREATE (a:A {name: 'a'}), (b1:X {name: 'b1'})\nCREATE (a)-[:KNOWS]->(b1);\n"
    )
    monkeypatch.setitem(tck._GRAPHS, "graphs", tmp_path / "tck" / "graphs")
    path = features / "triadic.feature"
    path.write_text(_NAMED_FEATURE)
    (scenario,) = tck.parse_feature(path)
    return scenario


def test_a_named_graph_s_create_script_is_its_session_s_setup(named_graph_scenario, monkeypatch):
    sent = []

    def session(setups, query, params):
        sent.append((list(setups), query))
        return 200, {"columns": ["c.name"], "rows": [["b2"]], "stats": {}}

    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    monkeypatch.setattr(bolt, "run", session)
    assert tck._session_scenario(named_graph_scenario) == tck.NAMED_GRAPH
    outcome = tck.run(named_graph_scenario, "http://x", "org_admin")
    # The script as a `having executed` runs it: one statement, its closing semicolon left out.
    assert sent == [
        (
            ["CREATE (a:A {name: 'a'}), (b1:X {name: 'b1'})\nCREATE (a)-[:KNOWS]->(b1)"],
            "MATCH (a:A)-[:KNOWS]->(b)-->(c)\n      RETURN c.name",
        )
    ]
    assert (outcome.status, outcome.reason) == ("passed", tck.NAMED_GRAPH)
    doc = tck.report([outcome], "2024.3")
    assert [g["group"] for g in doc["adapted"]] == ["Named TCK graphs"]


def test_a_named_graph_the_tck_does_not_have_is_said(named_graph_scenario, monkeypatch):
    monkeypatch.setitem(tck._GRAPHS, "graphs", Path("/nonexistent"))
    with pytest.raises(ValueError, match="the TCK has no named graph binary-tree-1"):
        tck.named_graph("the binary-tree-1 graph")
    assert tck.named_graph("an empty graph") is None


def test_a_named_graph_with_no_session_is_not_runnable(named_graph_scenario, monkeypatch):
    monkeypatch.setattr(bolt, "_BOLT", {})
    outcome = tck.run(named_graph_scenario, "http://x", "org_admin")
    assert (outcome.status, outcome.reason) == ("not_runnable", "its setup uses a named TCK graph")


# --- test procedures as commands -----------------------------------------------------------------

_PROCEDURE_FEATURE = '''
Feature: Call2 - Procedure arguments

  Scenario: [1] In-query call to procedure with explicit arguments
    Given an empty graph
    And there exists a procedure test.my.proc(name :: STRING?, id :: INTEGER?) :: (city :: STRING?, country_code :: INTEGER?):
      | name     | id | city      | country_code |
      | 'Stefan' | 1  | 'Berlin'  | 49           |
      | 'Stefan' | 2  | 'München' | 49           |
    When executing query:
      """
      CALL test.my.proc('Stefan', 1) YIELD city, country_code
      RETURN city, country_code
      """
    Then the result should be, in order:
      | city     | country_code |
      | 'Berlin' | 49           |
    And no side effects

  Scenario: [2] A call with no procedure
    Given an empty graph
    When executing query:
      """
      CALL test.my.proc
      """
    Then a ProcedureError should be raised at compile time: ProcedureNotFound

  Scenario: [3] Standalone call to procedure with null argument
    Given an empty graph
    And there exists a procedure test.my.proc(in :: INTEGER?) :: (out :: STRING?):
      | in   | out   |
      | null | 'nix' |
    When executing query:
      """
      CALL test.my.proc(null)
      """
    Then the result should be, in order:
      | out   |
      | 'nix' |

  Scenario: [4] A VOID procedure
    Given an empty graph
    And there exists a procedure test.doNothing() :: ():
      |
    When executing query:
      """
      CALL test.doNothing()
      """
    Then the result should be empty

  Scenario: [5] The first procedure again
    Given an empty graph
    And there exists a procedure test.my.proc(name :: STRING?, id :: INTEGER?) :: (city :: STRING?, country_code :: INTEGER?):
      | name     | id | city      | country_code |
      | 'Stefan' | 1  | 'Berlin'  | 49           |
      | 'Stefan' | 2  | 'München' | 49           |
    When executing query:
      """
      CALL test.my.proc('Stefan', 2)
      """
    Then the result should be, in order:
      | city      | country_code |
      | 'München' | 49           |
'''


@pytest.fixture
def procedure_scenarios(tmp_path):
    path = tmp_path / "call2.feature"
    path.write_text(_PROCEDURE_FEATURE)
    return tck.parse_feature(path)


def test_a_procedure_s_signature_and_rows_are_read(procedure_scenarios):
    proc = tck.procedure(procedure_scenarios[0])
    assert proc == procedures.Procedure(
        "test.my.proc",
        (("name", "STRING"), ("id", "INTEGER")),
        (("city", "STRING"), ("country_code", "INTEGER")),
        (("Stefan", 1, "Berlin", 49), ("Stefan", 2, "München", 49)),
    )
    assert tck.procedure(procedure_scenarios[1]) is None
    void = tck.procedure(procedure_scenarios[3])
    assert (void.name, void.inputs, void.outputs, void.rows) == ("test.doNothing", (), (), ())


def test_a_procedure_is_a_statement_command_bound_in_cypher_to_its_name(procedure_scenarios):
    from provisa.core.command_bindings import check_names
    from provisa.core.models import Function

    entry = procedures.command(tck.procedure(procedure_scenarios[0]))
    assert {k: entry[k] for k in ("name", "impl_kind", "kind", "domain_id", "visible_to")} == {
        "name": "tck_test_my_proc",
        "impl_kind": "statement",
        "kind": "query",
        "domain_id": "tck_empty",
        "visible_to": [],
    }
    assert entry["arguments"] == [
        {"name": "name", "type": "text"},
        {"name": "id", "type": "bigint"},
    ]
    assert entry["language_bindings"] == {"cypher": {"name": "test.my.proc"}}
    # The model takes it as declared: a query, its statement naming each argument.
    function = Function(**entry)
    assert function.kind == "query"
    check_names([function.model_dump()])


@pytest.mark.parametrize(
    ("proc", "args", "rows"),
    [
        (
            procedures.Procedure(
                "test.my.proc",
                (("name", "STRING"), ("id", "INTEGER")),
                (("city", "STRING"), ("country_code", "INTEGER")),
                (("Stefan", 1, "Berlin", 49), ("Stefan", 2, "München", 49), ("Petra", 1, "x", 1)),
            ),
            ["Stefan", 2],
            [("München", 49)],
        ),
        (
            procedures.Procedure(
                "test.my.proc",
                (("in", "NUMBER"),),
                (("out", "STRING"),),
                ((42, "wisdom"), (42.3, "about right")),
            ),
            [42],
            [("wisdom",)],
        ),
        (
            procedures.Procedure(
                "test.my.proc", (("in", "INTEGER"),), (("out", "STRING"),), ((None, "nix"),)
            ),
            [None],
            [("nix",)],
        ),
        (
            procedures.Procedure("test.my.proc", (("in", "INTEGER"),), (("out", "INTEGER"),), ()),
            [1],
            [],
        ),
        (
            procedures.Procedure(
                "test.labels", (), (("label", "STRING"),), (("C",), ("A",), ("B",))
            ),
            [],
            [("C",), ("A",), ("B",)],
        ),
    ],
)
def test_a_procedure_s_statement_answers_its_rows_for_the_arguments(proc, args, rows):
    # As the pipeline runs it: the statement's arguments positional, on the DuckDB engine.
    import duckdb
    import sqlglot

    from provisa.core.statement_declaration import declared

    entry = procedures.command(proc)
    found = declared(entry["name"], entry["binding"], [a["name"] for a in entry["arguments"]])
    sql = sqlglot.transpile(found.sql, read="postgres", write="duckdb")[0]
    assert duckdb.execute(sql, args).fetchall() == rows


def test_a_void_procedure_is_no_command(procedure_scenarios):
    with pytest.raises(procedures.NotACommand, match="VOID"):
        procedures.command(tck.procedure(procedure_scenarios[3]))


def test_a_procedure_scenario_puts_the_model_holding_its_command(
    procedure_scenarios, tmp_path, monkeypatch
):
    import yaml

    model = tmp_path / "provisa.yaml"
    model.write_text(yaml.safe_dump({"domains": [{"id": "tck_empty"}], "tables": []}))
    monkeypatch.setattr(procedures, "_MODEL", {})
    procedures.configure("http://x", model, "su", "pw")
    put = []

    def _put(body):
        put.append(yaml.safe_load(body))
        return {"success": True, "message": "Config uploaded and applied"}

    monkeypatch.setattr(procedures, "_put", _put)
    answers = {
        "CALL test.my.proc('Stefan', 1) YIELD city, country_code": (
            200,
            {"columns": ["city", "country_code"], "rows": [["Berlin", 49]]},
        ),
        "CALL test.my.proc": (400, {"detail": "unknown command"}),
        "CALL test.my.proc(null)": (200, {"columns": ["out"], "rows": [["nix"]]}),
        "CALL test.my.proc('Stefan', 2)": (
            200,
            {"columns": ["city", "country_code"], "rows": [["München", 49]]},
        ),
    }

    def post(_b, _r, query, _p):
        return answers[query.split("\n")[0]]

    monkeypatch.setattr(tck, "_post", post)
    monkeypatch.setattr(bolt, "_BOLT", {})
    order = tck.in_running_order(procedure_scenarios)
    # No procedure first; then each procedure's scenarios together.
    assert order == [1, 0, 4, 2, 3]
    outcomes = {n: tck.run(procedure_scenarios[n], "http://x", "org_admin") for n in order}
    assert [(o.status, o.reason) for _, o in sorted(outcomes.items())] == [
        ("passed", tck.AS_A_COMMAND),
        ("passed", ""),
        ("passed", tck.AS_A_COMMAND),
        (
            "failed",
            "its test procedure is no command: the procedure test.doNothing is VOID (it has no "
            "outputs), which no command expresses: a command answers rows of its columns",
        ),
        ("passed", tck.AS_A_COMMAND),
    ]
    # The model is put once per procedure declared, its other blocks as it booted.
    assert [[f["language_bindings"]["cypher"]["name"] for f in m["functions"]] for m in put] == [
        ["test.my.proc"],
        ["test.my.proc"],
    ]
    assert [len(m["functions"][0]["arguments"]) for m in put] == [2, 1]
    assert all(m["domains"] == [{"id": "tck_empty"}] for m in put)
    doc = tck.report(list(outcomes.values()), "2024.3")
    (group,) = doc["adapted"]
    assert (group["group"], len(group["cases"])) == ("Test procedures", 3)


def test_a_procedure_with_no_model_to_declare_it_in_is_a_failure(procedure_scenarios, monkeypatch):
    monkeypatch.setattr(procedures, "_MODEL", {})
    monkeypatch.setattr(bolt, "_BOLT", {})
    outcome = tck.run(procedure_scenarios[0], "http://x", "org_admin")
    assert outcome.status == "failed"
    assert "CONFORMANCE_MODEL is not set" in outcome.reason


def test_a_model_not_applied_is_said(procedure_scenarios, tmp_path, monkeypatch):
    model = tmp_path / "provisa.yaml"
    model.write_text("domains: []\n")
    monkeypatch.setattr(procedures, "_MODEL", {})
    procedures.configure("http://x", model, "su", "pw")
    monkeypatch.setattr(procedures, "_put", lambda _b: {"success": False, "message": "bad"})
    with pytest.raises(ValueError, match="was not applied"):
        procedures.declare(tck.procedure(procedure_scenarios[0]))


# --- the feature filter (REQ-2167) ---------------------------------------------------------------

_FEATURES = [
    "Merge1 - Merge node",
    "Merge2 - Merge node onCreate",
    "Set1 - Set a Property",
    "Match1 - Match nodes",
]


def _by_feature():
    return [tck.Scenario(f, f"s of {f}") for f in _FEATURES]


def test_a_feature_filter_selects_by_glob_on_the_prefix_and_the_full_name():
    chosen = tck.select_scenarios(_by_feature(), "Merge*, Set1")
    assert [s.feature for s in chosen] == _FEATURES[:3]
    full = tck.select_scenarios(_by_feature(), "Match1 - Match nodes")
    assert [s.feature for s in full] == [_FEATURES[3]]


def test_an_empty_feature_filter_selects_every_scenario():
    every = _by_feature()
    assert tck.select_scenarios(every, "") == every
    assert tck.select_scenarios(every, "  ") == every


def test_a_feature_filter_matching_nothing_raises_naming_it():
    with pytest.raises(ValueError, match=r"Nope\*"):
        tck.select_scenarios(_by_feature(), "Nope*")
    with pytest.raises(ValueError, match="names no feature glob"):
        tck.select_scenarios(_by_feature(), ",")


def test_a_filtered_result_carries_the_filter_and_a_full_one_does_not():
    outcome = tck.Outcome("Merge1 - Merge node", "s", "passed", "")
    assert tck.report([outcome], "2024.3", "Merge*")["filter"] == "Merge*"
    assert "filter" not in tck.report([outcome], "2024.3", "")
    assert "filter" not in tck.report([outcome], "2024.3")


_CONTROL_FEATURE = '''
Feature: Merge6 - Merge relationships - on create

  Scenario: [3] Updating one property with ON CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'A'}), (:B {name: 'B'})
      """
    When executing query:
      """
      MATCH (a {name: 'A'}), (b {name: 'B'})
      MERGE (a)-[r:TYPE]->(b)
        ON CREATE SET r.name = 'foo'
      """
    Then the result should be empty
    And the side effects should be:
      | +relationships | 1 |
      | +properties    | 1 |
    When executing control query:
      """
      MATCH ()-[r:TYPE]->()
      RETURN [key IN keys(r) | key + '->' + r[key]] AS keyValue
      """
    Then the result should be, in any order:
      | keyValue      |
      | ['name->foo'] |
'''


@pytest.fixture
def control_scenario(tmp_path):
    path = tmp_path / "merge6.feature"
    path.write_text(_CONTROL_FEATURE)
    (scenario,) = tck.parse_feature(path)
    return scenario


@pytest.mark.parametrize(
    ("control_rows", "status"),
    [([[["name->foo"]]], "passed"), ([[["name->bar"]]], "failed")],
)
def test_a_control_query_runs_after_the_query_in_its_session_and_is_checked(
    control_scenario, monkeypatch, control_rows, status
):
    sent = []

    def session(setups, query, params, control=None):
        sent.append((list(setups), query.split()[0], control))
        effects = {"+relationships": 1, "+properties": 1}
        held = {"columns": ["keyValue"], "rows": control_rows}
        return 200, {"columns": [], "rows": [], "effects": effects, "control": held}

    monkeypatch.setattr(bolt, "_BOLT", {"uri": "bolt://x", "token": "t"})
    monkeypatch.setattr(bolt, "run", session)
    outcome = tck.run(control_scenario, "http://x", "org_admin")
    [(setups, verb, control)] = sent
    assert (setups, verb) == (["CREATE (:A {name: 'A'}), (:B {name: 'B'})"], "MATCH")
    assert control is not None and control.startswith("MATCH ()-[r:TYPE]->()")
    assert outcome.status == status
