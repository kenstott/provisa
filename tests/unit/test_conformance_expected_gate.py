# Copyright (c) 2026 Kenneth Stott
# Canary: 63a1347e-0fbb-40fb-ae2b-310ddd849726
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A conformance run fails on a case that fails and its suite's committed expected-failures list
does not hold (REQ-2167): scripts/conformance/baseline_gate.py against
scripts/conformance/expected/<surface>.json, and every conformance workflow ends in that gate.

"Conformance (Cypher)" concluded success on release commit ffea92f878 with 31 failing scenarios
(18 three days before) and nothing was red: the only per-scenario judgement lived outside the
repository. The list lives beside the suites, each entry naming the queue job that will fix it,
and it only shrinks: a listed case that now passes fails the run until it is taken off."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CONFORMANCE = REPO / "scripts" / "conformance"
WORKFLOWS = REPO / ".github" / "workflows"
sys.path.insert(0, str(CONFORMANCE))  # the gate's sibling import of by_design


def _load():
    spec = importlib.util.spec_from_file_location("expected_gate", CONFORMANCE / "baseline_gate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load()

SINCE = "2026-11-13"


def _entry(case: str, job: str = "J1275") -> dict:
    return {"case": case, "job": job, "since": SINCE}


def _failure(feature: str, scenario: str) -> dict:
    return {"feature": feature, "scenario": scenario, "reason": "refused"}


def _report(failures: list[dict], total: int = 10, **extra) -> dict:
    failed = len(failures)
    return {
        "surface": "bolt",
        "total": total,
        "passed": total - failed,
        "by_design": 0,
        "failed": failed,
        "failures": failures,
        **extra,
    }


def _run(tmp_path, monkeypatch, report: dict, expected, surface: str = "bolt") -> int:
    out = tmp_path / "report.json"
    out.write_text(json.dumps(report))
    (tmp_path / "expected").mkdir(exist_ok=True)
    (tmp_path / "expected" / f"{surface}.json").write_text(json.dumps(expected))
    monkeypatch.setattr(gate, "EXPECTED", tmp_path / "expected")
    return gate.main(["cases", surface, str(out)])


def _expected(entries: list[dict], total: int = 10) -> dict:
    return {"total": total, "expected": entries}


# --- cases ---------------------------------------------------------------------------------------


def test_a_new_failure_fails_the_run(tmp_path, monkeypatch, capsys):
    code = _run(tmp_path, monkeypatch, _report([_failure("F", "s1")]), _expected([]))
    assert code == 1
    assert "F::s1 failed and is not on the expected-failures list" in capsys.readouterr().err


def test_a_listed_failure_passes(tmp_path, monkeypatch, capsys):
    report = _report([_failure("F", "s1")])
    code = _run(tmp_path, monkeypatch, report, _expected([_entry("F::s1")]))
    assert code == 0, capsys.readouterr().err


def test_no_failures_and_an_empty_list_passes(tmp_path, monkeypatch, capsys):
    code = _run(tmp_path, monkeypatch, _report([]), _expected([]))
    assert code == 0, capsys.readouterr().err


def test_a_listed_case_that_now_passes_fails_as_fixed_but_still_listed(
    tmp_path, monkeypatch, capsys
):
    code = _run(tmp_path, monkeypatch, _report([]), _expected([_entry("F::s1")]))
    assert code == 1
    err = capsys.readouterr().err
    assert "F::s1 fixed but still listed" in err
    assert "J1275" in err


def test_a_filtered_run_cannot_pass_as_a_full_run(tmp_path, monkeypatch, capsys):
    report = _report([], total=10, filter="Merge*")
    code = _run(tmp_path, monkeypatch, report, _expected([]))
    assert code == 1
    assert "filtered" in capsys.readouterr().err


def test_a_partial_run_cannot_pass_as_a_full_run(tmp_path, monkeypatch, capsys):
    """A run with fewer cases than the suite has (a crash midway, a step bound hit) judges no
    full run, even with nothing failed."""
    code = _run(tmp_path, monkeypatch, _report([], total=7), _expected([], total=10))
    assert code == 1
    assert "7 of the suite's 10 cases" in capsys.readouterr().err


def test_an_empty_run_fails(tmp_path, monkeypatch):
    code = _run(tmp_path, monkeypatch, _report([], total=0), _expected([], total=10))
    assert code == 1


def test_an_entry_without_a_job_is_refused(tmp_path, monkeypatch, capsys):
    entry = {"case": "F::s1", "since": SINCE}
    code = _run(tmp_path, monkeypatch, _report([_failure("F", "s1")]), _expected([entry]))
    assert code == 1
    assert "no job" in capsys.readouterr().err


@pytest.mark.parametrize(
    "entry, said",
    [
        ({"case": "F::s1", "job": "J1275"}, "no since"),
        ({"case": "F::s1", "job": "someone", "since": SINCE}, "job"),
        ({"case": "F::s1", "job": "J1275", "since": "yesterday"}, "since"),
        ({"job": "J1275", "since": SINCE}, "no case"),
        ({"case": "F::s1", "job": "J1275", "since": SINCE, "owner": "x"}, "owner"),
    ],
)
def test_a_malformed_entry_is_refused(tmp_path, monkeypatch, capsys, entry, said):
    code = _run(tmp_path, monkeypatch, _report([_failure("F", "s1")]), _expected([entry]))
    assert code == 1
    assert said in capsys.readouterr().err


def test_a_case_listed_twice_is_refused(tmp_path, monkeypatch, capsys):
    report = _report([_failure("F", "s1")])
    code = _run(tmp_path, monkeypatch, report, _expected([_entry("F::s1"), _entry("F::s1")]))
    assert code == 1
    assert "listed twice" in capsys.readouterr().err


def test_a_missing_report_fails(tmp_path, monkeypatch, capsys):
    (tmp_path / "expected").mkdir()
    (tmp_path / "expected" / "bolt.json").write_text(json.dumps(_expected([])))
    monkeypatch.setattr(gate, "EXPECTED", tmp_path / "expected")
    assert gate.main(["cases", "bolt", str(tmp_path / "absent.json")]) == 1
    assert "no report" in capsys.readouterr().err


def test_a_missing_expected_file_fails(tmp_path, monkeypatch, capsys):
    out = tmp_path / "report.json"
    out.write_text(json.dumps(_report([])))
    monkeypatch.setattr(gate, "EXPECTED", tmp_path / "expected")
    assert gate.main(["cases", "bolt", str(out)]) == 1
    assert "no expected-failures file" in capsys.readouterr().err


def test_a_report_whose_failures_do_not_add_up_fails(tmp_path, monkeypatch, capsys):
    report = _report([_failure("F", "s1")])
    report["failed"] = 2
    code = _run(tmp_path, monkeypatch, report, _expected([_entry("F::s1")]))
    assert code == 1
    assert "lists 1 failure(s) but counts 2" in capsys.readouterr().err


def test_pgwire_failures_are_read_from_its_parts(tmp_path, monkeypatch, capsys):
    part = {"total": 5, "passed": 4, "by_design": 0, "failed": 1}
    report = {
        "surface": "pgwire",
        "total": 20,
        "passed": 19,
        "by_design": 0,
        "failed": 1,
        "pgspec": {**part, "failed": 0, "passed": 5, "failures": []},
        "catalog_relations": {**part, "failed": 0, "passed": 5, "failures": []},
        "catalog_functions": {**part, "failures": [_failure("catalog function", "f(int)")]},
        "type_conversions": {**part, "failed": 0, "passed": 5, "failures": []},
    }
    expected = _expected([_entry("catalog function::f(int)")], total=20)
    code = _run(tmp_path, monkeypatch, report, expected, surface="pgwire")
    assert code == 0, capsys.readouterr().err
    report["catalog_functions"] = None  # the comparison did not complete
    code = _run(tmp_path, monkeypatch, report, expected, surface="pgwire")
    assert code == 1
    assert "catalog_functions" in capsys.readouterr().err


def test_a_surface_at_its_baseline_is_judged_by_its_list(tmp_path, monkeypatch, capsys):
    """pgwire is at its baseline and its summary gates pgspec; a catalog failure it does not gate
    is held by the list like any other surface's."""
    report = {**_report([_failure("F", "s1")]), "surface": "graphql"}
    code = _run(tmp_path, monkeypatch, report, _expected([_entry("F::s1")]), surface="graphql")
    assert code == 0, capsys.readouterr().err
    code = _run(tmp_path, monkeypatch, report, _expected([]), surface="graphql")
    assert code == 1


# --- functions -----------------------------------------------------------------------------------

import function_legs  # noqa: E402  (beside the gate, on the path above)
import function_report  # noqa: E402
import functions_baseline  # noqa: E402

RIGHT = {"outcome": "pass"}
REFUSED = {"outcome": "refused", "detail": {"statement": "SELECT r()", "error": "r() has no form"}}


def _base_wrong() -> dict:
    return {
        "outcome": "wrong_answer",
        "detail": {"statement": "SELECT w(3)", "difference": "'4', PostgreSQL '3'"},
    }


def _base_error() -> dict:
    return {
        "outcome": "error",
        "detail": {"statement": "SELECT e(3)", "error": "ERROR:  boom", "sqlstate": "XX000"},
    }


def _edge_wrong(calls: int = 1) -> dict:
    return {
        "outcome": "wrong_answer",
        "wrong_at_edge": True,
        "edges": {"calls": 8, "agreed": 8 - calls, "wrong": calls, "raised": 0, "errors": 0},
        "detail": {
            "statement": "SELECT w('-9223372036854775808'::bigint) LIMIT 1001",
            "edge": "bigint least",
            "difference": "'9223372036854775808', PostgreSQL raised 22003 ERROR:  out of range",
        },
    }


def _edge_error(calls: int = 1, postgres: str | None = None) -> dict:
    detail = {
        "statement": "SELECT e('NaN'::numeric) LIMIT 1001",
        "edge": "numeric special",
        "error": "ERROR:  no NaN here",
        "sqlstate": "22003",
        "unstated": "it raises at an edge value; its grid row on trino states no bound",
    }
    if postgres:
        detail["postgres"] = postgres
    return {
        "outcome": "error",
        "error_at_edge": True,
        "edges": {"calls": 8, "agreed": 8 - calls, "wrong": 0, "raised": calls, "errors": 0},
        "detail": detail,
    }


def _functions_doc(*failing: dict, right: int = 8, refused: int = 1) -> dict:
    """A merged run of one engine: ``right`` overloads passing, ``refused`` refused by name, and
    one overload f0, f1, ... per outcome of ``failing``."""
    outcomes = [*failing, *([RIGHT] * right), *([REFUSED] * refused)]
    fns = [
        {"function": f"f{i}", "overload": "integer", "family": "Numeric", "outcomes": {"trino": o}}
        for i, o in enumerate(outcomes)
    ]
    return {
        "engines": ["trino"],
        "engines_without_a_result": [],
        "engines_incomplete": [],
        "by_engine": {"trino": function_report.result_counts(outcomes)},
        "functions": fns,
    }


def _baseline(doc: dict, kept: dict | None = None, job: str = "J1388b") -> dict:
    return functions_baseline.baseline(doc, kept or {}, job, SINCE, {"run": "1", "sha": "abc"})


def _run_functions(tmp_path, monkeypatch, doc, expected) -> int:
    out = tmp_path / "function_support.json"
    out.write_text(json.dumps(doc))
    (tmp_path / "expected").mkdir(exist_ok=True)
    (tmp_path / "expected" / "functions.json").write_text(functions_baseline.written(expected))
    monkeypatch.setattr(gate, "EXPECTED", tmp_path / "expected")
    return gate.main(["functions", str(out)])


def test_functions_at_its_baseline_passes_and_prints_it(tmp_path, monkeypatch, capsys):
    """A run judged against the file written from it is green, whatever it found: the edge
    findings are the measured starting point."""
    doc = _functions_doc(_edge_wrong(), _edge_error(), _base_error())
    assert _run_functions(tmp_path, monkeypatch, doc, _baseline(doc)) == 0, capsys.readouterr().err
    out = capsys.readouterr().out
    assert "base calls: 10 right, 0 wrong, 1 engine errors, 1 refused by name" in out
    assert "edge calls: 8 served, 1 wrong, 1 engine errors" in out


def test_functions_an_edge_failure_does_not_lower_the_base_count():
    """An edge is tried only when the base calls are right: an overload wrong at an edge is one
    whose base calls pass, so base passed is what it was before the edge measure."""
    before = gate.function_counts(_functions_doc(RIGHT, RIGHT), "trino")
    after = gate.function_counts(_functions_doc(_edge_wrong(), _edge_error()), "trino")
    assert before["base"] == after["base"]
    assert (before["edge"]["served"], after["edge"]["served"]) == (10, 8)
    assert (after["edge"]["wrong_answer"], after["edge"]["engine_error"]) == (1, 1)


def test_functions_a_new_edge_wrong_answer_fails(tmp_path, monkeypatch, capsys):
    """One overload more answering wrong at an edge than the baseline lists: red, and the error
    names the call and both answers."""
    expected = _baseline(_functions_doc(_edge_wrong(), RIGHT))
    doc = _functions_doc(_edge_wrong(), _edge_wrong())
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    err = capsys.readouterr().err
    assert "trino::f1(integer) is a wrong answer at an edge value and is not on" in err
    assert "bigint" in err and "PostgreSQL raised 22003" in err
    assert "trino::f0(integer)" not in err  # the listed one is still the baseline


def test_functions_a_base_call_wrong_answer_fails(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(_edge_wrong(), RIGHT))
    doc = _functions_doc(_edge_wrong(), _base_wrong())
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    err = capsys.readouterr().err
    assert "trino::f1(integer) is a wrong answer on a base call and is not on" in err
    assert "base passed 9, below the committed 10" in err


def test_functions_a_new_edge_engine_error_fails(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(RIGHT))
    doc = _functions_doc(_edge_error())
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    assert "trino::f0(integer) is an engine error at an edge value" in capsys.readouterr().err


def test_functions_a_listed_edge_entry_whose_base_call_now_fails_is_red(
    tmp_path, monkeypatch, capsys
):
    """The list holds it at an edge; failing its base call is a regression of the base measure,
    which the entry does not cover."""
    expected = _baseline(_functions_doc(_edge_wrong()))
    doc = _functions_doc(_base_wrong())
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    err = capsys.readouterr().err
    assert "is listed as a wrong answer at an edge value (J1388b) and is now" in err
    assert "a wrong answer on a base call" in err


def test_functions_a_listed_engine_error_now_a_wrong_answer_is_red(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(_edge_error()))
    doc = _functions_doc(_edge_wrong())
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    assert "is listed as an engine error at an edge value" in capsys.readouterr().err


def test_functions_more_failing_edge_calls_than_listed_is_red(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(_edge_wrong(calls=1)))
    doc = _functions_doc(_edge_wrong(calls=2))
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    assert "fails 2 edge calls, above the listed 1" in capsys.readouterr().err


def test_functions_fewer_failing_edge_calls_than_listed_is_said(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(_edge_wrong(calls=2)))
    doc = _functions_doc(_edge_wrong(calls=1))
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 0
    assert "fails 1 edge calls (listed 2): regenerate" in capsys.readouterr().err


def test_functions_a_listed_failure_now_right_fails(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(_edge_wrong()))
    assert _run_functions(tmp_path, monkeypatch, _functions_doc(RIGHT), expected) == 1
    err = capsys.readouterr().err
    assert "trino::f0(integer) fixed but still listed (J1388b)" in err


def test_functions_base_passed_below_the_committed_count_fails(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(right=8, refused=1))
    doc = _functions_doc(right=7, refused=2)
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    err = capsys.readouterr().err
    assert "base passed 7, below the committed 8" in err
    assert "base refused_by_name 2, above the committed 1" in err


def test_functions_a_count_that_moves_the_good_way_is_said(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(right=7, refused=2))
    doc = _functions_doc(right=8, refused=1)
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 0
    err = capsys.readouterr().err
    assert "base passed 8 (committed 7): regenerate functions.json" in err
    assert "base refused_by_name 1 (committed 2)" in err


def test_functions_an_engine_with_no_result_fails(tmp_path, monkeypatch):
    doc = _functions_doc()
    expected = _baseline(doc)
    doc["engines_without_a_result"] = ["trino"]
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1


def test_functions_an_engine_missing_from_the_run_fails(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc())
    expected["engines"]["duckdb"] = dict(expected["engines"]["trino"])
    assert _run_functions(tmp_path, monkeypatch, _functions_doc(), expected) == 1
    assert "duckdb" in capsys.readouterr().err


def test_functions_a_partial_run_fails(tmp_path, monkeypatch, capsys):
    expected = _baseline(_functions_doc(right=8))
    assert _run_functions(tmp_path, monkeypatch, _functions_doc(right=5), expected) == 1
    assert "6 of the committed 9 overloads: not a full run" in capsys.readouterr().err


def test_functions_an_entry_without_a_job_is_refused(tmp_path, monkeypatch, capsys):
    doc = _functions_doc(_edge_wrong())
    expected = _baseline(doc)
    del expected["expected"][0]["job"]
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    assert "no job" in capsys.readouterr().err


@pytest.mark.parametrize(
    "change, said",
    [
        (lambda e: e.pop("at"), "does not say where"),
        (lambda e: e.update(kind="slow"), "does not say where"),
        (lambda e: e.pop("statement"), "does not name its failing call"),
        (lambda e: e.pop("calls"), "an edge entry, and no other, has calls and edge"),
        (lambda e: e.update(case="duckdb::f0(integer)"), "an engine the file does not hold"),
        (lambda e: e.update(owner="x"), "owner"),
    ],
)
def test_functions_a_malformed_entry_is_refused(tmp_path, monkeypatch, capsys, change, said):
    doc = _functions_doc(_edge_wrong())
    expected = _baseline(doc)
    change(expected["expected"][0])
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    assert said in capsys.readouterr().err


def test_functions_counts_that_are_not_the_lists_own_are_refused(tmp_path, monkeypatch, capsys):
    """The file is written from a run: a count typed over, or an entry taken off by hand without
    its count, is no baseline."""
    doc = _functions_doc(_edge_wrong(), _edge_wrong())
    expected = _baseline(doc)
    del expected["expected"][1]
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    err = capsys.readouterr().err
    assert "trino counts 2 edge wrong_answer and lists 1: regenerate it" in err


# --- the functions file's generator ---------------------------------------------------------------


def test_the_baseline_names_each_entrys_engine_overload_call_and_both_answers():
    doc = _functions_doc(
        _edge_wrong(),
        _edge_error(postgres="22003 ERROR:  numeric field overflow"),
        _edge_error(),
        _base_error(),
    )
    wrong, raised_too, raised, base = _baseline(doc)["expected"]
    assert wrong == {
        "case": "trino::f0(integer)",
        "at": "edge",
        "kind": "wrong_answer",
        "calls": 1,
        "statement": "SELECT w('-9223372036854775808'::bigint) LIMIT 1001",
        "edge": "bigint least",
        "said": "'9223372036854775808', PostgreSQL raised 22003 ERROR:  out of range",
        "job": "J1388b",
        "since": SINCE,
    }
    assert (raised_too["said"], raised_too["sqlstate"]) == ("ERROR:  no NaN here", "22003")
    assert raised_too["postgres"] == "22003 ERROR:  numeric field overflow"
    assert raised["postgres"] == "answers"  # PostgreSQL answered where the engine raised
    assert (base["at"], base["kind"]) == ("base", "engine_error")
    assert "calls" not in base and "edge" not in base and "postgres" not in base


def test_a_regenerated_baseline_keeps_a_listed_entrys_job_and_day():
    doc = _functions_doc(_edge_wrong(), _edge_error())
    first = _baseline(doc)
    first["expected"][0].update(job="J1400", since="2026-11-20")
    kept = {e["case"]: e for e in first["expected"]}
    moved = _functions_doc(_edge_wrong(), _edge_wrong(), _edge_error())
    again = functions_baseline.baseline(moved, kept, "J1500", "2026-12-01", {})
    by_case = {e["case"]: e for e in again["expected"]}
    assert (by_case["trino::f0(integer)"]["job"], by_case["trino::f0(integer)"]["since"]) == (
        "J1400",
        "2026-11-20",
    )
    # f1 was listed as an engine error and is now a wrong answer: a new entry.
    assert by_case["trino::f1(integer)"]["job"] == "J1500"
    assert by_case["trino::f2(integer)"]["since"] == "2026-12-01"


def test_a_baseline_is_not_written_from_a_run_with_a_leg_missing():
    doc = _functions_doc()
    doc["engines_incomplete"] = ["trino"]
    # The generator's own import of the gate, not this file's copy of it (``_load``).
    with pytest.raises(functions_baseline.baseline_gate.Refused, match="of a full run"):
        _baseline(doc)


def test_a_long_answer_is_clipped_in_the_list():
    doc = _functions_doc(_edge_wrong())
    doc["functions"][0]["outcomes"]["trino"]["detail"]["difference"] = "x" * 5000
    (entry,) = _baseline(doc)["expected"]
    assert len(entry["said"]) == functions_baseline.SAID_MAX


def test_the_written_file_reads_back_and_lists_one_entry_a_line():
    made = _baseline(_functions_doc(_edge_wrong(), _edge_error()))
    text = functions_baseline.written(made)
    assert json.loads(text) == made
    assert sum(1 for line in text.splitlines() if line.startswith('  {"case"')) == 2
    assert json.loads(functions_baseline.written({**made, "expected": []}))["expected"] == []


# --- the committed files --------------------------------------------------------------------------

CASE_SURFACES = ("cypher", "bolt", "flight-sql", "pgwire", "graphql", "sparql")


@pytest.mark.parametrize("surface", CASE_SURFACES)
def test_each_committed_expected_file_is_well_formed(surface):
    assert gate.load_expected(surface) is not None


def test_the_committed_functions_file_is_well_formed():
    """Its counts are its list's own, base calls and edge calls apart, every entry names its
    engine, overload, failing call and what was said, and it judges every engine the run that
    writes it measures: the free lane's legs that build_engine builds (function_legs.py plan, the
    workflow's matrix), so an engine the run measures is never left unjudged."""
    path, engines, listed = gate.load_functions_expected()
    planned = function_legs.plan("free", function_legs.load())["engines"].split(",")
    assert sorted(engines) == sorted(planned)
    assert json.loads(path.read_text())["measured"]["run"]
    for counts in engines.values():
        edge = counts["edge"]
        assert counts["base"]["passed"] == (
            edge["served"] + edge["wrong_answer"] + edge["engine_error"]
        )
    assert len(listed) == sum(
        c[at][kind] for c in engines.values() for at in gate.AT for kind in gate.KINDS.values()
    )


def test_the_committed_functions_file_is_as_its_generator_writes_it():
    """Typed by nobody: writing the file again from its own content changes nothing."""
    path = gate.EXPECTED / "functions.json"
    assert functions_baseline.written(json.loads(path.read_text())) == path.read_text()


# --- the workflows -------------------------------------------------------------------------------

#: Every conformance workflow, and the gate command its judging job ends in. A workflow that can
#: conclude success while a case fails ends in the gate.
GATED = {
    "conformance-cypher.yml": ("cypher", "baseline_gate.py cases cypher"),
    "conformance-bolt.yml": ("bolt", "baseline_gate.py cases bolt"),
    "conformance-flight-sql.yml": ("flight-sql", "baseline_gate.py cases flight-sql"),
    "conformance-pgwire.yml": ("pgwire", "baseline_gate.py cases pgwire"),
    "conformance-graphql.yml": ("graphql", "baseline_gate.py cases graphql"),
    "conformance-sparql.yml": ("sparql", "baseline_gate.py cases sparql"),
    "conformance-functions.yml": ("merge", "baseline_gate.py functions"),
}

#: Conformance workflows that cannot drop silently without the gate, with the reason.
CANNOT_DROP_SILENTLY = {
    # driver_suite.py summarise fails the job on a test that fails and is not on
    # scripts/conformance/drivers/<driver>.yaml, and on a listed test that passes (REQ-2203): the
    # same per-case list, kept per driver with each entry's reason.
    "conformance-drivers.yml": "driver_suite.py summarise",
}


def test_every_conformance_workflow_is_gated_or_stated():
    found = {p.name for p in WORKFLOWS.glob("conformance-*.yml")}
    assert found == set(GATED) | set(CANNOT_DROP_SILENTLY)


@pytest.mark.parametrize("name", sorted(GATED))
def test_a_gated_workflow_ends_in_the_gate(name):
    job_key, command = GATED[name]
    doc = yaml.safe_load((WORKFLOWS / name).read_text())
    jobs = doc["jobs"]
    job = jobs[job_key] if job_key in jobs else next(iter(jobs.values()))
    last = job["steps"][-1]
    assert command in last.get("run", ""), f"{name}: its last step is {last.get('name')!r}"
    # Judged whatever an earlier step did, and only a cancelled run goes unjudged.
    assert last.get("if") == "${{ !cancelled() }}"
    assert not last.get("continue-on-error")


@pytest.mark.parametrize("name, command", sorted(CANNOT_DROP_SILENTLY.items()))
def test_a_stated_workflow_runs_its_own_list_gate(name, command):
    text = (WORKFLOWS / name).read_text()
    assert command in text


@pytest.mark.parametrize("name", sorted(set(GATED) | set(CANNOT_DROP_SILENTLY)))
def test_every_conformance_workflow_runs_for_a_release_or_by_hand_and_never_on_a_push_to_main(name):
    doc = yaml.safe_load((WORKFLOWS / name).read_text())
    on = doc[True] if True in doc else doc["on"]  # YAML 1.1 reads the bare key on as True
    assert "schedule" not in on, f"{name} runs on a schedule: main runs no conformance suite"
    push = on.get("push") or {}
    assert "main" not in (push.get("branches") or [])
