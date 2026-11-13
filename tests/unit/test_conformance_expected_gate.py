# Copyright (c) 2026 Kenneth Stott
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


def _fn(name: str, outcomes: dict[str, str]) -> dict:
    return {
        "function": name,
        "overload": "integer",
        "outcomes": {e: {"outcome": o} for e, o in outcomes.items()},
    }


def _functions_doc(
    passed: int = 8, errors: int = 1, refused: int = 1, wrong: tuple[str, ...] = ()
) -> dict:
    fns = [_fn(f"w{i}", {"trino": "wrong_answer"}) for i, _ in enumerate(wrong)]
    return {
        "engines": ["trino"],
        "engines_without_a_result": [],
        "engines_incomplete": [],
        "by_engine": {
            "trino": {
                "total": 10,
                "passed": passed,
                "differs_wrong_answer": len(wrong),
                "differs_engine_error": errors,
                "refused_by_name": refused,
            }
        },
        "functions": fns,
    }


def _functions_expected(entries=(), passed=8, errors=1, refused=1) -> dict:
    return {
        "engines": {
            "trino": {
                "total": 10,
                "passed": passed,
                "differs_engine_error": errors,
                "refused_by_name": refused,
            }
        },
        "expected": list(entries),
    }


def _run_functions(tmp_path, monkeypatch, doc, expected) -> int:
    out = tmp_path / "function_support.json"
    out.write_text(json.dumps(doc))
    (tmp_path / "expected").mkdir(exist_ok=True)
    (tmp_path / "expected" / "functions.json").write_text(json.dumps(expected))
    monkeypatch.setattr(gate, "EXPECTED", tmp_path / "expected")
    return gate.main(["functions", str(out)])


def test_functions_at_the_committed_counts_pass(tmp_path, monkeypatch, capsys):
    assert _run_functions(tmp_path, monkeypatch, _functions_doc(), _functions_expected()) == 0, (
        capsys.readouterr().err
    )


def test_functions_an_unlisted_wrong_answer_fails(tmp_path, monkeypatch, capsys):
    doc = _functions_doc(passed=7, wrong=("w0",))
    assert _run_functions(tmp_path, monkeypatch, doc, _functions_expected(passed=7)) == 1
    assert "trino::w0(integer) answers wrong" in capsys.readouterr().err


def test_functions_a_listed_wrong_answer_passes(tmp_path, monkeypatch, capsys):
    doc = _functions_doc(passed=7, wrong=("w0",))
    expected = _functions_expected([_entry("trino::w0(integer)", "J1272")], passed=7)
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 0, capsys.readouterr().err


def test_functions_a_listed_wrong_answer_now_right_fails(tmp_path, monkeypatch, capsys):
    expected = _functions_expected([_entry("trino::w0(integer)", "J1272")])
    assert _run_functions(tmp_path, monkeypatch, _functions_doc(), expected) == 1
    assert "fixed but still listed" in capsys.readouterr().err


def test_functions_passed_below_the_committed_count_fails(tmp_path, monkeypatch, capsys):
    doc = _functions_doc(passed=7, errors=2)
    assert _run_functions(tmp_path, monkeypatch, doc, _functions_expected()) == 1
    err = capsys.readouterr().err
    assert "passed 7, below the committed 8" in err
    assert "differs_engine_error 2, above the committed 1" in err


def test_functions_refusals_may_not_rise(tmp_path, monkeypatch, capsys):
    doc = _functions_doc(passed=7, refused=2)
    assert _run_functions(tmp_path, monkeypatch, doc, _functions_expected(passed=7)) == 1
    assert "refused_by_name 2, above the committed 1" in capsys.readouterr().err


def test_functions_an_engine_with_no_result_fails(tmp_path, monkeypatch):
    doc = _functions_doc()
    doc["engines_without_a_result"] = ["trino"]
    assert _run_functions(tmp_path, monkeypatch, doc, _functions_expected()) == 1


def test_functions_an_engine_missing_from_the_run_fails(tmp_path, monkeypatch, capsys):
    expected = _functions_expected()
    expected["engines"]["duckdb"] = dict(expected["engines"]["trino"])
    assert _run_functions(tmp_path, monkeypatch, _functions_doc(), expected) == 1
    assert "duckdb" in capsys.readouterr().err


def test_functions_an_entry_without_a_job_is_refused(tmp_path, monkeypatch, capsys):
    doc = _functions_doc(passed=7, wrong=("w0",))
    expected = _functions_expected([{"case": "trino::w0(integer)", "since": SINCE}], passed=7)
    assert _run_functions(tmp_path, monkeypatch, doc, expected) == 1
    assert "no job" in capsys.readouterr().err


# --- the committed files --------------------------------------------------------------------------

CASE_SURFACES = ("cypher", "bolt", "flight-sql", "pgwire", "graphql")


@pytest.mark.parametrize("surface", CASE_SURFACES)
def test_each_committed_expected_file_is_well_formed(surface):
    assert gate.load_expected(surface) is not None


def test_the_committed_functions_file_is_well_formed():
    assert gate.load_functions_expected() is not None


# --- the workflows -------------------------------------------------------------------------------

#: Every conformance workflow, and the gate command its judging job ends in. A workflow that can
#: conclude success while a case fails ends in the gate.
GATED = {
    "conformance-cypher.yml": ("cypher", "baseline_gate.py cases cypher"),
    "conformance-bolt.yml": ("bolt", "baseline_gate.py cases bolt"),
    "conformance-flight-sql.yml": ("flight-sql", "baseline_gate.py cases flight-sql"),
    "conformance-pgwire.yml": ("pgwire", "baseline_gate.py cases pgwire"),
    "conformance-graphql.yml": ("graphql", "baseline_gate.py cases graphql"),
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
def test_every_conformance_workflow_runs_nightly_and_never_on_a_push_to_main(name):
    doc = yaml.safe_load((WORKFLOWS / name).read_text())
    on = doc[True] if True in doc else doc["on"]  # YAML 1.1 reads the bare key on as True
    assert on.get("schedule"), f"{name} has no schedule"
    push = on.get("push") or {}
    assert "main" not in (push.get("branches") or [])
