# Copyright (c) 2026 Kenneth Stott
# Canary: 63ec2e1e-d497-4596-a056-944bc4d2418f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A driver's own test suite against its expected-failure list (REQ-2203): the four counts, and
a run that fails only on an unexpected failure or an unexpected pass."""

# Requirements: REQ-2203

from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CONFORMANCE = REPO / "scripts" / "conformance"
WORKFLOW = REPO / ".github" / "workflows" / "conformance-drivers.yml"


def _load():
    spec = importlib.util.spec_from_file_location("driver_suite", CONFORMANCE / "driver_suite.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["driver_suite"] = module  # a dataclass resolves its module by name
    spec.loader.exec_module(module)
    return module


ds = _load()


def _list(tmp_path: Path, body: dict, driver: str = "pgjdbc") -> Path:
    tmp_path.joinpath(f"{driver}.yaml").write_text(yaml.safe_dump(body))
    return tmp_path


_LIST = {
    "driver": "pgjdbc",
    "version": "REL42.7.14",
    "reasons": {
        "writes": {"requirement": "REQ-2203", "decision": "Tables are the model's."},
        "hold-cursor": {"defect": "WITH HOLD cursors are refused."},
    },
    "expected": {
        "a.WriteTest::testCreate": "writes",
        "a.WriteTest::testInsert": "writes",
        "a.CursorTest::testHold": "hold-cursor",
        "a.Fixed::testNowPasses": "hold-cursor",
        "a.Gone::testRemoved": "writes",
        "a.Skips::testSkipped": "writes",
    },
}


def _junit(path: Path, cases: Sequence[tuple[str, str, str | None, str]]) -> Path:
    """``cases``: (classname, name, failure|error|skipped|None, message)."""
    body = []
    for classname, name, mark, message in cases:
        inner = f'<{mark} message="{message}">trace</{mark}>' if mark else ""
        body.append(f'<testcase classname="{classname}" name="{name}">{inner}</testcase>')
    path.write_text(f'<testsuite name="s">{"".join(body)}</testsuite>')
    return path


_RUN = [
    ("a.Plain", "testSelect", None, ""),
    ("a.Plain", "testSelectAgain", None, ""),
    ("a.WriteTest", "testCreate", "failure", "ERROR: DDL is refused"),
    ("a.WriteTest", "testInsert", "error", "ERROR: writes are refused"),
    ("a.CursorTest", "testHold", "failure", "portal does not exist"),
    ("a.Fixed", "testNowPasses", None, ""),
    ("a.New", "testBroken", "failure", "unexpected EOF&#10;at line 2"),
    ("a.Skips", "testSkipped", "skipped", "needs server 18"),
    ("a.Skips", "testGated", "skipped", "numpy not installed"),
]


def _doc(tmp_path: Path) -> dict:
    expected = ds.load("pgjdbc", _list(tmp_path, _LIST))
    results = ds.read_results(str(_junit(tmp_path / "run.xml", _RUN)))
    return ds.summarise(expected, "REL42.7.14", results, "pgjdbc's own tests")


# --- the four counts --------------------------------------------------------------------------------


def test_each_test_is_in_one_of_the_four_counts(tmp_path):
    doc = _doc(tmp_path)
    # passed: two plain tests and the listed one that now passes.
    assert (doc["passed"], doc["by_design"], doc["failed"], doc["not_tested"]) == (3, 2, 2, 2)
    assert doc["total"] == len(_RUN) == 9
    assert [g["reason"] for g in doc["by_design_groups"]] == ["writes"]
    assert [t["test"] for t in doc["by_design_groups"][0]["tests"]] == [
        "a.WriteTest::testCreate",
        "a.WriteTest::testInsert",
    ]
    # An open defect is failed, as is a failure no entry names.
    assert [(d["reason"], d["count"]) for d in doc["defects"]] == [("hold-cursor", 1)]
    assert doc["unexpected_failures"] == [
        {"test": "a.New::testBroken", "message": "unexpected EOF"}
    ]


def test_a_test_the_suite_skipped_is_not_tested_even_when_listed(tmp_path):
    doc = _doc(tmp_path)
    assert {t["test"]: t["reason"] for t in doc["not_tested_tests"]} == {
        "a.Skips::testGated": "numpy not installed",
        "a.Skips::testSkipped": "needs server 18",
    }
    assert doc["listed_not_run"] == ["a.Gone::testRemoved"]


def test_the_run_fails_on_an_unexpected_failure_and_an_unexpected_pass(tmp_path, capsys):
    doc = _doc(tmp_path)
    assert doc["unexpected_passes"] == [{"test": "a.Fixed::testNowPasses", "listed": "hold-cursor"}]
    assert ds.gate_failed(doc)
    err = capsys.readouterr().err
    assert "::error::pgjdbc: a.New::testBroken failed and is not on the list" in err
    assert "::error::pgjdbc: a.Fixed::testNowPasses passed but is listed (hold-cursor)" in err
    # A listed test the suite did not run is reported, never a failure of the run.
    assert "a.Gone::testRemoved" not in err


def test_the_run_passes_when_every_failure_is_listed_and_no_listed_test_passes(tmp_path):
    expected = ds.load("pgjdbc", _list(tmp_path, _LIST))
    run = [c for c in _RUN if c[0] not in ("a.New", "a.Fixed")]
    results = ds.read_results(str(_junit(tmp_path / "run.xml", run)))
    doc = ds.summarise(expected, "REL42.7.14", results, "suite")
    assert not ds.gate_failed(doc)
    assert doc["failed"] == 1  # the open defect stays failed, and does not fail the run


def test_a_run_with_no_test_fails(tmp_path, capsys):
    expected = ds.load("pgjdbc", _list(tmp_path, {**_LIST, "reasons": {}, "expected": {}}))
    doc = ds.summarise(expected, "REL42.7.14", {}, "suite")
    assert ds.gate_failed(doc)
    assert "no test ran" in capsys.readouterr().err


def test_a_run_at_another_version_than_the_list_is_refused(tmp_path):
    expected = ds.load("pgjdbc", _list(tmp_path, _LIST))
    with pytest.raises(ValueError, match="re-seed the list"):
        ds.summarise(expected, "REL42.7.15", {}, "suite")


# --- the results ------------------------------------------------------------------------------------


def test_results_are_read_from_files_and_directories_of_them(tmp_path):
    (tmp_path / "d").mkdir()
    _junit(tmp_path / "d" / "TEST-a.xml", [("a.X", "t1", None, "")])
    _junit(tmp_path / "d" / "TEST-b.xml", [("a.Y", "t2", "error", "boom")])
    _junit(tmp_path / "one.xml", [("a.Z", "t3", "skipped", "later")])
    results = ds.read_results(f"{tmp_path / 'd'},{tmp_path / 'one.xml'}")
    assert results == {
        "a.X::t1": {"outcome": "passed", "message": ""},
        "a.Y::t2": {"outcome": "failed", "message": "boom"},
        "a.Z::t3": {"outcome": "skipped", "message": "later"},
    }


def test_a_test_reported_twice_is_failed_when_any_report_failed(tmp_path):
    run = [
        ("a.P", "t[1]", None, ""),
        ("a.P", "t[1]", "failure", "second"),
        ("a.P", "t[1]", "skipped", "third"),
        ("a.Q", "u", "skipped", "s"),
        ("a.Q", "u", None, ""),
    ]
    results = ds.read_results(str(_junit(tmp_path / "r.xml", run)))
    assert results["a.P::t[1]"] == {"outcome": "failed", "message": "second"}
    assert results["a.Q::u"]["outcome"] == "passed"


def test_a_later_attempt_replaces_an_earlier_one_for_the_tests_it_reran(tmp_path):
    first = _junit(
        tmp_path / "1.xml",
        [
            ("a.P", "flaky", "failure", "late"),
            ("a.P", "broken", "failure", "x"),
            ("a.P", "ok", None, ""),
        ],
    )
    second = _junit(
        tmp_path / "2.xml", [("a.P", "flaky", None, ""), ("a.P", "broken", "failure", "y")]
    )
    results = ds.read_attempts(f"{first};{second}")
    assert results == {
        "a.P::flaky": {"outcome": "passed", "message": ""},
        "a.P::broken": {"outcome": "failed", "message": "y"},
        "a.P::ok": {"outcome": "passed", "message": ""},
    }


def test_a_message_is_its_first_line_and_bounded(tmp_path):
    run = [("a.P", "t", "failure", "x" * 500)]
    results = ds.read_results(str(_junit(tmp_path / "r.xml", run)))
    assert len(results["a.P::t"]["message"]) == ds.MESSAGE_CHARS


# --- the list ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "refused"),
    [
        ({"driver": "psycopg"}, "names driver"),
        ({"version": ""}, "names no version"),
        ({"reasons": {"writes": {"decision": "x"}}}, "names no requirement"),
        ({"reasons": {"writes": {"requirement": "REQ-1"}}}, "states no decision"),
        (
            {"reasons": {"writes": {"requirement": "REQ-1", "decision": "see REQ-2"}}},
            "names a requirement in its decision",
        ),
        (
            {"reasons": {"writes": {"requirement": "REQ-1", "decision": "d", "defect": "x"}}},
            "both a decision and a defect",
        ),
        ({"reasons": {"writes": {"defect": " "}}}, "says nothing"),
    ],
)
def test_a_reason_is_a_decision_or_a_defect_and_nothing_else(tmp_path, change, refused):
    body = {**_LIST, "expected": {"a.T::t": "writes"}, **change}
    if "reasons" in change:
        body["reasons"] = change["reasons"]
    with pytest.raises(ValueError, match=refused):
        ds.load("pgjdbc", _list(tmp_path, body))


def test_every_listed_test_names_a_declared_reason_and_every_reason_is_used(tmp_path):
    body = {**_LIST, "expected": {**_LIST["expected"], "a.T::t": "nowhere"}}
    with pytest.raises(ValueError, match="reasons not declared: \\['nowhere'\\]"):
        ds.load("pgjdbc", _list(tmp_path, body))
    body = {**_LIST, "reasons": {**_LIST["reasons"], "spare": {"defect": "x"}}}
    with pytest.raises(ValueError, match="reasons no test names: \\['spare'\\]"):
        ds.load("pgjdbc", _list(tmp_path, body))


# --- the report and the page ------------------------------------------------------------------------


def test_the_summary_gives_the_four_counts_and_what_the_gate_failed_on(tmp_path):
    text = ds.section(_doc(tmp_path), requirement=True)
    assert "**Results:** 3 passed, 2 skipped by design, 2 failed, 2 not tested, of 9 tests." in text
    assert "| writes | 2 | REQ-2203 | Tables are the model's. |" in text
    assert "| hold-cursor | 1 | WITH HOLD cursors are refused. |" in text
    assert "- failed, not listed: a.New::testBroken: unexpected EOF" in text
    assert "- passed, listed as hold-cursor: a.Fixed::testNowPasses" in text
    assert "- listed, not run: a.Gone::testRemoved" in text


def test_the_published_section_names_no_requirement(tmp_path):
    text = ds.section(_doc(tmp_path))
    assert not re.search(r"REQ-\d", text)
    assert "### Unexpected" not in text


def test_the_page_is_filled_between_the_drivers_markers_only(tmp_path):
    doc = _doc(tmp_path)
    page = (
        "# P\n\n<!-- driver-results:pgjdbc:begin -->\nSTALE\n<!-- driver-results:pgjdbc:end -->\n"
        "<!-- driver-results:psycopg:begin -->\nkept\n<!-- driver-results:psycopg:end -->\n"
    )
    filled = ds.fill(page, doc)
    assert "STALE" not in filled and "kept" in filled
    assert "**Results:** 3 passed" in filled
    assert ds.fill(filled, doc) == filled
    with pytest.raises(ValueError, match="no psycopg markers"):
        ds.fill("# P\n", {**doc, "driver": "psycopg"})


def test_main_writes_the_report_and_its_exit_status_is_the_gate(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ds, "LISTS", _list(tmp_path, _LIST))
    junit = _junit(tmp_path / "run.xml", _RUN)
    out_json, out_md = tmp_path / "r.json", tmp_path / "r.md"
    argv = ["summarise", "pgjdbc", "REL42.7.14", str(junit), str(out_json), str(out_md), "s"]
    assert ds.main(argv) == 1
    doc = json.loads(out_json.read_text())
    assert doc["surface"] == "driver-pgjdbc" and doc["passed"] == 3
    out = capsys.readouterr().out
    assert (
        "CONFORMANCE-COUNTS surface=driver-pgjdbc passed=3 by_design=2 failed=2 not_tested=2 "
        "total=9" in out
    )
    assert "DRIVER-UNEXPECTED-FAILURE a.New::testBroken" in out
    assert "DRIVER-UNEXPECTED-PASS a.Fixed::testNowPasses" in out
    page = tmp_path / "drivers.md"
    page.write_text("<!-- driver-results:pgjdbc:begin -->\n<!-- driver-results:pgjdbc:end -->\n")
    monkeypatch.setattr(ds, "PAGE", page)
    assert ds.main(["fill-docs", str(out_json)]) == 0
    assert "## pgjdbc" in page.read_text()


# --- seeding a list from a run ----------------------------------------------------------------------


def test_a_seeded_list_names_every_failure_and_keeps_the_reasons_it_had(tmp_path):
    lists = _list(tmp_path, _LIST)
    run = [
        *_RUN,
        ("a.New", "testBrokenToo", "failure", "unexpected EOF&#10;at line 9"),
        ("a.Odd", "testOther", "error", "KeyError: 'x'"),
    ]
    results = ds.read_results(str(_junit(tmp_path / "run.xml", run)))
    ds.seed("pgjdbc", "REL42.7.14", results, lists)
    seeded = yaml.safe_load((lists / "pgjdbc.yaml").read_text())
    expected = ds.load("pgjdbc", lists)
    # A failure the list named keeps its reason, a decision above all.
    assert expected.tests["a.WriteTest::testCreate"] == "writes"
    assert expected.tests["a.CursorTest::testHold"] == "hold-cursor"
    assert expected.reasons["writes"].requirement == "REQ-2203"
    # A failure it did not name is an open defect, one per error the suite saw, never a decision.
    new = {expected.tests["a.New::testBroken"], expected.tests["a.New::testBrokenToo"]}
    assert len(new) == 1 and expected.reasons[new.pop()].defect == "unexpected EOF"
    assert not expected.reasons[expected.tests["a.Odd::testOther"]].by_design
    # A test that passed, was skipped or did not run is no longer listed.
    for gone in ("a.Fixed::testNowPasses", "a.Skips::testSkipped", "a.Gone::testRemoved"):
        assert gone not in seeded["expected"]
    doc = ds.summarise(expected, "REL42.7.14", results, "s")
    assert not doc["unexpected_failures"] and not doc["unexpected_passes"]
    # Seeding moves nothing out of failed: every new entry is a defect.
    assert doc["failed"] == 4 and doc["by_design"] == 2


def test_seed_is_a_command_that_writes_the_drivers_list(tmp_path, monkeypatch):
    monkeypatch.setattr(ds, "LISTS", _list(tmp_path, {**_LIST, "version": "REL42.7.13"}))
    junit = _junit(tmp_path / "run.xml", _RUN)
    assert ds.main(["seed", "pgjdbc", "REL42.7.14", str(junit)]) == 0
    text = (tmp_path / "pgjdbc.yaml").read_text()
    assert text.startswith("# pgjdbc's expected-failure list")
    assert ds.load("pgjdbc", tmp_path).version == "REL42.7.14"


# --- the repository's lists, page and workflow ------------------------------------------------------

DRIVERS = sorted(p.stem for p in (CONFORMANCE / "drivers").glob("*.yaml"))


def test_the_first_drivers_are_pgjdbc_and_psycopg():
    assert DRIVERS == ["pgjdbc", "psycopg"]


@pytest.mark.parametrize("driver", DRIVERS)
def test_each_drivers_list_loads_and_its_page_section_exists(driver):
    expected = ds.load(driver)
    assert expected.driver == driver
    begin, end = ds._markers(driver)
    page = ds.PAGE.read_text()
    assert begin in page and end in page


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def test_the_workflow_is_dispatched_by_hand_and_carries_no_secret():
    doc = _workflow()
    assert "workflow_dispatch" in doc[True]  # YAML reads the key `on` as true
    assert doc["permissions"] == {"contents": "read"}
    assert not re.search(r"\bsecrets\.[A-Za-z_]", WORKFLOW.read_text())


@pytest.mark.parametrize("driver", DRIVERS)
def test_each_driver_is_a_job_at_its_lists_version_fetched_checksum_verified(driver):
    job = _workflow()["jobs"][driver]
    version = next(v for k, v in job["env"].items() if k.endswith("_VERSION") and "PYTEST" not in k)
    assert str(version) == ds.load(driver).version
    assert re.fullmatch(r"[0-9a-f]{64}", job["env"][f"{driver.upper()}_SHA256"])
    run = "\n".join(s.get("run") or "" for s in job["steps"])
    fetched = run.index("sha256sum -c")
    assert fetched < run.index("tar -xzf"), "the tarball is unpacked only once its checksum holds"
    uses = [s.get("uses") for s in job["steps"]]
    assert "./.github/actions/conformance-instance" in uses
    summarise = next(s for s in job["steps"] if s.get("name") == "Summarise")
    assert f"driver_suite.py summarise {driver} " in summarise["run"]
    assert 'exit "${ran:-0}"' in summarise["run"]
    # The suite's own run is data; the summary's exit status is the result.
    suite = next(s for s in job["steps"] if str(s.get("name", "")).startswith("Run "))
    assert suite["continue-on-error"] is True
