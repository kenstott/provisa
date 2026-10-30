# Copyright (c) 2026 Kenneth Stott
# Canary: 0d7a5e64-1c3f-4b28-b6e9-92f4c8a3d716
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The surface map (scripts/ci/surfaces.toml), what it plans, and the failure summary.

Files are read, never collected: collecting tests/integration starts the test stack."""

from __future__ import annotations

import importlib.util
import json
import sys
from fnmatch import fnmatch
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
CI = REPO / "scripts" / "ci"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, CI / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


surface_map = _load("surface_map")
verify_summary = _load("verify_summary")
DATA = surface_map.load()
SURFACES = surface_map.surfaces(DATA)
FILES = surface_map.integration_files()
LIVE_ONLY = "tests/integration/test_snowflake_source_e2e.py"


def _repo_files() -> list[str]:
    roots = ("provisa", "provisa-ui/src", "tests/integration", ".github", "scripts/ci")
    return [
        str(p.relative_to(REPO))
        for root in roots
        for p in (REPO / root).rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and "node_modules" not in p.parts
    ]


REPO_FILES = _repo_files()


# --- the map against the repository ---------------------------------------------------------------


def test_the_table_of_surfaces_is_all_there():
    assert {
        "control-plane", "compiler", "engines", "semantic", "ui", "delivery", "cypher-bolt",
        "pgwire", "flight-sql", "graphql", "sparql", "grpc", "mcp", "rest",
    } <= set(SURFACES)  # fmt: skip
    assert {n for n in SURFACES if n.startswith("source-")} == {
        f"source-{n}" for n in DATA["sources"]
    }
    assert len(DATA["sources"]) >= 40


def test_every_integration_file_is_mapped_or_falls_to_the_control_plane():
    """Every file belongs to exactly one surface; what no surface names is the control plane's."""
    tests = {n: s.tests for n, s in SURFACES.items()}
    assert len(FILES) > 400
    for path in FILES:
        named = surface_map.owners(path, tests)
        assert len(named) <= 1, f"{path} is on {named}"
        ran_by = [n for n in SURFACES if path in surface_map.files_of(n, SURFACES, [path])]
        assert ran_by == (named or [surface_map.DEFAULT]), path


def test_an_unmapped_file_really_runs_with_the_control_plane():
    """The default surface runs whole lanes: its own, and whatever lane any file it took needs."""
    planned = surface_map.plan(DATA, "control-plane", full=False, changed=[])
    assert {"core", "app"} <= set(planned.lanes)
    # A file in any other lane is mapped to its surface instead, or the default surface is the suite.
    assert planned.lanes == ("core", "app")
    for path in surface_map.files_of("control-plane", SURFACES, FILES):
        for test, marker_sets in surface_map.run_lane.test_markers(path).items():
            for marks in marker_sets:
                lanes = surface_map._lanes_selecting(path, marks)
                if lanes:
                    assert set(lanes) & set(planned.lanes), test
                else:
                    assert test in planned.not_run, test


def test_every_test_pattern_and_contract_file_names_something_that_exists():
    for surface in SURFACES.values():
        for pattern in surface.tests:
            assert any(fnmatch(f, pattern) for f in FILES), f"{surface.name}: {pattern}"
    for path in surface_map.contract_paths(DATA):
        assert (REPO / path).is_file(), path


def test_every_path_pattern_under_the_product_names_something_that_exists():
    for surface in SURFACES.values():
        for pattern in surface.paths:
            if pattern.startswith(("provisa/", "provisa-ui/")):
                assert any(fnmatch(f, pattern) for f in REPO_FILES), f"{surface.name}: {pattern}"


def test_no_file_of_the_repository_is_on_two_surfaces():
    code = {n: s.paths for n, s in SURFACES.items()}
    for path in REPO_FILES:
        assert len(surface_map.owners(path, code)) <= 1, path


def test_a_surface_names_only_suites_and_lanes_the_workflow_runs():
    known = set(surface_map.CONFORMANCE) | {"ui-unit", "ui-e2e-core", "ui-swap"}
    for surface in SURFACES.values():
        assert set(surface.suites) <= known, surface.name
        assert set(surface.lanes) <= set(surface_map.LANES), surface.name
        if surface.wider is not None:
            assert surface.wider in SURFACES, surface.name


# --- what a surface's run is ----------------------------------------------------------------------


def test_a_protocol_runs_its_conformance_suite_and_its_own_files_by_name():
    planned = surface_map.plan(DATA, "cypher-bolt", full=False, changed=[])
    assert planned.suites == ("conformance-cypher", "conformance-bolt")
    assert not planned.lanes and not planned.full
    assert "tests/integration/test_bolt_server.py" in planned.tests
    assert all("pgwire" not in t for t in planned.tests)


def test_a_test_only_a_live_lane_selects_is_left_out_of_the_named_tests():
    """integration-suite-lanes.yml refuses a targeted run naming one (run_lane.py routes)."""
    planned = surface_map.plan(DATA, "source-snowflake", full=False, changed=[])
    assert not any(t.startswith(LIVE_ONLY) for t in planned.tests)
    assert any(t.startswith(LIVE_ONLY) for t in planned.not_run)
    assert not planned.integration


@pytest.mark.parametrize("name", sorted(SURFACES))
def test_the_lanes_workflow_would_accept_each_surfaces_named_tests(name, monkeypatch):
    """The same refusal the plan job of integration-suite-lanes.yml applies, on what this plans."""
    planned = surface_map.plan(DATA, name, full=False, changed=[])
    run_lane = surface_map.run_lane
    monkeypatch.setenv("PROVISA_LANE_TESTS", " ".join(planned.tests))
    monkeypatch.setenv("PROVISA_INCLUDE_LIVE", ",,,")
    routed = run_lane.routes(run_lane.runnable_lanes())
    assert not set(routed) - set(surface_map.LANES), name
    assert not set(routed) & set(run_lane.LIVE), name


def test_no_plan_reaches_a_live_lane():
    for name in SURFACES:
        for full in (False, True):
            planned = surface_map.plan(DATA, name, full=full, changed=[])
            assert not set(planned.lanes) & {"warehouse", *surface_map.run_lane.LIVE}, name


def test_full_runs_every_conformance_suite_and_every_lane():
    planned = surface_map.plan(DATA, "ui", full=True, changed=[])
    assert set(surface_map.CONFORMANCE) <= set(planned.suites)
    assert {"ui-unit", "ui-e2e-core"} <= set(planned.suites)
    assert planned.lanes == ("all",) and planned.tests == ()


@pytest.mark.parametrize("path", surface_map.contract_paths(DATA))
def test_a_contract_file_changed_widens_the_run_unasked(path):
    planned = surface_map.plan(DATA, "rest", full=False, changed=["provisa/core/x.py", path])
    assert planned.full and planned.widened_by == (path,)
    assert planned.lanes == ("all",)


def test_any_other_change_does_not_widen():
    planned = surface_map.plan(DATA, "rest", full=False, changed=["provisa/federation/delta.py"])
    assert not planned.full and not planned.widened_by


def test_a_surface_that_does_not_exist_is_refused():
    with pytest.raises(ValueError, match="no such surface: nope"):
        surface_map.plan(DATA, "nope", full=False, changed=[])


def test_the_plan_is_written_as_github_outputs():
    planned = surface_map.plan(DATA, "pgwire", full=False, changed=[])
    lines = dict(line.split("=", 1) for line in surface_map.outputs(planned).splitlines())
    assert lines["suites"] == ",conformance-pgwire,"
    assert lines["integration"] == "true" and lines["full"] == "false"
    assert lines["lanes"] == ""


@pytest.mark.parametrize(
    ("paths", "expected"),
    [
        (["provisa/compiler/x.py"], "compiler"),
        (["provisa/cassandra/x.py", "tests/unit/test_x.py"], "source-cassandra"),
        (["provisa/cassandra/x.py", "provisa/mongodb/y.py"], "engines"),
        (["provisa/bolt/x.py", "provisa/pgwire/y.py"], "compiler"),
        (["provisa/bolt/x.py", "provisa/cassandra/y.py"], "control-plane"),
        (["provisa/compiler/x.py", "provisa/federation/y.py"], "control-plane"),
        (["provisa/core/x.py"], "control-plane"),
        (["provisa/core/glossary.py"], "semantic"),
        ([".github/workflows/lint.yml"], "delivery"),
        (["tests/integration/test_bolt_server.py"], "cypher-bolt"),
        (["tests/unit/test_x.py"], "delivery"),
    ],
)
def test_paths_take_their_surface_and_the_nearest_wider_one_across_several(paths, expected):
    assert surface_map.surface_of(DATA, paths) == expected


# --- the summary ----------------------------------------------------------------------------------


def _junit(path: Path, *cases: str) -> None:
    path.parent.mkdir(parents=True)
    path.write_text(f"<testsuites><testsuite>{''.join(cases)}</testsuite></testsuites>")


def test_the_summary_names_every_failing_test_per_suite(tmp_path):
    fail = '<testcase classname="{c}" name="{n}"><failure message="boom"/></testcase>'
    good = '<testcase classname="t.ok" name="fine"/>'
    _junit(
        tmp_path / "lane-core-1of6" / "core-1of6" / "junit.xml",
        good,
        fail.format(c="t.core", n="breaks"),
    )
    _junit(tmp_path / "unit-junit-2" / "unit-junit.xml", fail.format(c="t.unit", n="wrong"))
    for name, doc in (
        ("cypher", {"failures": [{"feature": "Match1", "scenario": "one", "reason": "no"}]}),
        (
            "pgwire",
            {"failed": 1, "pgspec": {"failures": [{"feature": "Auth", "scenario": "scram"}]}},
        ),
    ):
        (tmp_path / f"conformance-{name}").mkdir()
        (tmp_path / f"conformance-{name}" / f"conformance-{name}.json").write_text(json.dumps(doc))
    (tmp_path / "vitest-results-1").mkdir()
    (tmp_path / "vitest-results-1" / "vitest-results.json").write_text(
        json.dumps(
            {
                "testResults": [
                    {
                        "name": "a.test.tsx",
                        "status": "failed",
                        "assertionResults": [
                            {"status": "passed", "fullName": "fine"},
                            {"status": "failed", "fullName": "renders"},
                        ],
                    },
                    {"name": "b.test.tsx", "status": "failed", "message": "cannot load",
                     "assertionResults": []},
                ]
            }
        )
    )  # fmt: skip
    (tmp_path / "playwright-report-core").mkdir()
    (tmp_path / "playwright-report-core" / "playwright-results.json").write_text(
        json.dumps(
            {
                "suites": [
                    {
                        "title": "a.spec.ts",
                        "specs": [{"title": "passes", "ok": True}],
                        "suites": [{"title": "group", "specs": [{"title": "fails", "ok": False}]}],
                    }
                ]
            }
        )
    )
    (tmp_path / "server-logs-core").mkdir()

    text = verify_summary.report(
        tmp_path,
        {"plan": {"result": "success"}, "lint": {"result": "failure"}, "ui": {"result": "skipped"}},
        "https://example.test/run",
    )

    assert "**7 failing test(s)**" in text
    assert "### lane-core-1of6: 1" in text and "FAILURE t.core::breaks: boom" in text
    assert "### unit-junit-2: 1" in text and "FAILURE t.unit::wrong" in text
    assert "### conformance-cypher: 1" in text and "FAILED Match1 :: one: no" in text
    assert "### conformance-pgwire: 1" in text and "FAILED Auth :: scram" in text
    assert "FAILED a.test.tsx :: renders" in text and "FAILED b.test.tsx: cannot load" in text
    assert "FAILED a.spec.ts › group › fails" in text
    assert "fine" not in text and "passes" not in text
    # A job that failed naming no test is still listed; a skipped one is not called failed.
    assert text.split("### Jobs that did not pass")[1].splitlines()[1] == "- lint"
    assert "https://example.test/run" in text


def test_the_summary_of_a_clean_run_says_so(tmp_path):
    text = verify_summary.report(tmp_path, {"lint": {"result": "success"}})
    assert "No failing test." in text and "did not pass" not in text
