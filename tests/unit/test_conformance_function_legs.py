# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41f2b6-7e08-4a53-b8c1-6f2e05da7c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The legs of Conformance (functions) (REQ-2167, REQ-2171): one per engine kind a hosted runner can
boot, a reason for every other kind, the paid warehouses only in their own lane, and why a leg that
left no result failed."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

from provisa.core.engine_kinds import ENGINE_KINDS

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import function_legs as legs  # noqa: E402

DOC = legs.load()
WAREHOUSES = {"snowflake", "bigquery", "databricks", "fabric", "synapse", "redshift", "motherduck"}


def _leg(engine: str) -> dict:
    return next(leg for leg in DOC["legs"] if leg["engine"] == engine)


def test_every_engine_kind_is_a_leg_or_says_why_it_is_not_run():
    assert legs.uncovered(ENGINE_KINDS, DOC) == {"missing": [], "unknown": []}
    assert all(why.strip() for why in DOC["not_run"].values())


def test_an_engine_kind_neither_run_nor_explained_is_found():
    found = legs.uncovered(ENGINE_KINDS | {"newdb"}, DOC)
    assert found == {"missing": ["newdb"], "unknown": []}


def test_the_four_engines_measured_before_are_still_free_legs():
    for engine in ("duckdb", "pg", "clickhouse", "trino"):
        assert _leg(engine)["lane"] == "free"


def test_the_warehouses_are_the_paid_lane_and_nothing_else_is():
    paid = {leg["engine"] for leg in DOC["legs"] if leg["lane"] == "paid"}
    assert paid == WAREHOUSES
    for engine in paid:
        assert _leg(engine)["url-secret"] == f"FUNCTIONS_{engine.upper()}_ENGINE_URL"
        assert "service" not in _leg(engine)


def test_every_leg_names_its_image():
    assert all(leg["image"].strip() for leg in DOC["legs"])


def test_a_free_leg_with_a_server_starts_a_service_the_test_compose_file_defines():
    services = yaml.safe_load((REPO / "docker-compose.test.yml").read_text())["services"]
    for leg in DOC["legs"]:
        if "service" in leg:
            assert leg["lane"] == "free", leg["engine"]
            assert leg["service"] in services, leg["engine"]
            assert services[leg["service"]]["image"] == leg["image"], leg["engine"]


def test_a_free_engine_with_a_server_has_its_url():
    for leg in DOC["legs"]:
        if "service" in leg:
            assert leg["engine-url"], leg["engine"]


def test_the_free_plan_is_every_free_leg_in_file_order_and_no_paid_one():
    planned = legs.plan("free", DOC)
    free = [leg["engine"] for leg in DOC["legs"] if leg["lane"] == "free"]
    assert [e["engine"] for e in planned["matrix"]["include"]] == free
    assert planned["engines"] == ",".join(free)
    assert all(e["environment"] == "" for e in planned["matrix"]["include"])


def test_a_paid_leg_is_planned_only_when_the_dispatch_names_its_lane_and_in_its_environment():
    paid = legs.plan("paid", DOC)["matrix"]["include"]
    assert {e["engine"] for e in paid} == WAREHOUSES
    assert all(e["environment"] == legs.PAID_ENVIRONMENT for e in paid)
    every = legs.plan("all", DOC)["engines"].split(",")
    assert set(every) == {leg["engine"] for leg in DOC["legs"]}


def test_a_plan_entry_carries_every_field_the_workflow_reads():
    entry = legs.plan("free", DOC)["matrix"]["include"][0]
    assert set(entry) == {*legs.FIELDS, "environment"}
    assert _leg("trino")["boot-attempts"] == "300"
    trino = next(e for e in legs.plan("free", DOC)["matrix"]["include"] if e["engine"] == "trino")
    assert trino["boot-attempts"] == "300" and trino["wait"] == "600"
    duckdb = next(e for e in legs.plan("free", DOC)["matrix"]["include"] if e["engine"] == "duckdb")
    assert duckdb["boot-attempts"] == "90" and duckdb["odbc"] is False


def test_an_unknown_lane_is_refused():
    with pytest.raises(ValueError, match="lanes must be one of"):
        legs.plan("cheap", DOC)


def test_the_plan_command_writes_the_outputs_the_workflow_reads(capsys):
    assert legs.main(["function_legs.py", "plan", "--lanes", "free"]) == 0
    out = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    assert json.loads(out["matrix"]) == legs.plan("free", DOC)["matrix"]
    assert out["engines"] == legs.plan("free", DOC)["engines"]


@pytest.mark.parametrize(
    ("doc", "message"),
    [
        (
            {
                "legs": [{"engine": "a", "lane": "free"}, {"engine": "a", "lane": "free"}],
                "not_run": {},
            },
            "more than one leg",
        ),
        ({"legs": [{"engine": "a", "lane": "free"}], "not_run": {"a": "x"}}, "both run and not"),
        ({"legs": [{"engine": "a", "lane": "cheap"}], "not_run": {}}, "unknown lane"),
        ({"legs": [{"engine": "a", "lane": "free", "colour": 1}], "not_run": {}}, "unknown fields"),
    ],
)
def test_a_legs_file_that_contradicts_itself_is_refused(tmp_path, doc, message):
    path = tmp_path / "legs.yaml"
    path.write_text(yaml.safe_dump(doc))
    with pytest.raises(ValueError, match=message):
        legs.load(path)


def test_a_failed_leg_names_the_first_step_that_failed_and_the_end_of_its_logs(tmp_path):
    log = tmp_path / "engine-mysql.log"
    log.write_text("\n".join(f"line {i}" for i in range(100)))
    steps = {
        "client": {"outcome": "success"},
        "service": {"outcome": "failure"},
        "start": {"outcome": "skipped"},
        "measure": {"outcome": "failure"},
    }
    found = legs.failure("mysql", steps, [log, tmp_path / "absent.log"])
    assert found["step"] == "service"
    assert found["reason"] == "mysql: the leg failed starting the engine's server"
    assert found["logs"] == {"engine-mysql.log": [f"line {i}" for i in range(60, 100)]}


def test_a_leg_failed_outside_its_named_steps_still_says_so():
    found = legs.failure("pg", {"start": {"outcome": "success"}}, [])
    assert found["step"] == ""
    assert found["reason"] == "pg: the leg failed a step of the leg's own"


def test_the_failure_command_writes_the_file_the_merge_reads(tmp_path):
    out = tmp_path / "report" / "leg-failure-oracle.json"
    steps = json.dumps({"measure": {"outcome": "failure"}})
    argv = [
        "function_legs.py",
        "failure",
        "--engine",
        "oracle",
        "--steps",
        steps,
        "--out",
        str(out),
    ]
    assert legs.main(argv) == 0
    assert json.loads(out.read_text())["reason"] == "oracle: the leg failed measuring"


def test_wait_returns_once_the_engine_answers(tmp_path):
    legs.wait(f"sqlite:///{tmp_path / 'db.sqlite'}", "SELECT 1", timeout=5)


def test_wait_ends_with_the_last_error_when_the_engine_never_answers(tmp_path):
    url = f"sqlite:///{tmp_path / 'absent' / 'db.sqlite'}"
    with pytest.raises(SystemExit, match="never answered 'SELECT 1' in 0s: OperationalError"):
        legs.wait(url, "SELECT 1", timeout=0.2, interval=0.05)


def test_setup_runs_its_statement_on_the_engine(tmp_path):
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    legs.setup(url, "CREATE TABLE made (x INTEGER)")
    legs.wait(url, "SELECT count(*) FROM made", timeout=5)


def test_the_workflow_records_why_a_leg_failed_before_it_uploads_the_leg():
    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "conformance-functions.yml").read_text()
    )
    steps = workflow["jobs"]["probe"]["steps"]
    names = [s.get("name") for s in steps]
    record = steps[names.index("Record why the leg failed")]
    assert record["if"] == "${{ failure() }}"
    assert "function_legs.py failure" in record["run"]
    assert "leg-failure-$PROVISA_ENGINE.json" in record["run"]
    assert names.index("Record why the leg failed") < names.index("Upload results")
    ids = {s["id"] for s in steps if "id" in s}
    assert {step_id for step_id, _ in legs.STEPS} <= ids


def test_the_workflow_starts_a_leg_server_before_the_instance():
    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "conformance-functions.yml").read_text()
    )
    steps = workflow["jobs"]["probe"]["steps"]
    names = [s.get("name") for s in steps]
    service = steps[names.index("Start the engine's server")]
    assert service["if"] == "matrix.service != ''"
    assert 'engine_service.sh up "$SERVICE" "$WAIT"' in service["run"]
    assert names.index("Install the engine's client") < names.index("Start the engine's server")
    assert names.index("Start the engine's server") < names.index("Start the Provisa instance")
