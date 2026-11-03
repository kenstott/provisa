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
boot, a reason for every other kind, the live warehouses only in the warehouse lane and on that
lane's own secrets and gating, and why a leg that left no result failed."""

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

import warehouse_leg  # noqa: E402

DOC = legs.load()
WAREHOUSES = {
    "snowflake",
    "bigquery",
    "databricks",
    "fabric",
    "synapse",
    "redshift",
    "motherduck",
    "singlestore",
}
WORKFLOWS = REPO / ".github" / "workflows"


def _leg(engine: str) -> dict:
    return next(leg for leg in DOC["legs"] if leg["engine"] == engine)


def _workflow(name: str = "conformance-functions.yml") -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def _warehouse_lane() -> dict:
    """The integration suite's warehouse job, whose secrets and gating the warehouse legs reuse."""
    return _workflow("integration-suite-lanes.yml")["jobs"]["warehouse"]


def _lane_secrets() -> set[str]:
    """Every secret the warehouse lane's job reads, in its env or a step's."""
    job = _warehouse_lane()
    envs = [job["env"], *(step.get("env", {}) for step in job["steps"])]
    return {
        name for env in envs for name, value in env.items() if value == f"${{{{ secrets.{name} }}}}"
    }


def test_every_engine_kind_is_a_leg_or_says_why_it_is_not_run():
    assert legs.uncovered(ENGINE_KINDS, DOC) == {"missing": [], "unknown": []}
    assert all(why.strip() for why in DOC["not_run"].values())


def test_an_engine_kind_neither_run_nor_explained_is_found():
    found = legs.uncovered(ENGINE_KINDS | {"newdb"}, DOC)
    assert found == {"missing": ["newdb"], "unknown": []}


def test_the_four_engines_measured_before_are_still_free_legs():
    for engine in ("duckdb", "pg", "clickhouse", "trino"):
        assert _leg(engine)["lane"] == "free"


def test_the_warehouses_are_the_warehouse_lane_and_nothing_else_is():
    lane = {leg["engine"] for leg in DOC["legs"] if leg["lane"] == "warehouse"}
    assert lane == WAREHOUSES
    for engine in lane:
        assert "service" not in _leg(engine) and "demo-source" not in _leg(engine)
        assert set(warehouse_leg.UP) == set(warehouse_leg.DOWN) == lane


def test_a_warehouse_leg_reads_only_secrets_the_warehouse_lane_already_reads():
    lane = _lane_secrets()
    for engine in WAREHOUSES:
        names = _leg(engine)["secrets"]
        assert names and set(names) <= lane, (engine, sorted(set(names) - lane))
        assert not any(name.startswith("FUNCTIONS_") for name in names)


def test_singlestore_is_the_hosted_instance_not_a_container():
    assert _leg("singlestore")["secrets"] == [
        "SINGLESTORE_HOST",
        "SINGLESTORE_PORT",
        "SINGLESTORE_USERNAME",
        "SINGLESTORE_PASSWORD",
        "SINGLESTORE_DATABASE",
    ]
    services = yaml.safe_load((REPO / "docker-compose.test.yml").read_text())["services"]
    assert "singlestore" not in services


def test_a_leg_that_wakes_shared_compute_holds_the_warehouse_lanes_group():
    group = _warehouse_lane()["concurrency"]
    assert group == {"group": "live-cloud-warehouses", "queue": "max"}
    shared = {leg["engine"] for leg in DOC["legs"] if "concurrency" in leg}
    assert shared == {"snowflake", "databricks", "fabric"}
    assert all(_leg(engine)["concurrency"] == group["group"] for engine in shared)
    probe = _workflow()["jobs"]["probe"]
    assert probe["concurrency"]["group"].startswith("${{ matrix.concurrency || ")
    assert probe["concurrency"]["queue"] == "max"


def test_the_warehouse_legs_are_planned_only_by_a_dispatch_as_the_warehouse_lane_is():
    triggers = _workflow()[True]
    assert set(triggers) == {"workflow_dispatch"}
    assert "workflow_dispatch" in _warehouse_lane()["if"]


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
        if "service" in leg or "demo-source" in leg:
            assert leg["engine-url"], leg["engine"]


def test_a_demo_source_leg_starts_the_container_its_demo_source_defines():
    for leg in DOC["legs"]:
        if "demo-source" in leg:
            compose = REPO / "demo" / "sources" / leg["demo-source"] / "compose.yml"
            services = yaml.safe_load(compose.read_text())["services"]
            assert [s["image"] for s in services.values()] == [leg["image"]], leg["engine"]


def test_hana_reuses_the_swap_lanes_container_and_dialect():
    hana = _leg("saphana")
    assert hana["lane"] == "free" and hana["demo-source"] == "saphana"
    swap = _workflow("ui-e2e-swap-amd64.yml")["jobs"]["playwright"]["with"]
    assert "saphana" in swap["python-extras"].split()
    assert "saphana" in hana["extras"].split()
    assert "saphana" not in DOC["not_run"]


def test_the_free_plan_is_every_free_leg_in_file_order_and_no_paid_one():
    planned = legs.plan("free", DOC)
    free = [leg["engine"] for leg in DOC["legs"] if leg["lane"] == "free"]
    assert [e["engine"] for e in planned["matrix"]["include"]] == free
    assert planned["engines"] == ",".join(free)
    assert all(e["secrets"] == "" for e in planned["matrix"]["include"])


def test_a_warehouse_leg_is_planned_only_when_the_dispatch_names_its_lane():
    lane = legs.plan("warehouse", DOC)["matrix"]["include"]
    assert {e["engine"] for e in lane} == WAREHOUSES
    assert all(e["secrets"].split() == _leg(e["engine"])["secrets"] for e in lane)
    every = legs.plan("all", DOC)["engines"].split(",")
    assert set(every) == {leg["engine"] for leg in DOC["legs"]}


def test_a_plan_entry_carries_every_field_the_workflow_reads():
    entry = legs.plan("free", DOC)["matrix"]["include"][0]
    assert set(entry) == set(legs.FIELDS) - set(legs.URLS)
    assert _leg("trino")["boot-attempts"] == "300"
    trino = next(e for e in legs.plan("free", DOC)["matrix"]["include"] if e["engine"] == "trino")
    assert trino["boot-attempts"] == "300" and trino["wait"] == "600"
    duckdb = next(e for e in legs.plan("free", DOC)["matrix"]["include"] if e["engine"] == "duckdb")
    assert duckdb["boot-attempts"] == "90" and duckdb["odbc"] is False
    assert duckdb["extras"] == "dev"


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
        ({"legs": [{"engine": "a", "lane": "warehouse"}], "not_run": {}}, "names its secrets"),
        (
            {"legs": [{"engine": "a", "lane": "free", "secrets": ["X"]}], "not_run": {}},
            "names its secrets",
        ),
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
    assert service["if"] == "matrix.service != '' || matrix.demo-source != ''"
    assert 'engine_service.sh up "$SERVICE" "$WAIT"' in service["run"]
    assert 'engine_service.sh demo "$DEMO_SOURCE"' in service["run"]
    assert names.index("Install the engine's client") < names.index("Start the engine's server")
    assert names.index("Start the engine's server") < names.index("Start the Provisa instance")


def test_the_workflow_gives_the_warehouse_step_exactly_the_legs_secrets():
    steps = _workflow()["jobs"]["probe"]["steps"]
    names = [s.get("name") for s in steps]
    up = steps[names.index("Bring up the warehouse")]
    assert up["if"] == "matrix.lane == 'warehouse'"
    wanted = {name for engine in WAREHOUSES for name in _leg(engine)["secrets"]}
    assert up["env"] == {name: f"${{{{ secrets.{name} }}}}" for name in wanted}
    assert "warehouse_leg.py up" in up["run"] and '--env-file "$GITHUB_ENV"' in up["run"]
    assert names.index("Bring up the warehouse") < names.index("Start the Provisa instance")


def test_the_workflow_leaves_the_warehouse_stopped_after_the_instance_whatever_happened():
    steps = _workflow()["jobs"]["probe"]["steps"]
    names = [s.get("name") for s in steps]
    down = steps[names.index("Leave the warehouse stopped")]
    assert down["if"] == "${{ always() && matrix.lane == 'warehouse' }}"
    assert "warehouse_leg.py down" in down["run"] and "exit 1" in down["run"]
    assert names.index("Stop the Provisa instance") < names.index("Leave the warehouse stopped")


def test_a_warehouse_leg_missing_a_secret_fails_by_name_and_wakes_nothing(tmp_path):
    env_file, state = tmp_path / "github-env", tmp_path / "state"
    given = {"SINGLESTORE_HOST": "h", "SINGLESTORE_PORT": "3333"}
    with pytest.raises(SystemExit, match="not set: SINGLESTORE_USERNAME SINGLESTORE_PASSWORD"):
        warehouse_leg.up("singlestore", given, env_file, state)
    assert not env_file.exists()
    warehouse_leg.down("singlestore", given, state)  # nothing was brought up: nothing to stop


def _read_env(env_file: Path) -> dict[str, str]:
    lines = env_file.read_text().splitlines()
    found, i = {}, 0
    while i < len(lines):
        name, delimiter = lines[i].split("<<", 1)
        end = lines.index(delimiter, i + 1)
        found[name] = "\n".join(lines[i + 1 : end])
        i = end + 1
    return found


def test_a_warehouse_leg_gives_the_instance_its_secrets_and_engine_url(tmp_path, capsys):
    env_file, state = tmp_path / "github-env", tmp_path / "state"
    given = {
        "SINGLESTORE_HOST": "svc.singlestore.com",
        "SINGLESTORE_PORT": "3333",
        "SINGLESTORE_USERNAME": "admin",
        "SINGLESTORE_PASSWORD": "p@ss/word",
        "SINGLESTORE_DATABASE": "provisa",
    }
    warehouse_leg.up("singlestore", given, env_file, state)
    url = "singlestoredb://admin:p%40ss%2Fword@svc.singlestore.com:3333/provisa"
    assert _read_env(env_file) == {**given, "PROVISA_ENGINE_URL": url}
    assert f"::add-mask::{url}" in capsys.readouterr().out


def test_the_bigquery_key_is_a_file_and_not_given_to_the_instance(tmp_path):
    env_file, state = tmp_path / "github-env", tmp_path / "state"
    key = '{\n "type": "service_account"\n}'
    given = {"GOOGLE_CLOUD_PROJECT": "proj", "GOOGLE_APPLICATION_CREDENTIALS_JSON": key}
    warehouse_leg.up("bigquery", given, env_file, state)
    found = _read_env(env_file)
    assert found == {
        "GOOGLE_CLOUD_PROJECT": "proj",
        "GOOGLE_APPLICATION_CREDENTIALS": str(state / "gcp-key.json"),
        "PROVISA_ENGINE_URL": "bigquery://proj",
    }
    assert (state / "gcp-key.json").read_text() == key


def test_down_leaves_stopped_what_up_reached_even_when_the_wake_failed(tmp_path, monkeypatch):
    state, calls = tmp_path / "state", []

    def wake(env, state):
        raise RuntimeError("the warehouse did not start")

    monkeypatch.setitem(warehouse_leg.UP, "databricks", wake)
    monkeypatch.setitem(warehouse_leg.DOWN, "databricks", lambda env, state: calls.append(env))
    given = {
        "DATABRICKS_SERVER_HOSTNAME": "h",
        "DATABRICKS_HTTP_PATH": "p",
        "DATABRICKS_TOKEN": "t",
    }
    with pytest.raises(RuntimeError):
        warehouse_leg.up("databricks", given, tmp_path / "github-env", state)
    warehouse_leg.down("databricks", given, state)
    assert calls == [given]


def test_the_probe_job_reads_its_matrix_from_the_plan_output():
    workflow = _workflow()
    plan_job, probe = workflow["jobs"]["plan"], workflow["jobs"]["probe"]
    assert plan_job["outputs"]["matrix"] == "${{ steps.plan.outputs.matrix }}"
    assert "plan" in (probe["needs"] if isinstance(probe["needs"], list) else [probe["needs"]])
    assert "if" not in probe
    assert probe["strategy"]["matrix"] == "${{ fromJSON(needs.plan.outputs.matrix) }}"


@pytest.mark.parametrize("lanes", sorted(legs.LANES))
def test_no_plan_output_holds_a_url_the_runner_would_mask_and_drop(lanes):
    # The runner masks a URL's credentials and skips a job output holding a masked value
    # ("Skip output 'matrix' since it may contain secret"): the probe job then has no matrix.
    planned = legs.plan(lanes, DOC)
    assert "://" not in json.dumps(planned["matrix"]) and "://" not in planned["engines"]
    assert not set(legs.URLS) & set(planned["matrix"]["include"][0])


def test_the_probe_job_reads_its_urls_from_the_legs_file_not_the_matrix():
    text = (WORKFLOWS / "conformance-functions.yml").read_text()
    for field in legs.URLS:
        assert f"matrix.{field}" not in text
    steps = _workflow()["jobs"]["probe"]["steps"]
    names = [s.get("name") for s in steps]
    urls = steps[names.index("The leg's URLs")]
    assert 'function_legs.py urls --engine "$PROVISA_ENGINE"' in urls["run"]
    assert '>> "$GITHUB_ENV"' in urls["run"]
    assert names.index("Set up Python") < names.index("The leg's URLs")
    assert names.index("The leg's URLs") < names.index("Start the engine's server")


def test_the_urls_command_writes_a_legs_urls_as_environment_lines(capsys):
    assert legs.main(["function_legs.py", "urls", "--engine", "exasol"]) == 0
    out = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    exasol = _leg("exasol")
    assert out == {
        "PROVISA_ENGINE_URL": exasol["engine-url"],
        "LEG_SETUP_URL": exasol["setup-url"],
        "LEG_CONTROL_PLANE_URL": "",
    }
    assert legs.urls("trino", DOC)["LEG_CONTROL_PLANE_URL"] == _leg("trino")["control-plane-url"]
    with pytest.raises(ValueError, match="no leg"):
        legs.urls("nosuch", DOC)
