# Copyright (c) 2026 Kenneth Stott
# Canary: 6eb2c96e-6ab7-4a06-acda-1c5dc33497c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The CI lane runner: its lanes are scripts/test-all's, and its shards partition a lane."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _runner():
    spec = importlib.util.spec_from_file_location(
        "run_lane", REPO / "scripts" / "ci" / "run_lane.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["run_lane"] = module  # dataclasses resolve their module through sys.modules
    spec.loader.exec_module(module)
    return module


def test_the_shards_partition_the_lane():
    run_lane = _runner()
    files = run_lane.test_files(("tests/integration", "tests/steps"))
    assert len(files) > 100
    shards = [run_lane.shard(files, k, 6) for k in range(1, 7)]
    flat = [f for s in shards for f in s]
    assert sorted(flat) == files  # every file once
    assert max(map(len, shards)) - min(map(len, shards)) <= 1


def test_an_out_of_range_shard_is_refused():
    with pytest.raises(ValueError, match="out of range"):
        _runner().shard(["a"], 7, 6)


@pytest.mark.parametrize("lane", ["core", "app", "hive_s3", "kafka", "neo4j", "datastores"])
def test_the_lane_selectors_are_test_alls(lane):
    """scripts/test-all is the local definition of these lanes; CI must select the same tests."""
    test_all = (REPO / "scripts" / "test-all").read_text()
    run_lane = _runner()
    expanded = run_lane.LANES[lane].marker
    selectors = [
        m.replace("$_BASE", run_lane._BASE)
        .replace("$_KAFKA", run_lane._KAFKA)
        .replace("$_DATASTORE", run_lane._DATASTORE)
        .replace("$_HIVE_S3", run_lane._HIVE_S3)
        for m in re.findall(r'-m "([^"]+)"', test_all)
    ]
    assert expanded in selectors


def test_the_base_exclusions_are_test_alls():
    test_all = (REPO / "scripts" / "test-all").read_text()
    run_lane = _runner()
    assert f'_BASE="{run_lane._BASE}"' in test_all
    assert f'_KAFKA="{run_lane._KAFKA}"' in test_all
    assert f'_DATASTORE="{run_lane._DATASTORE}"' in test_all


def test_a_shard_runs_only_its_files():
    run_lane = _runner()
    cmd = run_lane.command("core", "2/6", [])
    files = run_lane.shard(run_lane.test_files(("tests/integration", "tests/steps")), 2, 6)
    assert cmd[3 : 3 + len(files)] == files
    assert "--durations=50" in cmd


def test_all_is_every_suite_lane_with_its_shards():
    include = _runner().matrix("all")["include"]
    assert [m["shard"] for m in include if m["lane"] == "core"] == [f"{k}/6" for k in range(1, 7)]
    assert [m["shard"] for m in include if m["lane"] == "app"] == ["1/3", "2/3", "3/3"]
    assert {"lane": "kafka", "shard": "", "timeout": 90} in include
    assert not [m for m in include if m["lane"] in ("cluster", "warehouse", "salesforce")]


def test_a_selection_runs_only_those_lanes():
    include = _runner().matrix("kafka, app")["include"]
    assert sorted({m["lane"] for m in include}) == ["app", "kafka"]


def test_an_unknown_lane_is_refused():
    with pytest.raises(ValueError, match="no such lane: kafak"):
        _runner().matrix("kafak")


def test_the_workflow_takes_its_matrix_from_the_runner():
    workflow = (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    assert "run_lane.py --matrix" in workflow
    assert "fromJSON(needs.plan.outputs.matrix)" in workflow


@pytest.mark.parametrize("lane", ["core", "app", "e2e", "warehouse", "salesforce"])
def test_every_lane_bounds_each_test(lane):
    """A hung test failed nothing: core 6/6 held its runner until the job was cancelled, and a
    cancelled job's log is gone. Every lane command now bounds each test (pytest-timeout), so a hang
    fails by name with every thread's stack."""
    run_lane = _runner()
    cmd = run_lane.command(lane, None, [])
    assert cmd[cmd.index("--timeout") + 1] == str(run_lane.TEST_TIMEOUT_S)
    assert cmd[cmd.index("--timeout-method") + 1] == "signal"


@pytest.mark.parametrize("lane", ["core", "app", "e2e", "warehouse", "salesforce"])
def test_every_lane_writes_a_long_tests_stacks_to_the_log_before_its_bound(lane):
    """The bound's stacks come in pytest's final report. A lane cancelled by its job limit never
    prints one: the e2e lane hung in seven module teardowns, 15 minutes each, and its log named
    no frame. faulthandler writes the stacks when the phase is still running, ahead of the bound."""
    run_lane = _runner()
    cmd = run_lane.command(lane, None, [])
    setting = cmd[cmd.index("-o", cmd.index("--timeout")) + 1]
    name, _, seconds = setting.partition("=")
    assert name == "faulthandler_timeout"
    assert 0 < int(seconds) < run_lane.TEST_TIMEOUT_S


def _suite_steps() -> list[dict]:
    import yaml

    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    )
    return workflow["jobs"]["suite"]["steps"]


def test_no_suite_step_fetches_the_splunk_cim_add_on():
    """A suite step that fetched it failed every lane whose files merely mention splunk, before any
    test ran, while the Splunkbase secrets were absent (run 37315768931: nine lanes, zero tests).
    tests/conftest.py fetches it when it is about to start splunk, so only a splunk test can fail."""
    runs = [step.get("run", "") for step in _suite_steps()]
    assert not [r for r in runs if "fetch-splunk-cim" in r]


def test_the_lane_gets_the_splunkbase_credentials_for_that_fetch():
    lane = next(step for step in _suite_steps() if step.get("name") == "Run lane")
    assert {"SPLUNKBASE_USERNAME", "SPLUNKBASE_PASSWORD"} <= set(lane["env"])


def test_a_tarball_fetched_inside_the_lane_is_saved_under_the_restored_key():
    """actions/cache's own post-step saves only when the job succeeded; the save is explicit, runs
    whatever the lane's outcome, and names the same path and key the restore does."""
    steps = _suite_steps()
    restore = next(
        s
        for s in steps
        if str(s.get("uses", "")).startswith("actions/cache/restore")
        and "splunk" in s["with"]["path"]
    )
    save = next(
        s
        for s in steps
        if str(s.get("uses", "")).startswith("actions/cache/save") and "splunk" in s["with"]["path"]
    )
    assert (save["with"]["path"], save["with"]["key"]) == (
        restore["with"]["path"],
        restore["with"]["key"],
    )
    assert "always()" in save["if"]
    fetch = (REPO / "scripts" / "fetch-splunk-cim.sh").read_text()
    assert 'CACHE="${PROVISA_SPLUNK_CIM_CACHE:-$HOME/.cache/provisa-splunk-cim}"' in fetch
    assert restore["with"]["path"] == "~/.cache/provisa-splunk-cim"


def _job_steps(job: str) -> list[dict]:
    import yaml

    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    )
    return workflow["jobs"][job]["steps"]


def _plugin_cache_action() -> list[dict]:
    import yaml

    action = REPO / ".github" / "actions" / "trino-plugins" / "action.yml"
    return yaml.safe_load(action.read_text())["runs"]["steps"]


_RESTORE = {"uses": "./.github/actions/trino-plugins", "with": {"phase": "restore"}}
_SAVE = {
    "uses": "./.github/actions/trino-plugins",
    "with": {"phase": "save", "cache-hit": "${{ steps.trino-plugins.outputs.cache-hit }}"},
}


def _plugin_cache_steps(steps: list[dict]) -> tuple[int, int]:
    """Where a job restores and saves the Trino plugins through the one action that does it;
    asserts each call is the action's, unaltered."""
    by_name = {step.get("name"): (i, step) for i, step in enumerate(steps)}
    at_restore, restore = by_name["Restore Trino plugins"]
    at_save, save = by_name["Cache Trino plugins"]
    assert restore["id"] == "trino-plugins"
    assert {k: restore[k] for k in _RESTORE} == _RESTORE
    assert {k: save[k] for k in _SAVE} == _SAVE
    assert "always()" in save["if"]
    return at_restore, at_save


@pytest.mark.parametrize("job", ["suite", "cluster", "warehouse", "salesforce"])
def test_every_collecting_job_caches_the_pinned_trino_plugins_around_its_lane(job):
    """tests/conftest.py fetches the pinned Trino plugin jars from Maven Central at collection, and
    a refused fetch ended the lane before any test ran (run 37573213103: neo4j 403, kafka 404).
    Each job that collects tests restores them by pin before its lane and saves them after --
    through .github/actions/trino-plugins, so no job can carry a copy that has drifted."""
    steps = _job_steps(job)
    lane = [step.get("name") for step in steps].index("Run lane")
    at_restore, at_save = _plugin_cache_steps(steps)
    assert at_restore < lane < at_save


def test_the_plugin_cache_action_restores_and_saves_under_one_key():
    by_name = {step["name"]: step for step in _plugin_cache_action()}
    assert list(by_name) == [
        "Trino plugin pin",
        "Restore Trino plugins",
        "Trino plugins fetched",
        "Cache Trino plugins",
    ]
    restored = by_name["Restore Trino plugins"]["with"]
    assert by_name["Cache Trino plugins"]["with"] == restored
    assert restored["key"] == "trino-plugins-${{ steps.trino-pin.outputs.version }}"
    assert "inputs.phase == 'restore'" in by_name["Restore Trino plugins"]["if"]
    # Saved whatever the tests' outcome, and only when the restore missed and every jar is there.
    for step in ("Trino plugins fetched", "Cache Trino plugins"):
        assert "always()" in by_name[step]["if"] and "inputs.phase == 'save'" in by_name[step]["if"]
    assert "inputs.cache-hit != 'true'" in by_name["Trino plugins fetched"]["if"]


def test_the_trino_plugin_pin_step_names_the_harness_pin():
    import subprocess

    pin = next(s for s in _plugin_cache_action() if s.get("name") == "Trino plugin pin")
    script = pin["run"].replace('>> "$GITHUB_OUTPUT" ', "")
    out = subprocess.run(  # noqa: S603 — the workflow's own step, run at the repo root
        ["bash", "-c", script], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout
    import tests.conftest as harness

    # The cache key names the pin and every plugin fetched at a version of its own.
    own = harness._TRINO_PLUGIN_VERSIONS
    key = "_".join([harness._TRINO_PLUGIN_VERSION, *(f"{n}-{own[n]}" for n in sorted(own))])
    lines = out.splitlines()
    assert lines[0] == f"version={key}"
    assert lines[1].removeprefix("jars=").split() == [
        f"trino/plugins/{n}/{n}-{harness._trino_plugin_version(n)}.jar"
        for n in harness._TRINO_PLUGINS
    ]


# --- live lanes: what runs is decided by lane selection, and each lane names its secrets ---------


def _lanes_workflow() -> dict:
    import yaml

    return yaml.safe_load(
        (REPO / ".github" / "workflows" / "integration-suite-lanes.yml").read_text()
    )


def _credentials_step(job: str) -> dict:
    (step,) = [s for s in _job_steps(job) if s.get("name") == "Credentials present"]
    return step


def test_the_live_salesforce_test_is_in_no_lane_but_its_own():
    """The org allows 15,000 API calls a day and a cold run costs about 2,450: the test carries
    requires_warehouse, and the nightly warehouse lane must not select it."""
    run_lane = _runner()
    marker = run_lane.LANES["warehouse"].marker
    assert marker == "requires_warehouse and not requires_salesforce"
    assert run_lane.LANES["salesforce"].marker == "requires_salesforce"
    # Every other lane excludes the live tests through the base exclusion or its own marker.
    for name, lane in run_lane.LANES.items():
        if name in ("warehouse", "salesforce"):
            continue
        assert "not requires_warehouse" in lane.marker or name in ("isolated", "e2e", "cluster")
    for test in ("test_salesforce_source_e2e.py", "test_cloudops_source_e2e.py"):
        source = (REPO / "tests" / "integration" / test).read_text()
        assert "pytest.mark.requires_warehouse" in source  # so no push lane selects it


def test_the_salesforce_lane_runs_only_when_a_dispatch_asks_for_it():
    job = _lanes_workflow()["jobs"]["salesforce"]
    assert job["if"] == "github.event_name == 'workflow_dispatch' && inputs.include_salesforce"
    import yaml

    caller = yaml.safe_load((REPO / ".github" / "workflows" / "integration-suite.yml").read_text())
    dispatch = caller[True]["workflow_dispatch"]["inputs"]["include_salesforce"]  # `on` is True
    assert dispatch["default"] is False
    assert caller["jobs"]["lanes"]["with"]["include_salesforce"] == (
        "${{ inputs.include_salesforce || false }}"
    )


def test_the_salesforce_lane_keeps_its_describe_caches_between_runs_by_org():
    steps = {s.get("name"): s for s in _job_steps("salesforce")}
    names = list(steps)
    restore, save = (
        steps["Restore Salesforce describe caches"],
        steps["Cache Salesforce describe caches"],
    )
    assert (
        names.index("Credentials present") < names.index(restore["name"]) < names.index("Run lane")
    )
    assert names.index("Run lane") < names.index(save["name"])
    assert restore["with"]["path"] == save["with"]["path"]
    for kept in ("salesforce-itest-state", "itest-trino-salesforce-describe"):
        assert f".runtime-deps/{kept}" in save["with"]["path"]
    assert restore["with"]["key"] == save["with"]["key"]
    assert "steps.org.outputs.digest" in save["with"]["key"]
    assert restore["with"]["restore-keys"].endswith("${{ steps.org.outputs.digest }}-")
    env = _lanes_workflow()["jobs"]["salesforce"]["env"]
    assert env["PROVISA_RUNTIME_DEPS_CACHE"] == "${{ github.workspace }}/.runtime-deps"


_CLOUDOPS_SECRETS = [
    "CLOUDOPS_AZURE_TENANT_ID",
    "CLOUDOPS_AZURE_CLIENT_ID",
    "CLOUDOPS_AZURE_CLIENT_SECRET",
    "CLOUDOPS_AZURE_SUBSCRIPTION_IDS",
    "CLOUDOPS_AWS_ACCESS_KEY_ID",
    "CLOUDOPS_AWS_SECRET_ACCESS_KEY",
    "CLOUDOPS_AWS_ACCOUNT_IDS",
    "CLOUDOPS_AWS_REGION",
    "CLOUDOPS_GCP_PROJECT_IDS",
]
_SALESFORCE_SECRETS = [
    "SF_LOGIN_URL",
    "SF_CONSUMER_KEY",
    "SF_CONSUMER_SECRET",
    "SF_PASSWORD",
    "SF_SECURITY_TOKEN",
    "SF_NICKNAME",
]


@pytest.mark.parametrize(
    ("job", "secrets"),
    [
        ("warehouse", [*_CLOUDOPS_SECRETS, "CLOUDOPS_GCP_CREDENTIALS_JSON_BASE64"]),
        ("salesforce", _SALESFORCE_SECRETS),
    ],
)
def test_a_live_lane_declares_its_secrets_and_fails_by_name_when_one_is_missing(job, secrets):
    """A live test runs in CI only in a lane whose job is handed its secrets and checks them
    before anything runs: a missing one ends the lane naming it, it is never a skipped test."""
    declared = {**_lanes_workflow()["jobs"][job]["env"], **_credentials_step(job).get("env", {})}
    script = _credentials_step(job)["run"]
    checked = set(re.search(r"for v in (.*?); do", script, re.S).group(1).replace("\\", "").split())
    for name in secrets:
        assert declared[name] == f"${{{{ secrets.{name} }}}}"
        assert name in checked
    assert "repo secrets not set:$missing" in script and "exit 1" in script


def test_the_cloudops_gcp_key_reaches_the_test_as_a_file_and_is_never_printed():
    script = _credentials_step("warehouse")["run"]
    assert (
        "printf '%s' \"$CLOUDOPS_GCP_CREDENTIALS_JSON_BASE64\" | base64 -d > "
        '"$RUNNER_TEMP/cloudops-gcp-key.json"'
    ) in script
    assert (
        'CLOUDOPS_GCP_CREDENTIALS_PATH=$RUNNER_TEMP/cloudops-gcp-key.json" >> "$GITHUB_ENV"'
        in script
    )
    assert "echo $CLOUDOPS" not in script and 'echo "$CLOUDOPS' not in script


def _workflow_jobs() -> list[tuple[str, str, list[dict]]]:
    import yaml

    found = []
    for path in sorted((REPO / ".github" / "workflows").glob("*.yml")):
        for name, job in (yaml.safe_load(path.read_text()).get("jobs") or {}).items():
            found.append((path.name, name, job.get("steps") or []))
    return found


def test_every_job_that_collects_the_container_suite_restores_the_trino_plugins_by_pin():
    """tests/conftest.py fetches the pinned Trino plugin jars from Maven Central when it collects
    tests/integration, tests/steps or tests/e2e, and a refused fetch ends the run before any
    test. The release run of the amd64-only engines workflow stopped there (37874425655: HTTP 404
    for a jar that exists): it was the one job that collected those tests without the suite's
    cache. Every such job restores and saves through the one plugin-cache action."""
    collecting, in_a_guest = [], []
    for workflow, job, steps in _workflow_jobs():
        runs = [(i, s.get("run") or "") for i, s in enumerate(steps)]
        tests_at = [
            i
            for i, run in runs
            if ("pytest" in run or "run_lane.py" in run)
            and "--matrix" not in run
            and any(
                d in run for d in ("tests/integration", "tests/steps", "tests/e2e", "run_lane.py")
            )
        ]
        if not tests_at:
            continue
        if any("packaging/nixos/vm-run" in run for _i, run in runs):
            # Runs inside a NixOS guest, which has no access to the runner's cache: it fetches
            # the jars from Maven Central on every run and is exposed to the same refusal. Named
            # here so it is seen, not hidden; closing it means carrying the jars into the guest.
            in_a_guest.append(f"{workflow}:{job}")
            continue
        collecting.append(f"{workflow}:{job}")
        names = [s.get("name") for s in steps]
        assert "Restore Trino plugins" in names and "Cache Trino plugins" in names, (
            f"{workflow}:{job} collects the container suite without the Trino plugin cache"
        )
        at_restore, at_save = _plugin_cache_steps(steps)
        assert at_restore < tests_at[0] < at_save, f"{workflow}:{job}"
    assert "amd64-engines.yml:exasol" in collecting and len(collecting) >= 5, collecting
    assert in_a_guest == ["nixos.yml:lane"], in_a_guest
