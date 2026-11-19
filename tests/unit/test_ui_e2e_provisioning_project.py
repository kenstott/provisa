# Copyright (c) 2026 Kenneth Stott
# Canary: 9b0d692c-8ff1-4603-8843-c5ee5fe0780d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The core UI lane runs the specs that start and stop containers alone, one worker, last.

A compose stack brought up or down is a Docker network made or removed on the host, and Chromium
aborts every request in flight when the host's interfaces change (net::ERR_NETWORK_CHANGED) --
in whichever worker's page is loading. On v0.1.0-alpha.477 seventeen core-lane tests failed on it
(run 37874182519). playwright.config.ts names those specs (PROVISIONING_SPECS) and gives them a
project of their own; the workflow and the npm script run it after "core" with one worker."""

from __future__ import annotations

import fnmatch
import json
import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
UI = REPO / "provisa-ui"
CONFIG = (UI / "playwright.config.ts").read_text()
RUNNER = "playwright " + "test"

# What marks a spec as changing the host's Docker networks while it runs.
_STARTS_CONTAINERS = re.compile(
    r"\"sources\",\s*\"provision\.py\"|spawnSync\(\s*\"docker\"|execFileSync\(\s*\"docker\"|"
    r"startDemoSources\(|removeDemoSources\("
)


def _list(name: str) -> set[str]:
    body = re.search(rf"const {name} = \[(.*?)\];", CONFIG, re.S)
    assert body, name
    return {m.rsplit("/", 1)[-1] for m in re.findall(r'"([^"]+)"', body.group(1))}


def test_every_core_spec_that_starts_containers_is_in_the_provisioning_project():
    elsewhere = _list("TRINO_SPECS") | _list("SWAP_SPECS") | _list("REGIONS_SPECS")
    starts = {
        spec.name
        for spec in (UI / "e2e").glob("*.spec.ts")
        if _STARTS_CONTAINERS.search(spec.read_text()) and spec.name not in elsewhere
    }
    assert starts, "the marker no longer matches any spec"
    assert starts <= _list("PROVISIONING_SPECS"), sorted(starts - _list("PROVISIONING_SPECS"))


def test_the_core_project_leaves_those_specs_to_the_provisioning_project():
    core = re.search(r'name: "core",\s*testIgnore: \[(.*?)\]', CONFIG, re.S)
    assert core and "...PROVISIONING_SPECS" in core.group(1)
    assert re.search(r'name: "core-provisioning",\s*testMatch: \[\.\.\.PROVISIONING_SPECS,', CONFIG)


# --- The AskAmerica specs run in the one-worker project too (maintainer 2026-11-16) ---
# The connector of an AskAmerica source held about a core for four minutes of run 43781680787 and
# the specs on the other worker reached their time limits waiting for CPU.

_ASKAMERICA_KEY = "process.env.FREE_ASKAMERICA_KEY"
_DESCRIBE = re.compile(r'test\.describe\(\s*(["`])(.*?)\1', re.S)


def _regex(name: str) -> re.Pattern[str]:
    found = re.search(rf"const {name} =\s*/(.*)/;", CONFIG)
    assert found, name
    return re.compile(found.group(1))


def _matches(name: str, spec: str) -> bool:
    body = re.search(rf"const {name} = \[(.*?)\];", CONFIG, re.S)
    assert body, name
    return any(
        fnmatch.fnmatch(spec, glob.rsplit("/", 1)[-1])
        for glob in re.findall(r'"([^"]+)"', body.group(1))
    )


def _projects_of(spec: str, describe: str) -> set[str]:
    """Which of the core lane's two projects run the tests of ``describe`` in ``spec``: the
    selection playwright.config.ts states, by file (testMatch, testIgnore) and then by title
    (grepInvert, read against "<project> <file> <describe> <test>")."""
    elsewhere = ("TRINO_SPECS", "SWAP_SPECS", "REGIONS_SPECS", "WAREHOUSE_SPECS")
    one_worker = ("PROVISIONING_SPECS", "ASKAMERICA_SPECS")
    found = set()
    if not any(_matches(name, spec) for name in elsewhere + one_worker) and not (
        _regex("WAREHOUSE_TITLES").search(describe) or _regex("ASKAMERICA_TITLES").search(describe)
    ):
        found.add("core")
    if any(_matches(name, spec) for name in (*one_worker, "ASKAMERICA_TITLE_SPECS")) and not (
        _regex("ASKAMERICA_TITLE_SPECS_OTHER_CASES").search(
            f"core-provisioning {spec} {describe} a test"
        )
    ):
        found.add("core-provisioning")
    return found


def _describes(spec: Path) -> list[tuple[int, str]]:
    return [(m.start(), m.group(2)) for m in _DESCRIBE.finditer(spec.read_text())]


def test_the_two_core_projects_state_the_askamerica_selection():
    core = re.search(r'name: "core",\s*testIgnore: \[(.*?)\]', CONFIG, re.S)
    assert core and "...ASKAMERICA_SPECS" in core.group(1)
    body = CONFIG[CONFIG.index('name: "core-provisioning",') :]
    body = body[: body.index("},")]
    assert "...ASKAMERICA_SPECS, ...ASKAMERICA_TITLE_SPECS]" in body
    assert "grepInvert: ASKAMERICA_TITLE_SPECS_OTHER_CASES" in body


def test_every_spec_that_adds_an_askamerica_source_runs_in_the_one_worker_project_only():
    # Of the core lane: the engine-swap specs that add one are the swap lane's.
    elsewhere = ("TRINO_SPECS", "SWAP_SPECS", "REGIONS_SPECS", "WAREHOUSE_SPECS")
    readers = [
        s
        for s in sorted((UI / "e2e").glob("*.spec.ts"))
        if _ASKAMERICA_KEY in s.read_text() and not any(_matches(n, s.name) for n in elsewhere)
    ]
    assert {s.name for s in readers} >= {
        "askamerica-server-listening.spec.ts",
        "askamerica-table-picker.spec.ts",
        "source-to-query-special-cases.spec.ts",
    }
    for spec in readers:
        text, describes = spec.read_text(), _describes(spec)
        for read in re.finditer(re.escape(_ASKAMERICA_KEY), text):
            # The describe the key is read in: the last one opened before it.
            title = [t for at, t in describes if at < read.start()][-1]
            assert _projects_of(spec.name, title) == {"core-provisioning"}, (spec.name, title)


def test_every_other_case_of_a_spec_split_by_title_stays_in_the_core_project():
    """No case is run by both projects or by neither: of the special-cases file, the govdata case
    is the one-worker project's, google_sheets is the nightly warehouse project's, and every
    other one stays in "core"."""
    spec = UI / "e2e" / "source-to-query-special-cases.spec.ts"
    where = {title: _projects_of(spec.name, title) for _, title in _describes(spec)}
    assert len(where) >= 4
    for title, projects in where.items():
        if title.startswith("govdata: "):
            assert projects == {"core-provisioning"}, title
        elif title.startswith("google_sheets: "):
            assert projects == set(), title
            assert _regex("WAREHOUSE_TITLES").search(title)
        else:
            assert projects == {"core"}, title


def test_every_spec_file_is_run_by_exactly_one_project_of_the_lane_or_by_another_lane():
    elsewhere = ("TRINO_SPECS", "SWAP_SPECS", "REGIONS_SPECS", "WAREHOUSE_SPECS")
    for spec in sorted((UI / "e2e").glob("*.spec.ts")):
        if _matches("ASKAMERICA_TITLE_SPECS", spec.name):
            continue  # split by title: the test above
        projects = _projects_of(spec.name, "a describe")
        if any(_matches(name, spec.name) for name in elsewhere):
            assert projects == set(), spec.name
        else:
            assert len(projects) == 1, (spec.name, projects)


def test_the_askamerica_key_reaches_the_job_that_runs_its_specs():
    """Without the key each of them is skipped by name, and a skip is not a pass."""
    jobs = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())["jobs"]
    assert "FREE_ASKAMERICA_KEY" in jobs["provisioning"]["secrets"]
    assert "FREE_ASKAMERICA_KEY" not in jobs["playwright"]["secrets"]


def test_the_lane_runs_the_provisioning_project_alone_with_one_worker():
    """Two jobs of the core UI workflow, each a call of the lane workflow and so each on a runner
    of its own: "core" with the lane's workers, and "core-provisioning" with one. No other
    worker's page is on the host whose networks the provisioning specs change."""
    workflow = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())
    jobs = workflow["jobs"]
    assert {job: body["uses"] for job, body in jobs.items()} == {
        "playwright": "./.github/workflows/lane.yml",
        "provisioning": "./.github/workflows/lane.yml",
    }
    core, provisioning = jobs["playwright"]["with"]["run"], jobs["provisioning"]["with"]["run"]
    assert RUNNER in core and "--project=core " in core and "--workers" not in core
    assert RUNNER in provisioning and "--project=core-provisioning --workers=1" in provisioning
    assert "needs" not in jobs["provisioning"]  # side by side, not after
    # Each keeps a report of its own.
    names = {body["with"]["artifact-name"] for body in jobs.values()}
    assert names == {
        "playwright-report-core-${{ matrix.shard }}of2",
        "playwright-report-core-provisioning",
    }
    # The provisioning job is the one that starts SQL Server's primer.
    assert jobs["provisioning"]["with"]["odbc"] is True

    script = json.loads((UI / "package.json").read_text())["scripts"]["test:e2e:core"]
    first = re.search(r"--project=core(?!-)", script)
    second = script.index("--project=core-provisioning")
    assert first and first.start() < second and "--workers=1" in script[second:]


def test_the_core_job_runs_one_worker_per_runner_in_shards():
    """Two workers on one 4-vCPU runner held it at a load of about 10 (run 43995449668): schema
    rebuilds took a median 11.1 s against 3.3 s in the one-worker provisioning job of the same run,
    and the longest specs ran out of their bounds. The "core" job is two shards, each one worker
    (a backend and a browser) on a runner of its own."""
    job = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())["jobs"][
        "playwright"
    ]
    assert job["strategy"]["matrix"]["shard"] == [1, 2]
    assert job["strategy"]["fail-fast"] is False
    given = job["with"]
    env = dict(line.split("=", 1) for line in given["env"].splitlines() if line)
    assert env["PROVISA_E2E_WORKERS"] == "1"
    assert env["PROVISA_E2E_SHARD"] == "${{ matrix.shard }}/2"
    assert "--project=core --shard=$PROVISA_E2E_SHARD " in given["run"]
    # Each shard is named, cached and reported as its own lane.
    assert given["name"] == "ui-core ${{ matrix.shard }}/2"


def test_the_warehouse_ui_job_runs_one_worker_on_its_runner():
    """The warehouse UI job runs the same backends on the same 4-vCPU runner as a core shard: two
    workers there held a core runner at a load of about 10 (run 43995449668). It runs one, and is
    not sharded: two shards would each hold the live-cloud-warehouses group, and each one's
    after-script would stop the warehouses the other was still using."""
    job = yaml.safe_load((REPO / ".github/workflows/integration-suite-lanes.yml").read_text())[
        "jobs"
    ]["warehouse-ui"]
    assert "strategy" not in job
    assert job["concurrency"]["group"] == "live-cloud-warehouses"
    given = job["with"]
    env = dict(line.split("=", 1) for line in given["env"].splitlines() if line)
    assert env["PROVISA_E2E_WORKERS"] == "1"
    assert "--shard" not in given["run"] and "--workers" not in given["run"]


def test_each_embedded_control_plane_stops_after_its_backend():
    """Playwright stops its webServers after globalTeardown, in the reverse of their order. Stopped
    by globalTeardown, the control planes went while their backends still ran, whose replica
    housekeeping then failed on the gone socket (run 43995449668). Each is stopped by a webServer
    entry listed before every other, so torn down after them, on its graceful-shutdown signal."""
    config = (UI / "playwright.config.ts").read_text()
    servers = config[config.index("webServer: ONLY_REGIONS_DEMO") :]
    first = servers[servers.index(": [") + 3 :].lstrip()
    assert first.startswith("...embeddedControlPlaneStops,")
    stops = config[config.index("const embeddedControlPlaneStops") :]
    stops = stops[: stops.index(": [];")]
    assert "provisa.core.control_plane_pg stop" in stops
    assert "trap " in stops and " TERM;" in stops
    assert 'gracefulShutdown: { signal: "SIGTERM"' in stops
    teardown = (UI / "e2e" / "global-teardown.ts").read_text()
    assert "control_plane_pg" not in teardown
    assert "PROVISA_E2E_EMBEDDED_CONTROL_PLANES" not in config + teardown


def test_each_core_job_starts_only_the_demo_sources_its_project_reads():
    """Global setup starts the demo sources a run needs and keeps them up for the whole run. A
    run of the "core" project alone needs the Hasura import's database and the Neo4j export
    target, not the nine big sources of source-to-query.spec.ts, which is the provisioning
    project's: started for both, they held about 5 GB of the "core" runner for nothing (run
    38050265191)."""
    resolver = (UI / "e2e" / "resolve-needed-sources.ts").read_text()
    assert (
        'projects[0] === "core"' in resolver and 'projects[0] === "core-provisioning"' in resolver
    )
    # What the resolver assumes about where each container-reading file runs.
    provisioning = _list("PROVISIONING_SPECS")
    assert "source-to-query.spec.ts" in provisioning
    for core_file in ("hasura-import.spec.ts", "neo4j-docker-export.spec.ts"):
        assert core_file not in provisioning
        assert core_file in resolver
    # Each job of the lane names exactly one project, so the resolver decides by it.
    workflow = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())
    runs = {job: body["with"]["run"] for job, body in workflow["jobs"].items()}
    assert re.findall(r"--project=([\w-]+)", runs["playwright"]) == ["core"]
    assert re.findall(r"--project=([\w-]+)", runs["provisioning"]) == ["core-provisioning"]
