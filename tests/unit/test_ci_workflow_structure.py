# Copyright (c) 2026 Kenneth Stott
# Canary: 318949f1-7d73-4478-a8d0-ee2fb2560c5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The workflows share their setup through composite actions, and none carries a copy.

Every false red of the v0.1.0-alpha.477 round was one lane's setup having drifted from
another's: the amd64 job had no Trino plugin cache (Maven Central 404, no test ran), the core UI
lane had neither data-quality checker nor the SQL Server ODBC driver, and two of the four macOS
installer jobs carried a shorter copy of the signing import. Each kind of setup now lives in one
action under .github/actions and every job that needs it calls that action. These guards hold
the shape, with no list of exceptions beyond the one stated below (the NixOS guest)."""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = sorted((REPO / ".github" / "workflows").glob("*.yml"))
ACTIONS = sorted((REPO / ".github" / "actions").glob("*/action.yml"))

# A command that runs inside the NixOS guest (packaging/nixos/vm-run '...'), not on the runner.
# A composite action runs on the runner: it cannot install into the guest, which has no access
# to the runner's tool cache, its node_modules or its apt. The guest's installs are the one
# setup the actions cannot carry.
_IN_THE_GUEST = "packaging/nixos/vm-run"

# Setup that belongs to an action, and the action that owns it.
_OWNED = {
    r"astral-sh/setup-uv": "setup-python",
    r"\buv sync\b": "setup-python",
    r"\bnpm ci\b": "setup-ui",
    r"playwright install": "setup-ui",
    r"msodbcsql18": "mssql-odbc",
    r"pip install[^\n]*mkdocs": "docs-site-deps",
    r"security import\b": "apple-signing",
    r"key: (\$\{\{ inputs.key-prefix \}\})?trino-plugins-": "trino-plugins",
    r"KEY: (\$\{\{ inputs.key-prefix \}\})?provisa-functions-": "trino-plugins",
    r"key: pyodide-downloads-": "pyodide-downloads",
    r"duckdb-ext-wheels-\$\{\{ steps\.label": "duckdb-ext-wheels",
    r"gh release create": "ensure-release",
    r"rm -rf /usr/share/dotnet": "free-runner-disk",
}


def _jobs():
    for path in WORKFLOWS:
        for job, body in (yaml.safe_load(path.read_text()).get("jobs") or {}).items():
            yield path.name, job, body


def _steps():
    for workflow, job, body in _jobs():
        for step in body.get("steps") or []:
            yield f"{workflow}:{job}:{step.get('name') or step.get('uses')}", step


def test_every_local_action_or_workflow_a_job_uses_exists():
    used = []
    for where, step in _steps():
        if (step.get("uses") or "").startswith("./"):
            used.append((where, step["uses"]))
    for workflow, job, body in _jobs():
        if (body.get("uses") or "").startswith("./"):
            used.append((f"{workflow}:{job}", body["uses"]))
    assert used, "no local action is used at all"
    for where, target in used:
        path = REPO / target
        found = (
            path.is_file()
            if target.endswith((".yml", ".yaml"))
            else (path / "action.yml").is_file()
        )
        assert found, f"{where} uses {target}, which does not exist"


def test_no_workflow_step_carries_setup_an_action_owns():
    """On the runner, each kind of setup is installed by its action and nowhere else."""
    inline = []
    for where, step in _steps():
        text = f"{step.get('uses') or ''}\n{step.get('run') or ''}"
        if _IN_THE_GUEST in text:
            continue
        for pattern, action in _OWNED.items():
            if re.search(pattern, text):
                inline.append(f"{where}: `{pattern}` belongs to .github/actions/{action}")
    assert inline == []


def test_each_owned_setup_is_in_exactly_its_own_action():
    for pattern, action in _OWNED.items():
        holders = [p.parent.name for p in ACTIONS if re.search(pattern, p.read_text())]
        assert holders == [action], f"`{pattern}` is in {holders}, expected only {action}"


@pytest.mark.parametrize("action", ACTIONS, ids=lambda p: p.parent.name)
def test_a_composite_action_names_its_shell_and_hides_no_failing_command(action):
    """A composite step has no default shell, and `bash` there is not `bash -eo pipefail`: a step
    that pipes one command into another says `set -o pipefail` itself (the rule the workflows'
    own pipe guard holds for their steps)."""
    body = yaml.safe_load(action.read_text())
    assert body["runs"]["using"] == "composite"
    for step in body["runs"]["steps"]:
        if "run" not in step:
            continue
        assert step.get("shell") == "bash", (
            f"{action.parent.name}: {step.get('name')} names no shell"
        )
        script = re.sub(r"<<'EOF'.*?^\s*EOF$", "", step["run"], flags=re.S | re.M)
        script = re.sub(r"\\\n\s*", " ", script)
        piped = any(re.search(r"[^|]\|[^|]", line.split("#")[0]) for line in script.splitlines())
        if piped:
            assert re.search(r"set -[a-z]*o pipefail", script), (
                f"{action.parent.name}: {step.get('name')} pipes without pipefail"
            )


def test_an_action_reads_no_secret_itself():
    """A composite action cannot read secrets; what it needs is an input the caller passes."""
    for action in ACTIONS:
        assert not re.search(r"\bsecrets\.[A-Za-z_]", action.read_text()), action.parent.name


def test_every_job_that_needs_a_python_environment_gets_it_from_the_action():
    """A job that runs `uv run`/`uv pip`/`uv build` on the runner has called setup-python."""
    for workflow, job, body in _jobs():
        steps = body.get("steps") or []
        uses_uv = any(
            re.search(r"\buv (run|pip|build|export|tool)\b|\buvx\b", s.get("run") or "")
            and _IN_THE_GUEST not in (s.get("run") or "")
            for s in steps
        )
        if uses_uv:
            assert any((s.get("uses") or "") == "./.github/actions/setup-python" for s in steps), (
                f"{workflow}:{job} runs uv without the setup-python action"
            )


def test_the_python_sdk_job_tests_in_the_project_environment():
    """REQ-1984: the SDK's tests read each decorator through Provisa's own reader and manifest,
    which import the project's dependencies; with pytest and pyright alone,
    provisa.core.package_registries could not import sqlalchemy (run 45024448489)."""
    path = REPO / ".github" / "workflows" / "release-packages.yml"
    steps = yaml.safe_load(path.read_text())["jobs"]["build-sdk-python"]["steps"]
    uses = [s.get("uses") or "" for s in steps]
    assert "./.github/actions/setup-python" in uses
    assert not any(u.startswith("actions/setup-python") for u in uses)
    (test,) = [s for s in steps if "provisa-sdk/python/tests" in (s.get("run") or "")]
    assert test["run"].strip().startswith("uv run --frozen "), test["run"]
    assert uses.index("./.github/actions/setup-python") < steps.index(test)
    assert not any("pip install" in (s.get("run") or "") for s in steps)


def test_every_prebuilt_job_that_builds_the_app_image_frees_runner_disk_first():
    """The arm64 build of provisa/provisa:local (QEMU, the full site-packages copied between
    stages) next to the saved core and Trino images needs more disk than a hosted runner always
    leaves free: on an AWS-backed ubuntu-latest runner (run 41836852453) it ran out at the
    installer-to-runtime COPY, where Azure-backed runners had passed with the same inputs."""
    path = REPO / ".github" / "workflows" / "release-prebuilt.yml"
    builders = []
    for job, body in (yaml.safe_load(path.read_text())["jobs"] or {}).items():
        steps = body.get("steps") or []
        builds = [
            i for i, s in enumerate(steps) if "-t provisa/provisa:local" in (s.get("run") or "")
        ]
        if not builds:
            continue
        builders.append(job)
        frees = [
            i for i, s in enumerate(steps) if s.get("uses") == "./.github/actions/free-runner-disk"
        ]
        assert frees and frees[0] < builds[0], f"{job} builds the app image without freeing disk"
    assert set(builders) == {"pull-images-core", "pull-images-core-amd64"}, builders


def test_a_release_app_image_stages_the_duckdb_extension_wheels_its_own_run_built():
    """Run 45030695628: a release dry run publishes nothing, so the Pages index had no
    provisa-duckdb-ext wheels and the app image's stage step refused at Dockerfile
    `RUN python -m provisa.runtime_deps.stage_assets`. A real release builds its images before
    publish writes the Pages index, so it is no different. Both build the wheels in their own run
    and every app image build takes them as a build context. Neither source holding a wheel still
    stops the build."""
    for name in ("Dockerfile", "Dockerfile.dev"):
        dockerfile = (REPO / name).read_text()
        assert dockerfile.count("\nFROM scratch AS duckdb-ext-wheels\n") == 1, name
        stage = (
            "RUN --mount=type=bind,from=duckdb-ext-wheels,target=/duckdb-ext-wheels \\\n"
            "    PIP_FIND_LINKS=/duckdb-ext-wheels python -m provisa.runtime_deps.stage_assets\n"
        )
        assert stage in dockerfile, name
        assert dockerfile.count("provisa.runtime_deps.stage_assets") == 1, name

    extensions = yaml.safe_load(
        (REPO / ".github" / "workflows" / "build-duckdb-extensions.yml").read_text()
    )
    called = extensions[True]["workflow_call"]["inputs"]["publish"]
    assert called == {**called, "type": "boolean", "required": True}
    # Called from a release on its tag, it must not cancel the run "Release: cut" dispatches there.
    assert "${{ github.workflow }}" in extensions["concurrency"]["group"]
    upload = next(
        s
        for s in extensions["jobs"]["wheel"]["steps"]
        if s.get("uses", "").startswith("actions/upload-artifact")
    )
    assert upload["with"]["name"] == "provisa-duckdb-ext-wheels"

    jobs = yaml.safe_load((REPO / ".github" / "workflows" / "release-prebuilt.yml").read_text())[
        "jobs"
    ]
    leg = jobs["duckdb-extensions"]
    assert leg["uses"] == "./.github/workflows/build-duckdb-extensions.yml"
    assert leg["with"] == {"publish": False}  # "Release: cut" dispatches the attaching run
    for job in ("pull-images-core", "pull-images-core-amd64"):
        body = jobs[job]
        assert "duckdb-extensions" in body["needs"], job
        steps = body["steps"]
        names = [s.get("name") for s in steps]
        # A release's image as a dry run's: built before publish writes the Pages index.
        download = steps[names.index("DuckDB extension wheels")]
        assert "if" not in download, job
        assert download["uses"].startswith("actions/download-artifact@"), job
        assert download["with"] == {
            "name": upload["with"]["name"],
            "path": "${{ runner.temp }}/duckdb-ext-wheels",
        }, job
        build = next(s for s in steps if "-t provisa/provisa:local" in (s.get("run") or ""))
        assert steps.index(download) < steps.index(build), job
        assert build["env"]["DUCKDB_EXT_WHEELS"] == download["with"]["path"], job
        assert '--build-context duckdb-ext-wheels="$DUCKDB_EXT_WHEELS"' in build["run"], job


_DUCKDB_EXT_ACTION = "./.github/actions/duckdb-ext-wheels"


def _workflow_jobs(name: str) -> dict:
    return yaml.safe_load((REPO / ".github" / "workflows" / name).read_text())["jobs"]


def _action_step(steps: list[dict], phase: str) -> int:
    [index] = [
        i
        for i, s in enumerate(steps)
        if s.get("uses") == _DUCKDB_EXT_ACTION and (s.get("with") or {}).get("phase") == phase
    ]
    return index


def test_every_ci_image_build_is_given_this_commits_duckdb_extension_wheels():
    """REQ-294: an image of a commit no release published finds no provisa-duckdb-ext wheels on
    the Pages index, and its stage step stops. Every CI build of Dockerfile or Dockerfile.dev is
    given the wheels the duckdb-ext-wheels action built from this commit, before it builds."""
    # The dev image, built by the tests the integration lanes and the cluster lane run.
    for path in (
        "tests/e2e/test_helm_minikube.py",
        "tests/integration/test_master_key_survives_recreate_compose.py",
    ):
        text = (REPO / path).read_text()
        assert "Dockerfile.dev" in text, path
        assert "*duckdb_ext_wheels.build_context_args()," in text, path
    from tests import duckdb_ext_wheels

    assert duckdb_ext_wheels.STAGE == "duckdb-ext-wheels"
    lanes = _workflow_jobs("integration-suite-lanes.yml")
    _action_step(lanes["trino-plugins"]["steps"], "prime")
    cluster = lanes["cluster"]
    assert "trino-plugins" in cluster["needs"]
    names = [s.get("name") for s in cluster["steps"]]
    assert _action_step(cluster["steps"], "restore") < names.index("Run lane")
    suite = lanes["suite"]
    assert "trino-plugins" in suite["needs"]
    assert suite["with"]["duckdb-ext-wheels"] is True
    lane = yaml.safe_load((REPO / LANE.removeprefix("./")).read_text())
    assert lane[True]["workflow_call"]["inputs"]["duckdb-ext-wheels"]["default"] is False
    steps = lane["jobs"]["lane"]["steps"]
    restore = _action_step(steps, "restore")
    assert steps[restore]["if"] == "inputs.duckdb-ext-wheels"
    assert restore < [s.get("name") for s in steps].index("Run lane")

    # The app image: docker-image.yml's build, and the AppImage's (release and NixOS).
    image = _workflow_jobs("docker-image.yml")["image"]["steps"]
    [build] = [i for i, s in enumerate(image) if "docker build" in (s.get("run") or "")]
    assert _action_step(image, "prime") < build
    assert '--build-context duckdb-ext-wheels="$PROVISA_DUCKDB_EXT_WHEELS"' in image[build]["run"]
    for workflow, job in (("release-linux.yml", "build-linux"), ("nixos.yml", "appimage-build")):
        steps = _workflow_jobs(workflow)[job]["steps"]
        [build] = [i for i, s in enumerate(steps) if "build-appimage.sh" in (s.get("run") or "")]
        assert _action_step(steps, "prime") < build, workflow
    appimage = (REPO / "packaging/linux/build-appimage.sh").read_text()
    assert (
        'duckdb_ext=(--build-context "duckdb-ext-wheels=${PROVISA_DUCKDB_EXT_WHEELS}")' in appimage
    )
    assert '${duckdb_ext[@]+"${duckdb_ext[@]}"} \\\n' in appimage


def test_the_duckdb_ext_wheels_action_builds_only_on_a_priming_miss():
    """The prime phase mirrors and builds only when the cache misses; the restore phase never
    builds, and takes the run's artifact on a miss. Both name the directory for the image build."""
    action = yaml.safe_load((REPO / ".github/actions/duckdb-ext-wheels/action.yml").read_text())
    steps = {s["name"]: s for s in action["runs"]["steps"]}
    miss = "steps.cache.outputs.cache-hit != 'true'"
    build = steps["Mirror the DuckDB extensions and build the wheels"]
    assert build["if"] == f"inputs.phase == 'prime' && {miss}"
    assert "scripts/ci/mirror_duckdb_extensions.py" in build["run"]
    assert "scripts/ci/build_duckdb_ext_wheels.sh" in build["run"]
    assert steps["Cache the duckdb-ext wheels"]["if"] == f"inputs.phase == 'prime' && {miss}"
    download = steps["Download the run's duckdb-ext wheels"]
    assert download["if"] == f"inputs.phase == 'restore' && {miss}"
    upload = steps["Upload the run's duckdb-ext wheels"]
    assert upload["if"] == "inputs.phase == 'prime'"
    assert download["with"]["name"] == upload["with"]["name"] == "duckdb-ext-wheels"
    assert upload["with"]["if-no-files-found"] == "error"
    restore = steps["Restore the duckdb-ext wheels"]["with"]
    assert restore["key"] == steps["Cache the duckdb-ext wheels"]["with"]["key"]
    assert "PROVISA_DUCKDB_EXT_WHEELS=" in steps["duckdb-ext wheels directory"]["run"]
    # The key covers what decides the wheels' content.
    key = steps["duckdb-ext wheels key"]["env"]["KEY"]
    for source in (
        "packaging/duckdb-ext/**",
        "scripts/ci/mirror_duckdb_extensions.py",
        "scripts/ci/build_duckdb_ext_wheels.sh",
        "provisa/federation/duckdb_extensions.py",
    ):
        assert f"'{source}'" in key, source


# --- one lane workflow (stage 2) ------------------------------------------------------------------

LANE = "./.github/workflows/lane.yml"
_RUNNER = "playwright " + "test"
_LEAF_CHECK = "ci-leaf-check.yml"

# The suite's jobs that keep steps of their own (lane.yml's header says why): each carries live
# credentials or cluster tooling, and the lane workflow declares only the few secrets its callers
# pass by name instead of inheriting every one.
_OWN_STEPS = {
    "integration-suite-lanes.yml": {
        "cluster",
        "warehouse",
        "salesforce",
        "athena",
        "servicenow",
        "microsoft_365",
    },
    # REQ-2012: carries the paid model keys of its own, as the live-account jobs do.
    "polly-bakeoff.yml": {"bakeoff"},
    # REQ-2006: walks the bundle it built before publishing it, with the build's live keys.
    "demo-bundles.yml": {"bundle"},
}


def _runs_a_lane(command: str) -> bool:
    container_suite = ("pytest" in command or "run_lane.py" in command) and any(
        d in command for d in ("tests/integration", "tests/steps", "tests/e2e", "run_lane.py")
    )
    return (container_suite and "--matrix" not in command) or _RUNNER in command


def _lane_callers():
    return [(w, j, b) for w, j, b in _jobs() if b.get("uses") == LANE]


def test_every_job_that_runs_the_container_suite_or_playwright_is_the_lane_workflow():
    callers = {f"{w}:{j}" for w, j, _b in _lane_callers() if w != _LEAF_CHECK}
    assert callers == {
        "amd64-engines.yml:exasol",
        "integration-suite-lanes.yml:suite",
        # Maintainer 2026-10-29: the UI specs on live cloud warehouses run only here, nightly.
        "integration-suite-lanes.yml:warehouse-ui",
        "ui-e2e-core.yml:playwright",
        "ui-e2e-core.yml:provisioning",
        "ui-e2e-swap-amd64.yml:playwright",
        "ui-e2e-trino.yml:playwright",
    }
    for workflow, job, body in _jobs():
        if workflow == "lane.yml" or job in _OWN_STEPS.get(workflow, ()):
            continue
        for step in body.get("steps") or []:
            command = step.get("run") or ""
            if _IN_THE_GUEST in command:
                continue
            assert not _runs_a_lane(command), (
                f"{workflow}:{job} runs a lane with steps of its own: {step.get('name')}"
            )


def test_a_lane_caller_names_a_command_and_scripts_that_exist():
    for workflow, job, body in _lane_callers():
        given = body["with"]
        # The lane workflow's own self-test calls it with a command that only writes a file.
        assert workflow == _LEAF_CHECK or _runs_a_lane(str(given["run"])), (
            f"{workflow}:{job} runs no lane"
        )
        for hook in ("prepare", "on-failure", "after"):
            if hook in given:
                named = [w for w in str(given[hook]).split() if w.endswith((".sh", ".py"))][0]
                script = (REPO / named.removeprefix("../")).resolve()
                assert script.is_file(), f"{workflow}:{job} {hook}: {script} does not exist"
                assert script.parent == REPO / "scripts" / "ci" / "lanes"


@pytest.mark.parametrize(
    "script", sorted((REPO / "scripts" / "ci" / "lanes").glob("*.sh")), ids=lambda p: p.name
)
def test_a_lane_script_fails_loud(script):
    """Its first command is `set -euo pipefail`: no later command's failure, and no failing side
    of a pipe, is passed over."""
    commands = [
        line.strip()
        for line in script.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    assert commands[0] == "set -euo pipefail", script.name
    used = {
        word.removeprefix("../")
        for _w, _j, b in _lane_callers()
        # ``run``: the core UI job's opt-in py-spy recorder (ui-e2e-core.yml ``pyspy``).
        for h in ("prepare", "on-failure", "after", "run")
        for word in str(b["with"].get(h, "")).split()
        if word.endswith(".sh")
    }
    used |= {"scripts/ci/lanes/suite-prepare.sh"}  # also the warehouse and salesforce jobs' step
    # ... and what the lane workflow itself runs (the sampler).
    used |= set(
        re.findall(
            r"scripts/ci/lanes/[\w-]+\.sh", (REPO / ".github/workflows/lane.yml").read_text()
        )
    )
    assert str(script.relative_to(REPO)) in used, f"{script.name} is called by no lane"


# What the lane workflow may be handed, by caller. The four every lane kind may need, and the
# live sources' credentials, which ONLY the UI lanes pass: the live cloud warehouses' only the
# nightly warehouse UI job (maintainer 2026-10-29: the core UI lane never touches a live
# warehouse), the live APIs' and SharePoint's the core UI workflow.
_LANE_SECRETS = {
    "ANTHROPIC_API_KEY",
    "OPENROUTER_API_KEY",
    "SPLUNKBASE_USERNAME",
    "SPLUNKBASE_PASSWORD",
    "SP_CERT_P12_BASE64",
}
_WAREHOUSE_SECRETS = {
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE",
    "DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN",
    "GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS_JSON", "GSHEETS_TEST_SHEET_ID",
    "FABRIC_RESOURCE_GROUP", "FABRIC_CAPACITY_NAME",
    "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID",
}  # fmt: skip
_LIVE_SOURCE_SECRETS = _WAREHOUSE_SECRETS | {
    "SP_TENANT_ID", "SP_CLIENT_ID", "SP_SITE_URL", "SP_CERT_PASSWORD",
    "FREE_ASKAMERICA_KEY", "KAGGLE_API_TOKEN", "STRIPE_API_KEY",
    # SingleStore Cloud shared-tier workspace: a live source, not a warehouse (its spec is in the
    # core-provisioning project, source-to-query-community-ext.spec.ts).
    "SINGLESTORE_HOST", "SINGLESTORE_PORT", "SINGLESTORE_USERNAME", "SINGLESTORE_PASSWORD",
    "SINGLESTORE_DATABASE",
}  # fmt: skip
_WAREHOUSE_UI = ("integration-suite-lanes.yml", "warehouse-ui")


def test_the_lane_workflow_is_passed_its_secrets_by_name():
    """It declares each secret it can be given and inherits none; a lane job reads only what its
    caller passed. The live sources' credentials are passed by the UI lanes alone, and the live
    cloud warehouses' by the nightly warehouse UI job alone (maintainer 2026-10-29: the core UI
    lane never touches a live warehouse)."""
    path = REPO / ".github" / "workflows" / "lane.yml"
    lane = yaml.safe_load(path.read_text())
    declared = set(lane[True]["workflow_call"]["secrets"])
    assert declared == _LANE_SECRETS | _LIVE_SOURCE_SECRETS
    assert set(re.findall(r"secrets\.([A-Z0-9_]+)", path.read_text())) == declared
    assert "toJSON(secrets" not in path.read_text().replace(" ", "")
    for workflow, job, body in _lane_callers():
        passed = body.get("secrets", {})
        assert passed != "inherit", f"{workflow}:{job} hands the lane every secret"
        assert set(passed) <= declared, f"{workflow}:{job}"
        if (workflow, job) == _WAREHOUSE_UI:
            assert set(passed) == _WAREHOUSE_SECRETS, f"{workflow}:{job}"
        elif workflow == "ui-e2e-core.yml":
            # Maintainer 2026-10-29: no live warehouse's credential reaches the core UI lane.
            assert not set(passed) & _WAREHOUSE_SECRETS, f"{workflow}:{job}"
        else:
            assert not set(passed) & _LIVE_SOURCE_SECRETS, f"{workflow}:{job}"
    core_path = REPO / ".github/workflows/ui-e2e-core.yml"
    core = yaml.safe_load(core_path.read_text())["jobs"]
    passed = set(core["playwright"]["secrets"]) | set(core["provisioning"]["secrets"])
    assert _LIVE_SOURCE_SECRETS - _WAREHOUSE_SECRETS <= passed
    # ... nor is declared by it, or named anywhere in it (maintainer 2026-10-29).
    assert not set(re.findall(r"secrets\.([A-Z0-9_]+)", core_path.read_text())) & _WAREHOUSE_SECRETS
    # A secret reaches the lane's own commands and no other step: no action, no upload.
    steps = lane["jobs"]["lane"]["steps"]
    with_secrets = [
        s["name"] for s in steps if any("secrets." in str(v) for v in (s.get("env") or {}).values())
    ]
    assert with_secrets == ["Prepare", "Run lane", "After the lane"]
    assert "env" not in lane["jobs"]["lane"], "a job-level environment reaches every step"


def test_no_step_or_lane_script_prints_its_environment():
    """A secret in a step's environment must not reach the log: no command dumps the
    environment, traces its own expansion, or serialises the secrets context."""
    dump = re.compile(
        r"(^|[;&|]\s*|\$\()\s*(printenv|env)\s*($|[|>;&)])|^\s*set\s*$|\bset -[a-z]*x|"
        r"\bexport -p\b|\bdeclare -x\b|toJSON\(\s*(secrets|env)\b",
        re.M,
    )
    sources = [(where, step.get("run") or "") for where, step in _steps()]
    sources += [(str(p.relative_to(REPO)), p.read_text()) for p in ACTIONS]
    sources += [
        (str(p.relative_to(REPO)), p.read_text())
        for p in sorted((REPO / "scripts" / "ci" / "lanes").iterdir())
        if p.is_file()
    ]
    assert len(sources) > 100
    for where, text in sources:
        code = "\n".join(
            line.split(" #")[0] for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        found = dump.search(code)
        assert not found, f"{where}: {found.group(0)!r}"


def test_the_core_ui_lanes_credential_files_are_private_and_removed():
    def commands(name: str) -> str:
        text = (REPO / "scripts" / "ci" / "lanes" / name).read_text()
        return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

    prepare, after = commands("ui-core-prepare.sh"), commands("ui-core-after.sh")
    assert "umask 077" in prepare and "set +x" in prepare
    assert '"${RUNNER_TEMP:?}/lane-credentials"' in prepare
    # The secret is a variable's value written to a file, never a command's argument or output.
    for line in prepare.splitlines():
        if "_JSON" in line or "_BASE64" in line:
            assert line.strip().startswith(("if [ -n", "printf '%s' \"$")), line
    assert "az login" not in prepare and "az login" not in after
    assert 'rm -rf "${RUNNER_TEMP:?}/lane-credentials"' in after
    # Maintainer 2026-11-10 ("Paid cloud resources"): the Fabric capacity is the run's own --
    # made by the prepare script, deleted (and read back gone) by the after-script whatever the
    # lane's outcome.
    assert 'scripts/fabric_e2e.py up "$GITHUB_ENV"' in prepare
    assert "scripts/fabric_e2e.py down" in after and "suspend_capacity()" not in after
    # REQ-254: the warehouses the cloud-warehouse spec wakes are stopped again, whatever happened.
    assert "stop_warehouse" in after and "suspend_warehouse" in after
    assert "it may be billing" in after
    lane = yaml.safe_load((REPO / ".github/workflows/lane.yml").read_text())["jobs"]["lane"]
    after_step = next(s for s in lane["steps"] if s.get("name") == "After the lane")
    assert "always()" in after_step["if"]


def test_the_warehouse_lane_leaves_every_compute_it_runs_stopped_whatever_happened():
    """REQ-254: a test stops what it started, which does not run when the lane dies or the job is
    cut short -- so the warehouse job itself pauses or stops every billable compute it runs on,
    in steps that run whatever the outcome, and fails when one does not end stopped."""
    job = yaml.safe_load((REPO / ".github/workflows/integration-suite-lanes.yml").read_text())[
        "jobs"
    ]["warehouse"]
    steps = {s.get("name"): s for s in job["steps"]}
    run_lane = [s.get("name") for s in job["steps"]].index("Run lane")
    for name, call in (
        ("Delete this run's Fabric workspace and capacity", "scripts/fabric_e2e.py down"),
        ("Leave the Databricks warehouse stopped", "stop_warehouse"),
        ("Leave the Snowflake warehouse suspended", "suspend_warehouse"),
    ):
        step = steps[name]
        assert [s.get("name") for s in job["steps"]].index(name) > run_lane, name
        if name == "Delete this run's Fabric workspace and capacity":
            # Whenever the step that creates the capacity ran, whatever its outcome (below).
            assert step["if"] == _FABRIC_DOWN_IF, name
        else:
            assert step["if"] == "always()", name
        assert call in step["run"] and "exit 1" in step["run"], name


def test_the_warehouse_lane_creates_its_own_fabric_capacity_before_any_test():
    """Maintainer 2026-11-10 ("Paid cloud resources"): no standing Fabric capacity. The warehouse
    job creates the run's own before the lane runs; the step after it deletes it (above)."""
    job = yaml.safe_load((REPO / ".github/workflows/integration-suite-lanes.yml").read_text())[
        "jobs"
    ]["warehouse"]
    names = [s.get("name") for s in job["steps"]]
    up = names.index("This run's Fabric capacity, workspace and warehouse")
    assert up < names.index("Run lane")
    assert 'scripts/fabric_e2e.py up "$GITHUB_ENV"' in job["steps"][up]["run"]


# Maintainer 2026-11-21: the Fabric workspace and warehouse are the run's own. Their endpoint,
# database and workspace id are written by ``scripts/fabric_e2e.py up`` to $GITHUB_ENV, never read
# from a standing secret.
_FABRIC_RUN_OUTPUTS = ("FABRIC_SQL_SERVER", "FABRIC_DATABASE", "FABRIC_WORKSPACE_ID")


def test_fabric_endpoint_database_and_workspace_are_run_outputs_not_secrets():
    """No workflow passes, declares or reads a standing Fabric workspace, warehouse or endpoint,
    and no step's own environment sets those names: a step-level value would hide the one
    ``up`` wrote to $GITHUB_ENV."""
    for path in sorted((REPO / ".github" / "workflows").glob("*.yml")):
        text = path.read_text()
        for name in _FABRIC_RUN_OUTPUTS:
            assert f"secrets.{name}" not in text, f"{path.name}: secrets.{name}"
    for where, step in _steps():
        assert not set(step.get("env") or {}) & set(_FABRIC_RUN_OUTPUTS), where
    for workflow in ("lane.yml", "integration-suite-lanes.yml"):
        doc = yaml.safe_load((REPO / ".github" / "workflows" / workflow).read_text())
        declared = set(doc[True]["workflow_call"].get("secrets") or {})
        assert not declared & set(_FABRIC_RUN_OUTPUTS), workflow
        for job in doc["jobs"].values():
            assert not set(job.get("env") or {}) & set(_FABRIC_RUN_OUTPUTS), workflow


# The delete step's condition: it runs whenever the create step ran, and only then.
_FABRIC_DOWN_IF = (
    'always() && contains(fromJSON(\'["success", "failure", "cancelled"]\'), '
    "steps.fabric-capacity.outcome)"
)


def _fabric_test_files() -> set[str]:
    """The integration test files that reach Fabric: they resume its capacity or connect to its
    SQL endpoint, so each must carry requires_fabric."""
    return {
        str(p.relative_to(REPO))
        for p in (REPO / "tests/integration").glob("test_*.py")
        if re.search(
            r"fabric_capacity|FABRIC_SQL_SERVER|FABRIC_CAPACITY|FABRIC_WORKSPACE", p.read_text()
        )
    }


def _fabric_up_runs(condition: str, tests: str) -> bool:
    """The create step's ``if:`` evaluated for a run's ``inputs.tests``. The condition may only be
    ``inputs.tests == ''`` or'd with ``contains(inputs.tests, '<file>')`` clauses; anything else
    fails here rather than being guessed at."""
    runs = False
    for clause in (c.strip() for c in condition.split("||")):
        if clause == "inputs.tests == ''":
            runs = runs or tests == ""
            continue
        named = re.fullmatch(r"contains\(inputs\.tests, '([^']+)'\)", clause)
        assert named, f"unexpected clause in the Fabric capacity step's condition: {clause!r}"
        runs = runs or named.group(1) in tests
    return runs


def test_the_warehouse_lane_creates_a_fabric_capacity_only_for_a_run_selecting_a_fabric_test():
    """REQ-254: a lane run that selects no Fabric test creates no capacity. The whole lane (no
    inputs.tests) still makes it; a targeted run makes it only when it names a test file that
    reaches Fabric -- every such file, so none runs without one. The delete step runs whenever
    the create step ran, whatever happened, and is skipped with it."""
    job = yaml.safe_load((REPO / ".github/workflows/integration-suite-lanes.yml").read_text())[
        "jobs"
    ]["warehouse"]
    steps = {s.get("name"): s for s in job["steps"]}
    up = steps["This run's Fabric capacity, workspace and warehouse"]
    assert up["id"] == "fabric-capacity"
    condition = up["if"]
    fabric = _fabric_test_files()
    assert fabric, "no integration test reaches Fabric: the condition below would be vacuous"
    assert set(re.findall(r"contains\(inputs\.tests, '([^']+)'\)", condition)) == fabric

    assert _fabric_up_runs(condition, "")
    for f in sorted(fabric):
        assert _fabric_up_runs(condition, f), f
        assert _fabric_up_runs(
            condition, f"tests/integration/test_google_sheets_source_e2e.py {f}::test_x"
        ), f
    # Run 42930869189: a targeted Google Sheets run made (and paid for) a capacity it never used.
    assert not _fabric_up_runs(condition, "tests/integration/test_google_sheets_source_e2e.py")
    assert not _fabric_up_runs(
        condition, "tests/integration/test_metadata_export_purview_e2e.py::test_x"
    )

    assert steps["Delete this run's Fabric workspace and capacity"]["if"] == _FABRIC_DOWN_IF


def test_every_integration_file_reaching_fabric_carries_requires_fabric():
    fabric = _fabric_test_files()
    assert fabric, "no integration test reaches Fabric: the check below would be vacuous"
    for f in sorted(fabric):
        assert "pytest.mark.requires_fabric" in (REPO / f).read_text(), f


def test_the_warehouse_lane_sets_snowflake_auto_suspend_before_any_test():
    """Maintainer 2026-10-29: CI must not run up the warehouse bill. Before any test runs, the
    Snowflake warehouse is set to suspend after 60 idle seconds (a run that dies before its
    teardown cannot leave it billing). No resource monitor: creating one needs ACCOUNTADMIN, which
    the CI user lacks (maintainer 2026-10-29), so the lane never tries."""
    text = (REPO / ".github/workflows/integration-suite-lanes.yml").read_text()
    job = yaml.safe_load(text)["jobs"]["warehouse"]
    names = [s.get("name") for s in job["steps"]]
    steps = {s.get("name"): s for s in job["steps"]}
    auto = steps["Snowflake warehouse suspends after 60 idle seconds"]
    assert names.index(auto["name"]) < names.index("Run lane")
    assert "if" not in auto
    assert "set_auto_suspend" in auto["run"] and "['SNOWFLAKE_WAREHOUSE'], 60)" in auto["run"]
    assert "RESOURCE MONITOR" not in text.upper()
    # The warehouse the steps act on is the secret's, with no default in its place.
    for name in (auto["name"], "Leave the Snowflake warehouse suspended"):
        assert "os.environ['SNOWFLAKE_WAREHOUSE']" in steps[name]["run"], name
        assert "COMPUTE_WH" not in steps[name]["run"], name


def test_the_ui_specs_on_live_warehouses_run_only_in_the_nightly_warehouse_lane():
    """Maintainer 2026-10-29: pushes run only unit tests, lint and requirements checks; the UI
    core lane never touches a live warehouse; the warehouse specs are the Playwright project
    "warehouse", which only the warehouse lane's UI job runs, on the nightly schedule or a
    dispatch that includes the warehouse lane."""
    config = (REPO / "provisa-ui" / "playwright.config.ts").read_text()
    assert 'const WAREHOUSE_SPECS = ["**/source-to-query-cloud-warehouse.spec.ts"];' in config
    core = config[config.index('name: "core",') :]
    core = core[: core.index("},")]
    assert "...WAREHOUSE_SPECS," in core
    assert "grepInvert: [WAREHOUSE_TITLES, ASKAMERICA_TITLES]" in core
    assert 'name: "warehouse",' in config
    workflows = REPO / ".github" / "workflows"
    for path in sorted(workflows.glob("*.yml")):
        if "--project=warehouse" in path.read_text():
            assert path.name == _WAREHOUSE_UI[0], path.name
    lanes = yaml.safe_load((workflows / _WAREHOUSE_UI[0]).read_text())
    job = lanes["jobs"][_WAREHOUSE_UI[1]]
    assert "--project=warehouse" in job["with"]["run"]
    assert job["if"] == lanes["jobs"]["warehouse"]["if"].replace(
        "needs.plan.outputs.warehouse == 'true'", "inputs.tests == ''"
    )
    assert "ui-core-after.sh" in job["with"]["after"]
    # The lanes workflow is started by the nightly schedule and on request (a dispatch on a
    # release branch gates it); never by a push.
    suite = yaml.safe_load((workflows / "integration-suite.yml").read_text())
    assert set(suite[True]) == {"schedule", "workflow_dispatch"}
    core_ui = yaml.safe_load((workflows / "ui-e2e-core.yml").read_text())
    assert set(core_ui[True]) == {"workflow_dispatch", "workflow_call"}
    for name in ("playwright", "provisioning"):
        assert "--project=core" in core_ui["jobs"][name]["with"]["run"], name
        assert "--project=warehouse" not in core_ui["jobs"][name]["with"]["run"], name


# The calls that stop the live compute every run shares (tests/integration/databricks_warehouse.py,
# snowflake_warehouse.py, fabric_capacity.py), and the credentials that reach it.
_SHARED_COMPUTE_STOPS = (
    "stop_warehouse",
    "suspend_warehouse",
    "ensure_capacity_paused",
    "scripts/fabric_e2e.py down",
)
_SHARED_COMPUTE_SECRETS = ("DATABRICKS_TOKEN", "SNOWFLAKE_PASSWORD", "FABRIC_CAPACITY_NAME")


def _stops_shared_compute(body: dict) -> bool:
    """A job that stops the shared live compute: in a step of its own, or through the `after`
    script of the lane workflow it calls, when it is passed the credentials that reach it."""
    if any(
        call in (step.get("run") or "")
        for step in body.get("steps") or []
        for call in _SHARED_COMPUTE_STOPS
    ):
        return True
    after = (body.get("with") or {}).get("after") or ""
    script = re.search(r"scripts/ci/lanes/[\w.-]+\.sh", after)
    if script is None:
        return False
    stops = any(call in (REPO / script.group(0)).read_text() for call in _SHARED_COMPUTE_STOPS)
    passed = body.get("secrets") or {}
    return stops and any(passed.get(name) for name in _SHARED_COMPUTE_SECRETS)


def test_jobs_that_stop_shared_live_compute_never_run_side_by_side():
    """REQ-254. Two runs on the same live warehouse fought: one run's stop took it from under the
    other's tests, and the other's queries restarted it, so the stop failed (run 40861830409's
    after-script: "RUNNING after 600s, not STOPPED" while run 40861827715's warehouse lane used
    it). Every job that stops the shared compute takes one repo-wide concurrency group that waits
    and never cancels (a cancelled run would still hold the compute it woke until its own stop),
    and keeps every waiting run."""
    stoppers = {
        f"{workflow}:{job}": body for workflow, job, body in _jobs() if _stops_shared_compute(body)
    }
    # Maintainer 2026-10-29: the core UI lane (ui-e2e-core.yml) is passed no warehouse's
    # credentials and stops nothing; the nightly warehouse UI job took its place.
    assert set(stoppers) == {
        "integration-suite-lanes.yml:warehouse",
        "integration-suite-lanes.yml:warehouse-ui",
    }
    for where, body in stoppers.items():
        group = body.get("concurrency")
        assert isinstance(group, dict), f"{where} takes no concurrency group"
        assert group.get("group") == "live-cloud-warehouses", where
        assert group.get("cancel-in-progress") in (None, False), where
        assert group.get("queue") == "max", where
    # Both core UI jobs run the same after-script, but are passed none of the compute's
    # credentials (maintainer 2026-10-29), so they stop nothing and run beside the others.
    core = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())["jobs"]
    for job in ("playwright", "provisioning"):
        assert "ui-core-after.sh" in core[job]["with"]["after"], job
        assert not _stops_shared_compute(core[job]), job
        assert "concurrency" not in core[job], job


def test_no_two_lanes_share_a_name_or_an_artifact():
    names = [b["with"]["name"] for _w, _j, b in _lane_callers()]
    assert len(names) == len(set(names)), names
    artifacts = [b["with"].get("artifact-name") for _w, _j, b in _lane_callers()]
    explicit = [a for a in artifacts if a]
    assert len(explicit) == len(set(explicit)), explicit


def test_reusable_workflows_nest_no_deeper_than_github_allows():
    calls = {}
    for workflow, _job, body in _jobs():
        target = body.get("uses") or ""
        if target.startswith("./.github/workflows/"):
            calls.setdefault(workflow, set()).add(target.rsplit("/", 1)[-1])

    def depth(workflow: str, seen: tuple = ()) -> int:
        assert workflow not in seen, f"cycle through {workflow}"
        return 1 + max((depth(c, (*seen, workflow)) for c in calls.get(workflow, ())), default=0)

    # A caller and the workflows nested under it: four levels in all.
    assert max(depth(w) for w in calls) <= 4


# --- the release: an orchestrator and its legs (stage 3) ------------------------------------------

_RELEASE = REPO / ".github" / "workflows" / "build-dmg.yml"
_LEAVES_THE_RUN = re.compile(
    r"gh release (upload|edit|create|view)|softprops/action-gh-release|pypi-publish|"
    r"attest-build-provenance|docker/login-action|\./\.github/actions/ensure-release|twine upload|"
    r"\./\.github/actions/attach-release-assets|"
    r"docker push|oras push|skopeo copy|crane push|provisa connector push|--registry-password-env|"
    r"actions/deploy-pages"
)


#: Nothing in the release goes to PyPI (maintainer 2026-11-21): the server, client, notebook and SDK
#: wheels are release assets, installed through the GitHub Pages index the publish leg writes.
_RELEASE_PYPI: tuple[str, ...] = ()
#: REQ-2225: the provisa wheel carries the platform connector block, so the release that attaches
#: it waits for the image the block names, which the orchestrator pushes itself, after every leg.
_RELEASE_IMAGE_PUSH = "publish-connector-image"
_RELEASE_PYPI_AFTER: dict[str, set[str]] = {}
#: Every orchestrator job that is not a leg.
_RELEASE_OWN = (*_RELEASE_PYPI, _RELEASE_IMAGE_PUSH)
_RELEASE_PUBLISH = (
    "${{ inputs.publish == true && github.ref_type == 'tag' && github.ref_name == inputs.version }}"
)


def _release_legs() -> dict[str, tuple[str, dict]]:
    """{orchestrator job: (leg file, parsed leg)}."""
    jobs = yaml.safe_load(_RELEASE.read_text())["jobs"]
    legs = {}
    for job, body in jobs.items():
        if job in _RELEASE_OWN:
            continue
        assert body["uses"].startswith("./.github/workflows/release-"), f"{job} is not a leg"
        name = body["uses"].rsplit("/", 1)[-1]
        legs[job] = (name, yaml.safe_load((REPO / ".github" / "workflows" / name).read_text()))
    return legs


def test_the_release_orchestrator_calls_legs_and_pushes_the_connector_image_itself():
    jobs = yaml.safe_load(_RELEASE.read_text())["jobs"]
    image_push = jobs.pop(_RELEASE_IMAGE_PUSH)
    given = {job: body["with"] for job, body in jobs.items()}
    first = next(iter(given.values()))
    assert set(first) == {"version", "publish"}
    assert all(w == first for w in given.values()), "the legs are not built for one version"
    assert first["version"] == "${{ inputs.version || github.ref_name }}"
    # A run publishes only when started on the tag it names; a pushed tag starts nothing.
    assert first["publish"] == _RELEASE_PUBLISH
    trigger = yaml.safe_load(_RELEASE.read_text())[True]
    assert "push" not in trigger
    assert trigger["workflow_dispatch"]["inputs"]["publish"]["default"] is False
    for body in jobs.values():
        assert "steps" not in body and body.get("secrets") != "inherit"
    builds = set(jobs) - {"publish"}
    assert not any("pypi" in job for job in jobs)
    # The connector image's push: gated the same way, after every build leg, holding only what a
    # push to the registry needs.
    assert image_push["if"] == _RELEASE_PUBLISH
    assert set(image_push["needs"]) == builds
    assert image_push["permissions"] == {"contents": "read", "packages": "write"}
    assert set(jobs["publish"]["needs"]) == builds | {_RELEASE_IMAGE_PUSH}


def test_every_step_of_the_release_that_leaves_the_run_waits_for_publish():
    """A dry run builds every artifact and publishes nothing: each step that creates or changes
    the release, uploads to PyPI, signs an attestation or logs in to push an image carries
    `if: inputs.publish` (or its whole job does), and an image build pushes only on publish."""
    seen = 0
    for _job, (name, leg) in _release_legs().items():
        for job, body in leg["jobs"].items():
            job_gated = body.get("if") == "inputs.publish"
            for step in body.get("steps") or []:
                where = f"{name}:{job}:{step.get('name')}"
                text = f"{step.get('uses') or ''}\n{step.get('run') or ''}"
                if _LEAVES_THE_RUN.search(text):
                    seen += 1
                    assert job_gated or step.get("if") == "inputs.publish", where
                push = (step.get("with") or {}).get("push")
                if push is not None:
                    seen += 1
                    assert job_gated or push == "${{ inputs.publish }}", where
    # The connector image's push (REQ-2225) is the orchestrator's own job, gated as a whole.
    for job, body in yaml.safe_load(_RELEASE.read_text())["jobs"].items():
        if job == _RELEASE_IMAGE_PUSH:
            assert body["if"] == _RELEASE_PUBLISH, job
            seen += sum(bool(_LEAVES_THE_RUN.search(str(s.get("run", "")))) for s in body["steps"])
    assert seen >= 16, f"only {seen} publishing steps found: the pattern no longer matches them"


def test_no_leg_of_the_release_reads_the_pushed_ref():
    """A leg is built for the `version` it is given -- the same on a tag, and the only version
    there is on a dry run, where the ref is a branch."""
    for _job, (name, _leg) in _release_legs().items():
        text = (REPO / ".github" / "workflows" / name).read_text()
        assert "github.ref_name" not in text and "GITHUB_REF_NAME" not in text, name
        assert "github.ref " not in text and "github.ref}" not in text, name
    metadata = (REPO / ".github" / "actions" / "release-metadata" / "action.yml").read_text()
    assert "GITHUB_REF" not in metadata and "github.ref" not in metadata


def test_every_asset_name_a_leg_uses_is_one_the_metadata_action_resolves():
    action = yaml.safe_load(
        (REPO / ".github" / "actions" / "release-metadata" / "action.yml").read_text()
    )
    resolved = set(action["outputs"])
    for _job, (name, leg) in _release_legs().items():
        text = (REPO / ".github" / "workflows" / name).read_text()
        used = set(re.findall(r"needs\.metadata\.outputs\.(\w+)", text))
        if not used:
            continue
        declared = leg["jobs"]["metadata"]["outputs"]
        assert used <= set(declared) <= resolved, name
        for output, value in declared.items():
            assert value == f"${{{{ steps.meta.outputs.{output} }}}}"
        step = leg["jobs"]["metadata"]["steps"][-1]
        assert step["uses"] == "./.github/actions/release-metadata"
        assert step["with"] == {"version": "${{ inputs.version }}"}


_INPUT_REF = re.compile(r"\$\{\{\s*inputs\.([\w-]+)\s*\}\}")
_INPUT_TEST = re.compile(r"^\(?\s*inputs\.([\w-]+)\s*(==|!=)\s*'([^']*)'\s*\)?$")


def _effective_steps(steps: list[dict]) -> list[dict]:
    """A job's steps with each local composite action (`uses: ./.github/actions/<name>`) replaced
    by the steps of its action.yml that its inputs let run. A conjunct of a step's `if` that tests
    an input against a literal is decided from the caller's `with:` (an input it does not pass
    takes the action's default, and GitHub gives an input with neither the empty string); any
    other conjunct depends on the run and is taken as possibly true. `${{ inputs.<name> }}` in a
    step's `with:` is replaced the same way, so an artifact name comes out as the caller sees it."""
    out: list[dict] = []
    for step in steps:
        uses = step.get("uses") or ""
        if not uses.startswith("./.github/actions/"):
            out.append(step)
            continue
        action = yaml.safe_load((REPO / uses / "action.yml").read_text())
        if action["runs"]["using"] != "composite":
            out.append(step)
            continue
        declared = action.get("inputs") or {}
        given = step.get("with") or {}
        for name in given:
            assert name in declared, f"{uses} takes no input {name!r}"
        # GitHub's own rule: an input neither passed nor defaulted reads as ''.
        values = {
            name: str(given.get(name, spec.get("default", ""))) for name, spec in declared.items()
        }

        def runs(condition: object) -> bool:
            for conjunct in str(condition).split("&&"):
                test = _INPUT_TEST.match(conjunct.strip())
                if test is None:
                    continue
                name, op, literal = test.groups()
                assert name in values, f"{uses} tests undeclared input {name!r}"
                if (values[name] == literal) != (op == "=="):
                    return False
            return True

        inner = []
        for sub in action["runs"]["steps"]:
            if "if" in sub and not runs(sub["if"]):
                continue
            with_ = {
                key: _INPUT_REF.sub(lambda m: values[m.group(1)], val)
                if isinstance(val, str)
                else val
                for key, val in (sub.get("with") or {}).items()
            }
            inner.append({**sub, "with": with_})
        out.extend(_effective_steps(inner))
    return out


def test_composite_action_steps_follow_the_callers_inputs():
    """The trino-plugins action uploads its artifact only in the prime phase and downloads it only
    in the restore phase, under the name the caller's key-prefix gives it."""
    step = {"uses": "./.github/actions/trino-plugins"}

    def artifacts(inputs: dict, kind: str) -> list[str]:
        return [
            s["with"]["name"]
            for s in _effective_steps([{**step, "with": inputs}])
            if kind in (s.get("uses") or "")
        ]

    assert artifacts({"phase": "prime"}, "upload-artifact") == ["trino-plugins"]
    assert artifacts({"phase": "prime"}, "download-artifact") == []
    assert artifacts({"phase": "restore"}, "upload-artifact") == []
    assert artifacts({"phase": "restore"}, "download-artifact") == ["trino-plugins"]
    assert artifacts({"phase": "prime", "key-prefix": "x-"}, "upload-artifact") == [
        "x-trino-plugins"
    ]


# The artifact each caching action's `phase: prime` uploads, under the caller's key-prefix.
_PRIMED_ARTIFACT = {
    "./.github/actions/trino-plugins": "trino-plugins",
    "./.github/actions/zaychik-image": "zaychik-image",
    "./.github/actions/duckdb-ext-wheels": "duckdb-ext-wheels",
}


def _primed_upload(step: dict) -> str | None:
    """The artifact a caching action step uploads: its prime phase uploads the run's artifact
    under the caller's key-prefix; any other step of it uploads none."""
    given = step.get("with") or {}
    if step.get("uses") not in _PRIMED_ARTIFACT or given.get("phase") != "prime":
        return None
    return given.get("key-prefix", "") + _PRIMED_ARTIFACT[step["uses"]]


def test_a_leg_that_reads_another_legs_artifact_starts_after_it():
    """Artifacts are the run's: a leg downloads what another leg uploaded, so the orchestrator
    must order them. Every named download in a leg is uploaded in that leg or in one it needs. A
    step that uses a local composite action uploads and downloads what that action's steps do for
    the inputs it passes, and a job that calls a local workflow what that workflow's jobs do (the
    dry run's duckdb-extensions job uploads the wheels its image builds read)."""
    orchestrator = yaml.safe_load(_RELEASE.read_text())["jobs"]

    def per_matrix_entry(body: dict) -> list[dict]:
        entries = ((body.get("strategy") or {}).get("matrix") or {}).get("include") or [{}]

        def named(step: dict, entry: dict) -> dict:
            with_ = dict(step.get("with") or {})
            if isinstance(with_.get("name"), str):
                for key, value in entry.items():
                    with_["name"] = with_["name"].replace(f"${{{{ matrix.{key} }}}}", str(value))
            return {**step, "with": with_}

        return [
            {**body, "steps": [named(st, entry) for st in body.get("steps") or []]}
            for entry in entries
        ]

    def expanded(body: dict) -> list[dict]:
        called = str(body.get("uses") or "")
        if not called.startswith("./.github/workflows/"):
            return [body]
        inner = yaml.safe_load((REPO / called).read_text())["jobs"].values()
        return [b for job in inner for m in per_matrix_entry(job) for b in expanded(m)]

    def needed(job: str) -> set[str]:
        direct = orchestrator[job].get("needs") or []
        direct = [direct] if isinstance(direct, str) else direct
        return set(direct).union(*(needed(d) for d in direct)) if direct else set()

    uploads: dict[str, set[str]] = {}
    downloads: dict[str, set[str]] = {}
    jobs = {job: [body] for job, body in orchestrator.items() if job in _RELEASE_OWN}
    jobs.update({job: list(leg["jobs"].values()) for job, (_n, leg) in _release_legs().items()})
    for job, bodies in jobs.items():
        for body in (b for top in bodies for b in expanded(top)):
            for step in _effective_steps(body.get("steps") or []):
                artifact = (step.get("with") or {}).get("name")
                if not artifact:
                    continue
                if "upload-artifact" in (step.get("uses") or ""):
                    uploads.setdefault(job, set()).add(artifact)
                if "download-artifact" in (step.get("uses") or ""):
                    downloads.setdefault(job, set()).add(artifact)
    crossed = 0
    for job, wanted in downloads.items():
        for artifact in wanted - uploads.get(job, set()):
            source = [other for other, made in uploads.items() if artifact in made]
            assert len(source) == 1, f"{job} downloads {artifact!r}, uploaded by {source}"
            assert source[0] in needed(job), (
                f"{job} reads {artifact!r} of {source[0]} without needing it"
            )
            crossed += 1
    assert crossed >= 5, crossed


def test_a_leg_is_passed_its_secrets_by_name():
    orchestrator = yaml.safe_load(_RELEASE.read_text())["jobs"]
    for job, (name, leg) in _release_legs().items():
        text = (REPO / ".github" / "workflows" / name).read_text()
        read = set(re.findall(r"secrets\.([A-Z0-9_]+)", text)) - {"GITHUB_TOKEN"}
        declared = set((leg[True]["workflow_call"].get("secrets") or {}))
        assert read == declared, f"{name} reads {read ^ declared} without declaring it"
        assert set(orchestrator[job].get("secrets") or {}) == declared, job


def test_the_sampler_shows_process_names_and_arguments_and_no_credential():
    """The runner's samples are uploaded: a process is shown by name and arguments, never by its
    environment, and an argument that names a password, token, secret or key loses its value."""
    import subprocess

    script = (REPO / "scripts" / "ci" / "lanes" / "sampler.sh").read_text()
    commands = "\n".join(
        line for line in script.splitlines() if line.strip() and not line.lstrip().startswith("#")
    )
    # A process's arguments are read from /proc/<pid>/cmdline, and `ps` is asked for no column
    # beyond pid and memory (no `e` modifier): nothing reads a process's environment.
    (asked,) = re.findall(r"ps (-\S+ \S+)", commands)
    assert asked == "-eo pid=,pmem=,rss="
    assert "/proc/$1/cmdline" in commands
    assert "environ" not in commands and "printenv" not in commands
    redact = re.search(r"sed -E '(s/.*?/Ig)'", script).group(1).removesuffix("Ig") + "g"
    shown = subprocess.run(  # noqa: S603 -- the script's own expression, on made-up lines
        ["perl", "-lpe", redact.replace("[= ]", "[= ]") + "i"],
        input=(
            "redis-server --requirepass hunter2 --port 6379\n"
            "psql --password=abc123 -h db\n"
            "tool --api-key sk-12345 --TOKEN=xyz\n"
            "uvicorn main:app --host 0.0.0.0 --port 3900\n"
        ),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert shown == [
        "redis-server --requirepass <redacted> --port 6379",
        "psql --password=<redacted> -h db",
        "tool --api-key <redacted> --TOKEN=<redacted>",
        "uvicorn main:app --host 0.0.0.0 --port 3900",
    ]


def test_a_test_that_did_not_run_is_named_and_counted_as_not_executed(
    tmp_path, monkeypatch, capsys
):
    """Skipped by its own condition, or never started after an earlier failure in a serial group:
    each is listed by name with its reason, under a count of its own -- never among the passed."""
    import importlib.util
    import json

    spec = importlib.util.spec_from_file_location(
        "playwright_not_executed", REPO / "scripts" / "ci" / "lanes" / "playwright_not_executed.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def test(status, results, reason=None):
        annotations = [{"type": "skip", "description": reason}] if reason else []
        return {"status": status, "results": results, "annotations": annotations}

    report = {
        "suites": [
            {
                "title": "kaggle-source.spec.ts",
                "file": "kaggle-source.spec.ts",
                "specs": [
                    {"title": "passes", "file": "kaggle-source.spec.ts",
                     "tests": [test("expected", [{"status": "passed"}])]},
                    {"title": "fails", "file": "kaggle-source.spec.ts",
                     "tests": [test("unexpected", [{"status": "failed"}, {"status": "failed"}])]},
                ],
                "suites": [
                    {
                        "title": "live Kaggle API",
                        "specs": [
                            {"title": "single-file dataset", "file": "kaggle-source.spec.ts",
                             "tests": [test("skipped", [{"status": "skipped"}],
                                            "KAGGLE_API_TOKEN not set in the environment")]},
                            {"title": "after the failure", "file": "kaggle-source.spec.ts",
                             "tests": [test("skipped", [])]},
                        ],
                    }
                ],
            }
        ]
    }  # fmt: skip
    path = tmp_path / "results.json"
    path.write_text(json.dumps(report))
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    assert module.main(["x", str(path)]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "NOT EXECUTED: 2 test(s)",
        "- kaggle-source.spec.ts › live Kaggle API › after the failure -- did not run",
        "- kaggle-source.spec.ts › live Kaggle API › single-file dataset -- "
        "KAGGLE_API_TOKEN not set in the environment",
    ]
    assert "### Not executed: 2" in summary.read_text()
    assert "not counted as passed" in summary.read_text()
    # A report that is not there is an error, not an empty list.
    with pytest.raises(FileNotFoundError):
        module.main(["x", str(tmp_path / "absent.json")])


def test_the_core_ui_jobs_report_what_did_not_execute_and_sample_the_runner():
    jobs = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())["jobs"]
    for job in ("playwright", "provisioning"):
        given = jobs[job]["with"]
        # The line reporter prints each failure as it fails: a runner stopped early still leaves
        # every failure it reached in the log.
        assert "--reporter=line,html,json" in given["run"]
        # The run stops itself inside the job's bound, so a slow run still leaves its reports: a
        # run killed by the job timeout writes none (release .479's core lane).
        limit = re.search(r"--global-timeout=(\d+)", given["run"])
        assert limit is not None, job
        assert 15 <= int(given["timeout"]) - int(limit.group(1)) / 60000 <= 30, job
        assert "PLAYWRIGHT_JSON_OUTPUT_NAME=playwright-results.json" in given["run"]
        assert given["after"] == "bash ../scripts/ci/lanes/ui-core-after.sh playwright-results.json"
        assert given["prepare"] == "scripts/ci/lanes/ui-core-prepare.sh"
        assert given["sampler"] is True
        assert "provisa-ui/playwright-results.json" in given["artifact-path"]


# --- the leaves' own check ------------------------------------------------------------------------


def _leaf_check() -> tuple[str, dict]:
    path = REPO / ".github" / "workflows" / _LEAF_CHECK
    return path.read_text(), yaml.safe_load(path.read_text())


def test_the_leaf_check_runs_only_on_ci_branches_and_never_on_main_or_a_tag():
    text, workflow = _leaf_check()
    trigger = workflow[True]
    assert set(trigger) == {"push", "workflow_dispatch"}
    assert trigger["push"] == {"branches": ["ci/**"], "paths": [".github/**", "scripts/ci/**"]}
    assert "tags" not in trigger["push"]
    jobs = workflow["jobs"]
    # A manual run started on any other ref does nothing: `scope` runs only on a ci/** branch
    # and every other job needs it, directly or through a job that does.
    assert jobs["scope"]["if"] == "startsWith(github.ref, 'refs/heads/ci/')"

    def reaches_scope(job: str) -> bool:
        needs = jobs[job].get("needs") or []
        needs = [needs] if isinstance(needs, str) else needs
        return "scope" in needs or any(reaches_scope(n) for n in needs)

    for job in jobs:
        assert job == "scope" or reaches_scope(job), f"{job} can run without scope"
    # The one job that runs `always()` says the branch condition itself.
    assert "always()" in jobs["verdict"]["if"]
    assert "startsWith(github.ref, 'refs/heads/ci/')" in jobs["verdict"]["if"]
    assert set(jobs["verdict"]["needs"]) == set(jobs) - {"verdict"}


def _needs(body: dict) -> list[str]:
    needs = body.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def test_the_leaf_check_expects_each_job_as_its_own_condition_allows_on_a_ci_branch():
    """The verdict runs only on a ci/** branch. A job whose condition needs a tag is skipped
    there, and the verdict expects it skipped; every other job it expects to succeed. A job
    downstream of a skipped one runs only with a condition of its own: under GitHub's implicit
    success() a skipped job anywhere upstream skips it (release-results and level-1 were, and
    the verdict failed every push)."""
    text, workflow = _leaf_check()
    jobs = workflow["jobs"]
    on_tag = {
        job for job, body in jobs.items() if "github.ref_type == 'tag'" in str(body.get("if"))
    }
    assert on_tag == {"release-publication"}, on_tag
    verdict = jobs["verdict"]["steps"][0]["run"]
    said = set(re.findall(r'expected\["([\w-]+)"\] = "skipped"', verdict))
    assert said == on_tag, said

    def below(job: str) -> bool:
        return any(n in on_tag or below(n) for n in _needs(jobs[job]))

    for job, body in jobs.items():
        if job == "verdict" or job in on_tag or not below(job):
            continue
        assert "if" in body, f"{job} is downstream of a job skipped on a branch and has no if"


def test_the_leaf_check_reads_no_secret_and_can_publish_nothing():
    for name in (_LEAF_CHECK, "ci-leaf-stub-leg.yml"):
        path = REPO / ".github" / "workflows" / name
        workflow = yaml.safe_load(path.read_text())
        # Its token reads the repository and nothing else, for every job.
        assert workflow["permissions"] == {"contents": "read"}
        for job, body in workflow["jobs"].items():
            read = set(re.findall(r"secrets\.([A-Za-z0-9_]+)", yaml.safe_dump(body)))
            assert not read, f"{name}:{job} reads {sorted(read)}"
            # What the lane's self-test passes as a secret is a made-up literal (to show an
            # expression over the matrix reaches one lane), never a repository secret.
            passed = body.get("secrets") or {}
            assert not [k for k, v in passed.items() if "secrets." in str(v)], f"{name}:{job}"
            assert "permissions" not in body, f"{name}:{job} widens its token"
            # No leg of the release is called, and no step is one that publishes.
            assert not str(body.get("uses", "")).startswith("./.github/workflows/release-")
            for step in body.get("steps") or []:
                step_text = f"{step.get('uses') or ''}\n{step.get('run') or ''}"
                assert not re.search(
                    r"pypi-publish|action-gh-release|gh release (create|upload|edit)|docker push|"
                    r"docker/login-action|attest-build-provenance",
                    step_text,
                ), f"{name}:{job}:{step.get('name')}"
                assert (step.get("with") or {}).get("push") in (None, False)
                # ensure-release, the one action here that could create a release, is skipped.
                if "ensure-release" in str(step.get("uses", "")):
                    assert step["if"] == "${{ fromJSON('false') }}"
    # The proof scaffolding is gone (it was removed when the refactor reached main): no job
    # calls a real caller workflow -- only the lane workflow and the stand-in leg -- and the file
    # that chose a proof level does not exist.
    jobs = yaml.safe_load((REPO / ".github" / "workflows" / _LEAF_CHECK).read_text())["jobs"]
    for job, body in jobs.items():
        if "uses" in body:
            assert body["uses"] in (LANE, "./.github/workflows/ci-leaf-stub-leg.yml"), job
    assert not (REPO / ".github" / "ci-proof-level").exists()


def test_no_workflow_inherits_secrets_or_serialises_them():
    """Every reusable workflow declares the secrets it can be given and is passed them by name.
    None is handed everything (`secrets: inherit`), and none turns the secrets context into
    text."""
    for path in WORKFLOWS:
        text = path.read_text()
        code = "\n".join(
            line.split(" #")[0] for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        assert "secrets: inherit" not in code, path.name
        assert "toJSON(secrets" not in code.replace(" ", ""), path.name
        workflow = yaml.safe_load(text)
        call = (workflow.get(True) or {}).get("workflow_call")
        if call is None and "workflow_call" not in (workflow.get(True) or {}):
            continue
        declared = set((call or {}).get("secrets") or {})
        read = set(re.findall(r"secrets\.([A-Za-z0-9_]+)", code)) - {"GITHUB_TOKEN"}
        assert read <= declared, f"{path.name} reads {sorted(read - declared)} without declaring it"
    # ... and each caller passes only names the workflow it calls declares.
    for workflow, job, body in _jobs():
        target = str(body.get("uses", ""))
        if not target.startswith("./.github/workflows/"):
            continue
        called = yaml.safe_load((REPO / target).read_text())
        declared = set(((called[True] or {}).get("workflow_call") or {}).get("secrets") or {})
        assert set(body.get("secrets") or {}) <= declared, f"{workflow}:{job} -> {target}"


def test_the_leaf_check_covers_every_leaf_or_says_why_not():
    text, workflow = _leaf_check()
    used = {
        str(step["uses"]).rsplit("/", 1)[-1]
        for body in workflow["jobs"].values()
        for step in body.get("steps") or []
        if str(step.get("uses", "")).startswith("./.github/actions/")
    }
    actions = {p.parent.name for p in ACTIONS}
    left_out = {"apple-signing", "nixos-vm", "conformance-instance"}
    assert used == actions - left_out, used ^ (actions - left_out)
    header = text.split("\nname: ", 1)[0]
    for action in left_out:
        assert f"#   - {action}:" in header, f"the header does not say why {action} is left out"
    called = {str(b.get("uses", "")) for b in workflow["jobs"].values()}
    assert LANE in called and "./.github/workflows/ci-leaf-stub-leg.yml" in called
    # A lane that fails on purpose does not fail the run, and only the self-test may ask for
    # that: a real lane's failure always fails its run.
    assert workflow["jobs"]["lane-fails"]["with"]["failure-expected"] is True
    lane = yaml.safe_load((REPO / ".github/workflows/lane.yml").read_text())["jobs"]["lane"]
    assert lane["continue-on-error"] == "${{ inputs.failure-expected }}"
    for caller, job, body in _lane_callers():
        if (caller, job) != (_LEAF_CHECK, "lane-fails"):
            assert "failure-expected" not in body["with"], f"{caller}:{job}"


def test_the_reusable_workflows_are_checked_only_after_every_action_is():
    """Level 1's jobs need level 0's gate, which needs every composite action's check: a broken
    action is reported as itself, not as a lane that failed for an unclear reason."""
    _text, workflow = _leaf_check()
    jobs = workflow["jobs"]
    for job in ("lane", "lane-fails", "release-publication"):
        assert jobs[job]["needs"] == ["scope", "level-0"], job
    for job in ("lane", "lane-fails"):
        assert "if" not in jobs[job], job
    # The stand-in leg also needs the publication a dry run skips, and still runs (as the
    # release's `publish` needs its PyPI jobs).
    assert jobs["release-dry-run"]["needs"] == ["scope", "level-0", "release-publication"]
    assert "!cancelled()" in jobs["release-dry-run"]["if"]
    level_zero = set(jobs["level-0"]["needs"])
    assert level_zero == {
        job for job, body in jobs.items() if str(body.get("name", "")).startswith("action / ")
    }
    assert len(level_zero) == 16
    assert jobs["level-1"]["needs"] == ["lane-results", "release-results"]


def test_no_proof_job_fails_on_purpose_uncontained_and_no_needed_job_is_contained():
    """A workflow that is red by design trains everyone to ignore red, so the proof has none:
    its one deliberate failure (a lane) is contained. And containment is never put on a job
    whose failure a later job's condition must see -- a job that fails under continue-on-error
    shows `success` to the job that needs it (run 37983239912)."""
    assert not (REPO / ".github" / "workflows" / "ci-leaf-red-check.yml").exists()
    for path in WORKFLOWS:
        jobs = yaml.safe_load(path.read_text())["jobs"]
        needed = set()
        for body in jobs.values():
            needs = body.get("needs") or []
            needed |= {needs} if isinstance(needs, str) else set(needs)
        for job, body in jobs.items():
            if "continue-on-error" in body:
                assert job not in needed, f"{path.name}:{job} is contained and needed"


def test_the_primer_names_only_guards_actions_and_scripts_that_exist():
    """.github/CI.md states each rule beside the test that holds it. A rule whose test was
    renamed or removed would read as held when it is not."""
    primer = (REPO / ".github" / "CI.md").read_text()
    cited = set(re.findall(r"`(test_[a-z0-9_]+)`", primer))
    assert len(cited) >= 12
    defined = set()
    for path in (REPO / "tests" / "unit").glob("test_*.py"):
        defined |= set(re.findall(r"^def (test_[a-z0-9_]+)\(", path.read_text(), re.M))
    assert cited <= defined, sorted(cited - defined)
    # Every action is in the map, and nothing is in the map that is not an action.
    actions = {path.parent.name for path in (REPO / ".github" / "actions").glob("*/action.yml")}
    assert set(re.findall(r"^\| `([a-z-]+)` \|", primer, re.M)) == actions
    for path in re.findall(r"`((?:scripts/ci|\.github/workflows)/[\w./-]+\.(?:py|yml))`", primer):
        assert (REPO / path).exists(), path


def test_the_repository_front_page_is_the_product_readme():
    """GitHub renders .github/README.md in preference to the root README.md. The CI primer once
    lived there and took the repository's front page; it is .github/CI.md now."""
    assert not (REPO / ".github" / "README.md").exists()
    assert (REPO / ".github" / "CI.md").exists()
    assert not (REPO / "docs" / "README.md").exists()
    assert (REPO / "README.md").exists()


def test_a_job_checks_out_the_commit_its_workflow_came_from_or_the_one_it_was_asked_for():
    # A job's local actions (./.github/actions/...) and lane scripts are read from what it
    # checked out. The nightly warehouse job once checked out a named branch, `integration`:
    # with the workflow from main and the tree from a branch that had none of main's actions,
    # its first `uses:` could not be found. A checkout therefore names no branch -- only the
    # ref a caller or a dispatch passed, or the run's own commit.
    named = []
    for where, step in _steps():
        if not str(step.get("uses", "")).startswith("actions/checkout@"):
            continue
        ref = (step.get("with") or {}).get("ref")
        if ref is None:
            continue
        if (step.get("with") or {}).get("repository"):
            # Another repository (the ServiceNow adapter a lane builds, REQ-1954): no action of
            # this one is read from it, and it too names no branch -- only a ref it was given.
            if not re.fullmatch(r"\$\{\{ inputs\.\w+ \}\}", str(ref)):
                named.append(f"{where}: ref {ref!r}")
            continue
        if not re.fullmatch(r"\$\{\{ inputs\.ref \|\| (github\.sha|'') \}\}", str(ref)):
            named.append(f"{where}: ref {ref!r}")
    assert named == []


def test_no_workflow_is_started_by_a_push_to_a_branch_nobody_pushes_to():
    # `integration` was where work was pushed before main; nothing moves it now. A push starts a
    # workflow on main, on a release branch, or on a ci/** proving branch.
    allowed = {"main", "release/**", "ci/**"}
    other = []
    for path in WORKFLOWS:
        triggers = yaml.safe_load(path.read_text())[True]
        push = triggers.get("push") if isinstance(triggers, dict) else None
        for branch in (push or {}).get("branches") or []:
            if branch not in allowed:
                other.append(f"{path.name}: {branch}")
    assert other == []


_UI_LANES = ("ui-e2e-core.yml", "ui-e2e-trino.yml", "ui-e2e-swap-amd64.yml")


@pytest.mark.parametrize("workflow", _UI_LANES)
def test_a_ui_lane_can_be_run_for_named_specs_only(workflow):
    """A dispatch may name the specs to retest (`specs`: file or file:line, space-separated);
    every job of the lane keeps its provisioning and hands Playwright only those, a job whose
    project holds none of them passing with none run, and a named spec that is not there failing
    the job before Playwright starts. Empty, the whole lane runs."""
    body = yaml.safe_load((REPO / ".github" / "workflows" / workflow).read_text())
    specs = body[True]["workflow_dispatch"]["inputs"]["specs"]
    assert (specs["type"], specs["required"], specs["default"]) == ("string", False, "")
    # The lane's jobs; the Trino lanes' plugin-priming job runs no Playwright.
    lanes = {j: g for j, g in body["jobs"].items() if g.get("uses") == _LANE_WORKFLOW}
    assert lanes, workflow
    for job, given in lanes.items():
        with_ = given["with"]
        assert "PROVISA_E2E_SPECS=${{ inputs.specs }}" in with_["env"].splitlines(), job
        run = with_["run"]
        assert run.startswith('for spec in $PROVISA_E2E_SPECS; do test -f "${spec%%:*}"'), job
        assert run.endswith("$PROVISA_E2E_SPECS ${PROVISA_E2E_SPECS:+--pass-with-no-tests}"), job


def _refusal_step():
    job = yaml.safe_load((REPO / ".github/workflows/focused-tests.yml").read_text())["jobs"][
        "python"
    ]
    steps = job["steps"]
    step = next(s for s in steps if s.get("name") == "Refuse integration files")
    # It runs before anything is checked out or installed.
    assert steps.index(step) == 0
    assert step["env"] == {"TESTS": "${{ inputs.python }}"}
    return step["run"]


def _refusal(tests: str):
    import subprocess

    return subprocess.run(
        ["bash", "-c", _refusal_step()],
        env={"TESTS": tests, "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    "tests",
    [
        "tests/integration/test_x.py",
        "tests/unit/test_a.py tests/integration/test_x.py::test_one",
        "./tests/integration/sub/test_y.py",
        "tests/steps/steps_security.py",
        "tests/features/a.feature",
        "tests/e2e/test_z.py::test_one",
    ],
)
def test_focused_tests_refuses_an_integration_file_by_name(tests):
    """An integration file is refused at once, named, with the run that does take it."""
    done = _refusal(tests)
    assert done.returncode == 1
    named = next(t for t in tests.split() if not t.startswith("tests/unit"))
    assert f"::error::{named} is an integration, steps, features or e2e test" in done.stdout
    assert "Integration suite workflow" in done.stdout


def test_focused_tests_passes_files_that_need_no_stack():
    done = _refusal("tests/unit/test_a.py tests/unit/test_b.py::test_c")
    assert done.returncode == 0 and done.stdout == ""


def test_the_microsoft_365_lane_reads_its_secrets_from_the_live_environment_and_checks_them():
    """The repository is at GitHub's 100-secret cap: the live lanes' secrets move to the `live`
    environment (maintainer 2026-11-10). The microsoft_365 job names that environment, reads the
    same secret names, and still fails by name, before the lane runs, when one is missing.
    The tenant has one mailbox, the maintainer's: no second address is read from any secret."""
    path = REPO / ".github" / "workflows" / "integration-suite-lanes.yml"
    job = yaml.safe_load(path.read_text())["jobs"]["microsoft_365"]
    assert job["environment"] == "live"
    required = (
        "M365_TENANT_ID",
        "M365_CLIENT_ID",
        "M365_CLIENT_SECRET",
        "M365_TEST_MAILBOXES",
    )
    for name in required:
        assert job["env"][name] == f"${{{{ secrets.{name} }}}}", name
    names = [s.get("name") for s in job["steps"]]
    assert names.index("Credentials present") < names.index("Run lane")
    check = job["steps"][names.index("Credentials present")]["run"]
    loop = re.search(r"for v in (.*?); do", check, re.S)
    assert loop is not None and set(loop.group(1).split()) == set(required)
    assert "exit 1" in check and "environment live" in check


def test_the_odbc_install_refreshes_only_microsofts_package_list():
    """A full `apt-get update` reaches every Ubuntu mirror: an outage there failed the ODBC step
    (run 43036808445) though msodbcsql18 is Microsoft's. The retry and timeout options stay."""
    body = yaml.safe_load((REPO / ".github" / "actions" / "mssql-odbc" / "action.yml").read_text())

    # The one step that refreshes a package list (J1189b split the install into resolve, cache and
    # install steps; only the resolve step runs `apt-get update`).
    def _code(step: dict) -> list[str]:
        script = re.sub(r"\\\n\s*", " ", step.get("run", ""))
        return [line for line in script.splitlines() if not line.lstrip().startswith("#")]

    refreshing = [
        s
        for s in body["runs"]["steps"]
        if any("apt-get" in line and " update" in line for line in _code(s))
    ]
    (step,) = refreshing
    script = re.sub(r"\\\n\s*", " ", step["run"])
    updates = [line for line in _code(step) if "apt-get" in line and " update" in line]
    assert len(updates) == 1
    for option in (
        "-o Dir::Etc::sourcelist=/etc/apt/sources.list.d/mssql-release.list",
        "-o Dir::Etc::sourceparts=-",
        "-o APT::Get::List-Cleanup=0",
        "$APT_OPTS",
    ):
        assert option in updates[0], option
    assert "$CURL_OPTS" in script
    assert "APT_OPTS" in step["env"] and "CURL_OPTS" in step["env"]


def test_every_odbc_install_goes_through_the_one_action_that_refreshes_only_microsofts_list():
    """No workflow installs msodbcsql18 itself: the action is the one place that does."""
    for path in WORKFLOWS:
        assert "msodbcsql18" not in path.read_text(), path.name
    users = [p.name for p in WORKFLOWS if "./.github/actions/mssql-odbc" in p.read_text()]
    assert users, "no workflow uses the mssql-odbc action"


def test_no_workflow_or_action_installs_playwright_with_a_full_apt_refresh():
    """`playwright install --with-deps` runs a full `apt-get update`, which fails (apt exit 100)
    when an Ubuntu mirror is unreachable. The runner image carries Chromium's system libraries,
    so every install is plain `playwright install chromium`, through the setup-ui action."""
    for path in [*WORKFLOWS, *ACTIONS]:
        for line in path.read_text().splitlines():
            if line.lstrip().startswith("#"):
                continue
            assert "--with-deps" not in line or "playwright" not in line, f"{path.name}: {line}"
        assert "chromium-with-deps" not in path.read_text(), path.name
    action = yaml.safe_load((REPO / ".github" / "actions" / "setup-ui" / "action.yml").read_text())
    commands = [s["run"] for s in action["runs"]["steps"] if "playwright" in s.get("run", "")]
    assert commands == ["npx playwright install chromium"]


@pytest.mark.parametrize("name", ["ui-e2e-trino.yml", "ui-e2e-swap-amd64.yml"])
def test_the_trino_and_swap_ui_lanes_install_plain_chromium(name):
    body = yaml.safe_load((REPO / ".github" / "workflows" / name).read_text())
    (job,) = [j for j in body["jobs"].values() if j.get("uses") == _LANE_WORKFLOW]
    assert job["with"]["ui"] == "chromium"
    # The lane mounts the plugin jars a job of the same run primed; its stack script fetches none.
    assert job["with"]["trino-plugins"] is True and job["needs"] == ["trino-plugins"]


_MSSQL_ODBC = REPO / ".github" / "actions" / "mssql-odbc" / "action.yml"


def _odbc_steps() -> list[dict]:
    return yaml.safe_load(_MSSQL_ODBC.read_text())["runs"]["steps"]


def test_the_mssql_odbc_action_caches_the_deb_set():
    """An Ubuntu archive outage (runs 43056312618, 43059293707) must not fail the install: the
    msodbcsql18 .deb set is cached."""
    caches = [s for s in _odbc_steps() if str(s.get("uses", "")).startswith("actions/cache")]
    assert len(caches) == 1
    assert caches[0]["with"]["path"] and caches[0]["with"]["key"]
    miss = next(s for s in _odbc_steps() if "!= 'true'" in s.get("if", ""))
    assert "--download-only" in miss["run"]
    assert "/var/cache/apt/archives/*.deb" in miss["run"]


def test_a_mssql_odbc_cache_hit_installs_offline():
    hit = next(s for s in _odbc_steps() if "== 'true'" in s.get("if", ""))
    assert "cache-hit" in hit["if"]
    assert "dpkg -i" in hit["run"]
    assert not re.search(r"apt-get|curl|apt update", hit["run"]), "a hit must not touch the network"
    assert "ACCEPT_EULA=Y" in hit["run"]


def test_the_mssql_odbc_cache_key_includes_the_package_versions():
    steps = _odbc_steps()
    resolve = next(s for s in steps if s.get("id") == "resolve")
    cache = next(s for s in steps if str(s.get("uses", "")).startswith("actions/cache"))
    assert cache["with"]["key"] == "${{ steps.resolve.outputs.key }}"
    assert "apt-cache policy msodbcsql18" in resolve["run"]
    assert "msodbcsql18-${msodbc_version}" in resolve["run"]
    assert "install -s" in resolve["run"] and "sha256sum" in resolve["run"]
    assert "deps-${deps_hash}" in resolve["run"]


def test_the_mssql_odbc_install_keeps_its_retries_and_has_no_fallback():
    for step in _odbc_steps():
        if "run" not in step:
            continue
        assert "|| true" not in step["run"] and "continue-on-error" not in step
    miss = next(s for s in _odbc_steps() if "!= 'true'" in s.get("if", ""))
    assert "Acquire::Retries=5" in miss["env"]["APT_OPTS"]
    assert "ACCEPT_EULA=Y" in miss["run"]


def _npm_audit_gate():
    spec = importlib.util.spec_from_file_location(
        "npm_audit_gate", REPO / "scripts/ci/npm_audit_gate.py"
    )
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    return gate


def test_the_cve_audit_runs_the_npm_gate_over_every_npm_lockfile():
    """Every committed npm lockfile is audited, from the lockfile alone (no install)."""
    job = yaml.safe_load((REPO / ".github/workflows/cve-audit.yml").read_text())["jobs"][
        "npm-audit"
    ]
    setup = next(s for s in job["steps"] if s.get("uses") == "./.github/actions/setup-ui")
    assert setup["with"]["install"] == "false"
    runs = [s["run"] for s in job["steps"] if "scripts/ci/npm_audit_gate.py" in s.get("run", "")]
    assert len(runs) == 1
    audited = set(runs[0].split("npm_audit_gate.py", 1)[1].split())
    lockfiles = {
        str(p.parent.relative_to(REPO))
        for pattern in ("*/package-lock.json", "*/*/package-lock.json")
        for p in REPO.glob(pattern)
        if "node_modules" not in p.parts
    }
    assert lockfiles and audited == lockfiles


def test_the_npm_gate_allow_list_names_dev_advisories_with_a_reason():
    for ghsa, why in _npm_audit_gate().ALLOW.items():
        assert re.fullmatch(r"GHSA(-[23456789cfghjmpqrvwx]{4}){3}", ghsa), ghsa
        assert len(why.split()) >= 8, f"{ghsa}: say which package carries it and what blocks it"


def _audit_report(*advisories: tuple[str, str, str]) -> dict:
    """An npm audit --json report holding (package, severity, GHSA id) advisories."""
    vulns: dict = {}
    for package, severity, ghsa in advisories:
        via = {
            "name": package,
            "severity": severity,
            "title": "t",
            "url": f"https://github.com/advisories/{ghsa}",
        }
        vulns.setdefault(package, {"via": []})["via"].append(via)
    return {"vulnerabilities": vulns}


def test_the_npm_gate_fails_a_production_advisory_even_when_allowed():
    gate = _npm_audit_gate()
    prod = _audit_report(("dompurify", "high", "GHSA-aaaa-bbbb-cccc"))
    problems = gate.violations(
        [("ui", prod, prod)], {"GHSA-aaaa-bbbb-cccc": "listed by mistake for a prod package"}
    )
    assert any("production GHSA-aaaa-bbbb-cccc" in p for p in problems)


def test_the_npm_gate_passes_an_allowed_dev_advisory_and_fails_an_unlisted_one():
    gate = _npm_audit_gate()
    full = _audit_report(
        ("vite", "critical", "GHSA-aaaa-bbbb-cccc"), ("zod", "high", "GHSA-dddd-eeee-ffff")
    )
    problems = gate.violations([("ui", full, _audit_report())], {"GHSA-aaaa-bbbb-cccc": "why"})
    assert problems == ["ui: dev-only GHSA-dddd-eeee-ffff zod high: t (not in ALLOW)"]


def test_the_npm_gate_ignores_moderate_and_fails_a_stale_allow_entry():
    gate = _npm_audit_gate()
    full = _audit_report(("glob", "moderate", "GHSA-aaaa-bbbb-cccc"))
    assert gate.violations([("ui", full, full)], {}) == []
    stale = gate.violations([("ui", full, full)], {"GHSA-aaaa-bbbb-cccc": "why"})
    assert stale == [
        "ALLOW names GHSA-aaaa-bbbb-cccc, which no dev-only advisory matches: remove it"
    ]


# Node: the repository's one pin is the root .nvmrc. CI, the image's renderer stage and the NixOS
# guest build on it, so a bump in one place without the others fails here.
_NVMRC = REPO / ".nvmrc"


def _node_pin() -> str:
    return _NVMRC.read_text().strip()


def _setup_node_steps():
    for where, step in _steps():
        yield where, step
    for action in ACTIONS:
        for step in yaml.safe_load(action.read_text())["runs"]["steps"]:
            yield f"{action.parent.name}:{step.get('name') or step.get('uses')}", step


def test_node_is_pinned_once_at_the_repository_root():
    import subprocess

    tracked = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "--", "*.nvmrc", ".nvmrc"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    assert tracked == [".nvmrc"], f"Node pins tracked: {tracked}; the one pin is the root .nvmrc"
    assert re.fullmatch(r"\d+\.\d+\.\d+", _node_pin()), f".nvmrc is {_node_pin()!r}, not x.y.z"


def test_every_setup_node_reads_the_nvmrc_and_hard_codes_no_version():
    setup_node = 0
    for where, step in _setup_node_steps():
        with_ = step.get("with") or {}
        assert "node-version" not in with_, (
            f"{where} hard-codes node-version {with_['node-version']}"
        )
        if (step.get("uses") or "").startswith("actions/setup-node"):
            setup_node += 1
            assert with_.get("node-version-file") == ".nvmrc", (
                f"{where} sets up Node without node-version-file: .nvmrc"
            )
    assert setup_node, "no actions/setup-node step found"


def test_no_workflow_or_action_names_a_node_version_anywhere():
    for path in [*WORKFLOWS, *ACTIONS]:
        for n, line in enumerate(path.read_text().splitlines(), 1):
            assert not re.search(r"\bnode-version\s*:", line), f"{path.name}:{n}: {line.strip()}"


def test_the_images_node_is_the_nvmrc():
    tags = re.findall(r"^FROM node:(\S+?)-", (REPO / "Dockerfile").read_text(), flags=re.M)
    assert tags == [_node_pin()], f"Dockerfile node images {tags}, .nvmrc {_node_pin()}"


def test_the_nixos_guests_node_is_the_nvmrc_major():
    nix = (REPO / "packaging" / "nixos" / "configuration.nix").read_text()
    majors = re.findall(r"\bnodejs_(\d+)\b", nix)
    assert majors == [_node_pin().split(".")[0]], f"nix nodejs {majors}, .nvmrc {_node_pin()}"


def test_the_uis_engines_admit_the_nvmrc():
    import json

    engines = json.loads((REPO / "provisa-ui" / "package.json").read_text())["engines"]["node"]
    floor = re.fullmatch(r">=(\d+)\.(\d+)\.(\d+)", engines)
    assert floor, f"provisa-ui engines.node is {engines!r}, expected >=x.y.z"
    pin = tuple(int(x) for x in _node_pin().split("."))
    assert tuple(int(x) for x in floor.groups()) <= pin, f"engines {engines} refuses .nvmrc {pin}"


# --- the Trino plugin jars: primed once per workflow run, restored before every test ------------

_PLUGINS_ACTION = "./.github/actions/trino-plugins"
_LANE_WORKFLOW = "./.github/workflows/lane.yml"


def _plugin_phases(body: dict) -> list[str]:
    """The plugin-cache phases a job runs: its own action steps, or `restore` for a call of the
    lane workflow with trino-plugins switched on (literally or by an expression)."""
    if body.get("uses") == _LANE_WORKFLOW:
        return ["restore"] if (body.get("with") or {}).get("trino-plugins", False) else []
    return [
        (s.get("with") or {}).get("phase")
        for s in body.get("steps") or []
        if s.get("uses") == _PLUGINS_ACTION
    ]


def _needed(jobs: dict, job: str) -> set[str]:
    """Every job ``job`` waits for, directly or through another."""
    seen: set[str] = set()
    todo = [job]
    while todo:
        needs = jobs[todo.pop()].get("needs") or []
        for n in [needs] if isinstance(needs, str) else needs:
            if n not in seen:
                seen.add(n)
                todo.append(n)
    return seen


def test_every_plugin_restore_waits_for_a_job_that_primed_the_cache():
    """Release 510 (run 44081179966): each parallel lane missed an evicted plugin cache and
    fetched the jars from Maven Central inside collection at the same moment; core 17/18 got HTTP
    429 as an INTERNALERROR. Now one job per workflow run primes the cache (`phase: prime`: a hit,
    or one download, checksum-verified, saved at once) and every job that restores it (`phase:
    restore`, which takes a missed entry from the run's artifact and verifies each jar) waits for that job, so the cache step
    precedes pytest and no lane ever downloads."""
    restoring = []
    for path in WORKFLOWS:
        if path.name == "lane.yml":
            continue  # the callee: each caller's job is checked where it calls it
        jobs = yaml.safe_load(path.read_text()).get("jobs") or {}
        primers = {j for j, body in jobs.items() if "prime" in _plugin_phases(body)}
        for job, body in jobs.items():
            phases = _plugin_phases(body)
            assert set(phases) <= {"prime", "restore"}, f"{path.name}:{job}: {phases}"
            if "restore" in phases and "prime" not in phases:
                restoring.append(f"{path.name}:{job}")
                assert _needed(jobs, job) & primers, (
                    f"{path.name}:{job} restores the Trino plugins without waiting for a job "
                    f"that primes them (primers here: {sorted(primers)})"
                )
    assert restoring, "no job restores the Trino plugins"


def test_a_plugin_cache_miss_is_answered_by_the_runs_artifact_not_a_failure_or_a_download():
    """Runs 44942101401 and 44940962731: the cache entry the priming job saved was evicted before
    the lane's restore, which failed on the miss. The cache is branch-scoped and evictable; the
    run's own artifact is not. Priming uploads the verified jars; a restore that missed takes
    them from there."""
    steps = yaml.safe_load(
        (REPO / ".github" / "actions" / "trino-plugins" / "action.yml").read_text()
    )["runs"]["steps"]
    for s in steps:
        if str(s.get("uses", "")).startswith("actions/cache/restore"):
            assert s["with"]["fail-on-cache-miss"] is False, s["name"]
    names = [s.get("name") for s in steps]
    down = steps[names.index("Download the run's Trino plugins")]
    up = steps[names.index("Upload the run's Trino plugins")]
    assert down["uses"].startswith("actions/download-artifact")
    assert up["uses"].startswith("actions/upload-artifact")
    assert down["with"]["name"] == up["with"]["name"]
    assert "restore" in down["if"] and "prime" in up["if"]
    assert names.index("Download the run's Trino plugins") < names.index("Verify Trino plugins")
    assert names.index("Upload the run's Trino plugins") > names.index(
        "Verify the provisa-functions plugin"
    )


def test_the_plugin_jars_are_cached_by_their_pin_and_downloaded_only_on_a_miss():
    """Run 45293530114: the priming job missed the cache and Maven Central refused the first jar
    with HTTP 429. The jars are cached under a key made of the pinned versions
    (tests/trino_plugins.py), the restore comes first, and the download (whose 429s are retried a
    bounded number of times, tests/unit/test_trino_plugins_pinned.py) and the save run only in the
    priming phase on a miss, under the same key."""
    steps = yaml.safe_load(
        (REPO / ".github" / "actions" / "trino-plugins" / "action.yml").read_text()
    )["runs"]["steps"]
    names = [s.get("name") for s in steps]
    pin = steps[names.index("Trino plugin pin")]
    restore = steps[names.index("Restore Trino plugins")]
    download = steps[names.index("Download Trino plugins")]
    save = steps[names.index("Cache Trino plugins")]
    assert "tests/trino_plugins.py" in pin["run"]
    assert restore["uses"].startswith("actions/cache/restore@")
    assert save["uses"].startswith("actions/cache/save@")
    key = restore["with"]["key"]
    assert "${{ steps.trino-pin.outputs.version }}" in key and key.endswith("-jars")
    assert save["with"]["key"] == key and save["with"]["path"] == restore["with"]["path"]
    miss = "inputs.phase == 'prime' && steps.trino-plugins.outputs.cache-hit != 'true'"
    assert download["if"] == miss and save["if"] == miss
    assert download["run"] == "python3 tests/trino_plugins.py fetch"
    assert (
        names.index("Trino plugin pin")
        < names.index("Restore Trino plugins")
        < names.index("Download Trino plugins")
        < names.index("Cache Trino plugins")
        < names.index("Verify Trino plugins")
    )


_ZAYCHIK_ACTION = "./.github/actions/zaychik-image"


def _zaychik_phases(body: dict) -> list[str]:
    """The zaychik-image phases a job runs: its own action steps, or `restore` for a call of the
    lane workflow with zaychik-image switched on."""
    if body.get("uses") == _LANE_WORKFLOW:
        return ["restore"] if (body.get("with") or {}).get("zaychik-image", False) else []
    return [
        (s.get("with") or {}).get("phase")
        for s in body.get("steps") or []
        if s.get("uses") == _ZAYCHIK_ACTION
    ]


def test_every_zaychik_restore_waits_for_the_one_job_that_primed_the_image():
    """Runs 45030691606, 45030697705, 45030636599: lanes failed at "Restore the zaychik image" on
    a cache miss. A restore takes a missed entry from the run's artifact, which exists only once a
    job of the same run primed it, so every restoring job waits for that job. One job per run
    primes it: the artifact's name is the run's, and a second upload under it is a 409."""
    restoring = []
    for path in WORKFLOWS:
        if path.name == "lane.yml":
            continue  # the callee: each caller's job is checked where it calls it
        jobs = yaml.safe_load(path.read_text()).get("jobs") or {}
        primers = {j for j, body in jobs.items() if "prime" in _zaychik_phases(body)}
        assert len(primers) <= 1, f"{path.name}: zaychik-image primed by {sorted(primers)}"
        for job, body in jobs.items():
            phases = _zaychik_phases(body)
            assert set(phases) <= {"prime", "restore"}, f"{path.name}:{job}: {phases}"
            assert phases.count("prime") <= 1, f"{path.name}:{job}: {phases}"
            if "restore" in phases and "prime" not in phases:
                restoring.append(f"{path.name}:{job}")
                assert _needed(jobs, job) & primers, (
                    f"{path.name}:{job} restores the zaychik image without waiting for a job "
                    f"that primes it (primers here: {sorted(primers)})"
                )
    assert restoring, "no job restores the zaychik image"


def test_the_zaychik_image_is_primed_in_the_default_branch_cache_scope():
    """Run 45044138083: a branch's priming job missed (the only entry was a sibling branch's,
    which a branch cannot read) and built zaychik/ by Maven, which Maven Central refused with a
    429. A workflow on main primes the entry whenever zaychik/ or the action changes, and daily
    after an eviction, so every branch's prime reads main's entry and builds nothing."""
    doc = yaml.safe_load((REPO / ".github" / "workflows" / "zaychik-image.yml").read_text())
    on = doc[True]
    assert on["push"]["branches"] == ["main"]
    assert {"zaychik/**", ".github/actions/zaychik-image/**"} <= set(on["push"]["paths"])
    assert on["schedule"] and "workflow_dispatch" in on
    (job,) = doc["jobs"].values()
    assert _zaychik_phases(job) == ["prime"]
    # The key covers the build's whole context, so main's entry is the one a branch computes.
    action = (REPO / ".github" / "actions" / "zaychik-image" / "action.yml").read_text()
    assert "zaychik-image-${{ hashFiles('zaychik/**') }}" in action


def test_a_zaychik_cache_miss_is_answered_by_the_runs_artifact():
    """The cache entry the priming job saved can be gone by a lane's restore (evicted, or never
    readable from the lane's scope). The priming job uploads the image as the run's artifact on a
    hit and on a miss; a restore that missed downloads it, and fails by name only when neither
    exists."""
    steps = yaml.safe_load(
        (REPO / ".github" / "actions" / "zaychik-image" / "action.yml").read_text()
    )["runs"]["steps"]
    for s in steps:
        if str(s.get("uses", "")).startswith("actions/cache/restore"):
            assert s["with"]["fail-on-cache-miss"] is False, s["name"]
    names = [s.get("name") for s in steps]
    down = steps[names.index("Download the run's zaychik image")]
    up = steps[names.index("Upload the run's zaychik image")]
    assert down["uses"].startswith("actions/download-artifact")
    assert up["uses"].startswith("actions/upload-artifact")
    assert down["with"]["name"] == up["with"]["name"]
    assert "restore" in down["if"] and "cache-hit != 'true'" in down["if"]
    assert up["if"] == "inputs.phase == 'prime'"
    assert up["with"]["if-no-files-found"] == "error"
    assert names.index("Build the zaychik image") < names.index("Upload the run's zaychik image")
    assert names.index("Download the run's zaychik image") < names.index("Load the zaychik image")


def test_the_lane_workflow_restores_the_plugins_before_its_lane_and_saves_none():
    steps = yaml.safe_load((REPO / ".github" / "workflows" / "lane.yml").read_text())["jobs"][
        "lane"
    ]["steps"]
    names = [s.get("name") for s in steps]
    restore = steps[names.index("Restore Trino plugins")]
    assert restore["uses"] == _PLUGINS_ACTION and restore["with"] == {"phase": "restore"}
    assert names.index("Restore Trino plugins") < names.index("Run lane")
    assert [s for s in steps if s.get("uses") == _PLUGINS_ACTION] == [restore]


def test_no_workflow_fetches_the_conftest_plugin_jars_outside_the_action():
    """The jars come from the action's one fetch per pin; a workflow step of its own that ran the
    fetch would be a second, uncached path to Maven Central."""
    for where, step in _steps():
        assert "trino_plugins.py fetch" not in (step.get("run") or ""), where


def test_nothing_in_ci_names_the_plugin_jars_maven_location_but_the_pin():
    """Maven Central is reached for the plugin jars from one place, tests/trino_plugins.py, run by
    the action's priming job. The UI Trino and swap lanes' stack script and the release's plugin
    job each fetched the jars with curl of their own, once per job and run, outside the cache
    and the checksums."""
    from tests import trino_plugins

    maven = trino_plugins._TRINO_PLUGIN_MAVEN
    sources = [
        *WORKFLOWS,
        *(REPO / ".github" / "actions").rglob("*.y*ml"),
        *(REPO / "scripts" / "ci").rglob("*"),
    ]
    named = [
        str(p.relative_to(REPO))
        for p in sources
        if p.is_file() and p.suffix in {".yml", ".yaml", ".sh", ".py"} and maven in p.read_text()
    ]
    assert named == []


def test_the_release_builds_both_sdks_and_attaches_them_to_the_draft():
    """REQ-1984: each SDK is built in the packages leg (a dry run builds both), stamped with the
    release's version, and attached to the draft release: nothing goes to PyPI or npm (maintainer
    2026-11-21). A dry run keeps each in the run instead."""
    legs = dict(_release_legs().values())
    jobs = legs["release-packages.yml"]["jobs"]
    py, ts = jobs["build-sdk-python"], jobs["build-sdk-typescript"]
    assert "needs.metadata.outputs.py_version" in str(py)
    assert "needs.metadata.outputs.npm_version" in str(ts)
    for job, artifact, files in (
        (py, "sdk-python-dist", ("provisa_sdk-*.whl", "provisa_sdk-*.tar.gz")),
        (ts, "sdk-typescript-dist", ("provisa-sdk-*.tgz",)),
    ):
        uploads = [s for s in job["steps"] if "upload-artifact" in s.get("uses", "")]
        assert [s["with"]["name"] for s in uploads] == [artifact]
        assert uploads[0]["if"] == "${{ !inputs.publish }}"
        assert not _LEAVES_THE_RUN.search("\n".join(str(s.get("run", "")) for s in job["steps"]))
        [attach] = [s for s in job["steps"] if "attach-release-assets" in str(s.get("uses", ""))]
        assert attach["if"] == "inputs.publish"
        for pattern in files:
            assert pattern in attach["with"]["files"], pattern
    orchestrator = yaml.safe_load(_RELEASE.read_text())["jobs"]
    assert not any("pypi" in job or "npm" in job for job in orchestrator)


def test_no_workflow_publishes_to_npm():
    """Every package ships as a GitHub release asset only (maintainer 2026-11-21)."""
    for path in sorted((REPO / ".github").rglob("*.yml")):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            assert not re.search(r"\bnpm publish\b|registry\.npmjs\.org", line), (
                f"{path.name}:{n}: {line.strip()}"
            )


def _metadata_outputs(tag: str) -> dict[str, str]:
    """Runs the metadata action's own script for `tag` and returns what it writes."""
    import subprocess
    import tempfile

    action = yaml.safe_load(
        (REPO / ".github" / "actions" / "release-metadata" / "action.yml").read_text()
    )
    [step] = [s for s in action["runs"]["steps"] if s.get("id") == "meta"]
    with tempfile.NamedTemporaryFile("r+", suffix=".out") as out:
        subprocess.run(  # noqa: S603 -- the repository's own script, on a made-up tag
            ["bash", "-c", step["run"]],
            env={"VERSION": tag, "GITHUB_OUTPUT": out.name, "PATH": os.environ["PATH"]},
            check=True,
        )
        out.seek(0)
        return dict(line.rstrip("\n").split("=", 1) for line in out if "=" in line)


@pytest.mark.parametrize(
    ("tag", "pypi", "npm"),
    [
        ("v0.1.0-alpha.481", "0.1.0a481", "0.1.0-alpha.481"),
        ("v0.2.0-beta.3", "0.2.0b3", "0.2.0-beta.3"),
        ("v1.0.0-rc.12", "1.0.0rc12", "1.0.0-rc.12"),
        ("v1.2.3", "1.2.3", "1.2.3"),
    ],
)
def test_the_sdk_versions_map_from_the_release_tag(tag, pypi, npm):
    """REQ-1984: PyPI takes the PEP 440 form, npm the tag as semver."""
    got = _metadata_outputs(tag)
    assert got["py_version"] == pypi
    assert got["npm_version"] == npm


def test_no_job_uploads_a_second_artifact_under_a_name_its_priming_action_uploads():
    """The trino-plugins and zaychik-image actions' prime phase uploads the run's artifact
    (`trino-plugins`, `zaychik-image`). release-prebuilt.yml's download-plugins job also uploaded
    `trino-plugins` itself after priming; a second upload under one name in a run is a 409."""
    for path in WORKFLOWS:
        jobs = yaml.safe_load(path.read_text()).get("jobs") or {}
        for job, body in jobs.items():
            steps = body.get("steps") or []
            taken = {_primed_upload(s) for s in steps} - {None}
            for s in steps:
                if str(s.get("uses", "")).startswith("actions/upload-artifact"):
                    name = (s.get("with") or {}).get("name")
                    assert name not in taken, (
                        f"{path.name}:{job} uploads {name!r}, which its priming action uploads"
                    )


# --- the nightly on main and the release-ready summary (REQ-1989, amended 2026-11-22) ------------

_NIGHTLY_MAIN = REPO / ".github" / "workflows" / "nightly-main.yml"
_RELEASE_READY = REPO / ".github" / "workflows" / "release-ready.yml"
_NIGHTLY_LANE_NAMES = {"unit", "lint", "requirements", "integration", "e2e", "packaging"}


def _script(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _code(path: Path) -> str:
    return "\n".join(
        line for line in path.read_text().splitlines() if not line.lstrip().startswith("#")
    )


def test_the_nightly_on_main_runs_every_night_on_main_only():
    workflow = yaml.safe_load(_NIGHTLY_MAIN.read_text())
    assert [entry["cron"] for entry in workflow[True]["schedule"]] == ["0 5 * * *"]
    assert "workflow_dispatch" in workflow[True] and "push" not in workflow[True]
    assert workflow["permissions"] == {"contents": "write", "actions": "write"}
    assert workflow["concurrency"] == {"group": "nightly-main", "cancel-in-progress": False}
    (job,) = workflow["jobs"].values()
    assert job["if"] == "github.ref == 'refs/heads/main'"


def test_the_nightly_pins_main_numbers_the_dry_run_and_starts_the_lanes_in_that_order():
    workflow = yaml.safe_load(_NIGHTLY_MAIN.read_text())
    steps = workflow["jobs"]["start"]["steps"]
    assert steps[0]["uses"].startswith("actions/checkout@")
    assert steps[0]["with"] == {"fetch-depth": 0}
    runs = [str(s.get("run", "")) for s in steps]
    version = next(i for i, r in enumerate(runs) if "release_nightly.py --next" in r)
    pin = next(i for i, r in enumerate(runs) if 'HEAD:refs/heads/nightly/main"' in r)
    start = next(i for i, r in enumerate(runs) if "release_gate.py --nightly-dispatch" in r)
    assert version < pin < start
    assert steps[version]["id"] == "version"
    assert steps[start]["env"]["VERSION"] == "${{ steps.version.outputs.version }}"
    assert '--version "$VERSION"' in runs[start]
    gate = _script("release_gate")
    assert gate.NIGHTLY_BRANCH == "nightly/main"


def test_the_nightly_includes_every_lane_and_each_packaging_leg_with_publish_false():
    gate = _script("release_gate")
    assert set(gate.NIGHTLY_LANES) == _NIGHTLY_LANE_NAMES
    assert gate.NIGHTLY_LANES["packaging"] == ("build-dmg.yml", "build-duckdb-extensions.yml")
    dispatches = {argv[3]: argv for argv in gate.nightly_dispatches("v0.1.0-alpha.9")}
    every = [w for workflows in gate.NIGHTLY_LANES.values() for w in workflows]
    assert list(dispatches) == every
    for workflow, argv in dispatches.items():
        assert argv[:3] == ["gh", "workflow", "run"]
        assert argv[4:6] == ["--ref", "nightly/main"], workflow
        assert "publish=true" not in argv, workflow
    assert dispatches["build-dmg.yml"][6:] == [
        "-f",
        "version=v0.1.0-alpha.9",
        "-f",
        "publish=false",
    ]
    assert dispatches["build-duckdb-extensions.yml"][6:] == ["-f", "publish=false"]
    # build-dmg.yml is the whole release build: its macOS, Windows and Linux AppImage legs.
    legs = {body.get("uses") for body in yaml.safe_load(_RELEASE.read_text())["jobs"].values()}
    for leg in ("release-macos.yml", "release-windows.yml", "release-linux.yml"):
        assert f"./.github/workflows/{leg}" in legs, leg
    # Every workflow the nightly starts exists and can be dispatched.
    for workflow in every:
        triggers = yaml.safe_load((REPO / ".github" / "workflows" / workflow).read_text())[True]
        assert "workflow_dispatch" in triggers, workflow


def test_the_dry_run_is_numbered_as_the_next_release(tmp_path):
    def git(*args: str, cwd: Path = tmp_path) -> None:
        subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)

    origin = tmp_path / "origin.git"
    git("init", "-q", "--bare", "-b", "main", str(origin))
    work = tmp_path / "work"
    git("clone", "-q", str(origin), str(work))
    git("-c", "user.email=t@example.test", "-c", "user.name=t", "commit", "-q", "--allow-empty",
        "-m", "one", cwd=work)  # fmt: skip
    git("tag", "v0.1.0-alpha.5", cwd=work)
    git("push", "-q", "origin", "main", "--tags", cwd=work)
    nightly = _script("release_nightly")
    assert nightly.upcoming(work) == "v0.1.0-alpha.6"
    # An open alpha (its branch, untagged) is the release before the next one.
    git("push", "-q", "origin", "main:refs/heads/release/v0.1.0-alpha.6", cwd=work)
    assert nightly.upcoming(work) == "v0.1.0-alpha.7"


def test_release_ready_reads_every_lane_on_one_commit_of_the_pin(monkeypatch, capsys):
    gate = _script("release_gate")
    asked = []

    def state(workflow, branch, sha):
        asked.append((workflow, branch, sha))
        return "completed success https://example.test/run"

    monkeypatch.setattr(gate, "run_state", state)
    assert gate.main(["--ready", "--sha", "c0ffee"]) == 0
    every = [w for workflows in gate.NIGHTLY_LANES.values() for w in workflows]
    assert asked == [(w, "nightly/main", "c0ffee") for w in every]
    out = capsys.readouterr().out
    assert "Release-ready: c0ffee" in out
    for lane in _NIGHTLY_LANE_NAMES:
        assert f"| {lane} |" in out, lane


@pytest.mark.parametrize(
    "workflow",
    [
        "unit-tests.yml",
        "lint.yml",
        "requirements-ci.yml",
        "integration-suite.yml",
        "ui-e2e-core.yml",
        "build-dmg.yml",
        "build-duckdb-extensions.yml",
    ],
)
@pytest.mark.parametrize(
    "bad", ["completed failure u", "completed cancelled u", "in_progress  u", "never run  "]
)
def test_release_ready_is_red_when_any_lane_is_not_green(workflow, bad, monkeypatch, capsys):
    gate = _script("release_gate")
    monkeypatch.setattr(
        gate,
        "run_state",
        lambda w, branch, sha: bad if w == workflow else "completed success u",
    )
    assert gate.main(["--ready", "--sha", "c0ffee"]) == 1
    out = capsys.readouterr().out
    assert "Not release-ready: c0ffee" in out and f"`{workflow}` | {bad}" in out


def test_release_ready_stops_when_a_lookup_cannot_answer(monkeypatch, capsys):
    gate = _script("release_gate")

    def broken(workflow, branch, sha):
        raise gate.LookupFailed("gh run list: HTTP 502")

    monkeypatch.setattr(gate, "run_state", broken)
    assert gate.main(["--ready", "--sha", "c0ffee"]) == 1
    assert "HTTP 502" in capsys.readouterr().out


def test_release_ready_runs_daily_reads_the_pin_and_can_change_nothing():
    workflow = yaml.safe_load(_RELEASE_READY.read_text())
    assert [entry["cron"] for entry in workflow[True]["schedule"]] == ["30 20 * * *"]
    assert "workflow_dispatch" in workflow[True]
    assert workflow["permissions"] == {"contents": "read", "actions": "read"}
    job = workflow["jobs"]["ready"]
    assert job["outputs"]["sha"] == "${{ steps.night.outputs.sha }}"
    runs = [str(s.get("run", "")) for s in job["steps"]]
    assert any("git ls-remote origin refs/heads/nightly/main" in r for r in runs)
    check = next(s for s in job["steps"] if "release_gate.py --ready" in str(s.get("run", "")))
    assert check["env"]["SHA"] == "${{ steps.night.outputs.sha }}"
    assert '--sha "$SHA"' in check["run"] and "set -euo pipefail" in check["run"]


@pytest.mark.parametrize("path", [_NIGHTLY_MAIN, _RELEASE_READY])
def test_the_nightly_and_the_summary_publish_nothing(path):
    code = _code(path)
    assert not _LEAVES_THE_RUN.search(code), path.name
    assert "publish=true" not in code and "gh release" not in code and "git tag" not in code


def test_the_reasoner_bundle_is_built_checked_signed_and_kept_never_published():
    # REQ-2138: the reasoner jar's Central bundle is built from source by the pinned javac, its
    # sum checked against reasoner_bundle's pin by the build script, signed and kept as a workflow
    # artifact; the Maven Central promote is a separate step the maintainer confirms.
    path = REPO / ".github" / "workflows" / "reasoner-maven-bundle.yml"
    workflow = yaml.safe_load(path.read_text())
    assert set(workflow[True]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    steps = workflow["jobs"]["bundle"]["steps"]
    runs = [str(s.get("run") or "") for s in steps]
    build = [i for i, r in enumerate(runs) if "scripts/ci/reasoner_maven_bundle.py bundles" in r]
    sign = [i for i, r in enumerate(runs) if "--detach-sign" in r and "central-bundle.zip" in r]
    keep = [
        i
        for i, s in enumerate(steps)
        if str(s.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(build) == len(sign) == len(keep) == 1 and build[0] < sign[0] < keep[0]
    assert (REPO / "scripts" / "ci" / "reasoner_maven_bundle.py").is_file()
    text = path.read_text()
    for publish in ("central.sonatype", "oss.sonatype", "curl", "mvn deploy", "gh release"):
        assert publish not in text, publish


def test_the_reasoner_build_refuses_a_jar_whose_sum_is_not_the_pinned_one(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "reasoner_maven_bundle", REPO / "scripts" / "ci" / "reasoner_maven_bundle.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def build(out, **_):
        out.write_bytes(b"not the pinned jar")
        return "0" * 64

    monkeypatch.setattr(module, "build_jar", build)
    assert module.main(["reasoner_maven_bundle.py", str(tmp_path)]) == 1
    assert not (tmp_path / "sizes.json").exists()

    pinned = module.REASONER.sha256
    monkeypatch.setattr(module, "build_jar", lambda out, **_: (out.write_bytes(b"j"), pinned)[1])
    assert module.main(["reasoner_maven_bundle.py", str(tmp_path / "ok")]) == 0
    version = tmp_path / "ok" / module.REASONER.artifact / "io" / "simpleishard"
    version = version / module.REASONER.artifact / module.REASONER.version
    stem = f"{module.REASONER.artifact}-{module.REASONER.version}"
    for name in (f"{stem}.jar", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
        assert (version / name).is_file() and (version / f"{name}.sha256").is_file(), name


def test_the_reasoner_jar_is_reproducible_from_its_entries(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "reasoner_maven_bundle", REPO / "scripts" / "ci" / "reasoner_maven_bundle.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.write_jar({"b.class": b"b", "a/A.class": b"a"}, tmp_path / "one.jar")
    module.write_jar({"a/A.class": b"a", "b.class": b"b"}, tmp_path / "two.jar")
    assert (tmp_path / "one.jar").read_bytes() == (tmp_path / "two.jar").read_bytes()
