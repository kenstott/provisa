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

import re
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
    r"key: trino-plugins-": "trino-plugins",
    r"gh release create": "ensure-release",
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


# --- one lane workflow (stage 2) ------------------------------------------------------------------

LANE = "./.github/workflows/lane.yml"
_RUNNER = "playwright " + "test"
_LEAF_CHECK = "ci-leaf-check.yml"

# The suite's three jobs that keep steps of their own (lane.yml's header says why): each carries
# live credentials or cluster tooling, and the lane workflow declares only the few secrets its
# callers pass by name instead of inheriting every one.
_OWN_STEPS = {
    "integration-suite-lanes.yml": {"cluster", "warehouse", "salesforce"},
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
        for h in ("prepare", "on-failure", "after")
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
# live sources' credentials, which ONLY the core UI workflow passes.
_LANE_SECRETS = {
    "ANTHROPIC_API_KEY",
    "SPLUNKBASE_USERNAME",
    "SPLUNKBASE_PASSWORD",
    "SP_CERT_P12_BASE64",
}
_LIVE_SOURCE_SECRETS = {
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE",
    "DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN",
    "GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS_JSON", "GSHEETS_TEST_SHEET_ID",
    "FABRIC_SQL_SERVER", "FABRIC_DATABASE", "FABRIC_RESOURCE_GROUP", "FABRIC_CAPACITY_NAME",
    "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID",
    "SP_TENANT_ID", "SP_CLIENT_ID", "SP_SITE_URL", "SP_CERT_PASSWORD",
}  # fmt: skip


def test_the_lane_workflow_is_passed_its_secrets_by_name():
    """It declares each secret it can be given and inherits none; a lane job reads only what its
    caller passed. The live sources' credentials are passed by the core UI workflow alone."""
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
        if workflow != "ui-e2e-core.yml":
            assert not set(passed) & _LIVE_SOURCE_SECRETS, f"{workflow}:{job}"
    core = yaml.safe_load((REPO / ".github/workflows/ui-e2e-core.yml").read_text())["jobs"]
    passed = set(core["playwright"]["secrets"]) | set(core["provisioning"]["secrets"])
    assert _LIVE_SOURCE_SECRETS <= passed
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
    # The Fabric capacity the lane resumed is paused again whatever the lane's outcome.
    assert "suspend_capacity()" in after
    lane = yaml.safe_load((REPO / ".github/workflows/lane.yml").read_text())["jobs"]["lane"]
    after_step = next(s for s in lane["steps"] if s.get("name") == "After the lane")
    assert "always()" in after_step["if"]


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
    r"docker push"
)


def _release_legs() -> dict[str, tuple[str, dict]]:
    """{orchestrator job: (leg file, parsed leg)}."""
    jobs = yaml.safe_load(_RELEASE.read_text())["jobs"]
    legs = {}
    for job, body in jobs.items():
        assert body["uses"].startswith("./.github/workflows/release-"), f"{job} is not a leg"
        name = body["uses"].rsplit("/", 1)[-1]
        legs[job] = (name, yaml.safe_load((REPO / ".github" / "workflows" / name).read_text()))
    return legs


def test_the_release_orchestrator_only_calls_legs_and_gives_each_the_same_version_and_publish():
    jobs = yaml.safe_load(_RELEASE.read_text())["jobs"]
    given = {job: body["with"] for job, body in jobs.items()}
    first = next(iter(given.values()))
    assert set(first) == {"version", "publish"}
    assert all(w == first for w in given.values()), "the legs are not built for one version"
    assert first["version"] == "${{ inputs.version || github.ref_name }}"
    # A tag push publishes. A manual run publishes only when started on the tag it names.
    assert first["publish"] == (
        "${{ github.event_name == 'push' || (inputs.publish == true && github.ref_type == 'tag' "
        "&& github.ref_name == inputs.version) }}"
    )
    trigger = yaml.safe_load(_RELEASE.read_text())[True]
    assert trigger["push"] == {"tags": ["v*"]}
    assert trigger["workflow_dispatch"]["inputs"]["publish"]["default"] is False
    for body in jobs.values():
        assert "steps" not in body and body.get("secrets") != "inherit"


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
    assert seen >= 14, f"only {seen} publishing steps found: the pattern no longer matches them"


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


def test_a_leg_that_reads_another_legs_artifact_starts_after_it():
    """Artifacts are the run's: a leg downloads what another leg uploaded, so the orchestrator
    must order them. Every named download in a leg is uploaded in that leg or in one it needs."""
    orchestrator = yaml.safe_load(_RELEASE.read_text())["jobs"]

    def needed(job: str) -> set[str]:
        direct = orchestrator[job].get("needs") or []
        direct = [direct] if isinstance(direct, str) else direct
        return set(direct).union(*(needed(d) for d in direct)) if direct else set()

    uploads: dict[str, set[str]] = {}
    downloads: dict[str, set[str]] = {}
    for job, (_name, leg) in _release_legs().items():
        for body in leg["jobs"].values():
            for step in body.get("steps") or []:
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
    # `ps` is asked for the arguments column and no environment (no `e` modifier, no /proc read).
    (asked,) = re.findall(r"ps (-\S+ \S+)", commands)
    assert asked == "-eo pid=,pcpu=,pmem=,rss=,args="
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
        assert "--reporter=list,html,json" in given["run"]
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


# The proof's level-2 jobs call one real caller of each kind and pass it, by name, exactly what
# that lane reads on main. They exist only on the proof branch: the flip commit removes them.
_REAL_CALLER_SECRETS = {
    "real-amd64-engines": set(),
    "real-suite-neo4j": {"SPLUNKBASE_USERNAME", "SPLUNKBASE_PASSWORD"},
    "real-ui-trino": {"SP_CERT_P12_BASE64"},
    "real-suite": {"ANTHROPIC_API_KEY", "SPLUNKBASE_USERNAME", "SPLUNKBASE_PASSWORD"},
    "real-ui-swap": set(),
}
# The level at which each starts (the neo4j lane alone only AT level 2: level 3 runs the suite).
_REAL_CALLER_GATE = {
    "real-amd64-engines": ">= 2",
    "real-suite-neo4j": "== 2",
    "real-ui-trino": ">= 2",
    "real-suite": ">= 3",
    "real-ui-swap": ">= 3",
}


def test_the_leaf_check_reads_no_secret_below_level_two_and_can_publish_nothing():
    for name in (_LEAF_CHECK, "ci-leaf-stub-leg.yml"):
        path = REPO / ".github" / "workflows" / name
        workflow = yaml.safe_load(path.read_text())
        # Its token reads the repository and nothing else, for every job.
        assert workflow["permissions"] == {"contents": "read"}
        for job, body in workflow["jobs"].items():
            allowed = _REAL_CALLER_SECRETS.get(job, set()) if name == _LEAF_CHECK else set()
            read = set(re.findall(r"secrets\.([A-Za-z0-9_]+)", yaml.safe_dump(body)))
            assert read == allowed, f"{name}:{job} reads {sorted(read)}"
            # A value passed as a secret is one of those, or a literal (the lane's self-test
            # passes a made-up string to show an expression over the matrix reaches one lane).
            passed = body.get("secrets") or {}
            assert {k for k, v in passed.items() if "secrets." in str(v)} == allowed, (
                f"{name}:{job}"
            )
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
    # A job that calls a real caller is a level-2 job, gated on the level file.
    jobs = yaml.safe_load((REPO / ".github" / "workflows" / _LEAF_CHECK).read_text())["jobs"]
    real = {
        job
        for job, body in jobs.items()
        if str(body.get("uses", "")).startswith("./.github/workflows/")
        and body["uses"] not in (LANE, "./.github/workflows/ci-leaf-stub-leg.yml")
    }
    assert real == set(_REAL_CALLER_SECRETS)
    for job in real:
        assert jobs[job]["needs"] == ["scope", "level-1"]
        assert jobs[job]["if"] == f"fromJSON(needs.scope.outputs.level) {_REAL_CALLER_GATE[job]}"
    # Neither suite call asks for the lanes that hold live credentials.
    for job in ("real-suite", "real-suite-neo4j"):
        assert set(jobs[job]["with"]) == {"lanes"}


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
    left_out = {"apple-signing", "nixos-vm"}
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


def test_the_proof_level_is_one_committed_file_and_each_level_needs_the_one_below():
    """A push proves up to the level `.github/ci-proof-level` names, so it does not start
    everything; level 1's jobs need level 0's gate, which needs every composite action's check."""
    _text, workflow = _leaf_check()
    level = (REPO / ".github" / "ci-proof-level").read_text().strip()
    assert level in {"0", "1", "2", "3"}, level
    jobs = workflow["jobs"]
    assert jobs["scope"]["outputs"] == {"level": "${{ steps.level.outputs.level }}"}
    level_one = {"lane", "lane-fails", "release-dry-run"}
    for job in level_one:
        assert jobs[job]["needs"] == ["scope", "level-0"], job
        assert jobs[job]["if"] == "fromJSON(needs.scope.outputs.level) >= 1", job
    level_zero = set(jobs["level-0"]["needs"])
    assert level_zero == {
        job for job, body in jobs.items() if str(body.get("name", "")).startswith("action / ")
    }
    assert len(level_zero) == 11


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
    """.github/README.md states each rule beside the test that holds it. A rule whose test was
    renamed or removed would read as held when it is not."""
    primer = (REPO / ".github" / "README.md").read_text()
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
