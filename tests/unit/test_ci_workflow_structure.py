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
    callers = {f"{w}:{j}" for w, j, _b in _lane_callers()}
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
        assert _runs_a_lane(str(given["run"])), f"{workflow}:{job} runs no lane"
        for hook in ("prepare", "on-failure"):
            if hook in given:
                script = REPO / str(given[hook]).split()[0]
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
        str(b["with"].get(h, "")).split()[0]
        for _w, _j, b in _lane_callers()
        for h in ("prepare", "on-failure")
        if b["with"].get(h)
    }
    used |= {"scripts/ci/lanes/suite-prepare.sh"}  # also the warehouse and salesforce jobs' step
    assert str(script.relative_to(REPO)) in used, f"{script.name} is called by no lane"


def test_the_lane_workflow_is_passed_its_few_secrets_by_name():
    lane = yaml.safe_load((REPO / ".github" / "workflows" / "lane.yml").read_text())
    declared = set(lane[True]["workflow_call"]["secrets"])
    assert declared == {
        "ANTHROPIC_API_KEY",
        "SPLUNKBASE_USERNAME",
        "SPLUNKBASE_PASSWORD",
        "SP_CERT_P12_BASE64",
    }
    read = set(
        re.findall(r"secrets\.([A-Z0-9_]+)", (REPO / ".github/workflows/lane.yml").read_text())
    )
    assert read == declared
    for workflow, job, body in _lane_callers():
        passed = body.get("secrets", {})
        assert passed != "inherit", f"{workflow}:{job} hands the lane every secret"
        assert set(passed) <= declared, f"{workflow}:{job}"


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
