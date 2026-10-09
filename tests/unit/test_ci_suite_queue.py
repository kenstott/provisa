# Copyright (c) 2026 Kenneth Stott
# Canary: fca1fce5-f73a-45f7-9d73-e8adf08b195c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The container suite's runs queue behind each other, and only a branch's own push replaces its run.

Two runs at once (one per branch) need more runners than the account's concurrent-job limit, and
left each other's lanes waiting for hours. integration-suite.yml therefore holds two concurrency
groups, at the two levels GitHub offers: per ref on the workflow (a newer push replaces that
ref's run) and suite-wide on the one job that calls the lanes (runs wait, none is cancelled)."""

from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def _caller() -> dict:
    return yaml.safe_load((WORKFLOWS / "integration-suite.yml").read_text())


def test_a_newer_push_replaces_only_its_own_refs_run():
    concurrency = _caller()["concurrency"]
    assert "github.ref" in concurrency["group"]
    assert concurrency["cancel-in-progress"] is True


def test_the_whole_run_is_one_job_in_one_suite_wide_queue():
    jobs = _caller()["jobs"]
    assert list(jobs) == ["lanes"], "a job outside the queue would run beside another run's lanes"
    lanes = jobs["lanes"]
    assert lanes["uses"] == "./.github/workflows/integration-suite-lanes.yml"
    assert lanes["concurrency"]["group"] == "integration-suite"  # no ref: every branch shares it


def test_a_waiting_run_is_never_cancelled_by_another_branchs_run():
    """The default queue keeps ONE waiting run per group and cancels the rest, so a third push
    would cancel the other branch's waiting run. `queue: max` keeps them all; it cannot be
    combined with cancel-in-progress, which is the workflow-level group's job."""
    concurrency = _caller()["jobs"]["lanes"]["concurrency"]
    assert concurrency["queue"] == "max"
    assert "cancel-in-progress" not in concurrency


def test_the_suite_runs_on_pushes_to_both_branches_nightly_and_on_demand():
    triggers = _caller()[True]  # YAML reads the key `on` as a boolean
    assert triggers["push"]["branches"] == ["integration", "main"]
    assert "schedule" in triggers and "workflow_dispatch" in triggers


def test_the_dispatchers_choices_reach_the_lanes():
    passed = _caller()["jobs"]["lanes"]["with"]
    called = yaml.safe_load((WORKFLOWS / "integration-suite-lanes.yml").read_text())
    assert set(passed) == set(called[True]["workflow_call"]["inputs"])
    # Every secret the lanes workflow declares is passed, each by its own name; none is inherited.
    secrets = _caller()["jobs"]["lanes"]["secrets"]
    declared = called[True]["workflow_call"]["secrets"]
    assert secrets == {name: f"${{{{ secrets.{name} }}}}" for name in declared}
    assert len(declared) == 60


def _pipelines_without_pipefail(script: str) -> list[str]:
    """The lines of a bash step that pipe a command that can fail into another."""
    import re

    found = []
    joined = script.replace("\\\n", " ")  # a pipeline continued over lines is one line
    for line in joined.splitlines():
        code = line.split(" #")[0]
        if (
            code.lstrip().startswith("#")
            or "=~" in code
            or re.search(r"\bcase\b|\)\s*;;|^\s*[^ ]+\)", code)
        ):
            continue  # a comment, a regex match, a case pattern: their bars are not pipes
        code = re.sub(r"\$\((?:[^()]|\([^()]*\))*\)", "", code)  # $(...) has its own status
        code = re.sub(r"'[^']*'|\"[^\"]*\"", "", code).replace("||", "")
        stages = [stage.strip() for stage in code.split("|")]
        # A stage that only prints (echo, printf) cannot fail; anything else can.
        if any(stage and not re.match(r"(echo|printf)\b", stage) for stage in stages[:-1]):
            found.append(line.strip())
    return found


def test_no_workflow_step_hides_a_failing_command_behind_a_pipe():
    """A step's shell is `bash -e {0}` unless the workflow names one: with no pipefail, the
    status of `a | b` is b's. A BDD job piped pytest into `tail`, collected nothing, and was
    green for as long as it existed; a dependency export piped into `grep` could hand the CVE
    audit an empty file; a failed `docker save` piped into gzip could ship a truncated image.
    Every step that pipes a command that can fail sets pipefail, or runs under a named
    `shell: bash` (which GitHub runs with pipefail)."""
    import yaml

    hidden = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        workflow = yaml.safe_load(path.read_text())
        default_shell = ((workflow.get("defaults") or {}).get("run") or {}).get("shell")
        for job_name, job in (workflow.get("jobs") or {}).items():
            job_shell = ((job.get("defaults") or {}).get("run") or {}).get("shell")
            for step in job.get("steps") or []:
                script = step.get("run")
                shell = step.get("shell") or job_shell or default_shell
                if not script or shell == "bash" or "pipefail" in script:
                    continue
                if shell and not shell.startswith("bash"):
                    continue  # pwsh, python: not this rule's
                for line in _pipelines_without_pipefail(script):
                    hidden.append(f"{path.name}: {job_name}: {step.get('name')}: {line[:90]}")
    assert hidden == []


def test_the_pipeline_rule_tells_pipes_from_other_bars():
    assert _pipelines_without_pipefail("uv run pytest tests | tail -20") != []
    assert _pipelines_without_pipefail("docker save img | gzip -9 > x.tar.gz") != []
    assert _pipelines_without_pipefail("uv export \\\n  | grep -v x > out.txt") != []
    assert _pipelines_without_pipefail('printf "%s" "$SECRET" | base64 -d > key') == []
    assert _pipelines_without_pipefail('if [[ "$TAG" =~ -rc(\\.|$) ]]; then') == []
    assert _pipelines_without_pipefail("case x in *,all,*|*,cluster,*) c=true;; esac") == []
    assert _pipelines_without_pipefail("v=$(echo x | sed s/a/b/)") == []
    assert _pipelines_without_pipefail("cmd || true") == []
