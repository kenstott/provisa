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
    assert _caller()["jobs"]["lanes"]["secrets"] == "inherit"
