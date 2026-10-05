# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The helm cluster test's time bounds nest: each call's own deadline, inside the per-test bound,
inside the CI job's timeout. A bound that does not cover what it times out turns a named failure
("helm install failed" + the pod report) into a bare timeout, or a cancelled job whose log is
gone — the cluster lane lost a whole run to exactly that (helm --wait 1200 s under a 900 s
pytest-timeout)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]

# The cluster job's steps before the lane: checkout, uv sync, minikube + helm install, the zaychik
# image build — about 5 minutes on CI (run 37310375782); twice that as the allowance.
_JOB_PREP_S = 600


def _helm_module():
    spec = importlib.util.spec_from_file_location(
        "helm_minikube_bounds", REPO / "tests" / "e2e" / "test_helm_minikube.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["helm_minikube_bounds"] = module
    spec.loader.exec_module(module)
    return module


def test_the_per_test_bound_is_the_sum_of_every_deadline_one_test_can_be_charged():
    m = _helm_module()
    report = m.REPORT_BUDGET_S + m.KUBECTL_S
    helm = m.HELM_WAIT_S + m._HELM_SLACK_S
    setup = (
        m.DOWNLOAD_S
        + m.MINIKUBE_STATUS_S
        + m.MINIKUBE_START_S
        + m.KUBE_CONTEXT_S
        + m.DOCKER_BUILD_S
        + 2 * m.IMAGE_LOAD_S
        + m.DOCKER_TAG_S
        + m.NAMESPACE_DELETE_S
        + m.MINIKUBE_SSH_S
        + (1 + m._SECRETS) * m.KUBECTL_S
        + m.HELM_INSTALL_WAIT_S
        + m._HELM_SLACK_S
        + report
    )
    longest_test = 2 * (helm + report) + m.KUBECTL_S
    teardown = 2 * m.KUBECTL_S + m.MINIKUBE_SSH_S + m.MINIKUBE_DELETE_S
    assert m.CLUSTER_TEST_BOUND_S == setup + longest_test + teardown


def test_the_module_is_marked_with_that_bound():
    m = _helm_module()
    timeouts = [mark for mark in m.pytestmark if mark.name == "timeout"]
    assert [mark.args for mark in timeouts] == [(m.CLUSTER_TEST_BOUND_S,)]


def test_the_cluster_job_outlasts_the_bound():
    m = _helm_module()
    workflow = yaml.safe_load(
        (REPO / ".github" / "workflows" / "integration-suite.yml").read_text()
    )
    minutes = workflow["jobs"]["cluster"]["timeout-minutes"]
    assert minutes * 60 >= m.CLUSTER_TEST_BOUND_S + _JOB_PREP_S, (
        f"cluster job timeout-minutes {minutes} does not cover the {m.CLUSTER_TEST_BOUND_S} s "
        f"per-test bound plus {_JOB_PREP_S} s of prep"
    )
