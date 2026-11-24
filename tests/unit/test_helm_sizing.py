# Copyright (c) 2026 Kenneth Stott
# Canary: c06b9bbb-cd5f-4ebf-a435-1ef7549e03d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-056: the chart's defaults are production sizing; the minikube lane deploys lean test sizing.

values.yaml is what an operator installs, so its numbers are the production ones. The cluster lane
passes helm/provisa/ci-values.yaml, which shrinks the release to what boots on its minikube, and its
memory checks (no OOM kill, boot peak under 75% of the limit) read the limit that file sets.
Rendered with the real ``helm template``.
"""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "helm" / "provisa"
CI_VALUES = CHART / "ci-values.yaml"
_REQUIRED = [
    "encryption.existingSecret=provisa-master-key",
    "credentials.existingSecret=provisa-credentials",
    "auth.provider=local",
    "auth.sessionSecret.existingSecret=provisa-session",
    "auth.breakGlass.username=platform-admin",
    "auth.breakGlass.existingSecret=provisa-break-glass",
]

PRODUCTION_PROVISA = {
    "requests": {"cpu": "1", "memory": "1Gi"},
    "limits": {"cpu": "2", "memory": "4Gi"},
}
TEST_PROVISA = {
    "requests": {"cpu": "250m", "memory": "512Mi"},
    "limits": {"cpu": "1", "memory": "2Gi"},
}


def _render(*values_files: Path) -> list[dict]:
    helm = shutil.which("helm")
    assert helm is not None, "the helm CLI is required to verify the chart"
    cmd = [helm, "template", "rel", str(CHART)]
    for f in values_files:
        cmd.append(f"--values={f}")
    for s in _REQUIRED:
        cmd += ["--set", s]
    r = subprocess.run(cmd, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return [d for d in yaml.safe_load_all(r.stdout) if d]


def _container(docs: list[dict], deployment: str, container: str) -> dict:
    (dep,) = [d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == deployment]
    (c,) = [c for c in dep["spec"]["template"]["spec"]["containers"] if c["name"] == container]
    return c


def test_chart_defaults_are_production_sizing():
    api = _container(_render(), "rel-provisa", "provisa")
    assert api["resources"] == PRODUCTION_PROVISA


def test_ci_values_give_the_lean_test_sizing():
    api = _container(_render(CI_VALUES), "rel-provisa", "provisa")
    assert api["resources"] == TEST_PROVISA


def test_ci_values_shrink_the_release_for_minikube():
    docs = _render(CI_VALUES)
    deployments = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Deployment"}
    assert deployments["rel-provisa"]["spec"]["replicas"] == 1
    assert not [d for d in docs if d["kind"] == "HorizontalPodAutoscaler"]
    assert "rel-mongodb" not in {d["metadata"]["name"] for d in docs}


def test_the_cluster_lane_deploys_with_ci_values():
    spec = importlib.util.spec_from_file_location(
        "helm_minikube_sizing", REPO / "tests" / "e2e" / "test_helm_minikube.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["helm_minikube_sizing"] = module
    spec.loader.exec_module(module)
    assert module.CI_VALUES == CI_VALUES
    assert f"--values={CI_VALUES}" in module._CHART_SETS
