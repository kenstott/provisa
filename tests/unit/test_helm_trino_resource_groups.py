# Copyright (c) 2026 Kenneth Stott
# Canary: 0a9f9f2a-79fd-4805-a2ad-2104a2b582b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-056: the chart ships Trino's resource groups, the same file the repository's Trino uses.

The file places each engine statement in its org's group by the statement's source (see
tests/unit/test_trino_resource_group_source.py). It is read by the coordinator alone, and a
subPath mount is never refreshed, so a change to it rolls the coordinator.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "helm" / "provisa"
_BASE = ["--set", "encryption.existingSecret=k", "--set", "auth.provider=none"]


def _render(*sets: str) -> list[dict]:
    helm = shutil.which("helm")
    assert helm is not None, "the helm CLI is required to verify the chart"
    cmd = [helm, "template", "rel", str(CHART), *_BASE]
    for s in sets:
        cmd += ["--set", s]
    r = subprocess.run(cmd, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return [d for d in yaml.safe_load_all(r.stdout) if isinstance(d, dict)]


def _named(docs: list[dict], kind: str, name: str) -> dict:
    kinds = {"Deployment", "StatefulSet"} if kind == "workload" else {kind}
    return next(d for d in docs if d["kind"] in kinds and d["metadata"]["name"] == name)


def _container(docs: list[dict], deployment: str) -> dict:
    pod = _named(docs, "workload", deployment)["spec"]["template"]["spec"]
    return next(c for c in pod["containers"] if c["name"] == "trino")


def test_the_config_map_carries_the_repositorys_resource_groups():
    data = _named(_render(), "ConfigMap", "rel-trino-config")["data"]
    assert json.loads(data["resource-groups.json"]) == json.loads(
        (REPO / "trino" / "etc" / "resource-groups.json").read_text()
    )
    # The chart cannot read outside its own directory, so it carries a copy; it must not drift.
    assert (CHART / "files" / "resource-groups.json").read_text() == (
        REPO / "trino" / "etc" / "resource-groups.json"
    ).read_text()
    assert (
        data["resource-groups.properties"]
        == (REPO / "trino" / "etc" / "resource-groups.properties").read_text()
    )


def test_the_coordinator_mounts_both_files():
    mounts = {
        m["mountPath"]: m.get("subPath")
        for m in _container(_render(), "rel-trino-coordinator")["volumeMounts"]
    }
    assert mounts["/etc/trino/resource-groups.json"] == "resource-groups.json"
    assert mounts["/etc/trino/resource-groups.properties"] == "resource-groups.properties"


def test_workers_do_not_mount_them():
    mounts = [m["mountPath"] for m in _container(_render(), "rel-trino-worker")["volumeMounts"]]
    assert not [m for m in mounts if "resource-groups" in m]


def test_a_change_to_the_trino_config_rolls_the_coordinator():
    def digest(*sets):
        pod = _named(_render(*sets), "workload", "rel-trino-coordinator")["spec"]["template"]
        return pod["metadata"]["annotations"]["checksum/trino-config"]

    assert digest() == digest()
    assert digest() != digest("trino.memory.maxMemory=7GB")
