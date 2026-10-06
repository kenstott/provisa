# Copyright (c) 2026 Kenneth Stott
# Canary: 18df322e-c1c7-4f57-86d1-587fef7698dd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1494: the chart's platform fake key -- one Secret, given to Provisa as PROVISA_FAKE_KEY and
mounted into every Trino pod, where the engine reads it as <fingerprint>.key."""

from __future__ import annotations

import base64
import shutil
import subprocess
from pathlib import Path

import yaml

from provisa.fakes.digest import fingerprint

CHART = Path(__file__).resolve().parents[2] / "helm" / "provisa"
_BASE = ["--set", "encryption.existingSecret=k", "--set", "auth.provider=none"]


def _render() -> list[dict]:
    helm = shutil.which("helm")
    assert helm is not None, "the helm CLI is required to verify the chart"
    r = subprocess.run(
        [helm, "template", "rel", str(CHART), *_BASE], capture_output=True, text=True
    )
    assert r.returncode == 0, r.stderr
    return [d for d in yaml.safe_load_all(r.stdout) if isinstance(d, dict)]


def test_the_key_secret_names_its_engine_file_by_fingerprint():
    docs = _render()
    secret = next(
        d for d in docs if d["kind"] == "Secret" and d["metadata"]["name"] == "rel-fake-key"
    )
    key = base64.b64decode(secret["data"]["key"]).decode()
    assert len(key) == 64 and int(key, 16) >= 0
    fp = fingerprint(bytes.fromhex(key))
    assert base64.b64decode(secret["data"][f"{fp}.key"]).decode() == key
    assert secret["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"


def test_provisa_gets_the_key_and_every_trino_pod_mounts_it():
    docs = _render()
    seen = []
    for d in docs:
        if d["kind"] not in ("Deployment", "StatefulSet"):
            continue
        spec = d["spec"]["template"]["spec"]
        for c in spec["containers"]:
            env = {e["name"]: e for e in c.get("env", [])}
            if c["name"] in ("trino", "provisa"):
                seen.append((d["metadata"]["name"], c["name"]))
            if c["name"] == "trino":
                assert env["PROVISA_FAKE_KEY_DIR"]["value"] == "/etc/provisa/fake-key"
                assert {
                    "name": "fake-key",
                    "mountPath": "/etc/provisa/fake-key",
                    "readOnly": True,
                } in c["volumeMounts"]
                assert {"name": "fake-key", "secret": {"secretName": "rel-fake-key"}} in spec[
                    "volumes"
                ]
            elif c["name"] == "provisa":
                ref = env["PROVISA_FAKE_KEY"]["valueFrom"]["secretKeyRef"]
                assert ref == {"name": "rel-fake-key", "key": "key"}
    assert sorted(seen) == [
        ("rel-provisa", "provisa"),
        ("rel-trino-coordinator", "trino"),
        ("rel-trino-worker", "trino"),
    ]
