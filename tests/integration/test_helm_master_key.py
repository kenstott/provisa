# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7c9b52-1f48-4a06-9d35-8b0e2c6f4a71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Helm chart gives the deployment's master key a home that outlives a pod.

The product keeps a master key it mints in its data directory. In a pod that directory is the
container's own filesystem: replace the pod and the key is gone, while the vault it encrypted is
still in the control-plane database — the next pod cannot open it and refuses to start. The
chart therefore takes the key from a Kubernetes Secret, or puts the data directory on a
persistent volume, and does not render with neither. It has no default key and generates none."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.integration]

CHART = Path(__file__).resolve().parents[2] / "helm" / "provisa"
needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="helm CLI not installed")


def _render(*sets: str) -> subprocess.CompletedProcess:
    # The chart also refuses to render until an auth provider is chosen (REQ-1265,
    # tests/unit/test_helm_auth.py); these tests are about the master key.
    cmd = ["helm", "template", "rel", str(CHART), "--set", "auth.provider=none"]
    for s in sets:
        cmd += ["--set", s]
    return subprocess.run(cmd, capture_output=True, text=True)


def _documents(rendered: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(rendered) if isinstance(d, dict)]


def _api(rendered: str) -> tuple[dict, dict]:
    """The API Deployment's pod spec and its ``provisa`` container."""
    deployment = next(
        d
        for d in _documents(rendered)
        if d["kind"] == "Deployment" and d["metadata"]["name"] == "rel-provisa"
    )
    pod = deployment["spec"]["template"]["spec"]
    return pod, next(c for c in pod["containers"] if c["name"] == "provisa")


def _env(container: dict) -> dict[str, dict]:
    return {e["name"]: e for e in container["env"]}


@needs_helm
class TestMasterKeyHasAHome:
    def test_rendering_fails_when_the_key_has_nowhere_to_live(self):
        r = _render()
        assert r.returncode != 0
        assert "the deployment's master key has nowhere to live" in r.stderr
        assert "encryption.existingSecret" in r.stderr
        assert "encryption.dataVolume.enabled=true" in r.stderr

    def test_a_secret_is_given_to_the_api_as_the_key(self):
        r = _render("encryption.existingSecret=provisa-master-key")
        assert r.returncode == 0, r.stderr
        _pod, container = _api(r.stdout)
        key = _env(container)["PROVISA_ENCRYPTION_KEY"]
        assert key["valueFrom"]["secretKeyRef"] == {
            "name": "provisa-master-key",
            "key": "master-key",
        }
        assert "value" not in key  # never a literal in the manifest
        assert "PROVISA_DATA_DIR" not in _env(container)
        assert not [
            d
            for d in _documents(r.stdout)
            if d["kind"] == "PersistentVolumeClaim" and "provisa-data" in d["metadata"]["name"]
        ]

    def test_the_secrets_key_name_is_the_operators(self):
        r = _render("encryption.existingSecret=vault-synced", "encryption.secretKey=PROVISA_KEY")
        assert r.returncode == 0, r.stderr
        ref = _env(_api(r.stdout)[1])["PROVISA_ENCRYPTION_KEY"]["valueFrom"]["secretKeyRef"]
        assert ref == {"name": "vault-synced", "key": "PROVISA_KEY"}

    def test_a_data_volume_is_mounted_at_the_data_directory(self):
        r = _render("encryption.dataVolume.enabled=true")
        assert r.returncode == 0, r.stderr
        pod, container = _api(r.stdout)
        assert _env(container)["PROVISA_DATA_DIR"]["value"] == "/var/lib/provisa"
        assert {"name": "data", "mountPath": "/var/lib/provisa"} in container["volumeMounts"]
        assert {
            "name": "data",
            "persistentVolumeClaim": {"claimName": "rel-provisa-data"},
        } in pod["volumes"]
        claim = next(
            d
            for d in _documents(r.stdout)
            if d["kind"] == "PersistentVolumeClaim" and d["metadata"]["name"] == "rel-provisa-data"
        )
        assert claim["spec"]["accessModes"] == ["ReadWriteMany"]  # every replica mounts it
        assert claim["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
        assert "PROVISA_ENCRYPTION_KEY" not in _env(container)  # the chart invents no key

    def test_an_existing_claim_is_used_and_none_is_created(self):
        r = _render(
            "encryption.dataVolume.enabled=true", "encryption.dataVolume.existingClaim=ops-data"
        )
        assert r.returncode == 0, r.stderr
        pod, _container = _api(r.stdout)
        assert {"name": "data", "persistentVolumeClaim": {"claimName": "ops-data"}} in pod[
            "volumes"
        ]
        assert not [
            d
            for d in _documents(r.stdout)
            if d["kind"] == "PersistentVolumeClaim" and d["metadata"]["name"] == "rel-provisa-data"
        ]

    def test_a_single_writer_volume_cannot_be_the_keys_only_home_for_several_replicas(self):
        r = _render(
            "encryption.dataVolume.enabled=true", "encryption.dataVolume.accessMode=ReadWriteOnce"
        )
        assert r.returncode != 0
        assert "every replica must mount the same volume" in r.stderr
        # One replica, no autoscaling: a single-writer volume is enough.
        one = _render(
            "encryption.dataVolume.enabled=true",
            "encryption.dataVolume.accessMode=ReadWriteOnce",
            "provisa.replicaCount=1",
            "provisa.hpa.enabled=false",
        )
        assert one.returncode == 0, one.stderr

    def test_both_may_be_set(self):
        r = _render("encryption.existingSecret=k", "encryption.dataVolume.enabled=true")
        assert r.returncode == 0, r.stderr
        env = _env(_api(r.stdout)[1])
        assert "PROVISA_ENCRYPTION_KEY" in env and "PROVISA_DATA_DIR" in env

    def test_the_chart_holds_no_key_of_its_own(self):
        values = (CHART / "values.yaml").read_text()
        loaded = yaml.safe_load(values)["encryption"]
        assert loaded["existingSecret"] == "" and loaded["dataVolume"]["enabled"] is False
        for template in (CHART / "templates").glob("*.yaml"):
            text = template.read_text()
            assert "randAlphaNum" not in text and "genPrivateKey" not in text, template.name
