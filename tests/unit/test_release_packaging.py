# Copyright (c) 2026 Kenneth Stott
# Canary: 6c0b2a95-4d38-4e17-9a52-8f1e3d70b264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-854: release artifact packaging boundaries.

The core runtime image set (docker-compose.core.yml + core-images tarball) must:
- include busybox as a core dependency;
- bundle NO third-party billing mock service in ANY compose (REQ-1015: Lemon Squeezy is
  Merchant-of-Record over the public REST API; tests stub it via HTTP fixtures);
- exclude python:3.12-slim from the packaged/airgap image set — it is BUILD-TIME only
  (Dockerfiles), never a shipped service image.

These are static assertions over the compose manifests — no containers started.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]


def _service_images(compose_file: str) -> list[str]:
    path = _ROOT / compose_file
    doc = yaml.safe_load(path.read_text()) or {}
    return [
        svc["image"]
        for svc in (doc.get("services") or {}).values()
        if isinstance(svc, dict) and "image" in svc
    ]


def _service_names(compose_file: str) -> set[str]:
    path = _ROOT / compose_file
    doc = yaml.safe_load(path.read_text()) or {}
    return set((doc.get("services") or {}).keys())


# ---- busybox is a core dependency (REQ-854) ---------------------------------


def test_busybox_is_present_in_the_core_image_set():
    images = _service_images("docker-compose.core.yml")
    assert any(img == "busybox" or img.startswith("busybox:") for img in images), images


# ---- no bundled billing mock service anywhere (REQ-854, REQ-1015) -----------


def test_no_billing_mock_service_bundled():
    # Lemon Squeezy (MoR) is called over the public REST API; no stripe-mock (or any billing
    # mock) ships in dev, core, or airgap. Tests stub Lemon Squeezy via HTTP fixtures.
    for compose in (
        "docker-compose.dev.yml",
        "docker-compose.core.yml",
        "docker-compose.airgap.yml",
    ):
        images = _service_images(compose)
        names = _service_names(compose)
        assert "stripe-mock" not in names, compose
        assert not any("stripe" in img or "lemonsqueezy" in img for img in images), compose


# ---- python:3.12-slim is build-time only (REQ-854) --------------------------


def test_python_slim_is_not_a_shipped_service_image():
    # Build-time base only — it must not appear as a runtime service image in the
    # packaged (core) or airgap manifests.
    for compose in ("docker-compose.core.yml", "docker-compose.airgap.yml"):
        images = _service_images(compose)
        assert not any("3.12-slim" in img for img in images), (compose, images)


def test_python_slim_is_used_as_a_build_base_in_the_dockerfile():
    # Confirms the exclusion above is about a real build-time base, not a nonexistent image.
    df = (_ROOT / "Dockerfile").read_text()
    assert "python:3.12-slim" in df


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
