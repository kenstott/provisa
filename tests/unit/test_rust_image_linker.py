# Copyright (c) 2026 Kenneth Stott
# Canary: 5a4f0e0d-1ca7-43e3-9b03-c27eca0c5b0c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product image can prepare a Rust source (REQ-2227, amended 2026-11-10): its runtime stage
carries gcc and libc6-dev, because build scripts and procedural macros are compiled and linked on
the build machine. The image workflow proves it by preparing the Rust fixture inside the built
image (scripts/ci/rust_source_smoke.py)."""

# Requirements: REQ-2227
from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]
FIXTURE = "tests/fixtures/rust_source"


def _runtime_stage() -> str:
    """The Dockerfile's last stage: the image that ships."""
    text = (REPO / "Dockerfile").read_text()
    starts = [m.start() for m in re.finditer(r"^FROM ", text, re.MULTILINE)]
    return text[starts[-1] :]


def _runs(stage: str) -> list[str]:
    """Each RUN instruction of a stage, its continuation lines joined."""
    joined = re.sub(r"\\\n", " ", stage)
    return [line for line in joined.splitlines() if line.startswith("RUN ")]


def test_the_runtime_stage_installs_a_c_linker_and_cleans_the_apt_cache_in_that_layer():
    [run] = [r for r in _runs(_runtime_stage()) if re.search(r"\bgcc\b", r)]
    install = re.search(r"apt-get install -y --no-install-recommends ([^&]+)", run)
    assert install is not None, run
    assert {"gcc", "libc6-dev"} <= set(install.group(1).split()), run
    assert run.index("apt-get update") < install.start(), run
    assert run.rstrip().endswith("rm -rf /var/lib/apt/lists/*"), run


def test_the_image_workflow_prepares_the_rust_fixture_inside_the_built_image():
    workflow = (REPO / ".github" / "workflows" / "docker-image.yml").read_text()
    run = re.search(r"run: docker run [^\n]*rust_source_smoke\.py", workflow)
    assert run is not None, "docker-image.yml does not run the Rust source smoke"
    assert f"{FIXTURE}:/fixture:ro" in run.group(0), run.group(0)
    assert "python - /fixture <" in run.group(0), run.group(0)


def test_the_smoke_prepares_through_the_rust_language_and_its_fixture_derives_with_serde():
    smoke = (REPO / "scripts" / "ci" / "rust_source_smoke.py").read_text()
    assert 'LANGUAGES["rust"].prepare()' in smoke
    # serde's derive is a procedural macro and serde has a build script: both are linked by cc.
    manifest = (REPO / FIXTURE / "Cargo.toml").read_text()
    assert re.search(r'^serde = \{[^}]*features = \["derive"\]', manifest, re.MULTILINE), manifest
