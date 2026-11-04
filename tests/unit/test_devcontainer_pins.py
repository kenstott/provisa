# Copyright (c) 2026 Kenneth Stott
# Canary: e9f18d82-05cd-4f13-94e0-77cdeadfd9c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2205: the dev container's toolchains and images are the repository's own pins, so a bump in
one place without the other fails here."""

# Requirements: REQ-2205

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from provisa.runtime_deps.code_runtimes import RUNTIMES

REPO_ROOT = Path(__file__).resolve().parents[2]
DEVCONTAINER = REPO_ROOT / ".devcontainer"
SEMVER = r"\d+\.\d+\.\d+"


def _devcontainer() -> dict:
    return json.loads((DEVCONTAINER / "devcontainer.json").read_text())


def _feature(name: str) -> tuple[str, dict]:
    """The pinned tag and options of ghcr.io/devcontainers/features/<name>."""
    prefix = f"ghcr.io/devcontainers/features/{name}:"
    found = [
        (ref, opts) for ref, opts in _devcontainer()["features"].items() if ref.startswith(prefix)
    ]
    assert len(found) == 1, f"one {name} feature expected, found {[ref for ref, _ in found]}"
    ref, opts = found[0]
    return ref.removeprefix(prefix), opts


def _from_image(dockerfile: Path, image: str) -> str:
    """The tag of the first ``FROM [--platform=...] <image>:<tag>`` in ``dockerfile``."""
    match = re.search(
        rf"^FROM\s+(?:--platform=\S+\s+)?{re.escape(image)}:(\S+)", dockerfile.read_text(), re.M
    )
    assert match, f"{dockerfile.name} has no FROM {image}"
    return match.group(1)


def _uv_version(dockerfile: Path) -> str:
    match = re.search(r"COPY --from=ghcr\.io/astral-sh/uv:(\S+) ", dockerfile.read_text())
    assert match, f"{dockerfile.name} copies no uv"
    return match.group(1)


def _postgres_image(compose: Path) -> str:
    return yaml.safe_load(compose.read_text())["services"]["postgres"]["image"]


def test_python_is_the_requires_python_minor():
    requires = re.search(
        r'^requires-python\s*=\s*">=(\d+\.\d+),<', (REPO_ROOT / "pyproject.toml").read_text(), re.M
    )
    assert requires
    tag = _from_image(DEVCONTAINER / "Dockerfile", "mcr.microsoft.com/devcontainers/python")
    assert re.fullmatch(rf"{SEMVER}-(\d+\.\d+)-\w+", tag), tag
    assert tag.split("-")[1] == requires.group(1)
    assert (
        _devcontainer()["containerEnv"]["UV_PYTHON"] == f"/usr/local/bin/python{requires.group(1)}"
    )


def test_uv_is_the_product_images_uv():
    assert _uv_version(DEVCONTAINER / "Dockerfile") == _uv_version(REPO_ROOT / "Dockerfile")


def test_rust_is_the_product_images_toolchain():
    rust = _from_image(REPO_ROOT / "Dockerfile", "rust")
    assert _feature("rust")[1]["version"] == rust.split("-")[0]


def test_go_is_the_code_source_pin():
    assert _feature("go")[1]["version"] == RUNTIMES["go"].version


def test_node_is_provisa_uis_pin():
    nvmrc = (REPO_ROOT / ".nvmrc").read_text().strip()
    assert _feature("node")[1]["version"] == nvmrc


def test_control_plane_is_the_compose_stacks_image():
    assert _postgres_image(DEVCONTAINER / "compose.yml") == _postgres_image(
        REPO_ROOT / "docker-compose.core.yml"
    )


def test_every_feature_is_pinned_to_a_release():
    for ref in _devcontainer()["features"]:
        assert re.fullmatch(rf"ghcr\.io/devcontainers/features/[\w-]+:{SEMVER}", ref), ref


def test_docker_is_available_inside():
    _feature("docker-in-docker")


def test_the_environment_lives_outside_the_checkout():
    # A host's own .venv in the bind-mounted checkout is never synced over.
    env = _devcontainer()["containerEnv"]["UV_PROJECT_ENVIRONMENT"]
    assert not env.startswith(_devcontainer()["workspaceFolder"])


def test_the_slow_setup_is_one_a_codespaces_prebuild_runs():
    # A prebuild runs onCreateCommand and updateContentCommand; postCreateCommand runs on every
    # codespace's first start, so the sync and builds there would never be cached.
    config = _devcontainer()
    assert config["updateContentCommand"] == "bash .devcontainer/update-content.sh"
    assert (DEVCONTAINER / "update-content.sh").is_file()
    assert "postCreateCommand" not in config


def test_codespaces_picks_a_machine_that_builds_it():
    assert _devcontainer()["hostRequirements"] == {"cpus": 4, "memory": "16gb", "storage": "32gb"}
