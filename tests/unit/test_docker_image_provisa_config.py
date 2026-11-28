# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8a51c6-92f4-4b7e-a0d3-6e1f2b9c4a75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every launch of the app image in the Docker Image workflow sets PROVISA_CONFIG. The variable has
no default (provisa/core/config_location.py raises naming it), and the image bakes none, so a
`docker run` without it failed the UI smoke: the UI server's query-node gate reads the loaded config
on every request, and GET / raised "PROVISA_CONFIG is not set".

- The Dockerfile sets no PROVISA_CONFIG: the launcher names the config, the image never defaults it.
- Every `docker run` of the image in docker-image.yml passes PROVISA_CONFIG.
- The file it names is one the Dockerfile's runtime stage bakes into /app/config."""

from __future__ import annotations

import posixpath
import re
import shlex

import pytest
import yaml

from tests.unit.test_dockerfile_copy_sources import REPO, _context_copies, _instructions

pytestmark = pytest.mark.unit

WORKFLOW = REPO / ".github" / "workflows" / "docker-image.yml"
IMAGE = "provisa/provisa:ci"


def _image_runs() -> list[list[str]]:
    """The words of each `docker run` command in the workflow that runs the image it builds."""
    workflow = yaml.safe_load(WORKFLOW.read_text())
    runs = []
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            script = step.get("run")
            if not script:
                continue
            for line in script.splitlines():
                # Redirections (`< script.py`) are the shell's, not docker's.
                command = re.split(r"\s<\s", line, maxsplit=1)[0]
                words = shlex.split(command)
                if words[:2] == ["docker", "run"] and IMAGE in words:
                    runs.append(words)
    return runs


def _env_value(words: list[str], name: str) -> str | None:
    """The value a `docker run` gives ``name`` with -e/--env, or None when it gives none."""
    for flag, value in zip(words, words[1:]):
        if flag in ("-e", "--env") and value.split("=", 1)[0] == name:
            return value.split("=", 1)[1] if "=" in value else None
    return None


def _baked_configs() -> set[str]:
    """Absolute paths of the files the runtime stage copies into /app/config."""
    baked = set()
    for sources, destination in _context_copies(_instructions()):
        if not destination.rstrip("/").endswith("config"):
            continue
        target = posixpath.normpath(posixpath.join("/app", destination))
        for source in sources:
            baked.add(posixpath.join(target, posixpath.basename(source)))
    return baked


def test_the_dockerfile_bakes_no_provisa_config():
    for instruction in _instructions():
        if instruction.split()[0].upper() in ("ENV", "ARG"):
            assert "PROVISA_CONFIG" not in instruction, instruction


def test_the_workflow_runs_the_image():
    assert _image_runs(), f"no `docker run {IMAGE}` in {WORKFLOW}"


@pytest.mark.parametrize("words", _image_runs(), ids=lambda words: " ".join(words[-3:]))
def test_every_image_run_sets_provisa_config_to_a_baked_file(words):
    value = _env_value(words, "PROVISA_CONFIG")
    assert value, f"`{' '.join(words)}` launches the image without PROVISA_CONFIG"
    assert value in _baked_configs(), (
        f"PROVISA_CONFIG={value} is not a config the Dockerfile bakes: {sorted(_baked_configs())}"
    )
