# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1e7a52-90d4-4b6f-8e2a-d5f04b9c1a67
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""build-duckdb-extensions.yml publishes provisa-duckdb-ext as one platform-tagged wheel per mirrored
platform, each holding only that platform's extensions and checked under PyPI's 100 MB limit before
publishing (maintainer decision 2026-11-06; a universal wheel of every platform's DuckDB 2.0
extensions was refused by PyPI, run 42238598659). The publish step stays in this workflow with
trusted publishing (id-token: write, no password): PyPI's trusted publisher names this file."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "build-duckdb-extensions.yml"


def _collect():
    spec = importlib.util.spec_from_file_location(
        "duckdb_ext_collect_wf", REPO / "packaging/duckdb-ext/collect.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["duckdb_ext_collect_wf"] = module
    spec.loader.exec_module(module)
    return module


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def _steps(job: str) -> dict[str, dict]:
    return {step["name"]: step for step in _jobs()[job]["steps"] if "name" in step}


def _names(job: str) -> list[str]:
    return [step.get("name", "") for step in _jobs()[job]["steps"]]


def test_a_wheel_per_mirrored_platform_each_tagged_for_it():
    # The packaging's platforms are exactly the mirror matrix's, each with its own wheel tag.
    labels = {leg["label"] for leg in _jobs()["mirror"]["strategy"]["matrix"]["include"]}
    platforms = _collect().PLATFORMS
    assert set(platforms) == labels
    assert {tag for _plat, tag in platforms.values()} == {
        "macosx_11_0_arm64",
        "manylinux_2_28_x86_64",
        "manylinux_2_28_aarch64",
        "win_amd64",
    }


def test_the_wheel_job_builds_each_platform_alone_and_retags_it():
    run = _steps("wheel")["Build one wheel per platform"]["run"]
    assert "for label in $(python packaging/duckdb-ext/collect.py labels)" in run
    assert 'collect.py collect mirror-extracted "$label"' in run
    # A previous platform's blobs in setuptools' build dir would ride along in the next wheel.
    assert "rm -rf packaging/duckdb-ext/build packaging/duckdb-ext/*.egg-info" in run
    assert "python -m build --wheel --outdir" in run
    assert "--sdist" not in run  # an sdist would carry every platform's blobs again
    assert 'collect.py wheel-tag "$label"' in run
    assert "python -m wheel tags --remove" in run


def test_each_wheels_size_is_checked_before_publishing():
    steps = _steps("wheel")
    check = "Check each wheel is under PyPI's 100 MB limit"
    assert steps[check]["run"] == "python packaging/duckdb-ext/collect.py check-wheels dist-ext"
    names = _names("wheel")
    assert names.index("Build one wheel per platform") < names.index(check)
    assert names.index(check) < names.index("Publish provisa-duckdb-ext to PyPI")


def test_publishing_stays_trusted_in_this_workflow():
    job = _jobs()["wheel"]
    assert job["permissions"]["id-token"] == "write"
    assert "environment" not in job
    publish = _steps("wheel")["Publish provisa-duckdb-ext to PyPI"]
    assert publish["uses"] == "pypa/gh-action-pypi-publish@release/v1"
    assert publish["with"]["packages-dir"] == "dist-ext/"
    assert "password" not in publish["with"]
    assert "provisa-duckdb-ext" in (REPO / "packaging/duckdb-ext/pyproject.toml").read_text()


def test_the_mirror_carries_crypto_and_community_only():
    # The core extensions come from the third-party duckdb-extension-* wheels (REQ-1142 amendment,
    # TEMPORARY EXTENSION SOURCES FOR DUCKDB 2.0): nothing here installs or packs them.
    steps = _steps("mirror")
    run = steps["Mirror + prove-load DuckDB extensions"]["run"]
    assert run.startswith("uv run --no-sync python scripts/ci/mirror_duckdb_extensions.py ")
    assert "duckdb-extension-" not in WORKFLOW.read_text()
    assert _collect().PRIORITY[0] == "crypto" and _collect().PRIORITY[-1] == "firebird"
