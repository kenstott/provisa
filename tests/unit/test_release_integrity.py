# Copyright (c) 2026 Kenneth Stott
# Canary: 6c0b2a95-4d38-4e17-9a52-8f1e3d70b264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1175: GitHub Release integrity (never partial).

Validates that the build-dmg workflow ensures a release can only be published
in a complete state (all installers attached). The release is created as a
DRAFT by ensure-release and remains draft until build-dmg's publish-release
job verifies every installer is present and undrafts it. This prevents a
partial release from being published when a sibling workflow succeeds but
build-dmg fails.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]


def test_ensure_release_action_creates_draft():
    """Ensure release action must create the release as a DRAFT (--draft flag)."""
    action_file = _ROOT / ".github" / "actions" / "ensure-release" / "action.yml"
    content = action_file.read_text()
    assert "--draft" in content, "ensure-release action must create releases as DRAFT"


def test_build_dmg_publish_job_exists():
    """build-dmg workflow must have a publish-release job."""
    workflow = yaml.safe_load(
        (_ROOT / ".github" / "workflows" / "build-dmg.yml").read_text()
    )
    assert "jobs" in workflow
    assert "publish-release" in workflow["jobs"]


def test_build_dmg_publish_job_depends_on_all_builds():
    """publish-release job must wait for all build jobs to complete."""
    workflow = yaml.safe_load(
        (_ROOT / ".github" / "workflows" / "build-dmg.yml").read_text()
    )
    publish_job = workflow["jobs"]["publish-release"]
    needs = set(publish_job.get("needs", []))

    # All platform-specific build jobs must be completed before publishing
    required_needs = {
        "metadata",
        "build-macos-core",
        "build-linux",
        "build-windows-core",
        "build-windows-container",
        "build-jdbc",
    }
    assert required_needs.issubset(
        needs
    ), f"publish-release missing dependency on: {required_needs - needs}"


def test_build_dmg_publish_uses_fail_on_unmatched_files():
    """softprops/action-gh-release must use fail_on_unmatched_files: true."""
    workflow = yaml.safe_load(
        (_ROOT / ".github" / "workflows" / "build-dmg.yml").read_text()
    )
    publish_job = workflow["jobs"]["publish-release"]
    steps = publish_job.get("steps", [])

    attach_step = None
    for step in steps:
        if step.get("name") == "Attach installers to draft release":
            attach_step = step
            break

    assert attach_step is not None, "Missing 'Attach installers to draft release' step"
    assert (
        attach_step.get("with", {}).get("fail_on_unmatched_files") is True
    ), "fail_on_unmatched_files must be true to prevent partial releases"


def test_build_dmg_publish_keeps_draft_status():
    """publish step must upload as draft: true (never as published)."""
    workflow = yaml.safe_load(
        (_ROOT / ".github" / "workflows" / "build-dmg.yml").read_text()
    )
    publish_job = workflow["jobs"]["publish-release"]
    steps = publish_job.get("steps", [])

    attach_step = None
    for step in steps:
        if step.get("name") == "Attach installers to draft release":
            attach_step = step
            break

    assert attach_step is not None, "Missing 'Attach installers to draft release' step"
    assert (
        attach_step.get("with", {}).get("draft") is True
    ), "Attach step must use draft: true to keep release unpublished until verified"


def test_sibling_workflows_use_gh_release_upload_clobber():
    """Sibling workflows (duckdb, pg, exports) must use gh release upload --clobber."""
    for workflow_name in (
        "build-duckdb-extensions.yml",
        "build-pg-extensions.yml",
        "release-exports.yml",
    ):
        content = (_ROOT / ".github" / "workflows" / workflow_name).read_text()
        # Verify the workflow uses gh release upload with clobber (read raw, not parsed)
        assert (
            "gh release upload" in content and "--clobber" in content
        ), f"{workflow_name} must use 'gh release upload --clobber' to upload without draft/prerelease flipping"


def test_release_manifest_includes_all_installer_types():
    """publish-release files list must include all expected installer types."""
    workflow = yaml.safe_load(
        (_ROOT / ".github" / "workflows" / "build-dmg.yml").read_text()
    )
    publish_job = workflow["jobs"]["publish-release"]
    steps = publish_job.get("steps", [])

    attach_step = None
    for step in steps:
        if step.get("name") == "Attach installers to draft release":
            attach_step = step
            break

    assert attach_step is not None, "Missing 'Attach installers to draft release' step"
    files_str = attach_step.get("with", {}).get("files", "")
    assert isinstance(files_str, str)

    # The files section should reference all expected installer types via metadata outputs. The
    # Runtime DMG (dmg_runtime_name) was removed from the installer set (native venv tier replaced it),
    # so it is intentionally absent from both the workflow outputs and this list.
    expected_patterns = [
        "dmg_name",
        "dmg_obs_name",
        "dmg_demo_name",
        "linux_name",
        "windows_name",
        "windows_container_name",
        "jdbc_name",
    ]
    for pattern in expected_patterns:
        assert (
            pattern in files_str
        ), f"Installer manifest must include {pattern} to ensure complete release"


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
