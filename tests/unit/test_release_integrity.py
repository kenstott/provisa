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


_WORKFLOWS = _ROOT / ".github" / "workflows"
# The release is an orchestrator (build-dmg.yml) and its legs; publish-release is a job of the
# last leg, which the orchestrator starts only after every other leg has succeeded.
_LEGS = {
    "prebuilt": "release-prebuilt.yml",
    "packages": "release-packages.yml",
    "macos": "release-macos.yml",
    "windows": "release-windows.yml",
    "linux": "release-linux.yml",
    "publish": "release-publish.yml",
}


def _load(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text())


def _publish_job() -> dict:
    return _load("release-publish.yml")["jobs"]["publish-release"]


def test_build_dmg_publish_job_exists():
    """The release has a publish-release job, in the leg the orchestrator calls `publish`."""
    orchestrator = _load("build-dmg.yml")["jobs"]
    assert {job: body["uses"] for job, body in orchestrator.items()} == {
        job: f"./.github/workflows/{leg}" for job, leg in _LEGS.items()
    }
    assert "publish-release" in _load("release-publish.yml")["jobs"]


def test_build_dmg_publish_job_depends_on_all_builds():
    """Publishing waits for every build: the orchestrator's `publish` job needs every other leg,
    and each build job is a job of one of those legs."""
    orchestrator = _load("build-dmg.yml")["jobs"]
    assert set(orchestrator["publish"]["needs"]) == set(_LEGS) - {"publish"}
    built = {job for leg in set(_LEGS) - {"publish"} for job in _load(_LEGS[leg])["jobs"]}
    required = {
        "build-macos-core",
        "build-macos-container",
        "build-macos-obs",
        "build-macos-demo",
        "build-linux",
        "build-windows-core",
        "build-windows-container",
        "build-jdbc",
        "build-python-client",
        "package-obs-images",
        "package-demo-images",
        "package-core-images",
        "package-core-images-amd64",
        "package-plugins",
        # The server wheel too (v0.1.0-alpha.478 was undrafted while its job had failed).
        "build-provisa-wheel",
    }
    assert required <= built, f"no leg builds: {required - built}"
    # A leg that reads another leg's artifacts starts after it.
    for leg in ("macos", "windows", "linux"):
        assert orchestrator[leg]["needs"] == "prebuilt"
    # Inside the last leg the engine image still follows the release it reads its assets from.
    assert (
        "publish-release" in _load("release-publish.yml")["jobs"]["publish-engine-image"]["needs"]
    )


def test_build_dmg_publish_uses_fail_on_unmatched_files():
    """softprops/action-gh-release must use fail_on_unmatched_files: true."""
    publish_job = _publish_job()
    steps = publish_job.get("steps", [])

    attach_step = None
    for step in steps:
        if step.get("name") == "Attach installers to draft release":
            attach_step = step
            break

    assert attach_step is not None, "Missing 'Attach installers to draft release' step"
    assert attach_step.get("with", {}).get("fail_on_unmatched_files") is True, (
        "fail_on_unmatched_files must be true to prevent partial releases"
    )


def test_build_dmg_publish_keeps_draft_status():
    """publish step must upload as draft: true (never as published)."""
    publish_job = _publish_job()
    steps = publish_job.get("steps", [])

    attach_step = None
    for step in steps:
        if step.get("name") == "Attach installers to draft release":
            attach_step = step
            break

    assert attach_step is not None, "Missing 'Attach installers to draft release' step"
    assert attach_step.get("with", {}).get("draft") is True, (
        "Attach step must use draft: true to keep release unpublished until verified"
    )


def test_sibling_workflows_use_gh_release_upload_clobber():
    """Sibling workflows (duckdb, pg, exports) must use gh release upload --clobber."""
    for workflow_name in (
        "build-duckdb-extensions.yml",
        "build-pg-extensions.yml",
        "release-exports.yml",
    ):
        content = (_ROOT / ".github" / "workflows" / workflow_name).read_text()
        # Verify the workflow uses gh release upload with clobber (read raw, not parsed)
        assert "gh release upload" in content and "--clobber" in content, (
            f"{workflow_name} must use 'gh release upload --clobber' to upload without draft/prerelease flipping"
        )


def test_release_manifest_includes_all_installer_types():
    """publish-release files list must include all expected installer types."""
    publish_job = _publish_job()
    steps = publish_job.get("steps", [])

    attach_step = None
    for step in steps:
        if step.get("name") == "Attach installers to draft release":
            attach_step = step
            break

    assert attach_step is not None, "Missing 'Attach installers to draft release' step"
    # One list: the "Release assets present" step holds it, proves every file is there (on a dry
    # run too, where nothing is attached), and hands it to the attach step.
    assert attach_step["with"]["files"] == "${{ steps.assets.outputs.files }}"
    assets_step = next(s for s in steps if s.get("id") == "assets")
    assert steps.index(assets_step) < steps.index(attach_step)
    assert "if" not in assets_step, "the asset check runs on every run"
    files_str = assets_step["env"]["FILES"]
    assert isinstance(files_str, str)
    assert (
        'compgen -G "$pattern"' in assets_step["run"] and '[ "$missing" = 0 ]' in assets_step["run"]
    )

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
        assert pattern in files_str, (
            f"Installer manifest must include {pattern} to ensure complete release"
        )


def test_a_cve_scan_gives_the_advisory_lookup_time_to_answer():
    """pip-audit's lookup of PyPI has a 15 s socket timeout by default; on v0.1.0-alpha.478 it
    timed out and failed the server wheel's job with no finding at all (run 37959190134). Every
    scan gives it 60 s. A timeout still fails the job -- a scan that did not finish is not a
    pass -- and pip-audit has no retry option of its own, so none is wrapped around it."""
    scans = []
    for path in sorted((_ROOT / ".github" / "workflows").glob("*.yml")):
        for line in path.read_text().splitlines():
            code = line.split("#")[0]
            if "pip-audit -" in code and "pip install" not in code:
                scans.append((path.name, code.strip()))
    assert len(scans) == 5, scans
    for name, command in scans:
        assert "pip-audit --timeout 60 " in command, f"{name}: {command}"
        assert "||" not in command and "until " not in command and "retry" not in command, name


def test_no_publication_is_reachable_without_every_build():
    """Nothing that cannot be taken back happens until every artifact exists. Every job that
    uploads to PyPI, pushes an image or attaches to and undrafts the release is a job of the
    publish leg, which the orchestrator starts only after every other leg -- so each needs every
    job that builds or packages an artifact. On v0.1.0-alpha.478 the release was undrafted and
    the client was on PyPI while the server wheel's job had failed (run 37959190134)."""
    import re

    publishes = re.compile(
        r"pypi-publish|softprops/action-gh-release|gh release edit|docker push|twine upload"
    )
    where: dict[str, str] = {}
    builds: dict[str, str] = {}
    for leg, name in _LEGS.items():
        for job, body in _load(name)["jobs"].items():
            if job.startswith(("build-", "package-")):
                builds[job] = leg
            for step in body.get("steps") or []:
                text = f"{step.get('uses') or ''}\n{step.get('run') or ''}"
                pushes = (step.get("with") or {}).get("push") is not None
                if publishes.search(text) or pushes:
                    where[job] = leg
    assert where == {
        "publish-pypi": "publish",
        "publish-provisa-pypi": "publish",
        "publish-release": "publish",
        "publish-engine-image": "publish",
        "publish-zaychik-image": "publish",
    }
    assert len(builds) >= 15 and "publish" not in builds.values(), builds
    orchestrator = _load("build-dmg.yml")["jobs"]
    assert set(orchestrator["publish"]["needs"]) == set(builds.values()) == set(_LEGS) - {"publish"}
    # Inside the leg: undrafted only after both PyPI publications; the engine image, whose build
    # downloads the release's public assets, only after the release.
    leg = _load("release-publish.yml")["jobs"]
    assert {"publish-pypi", "publish-provisa-pypi"} <= set(leg["publish-release"]["needs"])
    assert "publish-release" in leg["publish-engine-image"]["needs"]
    # A dry run skips the two publications; the release job (the asset check) still runs, and
    # never after one that failed.
    ran = leg["publish-release"]["if"]
    assert "!cancelled()" in ran and "contains(needs.*.result, 'failure')" in ran


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
