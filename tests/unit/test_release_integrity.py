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

import re
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
    # REQ-2225: the platform connector block every package carries; the packaging legs read it.
    "platform-connectors": "release-platform-connectors.yml",
    "prebuilt": "release-prebuilt.yml",
    "packages": "release-packages.yml",
    "macos": "release-macos.yml",
    "windows": "release-windows.yml",
    "linux": "release-linux.yml",
    "publish": "release-publish.yml",
}
# PyPI's own jobs are the orchestrator's: PyPI trusted publishing does not accept a token issued
# to a reusable workflow (pypa/gh-action-pypi-publish#166), which every leg is.
_PYPI = ("publish-pypi", "publish-notebook-pypi", "publish-provisa-pypi")
#: REQ-2225: the orchestrator's other publication, the Spark connector's image that
#: platform-connectors built and kept in the run.
_IMAGE_PUSH = "publish-connector-image"
#: Build jobs whose names do not start with build-/package-: the platform connector block.
_BUILDS_ALSO = frozenset({"platform-connectors"})
#: REQ-2071: provisa pins provisa-notebook at its own version, so provisa waits for it on PyPI.
#: REQ-2225: the provisa wheel carries the platform connector block, so it waits for the image the
#: block names.
_PYPI_AFTER = {"publish-provisa-pypi": {"publish-notebook-pypi", _IMAGE_PUSH}}

#: What leaves the run: a PyPI upload, a release attachment or undraft, an image push -- by
#: docker, oras, skopeo or crane, or by `provisa connector` (`push`, or a `build`/`rebuild`, which
#: pushes unless it writes an OCI layout), and any registry sign-in, which is only for a push.
_PUBLISHES = re.compile(
    r"pypi-publish|softprops/action-gh-release|gh release edit|twine upload"
    r"|docker push|oras push|oras cp|skopeo copy|crane push|crane copy"
    r"|provisa connector push|--registry-password-env|docker/login-action"
)
_CONNECTOR_BUILD = re.compile(r"provisa connector (build|rebuild)\b")


def _publishes(step: dict) -> bool:
    """Whether a workflow step sends anything out of the run."""
    text = f"{step.get('uses') or ''}\n{step.get('run') or ''}"
    if _PUBLISHES.search(text) or (step.get("with") or {}).get("push") is not None:
        return True
    return bool(_CONNECTOR_BUILD.search(text)) and "--oci-layout" not in text


def _load(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text())


def _publish_job() -> dict:
    return _load("release-publish.yml")["jobs"]["publish-release"]


def test_build_dmg_publish_job_exists():
    """The release has a publish-release job, in the leg the orchestrator calls `publish`."""
    orchestrator = _load("build-dmg.yml")["jobs"]
    assert {job: body["uses"] for job, body in orchestrator.items() if "uses" in body} == {
        job: f"./.github/workflows/{leg}" for job, leg in _LEGS.items()
    }
    assert set(orchestrator) == set(_LEGS) | set(_PYPI) | {_IMAGE_PUSH}
    assert "publish-release" in _load("release-publish.yml")["jobs"]


def test_build_dmg_publish_job_depends_on_all_builds():
    """Publishing waits for every build: the orchestrator's `publish` job needs every other leg,
    and each build job is a job of one of those legs."""
    orchestrator = _load("build-dmg.yml")["jobs"]
    assert set(orchestrator["publish"]["needs"]) == (set(_LEGS) - {"publish"}) | set(_PYPI) | {
        _IMAGE_PUSH
    }
    assert set(orchestrator[_IMAGE_PUSH]["needs"]) == set(_LEGS) - {"publish"}
    for job in _PYPI:
        assert set(orchestrator[job]["needs"]) == (set(_LEGS) - {"publish"}) | _PYPI_AFTER.get(
            job, set()
        ), job
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
        "platform-connectors",
    }
    assert required <= built, f"no leg builds: {required - built}"
    # A leg that reads another leg's artifacts starts after it.
    for leg in ("macos", "windows", "linux"):
        assert orchestrator[leg]["needs"] == "prebuilt"
    for leg in ("prebuilt", "packages"):
        assert orchestrator[leg]["needs"] == "platform-connectors"
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


def test_the_release_publishes_the_macos_duckdb_extensions_asset_with_its_checksum():
    """The Core DMG ships no DuckDB extension (codesign cannot sign them, so Apple rejected the
    DMG): they are a release asset of their own, named by release-metadata, built by the Core
    build, and attached with the .sha256 beside it (maintainer decision 2026-11-11)."""
    metadata = (_ROOT / ".github/actions/release-metadata/action.yml").read_text()
    assert (
        'echo "duckdb_extensions_name=provisa-duckdb-extensions-${TAG}-macOS-arm64.tar.gz"'
        in metadata
    )
    name = "${{ needs.metadata.outputs.duckdb_extensions_name }}"

    macos = _load("release-macos.yml")["jobs"]
    assert "duckdb_extensions_name" in macos["metadata"]["outputs"]
    core = macos["build-macos-core"]["steps"]
    build = next(s for s in core if "build-dmg.sh stage" in s.get("run", ""))
    assert build["env"]["DUCKDB_EXTENSIONS_ASSET"] == name
    upload = next(
        s
        for s in core
        if s.get("uses", "").startswith("actions/upload-artifact")
        and s["with"]["name"] == "macos-duckdb-extensions-dist"
    )
    assert core.index(build) < core.index(upload)
    assert upload["with"]["path"].split() == [
        f"packaging/macos/dist/{name}",
        f"packaging/macos/dist/{name}.sha256",
    ]
    assert upload["with"]["if-no-files-found"] == "error"

    publish = _publish_job()
    assert "duckdb_extensions_name" in _load("release-publish.yml")["jobs"]["metadata"]["outputs"]
    download = next(s for s in publish["steps"] if s.get("name") == "Download all artifacts")
    assert download["with"]["pattern"] == "*-dist"
    files = next(s for s in publish["steps"] if s.get("id") == "assets")["env"]["FILES"].split()
    assert f"release-assets/{name}" in files
    assert f"release-assets/{name}.sha256" in files


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
    where: dict[str, str] = {}
    builds: dict[str, str] = {}
    for leg, name in {**_LEGS, "orchestrator": "build-dmg.yml"}.items():
        for job, body in _load(name)["jobs"].items():
            # A job that calls a leg (the orchestrator's platform-connectors) is not the build.
            builds_here = job.startswith(("build-", "package-")) or job in _BUILDS_ALSO
            if builds_here and "uses" not in body:
                builds[job] = leg
            for step in body.get("steps") or []:
                if _publishes(step):
                    where[job] = leg
    assert where == {
        "publish-pypi": "orchestrator",
        "publish-notebook-pypi": "orchestrator",
        "publish-provisa-pypi": "orchestrator",
        _IMAGE_PUSH: "orchestrator",
        "publish-release": "publish",
        "publish-engine-image": "publish",
        "publish-zaychik-image": "publish",
    }
    assert len(builds) >= 15 and "publish" not in builds.values(), builds
    orchestrator = _load("build-dmg.yml")["jobs"]
    legs_built = set(builds.values())
    assert legs_built == set(_LEGS) - {"publish"}
    # Every PyPI publication needs every build (provisa's also its pinned notebook package); the
    # publish leg needs them too, so the release is undrafted only after every wheel is on PyPI.
    for job in (*_PYPI, _IMAGE_PUSH):
        assert set(orchestrator[job]["needs"]) == legs_built | _PYPI_AFTER.get(job, set()), job
    assert set(orchestrator["publish"]["needs"]) == legs_built | set(_PYPI) | {_IMAGE_PUSH}
    # The image push is gated as the PyPI uploads are: never on a dry run.
    assert orchestrator[_IMAGE_PUSH]["if"] == orchestrator["publish"]["with"]["publish"]
    # A dry run skips the two publications; the publish leg (the asset check) still runs, and
    # never after one that failed.
    ran = orchestrator["publish"]["if"]
    assert "!cancelled()" in ran and "contains(needs.*.result, 'failure')" in ran
    # Inside the leg the engine image, whose build downloads the release's public assets, only
    # after the release; and the release job runs on a dry run, never after a failed need.
    leg = _load("release-publish.yml")["jobs"]
    assert "publish-release" in leg["publish-engine-image"]["needs"]
    ran = leg["publish-release"]["if"]
    assert "!cancelled()" in ran and "contains(needs.*.result, 'failure')" in ran


@pytest.mark.parametrize(
    ("step", "publishes"),
    [
        # The platform connector leg as J1166 landed it: built and pushed in the first leg.
        (
            {
                "run": "uv run provisa connector build https://github.com/x/y.git --name spark "
                "--image ghcr.io/x/spark --arch amd64 --registry-user bot "
                "--registry-password-env REGISTRY_PASSWORD > block.yaml"
            },
            True,
        ),
        # A build with no sign-in still pushes (anonymously) unless it writes a layout.
        ({"run": "provisa connector build g --name s --image r/s --arch amd64"}, True),
        ({"run": "provisa connector rebuild --config model.yaml --write"}, True),
        ({"run": "provisa connector push image --config c.yaml --name spark"}, True),
        ({"run": "docker push ghcr.io/x/y:1"}, True),
        ({"run": "oras push ghcr.io/x/y:1 a.tar"}, True),
        ({"run": "skopeo copy oci:image docker://ghcr.io/x/y:1"}, True),
        ({"run": "crane push image.tar ghcr.io/x/y:1"}, True),
        ({"uses": "docker/login-action@v3"}, True),
        ({"uses": "docker/build-push-action@v6", "with": {"push": False}}, True),
        # Built into the run, nothing pushed.
        (
            {
                "run": "uv run provisa connector build g --name spark --image ghcr.io/x/spark "
                "--arch amd64 --oci-layout spark-connector-image > block.yaml"
            },
            False,
        ),
        ({"uses": "actions/upload-artifact@v4"}, False),
        ({"run": "docker build --platform linux/amd64 --output type=local,dest=w ."}, False),
    ],
)
def test_an_image_push_is_seen_as_a_publication(step, publishes):
    """REQ-2225: a step that pushes an image leaves the run as surely as a PyPI upload, so it is
    held to the same rule -- J1166's connector build pushed from the first leg and was not seen."""
    assert _publishes(step) is publishes


def test_pypi_is_published_from_the_orchestrator_never_a_reusable_workflow():
    """PyPI trusted publishing does not accept a token whose job_workflow_ref is a reusable
    workflow: alpha.483's PyPI jobs, then in release-publish.yml, failed with invalid-publisher
    (run 38588660256). Every PyPI upload is a job of build-dmg.yml itself, gated as its other
    publications are and holding the OIDC token."""
    for path in sorted(_WORKFLOWS.glob("*.yml")):
        workflow = yaml.safe_load(path.read_text())
        reusable = "workflow_call" in (workflow.get(True) or {})
        for job, body in (workflow.get("jobs") or {}).items():
            uploads = any(
                "pypi-publish" in str(step.get("uses", "")) for step in body.get("steps") or []
            )
            if uploads:
                assert not reusable, f"{path.name}:{job} publishes to PyPI from a reusable workflow"
    orchestrator = _load("build-dmg.yml")["jobs"]
    gate = orchestrator["publish"]["with"]["publish"]
    for job in _PYPI:
        assert orchestrator[job]["permissions"] == {"id-token": "write"}, job
        assert orchestrator[job]["if"] == gate, job


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
