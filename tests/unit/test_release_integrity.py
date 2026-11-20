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
# The provisa server wheel is not uploaded to PyPI (it outgrew the 10 GB project limit, alpha.510):
# the publish leg attaches it to the release and lists it in the GitHub Pages index.
_PYPI = ("publish-pypi", "publish-notebook-pypi")
#: REQ-2225: the orchestrator's other publication, the Spark connector's image that
#: platform-connectors built and kept in the run.
_IMAGE_PUSH = "publish-connector-image"
#: Build jobs whose names do not start with build-/package-: the platform connector block.
_BUILDS_ALSO = frozenset({"platform-connectors"})
_PYPI_AFTER: dict[str, set[str]] = {}

#: What leaves the run: a PyPI upload, a release attachment or undraft, an image push -- by
#: docker, oras, skopeo or crane, or by `provisa connector` (`push`, or a `build`/`rebuild`, which
#: pushes unless it writes an OCI layout), and any registry sign-in, which is only for a push.
_PUBLISHES = re.compile(
    r"pypi-publish|softprops/action-gh-release|gh release edit|twine upload"
    r"|docker push|oras push|oras cp|skopeo copy|crane push|crane copy"
    r"|provisa connector push|--registry-password-env|docker/login-action|actions/deploy-pages"
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


_ATTACH = "./.github/actions/attach-release-assets"

#: Every release asset, by the job that builds it: the metadata outputs (or file names) its attach
#: step names. That job attaches it to the draft itself; nothing copies it there later.
_ASSETS: dict[tuple[str, str], tuple[str, ...]] = {
    ("release-prebuilt.yml", "package-plugins"): ("outputs.plugins_name",),
    ("release-prebuilt.yml", "pull-images-core"): ("outputs.trino_image_name",),
    ("release-prebuilt.yml", "pull-images-core-amd64"): ("outputs.trino_image_amd64_name",),
    ("release-prebuilt.yml", "package-obs-images"): ("outputs.obs_images_name",),
    ("release-prebuilt.yml", "package-demo-images"): ("outputs.demo_images_name",),
    ("release-prebuilt.yml", "package-core-images"): ("outputs.core_images_name",),
    ("release-prebuilt.yml", "package-core-images-amd64"): ("outputs.core_images_amd64_name",),
    ("release-macos.yml", "build-macos-core"): ("outputs.dmg_name",),
    ("release-macos.yml", "build-macos-container"): ("outputs.dmg_container_name",),
    ("release-macos.yml", "build-macos-obs"): ("outputs.dmg_obs_name",),
    ("release-macos.yml", "build-macos-demo"): ("outputs.dmg_demo_name",),
    ("release-windows.yml", "build-windows-core"): ("outputs.windows_name",),
    ("release-windows.yml", "build-windows-container"): ("outputs.windows_container_name",),
    ("release-linux.yml", "build-linux"): ("outputs.linux_name",),
    ("release-packages.yml", "build-jdbc"): (
        "outputs.jdbc_name",
        "provisa-jdbc-sbom.cdx.json",
        "provisa-jdbc-cve-report.json",
    ),
    ("release-packages.yml", "build-provisa-wheel"): ("dist/provisa-*.whl",),
    ("release-packages.yml", "build-python-client"): (
        "provisa-client/dist/provisa_client-*.whl",
        "provisa-client-sbom.cdx.json",
        "provisa-client-cve-report.json",
    ),
    ("release-packages.yml", "build-provisa-notebook"): (
        "provisa-notebook/dist/provisa_notebook-*.whl",
    ),
    ("release-packages.yml", "provisa-sbom"): ("provisa-sbom.cdx.json", "provisa-cve-report.json"),
}
#: Artifacts a release also keeps in the run, because a job of the same run reads them: the PyPI
#: uploads read the client and notebook wheels.
_READ_IN_THE_RUN = {"python-client-dist", "provisa-notebook-dist"}


def _attach_steps(name: str, job: str) -> list[dict]:
    return [s for s in _load(name)["jobs"][job]["steps"] if s.get("uses") == _ATTACH]


def test_each_job_that_builds_a_release_asset_attaches_it_to_the_draft_itself():
    """Uploaded once, by the job that built it: alpha.510's publish leg downloaded every artifact
    (multi-GB image tarballs) and uploaded it again, 30+ minutes; GitHub has no server-side copy
    from an artifact to a release asset."""
    for (name, job), wanted in _ASSETS.items():
        body = _load(name)["jobs"][job]
        [step] = _attach_steps(name, job)
        where = f"{name}:{job}"
        # Only a release attaches: a dry run attaches nothing.
        assert step["if"] == "inputs.publish", where
        assert step["with"]["tag"] == "${{ inputs.version }}", where
        for asset in wanted:
            assert asset in step["with"]["files"], (where, asset)
        # The token may write the release, and the local action and the tag's notes page are on
        # disk: the job's first step is its checkout.
        assert body["permissions"]["contents"] == "write", where
        assert body["steps"][0]["uses"] == "actions/checkout@v4", where
    # No other job attaches anything.
    for name in {n for n, _j in _ASSETS} | set(_LEGS.values()):
        for job in _load(name)["jobs"]:
            if (name, job) not in _ASSETS:
                assert not _attach_steps(name, job), f"{name}:{job}"


def test_the_orchestrator_lets_each_leg_that_attaches_write_the_release():
    """A nested job cannot exceed its caller, and the repository's default token only reads."""
    orchestrator = _load("build-dmg.yml")["jobs"]
    for leg in {n for n, _j in _ASSETS}:
        caller = next(j for j, b in orchestrator.items() if b.get("uses", "").endswith(leg))
        assert orchestrator[caller]["permissions"]["contents"] == "write", caller


def test_a_dry_run_attaches_nothing_and_keeps_each_built_asset_in_the_run():
    """A dry run (the release's packaging check) leaves the run nothing, and its artifacts are
    where its installers are taken from: each asset is uploaded to the run on a dry run, and the
    upload fails when the file is missing -- the proof the asset set is whole. On a release the
    artifact is skipped unless another job of the run reads it."""
    for (name, job), _wanted in _ASSETS.items():
        if job == "provisa-sbom":
            continue  # reports, attached only; the dry run's scan already gates the job
        steps = _load(name)["jobs"][job]["steps"]
        uploads = [s for s in steps if s.get("uses") == "actions/upload-artifact@v4"]
        kept = [s for s in uploads if s["with"]["name"] in _READ_IN_THE_RUN]
        dry = [s for s in uploads if s.get("if") == "${{ !inputs.publish }}"]
        where = f"{name}:{job}"
        assert len(kept) + len(dry) == 1, (where, [s["with"]["name"] for s in uploads])
        for step in kept:
            assert "if" not in step, where
        for step in dry:
            assert step["with"]["if-no-files-found"] == "error", where


def test_the_attach_action_drafts_and_uploads_and_never_undrafts():
    text = (_ROOT / ".github" / "actions" / "attach-release-assets" / "action.yml").read_text()
    action = yaml.safe_load(text)
    assert set(action["inputs"]) == {"tag", "files"}, "a switch belongs to the caller's if"
    steps = action["runs"]["steps"]
    assert steps[0]["uses"] == "./.github/actions/ensure-release"
    assert steps[0]["with"] == {"tag": "${{ inputs.tag }}"}
    run = steps[1]["run"]
    assert 'gh release upload "$TAG" "${upload[@]}" --clobber' in run
    assert '[ "$missing" = 0 ]' in run
    assert "gh release edit" not in text and "--draft=false" not in text
    assert "if" not in steps[0] and "if" not in steps[1]


def test_the_publish_leg_copies_no_asset_and_verifies_every_one():
    """The publish leg downloads nothing and attaches nothing: it reads the draft's asset list,
    refuses to undraft when any expected asset is missing, and undrafts."""
    text = (_WORKFLOWS / "release-publish.yml").read_text()
    assert "download-artifact" not in text
    assert "softprops/action-gh-release" not in text and "gh release upload" not in text
    assert _ATTACH not in text
    steps = _publish_job()["steps"]
    [verify] = [s for s in steps if s["name"].startswith("Publish release")]
    assert verify["if"] == "inputs.publish"
    required = verify["env"]["REQUIRED"]
    names = {
        a.removeprefix("outputs.")
        for wanted in _ASSETS.values()
        for a in wanted
        if a.startswith("outputs.")
    }
    for output in names:
        assert f"${{{{ needs.metadata.outputs.{output} }}}}" in required, output
    py = "${{ needs.metadata.outputs.py_version }}"
    for wheel in (
        f"provisa_client-{py}-py3-none-any.whl",
        f"provisa_notebook-{py}-py3-none-any.whl",
        f"provisa-{py}-cp312-cp312-manylinux_2_28_x86_64.whl",
        f"provisa-{py}-cp312-cp312-manylinux_2_28_aarch64.whl",
        f"provisa-{py}-cp312-cp312-macosx_*_arm64.whl",
        f"provisa-{py}-cp312-cp312-win_amd64.whl",
    ):
        assert wheel in required.splitlines(), wheel
    assert "gh release view" in verify["run"] and "exit 1" in verify["run"]


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
        _IMAGE_PUSH: "orchestrator",
        "publish-release": "publish",
        "publish-wheel-index": "publish",
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


# ── The provisa server off PyPI: release assets through a GitHub Pages index ─────────────────


def test_the_provisa_server_wheel_is_never_uploaded_to_pypi():
    """alpha.510's upload failed with PyPI's 10 GB project limit, and the release waited on it.
    No job downloads the server's wheels for a PyPI upload, and the publish leg needs no such job."""
    orchestrator = _load("build-dmg.yml")["jobs"]
    assert "publish-provisa-pypi" not in orchestrator
    assert "publish-provisa-pypi" not in orchestrator["publish"]["needs"]
    for job, body in orchestrator.items():
        steps = body.get("steps") or []
        if not any("pypi-publish" in str(s.get("uses", "")) for s in steps):
            continue
        for step in steps:
            given = step.get("with") or {}
            assert not str(given.get("pattern", "")).startswith("provisa-wheel-"), job


def test_the_publish_leg_can_be_dispatched_to_publish_a_release_whose_leg_was_skipped():
    """A release whose publish leg was skipped is published by dispatching the leg on its version:
    its assets are already on the draft, so the dispatch verifies, undrafts and writes the index,
    reading no other run."""
    on = _load("release-publish.yml")[True]
    assert set(on["workflow_call"]["inputs"]) == {"version", "publish"}
    dispatch = on["workflow_dispatch"]["inputs"]
    assert set(dispatch) == {"version", "publish"}
    for name, kind in (("version", "string"), ("publish", "boolean")):
        assert dispatch[name]["type"] == kind, name
        assert dispatch[name]["required"] is True, name
        assert "default" not in dispatch[name], name
    assert _publish_job()["permissions"] == {"contents": "write"}
    # The orchestrator grants what the leg's jobs ask for: a nested job cannot exceed its caller.
    granted = _load("build-dmg.yml")["jobs"]["publish"]["permissions"]
    leg = _load("release-publish.yml")["jobs"]
    for body in leg.values():
        for scope, level in (body.get("permissions") or {}).items():
            assert granted.get(scope) in (level, "write"), (scope, level)


def test_the_wheel_index_is_regenerated_on_pages_after_the_release_is_published():
    """After the undraft, every published release's provisa wheels are listed in a PEP 503 index
    on GitHub Pages, generated by the tested script; never on a dry run."""
    job = _load("release-publish.yml")["jobs"]["publish-wheel-index"]
    assert job["if"] == "inputs.publish"
    assert job["needs"] == ["publish-release"]
    assert job["permissions"] == {"contents": "read", "pages": "write", "id-token": "write"}
    assert job["environment"]["name"] == "github-pages"
    runs = "\n".join(s.get("run", "") for s in job["steps"])
    assert "--paginate --slurp > releases.json" in runs
    assert "scripts/release_wheel_index.py releases.json site" in runs
    uses = [s.get("uses") for s in job["steps"] if s.get("uses")]
    assert uses == [
        "actions/checkout@v4",
        "actions/upload-pages-artifact@v3",
        "actions/deploy-pages@v4",
    ]
    assert (_ROOT / "scripts" / "release_wheel_index.py").is_file()
