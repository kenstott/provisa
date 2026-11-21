# Copyright (c) 2026 Kenneth Stott
# Canary: ee88a62b-db9c-42b9-a461-e957918cc193
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""provisa-notebook (the Data Science area's in-browser Python) is part of every install: provisa
pins it at its own version, every install path builds or fetches it, and a release publishes it
before provisa, after every artifact exists."""

# Requirements: REQ-2071

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"


def _toml(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def test_provisa_pins_provisa_notebook_at_its_own_version():
    provisa = _toml(REPO / "pyproject.toml")
    notebook = _toml(REPO / "provisa-notebook" / "pyproject.toml")
    version = provisa["project"]["version"]
    assert notebook["project"]["version"] == version
    assert f"provisa-notebook=={version}" in provisa["project"]["dependencies"]
    assert provisa["tool"]["uv"]["sources"]["provisa-notebook"] == {
        "path": "provisa-notebook",
        "editable": True,
    }


def test_the_release_builds_the_notebook_and_stamps_both_pins():
    leg = yaml.safe_load((WORKFLOWS / "release-packages.yml").read_text())["jobs"]
    notebook_steps = " ".join(s.get("run", "") for s in leg["build-provisa-notebook"]["steps"])
    assert "provisa-notebook/pyproject.toml" in notebook_steps
    assert "python -m provisa_notebook.build" in notebook_steps
    wheel_steps = " ".join(s.get("run", "") for s in leg["build-provisa-wheel"]["steps"])
    assert '"provisa-notebook==${{ needs.metadata.outputs.py_version }}"' in wheel_steps


def test_the_notebook_wheel_is_a_release_asset_and_not_published_to_pypi():
    """provisa pins provisa-notebook; users resolve both from the release's Pages index, so the
    notebook wheel is attached to the draft and required by the publish leg. Nothing goes to PyPI."""
    jobs = yaml.safe_load((WORKFLOWS / "build-dmg.yml").read_text())["jobs"]
    assert not any("pypi" in job for job in jobs)
    packages = yaml.safe_load((WORKFLOWS / "release-packages.yml").read_text())["jobs"]
    attach = [
        s["with"]["files"]
        for s in packages["build-provisa-notebook"]["steps"]
        if "attach-release-assets" in str(s.get("uses", ""))
    ]
    assert any("provisa_notebook-*.whl" in files for files in attach)
    publish = (WORKFLOWS / "release-publish.yml").read_text()
    assert "provisa_notebook-${{ needs.metadata.outputs.py_version }}-py3-none-any.whl" in publish
    assert "provisa_sdk-${{ needs.metadata.outputs.py_version }}-py3-none-any.whl" in publish
    sdk_attach = [
        s["with"]["files"]
        for s in packages["build-sdk-python"]["steps"]
        if "attach-release-assets" in str(s.get("uses", ""))
    ]
    assert any("provisa_sdk-*.whl" in files for files in sdk_attach)


def test_every_installer_and_launcher_builds_the_notebook_from_this_repository():
    for path in (
        "packaging/macos/build-dmg.sh",
        "packaging/linux/build-appimage.sh",
    ):
        assert "scripts/build-notebook-wheel.sh" in (REPO / path).read_text(), path
    assert "provisa_notebook.build" in (REPO / "packaging/windows/build-sfx.ps1").read_text()
    for path in ("Dockerfile", "Dockerfile.dev"):
        assert "python -m provisa_notebook.build" in (REPO / path).read_text(), path
    for path in ("start-ui.sh", "start-ui-install.sh"):
        assert "scripts/ensure-notebook.sh" in (REPO / path).read_text(), path


# ── Pyodide is downloaded once per pin, never from inside a build ─────────────────────────────
#
# provisa_notebook.build fetches about seventy files from Pyodide's CDN. Every image, installer
# and wheel build ran it, each asking the CDN again; one refused request inside `docker build`
# failed the whole cluster lane (run 43879947072: "HTTP Error 503: No healthy backends").

_ACTION = "./.github/actions/pyodide-downloads"
_CONTEXT = '--build-context pyodide-downloads="$PROVISA_NOTEBOOK_DOWNLOADS"'
# What makes a job step build provisa-notebook's assets on the runner or in an image.
_BUILDS_THE_NOTEBOOK = re.compile(
    r"provisa_notebook\.build(?! --download-only)"
    r"|build-notebook-wheel\.sh|build-dmg\.sh|build-appimage\.sh|build-sfx\.ps1"
    r"|docker build[^\n]*(?:\\\n[^\n]*)*?-t provisa/provisa:"
)


def test_both_images_take_pyodide_from_a_stage_a_build_can_replace():
    for path in ("Dockerfile", "Dockerfile.dev"):
        text = (REPO / path).read_text()
        assert text.count("\nFROM scratch AS pyodide-downloads\n") == 1, path
        [run] = [r for r in text.split("\nRUN ")[1:] if "python -m provisa_notebook.build >" in r]
        assert run.startswith(
            "--mount=type=bind,from=pyodide-downloads,target=/pyodide-downloads,rw "
        ), path
        assert "PROVISA_NOTEBOOK_DOWNLOADS=/pyodide-downloads PYTHONPATH=provisa-notebook" in run


def test_every_job_that_builds_the_notebook_has_pyodide_downloaded_first():
    builders = []
    for workflow in sorted(WORKFLOWS.glob("*.yml")):
        for job, body in yaml.safe_load(workflow.read_text())["jobs"].items():
            steps = body.get("steps") or []
            first = next(
                (
                    i
                    for i, step in enumerate(steps)
                    if _BUILDS_THE_NOTEBOOK.search(str(step.get("run", "")))
                ),
                None,
            )
            if first is None:
                continue
            builders.append(f"{workflow.name}:{job}")
            before = [step.get("uses") for step in steps[:first]]
            assert _ACTION in before, f"{workflow.name}:{job} builds the notebook without it"
            for step in steps:
                run = str(step.get("run", ""))
                if re.search(r"-t provisa/provisa:", run) and "docker build" in run:
                    assert _CONTEXT in run, f"{workflow.name}:{job} builds the image without it"
    assert {
        "docker-image.yml:image",
        "release-packages.yml:build-provisa-notebook",
        "release-prebuilt.yml:pull-images-core",
        "release-prebuilt.yml:pull-images-core-amd64",
        "release-linux.yml:build-linux",
        "release-macos.yml:build-macos-core",
        "release-windows.yml:build-windows-core",
    } <= set(builders)


def test_the_lanes_whose_tests_build_the_image_have_pyodide_downloaded_first():
    from tests import pyodide_downloads

    for path in (
        "tests/e2e/test_helm_minikube.py",
        "tests/integration/test_master_key_survives_recreate_compose.py",
    ):
        text = (REPO / path).read_text()
        assert "Dockerfile.dev" in text and "*build_context_args()," in text, path
    assert f"FROM scratch AS {pyodide_downloads.STAGE}\n" in (REPO / "Dockerfile.dev").read_text()
    lanes = yaml.safe_load((WORKFLOWS / "integration-suite-lanes.yml").read_text())["jobs"]
    cluster = [step.get("uses") or step.get("name") for step in lanes["cluster"]["steps"]]
    assert cluster.index(_ACTION) < cluster.index("Run lane")
    [suite] = [
        b
        for b in lanes.values()
        if str(b.get("with", {}).get("run", "")).find("run_lane.py ${{ matrix.lane }}") >= 0
    ]
    assert suite["with"]["pyodide-downloads"] is True
    lane = yaml.safe_load((WORKFLOWS / "lane.yml").read_text())["jobs"]["lane"]["steps"]
    [step] = [s for s in lane if s.get("uses") == _ACTION]
    assert step["if"] == "inputs.pyodide-downloads"
    names = [s.get("name") for s in lane]
    assert names.index("Pyodide downloads") < names.index("Run lane")


def test_the_action_downloads_only_on_a_miss_and_keys_its_cache_by_the_pins():
    action = yaml.safe_load((REPO / ".github/actions/pyodide-downloads/action.yml").read_text())
    steps = {s["name"]: s for s in action["runs"]["steps"]}
    key = "pyodide-downloads-${{ hashFiles('provisa-notebook/provisa_notebook/__init__.py') }}"
    assert steps["Restore Pyodide downloads"]["with"]["key"] == key
    assert steps["Cache Pyodide downloads"]["with"]["key"] == key
    assert "restore-keys" not in steps["Restore Pyodide downloads"]["with"]
    for name in ("Download Pyodide", "Cache Pyodide downloads"):
        assert steps[name]["if"] == "steps.restore.outputs.cache-hit != 'true'", name
    assert "provisa_notebook.build --download-only" in steps["Download Pyodide"]["run"]
    assert "PROVISA_NOTEBOOK_DOWNLOADS=$dir" in steps["Pyodide downloads directory"]["run"]
