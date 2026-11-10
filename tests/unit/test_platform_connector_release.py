# Copyright (c) 2026 Kenneth Stott
# Canary: 3ec81b60-74d2-4a9f-96a3-d05f1b2e7c48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a release does to offer the Spark connector (REQ-2225): the block step, the wheel of the
sdist-only Spark Connect client, and the workflow carrying the block into every package."""

# Requirements: REQ-2225

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_WORKFLOWS = _REPO / ".github" / "workflows"
_SPARK = _REPO / "connectors" / "spark"
#: Each job that builds a package holding the provisa package, by its workflow.
_PACKAGING_JOBS = {
    "release-prebuilt.yml": ("pull-images-core", "pull-images-core-amd64"),
    "release-packages.yml": ("build-provisa-wheel",),
    "release-linux.yml": ("build-linux",),
    "release-windows.yml": ("build-windows-core", "build-windows-container"),
    "release-macos.yml": ("build-macos-core", "build-macos-container"),
}


def _module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def filler():
    return _module("fill_platform_connectors", _REPO / "scripts" / "fill_platform_connectors.py")


@pytest.fixture
def builder():
    return _module("spark_build_client", _SPARK / "_build_client.py")


def _entry(**overrides) -> dict:
    return {
        "name": "spark",
        "git": "https://github.com/example/provisa.git",
        "ref": "v1.0.0",
        "commit": "a" * 40,
        "language": "python",
        "image": "ghcr.io/example/spark",
        "digest": "sha256:" + "b" * 64,
        "code_hash": "c" * 64,
        "arch": "amd64",
        **overrides,
    }


def test_the_block_step_writes_what_the_build_printed(filler, tmp_path):
    block, target = tmp_path / "block.yaml", tmp_path / "platform_connectors.yaml"
    block.write_text(yaml.safe_dump({"connectors": [_entry()]}))
    assert filler.fill(block, target) == ["spark"]
    written = yaml.safe_load(target.read_text())["connectors"]
    assert [c["name"] for c in written] == ["spark"] and written[0]["digest"] == _entry()["digest"]


@pytest.mark.parametrize(
    ("body", "why"),
    [
        ({"connectors": []}, "holds no connectors"),
        ({"connectors": [_entry(digest="sha256:nope")]}, "digest"),
        ({"connectors": [_entry(), _entry()]}, "more than once"),
    ],
)
def test_a_block_that_would_break_sources_stops_the_release(filler, tmp_path, body, why):
    block = tmp_path / "block.yaml"
    block.write_text(yaml.safe_dump(body))
    with pytest.raises(SystemExit, match=why):
        filler.fill(block, tmp_path / "target.yaml")


def test_the_project_keeps_one_entry_module():
    from provisa.python.source import entry_module

    # The image is refused when a project has two top-level modules with none named.
    assert entry_module(_SPARK) == "provisa_spark"


def test_the_client_comes_from_the_sdist_the_lock_pins(builder):
    version, url, digest = builder._locked_sdist(_SPARK)
    assert url.endswith(f"pyspark_client-{version}.tar.gz")
    assert len(digest) == 64


def test_a_sdist_that_is_not_the_locked_one_is_refused(builder, tmp_path, monkeypatch):
    class _Answer:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self):
            return b"something else"

    monkeypatch.setattr(builder.urllib.request, "urlopen", lambda _url: _Answer())
    with pytest.raises(SystemExit, match="not the"):
        builder._download("https://example/x.tar.gz", "0" * 64, tmp_path / "x.tar.gz")


def _stubbed(builder, project: Path, monkeypatch) -> str:
    """The build with its network and uv parts stood in for; the wheel's expected hash."""
    (project / "uv.lock").write_text("the sdist's hash")
    wheel_body = b"wheel"

    def fake_wheel(_uv, _sdist, out):
        out.mkdir(parents=True, exist_ok=True)
        wheel = out / "pyspark_client-4.3.0-py3-none-any.whl"
        wheel.write_bytes(wheel_body)
        return wheel

    monkeypatch.setattr(builder.shutil, "which", lambda _name: "uv")
    monkeypatch.setattr(builder, "_locked_sdist", lambda _p: ("4.3.0", "https://x/a.tar.gz", "d"))
    monkeypatch.setattr(builder, "_download", lambda *_a: None)
    monkeypatch.setattr(builder, "_build_wheel", fake_wheel)
    monkeypatch.setattr(builder, "_others", lambda *_a: "pandas==2.3.3 --hash=sha256:ff\n")
    return hashlib.sha256(wheel_body).hexdigest()


def test_the_build_without_an_index_keeps_the_wheel_in_the_project(
    builder, tmp_path, monkeypatch
) -> None:
    hashed = _stubbed(builder, tmp_path, monkeypatch)
    builder.build(tmp_path, None)
    lines = (tmp_path / "requirements.txt").read_text().splitlines()
    assert lines[0] == f"./wheels/pyspark_client-4.3.0-py3-none-any.whl --hash=sha256:{hashed}"
    assert lines[1].startswith("pandas==")
    assert (tmp_path / "wheels" / "pyspark_client-4.3.0-py3-none-any.whl").is_file()
    # uv.lock is read ahead of requirements.txt and holds the sdist's hash.
    assert not (tmp_path / "uv.lock").exists()


def test_the_build_for_an_index_names_the_client_by_version(builder, tmp_path, monkeypatch) -> None:
    project = tmp_path / "project"
    project.mkdir()
    hashed = _stubbed(builder, project, monkeypatch)
    out = tmp_path / "dist"
    builder.build(project, out)
    lines = (project / "requirements.txt").read_text().splitlines()
    assert lines[0] == f"pyspark-client==4.3.0 --hash=sha256:{hashed}"
    assert (out / "pyspark_client-4.3.0-py3-none-any.whl").is_file()
    assert not (project / "wheels").exists()


def test_the_requirements_pass_the_image_builds_own_check(builder, tmp_path, monkeypatch) -> None:
    from provisa.python.install import lockfile

    _stubbed(builder, tmp_path, monkeypatch)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\nversion = "1"\n')
    builder.build(tmp_path, None)
    found = lockfile(tmp_path)
    assert found is not None and found.name == "requirements.txt"


def _steps(workflow: str, job: str) -> list[dict]:
    return yaml.safe_load((_WORKFLOWS / workflow).read_text())["jobs"][job]["steps"]


@pytest.mark.parametrize(
    ("workflow", "job"),
    [(w, j) for w, jobs in _PACKAGING_JOBS.items() for j in jobs],
)
def test_every_job_that_packages_provisa_takes_the_block(workflow, job) -> None:
    steps = _steps(workflow, job)
    # Right after the checkout: the run's block, over the checkout's empty one.
    block = steps[1]
    assert block.get("uses") == "actions/download-artifact@v4", [s.get("uses") for s in steps[:3]]
    assert block["with"] == {"name": "platform-connectors", "path": "provisa/core"}


def test_the_block_is_written_before_the_legs_that_package_it() -> None:
    jobs = yaml.safe_load((_WORKFLOWS / "build-dmg.yml").read_text())["jobs"]
    assert jobs["prebuilt"]["needs"] == "platform-connectors"
    assert jobs["packages"]["needs"] == "platform-connectors"
    # macos, windows and linux wait on prebuilt, which waits on the block.
    for leg in ("macos", "windows", "linux"):
        assert jobs[leg]["needs"] == "prebuilt"


def test_the_leg_builds_the_image_into_the_run_and_pushes_nothing() -> None:
    orchestrator = yaml.safe_load((_WORKFLOWS / "build-dmg.yml").read_text())["jobs"]
    leg = orchestrator["platform-connectors"]
    assert leg["uses"] == "./.github/workflows/release-platform-connectors.yml"
    assert "github.ref_name == inputs.version" in leg["with"]["publish"]
    assert leg["permissions"] == {"contents": "read"}
    job = yaml.safe_load((_WORKFLOWS / "release-platform-connectors.yml").read_text())["jobs"][
        "platform-connectors"
    ]
    assert job["permissions"] == {"contents": "read"}
    steps = job["steps"]
    build = next(s for s in steps if "connector build" in s.get("run", ""))
    assert build["if"] == "inputs.publish"
    # The image to a layout, the block naming its digest; no registry is signed in to.
    assert "--oci-layout spark-connector-image" in build["run"]
    assert "--registry" not in build["run"] and "REGISTRY_PASSWORD" not in build.get("env", {})
    kept = next(s for s in steps if s.get("with", {}).get("name") == "spark-connector-image")
    assert kept["uses"] == "actions/upload-artifact@v4" and kept["if"] == "inputs.publish"
    assert kept["with"]["path"] == "spark-connector-image/"
    assert kept["with"]["if-no-files-found"] == "error"
    upload = next(s for s in steps if s.get("with", {}).get("name") == "platform-connectors")
    assert upload["uses"] == "actions/upload-artifact@v4"
    assert upload["with"]["path"] == "provisa/core/platform_connectors.yaml"
    # Uploaded on a dry run too: the legs' packages then carry the checkout's empty block.
    assert "if" not in upload


def test_the_image_is_pushed_after_every_leg_and_before_what_names_it() -> None:
    orchestrator = yaml.safe_load((_WORKFLOWS / "build-dmg.yml").read_text())["jobs"]
    job = orchestrator["publish-connector-image"]
    # Gated as every publication: only on a publishing run, after every leg has built.
    assert job["if"] == orchestrator["publish"]["with"]["publish"]
    legs = {name for name, body in orchestrator.items() if "uses" in body} - {"publish"}
    assert set(job["needs"]) == legs
    assert job["permissions"] == {"contents": "read", "packages": "write"}
    # The provisa wheel and the installers carry the block: they are published only after it.
    assert "publish-connector-image" in orchestrator["publish-provisa-pypi"]["needs"]
    assert "publish-connector-image" in orchestrator["publish"]["needs"]
    steps = job["steps"]
    downloads = {
        s["with"]["name"]: s["with"]["path"]
        for s in steps
        if s.get("uses") == "actions/download-artifact@v4"
    }
    assert downloads == {
        "platform-connectors": "provisa/core",
        "spark-connector-image": "spark-connector-image",
    }
    push = next(s for s in steps if "connector push" in s.get("run", ""))
    # Exactly the image the block names: push refuses a layout whose digest is another.
    assert "connector push spark-connector-image" in push["run"]
    assert "--config provisa/core/platform_connectors.yaml --name spark" in push["run"]
    assert "--registry-password-env REGISTRY_PASSWORD" in push["run"]
    assert push["env"] == {"REGISTRY_PASSWORD": "${{ secrets.GITHUB_TOKEN }}"}
