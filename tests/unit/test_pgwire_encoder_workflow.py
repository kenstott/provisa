# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgwire's Rust DataRow encoder (provisa-pgwire-encoder) is part of every install: provisa pins it
at its own version, the release compiles it for every platform a `pip install provisa` runs on and
publishes it before provisa, and every installer and image compiles it from this repository and
imports it, so an artifact without it is never built."""

# Requirements: REQ-2194

from __future__ import annotations

import fnmatch
import re
import tomllib
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
CRATE = REPO / "experiments" / "pgwire_encoder_rs"
HELPER = "scripts/build-pgwire-encoder-wheel.sh"
#: The Rust toolchain and maturin every recipe uses.
RUST = "1.95.0"
MATURIN = "1.15.0"


def _toml(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def _jobs(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())["jobs"]


def _runs(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def test_provisa_pins_the_encoder_at_its_own_version():
    provisa = _toml(REPO / "pyproject.toml")
    encoder = _toml(CRATE / "pyproject.toml")
    version = provisa["project"]["version"]
    assert encoder["project"]["name"] == "provisa-pgwire-encoder"
    assert encoder["project"]["version"] == version
    assert encoder["project"]["requires-python"] == provisa["project"]["requires-python"]
    assert f"provisa-pgwire-encoder=={version}" in provisa["project"]["dependencies"]
    assert provisa["tool"]["uv"]["sources"]["provisa-pgwire-encoder"] == {
        "path": "experiments/pgwire_encoder_rs",
        "editable": True,
    }
    lock = (REPO / "uv.lock").read_text()
    assert 'source = { editable = "experiments/pgwire_encoder_rs" }' in lock


def test_the_encoder_is_a_maturin_package_whose_module_is_the_crates():
    encoder = _toml(CRATE / "pyproject.toml")
    assert encoder["build-system"]["build-backend"] == "maturin"
    maturin = encoder["tool"]["maturin"]
    # The module name is the #[pymodule] function's: a mismatch builds a wheel that cannot import.
    (pymodule,) = re.findall(r"#\[pymodule\]\s*fn (\w+)", (CRATE / "src" / "lib.rs").read_text())
    assert maturin["module-name"] == pymodule == "pgwire_encoder_rs"
    assert maturin["bindings"] == "pyo3"
    assert maturin["locked"] is True and maturin["profile"] == "release"
    # A Rust source change rebuilds the checkout's editable install.
    keys = {entry["file"] for entry in encoder["tool"]["uv"]["cache-keys"]}
    assert {"Cargo.toml", "Cargo.lock", "src/**/*.rs"} <= keys


def test_no_shipped_build_is_tuned_for_the_build_machines_cpu():
    """A wheel built with target-cpu=native on one CI runner can stop on a user's older CPU with
    an illegal instruction. Only the benchmark's build.sh asks for it, on its own command line."""
    config = _toml(CRATE / ".cargo" / "config.toml")
    flags = [f for target in config.get("target", {}).values() for f in target.get("rustflags", [])]
    flags += config.get("build", {}).get("rustflags", [])
    assert not [f for f in flags if "target-cpu" in f], flags
    assert "target-cpu" not in (CRATE / "Cargo.toml").read_text()
    assert "target-cpu=native" in (CRATE / "build.sh").read_text()


def test_the_release_compiles_a_wheel_for_every_platform_and_imports_each():
    job = _jobs("release-packages.yml")["build-pgwire-encoder"]
    rows = {row["label"]: row for row in job["strategy"]["matrix"]["include"]}
    assert {label: (row["runner"], row["tag"]) for label, row in rows.items()} == {
        "linux-x86_64": ("ubuntu-latest", "manylinux_2_28_x86_64"),
        "linux-aarch64": ("ubuntu-24.04-arm", "manylinux_2_28_aarch64"),
        "macos-arm64": ("macos-14", r"macosx_\d+_\d+_arm64"),
        "macos-x86_64": ("macos-14", r"macosx_\d+_\d+_x86_64"),
        "windows-x64": ("windows-latest", "win_amd64"),
    }
    # The Intel Mac wheel is cross-compiled, and imported by an x86_64 Python (Rosetta).
    assert rows["macos-x86_64"]["target"] == "x86_64-apple-darwin"
    assert rows["macos-x86_64"]["arch"] == "x64"
    assert job["strategy"]["fail-fast"] is False
    steps = {step["name"]: step for step in job["steps"]}
    python = steps["Set up Python"]
    assert python["uses"] == "actions/setup-python@v5"
    assert python["with"] == {"python-version": "3.12", "architecture": "${{ matrix.arch }}"}
    build = steps["Build the wheel"]
    assert build["run"].startswith(HELPER)
    assert build["env"] == {"PGWIRE_ENCODER_TARGET": "${{ matrix.target }}"}
    verify = steps["Verify the wheel's tag and import it"]
    assert "-cp312-cp312-{os.environ['TAG']}" in verify["run"]
    assert 'python -c "import pgwire_encoder_rs' in verify["run"].replace('"$PY" -c', "python -c")
    upload = steps["Upload dist"]
    assert upload["with"]["name"] == "pgwire-encoder-wheel-${{ matrix.label }}-dist"
    assert upload["with"]["if-no-files-found"] == "error"


def test_the_release_stamps_the_encoder_and_provisas_pin_with_its_version():
    jobs = _jobs("release-packages.yml")
    stamp = _runs(jobs["build-pgwire-encoder"])
    assert "experiments/pgwire_encoder_rs/pyproject.toml" in stamp
    assert 'version = "${{ needs.metadata.outputs.py_version }}"' in stamp
    wheel = _runs(jobs["build-provisa-wheel"])
    assert '"provisa-pgwire-encoder==${{ needs.metadata.outputs.py_version }}"' in wheel


def test_the_encoder_is_published_before_provisa_and_only_after_every_leg():
    jobs = _jobs("build-dmg.yml")
    legs = {"prebuilt", "packages", "macos", "windows", "linux"}
    publish = jobs["publish-pgwire-encoder-pypi"]
    assert set(publish["needs"]) == legs
    assert publish["permissions"] == {"id-token": "write"}
    assert publish["if"] == jobs["publish"]["with"]["publish"]
    assert "publish-pgwire-encoder-pypi" in jobs["publish-provisa-pypi"]["needs"]
    assert "publish-pgwire-encoder-pypi" in jobs["publish"]["needs"]


def test_the_pypi_upload_takes_every_platforms_wheel():
    """The download is a pattern (one artifact per platform): it has to match each of them, and
    the -dist names are also what the release attaches."""
    (download, upload) = _jobs("build-dmg.yml")["publish-pgwire-encoder-pypi"]["steps"]
    assert download["uses"] == "actions/download-artifact@v4"
    assert upload["uses"] == "pypa/gh-action-pypi-publish@release/v1"
    assert download["with"]["merge-multiple"] is True
    pattern = download["with"]["pattern"]
    job = _jobs("release-packages.yml")["build-pgwire-encoder"]
    name = next(s for s in job["steps"] if s["name"] == "Upload dist")["with"]["name"]
    labels = [row["label"] for row in job["strategy"]["matrix"]["include"]]
    for label in labels:
        artifact = name.replace("${{ matrix.label }}", label)
        assert fnmatch.fnmatch(artifact, pattern), artifact
        assert fnmatch.fnmatch(artifact, "*-dist"), artifact
    attached = _jobs("release-publish.yml")["publish-release"]["steps"]
    assert any((s.get("with") or {}).get("pattern") == "*-dist" for s in attached)


def test_every_installer_compiles_the_encoder_and_fails_without_it():
    for path in ("packaging/macos/build-dmg.sh", "packaging/linux/build-appimage.sh"):
        text = (REPO / path).read_text()
        assert HELPER in text, path
        assert '--find-links "$encoder_dir"' in text, path
    assert "pgwire_encoder_rs" in (REPO / "packaging/macos/build-dmg.sh").read_text()
    assert (
        "provisa_pgwire_encoder-*.whl" in (REPO / "packaging/linux/build-appimage.sh").read_text()
    )
    first_launch = (REPO / "packaging/linux/first-launch.sh").read_text()
    assert '-c "import pgwire_encoder_rs"' in first_launch
    sfx = (REPO / "packaging/windows/build-sfx.ps1").read_text()
    assert "experiments\\pgwire_encoder_rs" in sfx
    assert "--find-links $EncoderDir" in sfx
    assert '& $RuntimePy -c "import pgwire_encoder_rs"' in sfx


def test_every_image_compiles_the_encoder_in_one_recipe_and_imports_it():
    """Dockerfile's encoder stage is the one Linux recipe: the images, the AppImage (through the
    helper), the release's arm64 wheelhouse and the cloud push all build it."""
    dockerfile = (REPO / "Dockerfile").read_text()
    stage = dockerfile[dockerfile.index("FROM --platform=$BUILDPLATFORM rust:") :]
    stage = stage[: stage.index("FROM scratch AS pgwire-encoder-wheel")]
    assert "--zig --compatibility manylinux_2_28" in stage
    assert "FROM --platform=$BUILDPLATFORM python:3.12-slim AS pgwire-encoder" in stage
    assert "COPY --from=pgwire-encoder /wheels/ /" in dockerfile
    dev = (REPO / "Dockerfile.dev").read_text()
    assert stage.split("\n", 1)[1].strip() in dev, "Dockerfile.dev's encoder stage drifted"
    for path, text in (("Dockerfile", dockerfile), ("Dockerfile.dev", dev)):
        assert 'python -c "import pgwire_encoder_rs"' in text, path
    helper = (REPO / HELPER).read_text()
    assert "--target pgwire-encoder-wheel" in helper
    prebuilt = _jobs("release-prebuilt.yml")["pull-images-core"]
    wheels = next(
        s for s in prebuilt["steps"] if s["name"] == "Build provisa wheels for linux/arm64"
    )
    run = wheels["run"]
    assert run.index("--target pgwire-encoder-wheel") < run.index("pip wheel")
    image = _jobs("docker-image.yml")["image"]
    assert 'python -c "import pgwire_encoder_rs"' in _runs(image)
    cloud = (REPO / "scripts/deploy-cloud.sh").read_text()
    assert "--target pgwire-encoder-wheel" in cloud
    ignored = (REPO / ".dockerignore").read_text().splitlines()
    assert "experiments/pgwire_encoder_rs/target/" in ignored


def test_every_recipe_names_one_toolchain_and_one_maturin():
    helper = (REPO / HELPER).read_text()
    assert f'MATURIN_VERSION="{MATURIN}"' in helper
    assert f'RUSTUP_TOOLCHAIN="${{RUSTUP_TOOLCHAIN:-{RUST}}}"' in helper
    for path in ("Dockerfile", "Dockerfile.dev"):
        text = (REPO / path).read_text()
        assert f"rust:{RUST}-slim-trixie" in text, path
        assert f'"maturin[zig]=={MATURIN}"' in text, path
    sfx = (REPO / "packaging/windows/build-sfx.ps1").read_text()
    assert f"'maturin=={MATURIN}'" in sfx and f"$env:RUSTUP_TOOLCHAIN = '{RUST}'" in sfx
    # Every recipe builds the lock's dependency set.
    for text in (helper, (REPO / "Dockerfile").read_text(), sfx):
        assert "maturin build --release --locked" in text or "build --release --locked" in text
