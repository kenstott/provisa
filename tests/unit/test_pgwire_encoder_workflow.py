# Copyright (c) 2026 Kenneth Stott
# Canary: d8921642-c565-473d-9076-7b455136b719
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's Rust encoders are compiled into the provisa package itself (REQ-2194): one extension
module, provisa._encoders, whose submodules are the encoders (pgwire now). provisa is released as
four platform wheels, one per supported platform (REQ-294: macOS arm64, Linux x86_64, Linux arm64,
Windows x64), each carrying the module, its licence notices and the UI built once; there
is no separate PyPI project. Every installer and image builds provisa with the module in it and
imports it, so an artifact without it is never built."""

# Requirements: REQ-2194, REQ-294

from __future__ import annotations

import fnmatch
import hashlib
import importlib.util
import re
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
EXTENSION = REPO / "rust" / "encoders"
PGWIRE = REPO / "rust" / "pgwire"
NOTICES = REPO / "provisa" / "_encoders.THIRD_PARTY_NOTICES"
#: The Rust toolchain every recipe uses.
RUST = "1.95.0"
#: The four platforms of provisa's wheels (no Intel Mac): label -> (runner, platform tag pattern).
PLATFORMS = {
    "linux-x86_64": ("ubuntu-latest", "manylinux_2_28_x86_64"),
    "linux-aarch64": ("ubuntu-24.04-arm", "manylinux_2_28_aarch64"),
    "macos-arm64": ("macos-15", r"macosx_\d+_\d+_arm64"),
    "windows-x64": ("windows-latest", "win_amd64"),
}


def _toml(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def _workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def _jobs(name: str) -> dict:
    return _workflow(name)["jobs"]


def _steps(job: dict) -> dict:
    return {step["name"]: step for step in job["steps"] if "name" in step}


def _runs(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def _checker():
    spec = importlib.util.spec_from_file_location(
        "check_provisa_wheel", REPO / "scripts" / "check-provisa-wheel.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── provisa builds the extension itself ──────────────────────────────────────


def test_provisa_compiles_its_encoders_with_setuptools_rust():
    provisa = _toml(REPO / "pyproject.toml")
    assert provisa["build-system"]["build-backend"] == "setuptools.build_meta"
    assert "setuptools-rust==1.13.0" in provisa["build-system"]["requires"]
    (ext,) = provisa["tool"]["setuptools-rust"]["ext-modules"]
    assert ext == {
        "target": "provisa._encoders",
        "path": "rust/encoders/Cargo.toml",
        "binding": "PyO3",
        # An editable install compiles the release build too: a debug encoder is many times slower.
        "debug": False,
        "args": ["--locked"],
    }
    # The two package roots and the staged UI are setuptools' own configuration, unchanged.
    find = provisa["tool"]["setuptools"]["packages"]["find"]
    assert find["where"] == [".", "vendor/buenavista"]
    assert "_ui/**/*" in provisa["tool"]["setuptools"]["package-data"]["provisa"]


def test_there_is_no_separate_encoder_package():
    provisa = _toml(REPO / "pyproject.toml")
    assert not [d for d in provisa["project"]["dependencies"] if "pgwire-encoder" in d]
    assert "provisa-pgwire-encoder" not in provisa["tool"]["uv"]["sources"]
    assert "provisa-pgwire-encoder" not in (REPO / "uv.lock").read_text()
    assert not (PGWIRE / "pyproject.toml").exists()
    assert not (REPO / "scripts" / "build-pgwire-encoder-wheel.sh").exists()
    for path in WORKFLOWS.glob("*.yml"):
        assert "pgwire-encoder" not in path.read_text(), path.name
    assert "pgwire-encoder" not in (REPO / "packaging/pypi/requirements-embedded.lock").read_text()


def test_uv_rebuilds_provisa_when_the_rust_source_changes():
    keys = {entry["file"] for entry in _toml(REPO / "pyproject.toml")["tool"]["uv"]["cache-keys"]}
    assert {
        "pyproject.toml",
        "rust/encoders/Cargo.toml",
        "rust/encoders/Cargo.lock",
        "rust/encoders/src/**/*.rs",
        "rust/pgwire/Cargo.toml",
        "rust/pgwire/src/**/*.rs",
        "rust/pgwire/third_party/**/*",
    } <= keys


def test_the_extension_serves_each_encoder_as_a_submodule():
    """provisa._encoders is one module; pgwire is its submodule (graphql and grpc join it)."""
    cargo = _toml(EXTENSION / "Cargo.toml")
    assert cargo["lib"] == {"name": "_encoders", "crate-type": ["cdylib"]}
    assert cargo["dependencies"]["pgwire_encoder"] == {"path": "../pgwire"}
    # The same pyo3 as the encoder crate, so the two link as one.
    assert cargo["dependencies"]["pyo3"] == _toml(PGWIRE / "Cargo.toml")["dependencies"]["pyo3"]
    lib = (EXTENSION / "src" / "lib.rs").read_text()
    assert '#[pyo3(name = "_encoders")]' in lib
    # The init it calls is the one the encoder crate's #[pymodule] exports.
    (pymodule,) = re.findall(r"#\[pymodule\]\s*fn (\w+)", (PGWIRE / "src" / "lib.rs").read_text())
    assert f"fn PyInit_{pymodule}()" in lib
    assert 'add_encoder(m, "pgwire",' in lib
    assert ".set_item(&qualified, &module)" in lib


def test_the_encoder_crate_is_linked_and_the_local_build_builds_the_shipped_module():
    assert _toml(PGWIRE / "Cargo.toml")["lib"]["crate-type"] == ["rlib"]
    build = (REPO / "rust" / "build-local.sh").read_text()
    assert 'cd "$root/rust/encoders"' in build
    assert "cargo build --release --lib" in build


def test_the_build_root_carries_the_encoder_crates_release_profile():
    """Only the build root's profile applies, so the extension repeats the encoder's."""
    assert _toml(EXTENSION / "Cargo.toml")["profile"] == _toml(PGWIRE / "Cargo.toml")["profile"]


def test_no_shipped_build_is_tuned_for_the_build_machines_cpu():
    """A wheel built with target-cpu=native on one CI runner can stop on a user's older CPU with
    an illegal instruction. Only rust/build-local.sh asks for it, for a benchmark (NATIVE=1)."""
    config = _toml(REPO / "rust" / ".cargo" / "config.toml")
    flags = [f for target in config.get("target", {}).values() for f in target.get("rustflags", [])]
    flags += config.get("build", {}).get("rustflags", [])
    assert not [f for f in flags if "target-cpu" in f], flags
    for path in (PGWIRE / "Cargo.toml", EXTENSION / "Cargo.toml", REPO / "pyproject.toml"):
        assert "target-cpu" not in path.read_text(), path
    assert not (EXTENSION / ".cargo").exists() and not (PGWIRE / ".cargo").exists()
    assert "target-cpu=native" in (REPO / "rust" / "build-local.sh").read_text()


def test_the_sdist_carries_the_rust_source():
    manifest = (REPO / "MANIFEST.in").read_text().splitlines()
    assert "include rust/encoders/Cargo.toml rust/encoders/Cargo.lock" in manifest
    assert "recursive-include rust/encoders/src *.rs" in manifest
    assert "include rust/pgwire/Cargo.toml" in manifest
    assert "recursive-include rust/pgwire/src *.rs" in manifest
    assert "graft rust/pgwire/third_party" in manifest


# ── licence notices ───────────────────────────────────────────────────────────


def test_the_notices_were_written_from_the_current_lock():
    """scripts/rust-third-party-notices.sh records the lock it read: a lock change without a
    rerun leaves a crate the module links without its notice."""
    lock = EXTENSION / "Cargo.lock"
    first = NOTICES.read_text().splitlines()[0]
    assert (
        first == f"rust/encoders/Cargo.lock sha256: {hashlib.sha256(lock.read_bytes()).hexdigest()}"
    )


def test_every_noticed_crate_is_in_the_lock_and_the_main_ones_are_noticed():
    text = NOTICES.read_text()
    # A registry crate, or one vendored under rust/pgwire/third_party (ryu-pg, chrono-tz).
    vendored = {path.name for path in (PGWIRE / "third_party").iterdir()}
    locked = {
        (pkg["name"], pkg["version"])
        for pkg in _toml(EXTENSION / "Cargo.lock")["package"]
        if "source" in pkg or pkg["name"] in vendored
    }
    noticed = set(re.findall(r"^  ([\w-]+) (\d+\.\d+\.\d+\S*)(?: \(|$)", text, re.M))
    assert noticed and noticed <= locked, noticed - locked
    names = {name for name, _ in noticed}
    assert {"arrow", "pyo3", "itoa", "ryu-pg", "chrono-tz"} <= names
    # Provisa's own crates carry Provisa's licence, not a third party's.
    assert not names & {"provisa_encoders", "pgwire_encoder"}


def test_the_notices_ship_beside_the_module():
    provisa = _toml(REPO / "pyproject.toml")
    assert (
        "_encoders.THIRD_PARTY_NOTICES" in provisa["tool"]["setuptools"]["package-data"]["provisa"]
    )
    assert "provisa/_encoders.THIRD_PARTY_NOTICES" in (REPO / "THIRD_PARTY_NOTICES").read_text()
    about = _toml(EXTENSION / "about.toml")
    assert set(about["targets"]) == {
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
    }
    assert about["private"] == {"ignore": True}


# ── the wheel check every recipe runs ─────────────────────────────────────────


def _wheel(tmp_path: Path, name: str, members: list[str]) -> Path:
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as zf:
        for member in members:
            zf.writestr(member, b"")
    return path


_GOOD = [
    "provisa/__init__.py",
    "provisa/_encoders.cpython-312-darwin.so",
    "provisa/_encoders.THIRD_PARTY_NOTICES",
]


def test_the_wheel_check_takes_a_platform_wheel_with_the_module(tmp_path):
    check = _checker().check
    wheel = _wheel(tmp_path, "provisa-1.2.3-cp312-cp312-macosx_11_0_arm64.whl", _GOOD)
    assert check(wheel, None) == []
    assert check(wheel, r"macosx_\d+_\d+_arm64") == []
    windows = ["provisa/_encoders.cp312-win_amd64.pyd", "provisa/_encoders.THIRD_PARTY_NOTICES"]
    assert (
        check(_wheel(tmp_path, "provisa-1.2.3-cp312-cp312-win_amd64.whl", windows), "win_amd64")
        == []
    )
    cross = ["provisa/_encoders.so", "provisa/_encoders.THIRD_PARTY_NOTICES"]
    linux = _wheel(tmp_path, "provisa-1.2.3-cp312-cp312-manylinux_2_28_aarch64.whl", cross)
    assert check(linux, "manylinux_2_28_aarch64") == []


@pytest.mark.parametrize(
    ("name", "members", "problem"),
    [
        ("provisa-1.2.3-py3-none-any.whl", _GOOD, "is not a cp312 platform wheel"),
        ("provisa-1.2.3-cp312-cp312-macosx_11_0_arm64.whl", _GOOD[:1] + _GOOD[2:], "found []"),
        (
            "provisa-1.2.3-cp312-cp312-macosx_11_0_arm64.whl",
            [*_GOOD, "provisa/_encoders.so"],
            "expected one compiled provisa._encoders module",
        ),
        (
            "provisa-1.2.3-cp312-cp312-macosx_11_0_arm64.whl",
            _GOOD[:2],
            "THIRD_PARTY_NOTICES is missing",
        ),
    ],
)
def test_the_wheel_check_refuses_a_wheel_without_the_module(tmp_path, name, members, problem):
    problems = _checker().check(_wheel(tmp_path, name, members), None)
    assert any(problem in p for p in problems), problems


def test_the_wheel_check_refuses_another_platform(tmp_path):
    wheel = _wheel(tmp_path, "provisa-1.2.3-cp312-cp312-macosx_11_0_arm64.whl", _GOOD)
    assert _checker().check(wheel, "manylinux_2_28_x86_64")


def test_the_zig_cargo_pins_glibc_on_the_target_only(tmp_path):
    """setuptools-rust hands rustc the plain target; cargo-zigbuild gets it with glibc 2.28."""
    fake = tmp_path / "cargo-zigbuild"
    fake.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n')
    fake.chmod(0o755)
    out = subprocess.run(
        [
            str(REPO / "scripts" / "cargo-zigbuild-manylinux"),
            "rustc",
            "--lib",
            "--target",
            "aarch64-unknown-linux-gnu",
            "--",
            "-Clink-arg=x",
        ],
        env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert out == [
        "rustc",
        "--lib",
        "--target",
        "aarch64-unknown-linux-gnu.2.28",
        "--",
        "-Clink-arg=x",
    ]
    metadata = subprocess.run(
        [str(REPO / "scripts" / "cargo-zigbuild-manylinux"), "metadata", "--format-version", "1"],
        env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert metadata == ["metadata", "--format-version", "1"]


# ── the release ───────────────────────────────────────────────────────────────


def test_the_ui_is_built_once_for_every_wheel():
    jobs = _jobs("release-packages.yml")
    ui = jobs["build-provisa-ui"]
    assert "npm run build" in _runs(ui)
    upload = _steps(ui)["Upload the built UI"]
    assert upload["with"]["name"] == "provisa-ui-build"
    assert upload["with"]["path"] == "provisa-ui/dist/"
    wheel = jobs["build-provisa-wheel"]
    assert "build-provisa-ui" in wheel["needs"]
    download = _steps(wheel)["Download the built UI"]
    assert download["with"] == {"name": "provisa-ui-build", "path": "provisa-ui/dist/"}
    build = _steps(wheel)[
        "Build the wheel (bundles the UI, config, buenavista and provisa._encoders)"
    ]
    assert build["run"].count("PROVISA_SKIP_UI_BUILD=1") == 2
    assert "npm" not in _runs(wheel)


def test_the_release_builds_a_provisa_wheel_for_every_platform_and_imports_each():
    job = _jobs("release-packages.yml")["build-provisa-wheel"]
    rows = {row["label"]: row for row in job["strategy"]["matrix"]["include"]}
    assert {label: (row["runner"], row["tag"]) for label, row in rows.items()} == PLATFORMS
    # No Intel Mac wheel: every row builds natively, none cross-compiles for another target.
    assert not [label for label, row in rows.items() if "target" in row]
    assert rows["macos-arm64"]["arch"] == "arm64"
    assert job["strategy"]["fail-fast"] is False
    steps = _steps(job)
    assert steps["Set up Python"]["with"] == {
        "python-version": "3.12",
        "architecture": "${{ matrix.arch }}",
    }
    assert "rustup target add" not in steps["Install the Rust toolchain"]["run"]
    verify = steps["Verify the wheel's tag and import its encoders"]
    assert (
        'scripts/check-provisa-wheel.py dist/provisa-*.whl --tag "$TAG" --import' in verify["run"]
    )
    assert verify["env"]["TAG"] == "${{ matrix.tag }}"
    assert "provisa/_ui/index.html" in steps["Verify the wheel is self-contained"]["run"]
    upload = steps["Upload dist"]
    assert upload["with"]["name"] == "provisa-wheel-${{ matrix.label }}-dist"
    assert upload["with"]["if-no-files-found"] == "error"


def test_the_sdist_is_published_only_because_a_wheel_builds_from_it():
    job = _jobs("release-packages.yml")["build-provisa-wheel"]
    rows = job["strategy"]["matrix"]["include"]
    assert [row["label"] for row in rows if row.get("sdist")] == ["linux-x86_64"]
    steps = _steps(job)
    assert (
        "scripts/build-wheel.sh --sdist --wheel"
        in steps["Build the wheel (bundles the UI, config, buenavista and provisa._encoders)"][
            "run"
        ]
    )
    check = steps["Build a wheel from the sdist and import its encoders"]
    assert check["if"] == "matrix.sdist"
    assert "pip wheel" in check["run"] and "dist/provisa-*.tar.gz" in check["run"]
    assert "check-provisa-wheel.py sdist-check/provisa-*.whl --import" in check["run"]


def test_the_release_stamps_provisas_version():
    stamp = _steps(_jobs("release-packages.yml")["build-provisa-wheel"])["Stamp the version"]["run"]
    assert 'version = "${{ needs.metadata.outputs.py_version }}"' in stamp
    assert '"provisa-notebook==${{ needs.metadata.outputs.py_version }}"' in stamp


def test_the_sbom_and_cve_gate_run_once_for_every_wheel():
    jobs = _jobs("release-packages.yml")
    sbom = jobs["provisa-sbom"]
    runs = _runs(sbom)
    assert "pip-audit" in runs and "cyclonedx-py" in runs
    assert "pip-audit" not in _runs(jobs["build-provisa-wheel"])


def test_provisas_pypi_upload_takes_every_platforms_wheel():
    """The download is a pattern (one artifact per platform): it has to match each of them, and
    the -dist names are also what the release attaches."""
    jobs = _jobs("build-dmg.yml")
    assert not [name for name in jobs if "pgwire" in name]
    download = _steps(jobs["publish-provisa-pypi"])["Download dist"]
    assert download["uses"] == "actions/download-artifact@v4"
    assert download["with"]["merge-multiple"] is True
    pattern = download["with"]["pattern"]
    for label in PLATFORMS:
        artifact = f"provisa-wheel-{label}-dist"
        assert fnmatch.fnmatch(artifact, pattern), artifact
        assert fnmatch.fnmatch(artifact, "*-dist"), artifact
    assert not fnmatch.fnmatch("provisa-ui-build", pattern)
    attached = _jobs("release-publish.yml")["publish-release"]["steps"]
    assert any((s.get("with") or {}).get("pattern") == "*-dist" for s in attached)


# ── installers and images ─────────────────────────────────────────────────────


def test_every_installer_builds_provisa_with_its_encoders_and_fails_without_them():
    dmg = (REPO / "packaging/macos/build-dmg.sh").read_text()
    assert "provisa._encoders.pgwire" in dmg and "_encoders.THIRD_PARTY_NOTICES" in dmg
    appimage = (REPO / "packaging/linux/build-appimage.sh").read_text()
    assert "check-provisa-wheel.py" in appimage
    assert "--tag manylinux_2_28_x86_64 --import" in appimage
    first_launch = (REPO / "packaging/linux/first-launch.sh").read_text()
    assert '-c "import provisa._encoders.pgwire"' in first_launch
    sfx = (REPO / "packaging/windows/build-sfx.ps1").read_text()
    assert "provisa._encoders.pgwire" in sfx and "_encoders.THIRD_PARTY_NOTICES" in sfx
    for text in (dmg, appimage, first_launch, sfx):
        assert "pgwire_encoder_rs" not in text and "pgwire-encoder" not in text
    build = (REPO / "scripts/build-wheel.sh").read_text()
    assert "--target provisa-wheel" in build
    assert 'scripts/check-provisa-wheel.py" "${REPO_ROOT}"/dist/provisa-*.whl' in build


def test_the_installers_check_the_runtime_not_the_checkout():
    """The DMG and SFX check the bundled runtime from the repository root, where `python -c` puts
    the checkout first on sys.path. The checkout's provisa has the notices but no compiled module,
    so an unisolated check imports it in place of the installed wheel (alpha.499). -I drops it."""
    dmg = (REPO / "packaging/macos/build-dmg.sh").read_text()
    assert '"$py" -I -c "import provisa, sqlite3, mcp_proxy, provisa._encoders.pgwire' in dmg
    sfx = (REPO / "packaging/windows/build-sfx.ps1").read_text()
    checks = re.findall(r"^& \$RuntimePy (.*?)-c \"import ", sfx, re.M)
    assert checks and all(flags == "-I " for flags in checks)


def test_a_macos_wheel_is_tagged_for_the_one_architecture_its_module_is_built_for():
    """A universal2 Python would tag an arm64-only module's wheel universal2, and an Intel Mac
    would install it and fail to import the module."""
    build = (REPO / "scripts/build-wheel.sh").read_text()
    assert 'MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"' in build
    assert "import platform; print(platform.machine())" in build
    assert '"-C--build-option=--plat-name=macosx-${MACOSX_DEPLOYMENT_TARGET}-${arch}"' in build


def test_a_linux_wheel_is_built_for_x86_64_and_arm64():
    """REQ-294: Linux arm64 is a supported platform; build-wheel.sh builds the Linux wheel for
    x86_64 and arm64 and refuses any other machine by name."""
    build = (REPO / "scripts/build-wheel.sh").read_text()
    assert "x86_64) platform=linux/amd64 ;;" in build
    assert "aarch64 | arm64) platform=linux/arm64 ;;" in build
    assert 'echo "[build-wheel] ERROR: no Linux wheel for $(uname -m)" >&2; exit 1' in build


def test_every_image_builds_provisa_in_one_linux_recipe_and_imports_its_encoders():
    """The Dockerfile's provisa-wheel stage is the one Linux recipe: the images, every Linux wheel
    (build-wheel.sh), the release's arm64 wheelhouse and the cloud push all build it."""
    dockerfile = (REPO / "Dockerfile").read_text()
    stage = dockerfile[dockerfile.index("FROM --platform=$BUILDPLATFORM rust:") :]
    stage = stage[: stage.index("FROM scratch AS provisa-wheel")]
    assert "FROM --platform=$BUILDPLATFORM python:3.12-slim AS provisa-wheel-build" in stage
    assert "CARGO=cargo-zigbuild-manylinux" in stage
    assert '"-C--build-option=--plat-name=manylinux_2_28_$arch"' in stage
    dev = (REPO / "Dockerfile.dev").read_text()
    assert stage.split("\n", 1)[1].strip() in dev, "Dockerfile.dev's provisa-wheel stage drifted"
    for path, text in (("Dockerfile", dockerfile), ("Dockerfile.dev", dev)):
        assert "COPY --from=provisa-wheel-build /wheels/ /tmp/provisa-wheel/" in text, path
        assert text.count('python -c "import provisa._encoders.pgwire"') == 2, path
        assert "import pgwire_encoder_rs" not in text, path
    # The runtime runs /app/provisa, so the module is copied into that tree.
    assert "COPY --from=installer /encoders/ ./provisa/" in dockerfile
    prebuilt = _jobs("release-prebuilt.yml")["pull-images-core"]
    run = _steps(prebuilt)["Build provisa wheels for linux/arm64"]["run"]
    assert run.index("--target provisa-wheel") < run.index("pip wheel")
    assert "--tag manylinux_2_28_aarch64" in run and "import provisa._encoders.pgwire" in run
    image = _jobs("docker-image.yml")["image"]
    assert 'python -c "import provisa._encoders.pgwire"' in _runs(image)
    cloud = (REPO / "scripts/deploy-cloud.sh").read_text()
    assert "--target provisa-wheel" in cloud
    assert "_encoders.cpython-312-$arch-linux-gnu.so" in cloud
    ignored = (REPO / ".dockerignore").read_text().splitlines()
    assert {"rust/encoders/target/", "provisa/_encoders*.so", "provisa/_encoders*.pyd"} <= set(
        ignored
    )


def test_every_recipe_names_one_toolchain():
    build = (REPO / "scripts/build-wheel.sh").read_text()
    assert f'RUSTUP_TOOLCHAIN="${{RUSTUP_TOOLCHAIN:-{RUST}}}"' in build
    for path in ("Dockerfile", "Dockerfile.dev"):
        assert f"rust:{RUST}-slim-trixie" in (REPO / path).read_text(), path
    assert (
        f"$env:RUSTUP_TOOLCHAIN = '{RUST}'"
        in (REPO / "packaging/windows/build-sfx.ps1").read_text()
    )
    for name, jobs in (
        ("release-macos.yml", ("build-macos-core", "build-macos-container")),
        ("release-windows.yml", ("build-windows-core",)),
        ("release-packages.yml", ("build-provisa-wheel",)),
    ):
        for job in jobs:
            install = _steps(_jobs(name)[job])["Install the Rust toolchain"]
            assert f"rustup toolchain install {RUST} --profile minimal" in install["run"], (
                name,
                job,
            )
