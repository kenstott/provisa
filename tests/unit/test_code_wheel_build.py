# Copyright (c) 2026 Kenneth Stott
# Canary: 9b41d6e3-2a7f-4c58-b0e9-6f13a8c5d274
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: Build image compiles the Linux provisa wheel from a source checkout, once, into the
wheelhouse; without the toolchain it is refused naming what to install."""

# Requirements: REQ-2194, REQ-2098
from __future__ import annotations

import logging
import zipfile
from collections.abc import Sequence
from pathlib import Path

import pytest

from provisa.codesource import wheel_build, wheel_source

VERSION = "0.1.0a510"
TAG = "v0.1.0-alpha.510"

TOOLCHAIN = Path("/rustup/toolchains/1.95.0/bin")
TOOLS = {"rustup": "/bin/rustup", "cargo-zigbuild": "/bin/cargo-zigbuild", "zig": "/bin/zig"}


def _which(missing: str | None = None):
    return lambda name: None if name == missing else TOOLS.get(name)


@pytest.fixture
def crate(tmp_path: Path) -> Path:
    root = tmp_path / "rust" / "encoders"
    root.mkdir(parents=True)
    (root / "Cargo.toml").write_text("[package]\n")
    return root


@pytest.fixture
def targets(monkeypatch):
    installed = ["aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu"]
    monkeypatch.setattr(wheel_build, "_installed_targets", lambda rustup: installed)
    monkeypatch.setattr(wheel_build, "_toolchain_bin", lambda rustup: TOOLCHAIN)
    return installed


class Cargo:
    """cargo-zigbuild as the build runs it: writes the module where cargo puts it."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], Path, dict[str, str]]] = []

    def __call__(self, argv: Sequence[str], cwd: Path, env: dict[str, str]) -> None:
        self.calls.append((list(argv), cwd, env))
        target = argv[argv.index("--target") + 1].rsplit(".", 2)[0]
        out = Path(env["CARGO_TARGET_DIR"]) / target / "release"
        out.mkdir(parents=True)
        (out / "lib_encoders.so").write_bytes(f"encoders {target}".encode())


@pytest.mark.parametrize(("arch", "machine"), [("arm64", "aarch64"), ("amd64", "x86_64")])
def test_the_wheel_is_built_from_source_for_the_architecture_and_cached(
    tmp_path: Path, crate: Path, targets, arch: str, machine: str, caplog
):
    house = tmp_path / "wheels"
    cargo = Cargo()
    with caplog.at_level(logging.INFO, logger=wheel_build.__name__):
        wheel = wheel_build.build_wheel(
            "0.1.0", arch, house, run=cargo, which=_which(), crate=crate
        )
    assert wheel == house / f"provisa-0.1.0-cp312-cp312-manylinux_2_28_{machine}.whl"
    (argv, cwd, env) = cargo.calls[0]
    assert argv[:2] == ["/bin/cargo-zigbuild", "zigbuild"]
    # The pinned toolchain's cargo and rustc, ahead of any other on PATH.
    assert env["PATH"].split(":")[0] == str(TOOLCHAIN)
    assert env["CARGO"] == str(TOOLCHAIN / "cargo")
    assert env["RUSTC"] == str(TOOLCHAIN / "rustc")
    assert env["RUSTUP_AUTO_INSTALL"] == "0"
    assert argv[argv.index("--target") + 1] == f"{machine}-unknown-linux-gnu.2.28"
    assert "--locked" in argv and "--release" in argv
    assert cwd == crate
    assert env["PYO3_CROSS_PYTHON_VERSION"] == "3.12"
    with zipfile.ZipFile(wheel) as zf:
        assert zf.read("provisa/_encoders.so") == f"encoders {machine}-unknown-linux-gnu".encode()
        wheel_meta = zf.read("provisa-0.1.0.dist-info/WHEEL").decode()
        assert f"Tag: cp312-cp312-manylinux_2_28_{machine}" in wheel_meta
        assert "provisa-0.1.0.dist-info/RECORD" in zf.namelist()
    assert not list(house.glob("*.partial"))
    text = caplog.text
    assert f"compiling the provisa 0.1.0 wheel for linux/{arch}" in text
    assert f"cached the provisa 0.1.0 wheel for linux/{arch} at {wheel}" in text


def test_a_cached_wheel_is_not_rebuilt(tmp_path: Path, crate: Path, targets):
    house = tmp_path / "wheels"
    cargo = Cargo()
    first = wheel_build.build_wheel("0.1.0", "arm64", house, run=cargo, which=_which(), crate=crate)

    def no_build(argv, cwd, env):
        raise AssertionError(f"rebuilt: {argv}")

    again = wheel_build.build_wheel(
        "0.1.0", "arm64", house, run=no_build, which=_which("rustup"), crate=crate
    )
    assert again == first
    assert len(cargo.calls) == 1


@pytest.mark.parametrize(
    ("missing", "named", "install"),
    [
        ("rustup", "rustup", "rustup toolchain install 1.95.0 --target aarch64-unknown-linux-gnu"),
        ("cargo-zigbuild", "cargo-zigbuild", "pip install cargo-zigbuild==0.24.0"),
        ("zig", "zig", "pip install ziglang==0.17.0"),
    ],
)
def test_a_missing_tool_is_refused_by_name_with_what_to_install(
    tmp_path: Path, crate: Path, targets, monkeypatch, missing: str, named: str, install: str
):
    monkeypatch.setattr(wheel_build, "_beside_python", lambda name: None)
    monkeypatch.setattr(wheel_build, "_zig_env", lambda which: {} if which("zig") else None)
    house = tmp_path / "wheels"
    with pytest.raises(wheel_build.WheelBuildRefused) as refused:
        wheel_build.build_wheel(
            "0.1.0", "arm64", house, run=Cargo(), which=_which(missing), crate=crate
        )
    assert refused.value.code == "codesource.image_wheel_toolchain"
    assert named in str(refused.value)
    assert install in str(refused.value)
    assert refused.value.params["install"] == install
    assert not house.exists()


def test_a_missing_target_is_refused_naming_the_rustup_target(
    tmp_path: Path, crate: Path, monkeypatch
):
    monkeypatch.setattr(wheel_build, "_installed_targets", lambda rustup: ["aarch64-apple-darwin"])
    with pytest.raises(wheel_build.WheelBuildRefused) as refused:
        wheel_build.build_wheel(
            "0.1.0", "amd64", tmp_path / "wheels", run=Cargo(), which=_which(), crate=crate
        )
    assert refused.value.params["install"] == (
        "rustup target add --toolchain 1.95.0 x86_64-unknown-linux-gnu"
    )


def test_a_missing_toolchain_is_refused_naming_its_install(
    tmp_path: Path, crate: Path, monkeypatch
):
    monkeypatch.setattr(wheel_build, "_installed_targets", lambda rustup: None)
    with pytest.raises(wheel_build.WheelBuildRefused) as refused:
        wheel_build.build_wheel(
            "0.1.0", "arm64", tmp_path / "wheels", run=Cargo(), which=_which(), crate=crate
        )
    assert "Rust 1.95.0 toolchain" in str(refused.value)
    assert refused.value.params["install"] == (
        "rustup toolchain install 1.95.0 --target aarch64-unknown-linux-gnu"
    )


def test_a_provisa_without_its_rust_sources_is_refused(tmp_path: Path, targets):
    with pytest.raises(wheel_build.WheelBuildRefused) as refused:
        wheel_build.build_wheel(
            "0.1.0", "arm64", tmp_path / "wheels", run=Cargo(), which=_which(),
            crate=tmp_path / "nowhere",
        )  # fmt: skip
    assert refused.value.code == "codesource.image_wheel_source"


def test_the_built_wheel_is_the_one_build_image_takes(
    tmp_path: Path, crate: Path, targets, monkeypatch
):
    """The wheelhouse source finds the source-built wheel by its version, tag and source hash."""
    monkeypatch.setattr(wheel_build, "encoders_crate", lambda: crate)
    house = tmp_path / "wheels"
    built = wheel_build.build_wheel(
        "0.1.0", "arm64", house, run=Cargo(), which=_which(), crate=crate
    )
    assert wheel_source.cached("0.1.0", "arm64", house) == built


# -- The ordered sources: cache, release, build from source (maintainer 2026-11-26) -----------


def test_the_sources_are_the_cache_then_the_release_then_the_source_build():
    cache, release, build = wheel_source.SOURCES
    assert cache is wheel_source.cached
    assert release.__qualname__ == "from_release.<locals>.source"
    assert build is wheel_build.build_wheel


class _NoRelease:
    """The release source with no published release of the version."""

    def __init__(self) -> None:
        self.listed: list[str] = []

    def list(self, tag: str) -> None:
        self.listed.append(tag)
        return None

    def fetch(self, url: str) -> bytes:
        raise AssertionError(f"fetched {url}")


def _sources(crate: Path, run, which=None, release: _NoRelease | None = None):
    def build(version: str, arch: str, house: Path) -> Path:
        return wheel_build.build_wheel(
            version, arch, house, run=run, which=which or _which(), crate=crate
        )

    release = release or _NoRelease()
    return (wheel_source.cached, wheel_source.from_release(release.list, release.fetch), build)


@pytest.fixture
def checkout(crate: Path, monkeypatch) -> Path:
    """This Provisa runs from the checkout whose rust/encoders is ``crate``."""
    (crate / "src").mkdir()
    (crate / "src" / "lib.rs").write_text("// encoders\n")
    monkeypatch.setattr(wheel_build, "encoders_crate", lambda: crate)
    return crate


def test_a_version_with_no_release_is_built_from_source(tmp_path: Path, checkout: Path, targets):
    house = tmp_path / "wheels"
    cargo = Cargo()
    release = _NoRelease()
    found = wheel_source.provisa_wheel(
        VERSION, "arm64", house, _sources(checkout, cargo, release=release)
    )
    assert release.listed == [TAG]
    assert found == house / f"provisa-{VERSION}-cp312-cp312-manylinux_2_28_aarch64.whl"
    assert len(cargo.calls) == 1
    assert wheel_build.built_from(found) == wheel_build.source_hash(checkout.parent)


def test_a_source_built_wheel_of_the_current_sources_is_taken_from_the_cache(
    tmp_path: Path, checkout: Path, targets
):
    house = tmp_path / "wheels"
    cargo = Cargo()
    first = wheel_source.provisa_wheel(VERSION, "arm64", house, _sources(checkout, cargo))
    release = _NoRelease()

    def no_build(argv, cwd, env):
        raise AssertionError(f"rebuilt: {argv}")

    again = wheel_source.provisa_wheel(
        VERSION, "arm64", house, _sources(checkout, no_build, release=release)
    )
    assert again == first
    assert release.listed == []


def test_a_source_built_wheel_of_other_sources_is_rebuilt(
    tmp_path: Path, checkout: Path, targets, caplog
):
    house = tmp_path / "wheels"
    cargo = Cargo()
    first = wheel_source.provisa_wheel(VERSION, "arm64", house, _sources(checkout, cargo))
    assert isinstance(first, Path)
    old = wheel_build.built_from(first)
    (checkout / "src" / "lib.rs").write_text("// encoders, changed\n")
    missed = wheel_source.cached(VERSION, "arm64", house)
    assert isinstance(missed, wheel_source.Miss)
    assert "other than this checkout's" in missed.reason
    with caplog.at_level(logging.INFO, logger=wheel_build.__name__):
        again = wheel_source.provisa_wheel(VERSION, "arm64", house, _sources(checkout, cargo))
    assert again == first and len(cargo.calls) == 2
    new = wheel_build.built_from(again)
    assert new == wheel_build.source_hash(checkout.parent) and new != old
    assert f"built from rust/ sources {old}, not the checkout's {new}: rebuilding" in caplog.text


def test_a_released_wheel_in_the_cache_is_taken_by_its_version_alone(
    tmp_path: Path, checkout: Path
):
    house = tmp_path / "wheels"
    house.mkdir()
    released = house / f"provisa-{VERSION}-cp312-cp312-manylinux_2_28_aarch64.whl"
    with zipfile.ZipFile(released, "w") as zf:
        zf.writestr("provisa/_encoders.so", b"released")
        zf.writestr(f"provisa-{VERSION}.dist-info/METADATA", f"Version: {VERSION}\n")

    def no_build(argv, cwd, env):
        raise AssertionError(f"built: {argv}")

    assert wheel_source.provisa_wheel(VERSION, "arm64", house, _sources(checkout, no_build)) == (
        released
    )


def test_a_source_built_wheel_without_a_checkout_is_not_served(
    tmp_path: Path, checkout: Path, targets, monkeypatch
):
    """An installed Provisa has no rust/ to check a source-built wheel against."""
    house = tmp_path / "wheels"
    wheel_source.provisa_wheel(VERSION, "arm64", house, _sources(checkout, Cargo()))
    monkeypatch.setattr(wheel_build, "encoders_crate", lambda: tmp_path / "nowhere")
    assert isinstance(wheel_source.cached(VERSION, "arm64", house), wheel_source.Miss)


def test_the_source_hash_is_of_the_rust_sources_only(tmp_path: Path, checkout: Path):
    rust = checkout.parent
    before = wheel_build.source_hash(rust)
    (checkout / "target" / "release").mkdir(parents=True)
    (checkout / "target" / "release" / "lib_encoders.so").write_bytes(b"built")
    (checkout / "README.md").write_text("notes")
    assert wheel_build.source_hash(rust) == before
    (rust / "pgwire" / "src").mkdir(parents=True)
    (rust / "pgwire" / "src" / "lib.rs").write_text("// a path dependency")
    assert wheel_build.source_hash(rust) != before
