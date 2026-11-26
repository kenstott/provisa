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

from provisa.codesource import wheel_build

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
    assert argv[:4] == ["/bin/rustup", "run", wheel_build.RUST_TOOLCHAIN, "/bin/cargo-zigbuild"]
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


def test_the_built_wheel_is_the_one_build_image_takes(tmp_path: Path, crate: Path, targets):
    """image_build._encoders finds the source-built wheel by its version and tag."""
    from provisa.codesource import image_build

    house = tmp_path / "wheels"
    wheel_build.build_wheel("0.1.0", "arm64", house, run=Cargo(), which=_which(), crate=crate)
    found = sorted(house.glob("provisa-*-manylinux_2_28_aarch64.whl"))
    assert [w for w in found if image_build._same_version(w, "0.1.0")]  # noqa: SLF001
