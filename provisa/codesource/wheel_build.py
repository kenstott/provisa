# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2c8a17-4d93-4b6f-8a01-c7f3e9d24b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build image's source-built wheel (REQ-2194): a Provisa run from a source checkout, whose version
has no published release, compiles the Linux provisa wheel the image's front end takes
provisa._encoders from, once per version and architecture, into the wheelhouse.

It is the Dockerfile's provisa-wheel-build recipe reduced to the one module the image needs: the
encoders crate (rust/encoders) cross-compiled with cargo-zigbuild for ``<arch>-unknown-linux-gnu``
against glibc 2.28 (scripts/cargo-zigbuild-manylinux), with the pinned Rust toolchain, for CPython
3.12, and packed as ``provisa-<version>-cp312-cp312-manylinux_2_28_<arch>.whl`` holding
``provisa/_encoders.so``. A build without the toolchain is refused by name, with what to install.
"""

# Requirements: REQ-2194, REQ-2098
from __future__ import annotations

import base64
import hashlib
import logging
import os
import shutil
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import sys
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import Runner, run_command

log = logging.getLogger(__name__)

#: The Rust toolchain every recipe builds the encoders with (Dockerfile's rust image,
#: scripts/build-wheel.sh RUSTUP_TOOLCHAIN).
RUST_TOOLCHAIN = "1.95.0"
#: The glibc the wheel's manylinux_2_28 tag promises (scripts/cargo-zigbuild-manylinux).
GLIBC = "2.28"
#: The CPython the image's front end runs (python:3.12, requires-python).
PYTHON_TAG = "cp312"
WHEEL_ARCH = {"amd64": "x86_64", "arm64": "aarch64"}
#: The pins the Dockerfile's provisa-wheel-build stage installs them at.
CARGO_ZIGBUILD = "cargo-zigbuild==0.24.0"
ZIGLANG = "ziglang==0.17.0"

Which = Callable[[str], str | None]


class WheelBuildRefused(BundleRefused):
    """A source build of the wheel that cannot be made, named with why."""


def wheel_name(version: str, arch: str) -> str:
    from packaging.version import Version

    canonical = str(Version(version)).replace("-", "_")
    return f"provisa-{canonical}-{PYTHON_TAG}-{PYTHON_TAG}-manylinux_2_28_{WHEEL_ARCH[arch]}.whl"


def encoders_crate() -> Path:
    """rust/encoders of the checkout this Provisa runs from."""
    import provisa

    return Path(provisa.__file__).resolve().parent.parent / "rust" / "encoders"


def _target(arch: str) -> str:
    return f"{WHEEL_ARCH[arch]}-unknown-linux-gnu"


def _installed_targets(rustup: str) -> list[str] | None:
    """The targets the pinned toolchain has, or None when the toolchain is not installed."""
    done = subprocess.run(  # nosec B603 - fixed argv, no shell
        [rustup, "target", "list", "--installed", "--toolchain", RUST_TOOLCHAIN],
        capture_output=True,
        text=True,
    )
    if done.returncode != 0:
        return None
    return done.stdout.split()


def _beside_python(name: str) -> str | None:
    """``name`` in the scripts directory of the interpreter Provisa runs on (a venv's bin)."""
    candidate = Path(sys.executable).parent / name
    return str(candidate) if candidate.is_file() and os.access(candidate, os.X_OK) else None


def _zig_env(which: Which) -> dict[str, str] | None:
    """How cargo-zigbuild finds zig: on PATH, or the ziglang package of Provisa's interpreter
    (where the Dockerfile's pip install puts it), named by CARGO_ZIGBUILD_ZIG_PATH."""
    if which("zig"):
        return {}
    from importlib.util import find_spec

    spec = find_spec("ziglang")
    if spec is None or spec.origin is None:
        return None  # not installed: the caller refuses naming zig
    zig = Path(spec.origin).parent / "zig"
    return {"CARGO_ZIGBUILD_ZIG_PATH": str(zig)} if zig.is_file() else None


def _refused(arch: str, missing: str, install: str) -> WheelBuildRefused:
    return WheelBuildRefused(
        "image_wheel_toolchain",
        f"building the provisa wheel for linux/{arch} from source needs {missing}: "
        f"install it with `{install}`",
        arch=arch,
        missing=missing,
        install=install,
    )


def _toolchain(arch: str, which: Which) -> tuple[str, str, dict[str, str]]:
    """rustup, cargo-zigbuild and zig's environment, or refused naming the first one missing."""
    target = _target(arch)
    rustup = which("rustup")
    if rustup is None:
        raise _refused(
            arch,
            "rustup (https://rustup.rs)",
            f"rustup toolchain install {RUST_TOOLCHAIN} --target {target}",
        )
    targets = _installed_targets(rustup)
    if targets is None:
        raise _refused(
            arch,
            f"the Rust {RUST_TOOLCHAIN} toolchain",
            f"rustup toolchain install {RUST_TOOLCHAIN} --target {target}",
        )
    if target not in targets:
        raise _refused(
            arch,
            f"the Rust {RUST_TOOLCHAIN} standard library for {target}",
            f"rustup target add --toolchain {RUST_TOOLCHAIN} {target}",
        )
    zigbuild = which("cargo-zigbuild") or _beside_python("cargo-zigbuild")
    if zigbuild is None:
        raise _refused(arch, "cargo-zigbuild", f"pip install {CARGO_ZIGBUILD}")
    zig = _zig_env(which)
    if zig is None:
        raise _refused(arch, "zig", f"pip install {ZIGLANG}")
    return rustup, zigbuild, zig


def _record_line(name: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{name},sha256={digest},{len(data)}"


def _pack(module: bytes, version: str, arch: str, out: Path) -> None:
    """A wheel holding provisa/_encoders.so for ``arch``, written whole or not at all."""
    from packaging.version import Version

    info = f"provisa-{str(Version(version)).replace('-', '_')}.dist-info"
    tag = f"{PYTHON_TAG}-{PYTHON_TAG}-manylinux_2_28_{WHEEL_ARCH[arch]}"
    files = {
        "provisa/_encoders.so": module,
        f"{info}/METADATA": (
            f"Metadata-Version: 2.1\nName: provisa\nVersion: {Version(version)}\n"
            "Summary: provisa._encoders, built from source for Build image\n"
        ).encode(),
        f"{info}/WHEEL": (
            f"Wheel-Version: 1.0\nGenerator: provisa.codesource.wheel_build\n"
            f"Root-Is-Purelib: false\nTag: {tag}\n"
        ).encode(),
    }
    record = [_record_line(name, data) for name, data in files.items()]
    record.append(f"{info}/RECORD,,")
    files[f"{info}/RECORD"] = ("\n".join(record) + "\n").encode()
    partial = out.with_name(out.name + ".partial")
    with zipfile.ZipFile(partial, "w", zipfile.ZIP_DEFLATED) as wheel:
        for name, data in files.items():
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.external_attr = (0o755 if name.endswith(".so") else 0o644) << 16
            entry.compress_type = zipfile.ZIP_DEFLATED
            wheel.writestr(entry, data)
    os.replace(partial, out)


def build_wheel(
    version: str,
    arch: str,
    house: Path,
    *,
    run: Runner = run_command,
    which: Which = shutil.which,
    crate: Path | None = None,
) -> Path:
    """The Linux provisa ``version`` wheel for ``arch`` in ``house``: the one already there, else
    compiled from this checkout's rust/encoders and cached there. Blocking."""
    if arch not in WHEEL_ARCH:
        raise WheelBuildRefused(
            "image_arch", f"a wheel is built for {', '.join(WHEEL_ARCH)}, not {arch!r}", arch=arch
        )
    out = house / wheel_name(version, arch)
    if out.is_file():
        return out
    crate = crate or encoders_crate()
    if not (crate / "Cargo.toml").is_file():
        raise WheelBuildRefused(
            "image_wheel_source",
            f"no provisa {version} wheel for linux/{arch} to build: {crate} is not a Provisa "
            "source checkout's rust/encoders",
            arch=arch,
        )
    rustup, zigbuild, zig_env = _toolchain(arch, which)
    target = _target(arch)
    log.info(
        "Build image: compiling the provisa %s wheel for linux/%s (%s, manylinux_2_28) from %s",
        version,
        arch,
        target,
        crate,
    )
    with tempfile.TemporaryDirectory(prefix="provisa-wheel-build-") as scratch:
        target_dir = Path(scratch) / "target"
        env = {
            **os.environ,
            **zig_env,
            "CARGO_TARGET_DIR": str(target_dir),
            "PYO3_CROSS_PYTHON_VERSION": "3.12",
        }
        # rustup run puts the pinned toolchain's cargo first on PATH for cargo-zigbuild; the
        # target's glibc suffix is cargo-zigbuild's (scripts/cargo-zigbuild-manylinux).
        run(
            [
                rustup, "run", RUST_TOOLCHAIN, zigbuild, "zigbuild", "--release", "--locked",
                "--lib", "--target", f"{target}.{GLIBC}",
            ],
            crate,
            env,
        )  # fmt: skip
        module = target_dir / target / "release" / "lib_encoders.so"
        if not module.is_file():
            raise WheelBuildRefused(
                "image_wheel_build",
                f"cargo-zigbuild built no {module.relative_to(target_dir)} for linux/{arch}",
                arch=arch,
            )
        house.mkdir(parents=True, exist_ok=True)
        _pack(module.read_bytes(), version, arch, out)
    log.info("Build image: cached the provisa %s wheel for linux/%s at %s", version, arch, out)
    return out
