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

The lead's decision (2026-11-26): a source-built wheel carries the SHA-256 of the rust/ sources it
was compiled from (SOURCE_HASH_FILE in its dist-info), and a wheel whose hash is not the
checkout's is stale and rebuilt, so a source checkout never serves encoders older than its rust/.
A released wheel carries none and is keyed by its version alone.
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

#: The dist-info file of a source-built wheel naming the SHA-256 of the rust/ sources it was
#: compiled from (source_hash).
SOURCE_HASH_FILE = "PROVISA_SOURCE_SHA256"
#: The files under rust/ a build compiles, as rust/ensure-local-build.sh selects them.
_SOURCE_NAMES = ("Cargo.toml", "Cargo.lock", "build.rs")
_SOURCE_DIRS = ("src", "third_party")

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


def _is_source(relative: Path) -> bool:
    parts = relative.parts
    return (
        relative.name in _SOURCE_NAMES
        or any(d in parts[:-1] for d in _SOURCE_DIRS)
        or parts[-2:] == (".cargo", "config.toml")
    )


def source_hash(rust: Path) -> str:
    """The SHA-256 of the Rust sources under ``rust`` (the checkout's rust/, whose crates the
    encoders crate depends on by path): every selected file's path and contents, in path order,
    with cargo's ``target`` directories left out."""
    files = sorted(
        path.relative_to(rust)
        for path in rust.rglob("*")
        if path.is_file()
        and "target" not in path.relative_to(rust).parts
        and _is_source(path.relative_to(rust))
    )
    digest = hashlib.sha256()
    for relative in files:
        data = (rust / relative).read_bytes()
        digest.update(f"{relative.as_posix()}\0{len(data)}\0".encode())
        digest.update(data)
    return digest.hexdigest()


def built_from(wheel: Path) -> str | None:
    """The rust/ source hash a source-built wheel carries, or None for a wheel not built from
    source here (a released one)."""
    with zipfile.ZipFile(wheel) as archive:
        names = [n for n in archive.namelist() if n.endswith(f".dist-info/{SOURCE_HASH_FILE}")]
        if not names:
            return None
        return archive.read(names[0]).decode().strip()


def checkout_hash() -> str | None:
    """The source hash of this Provisa's checkout, or None when it runs from no source checkout
    (an installed Provisa has no rust/)."""
    crate = encoders_crate()
    return source_hash(crate.parent) if (crate / "Cargo.toml").is_file() else None


def _target(arch: str) -> str:
    return f"{WHEEL_ARCH[arch]}-unknown-linux-gnu"


#: rustup never installs a toolchain behind a probe: a missing one is refused by name.
_NO_AUTO_INSTALL = {"RUSTUP_AUTO_INSTALL": "0"}


def _rustup(rustup: str, *args: str) -> str | None:
    """rustup's output for ``args`` on the pinned toolchain, or None when it is not installed."""
    done = subprocess.run(  # nosec B603 - fixed argv, no shell
        [rustup, *args, "--toolchain", RUST_TOOLCHAIN],
        capture_output=True,
        text=True,
        env={**os.environ, **_NO_AUTO_INSTALL},
    )
    return done.stdout if done.returncode == 0 else None


def _installed_targets(rustup: str) -> list[str] | None:
    """The targets the pinned toolchain has, or None when the toolchain is not installed."""
    listed = _rustup(rustup, "target", "list", "--installed")
    return None if listed is None else listed.split()


def _toolchain_bin(rustup: str) -> Path:
    """The pinned toolchain's bin directory (its cargo and rustc), put first on cargo-zigbuild's
    PATH: ``rustup run`` leaves a cargo that is not rustup's (Homebrew's) ahead of it."""
    cargo = _rustup(rustup, "which", "cargo")
    if cargo is None:
        raise WheelBuildRefused(
            "image_wheel_toolchain",
            f"rustup lists the Rust {RUST_TOOLCHAIN} toolchain but names no cargo in it",
        )
    return Path(cargo.strip()).parent


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


def _toolchain(arch: str, which: Which) -> tuple[Path, str, dict[str, str]]:
    """The pinned toolchain's bin, cargo-zigbuild and zig's environment, or refused naming the
    first one missing."""
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
    return _toolchain_bin(rustup), zigbuild, zig


def _record_line(name: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{name},sha256={digest},{len(data)}"


def _pack(module: bytes, version: str, arch: str, sources: str, out: Path) -> None:
    """A wheel holding provisa/_encoders.so for ``arch`` built from the rust/ sources whose hash
    is ``sources``, written whole or not at all."""
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
        f"{info}/{SOURCE_HASH_FILE}": f"{sources}\n".encode(),
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
    """The Linux provisa ``version`` wheel for ``arch`` in ``house``: the one already there when
    it was built from this checkout's rust/ sources as they are now, else compiled from its
    rust/encoders and cached there. Blocking."""
    if arch not in WHEEL_ARCH:
        raise WheelBuildRefused(
            "image_arch", f"a wheel is built for {', '.join(WHEEL_ARCH)}, not {arch!r}", arch=arch
        )
    out = house / wheel_name(version, arch)
    crate = crate or encoders_crate()
    if not (crate / "Cargo.toml").is_file():
        raise WheelBuildRefused(
            "image_wheel_source",
            f"no provisa {version} wheel for linux/{arch} to build: {crate} is not a Provisa "
            "source checkout's rust/encoders",
            arch=arch,
        )
    sources = source_hash(crate.parent)
    if out.is_file():
        built = built_from(out)
        if built == sources:
            return out
        log.info(
            "Build image: %s was built from rust/ sources %s, not the checkout's %s: rebuilding",
            out.name,
            built,
            sources,
        )
    toolchain, zigbuild, zig_env = _toolchain(arch, which)
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
            **_NO_AUTO_INSTALL,
            "PATH": os.pathsep.join([str(toolchain), os.environ["PATH"]]),
            "CARGO": str(toolchain / "cargo"),
            "RUSTC": str(toolchain / "rustc"),
            "CARGO_TARGET_DIR": str(target_dir),
            "PYO3_CROSS_PYTHON_VERSION": "3.12",
        }
        # The target's glibc suffix is cargo-zigbuild's (scripts/cargo-zigbuild-manylinux).
        run(
            [
                zigbuild, "zigbuild", "--release", "--locked", "--lib",
                "--target", f"{target}.{GLIBC}",
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
        _pack(module.read_bytes(), version, arch, sources, out)
    log.info("Build image: cached the provisa %s wheel for linux/%s at %s", version, arch, out)
    return out
