# Copyright (c) 2026 Kenneth Stott
# Canary: 42fb45e9-0167-47d8-8aec-9db0a5ccc93d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading a Rust source's declarations (REQ-2227).

The reader (``reader/``, beside this module) is a Rust program shipped as source with its own
``Cargo.toml`` and ``Cargo.lock``. It is compiled once by the pinned toolchain
(:mod:`provisa.rustlang.toolchain`), its dependencies (syn, for parsing Rust) fetched through the
organisation's Cargo registry or crates.io and checked against the committed ``Cargo.lock``
(``--locked``), and the binary is cached by the reader's source and the toolchain's version. It
parses the crate's source -- its library's root file and every module file it declares -- and
prints what its public functions say; :mod:`provisa.codesource.manifest` applies the rules.

Reading parses the code and runs none of it, compiling nothing of the code's (Node's reader reads
declarations the same way), so the reader is not sandboxed and needs none of the code's
dependencies.

A Rust source is a crate: its ``Cargo.toml`` names a package and a library. A crate whose
``rust-version`` is newer than the pinned toolchain is refused, as Go's ``go`` line is.
"""

# Requirements: REQ-2227
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import tempfile
import tomllib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import RUST_RULES, Manifest, read_manifest
from provisa.rustlang.toolchain import Toolchain

READER = Path(__file__).with_name("reader")
_READER_SOURCES = ("Cargo.toml", "Cargo.lock", "src/main.rs")


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split(".") if part.isdigit())


def crate_dir(code: Path, entry: str) -> Path:
    """The crate's directory: ``entry``, a directory of the code ("." its root)."""
    relative = Path(entry)
    if relative.is_absolute() or ".." in relative.parts:
        raise BundleRefused("no_crate", f"{entry} is not a directory of the code", entry=entry)
    found = code / relative
    if not found.is_dir():
        raise BundleRefused("no_crate", f"the code has no directory {entry}", entry=entry)
    return found


def crate_manifest(crate: Path, toolchain: str) -> dict[str, Any]:
    """The crate's ``Cargo.toml``, read, refused by name when it is not a library crate this
    toolchain builds: no ``Cargo.toml``, no package (a workspace's root alone), or a
    ``rust-version`` newer than ``toolchain``."""
    path = crate / "Cargo.toml"
    if not path.is_file():
        raise BundleRefused("no_cargo_toml", "the code has no Cargo.toml: a Rust source is a crate")
    try:
        manifest = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as bad:
        raise BundleRefused("cargo_toml", f"the code's Cargo.toml cannot be read: {bad}") from bad
    package = manifest.get("package")
    if not isinstance(package, dict) or not isinstance(package.get("name"), str):
        raise BundleRefused(
            "no_package", "the code's Cargo.toml declares no package: a Rust source is a crate"
        )
    wanted = package.get("rust-version")
    if isinstance(wanted, str) and _version(wanted) > _version(toolchain):
        raise BundleRefused(
            "rust_too_new",
            f"the crate's rust-version is {wanted}, and this Provisa's Rust is {toolchain}",
            rust=wanted,
            pinned=toolchain,
        )
    return manifest


def library_root(crate: Path, manifest: dict[str, Any]) -> Path:
    """The library's root file: ``[lib] path``, else ``src/lib.rs``."""
    lib = manifest.get("lib")
    given = lib.get("path") if isinstance(lib, dict) else None
    root = crate / (given if isinstance(given, str) else "src/lib.rs")
    if not root.is_file():
        raise BundleRefused(
            "no_library",
            "the crate has no library (src/lib.rs): a Rust source's functions are a library's",
        )
    return root


def cargo_environment(
    toolchain: Toolchain, *, cargo_home: Path, target_dir: Path, **extra: str
) -> dict[str, str]:
    """The environment every cargo command Provisa runs gets: exactly this, nothing inherited.
    The compiler is the pinned toolchain's, the caches the ones named; the system's directories
    are on the path for the C compiler that links build scripts and procedural macros."""
    path = [str(d) for d in toolchain.bin_dirs()] + ["/usr/bin", "/bin"]
    return {
        "PATH": os.pathsep.join(path),
        "HOME": str(cargo_home.parent),
        "CARGO_HOME": str(cargo_home),
        "CARGO_TARGET_DIR": str(target_dir),
        "RUSTC": str(toolchain.rustc),
        "CARGO_INCREMENTAL": "0",
        "CARGO_TERM_COLOR": "never",
        "CARGO_NET_RETRY": "2",
        **extra,
    }


#: Run a command's argv with exactly ``env`` in ``cwd``; its stdout, refused by name on failure.
CargoRunner = Callable[[Sequence[str], dict[str, str], Path], str]


def run_cargo(argv: Sequence[str], env: dict[str, str], cwd: Path) -> str:
    # The pinned cargo or Provisa's own reader, with a fixed argv and no shell.
    done = subprocess.run(  # nosec B603
        list(argv), capture_output=True, text=True, env=env, cwd=cwd
    )
    if done.returncode != 0:
        raise BundleRefused(
            "cargo_failed",
            f"{Path(argv[0]).name} {' '.join(argv[1:2])} failed: {done.stderr.strip()[-3000:]}",
        )
    return done.stdout


def reader_key(version: str) -> str:
    """The reader's cache key: its own source and the toolchain that compiles it."""
    digest = hashlib.sha256(version.encode())
    for name in _READER_SOURCES:
        digest.update(b"\0" + name.encode() + b"\0" + (READER / name).read_bytes())
    return digest.hexdigest()[:16]


def ensure_reader(
    toolchain: Toolchain,
    cache_root: Path,
    registry_args: Sequence[str] = (),
    registry_env: dict[str, str] | None = None,
    run: CargoRunner = run_cargo,
) -> Path:
    """The compiled reader for this toolchain, built and cached on a miss. ``registry_args`` and
    ``registry_env`` are how cargo reaches the organisation's Cargo registry
    (:func:`provisa.rustlang.install.registry_setup`); none, crates.io."""
    home = cache_root / "code" / "rust-reader" / reader_key(toolchain.version)
    binary = home / "reader"
    if binary.is_file():
        return binary
    home.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=home.parent))
    try:
        source = staging / "src"
        for name in _READER_SOURCES:
            (source / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(READER / name, source / name)
        env = cargo_environment(
            toolchain,
            cargo_home=cache_root / "code" / "cargo-home",
            target_dir=staging / "target",
            **(registry_env or {}),
        )
        run(
            [str(toolchain.cargo), "build", "--release", "--locked", *registry_args],
            env,
            source,
        )
        built = staging / "out"
        built.mkdir()
        os.replace(staging / "target" / "release" / "reader", built / "reader")
        if not binary.is_file():
            os.replace(built, home)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return binary


def read_document(reader: Path, crate: Path, toolchain: str) -> dict[str, Any]:
    """What the reader prints of the crate: every public function at its root, with its tags
    and its types as trees (docs/arch/code-sources.md, section 2)."""
    root = library_root(crate, crate_manifest(crate, toolchain))
    out = run_cargo([str(reader), str(root.resolve())], {"PATH": ""}, crate)
    return json.loads(out)


def read_code(*, reader: Path, code: Path, entry: str, toolchain: str) -> Manifest:
    """The code's tables, commands and streams, read from the public functions of the crate at
    ``entry``; refused with every problem by name
    (:func:`provisa.codesource.manifest.read_manifest`)."""
    return read_manifest(read_document(reader, crate_dir(code, entry), toolchain), RUST_RULES)
