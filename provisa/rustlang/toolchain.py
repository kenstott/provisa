# Copyright (c) 2026 Kenneth Stott
# Canary: 01af3a08-ada2-4f3e-9461-bc5f87309465
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pinned Rust toolchain a Rust source is compiled with (REQ-2227).

The toolchain comes as the components static.rust-lang.org publishes, each pinned and checked by
:mod:`provisa.runtime_deps.code_runtimes`: the compiler, cargo, and a standard library per target.
rustc finds the standard library in its sysroot -- the directory its own library is in -- so the
compiler's component and each standard library are joined into one sysroot, hard links of the
extracted files (nothing is copied), cached by the version and the targets it holds.

A host is one static binary. On Linux it is built for musl, linked self-contained by the
compiler's own rust-lld, so it needs no C library and runs on the image's distroless base. On
macOS it is built for the Mac itself (which has no static binaries; the host links the system's
library only, as a Go host does). Build scripts and procedural macros run on the machine doing
the build and are linked by its C compiler (``cc``), as every Rust build's are.
"""

# Requirements: REQ-2227
from __future__ import annotations

import os
import platform
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from provisa.codesource.bundle import BundleRefused
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

#: The target triple of each platform a build runs on.
HOSTS = {
    ("Darwin", "arm64"): "aarch64-apple-darwin",
    ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
    ("Linux", "aarch64"): "aarch64-unknown-linux-gnu",
}
#: The target a Linux host binary is built for, by machine (an image's GOARCH-style name too).
MUSL = {
    "x86_64": "x86_64-unknown-linux-musl",
    "amd64": "x86_64-unknown-linux-musl",
    "aarch64": "aarch64-unknown-linux-musl",
    "arm64": "aarch64-unknown-linux-musl",
}


@dataclass(frozen=True)
class Toolchain:
    cargo: Path
    rustc: Path
    #: The sysroot rustc runs from: its own component, with each target's standard library.
    sysroot: Path
    #: The triple of the machine the build runs on.
    host: str
    version: str

    def bin_dirs(self) -> list[Path]:
        return [self.cargo.parent, self.rustc.parent]


def host_triple(system: str | None = None, machine: str | None = None) -> str:
    key = (system or platform.system(), machine or platform.machine())
    found = HOSTS.get(key)
    if found is None:
        raise BundleRefused(
            "rust_platform", f"no Rust toolchain is pinned for {key[0]}/{key[1]}", platform=key[0]
        )
    return found


def static_target(host: str, machine: str | None = None) -> str:
    """The target a host binary is built for: musl of ``machine`` (the server's when None) on
    Linux, and the Mac's own triple on macOS."""
    if machine is None and host.endswith("-apple-darwin"):
        return host
    arch = machine or host.split("-", 1)[0]
    found = MUSL.get(arch)
    if found is None:
        raise BundleRefused("rust_platform", f"no Linux target is pinned for {arch}", platform=arch)
    return found


def _std_pin(target: str, host: str) -> str:
    """The pin of ``target``'s standard library: the host's own, or a target's of its own."""
    return "rust-std" if target == host else f"rust-std-{target}"


def _component(marker: Path, levels: int) -> Path:
    """The component directory a pinned file is in (its ``levels``-th parent)."""
    return marker.parents[levels - 1]


def _link_tree(source: Path, dest: Path) -> None:
    shutil.copytree(source, dest, symlinks=True, copy_function=os.link, dirs_exist_ok=True)


def ensure_toolchain(cache_root: Path, targets: Sequence[str]) -> Toolchain:
    """The toolchain, its sysroot holding the host's standard library and each of ``targets``',
    resolved and joined on a miss."""
    resolver = CodeRuntimeResolver(cache_root=cache_root)
    host = host_triple()
    version = RUNTIMES["rustc"].version
    rustc_component = _component(resolver.resolve("rustc"), 2)  # <component>/bin/rustc
    cargo = resolver.resolve("cargo")
    wanted = sorted({host, *targets})
    stds: dict[str, Path] = {}
    for target in wanted:
        # <archive>/rust-std-<target>/manifest.in
        stds[target] = (
            _component(resolver.resolve(_std_pin(target, host)), 1) / "lib" / "rustlib" / target
        )
    # Never named a bare triple: rustc takes ``<triple>/lib`` for its rustlib layout and looks for
    # its sysroot above it.
    home = cache_root / "code" / "rust-toolchain" / version / f"sysroot-{'+'.join(wanted)}"
    rustc = home / "bin" / "rustc"
    if not rustc.is_file():
        home.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=home.parent))
        staging.chmod(0o755)
        try:
            _link_tree(rustc_component, staging)
            for target, std in stds.items():
                _link_tree(std, staging / "lib" / "rustlib" / target)
            if not rustc.is_file():
                try:
                    os.replace(staging, home)
                except OSError:
                    if not rustc.is_file():  # another join moved its own into place meanwhile
                        raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)
    return Toolchain(cargo=cargo, rustc=rustc, sysroot=home, host=host, version=version)
