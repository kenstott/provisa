# Copyright (c) 2026 Kenneth Stott
# Canary: 9b7c4e21-5d38-4f6a-8e0b-3a1f6c2d7e95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust source's part of its image (REQ-2098, REQ-2227): its host, compiled by the pinned
toolchain for the image's machine (musl, linked self-contained by rust-lld) with the code linked
in, one static binary. The crates the build needs are fetched through the organisation's Cargo
registry and checked against the lock, as for an install on the server; nothing else of the code,
and no runtime, goes into the image. The front end runs on the Python every image without one of
its own carries."""

# Requirements: REQ-2098, REQ-2227
from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from provisa.codesource import oci
from provisa.codesource.image_build import ROOT, ImageParts, Plan
from provisa.codesource.install import Http, Runner
from provisa.rustlang import install
from provisa.rustlang.install import LOCKFILES, install_host, registry_setup
from provisa.rustlang.reader import ensure_reader
from provisa.rustlang.toolchain import ensure_toolchain, static_target
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

__all__ = ["LOCKFILES", "fetch", "layers"]


def fetch(code: Any, registries: Sequence[Any], http: Http, dest: Path) -> Path:
    """The code, fetched by the language's install (looked up when called)."""
    return install.fetch(code, registries, http, dest)


#: Where the host is in the image.
HOST = f"{ROOT}/host/host"


def layers(
    plan: Plan, root: Path, target: CodeRuntimeResolver, arch: str, workdir: Path, run: Runner
) -> ImageParts:
    machine = static_target("", arch)
    toolchain = ensure_toolchain(plan.cache_root, [machine])
    binary = workdir / "rust-host" / "host"
    with tempfile.TemporaryDirectory(prefix="provisa-image-rust-") as scratch:
        setup = registry_setup(plan.registries, Path(scratch))
        reader = ensure_reader(toolchain, plan.cache_root, setup.args, setup.env)
    install_host(
        root,
        toolchain=toolchain,
        reader_binary=reader,
        entry=plan.entry or ".",
        registries=plan.registries,
        cargo_home=plan.cache_root / "code" / "cargo-home",
        dest=binary,
        target=machine,
    )
    host = [
        oci.Entry(HOST, source=binary, mode=0o755),
        # The image's working directory: the code is linked into the host, so nothing is here.
        oci.Entry(f"{ROOT}/code", directory=True, mode=0o755),
    ]
    return ImageParts([host], [HOST, "{fd}"], {})
