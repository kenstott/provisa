# Copyright (c) 2026 Kenneth Stott
# Canary: 820544bc-87d8-4b08-8739-b4e4f4fd40af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a Rust source is made ready to run (REQ-2227): its code installed -- its host compiled
once per code hash -- its functions read, and how its host starts
(provisa/codesource/languages.py). Its entry is the directory of the crate its tables are in, the
code's root when the source names none."""

# Requirements: REQ-2227
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from provisa.codesource.install import Lockfile
from provisa.codesource.source import CodeConfig, Context, Prepared, granted


def package_code(package: str, version: str) -> Any:  # a CrateCode
    from provisa.rustlang.install import CrateCode

    return CrateCode(package, version)


def prepare(
    source_id: str, config: CodeConfig, code: Any, lock: Lockfile | None, ctx: Context
) -> Prepared:
    from provisa.rustlang.install import ensure_installed, host_binary, registry_setup
    from provisa.rustlang.reader import ensure_reader, read_code
    from provisa.rustlang.source import host_command
    from provisa.rustlang.toolchain import ensure_toolchain, host_triple, static_target

    entry = config.entry or "."
    toolchain = ensure_toolchain(ctx.cache_root, [static_target(host_triple())])
    with tempfile.TemporaryDirectory(prefix="provisa-rust-reader-") as scratch:
        setup = registry_setup(ctx.registries, Path(scratch))
        reader = ensure_reader(toolchain, ctx.cache_root, setup.args, setup.env)
    installed = ensure_installed(
        source_id=source_id,
        code=code,
        entry=entry,
        expected_hash=config.content_hash,
        registries=ctx.registries,
        toolchain=toolchain,
        reader_binary=reader,
        cache_root=ctx.cache_root,
        lockfile=lock,
    )
    manifest = read_code(
        reader=reader, code=installed.path, entry=entry, toolchain=toolchain.version
    )
    command = host_command(binary=host_binary(installed.path), egress=ctx.egress)
    return Prepared(
        source_id,
        installed.content_hash,
        manifest,
        granted(command, source_id, config, ctx),
        ctx.org_id,
        workers=config.workers,
    )
