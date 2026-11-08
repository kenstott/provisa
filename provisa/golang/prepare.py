# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6a2c8d-71e3-4b95-a4d0-9e2b5f7c3a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a Go source is made ready to run (REQ-2200): its code installed -- its modules fetched and
its host compiled once per code hash -- its functions read, and how its host starts
(provisa/codesource/languages.py). Its entry is the package directory of the module its tables
are in, the module's root when the source names none."""

# Requirements: REQ-2200
from __future__ import annotations

from typing import Any

from provisa.codesource.install import Lockfile
from provisa.codesource.source import CodeConfig, Context, Prepared, granted


def package_code(package: str, version: str) -> Any:  # a GoModuleCode
    from provisa.golang.install import GoModuleCode

    return GoModuleCode(package, version)


def prepare(
    source_id: str, config: CodeConfig, code: Any, lock: Lockfile | None, ctx: Context
) -> Prepared:
    import tempfile
    from pathlib import Path

    from provisa.golang.install import MODULES, ensure_installed, host_binary, proxy_address
    from provisa.golang.reader import ensure_reader, read_code
    from provisa.golang.source import host_command
    from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

    go = CodeRuntimeResolver(cache_root=ctx.cache_root).resolve("go")
    package = config.entry or "."
    reader = ensure_reader(
        go, RUNTIMES["go"].version, ctx.cache_root, proxy_address(ctx.registries)
    )
    installed = ensure_installed(
        source_id=source_id,
        code=code,
        package=package,
        expected_hash=config.content_hash,
        registries=ctx.registries,
        go=go,
        reader_binary=reader,
        cache_root=ctx.cache_root,
        lockfile=lock,
    )
    with tempfile.TemporaryDirectory(prefix="provisa-go-read-") as scratch:
        manifest = read_code(
            reader=reader,
            go=go,
            code=installed.path,
            package=package,
            gomodcache=installed.path / MODULES,
            scratch=Path(scratch),
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
