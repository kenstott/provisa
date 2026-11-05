# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2b8d14-9c3f-4a71-b0e6-8f4d2c7a1b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a Node source is made ready to run (REQ-1984): its bundle installed, its declarations read,
and how its host starts (provisa/codesource/languages.py)."""

# Requirements: REQ-1984
from __future__ import annotations

from typing import Any

from provisa.codesource.install import Lockfile
from provisa.codesource.source import CodeConfig, Context, Prepared, granted


def package_code(package: str, version: str) -> Any:  # an NpmCode
    from provisa.node.install import NpmCode

    return NpmCode(package, version)


def prepare(
    source_id: str, config: CodeConfig, code: Any, lock: Lockfile | None, ctx: Context
) -> Prepared:
    from provisa.node.install import ensure_installed
    from provisa.node.reader import ensure_typescript, read_bundle
    from provisa.node.worker import host_command
    from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

    resolver = CodeRuntimeResolver(cache_root=ctx.cache_root)
    executable = resolver.resolve(config.runtime)
    installed = ensure_installed(
        source_id=source_id,
        runtime=config.runtime,
        code=code,
        expected_hash=config.content_hash,
        registries=ctx.registries,
        runtime_executable=executable,
        cache_root=ctx.cache_root,
        lockfile=lock,
    )
    manifest = read_bundle(
        runtime=config.runtime,
        executable=executable,
        compiler=ensure_typescript(ctx.cache_root, ctx.registries),
        bundle=installed.path,
    )
    # The host's Arrow library, pinned and fetched as the runtime is (REQ-2098): <home>/package/.
    host_lib = resolver.resolve("apache-arrow").parent.parent
    command = host_command(
        runtime=config.runtime,
        executable=executable,
        bundle=installed.path,
        egress=ctx.egress,
        host_lib=host_lib,
    )
    return Prepared(
        source_id,
        installed.content_hash,
        manifest,
        granted(command, source_id, config, ctx),
        ctx.org_id,
        workers=config.workers,
    )
