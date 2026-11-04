# Copyright (c) 2026 Kenneth Stott
# Canary: a3f7c2e9-61d4-4b58-8e05-2c9b7d4f1a86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a Python source is made ready to run (REQ-1993): its code installed, its functions read,
and how its host starts (provisa/codesource/languages.py)."""

# Requirements: REQ-1993
from __future__ import annotations

from typing import Any

from provisa.codesource.install import Lockfile
from provisa.codesource.source import CodeConfig, Context, Prepared, granted


def package_code(package: str, version: str) -> Any:  # a PypiCode
    from provisa.python.install import PypiCode

    return PypiCode(package, version)


def prepare(
    source_id: str, config: CodeConfig, code: Any, lock: Lockfile | None, ctx: Context
) -> Prepared:
    from provisa.python.install import ensure_installed, environment_python
    from provisa.python.source import code_path, entry_module, host_command, read_code
    from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

    resolver = CodeRuntimeResolver(cache_root=ctx.cache_root)
    python, uv = resolver.resolve("python"), resolver.resolve("uv")
    installed = ensure_installed(
        source_id=source_id,
        code=code,
        expected_hash=config.content_hash,
        registries=ctx.registries,
        python=python,
        uv=uv,
        cache_root=ctx.cache_root,
        lockfile=lock,
    )
    root = code_path(installed.path)
    entry = entry_module(root, config.entry)
    interpreter = environment_python(installed.path, python)
    home = python.parent.parent
    manifest = read_code(
        python=interpreter, python_home=home, code=root, entry=entry, egress=ctx.egress
    )
    # The host's pyarrow, pinned and fetched as the runtime is (REQ-2098): <home>/pyarrow/.
    host_lib = resolver.resolve("pyarrow").parent.parent
    command = host_command(
        python=interpreter,
        python_home=home,
        code=root,
        entry=entry,
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
