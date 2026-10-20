# Copyright (c) 2026 Kenneth Stott
# Canary: c681b502-4552-467d-8573-f922f15bb099
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source as it is registered and run (REQ-1984, REQ-1993).

A ``node`` or ``python`` source keeps its configuration in its ``mapping`` (the source model's
type-specific slot):

    runtime       node | bun | deno for a node source; python for a python source
    code          {"kind": "npm" | "pypi", "package", "version"}
                  {"kind": "git", "repository", "commit", "subdirectory"}
                  {"kind": "upload", "upload": <content hash of the uploaded archive>}
    lockfile      {"filename", "upload": <SHA-256 of its bytes>}, given beside a package reference
    entry         a python source's entry module, when its code has more than one
    content_hash  the code's content hash, recorded when the source was saved

:func:`prepare` makes a source ready to run: its code installed (cached by content hash), its
manifest read, and how its worker starts. It is done once per source and content hash in a
process, at its first use; the worker itself is the shared pool's.
"""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import GitCode, Lockfile
from provisa.codesource.manifest import Manifest
from provisa.codesource.worker import HostCommand, Worker, pool

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.core.package_registries import Resolved

CODE_SOURCE_TYPES = frozenset({"node", "python"})
NODE_RUNTIMES = ("node", "bun", "deno")

_CODE_KINDS = {"node": ("npm", "git", "upload"), "python": ("pypi", "git", "upload")}
_CODE_FIELDS = {
    "npm": ("package", "version"),
    "pypi": ("package", "version"),
    "git": ("repository", "commit"),
    "upload": ("upload",),
}


@dataclass(frozen=True)
class CodeConfig:
    """A code source's ``mapping``, read and checked."""

    language: str  # node | python
    runtime: str
    code: dict[str, str]
    lockfile: dict[str, str] | None
    entry: str | None
    content_hash: str | None


def config_of(source_type: str, mapping: dict) -> CodeConfig:
    """The source's configuration, refused by name when it cannot be used."""
    if source_type not in CODE_SOURCE_TYPES:
        raise BundleRefused("not_code", f"{source_type!r} is not a code source type")
    runtime = mapping.get("runtime") or ("python" if source_type == "python" else "")
    allowed = NODE_RUNTIMES if source_type == "node" else ("python",)
    if runtime not in allowed:
        raise BundleRefused(
            "unknown_runtime",
            f"a {source_type} source runs on {', '.join(allowed)}, not {runtime!r}",
            runtime=str(runtime),
        )
    code = dict(mapping.get("code") or {})
    kinds = _CODE_KINDS[source_type]
    if code.get("kind") not in kinds:
        raise BundleRefused(
            "code_kind",
            f"a {source_type} source's code is {', '.join(kinds)}, not {code.get('kind')!r}",
        )
    missing = [k for k in _CODE_FIELDS[code["kind"]] if not code.get(k)]
    if missing:
        raise BundleRefused(
            "code_incomplete",
            f"the source's code needs {', '.join(missing)}",
            missing=", ".join(missing),
        )
    lockfile = mapping.get("lockfile") or None
    if lockfile is not None and code["kind"] not in ("npm", "pypi"):
        raise BundleRefused(
            "lockfile_beside_kept_code",
            "a lockfile is given beside a package reference; Git or uploaded code carries its own",
        )
    return CodeConfig(
        language=source_type,
        runtime=runtime,
        code=code,
        lockfile=lockfile,
        entry=mapping.get("entry") or None,
        content_hash=mapping.get("content_hash") or None,
    )


@dataclass(frozen=True)
class Prepared:
    """A source ready to run: its install, what it offers, and how its worker starts."""

    source_id: str
    content_hash: str
    manifest: Manifest
    command: HostCommand

    def worker(self) -> Worker:
        return pool.get(self.source_id, self.content_hash, lambda: self.command)


@dataclass(frozen=True)
class Context:
    """What preparing a source needs from the process: where caches live, the organisation's
    package registries (resolved), its uploads, and the egress allow-list."""

    cache_root: Path
    registries: list["Resolved"]
    egress: list[str]
    admin_db: "Database"
    org_id: str


_prepared: dict[tuple[str, str], Prepared] = {}
_lock = threading.Lock()


async def _code_reference(config: CodeConfig, ctx: Context) -> Any:  # the language's Code union
    from provisa.codesource import uploads
    from provisa.node.install import NpmCode
    from provisa.python.install import PypiCode

    code = config.code
    if code["kind"] == "npm":
        return NpmCode(code["package"], code["version"])
    if code["kind"] == "pypi":
        return PypiCode(code["package"], code["version"])
    if code["kind"] == "git":
        return GitCode(code["repository"], code["commit"], code.get("subdirectory", ""))
    return await uploads.read(ctx.admin_db, ctx.org_id, code["upload"])


async def _lockfile(config: CodeConfig, ctx: Context) -> Lockfile | None:
    from provisa.codesource import uploads

    if config.lockfile is None:
        return None
    kept = await uploads.read_file(ctx.admin_db, ctx.org_id, config.lockfile["upload"])
    return Lockfile(config.lockfile["filename"], kept)


def _prepare_node(
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
    return Prepared(source_id, installed.content_hash, manifest, command)


def _prepare_python(
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
    return Prepared(source_id, installed.content_hash, manifest, command)


async def prepare(source_id: str, source_type: str, mapping: dict, ctx: Context) -> Prepared:
    """The source made ready: installed (from its cache when it is there), read, and how its
    worker starts. Kept for the process by source and recorded content hash; a source being
    saved (no hash recorded yet) is prepared afresh, and the hash it reports is the one to
    record."""
    config = config_of(source_type, mapping)
    if config.content_hash is not None:
        with _lock:
            held = _prepared.get((source_id, config.content_hash))
        if held is not None:
            return held
    code = await _code_reference(config, ctx)
    lock = await _lockfile(config, ctx)
    build = _prepare_node if config.language == "node" else _prepare_python
    # Fetching, installing and reading block: off the event loop.
    ready = await asyncio.to_thread(build, source_id, config, code, lock, ctx)
    with _lock:
        _prepared[(source_id, ready.content_hash)] = ready
    return ready


def forget(source_id: str) -> None:
    """The source was edited or deleted: what was prepared for it, and its workers, go."""
    with _lock:
        for key in [k for k in _prepared if k[0] == source_id]:
            del _prepared[key]
    pool.stop_source(source_id)


async def context_for(state: Any) -> Context:  # the app state
    """The process's context for preparing a source of the organisation bound to the request."""
    from provisa.core.package_registries import resolved
    from provisa.core.request_context import require_current_org
    from provisa.runtime_deps.pgwire_bundles import default_cache_root

    org_id = require_current_org()
    return Context(
        cache_root=default_cache_root(),
        registries=await resolved(state.admin_db, org_id),
        egress=list(state.udf_egress_allowlist),
        admin_db=state.admin_db,
        org_id=org_id,
    )
