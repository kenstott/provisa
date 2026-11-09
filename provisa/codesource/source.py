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

A code source (a source type of :data:`provisa.codesource.languages.LANGUAGES`) keeps its
configuration in its ``mapping`` (the source model's type-specific slot):

    runtime       one of its language's runtimes (node | bun | deno; python; go)
    code          {"kind": <its language's package kind: npm | pypi | gomod>, "package", "version"}
                  {"kind": "git", "repository", "commit", "subdirectory"}
                  {"kind": "upload", "upload": <content hash of the uploaded archive>,
                   "filename": <the name of the file it was uploaded from, when known>}
                  {"kind": "directory", "path": <a directory on this server>} (REQ-2121): read
                   where it is, under the operator's allowed directories, single-tenant only
    lockfile      {"filename", "upload": <SHA-256 of its bytes>}, given beside code that carries none
    entry         where the code is entered, for a language whose source names it: a python
                  source's entry module, a go source's package directory
    content_hash  the code's content hash, recorded when the source was saved

:func:`prepare` makes a source ready to run: its code installed (cached by content hash), its
manifest read, and how its worker starts. It is done once per source and content hash in a
process, at its first use; the worker itself is the shared pool's.
"""

# Requirements: REQ-1984, REQ-1993, REQ-2104, REQ-2200
from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.codesource import remote_sources, services
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.bundle import content_hash as hash_of
from provisa.codesource.install import DIRECTORY_SKIPPED, DirectoryCode, GitCode, Lockfile
from provisa.codesource.languages import CODE_SOURCE_TYPES, LANGUAGES
from provisa.codesource.manifest import Manifest
from provisa.codesource.worker import HostCommand, Worker, pool

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.core.package_registries import Resolved

__all__ = ["CODE_SOURCE_TYPES", "CodeConfig", "Context", "Prepared", "prepare"]

#: The references every language's code may be, beside its own package kind, and their fields.
_SHARED_KINDS = ("git", "upload", "directory")
_CODE_FIELDS = {
    "git": ("repository", "commit"),
    "upload": ("upload",),
    "directory": ("path",),
}
_PACKAGE_FIELDS = ("package", "version")
#: REQ-2121: a code directory is read where it is, never pinned to a content hash.
DIRECTORY = "directory"


@dataclass(frozen=True)
class CodeConfig:
    """A code source's ``mapping``, read and checked."""

    language: str  # a source type of LANGUAGES
    runtime: str
    code: dict[str, str]
    lockfile: dict[str, str] | None
    entry: str | None
    content_hash: str | None
    #: REQ-2104: the names the code may read, each its vault reference, as written.
    secrets: dict[str, str | None] = field(default_factory=dict)
    #: REQ-2105: the monthly cap on what its LLM calls cost, USD; None: no cap.
    llm_cap_usd: float | None = None
    #: REQ-2098: dev (its code on this server) or built (its image on a remote host).
    mode: str = "dev"
    #: REQ-2098: a built source's remote host: host, port, pin, exports and kept manifest.
    remote: dict | None = None
    #: REQ-2106: its non-secret settings, which its code reads through Provisa's services.
    settings: dict[str, Any] = field(default_factory=dict)
    #: REQ-2131: how many workers it runs in each Provisa process; a @columns transform's batches
    #: spread across them.
    workers: int = 1


def _workers(mapping: dict) -> int:
    # REQ-2131: one worker unless the source names more -- the form's default, written or not.
    given = mapping.get("workers", 1)
    if isinstance(given, bool) or not isinstance(given, int) or given < 1:
        raise BundleRefused(
            "workers", f"a code source runs one or more workers, not {given!r}", workers=str(given)
        )
    return given


def config_of(source_type: str, mapping: dict) -> CodeConfig:
    """The source's configuration, refused by name when it cannot be used."""
    language = LANGUAGES.get(source_type)
    if language is None:
        raise BundleRefused("not_code", f"{source_type!r} is not a code source type")
    allowed = language.runtimes
    runtime = mapping.get("runtime") or (allowed[0] if len(allowed) == 1 else "")
    if runtime not in allowed:
        raise BundleRefused(
            "unknown_runtime",
            f"a {source_type} source runs on {', '.join(allowed)}, not {runtime!r}",
            runtime=str(runtime),
        )
    code = dict(mapping.get("code") or {})
    kinds = (language.package_kind, *_SHARED_KINDS)
    if code.get("kind") not in kinds:
        raise BundleRefused(
            "code_kind",
            f"a {source_type} source's code is {', '.join(kinds)}, not {code.get('kind')!r}",
        )
    fields = (
        _PACKAGE_FIELDS if code["kind"] == language.package_kind else _CODE_FIELDS[code["kind"]]
    )
    missing = [k for k in fields if not code.get(k)]
    if missing:
        raise BundleRefused(
            "code_incomplete",
            f"the source's code needs {', '.join(missing)}",
            missing=", ".join(missing),
        )
    if code["kind"] == DIRECTORY:
        code["path"] = str(checked_directory(code["path"]))
    lockfile = mapping.get("lockfile") or None
    return CodeConfig(
        language=source_type,
        runtime=runtime,
        code=code,
        lockfile=lockfile,
        entry=mapping.get("entry") or None,
        # REQ-2121: a directory is read as it is now, never held to the hash it had.
        content_hash=None if code["kind"] == DIRECTORY else mapping.get("content_hash") or None,
        secrets=services.declared(mapping.get("secrets")),
        llm_cap_usd=services.llm_cap(mapping.get("llm")),
        mode=remote_sources.mode_of(mapping),
        remote=remote_sources.remote_of(mapping)
        if remote_sources.mode_of(mapping) == remote_sources.BUILT
        else None,
        settings=services.declared_settings(mapping.get("settings")),
        workers=_workers(mapping),
    )


def checked_directory(path: str) -> Path:
    """A code directory on this server (REQ-2121): a directory under the operator's allowed
    directories (``sources.download_roots``, as a file source's mount path is, REQ-2113) once its
    symlinks are followed; refused by name otherwise, and always on a server of many
    organizations, whose disk is the platform's."""
    import os

    from provisa.core.secrets_providers import many_organisations
    from provisa.core.source_downloads import download_roots

    if many_organisations():
        raise BundleRefused(
            "directory_refused",
            f"{path}: a server of many organizations runs no code from its own disk",
            path=path,
        )
    expanded = os.path.expanduser(path)
    if not os.path.isabs(expanded):
        raise BundleRefused(
            "directory_relative", f"{path}: name the directory by its full path", path=path
        )
    real = os.path.realpath(expanded)
    roots = download_roots()
    if not any(real == r or real.startswith(r.rstrip(os.sep) + os.sep) for r in roots):
        raise BundleRefused(
            "directory_outside_roots",
            f"{path} resolves to {real}, outside the directories code may be read from: "
            f"{', '.join(roots) or 'none are listed'}",
            path=path,
            roots=", ".join(roots),
        )
    if not os.path.isdir(real):
        raise BundleRefused("directory_missing", f"{path}: there is no directory there", path=path)
    return Path(real)


@dataclass(frozen=True)
class Prepared:
    """A source ready to run: its install, what it offers, and how its worker starts."""

    source_id: str
    content_hash: str
    manifest: Manifest
    command: HostCommand
    #: The org it was prepared for: its worker carries that org's services grant (REQ-2104), so
    #: no other org's calls reach it.
    org_id: str
    #: REQ-2098: a built source's remote host, and the identity Provisa presents to it.
    remote: dict | None = None
    identity: Any = None
    #: REQ-2131: how many workers it runs in this process (``CodeConfig.workers``).
    workers: int = 1
    #: REQ-2221: what gives its remote host its setup values; None when its code has no @on-start.
    setup: Callable[[], dict[str, Any]] | None = None

    def worker(self, slot: int = 0) -> Worker:
        """Its worker number ``slot`` of :attr:`workers` (REQ-2131); one call that needs one
        worker takes the first."""
        if not 0 <= slot < self.workers:
            raise ValueError(
                f"source {self.source_id!r} runs {self.workers} workers, not {slot + 1}"
            )
        remote = self.remote
        if remote is not None:
            return pool.remote(
                self.source_id,
                self.content_hash,
                self.org_id,
                lambda: remote_sources.worker_for(
                    self.org_id,
                    self.source_id,
                    self.content_hash,
                    remote,
                    self.identity,
                    setup=self.setup,
                ),
                slot=slot,
            )
        return pool.get(
            self.source_id, self.content_hash, lambda: self.command, org=self.org_id, slot=slot
        )


@dataclass(frozen=True)
class Context:
    """What preparing a source needs from the process: where caches live, the organisation's
    package registries (resolved), its uploads, and the egress allow-list."""

    cache_root: Path
    registries: list["Resolved"]
    egress: list[str]
    admin_db: "Database"
    org_id: str


_prepared: dict[tuple[str, str, str], Prepared] = {}  # org, source, content hash
_lock = threading.Lock()


async def _code_reference(config: CodeConfig, ctx: Context) -> Any:  # the language's Code union
    from provisa.codesource import uploads

    code = config.code
    language = LANGUAGES[config.language]
    if code["kind"] == language.package_kind:
        return language.package_code(code["package"], code["version"])
    if code["kind"] == "git":
        return GitCode(code["repository"], code["commit"], code.get("subdirectory", ""))
    if code["kind"] == DIRECTORY:
        return DirectoryCode(code["path"])
    return await uploads.read(ctx.admin_db, ctx.org_id, code["upload"])


async def _lockfile(config: CodeConfig, ctx: Context) -> Lockfile | None:
    from provisa.codesource import uploads

    if config.lockfile is None:
        return None
    kept = await uploads.read_file(ctx.admin_db, ctx.org_id, config.lockfile["upload"])
    return Lockfile(config.lockfile["filename"], kept)


def granted(command: HostCommand, source_id: str, config: CodeConfig, ctx: Context) -> HostCommand:
    """``command`` with the source's services grant: its org and declared secrets (REQ-2104)."""
    grant = services.ServiceGrant(
        org_id=ctx.org_id,
        source_id=source_id,
        secrets=config.secrets,
        admin_db=ctx.admin_db,
        llm_cap_usd=config.llm_cap_usd,
        settings=config.settings,
        egress=tuple(ctx.egress),
    )
    return replace(command, services=grant)


async def prepare(source_id: str, source_type: str, mapping: dict, ctx: Context) -> Prepared:
    """The source made ready: installed (from its cache when it is there), read, and how its
    worker starts. Kept for the process by source and recorded content hash; a source being
    saved (no hash recorded yet) is prepared afresh, and the hash it reports is the one to
    record."""
    config = config_of(source_type, mapping)
    if config.code["kind"] == DIRECTORY:
        # REQ-2121: what is in the directory now; prepared afresh once it has changed.
        found = await asyncio.to_thread(hash_of, Path(config.code["path"]), DIRECTORY_SKIPPED)
        with _lock:
            held = _prepared.get((ctx.org_id, source_id, found))
        if held is not None:
            return held
    elif config.content_hash is not None:
        with _lock:
            held = _prepared.get((ctx.org_id, source_id, config.content_hash))
        if held is not None:
            return held
    if config.remote is not None:
        # REQ-2098: built -- nothing installed or read here; the manifest is the one kept.
        from provisa.codesource.lifecycle import setup_values
        from provisa.codesource.manifest import manifest_from_json

        if config.content_hash is None:
            raise BundleRefused("remote_unpinned", "a built source is pinned to its code's hash")
        kept = manifest_from_json(config.remote["manifest"])
        ready = Prepared(
            source_id,
            config.content_hash,
            kept,
            None,  # pyright: ignore[reportArgumentType] -- no local host command
            ctx.org_id,
            remote=config.remote,
            identity=await remote_sources.identity(ctx.org_id),
            workers=config.workers,
            setup=setup_values(ctx.org_id, source_id, kept, mapping, ctx.admin_db),
        )
        with _lock:
            _prepared[(ctx.org_id, source_id, config.content_hash)] = ready
        return ready
    code = await _code_reference(config, ctx)
    lock = await _lockfile(config, ctx)
    build = LANGUAGES[config.language].prepare()
    # Fetching, installing and reading block: off the event loop.
    ready = await asyncio.to_thread(build, source_id, config, code, lock, ctx)
    from provisa.codesource.lifecycle import refuse_in_dev

    refuse_in_dev(source_id, ready.manifest)  # REQ-2221: only a connector's host runs them
    with _lock:
        _prepared[(ctx.org_id, source_id, ready.content_hash)] = ready
    return ready


def forget(source_id: str) -> None:
    """The source was edited or deleted: what was prepared for it, and its workers, go."""
    with _lock:
        for key in [k for k in _prepared if k[1] == source_id]:
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
