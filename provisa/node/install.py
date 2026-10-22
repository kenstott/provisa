# Copyright (c) 2026 Kenneth Stott
# Canary: 4e3afb01-9878-45d1-8248-0d602b17eca6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Node source's install (REQ-1984): its code fetched from the pinned reference, hashed, its
dependencies installed once with the runtime's frozen-lockfile mode and no install scripts, and
the result cached by content hash.

The source names its code in one of three ways:

- an npm package at an exact version, from the organisation's registry for its scope, else its
  mirror, else the public npm registry; the tarball is checked against the registry's integrity;
- a Git repository at a commit (a tag is resolved to its commit when the source is saved), with
  a subdirectory;
- an archive uploaded to Provisa.

The content hash is taken over the bundle's files BEFORE any install, and is what a source
records: a later install that finds other content is refused, naming both hashes.

Credentials come from the organisation's package registries only (core/package_registries). An
npm registry's credential is a token, sent as a bearer token and written as ``_authToken``, or
``user:password`` (an Artifactory user and its API key, say), sent as Basic and written as
``_auth`` (REQ-2088). A registry's certificate authority is trusted by the fetches through it and,
as ``cafile``, by the install. The
install's configuration (an ``.npmrc`` for every runtime, a Git configuration for Git
dependencies) is written into that install's own directory and removed after it, whether it
succeeded or not; the install runs with a HOME of its own and none of the process's environment
but its PATH, so no other credential reaches it. Nothing installed keeps a credential.
"""

# Requirements: REQ-1984, REQ-2088
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit

from provisa.codesource import install as shared
from provisa.codesource.bundle import BundleRefused, unpack
from provisa.codesource.install import (
    DirectoryCode,
    GitCode,
    Http,
    Installed,
    Lockfile,
    Runner,
    UploadCode,
    get,
    gitconfig,
    http_get,
    run_command,
)
from provisa.core.package_registries import NPM, Resolved

#: The registry an npm package comes from when the organisation names none for it.
NPM_PUBLIC = "https://registry.npmjs.org/"

#: The lockfiles each runtime installs from in its frozen mode.
LOCKFILES: dict[str, tuple[str, ...]] = {
    # npm reads npm-shrinkwrap.json first: with both present, it ignores package-lock.json.
    "node": ("npm-shrinkwrap.json", "package-lock.json"),
    "bun": ("bun.lock", "bun.lockb"),
    "deno": ("deno.lock",),
}

_EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")


@dataclass(frozen=True)
class NpmCode:
    package: str
    version: str


Code = NpmCode | GitCode | UploadCode | DirectoryCode


# -- registries ------------------------------------------------------------------------------------


def _slash(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def npm_registry_for(package: str, registries: Sequence[Resolved]) -> Resolved | None:
    """The organisation's registry ``package`` is installed from: the one for its scope, else the
    mirror; None for the public npm registry."""
    npm = [r for r in registries if r.kind == NPM]
    if package.startswith("@"):
        scope = package.split("/", 1)[0]
        for registry in npm:
            if registry.scope == scope:
                return registry
    for registry in npm:
        if registry.mirror:
            return registry
    return None


def _basic(credential: str) -> str | None:
    """``user:password`` as npm's ``_auth`` value (base64), or None for a bare token."""
    user, sep, password = credential.partition(":")
    return base64.b64encode(f"{user}:{password}".encode()).decode() if sep else None


def npm_authorization(credential: str) -> str:
    """The Authorization header a registry's credential is sent as: Basic for ``user:password``,
    else Bearer for a token."""
    basic = _basic(credential)
    return f"Basic {basic}" if basic is not None else f"Bearer {credential}"


def npmrc(registries: Sequence[Resolved], cafile: Path | None = None) -> str:
    """The ``.npmrc`` every runtime reads: the mirror as the default registry, each scope's
    registry, and each registry's credential for its own address -- ``_auth`` for
    ``user:password``, ``_authToken`` for a token -- and ``cafile``, the trusted authorities,
    when a registry carries its own."""
    lines: list[str] = [f"cafile={cafile}"] if cafile is not None else []
    for registry in registries:
        if registry.kind != NPM:
            continue
        address = _slash(registry.address)
        if registry.mirror:
            lines.append(f"registry={address}")
        if registry.scope:
            lines.append(f"{registry.scope}:registry={address}")
        parts = urlsplit(address)
        basic = _basic(registry.credential)
        if basic is not None:
            lines.append(f"//{parts.netloc}{parts.path}:_auth={basic}")
        else:
            lines.append(f"//{parts.netloc}{parts.path}:_authToken={registry.credential}")
    return "\n".join(lines) + "\n"


# -- fetching the code ------------------------------------------------------------------------------


def _integrity_ok(data: bytes, integrity: str) -> bool:
    """Whether ``data`` matches any of the Subresource Integrity values npm gives a tarball."""
    for token in integrity.split():
        algorithm, _, expected = token.partition("-")
        if algorithm in ("sha512", "sha384", "sha256"):
            if base64.b64encode(hashlib.new(algorithm, data).digest()).decode() == expected:
                return True
    return False


def fetch_npm(
    code: NpmCode,
    registries: Sequence[Resolved],
    http: Http,
    dest: Path,
    *,
    pinned_integrity: str | None = None,
) -> Path:
    """The package's files at its exact version, checked against the registry's integrity -- and,
    for a package Provisa itself pins, against ``pinned_integrity``, which the registry must
    give too: a mirror cannot substitute another tarball."""
    if not _EXACT_VERSION.match(code.version):
        raise BundleRefused(
            "version_not_exact",
            f"{code.package}@{code.version} is not an exact version",
            package=code.package,
            version=code.version,
        )
    registry = npm_registry_for(code.package, registries)
    base = _slash(registry.address) if registry else NPM_PUBLIC
    headers = {"Accept": "application/json"}
    if registry is not None:
        headers["Authorization"] = npm_authorization(registry.credential)
    ca_bundle = registry.ca_bundle if registry is not None else None
    meta = json.loads(get(http, base + quote(code.package, safe="@"), headers, ca_bundle))
    found = (meta.get("versions") or {}).get(code.version)
    if found is None:
        raise BundleRefused(
            "version_not_found",
            f"{code.package}@{code.version} is not in {base}",
            package=code.package,
            version=code.version,
        )
    dist = found.get("dist") or {}
    tarball, integrity = dist.get("tarball"), dist.get("integrity")
    if pinned_integrity is not None and integrity != pinned_integrity:
        raise BundleRefused(
            "integrity_mismatch",
            f"{base} gives {code.package}@{code.version} the integrity {integrity}, not the pinned "
            f"{pinned_integrity}",
            package=code.package,
        )
    if not tarball or not integrity:
        raise BundleRefused(
            "no_integrity",
            f"{base} gives {code.package}@{code.version} no tarball integrity to check",
            package=code.package,
        )
    # The credential goes only to the registry's own host.
    same_host = registry is not None and urlsplit(tarball).netloc == urlsplit(base).netloc
    auth = {"Authorization": headers["Authorization"]} if same_host else {}
    data = get(http, tarball, auth, ca_bundle)
    if not _integrity_ok(data, integrity):
        raise BundleRefused(
            "integrity_mismatch",
            f"{code.package}@{code.version} from {tarball} does not match the registry's integrity",
            package=code.package,
        )
    return unpack(f"{code.package.replace('/', '-')}-{code.version}.tgz", data, dest)


def fetch(code: Code, registries: Sequence[Resolved], http: Http, dest: Path) -> Path:
    if isinstance(code, NpmCode):
        return fetch_npm(code, registries, http, dest)
    return shared.fetch_kept(code, registries, dest)


# -- installing its dependencies --------------------------------------------------------------------


def needs_install(root: Path, runtime: str) -> bool:
    """Whether the bundle has dependencies to install. A bundle that declares none (a single
    pre-bundled file) needs no install; one that does must carry a lockfile its runtime reads."""
    manifest = root / "package.json"
    declared: dict = {}
    if manifest.is_file():
        package = json.loads(manifest.read_text())
        for field in ("dependencies", "optionalDependencies"):
            declared.update(package.get(field) or {})
    if not declared:
        return False
    if not any((root / name).is_file() for name in LOCKFILES[runtime]):
        raise BundleRefused(
            "no_lockfile",
            f"the bundle declares dependencies but carries no {' or '.join(LOCKFILES[runtime])} "
            f"for {runtime}; add one beside the code on the source's form",
            runtime=runtime,
        )
    return True


def install_command(runtime: str, executable: Path) -> list[str]:
    """The runtime's frozen-lockfile install, with no install scripts."""
    if runtime == "node":
        return [
            str(executable),
            str(_npm_cli(executable)),
            "ci",
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
        ]
    if runtime == "bun":
        return [str(executable), "install", "--frozen-lockfile", "--ignore-scripts"]
    if runtime == "deno":
        # Deno runs no lifecycle script without --allow-scripts.
        return [str(executable), "install", "--frozen"]
    raise BundleRefused("unknown_runtime", f"{runtime!r} is not a runtime", runtime=runtime)


def code_command(runtime: str, executable: Path | str) -> list[str]:
    """Node's or Bun's command for running code, which installs nothing: a source's packages are
    installed when it is fetched or built (:func:`install_command`). Bun is given --no-install, as
    it otherwise installs a package an import does not find -- the optional @opentelemetry/api the
    host looks for (host-core.mjs) included -- and a sandboxed run has nowhere to write it."""
    return [str(executable), "--no-install"] if runtime == "bun" else [str(executable)]


def _npm_cli(node: Path) -> Path:
    """npm as Node's own release lays it out beside the executable."""
    for candidate in (
        node.parent.parent / "lib" / "node_modules" / "npm" / "bin" / "npm-cli.js",
        node.parent / "node_modules" / "npm" / "bin" / "npm-cli.js",
    ):
        if candidate.is_file():
            return candidate
    raise BundleRefused("no_npm", f"no npm beside {node}", node=str(node))


def _install(
    root: Path, runtime: str, executable: Path, registries: Sequence[Resolved], run: Runner
) -> None:
    """Install ``root``'s dependencies with configuration written into an isolated directory of
    the install's own, removed when it ends."""
    with tempfile.TemporaryDirectory(prefix="provisa-node-install-") as scratch:
        home = Path(scratch)
        rc, git = home / ".npmrc", home / ".gitconfig"
        cafile = shared.install_trust(registries, home)
        rc.write_text(npmrc(registries, cafile))
        git.write_text(gitconfig(registries))
        project_rc = root / ".npmrc"
        had_rc = project_rc.exists()
        if not had_rc:
            # Bun and Deno read the project's .npmrc; it is the install's, and goes with it.
            project_rc.write_text(rc.read_text())
        env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(home),
            "USERPROFILE": str(home),
            "NPM_CONFIG_USERCONFIG": str(rc),
            "npm_config_cache": str(home / "npm-cache"),
            "BUN_INSTALL_CACHE_DIR": str(home / "bun-cache"),
            "GIT_CONFIG_GLOBAL": str(git),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            # Deno's own cache is kept with the install: the worker reads its npm packages there.
            "DENO_DIR": str(root / ".deno"),
            # Deno reads its trusted authorities from DENO_CERT, not from the .npmrc.
            **({"DENO_CERT": str(cafile)} if cafile is not None else {}),
            **shared.trust_environment(cafile),
        }
        try:
            run(install_command(runtime, executable), root, env)
        finally:
            if not had_rc:
                project_rc.unlink(missing_ok=True)


def ensure_installed(
    *,
    source_id: str,
    runtime: str,
    code: Code,
    expected_hash: str | None,
    registries: Sequence[Resolved],
    runtime_executable: Path,
    cache_root: Path,
    lockfile: Lockfile | None = None,
    http: Http = http_get,
    run: Runner = run_command,
) -> Installed:
    """The source's bundle, installed and cached by content hash
    (:func:`provisa.codesource.install.ensure_installed`), with ``lockfile`` -- given beside the
    code -- put into it first."""
    if runtime not in LOCKFILES:
        raise BundleRefused("unknown_runtime", f"{runtime!r} is not a runtime", runtime=runtime)

    def _dependencies(root: Path) -> None:
        if needs_install(root, runtime):
            _install(root, runtime, runtime_executable, registries, run)

    return shared.ensure_installed(
        source_id=source_id,
        runtime=runtime,
        fetch=lambda dest: fetch(code, registries, http, dest),
        install=_dependencies,
        expected_hash=expected_hash,
        cache_root=cache_root,
        lockfile=lockfile,
        lockfiles=LOCKFILES[runtime],
    )
