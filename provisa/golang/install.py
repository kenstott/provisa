# Copyright (c) 2026 Kenneth Stott
# Canary: c9e41b7a-2d05-4f68-a3b1-6e8f0d2c5a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fetching a Go source's code and installing it (REQ-2200).

A Go source names its code as one of:

- a Go module at an exact version (``v1.2.3``), fetched as its zip from the organisation's
  default Go module proxy (the ``goproxy`` package registry marked default), else the public
  proxy;
- a Git repository at a commit, with a subdirectory;
- an archive uploaded to Provisa, or a directory on this server.

The code is a module: its ``go.mod`` names it, and one that requires other modules carries its
``go.sum`` (or one is given beside it on the source's form). The install fetches every module the
code's package needs through the same proxy into the install's own module cache, each checked
against ``go.sum`` -- a module it does not cover is refused by the go command, by name; nothing is
looked up in a checksum database and no ``go.sum`` is changed. Then the code is read
(:mod:`provisa.golang.reader`) and its host compiled with the code linked in
(:mod:`provisa.golang.build`), its own dependencies fetched through the proxy too and checked
against its committed ``go.sum``. The install -- code, modules and host -- is cached by the code's
content hash like every language's.

A proxy's credential reaches the go command in a ``.netrc`` in the install's own temporary
directory (the go command's own way of authenticating to a proxy), never in the install; a
proxy's certificate authority (REQ-2088) through SSL_CERT_FILE, likewise.
"""

# Requirements: REQ-2200, REQ-2088
from __future__ import annotations

import base64
import re
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from provisa.codesource import install as shared
from provisa.codesource.bundle import BundleRefused, unpack
from provisa.codesource.install import (
    DirectoryCode,
    GitCode,
    Http,
    Installed,
    Lockfile,
    UploadCode,
    get,
    http_get,
)
from provisa.codesource.manifest import Manifest
from provisa.core.package_registries import GOPROXY, Resolved
from provisa.golang import build, reader
from provisa.golang.reader import GoRunner, check_go_line, go_environment, run_go

#: The proxy a module comes from when the organisation names no default Go module proxy.
PUBLIC_PROXY = "https://proxy.golang.org"
LOCKFILES = ("go.sum",)
#: Where the install keeps the modules the code needs, and its compiled host.
MODULES = ".gomod"
HOST_DIR = ".provisa-host"
_VERSION = re.compile(r"^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?$")
_REQUIRE = re.compile(r"^\s*require\b", re.MULTILINE)


@dataclass(frozen=True)
class GoModuleCode:
    module: str
    #: An exact version: a module proxy is never asked for "latest".
    version: str


Code = GoModuleCode | GitCode | UploadCode | DirectoryCode


def default_proxy(registries: Sequence[Resolved]) -> Resolved | None:
    """The organisation's default Go module proxy; None for the public one."""
    return next((r for r in registries if r.kind == GOPROXY and r.mirror), None)


def proxy_address(registries: Sequence[Resolved]) -> str:
    """The GOPROXY every Go install of the organisation goes through: its default proxy, else the
    public one. Never ``direct``: no module is fetched from its version control."""
    found = default_proxy(registries)
    return PUBLIC_PROXY if found is None else found.address.rstrip("/")


def credentials(credential: str) -> tuple[str, str]:
    """A Go module proxy's credential, ``user:password`` as entered."""
    user, sep, password = credential.partition(":")
    if not sep or not user:
        raise BundleRefused(
            "goproxy_credential",
            "a Go module proxy's credential is user:password, the login the go command sends",
        )
    return user, password


def netrc(registries: Sequence[Resolved]) -> str:
    """The ``.netrc`` the go command authenticates to the organisation's Go module proxies with."""
    lines = []
    for registry in registries:
        if registry.kind != GOPROXY:
            continue
        user, password = credentials(registry.credential)
        host = urlsplit(registry.address).hostname
        lines.append(f"machine {host} login {user} password {password}")
    return "".join(f"{line}\n" for line in lines)


def escaped(path: str) -> str:
    """A module path or version as a module proxy names it: each upper-case letter as ``!`` and
    its lower case."""
    return re.sub(r"[A-Z]", lambda m: "!" + m.group(0).lower(), path)


def fetch_module(
    code: GoModuleCode, registries: Sequence[Resolved], http: Http, dest: Path
) -> Path:
    """The module's zip from the proxy, unpacked under ``dest``; its root."""
    if not _VERSION.match(code.version):
        raise BundleRefused(
            "go_version",
            f"{code.module}: {code.version!r} is not an exact module version (v1.2.3)",
            version=code.version,
        )
    proxy = default_proxy(registries)
    address = proxy_address(registries)
    headers = {}
    ca_bundle = None
    if proxy is not None:
        user, password = credentials(proxy.credential)
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
        ca_bundle = proxy.ca_bundle
    url = f"{address}/{escaped(code.module)}/@v/{escaped(code.version)}.zip"
    data = get(http, url, headers, ca_bundle)
    unpack(f"{code.version}.zip", data, dest)
    root = dest / f"{code.module}@{code.version}"
    if not (root / "go.mod").is_file():
        raise BundleRefused(
            "no_go_mod",
            f"{code.module}@{code.version} from {address} holds no go.mod",
            module=code.module,
        )
    return root.resolve()


def fetch(code: Code, registries: Sequence[Resolved], http: Http, dest: Path) -> Path:
    if isinstance(code, GoModuleCode):
        return fetch_module(code, registries, http, dest)
    return shared.fetch_kept(code, registries, dest)


def check_lockfile(root: Path) -> None:
    """Code that requires other modules carries the go.sum they are checked against."""
    check_go_line(root)
    if _REQUIRE.search((root / "go.mod").read_text()) and not (root / "go.sum").is_file():
        raise BundleRefused(
            "lockfile_missing",
            "the code's go.mod requires other modules and it carries no go.sum: add one beside "
            "it on the source's form",
        )


def install_environment(
    go: Path, root: Path, registries: Sequence[Resolved], scratch: Path, gocache: Path
) -> dict[str, str]:
    """The go command's environment for an install: the organisation's proxy, its credentials in
    a ``.netrc`` and its certificate authorities in a file, both in ``scratch``."""
    (scratch / ".netrc").write_text(netrc(registries))
    (scratch / ".netrc").chmod(0o600)
    ca_file = shared.install_trust(registries, scratch)
    return go_environment(
        go,
        gocache=gocache,
        gomodcache=root / MODULES,
        GOPROXY=proxy_address(registries),
        GOFLAGS="-mod=readonly -modcacherw",
        NETRC=str(scratch / ".netrc"),
        **({"SSL_CERT_FILE": str(ca_file)} if ca_file is not None else {}),
    )


def host_binary(installed: Path) -> Path:
    return installed / HOST_DIR / "host"


def _install(
    root: Path,
    *,
    go: Path,
    reader_binary: Path,
    package: str,
    registries: Sequence[Resolved],
    gocache: Path,
    run: GoRunner,
) -> Manifest:
    check_lockfile(root)
    with tempfile.TemporaryDirectory(prefix="provisa-go-install-") as scratch:
        env = install_environment(go, root, registries, Path(scratch), gocache)
        # Every module the package needs, through the proxy, each checked against go.sum.
        run([str(go), "list", "-deps", reader.pattern(package)], env, root)
        manifest = reader.read_code(
            reader=reader_binary,
            go=go,
            code=root,
            package=package,
            gomodcache=root / MODULES,
            scratch=Path(scratch),
            run=run,
        )
        build.build_host(
            go=go,
            code=root,
            package=package,
            exports=[e.name for e in manifest.exports],
            gomodcache=root / MODULES,
            proxy=env["GOPROXY"],
            dest=host_binary(root),
            run=run,
            extra_env={k: env[k] for k in ("NETRC", "SSL_CERT_FILE", "GOCACHE") if k in env},
        )
    return manifest


def ensure_installed(
    *,
    source_id: str,
    code: Code,
    package: str,
    expected_hash: str | None,
    registries: Sequence[Resolved],
    go: Path,
    reader_binary: Path,
    cache_root: Path,
    lockfile: Lockfile | None = None,
    http: Http = http_get,
    run: GoRunner = run_go,
) -> Installed:
    """The source's code installed -- its modules fetched and its host compiled -- and cached by
    content hash (:func:`provisa.codesource.install.ensure_installed`)."""
    gocache = cache_root / "code" / "go-build"
    gocache.mkdir(parents=True, exist_ok=True)

    def install(root: Path) -> None:
        _install(
            root,
            go=go,
            reader_binary=reader_binary,
            package=package,
            registries=registries,
            gocache=gocache,
            run=run,
        )

    return shared.ensure_installed(
        source_id=source_id,
        runtime="go",
        fetch=lambda dest: fetch(code, registries, http, dest),
        install=install,
        expected_hash=expected_hash,
        cache_root=cache_root,
        lockfile=lockfile,
        lockfiles=LOCKFILES,
    )
