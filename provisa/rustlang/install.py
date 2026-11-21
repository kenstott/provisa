# Copyright (c) 2026 Kenneth Stott
# Canary: 75cfd14d-9ab4-4225-b7b6-7769a3f7af90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fetching a Rust source's code and installing it (REQ-2227).

A Rust source names its code as one of:

- a crate at an exact version (``1.2.3``), downloaded from the organisation's default Cargo
  registry (the ``cargo`` package registry marked default), else crates.io;
- a Git repository at a commit, with a subdirectory;
- an archive uploaded to Provisa, or a directory on this server.

The code is a crate: its ``Cargo.toml`` names a package and a library, and one that depends on
other crates carries its ``Cargo.lock`` (or one is given beside it on the source's form). The
install reads the code (:mod:`provisa.rustlang.reader`) and compiles its host with the code
linked in (:mod:`provisa.rustlang.build`), every crate the build needs fetched through the same
registry into the shared cargo home and checked against the lock's checksums. The install --
code and host -- is cached by the code's content hash like every language's, so a second source
of the same code reuses the binary and compiles nothing.

A registry's token reaches cargo in its environment (``CARGO_REGISTRIES_<NAME>_TOKEN``, cargo's
own way), never in the install; its certificate authority (REQ-2088) through ``CARGO_HTTP_CAINFO``;
which registry stands in for crates.io in a config file in the install's own temporary directory.
"""

# Requirements: REQ-2227, REQ-2088
from __future__ import annotations

import json
import re
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

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
from provisa.codesource.manifest import RUST_RULES, Manifest, read_manifest
from provisa.core.package_registries import CARGO, Resolved
from provisa.rustlang import build
from provisa.rustlang.reader import (
    CargoRunner,
    crate_dir,
    crate_manifest,
    read_document,
    run_cargo,
)
from provisa.rustlang.toolchain import Toolchain, static_target

LOCKFILES = ("Cargo.lock",)
#: Where the install keeps its compiled host.
HOST_DIR = ".provisa-host"
#: The name the organisation's default registry has in cargo's config.
MIRROR = "provisa-mirror"
_VERSION = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?$")


@dataclass(frozen=True)
class CrateCode:
    crate: str
    #: An exact version: a registry is never asked for the latest.
    version: str


Code = CrateCode | GitCode | UploadCode | DirectoryCode


def default_registry(registries: Sequence[Resolved]) -> Resolved | None:
    """The organisation's default Cargo registry; None for crates.io."""
    return next((r for r in registries if r.kind == CARGO and r.mirror), None)


@dataclass(frozen=True)
class RegistrySetup:
    """How cargo reaches the organisation's registry: arguments and environment."""

    args: tuple[str, ...]
    env: dict[str, str]


def registry_setup(registries: Sequence[Resolved], scratch: Path) -> RegistrySetup:
    """cargo's arguments and environment for an install: the default registry in place of
    crates.io (a config file in ``scratch``), its token, and every registry's certificate
    authority (a file in ``scratch``). Neither when the organisation names none."""
    env: dict[str, str] = {}
    args: list[str] = []
    found = default_registry(registries)
    if found is not None:
        config = scratch / "cargo-config.toml"
        config.write_text(
            f'[registries.{MIRROR}]\nindex = "{found.address}"\n\n'
            f'[source.crates-io]\nreplace-with = "{MIRROR}"\n'
        )
        args = ["--config", str(config)]
        env[f"CARGO_REGISTRIES_{MIRROR.upper().replace('-', '_')}_TOKEN"] = found.credential
    ca_file = shared.install_trust(registries, scratch)
    if ca_file is not None:
        env["CARGO_HTTP_CAINFO"] = str(ca_file)
    return RegistrySetup(tuple(args), env)


#: crates.io's index, which a crate comes from when the organisation names no default registry.
CRATES_IO_INDEX = "sparse+https://index.crates.io/"
_MARKERS = ("{crate}", "{version}", "{prefix}", "{lowerprefix}", "{sha256-checksum}")


def _prefix(crate: str) -> str:
    """A crate's directory in an index, as cargo's index format places it."""
    if len(crate) <= 2:
        return str(len(crate))
    if len(crate) == 3:
        return f"3/{crate[0]}"
    return f"{crate[:2]}/{crate[2:4]}"


def download_url(dl: str, crate: str, version: str) -> str:
    """Where a crate file is, from its index's ``dl`` (cargo's registry index format): the
    template with its markers filled, or ``{dl}/{crate}/{version}/download`` when it has none."""
    if not any(marker in dl for marker in _MARKERS):
        return f"{dl.rstrip('/')}/{crate}/{version}/download"
    prefix = _prefix(crate)
    return (
        dl.replace("{crate}", crate)
        .replace("{version}", version)
        .replace("{prefix}", prefix)
        .replace("{lowerprefix}", prefix.lower())
    )


def fetch_crate(code: CrateCode, registries: Sequence[Resolved], http: Http, dest: Path) -> Path:
    """The crate file from the registry's sparse index (its ``config.json`` names where crate
    files are), unpacked under ``dest``; its root."""
    if not _VERSION.match(code.version):
        raise BundleRefused(
            "crate_version",
            f"{code.crate}: {code.version!r} is not an exact crate version (1.2.3)",
            version=code.version,
        )
    registry = default_registry(registries)
    index = CRATES_IO_INDEX if registry is None else registry.address
    if not index.startswith("sparse+"):
        raise BundleRefused(
            "cargo_index",
            f"{index}: a crate is fetched from a sparse index (sparse+https://...)",
            index=index,
        )
    headers: dict[str, str] = {}
    ca_bundle = None
    if registry is not None:
        headers["Authorization"] = registry.credential
        ca_bundle = registry.ca_bundle
    base = index.removeprefix("sparse+").rstrip("/")
    config = json.loads(get(http, f"{base}/config.json", headers, ca_bundle))
    if "{sha256-checksum}" in config.get("dl", ""):
        raise BundleRefused(
            "cargo_index",
            f"{index}: its crate files are named by checksum, which a source's exact version "
            "does not give",
            index=index,
        )
    url = download_url(config["dl"], code.crate, code.version)
    data = get(http, url, headers, ca_bundle)
    unpack(f"{code.crate}-{code.version}.tar.gz", data, dest)
    root = dest / f"{code.crate}-{code.version}"
    if not (root / "Cargo.toml").is_file():
        raise BundleRefused(
            "no_cargo_toml",
            f"{code.crate} {code.version} from {url} holds no Cargo.toml",
            crate=code.crate,
        )
    return root.resolve()


def fetch(code: Code, registries: Sequence[Resolved], http: Http, dest: Path) -> Path:
    if isinstance(code, CrateCode):
        return fetch_crate(code, registries, http, dest)
    return shared.fetch_kept(code, registries, dest)


def check_lockfile(crate: Path, toolchain: str) -> dict:
    """The crate's Cargo.toml; a crate that depends on others carries the Cargo.lock they are
    checked against."""
    manifest = crate_manifest(crate, toolchain)
    depends = any(
        manifest.get(section) for section in ("dependencies", "build-dependencies")
    ) or any(
        isinstance(t, dict) and (t.get("dependencies") or t.get("build-dependencies"))
        for t in (manifest.get("target") or {}).values()
    )
    if depends and not build.lock_of(crate).is_file():
        raise BundleRefused(
            "lockfile_missing",
            "the crate's Cargo.toml depends on other crates and it carries no Cargo.lock: add one "
            "beside it on the source's form",
        )
    return manifest


def host_binary(installed: Path) -> Path:
    return installed / HOST_DIR / "host"


def install_host(
    root: Path,
    *,
    toolchain: Toolchain,
    reader_binary: Path,
    entry: str,
    registries: Sequence[Resolved],
    cargo_home: Path,
    dest: Path,
    target: str,
    run: CargoRunner = run_cargo,
) -> Manifest:
    """The code read and its host compiled for ``target`` at ``dest``; the manifest."""
    crate = crate_dir(root, entry)
    package = check_lockfile(crate, toolchain.version)["package"]["name"]
    document = read_document(reader_binary, crate, toolchain.version)
    manifest = read_manifest(document, RUST_RULES)
    with tempfile.TemporaryDirectory(prefix="provisa-rust-install-") as scratch:
        setup = registry_setup(registries, Path(scratch))
        build.build_host(
            toolchain=toolchain,
            crate=crate,
            package=package,
            document=document,
            manifest=manifest,
            target=target,
            cargo_home=cargo_home,
            dest=dest,
            registry_args=setup.args,
            registry_env=setup.env,
            run=run,
        )
    return manifest


def ensure_installed(
    *,
    source_id: str,
    code: Code,
    entry: str,
    expected_hash: str | None,
    registries: Sequence[Resolved],
    toolchain: Toolchain,
    reader_binary: Path,
    cache_root: Path,
    lockfile: Lockfile | None = None,
    http: Http = http_get,
    run: CargoRunner = run_cargo,
) -> Installed:
    """The source's code installed -- its host compiled for this server -- and cached by content
    hash (:func:`provisa.codesource.install.ensure_installed`)."""
    cargo_home = cache_root / "code" / "cargo-home"

    def install(root: Path) -> None:
        install_host(
            root,
            toolchain=toolchain,
            reader_binary=reader_binary,
            entry=entry,
            registries=registries,
            cargo_home=cargo_home,
            dest=host_binary(root),
            target=static_target(toolchain.host),
            run=run,
        )

    return shared.ensure_installed(
        source_id=source_id,
        runtime="rust",
        fetch=lambda dest: fetch(code, registries, http, dest),
        install=install,
        expected_hash=expected_hash,
        cache_root=cache_root,
        lockfile=lockfile,
        lockfiles=LOCKFILES,
    )
