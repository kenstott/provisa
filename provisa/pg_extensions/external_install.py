# Copyright (c) 2026 Kenneth Stott
# Canary: 03ac60f6-de12-4131-9737-819480e87fdd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stage the bundled extension/FDW binaries into an operator's own Postgres (REQ-1873).

The bundles are the provisa-pg-ext wheel's ``<os>-<arch>/`` trees, each built for one PostgreSQL
major (manifest.json ``pg_major``). Today they are built for PostgreSQL 16 only, on linux-x64 and
darwin-arm64; any other major or platform is refused by name.

The sequence, and where each step can stop:

1. Read the TARGET's facts: its PostgreSQL major (``pg_config --version``), its platform
   (``uname`` on the target, never the Provisa host's: a Linux container on a Mac is linux), and
   where it keeps modules and extension files.
2. Choose the bundle whose manifest matches that major and platform, and verify every artifact's
   sha256. A mismatch or a corrupted file stops here, before anything is written.
3. Copy the files into a staging directory on the target, then move them into place.
4. pg_duckdb must be preloaded: the caller extends ``shared_preload_libraries`` (never replacing
   it) and the operator restarts the server. Nothing is restarted here.
5. ``create_extensions`` runs CREATE EXTENSION per extension and reports each outcome.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

# Requirements: REQ-1873

_OS = {"Linux": "linux", "Darwin": "darwin"}
_ARCH = {"x86_64": "x64", "amd64": "x64", "aarch64": "arm64", "arm64": "arm64"}
# Shared libraries that support an extension but are not one.
_NOT_EXTENSIONS = frozenset({"libduckdb"})
# Extensions that load only from shared_preload_libraries.
PRELOAD = frozenset({"pg_duckdb"})


class BundleUnavailable(Exception):
    """No bundle can be staged into this target; the message names why. Nothing was written."""


class Target(Protocol):
    label: str

    def run(self, cmd: list[str]) -> str: ...

    def copy_in(self, local: Path, remote: str) -> None: ...


class DockerTarget:
    """A Postgres in a running container, reached with ``docker exec`` and ``docker cp``."""

    def __init__(self, container: str) -> None:
        self.container = container
        self.label = f"container {container}"

    def run(self, cmd: list[str]) -> str:
        return _checked(["docker", "exec", self.container, *cmd])

    def copy_in(self, local: Path, remote: str) -> None:
        _checked(["docker", "cp", str(local), f"{self.container}:{remote}"])


class LocalTarget:
    """A Postgres on this machine: the ``pg_config`` on PATH names it."""

    label = "this machine"

    def run(self, cmd: list[str]) -> str:
        return _checked(cmd)

    def copy_in(self, local: Path, remote: str) -> None:
        _checked(["cp", str(local), remote])


def _checked(cmd: list[str]) -> str:
    done = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603 - fixed tool argv
    if done.returncode != 0:
        raise RuntimeError(
            f"{shlex.join(cmd)} failed: {done.stderr.strip() or done.stdout.strip()}"
        )
    return done.stdout.strip()


@dataclass(frozen=True)
class TargetFacts:
    pg_major: int
    platform: str  # <os>-<arch>, the bundle directory name
    pkglibdir: str
    sharedir: str


@dataclass(frozen=True)
class Installed:
    bundle: Path
    extensions: list[str]  # CREATE EXTENSION names, sorted
    needs_preload: list[str]


def read_facts(target: Target) -> TargetFacts:
    version = target.run(["pg_config", "--version"])  # "PostgreSQL 16.4 (Debian ...)"
    words = version.split()
    if len(words) < 2 or words[0] != "PostgreSQL":
        raise BundleUnavailable(f"pg_config on {target.label} reported {version!r}")
    major = int(words[1].split(".")[0])
    system, machine = target.run(["uname", "-s"]), target.run(["uname", "-m"])
    if system not in _OS or machine not in _ARCH:
        raise BundleUnavailable(
            f"no extension bundle is built for {system} {machine} ({target.label})"
        )
    return TargetFacts(
        pg_major=major,
        platform=f"{_OS[system]}-{_ARCH[machine]}",
        pkglibdir=target.run(["pg_config", "--pkglibdir"]),
        sharedir=target.run(["pg_config", "--sharedir"]),
    )


def _manifest(tree: Path) -> dict:
    return json.loads((tree / "manifest.json").read_text())


def select_bundle(ext_root: Path, facts: TargetFacts) -> Path:
    """The bundle built for the target's major and platform, or BundleUnavailable naming both
    the target and every bundle there is."""
    available = []
    for tree in sorted(p for p in Path(ext_root).iterdir() if (p / "manifest.json").is_file()):
        major = int(_manifest(tree)["pg_major"])
        available.append(f"PostgreSQL {major} on {tree.name}")
        if tree.name == facts.platform and major == facts.pg_major:
            return tree
    raise BundleUnavailable(
        f"no extension bundle for PostgreSQL {facts.pg_major} on {facts.platform}; "
        f"bundles exist for: {', '.join(available) or 'none'}"
    )


def verify_bundle(tree: Path) -> None:
    """Every artifact the manifest lists is present with its recorded sha256."""
    for artifact in _manifest(tree)["artifacts"]:
        path = tree / artifact["file"]
        if not path.is_file():
            raise BundleUnavailable(f"{artifact['name']}: {artifact['file']} is missing")
        if hashlib.sha256(path.read_bytes()).hexdigest() != artifact["sha256"]:
            raise BundleUnavailable(f"{artifact['name']}: {artifact['file']} fails its checksum")


def install(target: Target, ext_root: Path) -> Installed:
    """Stage the matching bundle into the target's module and extension directories."""
    facts = read_facts(target)
    tree = select_bundle(ext_root, facts)
    verify_bundle(tree)
    libs = sorted((tree / "lib").iterdir())
    shares = sorted((tree / "share" / "extension").iterdir())
    # A directory on the TARGET (inside the container, or this host for --local), uniquely named
    # and removed below; nothing of Provisa's own is written there.
    staging = f"/tmp/provisa-pg-ext-{uuid.uuid4().hex}"  # noqa: S108  # nosec B108
    target.run(["mkdir", "-p", f"{staging}/lib", f"{staging}/extension"])
    try:
        for f in libs:
            target.copy_in(f, f"{staging}/lib/{f.name}")
        for f in shares:
            target.copy_in(f, f"{staging}/extension/{f.name}")
        target.run(["mv", *[f"{staging}/lib/{f.name}" for f in libs], f"{facts.pkglibdir}/"])
        target.run(
            [
                "mv",
                *[f"{staging}/extension/{f.name}" for f in shares],
                f"{facts.sharedir}/extension/",
            ]
        )
    finally:
        target.run(["rm", "-rf", staging])
    names = sorted(
        a["name"] for a in _manifest(tree)["artifacts"] if a["name"] not in _NOT_EXTENSIONS
    )
    return Installed(tree, names, sorted(PRELOAD & set(names)))


def preload_with(current: str, library: str) -> str | None:
    """``shared_preload_libraries`` with ``library`` appended, or None when it is already there."""
    present = [p.strip() for p in current.split(",") if p.strip()]
    if library in present:
        return None
    return ",".join([*present, library])


def _psql(target: Target, user: str, database: str, sql: str) -> str:
    return target.run(["psql", "-v", "ON_ERROR_STOP=1", "-U", user, "-d", database, "-Atc", sql])


def apply_preload(target: Target, user: str, database: str, libraries: list[str]) -> bool:
    """Extend ``shared_preload_libraries`` with ``libraries``. True when it changed, in which case
    the server must be restarted before those extensions can be created."""
    current = _psql(target, user, database, "SHOW shared_preload_libraries")
    changed = False
    for library in libraries:
        extended = preload_with(current, library)
        if extended is not None:
            current, changed = extended, True
    if changed:
        _psql(target, user, database, f"ALTER SYSTEM SET shared_preload_libraries = '{current}'")
    return changed


def create_extensions(
    target: Target, user: str, database: str, names: list[str]
) -> dict[str, str | None]:
    """CREATE EXTENSION for each name. Maps each to None, or the reason it could not be created."""
    outcome: dict[str, str | None] = {}
    for name in names:
        try:
            _psql(target, user, database, f'CREATE EXTENSION IF NOT EXISTS "{name}"')
            outcome[name] = None
        except RuntimeError as exc:
            outcome[name] = str(exc)
    return outcome
