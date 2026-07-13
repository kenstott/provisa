# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Curated Postgres FDW/extension ARTIFACT catalog, indexed by ``(pg_major, platform, libc)`` (REQ-900).

An externally compiled ``.so`` must match three axes or the server refuses to load it: the PG major
ABI, the platform/arch, and (on Linux) the libc/C++ ABI (see docs/arch/fdw_catalog.md). This module is
the static pre-flight for the Postgres federation engine's curated v1 connector set: it answers "is
there a build artifact for this extension on THIS deployment's triple?" — BEFORE the runtime probe
opens the live server.

Resolution FAILS CLOSED (project rule, REQ-900): no artifact for a ``(pg_major, platform, libc)`` means
the source is unreachable on this Postgres deployment — never a silent fallback. Two gates compose:

1. ELIGIBILITY — the doc-stated PG-major support range per extension (``_ELIGIBILITY``). A triple whose
   pg_major is outside an extension's range can never have an artifact; it fails closed here.
2. ARTIFACT PRESENCE — a concrete built wheel registered for the exact triple (``register_artifact``).
   CI builds one wheel per ``(extension × pg_major × platform tag)`` and registers it at startup; until
   one is registered the extension is unavailable on that triple (fail closed).

The v1 curated set (docs/arch/fdw_catalog.md "Chosen v1 catalog set") maps each Postgres source type to
the extension that backs it (``SOURCE_TYPE_EXTENSION``). ``file_fdw`` is core contrib (ships with PG, no
artifact); every other extension is a build artifact resolved through this catalog.
"""

from __future__ import annotations

import platform as _platform
import sys
from dataclasses import dataclass
from pathlib import Path

from provisa.federation.connector_base import DriverProvider


class ArtifactUnavailable(Exception):  # REQ-900
    """No build artifact resolves for a ``(extension, pg_major, platform, libc)`` triple — the source
    is unreachable on this Postgres deployment. Raised on fail-closed resolution; never a fallback."""

    def __init__(
        self, extension: str, pg_major: int, platform: str, libc: str, reason: str
    ) -> None:
        self.extension = extension
        self.pg_major = pg_major
        self.platform = platform
        self.libc = libc
        super().__init__(
            f"no artifact for extension {extension!r} on (pg_major={pg_major}, platform={platform!r}, "
            f"libc={libc!r}): {reason}"
        )


@dataclass(frozen=True)
class ArtifactKey:  # REQ-900 — the pin triple plus the extension it builds
    extension: str
    pg_major: int
    platform: str  # e.g. "linux-x86_64" | "linux-arm64" | "macos-arm64" | "macos-x86_64"
    libc: str  # "glibc" | "musl" on Linux; "" on macOS (no libc axis)


@dataclass(frozen=True)
class Artifact:  # REQ-900 — a concrete built wheel for one triple
    key: ArtifactKey
    wheel: str  # the built wheel / package filename CI produced
    native_dep_provider: DriverProvider  # who supplies the extension's native lib on this triple


# Postgres source type -> the extension that backs it in the v1 curated set (docs/arch/fdw_catalog.md).
# ``csv`` is core contrib file_fdw (no artifact); parquet/iceberg/delta all route through pg_duckdb.
SOURCE_TYPE_EXTENSION: dict[str, str] = {
    "csv": "file_fdw",
    "sqlite": "sqlite_fdw",
    "mysql": "mysql_fdw",
    "sqlserver": "tds_fdw",
    "oracle": "oracle_fdw",
    "parquet": "pg_duckdb",
    "iceberg": "pg_duckdb",
    "delta_lake": "pg_duckdb",
}

# Core contrib extensions that ship with any PG built with contrib — no separately-built artifact
# (docs/arch/fdw_catalog.md). Always eligible; the runtime probe still gates functional availability.
_CORE_CONTRIB: frozenset[str] = frozenset({"file_fdw", "postgres_fdw"})

# Doc-stated PG-major support range per built extension (inclusive), from the catalog's FDW table.
# A pg_major outside the range can never load the extension → fail closed at the eligibility gate.
_ELIGIBILITY: dict[str, range] = {
    "sqlite_fdw": range(13, 18),  # 13–17
    "mysql_fdw": range(14, 19),  # 14–18
    "tds_fdw": range(13, 19),  # 13–18
    "oracle_fdw": range(15, 19),  # 15–18 (project pins pgserver's 16/17)
    "pg_duckdb": range(14, 19),  # 14–18
}

# Who supplies each extension's native dep on a built triple (docs/arch/fdw_catalog.md). Oracle Instant
# Client is Oracle-proprietary / not redistributable → OPERATOR (BYO); the rest ride inside the wheel.
_EXTENSION_DEP_PROVIDER: dict[str, DriverProvider] = {
    "sqlite_fdw": DriverProvider.SYSTEM,  # links the OS libsqlite3
    "mysql_fdw": DriverProvider.BUNDLED,
    "tds_fdw": DriverProvider.BUNDLED,  # bundled freetds
    "oracle_fdw": DriverProvider.OPERATOR,  # Instant Client — deployment supplies it
    "pg_duckdb": DriverProvider.BUNDLED,  # vendored DuckDB inside the wheel
}

# Registered concrete artifacts, keyed by the exact triple. Empty until CI registers built wheels —
# so every non-core extension fails closed on every triple until an artifact is registered (REQ-900).
_ARTIFACTS: dict[ArtifactKey, Artifact] = {}


def register_artifact(artifact: Artifact) -> None:
    """Register a CI-built wheel for its ``(extension, pg_major, platform, libc)`` triple. Called at
    startup as artifacts are discovered; makes the extension resolvable on exactly that triple."""
    _ARTIFACTS[artifact.key] = artifact


def clear_artifacts() -> None:
    """Drop all registered artifacts (test isolation / re-discovery)."""
    _ARTIFACTS.clear()


def is_eligible(extension: str, pg_major: int) -> bool:
    """Whether ``extension`` can EVER load on ``pg_major`` (doc-stated support range). Core contrib is
    always eligible. Eligibility is necessary but not sufficient — a concrete artifact must also exist
    for the full triple (see ``resolve_artifact``)."""
    if extension in _CORE_CONTRIB:
        return True
    supported = _ELIGIBILITY.get(extension)
    return supported is not None and pg_major in supported


def resolve_artifact(source_type: str, pg_major: int, platform: str, libc: str) -> Artifact | None:
    """Resolve the build artifact for a Postgres source type on a ``(pg_major, platform, libc)`` triple,
    or FAIL CLOSED (REQ-900). Returns ``None`` for core-contrib extensions (file_fdw — no artifact, it
    ships with PG). Raises ``ArtifactUnavailable`` when the source type is not in the curated set, the
    extension is ineligible for the pg_major, or no wheel is registered for the exact triple — never a
    silent fallback to a different triple or a best-effort guess."""
    extension = SOURCE_TYPE_EXTENSION.get(source_type)
    if extension is None:
        raise ArtifactUnavailable(
            str(source_type), pg_major, platform, libc, "source type not in the curated v1 catalog"
        )
    if extension in _CORE_CONTRIB:
        return None  # core contrib ships with PG — no separately-built artifact to resolve
    if not is_eligible(extension, pg_major):
        raise ArtifactUnavailable(
            extension, pg_major, platform, libc, f"pg_major outside {extension} support range"
        )
    artifact = _ARTIFACTS.get(ArtifactKey(extension, pg_major, platform, libc))
    if artifact is None:
        raise ArtifactUnavailable(
            extension, pg_major, platform, libc, "no built artifact registered for this triple"
        )
    return artifact


# --- artifact discovery: the catalog IS the bundled tree (REQ-900) ------------


def current_platform() -> str:
    """This host's platform/arch tag (``linux-x86_64`` | ``macos-arm64`` | …) — the second pin axis."""
    sys_platform = str(sys.platform)  # str-typed so the mapping stays live across platforms
    os_tag = {"linux": "linux", "darwin": "macos", "win32": "windows"}.get(
        sys_platform, sys_platform
    )
    arch = {"x86_64": "x86_64", "amd64": "x86_64", "arm64": "arm64", "aarch64": "arm64"}.get(
        _platform.machine().lower(), _platform.machine().lower()
    )
    return f"{os_tag}-{arch}"


def current_libc() -> str:
    """This host's libc axis: ``glibc``/``musl`` on Linux (the C++/ABI floor), ``""`` on macOS/Windows
    (no libc pin). ``glibc`` vs ``musl`` is distinguished by the platform libc tuple."""
    if str(sys.platform) != "linux":
        return ""
    name, _ = _platform.libc_ver()
    return "musl" if "musl" in name.lower() else "glibc"


def _artifact_suffix(platform: str) -> str:
    """The compiled-module suffix for a platform tag (``linux-…`` → ``.so``, ``macos-…`` → ``.dylib``,
    ``windows-…`` → ``.dll``) — follows the TARGET triple, not the host."""
    os_tag = platform.split("-", 1)[0]
    return {"linux": ".so", "macos": ".dylib", "windows": ".dll"}.get(os_tag, ".so")


def discover_bundled_artifacts(
    pkglibdir: str | Path, pg_major: int, *, platform: str | None = None, libc: str | None = None
) -> list[Artifact]:
    """Register an ``Artifact`` for each curated extension whose compiled module is PRESENT in the
    bundled ``pkglibdir`` (``pg_config --pkglibdir``), on the given / current triple (REQ-900). The
    catalog thus reflects what is actually installed, not a hand-maintained wheel list — a triple with
    no ``<ext>`` module on disk simply has no artifact and resolves fail-closed. Returns what it found;
    core contrib (file_fdw) is skipped (it ships with PG, needs no artifact)."""
    root = Path(pkglibdir)
    plat = platform or current_platform()
    lc = current_libc() if libc is None else libc
    suffix = _artifact_suffix(plat)
    found: list[Artifact] = []
    for extension in sorted(set(SOURCE_TYPE_EXTENSION.values()) - _CORE_CONTRIB):
        module = root / f"{extension}{suffix}"
        if not module.exists():
            continue  # not built for this triple → stays unreachable (fail closed)
        artifact = Artifact(
            ArtifactKey(extension, pg_major, plat, lc),
            wheel=module.name,
            native_dep_provider=_EXTENSION_DEP_PROVIDER[extension],
        )
        register_artifact(artifact)
        found.append(artifact)
    return found
