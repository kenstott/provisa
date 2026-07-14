# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Curated, versioned catalog of prebuilt PostgreSQL extensions (REQ-898).

Loads ``config/pg_extension_catalog.yaml`` — a pinned inventory of the OOTB extension/FDW set
(sqlite_fdw, parquet_fdw, parquet_s3_fdw, pg_lake, pg_duckdb, pg_analytics) — and resolves, for the
running pgserver's ``(pg_major, platform)``, the exact prebuilt artifact that layers onto the pip-
installed runtime WITHOUT a build toolchain.

Resolution FAILS LOUD (project rule, REQ-898): when no prebuilt artifact matches ``(pg_major,
platform)`` the resolver raises ``ExtensionArtifactUnavailable`` — it never falls back to a source
build or a different triple. Artifact FILES are located local-first, mirroring the installer add-on
seam (REQ-977): installer-adjacent dir → the on-disk FDW cache → (last) a published download URL.
"""

from __future__ import annotations

import os
import platform as _platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# The two pin axes REQ-898 indexes on.
PLATFORM_TAGS: frozenset[str] = frozenset(
    {"manylinux_x86_64", "manylinux_aarch64", "macos_arm64", "macos_x86_64", "windows_amd64"}
)

# The curated OOTB set REQ-898 names — the catalog MUST cover exactly these.
NAMED_EXTENSIONS: frozenset[str] = frozenset(
    {"sqlite_fdw", "parquet_fdw", "parquet_s3_fdw", "pg_lake", "pg_duckdb", "pg_analytics"}
)

_SCHEMA_VERSION = 1
_DEFAULT_CATALOG = Path(__file__).resolve().parents[2] / "config" / "pg_extension_catalog.yaml"
# On-disk cache the build/CI path already uses (scripts/ci/build_pg_extensions.sh).
_CACHE_ROOT = Path(os.environ.get("PROVISA_FDW_CACHE", str(Path.home() / ".cache" / "provisa-fdw")))
_RELEASE_BASE = "https://github.com/kenstott/provisa/releases/download"


class CatalogError(Exception):
    """The catalog file is malformed or violates its schema — a build-time defect, not a runtime miss."""


class ExtensionArtifactUnavailable(Exception):  # REQ-898
    """No prebuilt artifact resolves for a ``(extension, pg_major, platform)`` — fail LOUD, never a
    source-build fallback. The extension is simply unavailable on this pgserver deployment."""

    def __init__(self, extension: str, pg_major: int, platform: str, reason: str) -> None:
        self.extension = extension
        self.pg_major = pg_major
        self.platform = platform
        super().__init__(
            f"no prebuilt artifact for extension {extension!r} on "
            f"(pg_major={pg_major}, platform={platform!r}): {reason}"
        )


@dataclass(frozen=True)
class ExtensionBuild:
    """One concrete prebuilt bundle for an ``(extension, pg_major, platform)`` triple."""

    extension: str
    pg_major: int
    platform: str
    artifact: str  # bundle filename CI publishes for this triple


@dataclass(frozen=True)
class CatalogEntry:
    """A curated extension and every prebuilt triple published for it."""

    extension: str
    kind: str
    source_types: tuple[str, ...]
    native_deps: tuple[str, ...]
    builds: tuple[ExtensionBuild, ...]

    def build_for(self, pg_major: int, platform: str) -> ExtensionBuild | None:
        for b in self.builds:
            if b.pg_major == pg_major and b.platform == platform:
                return b
        return None


def current_platform() -> str:
    """This host's REQ-898 platform tag (``manylinux_x86_64`` | ``macos_arm64`` | …). Linux maps to
    ``manylinux`` (the redistributable-glibc baseline the prebuilt artifacts target)."""
    os_tag = {"linux": "manylinux", "darwin": "macos", "win32": "windows"}.get(
        str(sys.platform), str(sys.platform)
    )
    arch = {"x86_64": "x86_64", "amd64": "amd64", "arm64": "arm64", "aarch64": "aarch64"}.get(
        _platform.machine().lower(), _platform.machine().lower()
    )
    if os_tag == "manylinux":
        arch = {"amd64": "x86_64", "arm64": "aarch64"}.get(arch, arch)
    if os_tag == "windows":
        arch = {"x86_64": "amd64"}.get(arch, arch)
    return f"{os_tag}_{arch}"


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise CatalogError(msg)


def _parse_build(extension: str, raw: Any) -> ExtensionBuild:
    _require(isinstance(raw, dict), f"{extension}: each build must be a mapping, got {type(raw)}")
    assert isinstance(raw, dict)
    pg_major = raw.get("pg_major")
    platform = raw.get("platform")
    artifact = raw.get("artifact")
    _require(isinstance(pg_major, int), f"{extension}: build pg_major must be an int")
    _require(
        platform in PLATFORM_TAGS,
        f"{extension}: build platform {platform!r} not in {sorted(PLATFORM_TAGS)}",
    )
    _require(
        isinstance(artifact, str) and bool(artifact),
        f"{extension}: build artifact must be non-empty",
    )
    assert isinstance(pg_major, int) and isinstance(platform, str) and isinstance(artifact, str)
    return ExtensionBuild(extension, pg_major, platform, artifact)


def _parse_entry(extension: str, raw: Any) -> CatalogEntry:
    _require(isinstance(raw, dict), f"{extension}: entry must be a mapping")
    assert isinstance(raw, dict)
    kind = raw.get("kind")
    source_types = raw.get("source_types")
    native_deps = raw.get("native_deps", [])
    builds_raw = raw.get("builds")
    _require(isinstance(kind, str) and bool(kind), f"{extension}: kind must be a non-empty string")
    _require(
        isinstance(source_types, list) and bool(source_types),
        f"{extension}: source_types list required",
    )
    _require(isinstance(native_deps, list), f"{extension}: native_deps must be a list")
    _require(
        isinstance(builds_raw, list) and bool(builds_raw), f"{extension}: builds list required"
    )
    assert isinstance(kind, str) and isinstance(source_types, list)
    assert isinstance(native_deps, list) and isinstance(builds_raw, list)
    builds = tuple(_parse_build(extension, b) for b in builds_raw)
    seen: set[tuple[int, str]] = set()
    for b in builds:
        key = (b.pg_major, b.platform)
        _require(key not in seen, f"{extension}: duplicate build for {key}")
        seen.add(key)
    return CatalogEntry(
        extension,
        kind,
        tuple(str(s) for s in source_types),
        tuple(str(d) for d in native_deps),
        builds,
    )


class ExtensionCatalog:
    """The parsed, validated curated catalog + the fail-loud resolver over it (REQ-898)."""

    def __init__(self, entries: dict[str, CatalogEntry]) -> None:
        self._entries = entries

    @property
    def extensions(self) -> frozenset[str]:
        return frozenset(self._entries)

    def entry(self, extension: str) -> CatalogEntry:
        entry = self._entries.get(extension)
        if entry is None:
            raise ExtensionArtifactUnavailable(
                extension, -1, "*", "extension not in the curated REQ-898 catalog"
            )
        return entry

    def resolve(self, extension: str, pg_major: int, platform: str) -> ExtensionBuild:
        """Select the prebuilt build for ``(extension, pg_major, platform)`` or FAIL LOUD (REQ-898).

        Raises ``ExtensionArtifactUnavailable`` when the extension is uncurated, or no build was
        published for the exact ``(pg_major, platform)`` — never a source-build fallback."""
        if platform not in PLATFORM_TAGS:
            raise ExtensionArtifactUnavailable(
                extension,
                pg_major,
                platform,
                f"unknown platform tag (expected {sorted(PLATFORM_TAGS)})",
            )
        build = self.entry(extension).build_for(pg_major, platform)
        if build is None:
            raise ExtensionArtifactUnavailable(
                extension,
                pg_major,
                platform,
                "no prebuilt artifact published for this (pg_major, platform)",
            )
        return build

    def resolve_all(self, pg_major: int, platform: str) -> dict[str, ExtensionBuild]:
        """Resolve every curated extension for a deployment's ``(pg_major, platform)``. Fails loud on
        the FIRST extension with no artifact — a partial curated set is not a valid layer."""
        return {ext: self.resolve(ext, pg_major, platform) for ext in sorted(self._entries)}

    def locate_artifact(
        self, build: ExtensionBuild, *, search_roots: list[Path] | None = None
    ) -> Path:
        """Find the on-disk bundle for a resolved build, local-first (REQ-977 seam): installer-adjacent
        dir (``PROVISA_PG_EXT_DIR``) → the FDW cache → fail loud. Never downloads implicitly; the
        published URL is available via ``download_url`` for a gated, explicit fetch."""
        roots = search_roots if search_roots is not None else self.default_search_roots()
        for root in roots:
            candidate = root / build.artifact
            if candidate.is_file():
                return candidate
        raise ExtensionArtifactUnavailable(
            build.extension,
            build.pg_major,
            build.platform,
            f"artifact {build.artifact!r} not staged in {[str(r) for r in roots]} "
            f"(stage it or fetch {self.download_url(build)})",
        )

    @staticmethod
    def default_search_roots() -> list[Path]:
        """Local-first roots, in precedence order: installer-adjacent staging dir, then the FDW cache."""
        roots: list[Path] = []
        adjacent = os.environ.get("PROVISA_PG_EXT_DIR")
        if adjacent:
            roots.append(Path(adjacent))
        roots.append(_CACHE_ROOT / "pg-ext")
        return roots

    @staticmethod
    def download_url(build: ExtensionBuild) -> str:
        """The published release URL for a build's bundle — the LAST resort, fetched only on explicit
        opt-in (mirrors the installer add-on gate, REQ-977). Never invoked by resolution itself."""
        return f"{_RELEASE_BASE}/pg-ext/{build.artifact}"


def load_catalog(path: str | Path | None = None) -> ExtensionCatalog:
    """Load + validate the curated catalog. Raises ``CatalogError`` on any schema violation, and
    requires the catalog to cover exactly the REQ-898 named set (no missing/extra curated extensions)."""
    src = Path(path) if path is not None else _DEFAULT_CATALOG
    _require(src.is_file(), f"catalog file not found: {src}")
    data = yaml.safe_load(src.read_text())
    _require(isinstance(data, dict), f"{src}: top level must be a mapping")
    assert isinstance(data, dict)
    _require(
        data.get("schema_version") == _SCHEMA_VERSION,
        f"{src}: schema_version must be {_SCHEMA_VERSION}, got {data.get('schema_version')!r}",
    )
    exts_raw = data.get("extensions")
    _require(isinstance(exts_raw, dict) and bool(exts_raw), f"{src}: extensions mapping required")
    assert isinstance(exts_raw, dict)
    entries = {str(name): _parse_entry(str(name), body) for name, body in exts_raw.items()}
    covered = frozenset(entries)
    _require(
        covered == NAMED_EXTENSIONS,
        f"{src}: catalog must cover exactly the REQ-898 named set; "
        f"missing={sorted(NAMED_EXTENSIONS - covered)} extra={sorted(covered - NAMED_EXTENSIONS)}",
    )
    return ExtensionCatalog(entries)
