# Copyright (c) 2026 Kenneth Stott
# Canary: c1481608-0efc-4f92-8ce4-e709802f6f93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-898 — curated prebuilt PG-extension catalog: shape, resolver, fail-loud, coverage.

Structural / data-driven only (no real downloads, no live pgserver)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from provisa.pg_extensions.catalog import (
    NAMED_EXTENSIONS,
    PLATFORM_TAGS,
    CatalogError,
    ExtensionArtifactUnavailable,
    ExtensionBuild,
    ExtensionCatalog,
    current_platform,
    load_catalog,
)


@pytest.fixture(scope="module")
def catalog() -> ExtensionCatalog:
    return load_catalog()


# --- manifest shape / validation ---------------------------------------------


def test_catalog_loads(catalog: ExtensionCatalog) -> None:
    assert isinstance(catalog, ExtensionCatalog)
    assert catalog.extensions


def test_catalog_covers_exactly_the_named_set(catalog: ExtensionCatalog) -> None:
    # REQ-898 named set: sqlite_fdw, parquet_fdw/parquet_s3_fdw, pg_lake, pg_duckdb, pg_analytics.
    assert catalog.extensions == NAMED_EXTENSIONS
    for named in (
        "sqlite_fdw",
        "parquet_fdw",
        "parquet_s3_fdw",
        "pg_lake",
        "pg_duckdb",
        "pg_analytics",
    ):
        assert named in catalog.extensions


def test_every_build_is_on_a_known_pin_axis(catalog: ExtensionCatalog) -> None:
    for ext in catalog.extensions:
        entry = catalog.entry(ext)
        assert entry.builds
        for b in entry.builds:
            assert isinstance(b, ExtensionBuild)
            assert b.platform in PLATFORM_TAGS
            assert b.pg_major in (16, 17)
            assert b.artifact
            assert str(b.pg_major) in b.artifact and b.platform in b.artifact


def test_no_duplicate_pin_within_an_extension(catalog: ExtensionCatalog) -> None:
    for ext in catalog.extensions:
        pins = [(b.pg_major, b.platform) for b in catalog.entry(ext).builds]
        assert len(pins) == len(set(pins))


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "cat.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def test_missing_named_extension_is_rejected(tmp_path: Path) -> None:
    # Only one of the six named extensions present → coverage check fails loud.
    src = _write(
        tmp_path,
        """
        schema_version: 1
        extensions:
          sqlite_fdw:
            kind: fdw
            source_types: [sqlite]
            builds:
              - {pg_major: 16, platform: macos_arm64, artifact: sqlite_fdw-pg16-macos_arm64.tar.gz}
        """,
    )
    with pytest.raises(CatalogError, match="named set"):
        load_catalog(src)


def test_bad_platform_tag_is_rejected(tmp_path: Path) -> None:
    src = _write(
        tmp_path,
        """
        schema_version: 1
        extensions:
          sqlite_fdw:
            kind: fdw
            source_types: [sqlite]
            builds:
              - {pg_major: 16, platform: solaris_sparc, artifact: x.tar.gz}
        """,
    )
    with pytest.raises(CatalogError, match="platform"):
        load_catalog(src)


def test_wrong_schema_version_is_rejected(tmp_path: Path) -> None:
    src = _write(tmp_path, "schema_version: 99\nextensions: {}\n")
    with pytest.raises(CatalogError, match="schema_version"):
        load_catalog(src)


def test_missing_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(CatalogError, match="not found"):
        load_catalog(tmp_path / "nope.yaml")


# --- resolver: picks the right artifact for a (pg_major, platform) -----------


def test_resolver_picks_exact_build(catalog: ExtensionCatalog) -> None:
    b = catalog.resolve("pg_duckdb", 16, "macos_arm64")
    assert b.extension == "pg_duckdb"
    assert b.pg_major == 16
    assert b.platform == "macos_arm64"
    assert b.artifact == "pg_duckdb-pg16-macos_arm64.tar.gz"


def test_resolver_discriminates_pg_major(catalog: ExtensionCatalog) -> None:
    b16 = catalog.resolve("sqlite_fdw", 16, "manylinux_x86_64")
    b17 = catalog.resolve("sqlite_fdw", 17, "manylinux_x86_64")
    assert b16.artifact != b17.artifact
    assert "pg16" in b16.artifact and "pg17" in b17.artifact


def test_resolver_discriminates_platform(catalog: ExtensionCatalog) -> None:
    a = catalog.resolve("pg_analytics", 17, "macos_arm64")
    b = catalog.resolve("pg_analytics", 17, "manylinux_aarch64")
    assert a.artifact != b.artifact


def test_resolve_all_covers_every_named_extension(catalog: ExtensionCatalog) -> None:
    resolved = catalog.resolve_all(16, "macos_arm64")
    assert set(resolved) == NAMED_EXTENSIONS
    for ext, b in resolved.items():
        assert b.extension == ext
        assert b.pg_major == 16 and b.platform == "macos_arm64"


# --- fail loud on an unmatched (version, platform) ---------------------------


def test_unmatched_pg_major_fails_loud(catalog: ExtensionCatalog) -> None:
    with pytest.raises(ExtensionArtifactUnavailable) as ei:
        catalog.resolve("pg_duckdb", 13, "macos_arm64")  # 13 not published
    assert ei.value.extension == "pg_duckdb"
    assert ei.value.pg_major == 13


def test_windows_iceberg_fails_loud_not_fallback(catalog: ExtensionCatalog) -> None:
    # pg_duckdb / pg_lake / pg_analytics are Unix-only — no windows artifact, must NOT fall back.
    for ext in ("pg_duckdb", "pg_lake", "pg_analytics"):
        with pytest.raises(ExtensionArtifactUnavailable):
            catalog.resolve(ext, 16, "windows_amd64")


def test_unknown_platform_tag_fails_loud(catalog: ExtensionCatalog) -> None:
    with pytest.raises(ExtensionArtifactUnavailable, match="platform"):
        catalog.resolve("sqlite_fdw", 16, "beos_ppc")


def test_uncurated_extension_fails_loud(catalog: ExtensionCatalog) -> None:
    with pytest.raises(ExtensionArtifactUnavailable, match="not in the curated"):
        catalog.resolve("timescaledb", 16, "macos_arm64")


def test_resolve_all_fails_loud_on_first_gap(catalog: ExtensionCatalog) -> None:
    # windows has no artifact for the Unix-only engines → the whole layer resolution fails loud.
    with pytest.raises(ExtensionArtifactUnavailable):
        catalog.resolve_all(16, "windows_amd64")


# --- local-first artifact location (installer-adjacent -> cache -> fail) ------


def test_locate_artifact_prefers_local_stage(catalog: ExtensionCatalog, tmp_path: Path) -> None:
    b = catalog.resolve("sqlite_fdw", 16, "macos_arm64")
    staged = tmp_path / b.artifact
    staged.write_bytes(b"bundle")
    found = catalog.locate_artifact(b, search_roots=[tmp_path])
    assert found == staged


def test_locate_artifact_fails_loud_when_unstaged(
    catalog: ExtensionCatalog, tmp_path: Path
) -> None:
    b = catalog.resolve("sqlite_fdw", 16, "macos_arm64")
    with pytest.raises(ExtensionArtifactUnavailable, match="not staged"):
        catalog.locate_artifact(b, search_roots=[tmp_path])


def test_download_url_is_last_resort_not_auto(catalog: ExtensionCatalog) -> None:
    b = catalog.resolve("pg_duckdb", 17, "manylinux_x86_64")
    url = catalog.download_url(b)
    assert url.startswith("https://")
    assert b.artifact in url


# --- host platform tag -------------------------------------------------------


def test_current_platform_is_a_known_tag() -> None:
    assert current_platform() in PLATFORM_TAGS
