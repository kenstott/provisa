# Copyright (c) 2026 Kenneth Stott
# Canary: 1332a832-925f-416e-9e4e-4c58ec02212e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-900: the Postgres federation engine's curated v1 connector set completes the doc's chosen set —
sqlserver (tds_fdw), oracle (oracle_fdw), and delta_lake (pg_duckdb delta_scan) — and the
``(pg_major, platform, libc)`` artifact catalog resolves fail-closed."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_base import DriverProvider, RuntimeDep
from provisa.federation.connector_duckdb import (
    OracleFdwConnector,
    PgDuckdbDeltaConnector,
    TdsFdwConnector,
)
from provisa.federation.engine import build_pg_engine
from provisa.federation.fdw_artifact_catalog import (
    Artifact,
    ArtifactKey,
    ArtifactUnavailable,
    clear_artifacts,
    current_platform,
    discover_bundled_artifacts,
    is_eligible,
    register_artifact,
    resolve_artifact,
)


def _src(sid: str, type_: SourceType, **kw) -> Source:
    fields = {"host": "h", "port": 1433, "database": "db", "username": "u", "password": "p", **kw}
    return Source(id=sid, type=type_, **fields)


# ---- curated set completeness (REQ-900) --------------------------------------


def test_pg_engine_reaches_the_full_curated_v1_set():
    e = build_pg_engine()
    # Every curated v1 source type has a candidate connector registered (probe prunes at runtime).
    candidate_types = {c.source_type for c in e._candidates}
    for t in (
        "postgresql",
        "csv",
        "sqlite",
        "mysql",
        "sqlserver",
        "oracle",
        "parquet",
        "iceberg",
        "delta_lake",
    ):
        assert t in candidate_types, t


# ---- tds_fdw / sqlserver (REQ-900) -------------------------------------------


def test_tds_fdw_identity_read_only_and_runtime_deps():
    c = TdsFdwConnector()
    assert c.engine == "postgres"
    assert c.source_type == "sqlserver"
    assert c.key == "tds_fdw"
    assert c.mechanism is Mechanism.ATTACH_R  # read-only live attach
    assert c.capability().write is False
    assert c.runtime_deps == (RuntimeDep("freetds", DriverProvider.BUNDLED),)
    assert c.operator_deps == ()  # bundled — not BYO


def test_tds_fdw_attach_ddl_defaults_dbo_and_binds_endpoint():
    d = TdsFdwConnector().details(
        _src("crm", SourceType.sqlserver, host="mssql", port=1433, database="sales")
    )
    ddl = d["attach_ddl"]
    assert "CREATE EXTENSION IF NOT EXISTS tds_fdw" in ddl[0]
    assert any("FOREIGN DATA WRAPPER tds_fdw" in s and "servername 'mssql'" in s for s in ddl)
    assert any("IMPORT FOREIGN SCHEMA dbo FROM SERVER fdw_crm" in s for s in ddl)  # default schema


# ---- oracle_fdw / oracle (REQ-900) -------------------------------------------


def test_oracle_fdw_identity_operator_supplied_instant_client():
    c = OracleFdwConnector()
    assert c.source_type == "oracle"
    assert c.key == "oracle_fdw"
    assert c.mechanism is Mechanism.ATTACH_RW  # oracle_fdw is writable
    # Instant Client is Oracle-proprietary / not redistributable → OPERATOR-provided, shown BYO.
    assert c.runtime_deps == (RuntimeDep("Oracle Instant Client + SDK", DriverProvider.OPERATOR),)
    assert c.operator_deps == c.runtime_deps


def test_oracle_fdw_ezconnect_and_uppercased_schema():
    d = OracleFdwConnector().details(
        _src("erp", SourceType.oracle, host="ora", port=1521, database="ORCL", username="scott")
    )
    ddl = d["attach_ddl"]
    assert any("dbserver '//ora:1521/ORCL'" in s for s in ddl)  # EZConnect string
    assert any('IMPORT FOREIGN SCHEMA "SCOTT"' in s for s in ddl)  # Oracle schema upper-cased


# ---- pg_duckdb delta_lake (REQ-900) ------------------------------------------


def test_pg_duckdb_delta_is_scan_via_delta_scan():
    c = PgDuckdbDeltaConnector()
    assert c.source_type == "delta_lake"
    assert c.key == "pg_duckdb_delta"
    assert c.mechanism is Mechanism.SCAN
    assert c.reads_in_place is True
    assert c._reader == "delta_scan"
    d = c.details(_src("lake", SourceType.delta_lake, path="s3://bucket/tbl"))
    assert d["scan"].startswith("delta_scan('s3://bucket/tbl'")


@pytest.mark.asyncio
async def test_pg_duckdb_delta_probe_requires_delta_scan_registered():
    async def fetch(sql: str):
        if "shared_preload_libraries" in sql:
            return [{"v": "pg_duckdb"}]
        if "pg_extension" in sql:
            return [{"one": 1}]
        if "delta_scan" in sql:  # delta extension not compiled in
            return []
        return []

    r = await PgDuckdbDeltaConnector().probe(fetch)
    assert r.available is False
    assert r.remediation and "delta" in r.remediation


# ---- artifact catalog: fail-closed (REQ-900) ---------------------------------


def test_core_contrib_needs_no_artifact():
    # file_fdw ships with PG — resolution returns None (no separately-built artifact), not an error.
    assert resolve_artifact("csv", pg_major=16, platform="linux-x86_64", libc="glibc") is None


def test_unregistered_triple_fails_closed():
    clear_artifacts()
    with pytest.raises(ArtifactUnavailable):
        resolve_artifact("sqlite", pg_major=16, platform="linux-x86_64", libc="glibc")


def test_ineligible_pg_major_fails_closed_even_if_registered():
    clear_artifacts()
    # oracle_fdw supports 15–18; pg_major 13 is out of range → fail closed at the eligibility gate.
    assert is_eligible("oracle_fdw", 13) is False
    with pytest.raises(ArtifactUnavailable):
        resolve_artifact("oracle", pg_major=13, platform="linux-x86_64", libc="glibc")


def test_registered_artifact_resolves_for_exact_triple_only():
    clear_artifacts()
    key = ArtifactKey("tds_fdw", 16, "linux-x86_64", "glibc")
    register_artifact(
        Artifact(
            key,
            wheel="tds_fdw-2.0.5-pg16-manylinux2014_x86_64.whl",
            native_dep_provider=DriverProvider.BUNDLED,
        )
    )
    got = resolve_artifact("sqlserver", pg_major=16, platform="linux-x86_64", libc="glibc")
    assert got is not None and got.wheel.startswith("tds_fdw-2.0.5")
    # A different libc is a different ABI → no silent fallback to the glibc build.
    with pytest.raises(ArtifactUnavailable):
        resolve_artifact("sqlserver", pg_major=16, platform="linux-x86_64", libc="musl")
    clear_artifacts()


def test_uncurated_source_type_fails_closed():
    with pytest.raises(ArtifactUnavailable):
        resolve_artifact("mongodb", pg_major=16, platform="linux-x86_64", libc="glibc")


# ---- artifact discovery from the bundled tree (REQ-900) ----------------------


def test_discovery_registers_only_modules_present_on_disk(tmp_path):
    clear_artifacts()
    # A bundled pkglibdir with tds_fdw + pg_duckdb built, but not oracle_fdw/mysql_fdw/sqlite_fdw.
    (tmp_path / "tds_fdw.so").write_bytes(b"")
    (tmp_path / "pg_duckdb.so").write_bytes(b"")
    found = discover_bundled_artifacts(tmp_path, pg_major=16, platform="linux-x86_64", libc="glibc")
    assert {a.key.extension for a in found} == {"tds_fdw", "pg_duckdb"}
    # sqlserver + delta (pg_duckdb) now resolve; oracle (no module on disk) still fails closed.
    assert resolve_artifact("sqlserver", 16, "linux-x86_64", "glibc").wheel == "tds_fdw.so"
    assert resolve_artifact("delta_lake", 16, "linux-x86_64", "glibc").key.extension == "pg_duckdb"
    with pytest.raises(ArtifactUnavailable):
        resolve_artifact(
            "oracle", 16, "linux-x86_64", "glibc"
        )  # oracle_fdw.so absent → fail closed
    clear_artifacts()


def test_discovery_ignores_core_contrib_and_uses_current_triple(tmp_path):
    clear_artifacts()
    # Modules carry the current host's suffix (.so/.dylib/.dll) so discovery finds them on any OS.
    suffix = {"linux": ".so", "macos": ".dylib", "windows": ".dll"}[
        current_platform().split("-")[0]
    ]
    # file_fdw is core contrib — even if a stray module exists it is not a registered artifact.
    (tmp_path / f"file_fdw{suffix}").write_bytes(b"")
    (tmp_path / f"sqlite_fdw{suffix}").write_bytes(b"")
    found = discover_bundled_artifacts(tmp_path, pg_major=16)  # platform/libc default to this host
    exts = {a.key.extension for a in found}
    assert exts == {"sqlite_fdw"}  # file_fdw skipped
    assert found[0].key.platform == current_platform()
    clear_artifacts()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
