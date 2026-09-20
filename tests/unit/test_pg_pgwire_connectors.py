# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3a2f74-1b6d-4c9a-9f0e-2d7c5a6b9e11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg engine's pgwire-bundle attach connectors (REQ-1730 amendment to REQ-1690): files/sharepoint/
splunk reached LIVE via postgres_fdw pointed at the source's bundled Calcite pgwire server, the same
generalization DuckDB already had. No network / real Calcite jar / real Postgres — ``ensure_endpoint``
is monkeypatched exactly like ``test_replica_strategy.py``'s own DuckDB connector tests.
"""

from __future__ import annotations

import asyncio

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.engine import build_pg_engine
from provisa.federation.strategy import engine_attaches
from provisa.runtime_deps import pgwire_bundles as rd


def _files_source(**kw) -> Source:
    return Source(**{"id": "pg-files", "type": SourceType.files, "path": "/data/reports", **kw})


def _sharepoint_source(**kw) -> Source:
    return Source(
        **{
            "id": "pg-sp-team",
            "type": SourceType.sharepoint,
            "base_url": "https://contoso.sharepoint.com/sites/team",
            "database": "tenant-abc",
            "username": "client-123",
            "password": "shhh-secret",
            **kw,
        }
    )


def _splunk_source(**kw) -> Source:
    return Source(
        **{
            "id": "pg-splunk-prod",
            "type": SourceType.splunk,
            "host": "splunk.internal",
            "port": 8089,
            "password": "tok-xyz",
            "database": "search",
            **kw,
        }
    )


async def _fetch_ok(sql):
    # postgres_fdw's own probe checks pg_extension/pg_available_extensions.
    if "pg_extension" in sql:
        return [{"1": 1}]
    return []


async def _fetch_no_fdw(_sql):
    return []


def test_pg_engine_attaches_files_sharepoint_splunk_live():
    """Registering the connectors makes ``engine_attaches`` true for pg (REQ-1730) — the same
    reachability parity check DuckDB already has for these three types."""
    engine = build_pg_engine()
    for source_type in ("files", "sharepoint", "splunk"):
        assert engine_attaches(engine, source_type) is True
        assert engine.connectors[source_type].reads_in_place is True


def test_pg_splunk_details_builds_postgres_fdw_pointed_at_the_bundle(monkeypatch):
    from provisa.federation.connector_duckdb import PgSplunkConnector

    started: list[str] = []

    def _ensure(source):
        started.append(source.id)
        return pr.PortPair(5440, "127.0.0.1", 5540)

    monkeypatch.setattr(pr, "ensure_endpoint", _ensure)
    src = _splunk_source()
    details = PgSplunkConnector().details(src)
    assert started == ["pg-splunk-prod"]
    assert details["local_schema"] == "fdw_pgwire_pg-splunk-prod"
    ddl = details["attach_ddl"]
    assert "CREATE EXTENSION IF NOT EXISTS postgres_fdw" in ddl
    assert any(
        "FOREIGN DATA WRAPPER postgres_fdw" in stmt
        and "host '127.0.0.1'" in stmt
        and "port '5440'" in stmt
        and "dbname 'provisa'" in stmt
        for stmt in ddl
    )
    assert any("CREATE USER MAPPING" in stmt and "user 'provisa'" in stmt for stmt in ddl)
    assert any(
        "IMPORT FOREIGN SCHEMA pg_splunk_prod" in stmt and "INTO fdw_pgwire_pg-splunk-prod" in stmt
        for stmt in ddl
    )


def test_pg_files_and_sharepoint_details_use_their_own_source_ids(monkeypatch):
    from provisa.federation.connector_duckdb import PgFilesConnector, PgSharepointConnector

    monkeypatch.setattr(pr, "ensure_endpoint", lambda _source: pr.PortPair(5441, "127.0.0.1", 5541))

    files_details = PgFilesConnector().details(_files_source())
    assert files_details["local_schema"] == "fdw_pgwire_pg-files"

    sp_details = PgSharepointConnector().details(_sharepoint_source())
    assert sp_details["local_schema"] == "fdw_pgwire_pg-sp-team"
    assert any("IMPORT FOREIGN SCHEMA pg_sp_team" in stmt for stmt in sp_details["attach_ddl"])


def test_pg_pgwire_connector_probe_reports_cached_bundle(tmp_path, monkeypatch):
    from provisa.federation.connector_duckdb import PgSplunkConnector

    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "x86_64")
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    result = asyncio.run(PgSplunkConnector().probe(_fetch_ok))
    assert result.available is True
    assert "fetched on first use" in result.reason


def test_pg_pgwire_connector_probe_unavailable_on_an_unbuilt_platform(monkeypatch):
    from provisa.federation.connector_duckdb import PgSharepointConnector

    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "aarch64")
    result = asyncio.run(PgSharepointConnector().probe(_fetch_ok))
    assert result.available is False
    assert result.remediation is not None


def test_pg_pgwire_connector_probe_unavailable_without_postgres_fdw():
    from provisa.federation.connector_duckdb import PgFilesConnector

    result = asyncio.run(PgFilesConnector().probe(_fetch_no_fdw))
    assert result.available is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
