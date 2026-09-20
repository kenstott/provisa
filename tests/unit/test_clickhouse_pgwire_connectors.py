# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9e6b12-7a4f-4e58-b1c3-9f2a6d8e5c40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse engine's pgwire-bundle attach connectors (REQ-1730 amendment to REQ-1690): files/
sharepoint/splunk reached LIVE via the built-in PostgreSQL database engine pointed at the source's
bundled Calcite pgwire server, the same generalization DuckDB and pg already have. No network / real
Calcite jar / real ClickHouse — ``ensure_endpoint`` is monkeypatched exactly like
``test_replica_strategy.py``'s own DuckDB connector tests.
"""

from __future__ import annotations

import asyncio

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.engine import build_clickhouse_engine
from provisa.federation.strategy import engine_attaches
from provisa.runtime_deps import pgwire_bundles as rd


def _files_source(**kw) -> Source:
    return Source(**{"id": "ch-files", "type": SourceType.files, "path": "/data/reports", **kw})


def _sharepoint_source(**kw) -> Source:
    return Source(
        **{
            "id": "ch-sp-team",
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
            "id": "ch-splunk-prod",
            "type": SourceType.splunk,
            "host": "splunk.internal",
            "port": 8089,
            "password": "tok-xyz",
            "database": "search",
            **kw,
        }
    )


async def _fetch_unused(_sql):
    return []


def test_clickhouse_engine_attaches_files_sharepoint_splunk_live():
    """Registering the connectors makes ``engine_attaches`` true for clickhouse (REQ-1730) — the
    same reachability parity check DuckDB/pg already have for these three types."""
    engine = build_clickhouse_engine()
    for source_type in ("files", "sharepoint", "splunk"):
        assert engine_attaches(engine, source_type) is True
        assert engine.connectors[source_type].reads_in_place is True


def test_clickhouse_splunk_details_builds_postgresql_engine_pointed_at_the_bundle(monkeypatch):
    from provisa.federation.clickhouse_connectors import ClickHouseSplunkConnector

    started: list[str] = []

    def _ensure(source):
        started.append(source.id)
        return pr.PortPair(5440, "127.0.0.1", 5540)

    monkeypatch.setattr(pr, "ensure_endpoint", _ensure)
    src = _splunk_source()
    details = ClickHouseSplunkConnector().details(src)
    assert started == ["ch-splunk-prod"]
    assert details["local_schema"] == "ch_pgwire_ch-splunk-prod"
    (ddl,) = details["attach_ddl"]
    assert 'CREATE DATABASE IF NOT EXISTS "ch_pgwire_ch-splunk-prod"' in ddl
    assert "ENGINE = PostgreSQL('127.0.0.1:5440', 'provisa', 'provisa', ''" in ddl
    assert "'ch_splunk_prod')" in ddl


def test_clickhouse_files_and_sharepoint_details_use_their_own_source_ids(monkeypatch):
    from provisa.federation.clickhouse_connectors import (
        ClickHouseFilesConnector,
        ClickHouseSharepointConnector,
    )

    monkeypatch.setattr(pr, "ensure_endpoint", lambda _source: pr.PortPair(5441, "127.0.0.1", 5541))

    files_details = ClickHouseFilesConnector().details(_files_source())
    assert files_details["local_schema"] == "ch_pgwire_ch-files"

    sp_details = ClickHouseSharepointConnector().details(_sharepoint_source())
    assert sp_details["local_schema"] == "ch_pgwire_ch-sp-team"
    assert "'ch_sp_team')" in sp_details["attach_ddl"][0]


def test_clickhouse_pgwire_connector_probe_reports_cached_bundle(tmp_path, monkeypatch):
    from provisa.federation.clickhouse_connectors import ClickHouseSplunkConnector

    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "x86_64")
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    result = asyncio.run(ClickHouseSplunkConnector().probe(_fetch_unused))
    assert result.available is True
    assert "fetched on first use" in result.reason


def test_clickhouse_pgwire_connector_probe_unavailable_on_an_unbuilt_platform(monkeypatch):
    from provisa.federation.clickhouse_connectors import ClickHouseSharepointConnector

    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "aarch64")
    result = asyncio.run(ClickHouseSharepointConnector().probe(_fetch_unused))
    assert result.available is False
    assert result.remediation is not None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
