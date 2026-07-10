# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2f6c93-4e18-4d70-b6c2-9f0e3a15d84b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-908: PgDuckdbIcebergConnector — iceberg_scan attach payload, packaging
surface, and the two-stage probe (preload + iceberg_scan registered).

Pure logic — the async ``probe(fetch)`` is driven by a fake fetch callable; no
live Postgres. The gate specific to iceberg is that a pg_duckdb built WITHOUT
the iceberg extension passes the base preload probe but must fail here because
``iceberg_scan`` is not registered.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgDuckdbIcebergConnector


def _src(sid: str, **kw) -> Source:
    return Source(id=sid, type=SourceType.iceberg, path=kw.pop("path", "/warehouse/t"), **kw)


class _FakeFetch:
    """Async fetch keyed on distinctive substrings of the probe SQL."""

    def __init__(self, *, preloaded: bool, installed: bool, iceberg: bool):
        self._preloaded = preloaded
        self._installed = installed
        self._iceberg = iceberg

    async def __call__(self, sql: str):
        if "shared_preload_libraries" in sql:
            return [{"v": "pg_duckdb" if self._preloaded else ""}]
        if "pg_extension" in sql and "pg_duckdb" in sql:
            return [{"one": 1}] if self._installed else []
        if "pg_available_extensions" in sql:
            return []
        if "iceberg_scan" in sql:
            return [{"one": 1}] if self._iceberg else []
        return []


# ---- identity / packaging (REQ-908) -----------------------------------------


def test_iceberg_connector_identity_and_reader():
    c = PgDuckdbIcebergConnector()
    assert c.engine == "postgres"
    assert c.source_type == "iceberg"
    assert c.key == "pg_duckdb_iceberg"
    assert c.mechanism is Mechanism.ATTACH_RW
    assert c._reader == "iceberg_scan"


def test_iceberg_runtime_deps_document_static_linked_libs():
    deps = PgDuckdbIcebergConnector().runtime_deps
    assert any("libduckdb" in d for d in deps)
    assert any("aws-sdk-cpp" in d and "static-linked" in d for d in deps)


# ---- attach payload (REQ-908) ------------------------------------------------


def test_details_emit_iceberg_scan_with_allow_moved_paths():
    details = PgDuckdbIcebergConnector().details(_src("lake", path="s3://bucket/tbl"))
    scan = details["scan"]
    assert scan.startswith("iceberg_scan('s3://bucket/tbl'")
    assert "allow_moved_paths := true" in scan
    assert details["requires_preload"] == "pg_duckdb"
    assert details["reader"] == "iceberg_scan"


# ---- two-stage probe (REQ-904 / REQ-908) ------------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_preloaded_installed_and_iceberg_registered():
    r = await PgDuckdbIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=True)
    )
    assert r.available is True
    assert "iceberg" in r.reason


@pytest.mark.asyncio
async def test_probe_unavailable_when_pg_duckdb_lacks_iceberg_extension():
    # Base preload probe passes, but iceberg_scan is not registered -> fail closed.
    r = await PgDuckdbIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=False)
    )
    assert r.available is False
    assert "iceberg" in r.reason.lower()
    assert r.remediation and "iceberg" in r.remediation.lower()


@pytest.mark.asyncio
async def test_probe_unavailable_when_not_preloaded():
    r = await PgDuckdbIcebergConnector().probe(
        _FakeFetch(preloaded=False, installed=True, iceberg=True)
    )
    assert r.available is False
    assert "shared_preload_libraries" in r.reason


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
