# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2f6c93-4e18-4d70-b6c2-9f0e3a15d84b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1867: PgDuckdbDatabricksIcebergConnector — Unity Catalog Iceberg REST metadata resolution,
the resulting iceberg_scan attach payload, and the two-stage probe (preload + iceberg_scan
registered). Adds a live reach ALONGSIDE Databricks' existing DIRECT reach (REQ-987), never
replacing it.

Pure logic — the resolver's ``httpx`` call is mocked; the async ``probe(fetch)`` is driven by a fake
fetch callable. No live Postgres, no live Databricks workspace.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgDuckdbDatabricksIcebergConnector


def _src(sid: str, **kw) -> Source:
    federation_hints = kw.pop("federation_hints", {"schema": "sales", "table": "orders"})
    return Source(
        id=sid,
        type=SourceType.databricks,
        host=kw.pop("host", "acme.cloud.databricks.com"),
        password=kw.pop("password", "dapiTOKEN"),
        database=kw.pop("database", "workspace"),
        federation_hints=federation_hints,
        **kw,
    )


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


# ---- identity / packaging (REQ-1867) ----------------------------------------


def test_connector_identity_and_reach_modes():
    c = PgDuckdbDatabricksIcebergConnector()
    assert c.engine == "postgres"
    assert c.source_type == "databricks"
    assert c.key == "pg_duckdb_databricks_iceberg"
    assert c.mechanism is Mechanism.SCAN
    assert c.reads_in_place is True
    # ADDS a live reach alongside DIRECT, never replaces it (REQ-987 stays intact).
    assert c.reach_modes == frozenset({Mechanism.SCAN, Mechanism.DIRECT})
    assert c._reader == "iceberg_scan"


def test_runtime_deps_document_static_linked_libs():
    deps = PgDuckdbDatabricksIcebergConnector().runtime_deps
    assert any("libduckdb" in d.lib for d in deps)
    assert any("aws-sdk-cpp" in d.lib for d in deps)


# ---- resolver-backed attach payload (REQ-1867) ------------------------------


def test_details_resolves_metadata_location_and_emits_iceberg_scan():
    src = _src("dbx_orders")
    with patch(
        "provisa.federation.databricks_iceberg_resolver.resolve_databricks_iceberg_metadata_location",
        return_value="s3://bucket/workspace/sales/orders/metadata/00003-abc.metadata.json",
    ) as mock_resolve:
        details = PgDuckdbDatabricksIcebergConnector().details(src)

    mock_resolve.assert_called_once_with(src)
    scan = details["scan"]
    assert scan.startswith(
        "iceberg_scan('s3://bucket/workspace/sales/orders/metadata/00003-abc.metadata.json'"
    )
    assert "allow_moved_paths := true" in scan
    assert details["requires_preload"] == "pg_duckdb"
    assert details["reader"] == "iceberg_scan"


def test_details_propagates_resolver_errors_loud():
    src = _src("dbx_orders", federation_hints={})  # missing schema/table
    with pytest.raises(ValueError, match="schema.*table"):
        PgDuckdbDatabricksIcebergConnector().details(src)


# ---- two-stage probe (REQ-904/1867) -----------------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_preloaded_installed_and_iceberg_registered():
    r = await PgDuckdbDatabricksIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=True)
    )
    assert r.available is True
    assert "iceberg" in r.reason


@pytest.mark.asyncio
async def test_probe_unavailable_when_pg_duckdb_lacks_iceberg_extension():
    r = await PgDuckdbDatabricksIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=False)
    )
    assert r.available is False
    assert "iceberg" in r.reason.lower()
    assert r.remediation and "iceberg" in r.remediation.lower()


@pytest.mark.asyncio
async def test_probe_unavailable_when_not_preloaded():
    r = await PgDuckdbDatabricksIcebergConnector().probe(
        _FakeFetch(preloaded=False, installed=True, iceberg=True)
    )
    assert r.available is False
    assert "shared_preload_libraries" in r.reason


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
