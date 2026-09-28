# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2f6c93-4e18-4d70-b6c2-9f0e3a15d84b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1867: PgDuckdbBigQueryIcebergConnector — live BigLake Metastore Iceberg REST Catalog
resolution, the iceberg_scan attach payload it feeds, and the three-stage probe (preload +
iceberg_scan registered + google-auth importable).

Pure logic — ``_resolve_biglake_metadata_location`` is monkeypatched so no real GCP/HTTP call is
made, and the async ``probe(fetch)`` is driven by a fake fetch callable; no live Postgres.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgDuckdbBigQueryIcebergConnector


def _src(sid: str, **kw) -> Source:
    return Source(id=sid, type=SourceType.bigquery, database=kw.pop("database", "my-project"), **kw)


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
    c = PgDuckdbBigQueryIcebergConnector()
    assert c.engine == "postgres"
    assert c.source_type == "bigquery"
    assert c.key == "pg_duckdb_bigquery_iceberg"
    assert c.mechanism is Mechanism.ATTACH_R
    assert c.reach_modes == frozenset({Mechanism.ATTACH_R, Mechanism.DIRECT})
    assert c.reads_in_place is True  # ATTACH_R is a LIVE_IN_PLACE mode


def test_runtime_deps_document_static_linked_libs_and_google_auth():
    deps = PgDuckdbBigQueryIcebergConnector().runtime_deps
    assert any("libduckdb" in d.lib for d in deps)
    assert any("aws-sdk-cpp" in d.lib for d in deps)
    assert any("google-auth" in d.lib for d in deps)


# ---- attach payload (REQ-1867) ----------------------------------------------


def test_details_resolve_live_metadata_location_and_emit_iceberg_scan(monkeypatch):
    monkeypatch.setattr(
        "provisa.federation.connector_duckdb._resolve_biglake_metadata_location",
        lambda source: "gs://bucket/warehouse/ns/tbl/metadata/00042-abc.metadata.json",
    )
    details = PgDuckdbBigQueryIcebergConnector().details(
        _src(
            "bq_lake",
            federation_hints={
                "biglake_catalog": "c",
                "biglake_namespace": "ns",
                "biglake_table": "tbl",
            },
        )
    )
    scan = details["scan"]
    assert scan.startswith(
        "iceberg_scan('gs://bucket/warehouse/ns/tbl/metadata/00042-abc.metadata.json'"
    )
    assert "allow_moved_paths := true" in scan
    assert details["requires_preload"] == "pg_duckdb"
    assert details["reader"] == "iceberg_scan"


def test_resolve_raises_when_project_missing():
    from provisa.federation.connector_duckdb import _resolve_biglake_metadata_location

    src = Source(id="bq_lake", type=SourceType.bigquery)
    with pytest.raises(ValueError, match="project"):
        _resolve_biglake_metadata_location(src)


def test_resolve_raises_when_biglake_catalog_missing():
    from provisa.federation.connector_duckdb import _resolve_biglake_metadata_location

    src = Source(id="bq_lake", type=SourceType.bigquery, database="my-project")
    with pytest.raises(ValueError, match="biglake_catalog"):
        _resolve_biglake_metadata_location(src)


def test_resolve_calls_rest_catalog_with_bearer_token(monkeypatch):
    """The resolver authenticates via google.auth (reusing the BigQuery driver's ADC convention) and
    reads ``metadata-location`` from the Iceberg REST Catalog's LoadTable response."""
    from provisa.federation.connector_duckdb import _resolve_biglake_metadata_location

    class _FakeCredentials:
        token = "fake-token"

        def refresh(self, request):
            del request

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "metadata-location": "gs://bucket/warehouse/ns/tbl/metadata/00007.metadata.json"
            }

    captured = {}

    def _fake_get(url, headers):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeResponse()

    monkeypatch.setattr("google.auth.default", lambda scopes: (_FakeCredentials(), None))
    monkeypatch.setattr("httpx.get", _fake_get)

    src = Source(
        id="bq_lake",
        type=SourceType.bigquery,
        database="my-project",
        federation_hints={
            "biglake_catalog": "my_catalog",
            "biglake_namespace": "my_ns",
            "biglake_table": "my_tbl",
        },
    )
    location = _resolve_biglake_metadata_location(src)
    assert location == "gs://bucket/warehouse/ns/tbl/metadata/00007.metadata.json"
    assert (
        "projects/my-project/catalogs/my_catalog/namespaces/my_ns/tables/my_tbl" in captured["url"]
    )
    assert captured["headers"]["Authorization"] == "Bearer fake-token"


# ---- three-stage probe (REQ-904 / REQ-1867) ---------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_preloaded_installed_iceberg_and_google_auth_present():
    r = await PgDuckdbBigQueryIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=True)
    )
    assert r.available is True
    assert "google-auth" in r.reason


@pytest.mark.asyncio
async def test_probe_unavailable_when_pg_duckdb_lacks_iceberg_extension():
    r = await PgDuckdbBigQueryIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=False)
    )
    assert r.available is False
    assert "iceberg" in r.reason.lower()


@pytest.mark.asyncio
async def test_probe_unavailable_when_not_preloaded():
    r = await PgDuckdbBigQueryIcebergConnector().probe(
        _FakeFetch(preloaded=False, installed=True, iceberg=True)
    )
    assert r.available is False
    assert "shared_preload_libraries" in r.reason


@pytest.mark.asyncio
async def test_probe_unavailable_when_google_auth_not_importable(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _blocked_import(name, *args, **kwargs):
        if name == "google.auth" or name.startswith("google.auth"):
            raise ImportError("no google-auth")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)
    r = await PgDuckdbBigQueryIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=True)
    )
    assert r.available is False
    assert "google-auth" in r.reason


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
