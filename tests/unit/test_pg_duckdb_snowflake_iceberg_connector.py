# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2f6c93-4e18-4d70-b6c2-9f0e3a15d84b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1867: PgDuckdbSnowflakeIcebergConnector — a Snowflake-managed Iceberg table
attached IN PLACE via pg_duckdb's iceberg_scan, resolved to the table's LIVE
metadata_location through SYSTEM$GET_ICEBERG_TABLE_INFORMATION instead of a bare
storage path.

Pure logic: ``resolve_snowflake_iceberg_metadata_location`` (the live Snowflake call)
is monkeypatched, and the async ``probe(fetch)`` is driven by a fake fetch callable —
no live Postgres, no live Snowflake.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism
from provisa.federation.connector_duckdb import PgDuckdbSnowflakeIcebergConnector


def _src(sid: str, **kw) -> Source:
    return Source(
        id=sid, type=SourceType.snowflake, host=kw.pop("host", "acct.snowflakecomputing.com"), **kw
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


# ---- identity / reach modes (REQ-1867/947/951) -------------------------------


def test_snowflake_iceberg_connector_identity_and_reader():
    c = PgDuckdbSnowflakeIcebergConnector()
    assert c.engine == "postgres"
    assert c.source_type == "snowflake"
    assert c.key == "pg_duckdb_snowflake_iceberg"
    assert c.mechanism is Mechanism.SCAN
    assert c._reader == "iceberg_scan"


def test_snowflake_iceberg_reach_modes_include_direct_as_additional_reach():
    c = PgDuckdbSnowflakeIcebergConnector()
    assert c.reach_modes == frozenset({Mechanism.SCAN, Mechanism.DIRECT})
    assert c.reads_in_place is True  # SCAN is LIVE_IN_PLACE even though DIRECT is also declared


def test_snowflake_iceberg_runtime_deps_include_snowflake_connector():
    deps = PgDuckdbSnowflakeIcebergConnector().runtime_deps
    assert any("libduckdb" in d.lib for d in deps)
    assert any("snowflake-connector-python" in d.lib for d in deps)


# ---- attach payload (REQ-1867) -----------------------------------------------


def test_details_resolves_live_metadata_location(monkeypatch):
    monkeypatch.setattr(
        "provisa.federation.connector_duckdb.resolve_snowflake_iceberg_metadata_location",
        lambda source: "s3://bucket/warehouse/db/schema/tbl/metadata/00003-abc.metadata.json",
    )
    source = _src(
        "sf_lake",
        username="u",
        password="p",
        database="DB",
        federation_hints={"account": "acct", "iceberg_table": "DB.SCHEMA.TBL", "warehouse": "wh"},
    )
    details = PgDuckdbSnowflakeIcebergConnector().details(source)
    scan = details["scan"]
    assert scan.startswith(
        "iceberg_scan('s3://bucket/warehouse/db/schema/tbl/metadata/00003-abc.metadata.json'"
    )
    assert "allow_moved_paths := true" in scan
    assert details["requires_preload"] == "pg_duckdb"
    assert details["reader"] == "iceberg_scan"


# ---- resolver (REQ-1867) ------------------------------------------------------


def test_resolver_raises_loud_when_iceberg_table_hint_missing():
    from provisa.federation.connector_duckdb import resolve_snowflake_iceberg_metadata_location

    source = _src("sf_lake", username="u", password="p", federation_hints={"account": "acct"})
    with pytest.raises(ValueError, match="iceberg_table"):
        resolve_snowflake_iceberg_metadata_location(source)


def test_resolver_raises_loud_when_account_missing():
    from provisa.federation.connector_duckdb import resolve_snowflake_iceberg_metadata_location

    source = Source(
        id="sf_lake",
        type=SourceType.snowflake,
        username="u",
        password="p",
        federation_hints={"iceberg_table": "DB.SCHEMA.TBL"},
    )
    with pytest.raises(ValueError, match="account"):
        resolve_snowflake_iceberg_metadata_location(source)


# ---- three-stage probe (REQ-904/908/1867) -------------------------------------


@pytest.mark.asyncio
async def test_probe_available_when_preloaded_installed_iceberg_and_snowflake_lib_present():
    r = await PgDuckdbSnowflakeIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=True)
    )
    assert r.available is True


@pytest.mark.asyncio
async def test_probe_unavailable_when_pg_duckdb_lacks_iceberg_extension():
    r = await PgDuckdbSnowflakeIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=False)
    )
    assert r.available is False
    assert "iceberg" in r.reason.lower()


@pytest.mark.asyncio
async def test_probe_unavailable_when_snowflake_connector_not_importable(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "snowflake.connector" or name.startswith("snowflake"):
            raise ImportError("no module named snowflake")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    r = await PgDuckdbSnowflakeIcebergConnector().probe(
        _FakeFetch(preloaded=True, installed=True, iceberg=True)
    )
    assert r.available is False
    assert "snowflake-connector-python" in r.reason


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
