# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1635: the Snowflake Horizon Catalog adapter. ``MetadataSnapshot.data_products`` does not
exist yet (REQ-1634 Phase 4 lands it separately), so these tests use a locally-constructed
duck-typed stand-in exposing the contract the adapter documents: ``id``, ``name``, ``description``,
``member_tables: list[AssetRef]``."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from provisa.api.metadata_export.model import AssetKind, AssetRef
from provisa.api.metadata_export.registry import registered_providers
from provisa.api.metadata_export.snowflake_horizon import (
    SnowflakeHorizonExport,
    listing_statements,
    physical_parts,
    share_statements,
)


@dataclass
class _FakeDataProduct:
    id: str
    name: str
    description: str
    member_tables: list[AssetRef] = field(default_factory=list)


def _table_ref(source_id: str, schema: str, table: str) -> AssetRef:
    return AssetRef(kind=AssetKind.TABLE, parts=(source_id, schema, table))


def test_registered_under_its_provider_name():
    assert "snowflake_horizon" in registered_providers()


def test_physical_parts_resolves_source_id_to_catalog_name():
    ref = _table_ref("petstore-api", "public", "pets")
    assert physical_parts(ref) == ("petstore_api", "public", "pets")


def test_physical_parts_rejects_non_table_ref():
    ref = AssetRef(kind=AssetKind.SOURCE, parts=("petstore-api",))
    with pytest.raises(ValueError):
        physical_parts(ref)


def test_share_statements_grant_once_per_database_and_schema():
    tables = [("db", "public", "customers"), ("db", "public", "orders")]
    stmts = share_statements("provisa_c360_share", "Customer 360", tables)
    assert stmts[0].startswith('CREATE SHARE IF NOT EXISTS "provisa_c360_share"')
    assert sum(s.startswith("GRANT USAGE ON DATABASE") for s in stmts) == 1
    assert sum(s.startswith("GRANT USAGE ON SCHEMA") for s in stmts) == 1
    assert sum(s.startswith("GRANT SELECT ON TABLE") for s in stmts) == 2


def test_listing_statements_publish_over_the_share():
    stmts = listing_statements("provisa_c360_listing", "provisa_c360_share", "customer_360", "desc")
    assert 'FOR SHARE "provisa_c360_share"' in stmts[0]
    assert stmts[1] == 'ALTER LISTING "provisa_c360_listing" PUBLISH;'


class _FakeCursor:
    def __init__(self, existing_objects: bool = True):
        self.sql: list[str] = []
        self._existing_objects = existing_objects

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchall(self):
        return [("x",)] if self._existing_objects else []

    def close(self):
        pass


class _FakeConn:
    def __init__(self, existing_objects: bool = True):
        self.cursor_obj = _FakeCursor(existing_objects)

    def cursor(self):
        return self.cursor_obj


def _runtime(existing_objects: bool = True):
    rt = object.__new__(
        __import__(
            "provisa.federation.snowflake_runtime", fromlist=["SnowflakeFederationRuntime"]
        ).SnowflakeFederationRuntime
    )
    rt._conn = _FakeConn(existing_objects)
    return rt


def _exporter() -> SnowflakeHorizonExport:
    config = SimpleNamespace(enabled=True, provider="snowflake_horizon")
    return SnowflakeHorizonExport(config)  # type: ignore[arg-type]


def test_publish_is_a_noop_when_engine_is_not_snowflake(monkeypatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "duckdb:///local",
    )
    snapshot = SimpleNamespace(
        data_products=[
            _FakeDataProduct("c360", "customer_360", "desc", [_table_ref("s", "p", "t")])
        ]
    )
    result = asyncio.run(_exporter().publish(snapshot))
    assert result.ok
    assert result.total_published() == 0


def test_publish_is_a_noop_when_snapshot_has_no_data_products(monkeypatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "snowflake://user:pass@acct/db/schema",
    )
    snapshot = SimpleNamespace(data_products=[])
    result = asyncio.run(_exporter().publish(snapshot))
    assert result.ok
    assert result.total_published() == 0


def test_publish_creates_share_and_listing_for_each_data_product(monkeypatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "snowflake://user:pass@acct/db/schema",
    )
    rt = _runtime(existing_objects=True)
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.SnowflakeFederationRuntime",
        lambda url: rt,
    )
    monkeypatch.setattr(rt, "close", lambda: None)
    product = _FakeDataProduct(
        "c360", "customer_360", "Customer 360", [_table_ref("petstore-api", "public", "customers")]
    )
    snapshot = SimpleNamespace(data_products=[product])
    result = asyncio.run(_exporter().publish(snapshot))
    assert result.ok
    assert result.published["data_products"] == 1
    joined = " | ".join(rt._conn.cursor_obj.sql)
    assert 'CREATE SHARE IF NOT EXISTS "provisa_c360_share"' in joined
    assert (
        'GRANT SELECT ON TABLE "petstore_api"."public"."customers" TO SHARE "provisa_c360_share"'
        in joined
    )
    assert 'CREATE OR REPLACE LISTING "provisa_c360_listing"' in joined
    assert 'ALTER LISTING "provisa_c360_listing" PUBLISH;' in joined


def test_publish_reports_error_when_member_table_not_landed(monkeypatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "snowflake://user:pass@acct/db/schema",
    )
    rt = _runtime(existing_objects=False)
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.SnowflakeFederationRuntime",
        lambda url: rt,
    )
    monkeypatch.setattr(rt, "close", lambda: None)
    product = _FakeDataProduct(
        "c360", "customer_360", "Customer 360", [_table_ref("petstore-api", "public", "customers")]
    )
    snapshot = SimpleNamespace(data_products=[product])
    result = asyncio.run(_exporter().publish(snapshot))
    assert not result.ok
    assert "landing terminal missing" in result.errors[0].message


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
