# Copyright (c) 2026 Kenneth Stott
# Canary: 2f2500bd-83d1-415b-9f2a-f848ff7d9ab9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1635: the Snowflake Horizon Catalog adapter. Uses a locally-constructed duck-typed
stand-in for ``DataProductAsset`` (REQ-1634) exposing the contract the adapter reads: ``id``,
``name``, ``description``, ``members: list[AssetRef]``."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    GovernanceSignal,
    GovernanceTag,
    TableAsset,
)
from provisa.api.metadata_export.registry import registered_providers
from provisa.api.metadata_export.snowflake_horizon import (
    SnowflakeHorizonExport,
    _object_kind,
    _table_exists,
    comment_statements,
    listing_statements,
    physical_parts,
    physical_table_and_column,
    share_statements,
)


@dataclass
class _FakeDataProduct:
    id: str
    name: str
    description: str
    members: list[AssetRef] = field(default_factory=list)
    support_contact: str | None = "data-team@example.com"
    publish: bool = False


def _table_ref(source_id: str, schema: str, table: str) -> AssetRef:
    return AssetRef(kind=AssetKind.TABLE, parts=(source_id, schema, table))


def _column_ref(source_id: str, schema: str, table: str, column: str) -> AssetRef:
    return AssetRef(kind=AssetKind.COLUMN, parts=(source_id, schema, table, column))


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


def test_share_statements_grants_reference_usage_for_cross_database_member():
    # REQ-1637's exposure view lives in the member's per-source database but SELECTs from the
    # landing database. Snowflake refuses to share such a view without REFERENCE_USAGE on the
    # landing database — but a share has exactly one database eligible for USAGE (its primary,
    # the member's own database); a second USAGE grant on the landing database is rejected
    # ("Database 'landing' does not belong to the database that is being shared"), so the landing
    # database gets REFERENCE_USAGE only, after the member's USAGE grant and before SELECT.
    tables = [("pet_store_sqlite", "pet_store", "pets")]
    stmts = share_statements(
        "provisa_pet_health_share", "Pet Health", tables, landing_database="landing"
    )
    ref_grants = [s for s in stmts if s.startswith("GRANT REFERENCE_USAGE")]
    assert ref_grants == [
        'GRANT REFERENCE_USAGE ON DATABASE "landing" TO SHARE "provisa_pet_health_share";'
    ]
    assert not any(
        s == 'GRANT USAGE ON DATABASE "landing" TO SHARE "provisa_pet_health_share";' for s in stmts
    )
    member_usage_grant = (
        'GRANT USAGE ON DATABASE "pet_store_sqlite" TO SHARE "provisa_pet_health_share";'
    )
    assert member_usage_grant in stmts
    assert stmts.index(member_usage_grant) < stmts.index(ref_grants[0])
    assert stmts.index(ref_grants[0]) < stmts.index(
        next(s for s in stmts if s.startswith("GRANT SELECT"))
    )


def test_share_statements_skips_reference_usage_when_member_is_the_landing_database():
    tables = [("landing", "public", "customers")]
    stmts = share_statements(
        "provisa_c360_share", "Customer 360", tables, landing_database="landing"
    )
    assert not any(s.startswith("GRANT REFERENCE_USAGE") for s in stmts)


def test_listing_statements_creates_draft_organization_listing_over_the_share():
    stmts = listing_statements(
        "provisa_c360_listing",
        "provisa_c360_share",
        "customer_360",
        "desc",
        account="ACME1",
        role="ACCOUNTADMIN",
        region="AZURE_EASTUS2",
        support_contact="data-team@example.com",
    )
    assert len(stmts) == 1
    stmt = stmts[0]
    assert stmt.startswith('CREATE ORGANIZATION LISTING IF NOT EXISTS "provisa_c360_listing"')
    assert 'SHARE "provisa_c360_share"' in stmt
    assert "PUBLISH = FALSE;" in stmt
    assert 'organization_profile: "INTERNAL"' in stmt
    assert 'account: "ACME1"' in stmt
    assert 'roles:\n    - "ACCOUNTADMIN"' in stmt
    assert 'name: "PUBLIC.AZURE_EASTUS2"' in stmt
    assert 'support_contact: "data-team@example.com"' in stmt
    assert 'approver_contact: "data-team@example.com"' in stmt
    assert "PUBLISH = FALSE;" in stmt


def test_listing_statements_publish_true_sets_publish_true():
    stmt = listing_statements(
        "provisa_c360_listing",
        "provisa_c360_share",
        "customer_360",
        "desc",
        account="ACME1",
        role="ACCOUNTADMIN",
        region="AZURE_EASTUS2",
        support_contact="data-team@example.com",
        publish=True,
    )[0]
    assert "PUBLISH = TRUE;" in stmt


def test_physical_table_and_column_resolves_table_ref():
    ref = _table_ref("petstore-api", "public", "pets")
    assert physical_table_and_column(ref) == (("petstore_api", "public", "pets"), None)


def test_physical_table_and_column_resolves_column_ref():
    ref = _column_ref("petstore-api", "public", "pets", "owner_ssn")
    assert physical_table_and_column(ref) == (("petstore_api", "public", "pets"), "owner_ssn")


def test_physical_table_and_column_rejects_other_ref_kinds():
    ref = AssetRef(kind=AssetKind.SOURCE, parts=("petstore-api",))
    with pytest.raises(ValueError):
        physical_table_and_column(ref)


def _table_asset(
    source_id: str,
    schema: str,
    table: str,
    description: str = "",
    columns: list[ColumnAsset] | None = None,
) -> TableAsset:
    return TableAsset(
        ref=_table_ref(source_id, schema, table),
        name=table,
        source_id=source_id,
        domain_id=None,
        description=description,
        columns=columns or [],
    )


def test_comment_statements_sets_table_comment_on_a_real_table():
    table = _table_asset("petstore-api", "public", "pets", description="Pets for sale")
    kinds = {("petstore_api", "public", "pets"): "TABLE"}
    stmts = comment_statements([table], kinds, [])
    assert stmts == ['ALTER TABLE "petstore_api"."public"."pets" SET COMMENT = \'Pets for sale\';']


def test_comment_statements_sets_view_comment_via_alter_view():
    table = _table_asset("petstore-api", "public", "pets", description="Pets for sale")
    kinds = {("petstore_api", "public", "pets"): "VIEW"}
    stmts = comment_statements([table], kinds, [])
    assert stmts == ['ALTER VIEW "petstore_api"."public"."pets" SET COMMENT = \'Pets for sale\';']


def test_comment_statements_sets_column_comment_via_modify_column_on_a_table():
    column = ColumnAsset(
        ref=_column_ref("petstore-api", "public", "pets", "name"),
        name="name",
        data_type="text",
        description="Pet name",
    )
    table = _table_asset("petstore-api", "public", "pets", columns=[column])
    kinds = {("petstore_api", "public", "pets"): "TABLE"}
    stmts = comment_statements([table], kinds, [])
    assert stmts == [
        'ALTER TABLE "petstore_api"."public"."pets" MODIFY COLUMN "name" COMMENT \'Pet name\';'
    ]


def test_comment_statements_sets_column_comment_via_alter_column_on_a_view():
    column = ColumnAsset(
        ref=_column_ref("petstore-api", "public", "pets", "name"),
        name="name",
        data_type="text",
        description="Pet name",
    )
    table = _table_asset("petstore-api", "public", "pets", columns=[column])
    kinds = {("petstore_api", "public", "pets"): "VIEW"}
    stmts = comment_statements([table], kinds, [])
    assert stmts == [
        'ALTER VIEW "petstore_api"."public"."pets" ALTER COLUMN "name" COMMENT \'Pet name\';'
    ]


def test_comment_statements_skips_tables_missing_from_kinds():
    table = _table_asset("petstore-api", "public", "pets", description="Pets for sale")
    assert comment_statements([table], {}, []) == []


def test_comment_statements_skips_columns_and_tables_with_no_description():
    column = ColumnAsset(
        ref=_column_ref("petstore-api", "public", "pets", "name"),
        name="name",
        data_type="text",
        description="",
    )
    table = _table_asset("petstore-api", "public", "pets", columns=[column])
    kinds = {("petstore_api", "public", "pets"): "TABLE"}
    assert comment_statements([table], kinds, []) == []


def test_comment_statements_appends_governance_note_to_table_comment():
    table = _table_asset("petstore-api", "public", "pets", description="Pets for sale")
    kinds = {("petstore_api", "public", "pets"): "TABLE"}
    tags = [
        GovernanceTag(
            asset=_table_ref("petstore-api", "public", "pets"),
            signal=GovernanceSignal.RLS_RESTRICTED,
            rule_id="rule-42",
            restricted_roles=("analyst",),
            exempt_roles=("owner",),
        )
    ]
    stmts = comment_statements([table], kinds, tags)
    assert stmts == [
        'ALTER TABLE "petstore_api"."public"."pets" SET COMMENT = '
        "'Pets for sale\n\n"
        "[provisa:governance rls_restricted rule=rule-42 restricted=analyst exempt=owner]';"
    ]


def test_comment_statements_uses_governance_note_alone_when_no_description():
    table = _table_asset("petstore-api", "public", "pets", description="")
    kinds = {("petstore_api", "public", "pets"): "TABLE"}
    tags = [
        GovernanceTag(
            asset=_table_ref("petstore-api", "public", "pets"),
            signal=GovernanceSignal.MASKED,
            rule_id="rule-7",
        )
    ]
    stmts = comment_statements([table], kinds, tags)
    assert stmts == [
        'ALTER TABLE "petstore_api"."public"."pets" SET COMMENT = '
        "'[provisa:governance masked rule=rule-7]';"
    ]


class _FakeCursor:
    def __init__(self, existing_objects: bool = True, kind: str = "TABLE"):
        self.sql: list[str] = []
        self._existing_objects = existing_objects
        self._kind = kind
        self._last_sql = ""
        self.description: list[tuple[str, ...]] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self._last_sql = sql
        if sql.startswith("SHOW OBJECTS"):
            self.description = [("name",), ("kind",)]
        else:
            self.description = []

    def fetchall(self):
        return [("x",)] if self._existing_objects else []

    def fetchone(self):
        if self._last_sql.startswith("SHOW OBJECTS"):
            return ("x", self._kind) if self._existing_objects else None
        return ("ACME1", "ACCOUNTADMIN", "AZURE_EASTUS2")

    def close(self):
        pass


class _FakeConn:
    def __init__(self, existing_objects: bool = True, kind: str = "TABLE"):
        self.cursor_obj = _FakeCursor(existing_objects, kind)

    def cursor(self):
        return self.cursor_obj


def _runtime(existing_objects: bool = True, database: str = "landing", kind: str = "TABLE"):
    rt = object.__new__(
        __import__(
            "provisa.federation.snowflake_runtime", fromlist=["SnowflakeFederationRuntime"]
        ).SnowflakeFederationRuntime
    )
    rt._conn = _FakeConn(existing_objects, kind)
    rt._database = database
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
    assert 'CREATE ORGANIZATION LISTING IF NOT EXISTS "provisa_c360_listing"' in joined
    assert 'organization_profile: "INTERNAL"' in joined
    assert 'support_contact: "data-team@example.com"' in joined
    assert "PUBLISH = FALSE;" in joined


def test_publish_creates_listing_with_publish_true_when_product_opts_in(monkeypatch):
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
        "c360",
        "customer_360",
        "Customer 360",
        [_table_ref("petstore-api", "public", "customers")],
        publish=True,
    )
    snapshot = SimpleNamespace(data_products=[product])
    result = asyncio.run(_exporter().publish(snapshot))
    assert result.ok
    joined = " | ".join(rt._conn.cursor_obj.sql)
    assert "PUBLISH = TRUE;" in joined


def test_publish_reports_error_when_support_contact_missing(monkeypatch):
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
        "c360",
        "customer_360",
        "Customer 360",
        [_table_ref("petstore-api", "public", "customers")],
        support_contact=None,
    )
    snapshot = SimpleNamespace(data_products=[product])
    result = asyncio.run(_exporter().publish(snapshot))
    assert not result.ok
    assert "support_contact" in result.errors[0].message


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


def test_publish_is_a_noop_when_snapshot_has_no_products_or_tags(monkeypatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "snowflake://user:pass@acct/db/schema",
    )
    snapshot = SimpleNamespace(data_products=[], governance_tags=[])
    result = asyncio.run(_exporter().publish(snapshot))
    assert result.ok
    assert result.total_published() == 0


def test_publish_reports_error_when_governance_tag_has_no_matching_table_asset(monkeypatch):
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
    snapshot = SimpleNamespace(
        data_products=[],
        tables=[],
        governance_tags=[
            GovernanceTag(
                asset=_table_ref("petstore-api", "public", "pets"),
                signal=GovernanceSignal.MASKED,
                rule_id="rule-1",
            )
        ],
        model_tags=[],
    )
    result = asyncio.run(_exporter().publish(snapshot))
    assert not result.ok
    assert "governed asset" in result.errors[0].message
    assert result.total_published() == 0


def test_object_kind_returns_table_when_object_is_a_real_table():
    rt = _runtime(existing_objects=True, kind="TABLE")
    assert _object_kind(rt, ("landing", "mat", "pets")) == "TABLE"


def test_object_kind_returns_view_when_object_is_a_view():
    rt = _runtime(existing_objects=True, kind="VIEW")
    assert _object_kind(rt, ("petstore_api", "public", "pets")) == "VIEW"


def test_object_kind_returns_none_when_object_does_not_exist():
    rt = _runtime(existing_objects=False)
    assert _object_kind(rt, ("petstore_api", "public", "pets")) is None


def test_table_exists_delegates_to_object_kind():
    assert _table_exists(_runtime(existing_objects=True, kind="TABLE"), ("landing", "mat", "pets"))
    assert not _table_exists(_runtime(existing_objects=False), ("landing", "mat", "pets"))


def test_publish_applies_descriptions_to_a_view(monkeypatch):
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.configured_engine_url",
        lambda: "snowflake://user:pass@acct/db/schema",
    )
    rt = _runtime(existing_objects=True, kind="VIEW")
    monkeypatch.setattr(
        "provisa.api.metadata_export.snowflake_horizon.SnowflakeFederationRuntime",
        lambda url: rt,
    )
    monkeypatch.setattr(rt, "close", lambda: None)
    table = _table_asset("petstore-api", "public", "pets", description="Pets for sale")
    snapshot = SimpleNamespace(data_products=[], governance_tags=[], model_tags=[], tables=[table])
    result = asyncio.run(_exporter().publish(snapshot))
    assert result.ok
    assert result.published["descriptions"] == 1
    joined = " | ".join(rt._conn.cursor_obj.sql)
    assert 'ALTER VIEW "petstore_api"."public"."pets" SET COMMENT = \'Pets for sale\';' in joined


def test_publish_reports_error_when_described_table_not_landed(monkeypatch):
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
    table = _table_asset("petstore-api", "public", "pets", description="Pets for sale")
    snapshot = SimpleNamespace(data_products=[], governance_tags=[], model_tags=[], tables=[table])
    result = asyncio.run(_exporter().publish(snapshot))
    assert not result.ok
    assert "landing terminal missing" in result.errors[0].message


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
