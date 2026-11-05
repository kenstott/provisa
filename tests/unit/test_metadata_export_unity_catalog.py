# Copyright (c) 2026 Kenneth Stott
# Canary: 30ca6bac-eb42-463d-a76a-bd3a4c9496be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2117: the Unity Catalog target, against a stand-in Databricks warehouse.

A data product becomes a schema of the engine's catalog, ``<catalog>.<product id>``: its prose in
the schema's comment, its owner, team, version and status as tags, and one view per member over
the member's replica -- nothing copied -- each view described and its governance facts tagged by
rule, never by rule body. A former member's view is dropped; with ``delta_share`` the views are
shared, and a member not in Delta is reported and left out."""

# Requirements: REQ-2117

from __future__ import annotations

from typing import Any

import pytest

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.registry import metadata_export
from provisa.api.metadata_export.unity_catalog import UnityCatalogExport, schema_tags
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    MetadataExportTarget,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)

pytestmark = pytest.mark.unit

CATALOG = "main"
REPLICAS = "org_acme__replicas"


class _Cursor:
    """Records each statement; answers the information_schema reads from what it holds."""

    def __init__(self, warehouse: "_Warehouse") -> None:
        self.warehouse = warehouse
        self._rows: list[tuple] = []
        self.description: list[tuple] = []

    def execute(self, sql: str) -> None:
        self.warehouse.statements.append(sql)
        self._rows = self.warehouse.answer(sql)
        # SHOW ALL IN SHARE names the share-relative object, then the full name.
        self.description = (
            [("name",), ("type",), ("shared_object",)] if "SHOW ALL IN SHARE" in sql else []
        )

    def fetchall(self) -> list[tuple]:
        return self._rows

    def close(self) -> None:
        pass


class _Warehouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.views: list[str] = []
        self.shared: list[str] = []
        self.schema_tags: list[str] = []
        self.formats: dict[str, str] = {}

    def answer(self, sql: str) -> list[tuple]:
        if "information_schema.schema_tags" in sql:
            return [(t,) for t in self.schema_tags]
        if "information_schema.views" in sql:
            return [(v,) for v in self.views]
        if "SHOW ALL IN SHARE" in sql:
            return [(s.split(".", 1)[1], "VIEW", s) for s in self.shared]
        if "data_source_format" in sql:
            table = sql.split("table_name = '")[1].split("'")[0]
            return [(self.formats.get(table, "DELTA"),)]
        return []

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def close(self) -> None:
        pass


class _Runtime:
    def __init__(self, warehouse: _Warehouse) -> None:
        self.warehouse = warehouse

    def ensure_materialize_attached(self) -> str:
        return CATALOG

    def _open(self) -> _Warehouse:
        return self.warehouse

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        return (source.id, source.schema_name, source.table_name)

    def close(self) -> None:
        pass


def _snapshot(*, tables: list[Table] | None = None, version: str | None = "1.2.0"):
    config = ProvisaConfig(
        sources=[
            Source(
                id="pg",
                type=SourceType.postgresql,
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )
        ],
        domains=[Domain(id="sales", description="Sales")],
        tables=tables
        if tables is not None
        else [
            Table(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name=name,
                description=f"{name} of the shop",
                product_id="customer_360",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["admin"], description="Key"),
                    Column(
                        name="ssn",
                        data_type="text",
                        visible_to=["admin"],
                        masks=[
                            {
                                "mask": {"type": "regex", "pattern": "(.*)", "replace": "X"},
                            }
                        ],
                    ),
                ],
            )
            for name in ("orders", "customers")
        ],
        data_products=[
            DataProduct(
                id="customer_360",
                domain_id="sales",
                name="Customer 360",
                owner_role="data_owner",
                team_role="analyst",
                purpose="One view of each customer",
                limitations="No refunds",
                version=version,
                status="live",
            )
        ],
        roles=[],
    )
    return build_snapshot(config, org_id="acme", dialect="databricks", contexts={})


@pytest.fixture
def warehouse(monkeypatch):
    built = _Warehouse()
    monkeypatch.setattr(UnityCatalogExport, "_runtime_or_none", lambda self: _Runtime(built))
    monkeypatch.setattr(
        "provisa.federation.replica_address.replica_schema", lambda org_id, **_: REPLICAS
    )
    return built


def _export(**extra: Any) -> UnityCatalogExport:
    exporter = metadata_export(MetadataExportTarget(id="uc", provider="unity_catalog", **extra))
    assert isinstance(exporter, UnityCatalogExport)
    # Both members are served from their replicas (REQ-1912).
    exporter.export_views = {
        ("pg", "public", "orders"): ("x", "y"),
        ("pg", "public", "customers"): ("x", "y"),
    }
    return exporter


def test_a_unity_catalog_target_needs_no_endpoint():
    target = MetadataExportTarget(id="uc", provider="unity_catalog")
    assert target.endpoint == "" and target.delta_share is False


async def test_a_product_is_a_schema_of_views_over_its_members_replicas(warehouse):
    snapshot = _snapshot()
    result = await _export().publish(snapshot)
    assert result.ok, result.errors
    assert result.published == {"data_products": 1, "views": 2}
    sql = warehouse.statements
    assert "CREATE SCHEMA IF NOT EXISTS `main`.`customer_360`" in sql
    (comment,) = [s for s in sql if s.startswith("COMMENT ON SCHEMA")]
    assert "One view of each customer" in comment and "Limitations: No refunds" in comment
    (orders,) = [s for s in sql if "VIEW `main`.`customer_360`.`orders`" in s and "CREATE" in s]
    # Nothing copied: the view reads the member's replica, each column described.
    assert orders.endswith(
        "AS SELECT `id`, `ssn` FROM `main`.`org_acme__replicas`.`pg__public__orders`"
    )
    assert "`id` COMMENT 'Key'" in orders and "COMMENT 'orders of the shop'" in orders
    # The mask is a fact about the column, named by its rule, never its pattern.
    masked = [s for s in sql if "provisa_governance:masked" in s]
    assert masked and all("(.*)" not in s for s in masked)
    # Databricks tags a view's column through ALTER TABLE; it has no ALTER VIEW ... ALTER COLUMN.
    assert any(
        s.startswith("ALTER TABLE `main`.`customer_360`.`") and "ALTER COLUMN `ssn` SET TAGS" in s
        for s in masked
    )
    assert not any(s.startswith("ALTER VIEW") and "ALTER COLUMN" in s for s in sql)
    (product,) = snapshot.data_products
    assert result.bindings[product.semantic_uri] == ("main.customer_360", "main.customer_360")


def test_the_schemas_tags_are_the_products_short_fields():
    (product,) = _snapshot().data_products
    assert schema_tags(product) == {
        "provisa_product:owner": "data_owner",
        "provisa_product:team": "analyst",
        "provisa_product:version": "1.2.0",
        "provisa_product:status": "live",
    }


async def test_a_tag_the_product_no_longer_has_is_unset_and_a_former_member_dropped(warehouse):
    warehouse.schema_tags = ["provisa_product:version", "someone_elses_tag"]
    warehouse.views = ["orders", "customers", "returns"]
    result = await _export().publish(_snapshot(version=None))
    assert result.ok, result.errors
    assert (
        "ALTER SCHEMA `main`.`customer_360` UNSET TAGS ('provisa_product:version')"
        in warehouse.statements
    )
    assert not any("someone_elses_tag" in s for s in warehouse.statements)
    assert "DROP VIEW IF EXISTS `main`.`customer_360`.`returns`" in warehouse.statements
    assert not any(
        "DROP VIEW IF EXISTS `main`.`customer_360`.`orders`" in s for s in warehouse.statements
    )


async def test_the_share_holds_the_delta_members_and_reports_the_rest(warehouse):
    warehouse.formats = {"pg__public__customers": "PARQUET"}
    warehouse.shared = ["main.customer_360.returns"]
    result = await _export(delta_share=True).publish(_snapshot())
    sql = warehouse.statements
    assert "CREATE SHARE IF NOT EXISTS `provisa_customer_360`" in sql
    assert "ALTER SHARE `provisa_customer_360` ADD VIEW `main`.`customer_360`.`orders`" in sql
    assert not any("ADD VIEW `main`.`customer_360`.`customers`" in s for s in sql)
    assert "ALTER SHARE `provisa_customer_360` REMOVE VIEW `main`.`customer_360`.`returns`" in sql
    assert [e.message for e in result.errors] == [
        "member 'customers' is not in Delta, so it cannot be shared"
    ]
    assert result.published["shares"] == 1


async def test_on_another_engine_nothing_is_published(monkeypatch):
    monkeypatch.setattr(UnityCatalogExport, "_runtime_or_none", lambda self: None)
    result = await _export().publish(_snapshot())
    assert result.ok and result.published == {}
