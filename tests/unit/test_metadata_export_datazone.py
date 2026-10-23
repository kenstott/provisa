# Copyright (c) 2026 Kenneth Stott
# Canary: ad588600-bab9-4b43-b27f-e4a4e9e96c6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: the DataZone target, against botocore's stub of the DataZone API.

A data product becomes a DataZone data product in the target's project, over one asset of the
Provisa table type per member, each published; the form and asset types are created the first
time; a second publish adds revisions to what it finds by the members' Provisa URIs and the
product's name."""

# Requirements: REQ-2118

from __future__ import annotations

import json
from typing import Any

import boto3
import pytest
from botocore.stub import ANY, Stubber
from pydantic import ValidationError

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.datazone import (
    PRODUCT_FORM,
    TABLE_FORM,
    TABLE_TYPE,
    DataZoneExport,
    product_form,
    table_form,
)
from provisa.api.metadata_export.registry import metadata_export
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

DOMAIN = "dzd-1"
ANY_PARAMS = None  # botocore's Stubber: None leaves the request's parameters unchecked
PROJECT = "prj-1"
SCOPE = {"domainIdentifier": DOMAIN}


def _target() -> MetadataExportTarget:
    return MetadataExportTarget(
        id="datazone",
        provider="datazone",
        aws_region="us-east-1",
        datazone_domain_id=DOMAIN,
        datazone_project_id=PROJECT,
        aws_access_key_id="AKIA-TEST",
        aws_secret_access_key="secret",
        timeout_seconds=5,
    )


def _snapshot(*, status="draft", members=True):
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
        tables=[
            Table(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                description="Orders placed",
                product_id="sales_360" if members else None,
                cache_ttl=3600,
                columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
            )
        ],
        data_products=[
            DataProduct(
                id="sales_360",
                domain_id="sales",
                name="Sales 360",
                owner_role="data_owner",
                purpose="Every sale",
                usage="Join on customer_id",
                version="1.2.0",
                status=status,
                status_reason="Replaced." if status in ("deprecated", "retired") else None,
            )
        ],
        roles=[],
    )
    return build_snapshot(config, org_id="acme", dialect="postgres", contexts={})


@pytest.fixture
def stubbed(monkeypatch):
    client = boto3.client(
        "datazone",
        region_name="us-east-1",
        aws_access_key_id="AKIA-TEST",
        aws_secret_access_key="secret",
    )
    stubber = Stubber(client)
    monkeypatch.setattr(DataZoneExport, "_client", lambda self: client)
    with stubber:
        yield stubber
    stubber.assert_no_pending_responses()


def _types_exist(stub: Stubber) -> None:
    for name, rev in ((TABLE_FORM, "3"), (PRODUCT_FORM, "4")):
        stub.add_response(
            "get_form_type",
            {"domainId": DOMAIN, "name": name, "revision": rev, "model": {"smithy": "x"}},
            {**SCOPE, "formTypeIdentifier": name},
        )
    stub.add_response(
        "get_asset_type",
        {
            "domainId": DOMAIN,
            "name": TABLE_TYPE,
            "revision": "5",
            "formsOutput": {},
            "owningProjectId": PROJECT,
        },
        {**SCOPE, "identifier": TABLE_TYPE},
    )


def _search(stub: Stubber, scope: str, items: list[dict[str, Any]]) -> None:
    stub.add_response(
        "search",
        {"items": items},
        {**SCOPE, "owningProjectIdentifier": PROJECT, "searchScope": scope},
    )


def _listing(stub: Stubber, entity: str, kind: str, revision: str) -> None:
    stub.add_response(
        "create_listing_change_set",
        {"listingId": f"l-{entity}", "listingRevision": "1", "status": "ACTIVE"},
        {
            **SCOPE,
            "entityIdentifier": entity,
            "entityType": kind,
            "entityRevision": revision,
            "action": "PUBLISH",
        },
    )


def test_a_datazone_target_names_its_region_domain_and_project():
    with pytest.raises(ValidationError, match="AWS region, the DataZone domain"):
        MetadataExportTarget(id="d", provider="datazone", aws_region="us-east-1")
    assert isinstance(metadata_export(_target()), DataZoneExport)


def test_the_forms_carry_the_members_and_the_products_fields():
    snapshot = _snapshot()
    (table,) = snapshot.tables
    form = table_form(table)
    assert form["provisaUri"] == table.semantic_uri
    assert form["freshnessSeconds"] == 3600
    assert json.loads(form["columns"]) == [{"name": "id", "type": "integer"}]
    assert product_form(snapshot.data_products[0]) == {
        "purpose": "Every sale",
        "usage": "Join on customer_id",
        "version": "1.2.0",
        "status": "draft",
    }


async def test_the_types_are_created_once_and_a_new_product_is_published(stubbed):
    snapshot = _snapshot()
    (table,) = snapshot.tables
    for name, rev in ((TABLE_FORM, "1"), (PRODUCT_FORM, "1")):
        stubbed.add_client_error(
            "get_form_type",
            "ResourceNotFoundException",
            expected_params={**SCOPE, "formTypeIdentifier": name},
        )
        stubbed.add_response(
            "create_form_type",
            {"domainId": DOMAIN, "name": name, "revision": rev},
            {
                **SCOPE,
                "name": name,
                "model": {"smithy": ANY},
                "owningProjectIdentifier": PROJECT,
                "status": "ENABLED",
            },
        )
    stubbed.add_client_error(
        "get_asset_type",
        "ResourceNotFoundException",
        expected_params={**SCOPE, "identifier": TABLE_TYPE},
    )
    stubbed.add_response(
        "create_asset_type",
        {
            "domainId": DOMAIN,
            "name": TABLE_TYPE,
            "revision": "1",
            "formsOutput": {},
            "owningProjectId": PROJECT,
        },
        {
            **SCOPE,
            "name": TABLE_TYPE,
            "description": ANY,
            "formsInput": {
                TABLE_FORM: {"typeIdentifier": TABLE_FORM, "typeRevision": "1", "required": True}
            },
            "owningProjectIdentifier": PROJECT,
        },
    )
    _search(stubbed, "ASSET", [])
    _search(stubbed, "DATA_PRODUCT", [])
    stubbed.add_response(
        "create_asset",
        {
            "id": "asset-1",
            "name": table.name,
            "typeIdentifier": TABLE_TYPE,
            "typeRevision": "1",
            "revision": "1",
            "owningProjectId": PROJECT,
            "domainId": DOMAIN,
            "formsOutput": [],
        },
        {
            **SCOPE,
            "name": table.name,
            "description": "Orders placed",
            "externalIdentifier": table.semantic_uri,
            "typeIdentifier": TABLE_TYPE,
            "typeRevision": "1",
            "formsInput": [
                {
                    "formName": TABLE_FORM,
                    "typeIdentifier": TABLE_FORM,
                    "typeRevision": "1",
                    "content": json.dumps(table_form(table)),
                }
            ],
            "owningProjectIdentifier": PROJECT,
        },
    )
    _listing(stubbed, "asset-1", "ASSET", "1")
    stubbed.add_response(
        "create_data_product",
        {
            "domainId": DOMAIN,
            "id": "dp-1",
            "revision": "1",
            "owningProjectId": PROJECT,
            "name": "Sales 360",
            "status": "CREATED",
        },
        {
            **SCOPE,
            "name": "Sales 360",
            "description": "Every sale",
            "owningProjectIdentifier": PROJECT,
            "items": [{"itemType": "ASSET", "identifier": "asset-1"}],
            "formsInput": [
                {
                    "formName": PRODUCT_FORM,
                    "typeIdentifier": PRODUCT_FORM,
                    "typeRevision": "1",
                    "content": json.dumps(product_form(snapshot.data_products[0])),
                }
            ],
        },
    )
    _listing(stubbed, "dp-1", "DATA_PRODUCT", "1")
    result = await metadata_export(_target()).publish(snapshot)
    assert result.ok, result.errors
    assert result.published == {"data_products": 1, "data_assets": 1}


async def test_a_second_publish_adds_revisions_to_what_it_finds(stubbed):
    snapshot = _snapshot()
    (table,) = snapshot.tables
    _types_exist(stubbed)
    _search(
        stubbed,
        "ASSET",
        [
            {
                "assetItem": {
                    "domainId": DOMAIN,
                    "identifier": "asset-1",
                    "name": table.name,
                    "typeIdentifier": TABLE_TYPE,
                    "typeRevision": "5",
                    "externalIdentifier": table.semantic_uri,
                    "owningProjectId": PROJECT,
                }
            }
        ],
    )
    _search(
        stubbed,
        "DATA_PRODUCT",
        [
            {
                "dataProductItem": {
                    "domainId": DOMAIN,
                    "id": "dp-1",
                    "name": "Sales 360",
                    "owningProjectId": PROJECT,
                }
            }
        ],
    )
    stubbed.add_response(
        "create_asset_revision",
        {
            "id": "asset-1",
            "name": table.name,
            "typeIdentifier": TABLE_TYPE,
            "typeRevision": "5",
            "revision": "2",
            "owningProjectId": PROJECT,
            "domainId": DOMAIN,
            "formsOutput": [],
        },
        {**SCOPE, "identifier": "asset-1", "name": ANY, "description": ANY, "formsInput": ANY},
    )
    _listing(stubbed, "asset-1", "ASSET", "2")
    stubbed.add_response(
        "create_data_product_revision",
        {
            "domainId": DOMAIN,
            "id": "dp-1",
            "revision": "2",
            "owningProjectId": PROJECT,
            "name": "Sales 360",
            "status": "CREATED",
        },
        {
            **SCOPE,
            "identifier": "dp-1",
            "name": "Sales 360",
            "description": "Every sale",
            "items": [{"itemType": "ASSET", "identifier": "asset-1"}],
            "formsInput": ANY,
        },
    )
    _listing(stubbed, "dp-1", "DATA_PRODUCT", "2")
    result = await metadata_export(_target()).publish(snapshot)
    assert result.ok, result.errors
    assert result.published == {"data_products": 1, "data_assets": 1}


async def test_a_refused_call_is_reported_against_its_product(stubbed):
    snapshot = _snapshot()
    _types_exist(stubbed)
    _search(stubbed, "ASSET", [])
    _search(stubbed, "DATA_PRODUCT", [])
    stubbed.add_client_error(
        "create_asset", "AccessDeniedException", "not a member of the project", 403
    )
    result = await metadata_export(_target()).publish(snapshot)
    assert len(result.errors) == 1
    assert "not a member of the project" in result.errors[0].message
    assert result.published == {}


def _asset(asset_id: str, revision: str, table: Any) -> dict[str, Any]:
    return {
        "id": asset_id,
        "name": table.name,
        "typeIdentifier": TABLE_TYPE,
        "typeRevision": "5",
        "revision": revision,
        "owningProjectId": PROJECT,
        "domainId": DOMAIN,
        "formsOutput": [],
    }


def _product(product_id: str, revision: str) -> dict[str, Any]:
    return {
        "domainId": DOMAIN,
        "id": product_id,
        "revision": revision,
        "owningProjectId": PROJECT,
        "name": "Sales 360",
        "status": "CREATED",
    }


async def test_two_publishes_in_a_row_reuse_the_bound_ids_without_a_search(stubbed):
    """REQ-1389: the first publish records each asset's and product's id; the second revises
    them by those ids and never searches, so DataZone's late index cannot duplicate them."""
    snapshot = _snapshot()
    (table,) = snapshot.tables
    (product,) = snapshot.data_products
    # The first publish: nothing bound, so the project is searched, and both are created.
    _types_exist(stubbed)
    _search(stubbed, "ASSET", [])
    _search(stubbed, "DATA_PRODUCT", [])
    stubbed.add_response("create_asset", _asset("asset-1", "1", table), ANY_PARAMS)
    _listing(stubbed, "asset-1", "ASSET", "1")
    stubbed.add_response("create_data_product", _product("dp-1", "1"), ANY_PARAMS)
    _listing(stubbed, "dp-1", "DATA_PRODUCT", "1")
    first = await metadata_export(_target()).publish(snapshot)
    assert first.ok, first.errors
    assert first.bindings == {
        table.semantic_uri: ("asset-1", table.semantic_uri),
        product.semantic_uri: ("dp-1", "Sales 360"),
    }
    # The second publish, as the publish path loads the bindings in: no search at all.
    _types_exist(stubbed)
    stubbed.add_response(
        "create_asset_revision",
        _asset("asset-1", "2", table),
        {**SCOPE, "identifier": "asset-1", "name": ANY, "description": ANY, "formsInput": ANY},
    )
    _listing(stubbed, "asset-1", "ASSET", "2")
    stubbed.add_response(
        "create_data_product_revision",
        _product("dp-1", "2"),
        {
            **SCOPE,
            "identifier": "dp-1",
            "name": "Sales 360",
            "description": ANY,
            "items": [{"itemType": "ASSET", "identifier": "asset-1"}],
            "formsInput": ANY,
        },
    )
    _listing(stubbed, "dp-1", "DATA_PRODUCT", "2")
    second_export = metadata_export(_target())
    second_export.stored_bindings = first.bindings
    second = await second_export.publish(snapshot)
    assert second.ok, second.errors
    assert second.bindings == first.bindings


async def test_a_bound_asset_deleted_in_datazone_is_published_anew(stubbed):
    snapshot = _snapshot()
    (table,) = snapshot.tables
    (product,) = snapshot.data_products
    _types_exist(stubbed)
    stubbed.add_client_error(
        "create_asset_revision", "ResourceNotFoundException", "Asset not found", 404
    )
    stubbed.add_response("create_asset", _asset("asset-2", "1", table), ANY_PARAMS)
    _listing(stubbed, "asset-2", "ASSET", "1")
    stubbed.add_response("create_data_product_revision", _product("dp-1", "2"), ANY_PARAMS)
    _listing(stubbed, "dp-1", "DATA_PRODUCT", "2")
    exporter = metadata_export(_target())
    exporter.stored_bindings = {
        table.semantic_uri: ("asset-gone", table.semantic_uri),
        product.semantic_uri: ("dp-1", "Sales 360"),
    }
    result = await exporter.publish(snapshot)
    assert result.ok, result.errors
    assert result.bindings[table.semantic_uri] == ("asset-2", table.semantic_uri)


async def test_a_retired_product_is_revised_retired_with_no_assets(stubbed):
    """REQ-1634 STATUS: retired, its members went to its successor; its revision carries the
    retired status and no items (DataZone takes none rather than an empty list), and is
    published so the listing says so."""
    snapshot = _snapshot(status="retired", members=False)
    (product,) = snapshot.data_products
    assert product.members == ()
    _types_exist(stubbed)
    stubbed.add_response(
        "create_data_product_revision",
        _product("dp-1", "3"),
        {
            **SCOPE,
            "identifier": "dp-1",
            "name": "Sales 360",
            "description": "Every sale",
            "formsInput": [
                {
                    "formName": PRODUCT_FORM,
                    "typeIdentifier": PRODUCT_FORM,
                    "typeRevision": "4",
                    "content": json.dumps(product_form(product)),
                }
            ],
        },
    )
    _listing(stubbed, "dp-1", "DATA_PRODUCT", "3")
    exporter = metadata_export(_target())
    exporter.stored_bindings = {product.semantic_uri: ("dp-1", "Sales 360")}
    result = await exporter.publish(snapshot)
    assert result.ok, result.errors
    assert json.loads(json.dumps(product_form(product)))["status"] == "retired"
