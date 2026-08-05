# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b45c1-9d38-4a06-b3f7-8c1e0d6a52f9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1069 e2e: the same governed config reaches a real OpenMetadata server as entities.

Assertions read the entities back out of OpenMetadata's own API, so what passes is what the
catalog stored — not what the adapter believed it sent.
"""

# Requirements: REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import base64
import os

import httpx
import pytest

from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.openmetadata import CLASSIFICATION
from provisa.core.models import MetadataExportConfig
from tests.integration.metadata_export_fixture import (
    MASK_PATTERN,
    RLS_FILTER,
    governed_snapshot,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_openmetadata,
    pytest.mark.asyncio(loop_scope="session"),
]

ORDERS_FQN = "wh.default.public.orders"
TOTALS_FQN = "wh.default.public.order_totals"


def _base_url() -> str:
    return f"http://localhost:{os.environ['OPENMETADATA_PORT']}"


async def _admin_token() -> str:
    """The bootstrap admin's JWT.

    OpenMetadata's basic-auth login takes the password base64-encoded; that is transport
    encoding the server requires, not a credential protection, and the container is the
    isolated test stack's own.
    """
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{_base_url()}/api/v1/users/login",
            json={
                "email": "admin@open-metadata.org",
                "password": base64.b64encode(b"admin").decode(),
            },
        )
    response.raise_for_status()
    return response.json()["accessToken"]


@pytest.fixture
async def token() -> str:
    return await _admin_token()


# Per test, not per module: the heavy-service fixture provisions OpenMetadata for one test and
# removes it afterwards, so a module-scoped publish would leave every test after the first
# reading a catalog that no longer holds it. Publishing is an upsert, so repeating it is cheap
# and lands the same state each time.
@pytest.fixture
async def published(token) -> dict:
    export = metadata_export(
        MetadataExportConfig(
            enabled=True,
            provider="openmetadata",
            endpoint=_base_url(),
            token=token,
            timeout_seconds=60,
        )
    )
    await export.health()
    result = await export.publish(governed_snapshot())
    assert result.ok, [e.message for e in result.errors]
    return {"result": result}


async def _get(token: str, path: str, **params) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            f"{_base_url()}{path}",
            params=params or None,
            headers={"Authorization": f"Bearer {token}"},
        )
    response.raise_for_status()
    return response.json()


async def test_every_governed_table_and_column_lands_in_the_catalog(published, token):
    assert published["result"].published["table"] == 3
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags")
    columns = {c["name"]: c for c in table["columns"]}
    assert set(columns) == {"id", "amount", "customer_id", "ssn", "margin"}
    # dataType is OpenMetadata's own enum; the source type rides in dataTypeDisplay so nothing
    # about the real column type is lost in translation.
    assert columns["amount"]["dataTypeDisplay"] == "numeric"
    assert columns["amount"]["description"] == "Order total in cents"


async def test_governance_signals_arrive_as_classification_tags(published, token):
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags")
    assert {t["tagFQN"] for t in table["tags"]} == {f"{CLASSIFICATION}.rls_restricted"}
    columns = {c["name"]: c for c in table["columns"]}
    assert {t["tagFQN"] for t in columns["ssn"]["tags"]} == {f"{CLASSIFICATION}.masked"}
    assert {t["tagFQN"] for t in columns["margin"]["tags"]} == {
        f"{CLASSIFICATION}.visibility_restricted"
    }
    assert not columns["id"].get("tags")


async def test_column_lineage_of_the_derived_view_is_queryable(published, token):
    assert published["result"].published["lineage"] == 1
    graph = await _get(
        token, f"/api/v1/lineage/table/name/{TOTALS_FQN}", upstreamDepth=1, downstreamDepth=0
    )
    upstream_fqns = {n["fullyQualifiedName"] for n in graph["nodes"]}
    assert ORDERS_FQN in upstream_fqns
    columns_lineage = [
        pair
        for edge in graph["upstreamEdges"]
        for pair in edge.get("lineageDetails", {}).get("columnsLineage", [])
    ]
    net = next(p for p in columns_lineage if p["toColumn"].endswith(".net"))
    assert net["fromColumns"] == [f"{ORDERS_FQN}.amount"]
    assert "amount" in net["function"]


async def test_the_unstewarded_domain_publishes_without_a_fabricated_owner(published, token):
    """REQ-609: the catalog has to show the governance gap, not a placeholder that hides it."""
    lab = await _get(token, "/api/v1/domains/name/lab", fields="owners")
    assert not lab.get("owners")
    assert "governance pending" in lab["description"].lower()
    sales = await _get(token, "/api/v1/domains/name/sales", fields="owners")
    assert [o["name"] for o in sales["owners"]] == ["data-steward"]


async def test_the_catalog_is_never_handed_a_path_to_the_source(published, token):
    """OpenMetadata must reach the data only through Provisa — a real connection config would
    give it an ungoverned route to the warehouse."""
    service = await _get(token, "/api/v1/services/databaseServices/name/wh")
    config = service["connection"]["config"]
    assert config["type"] == "CustomDatabase"
    assert "password" not in config and "hostPort" not in config


async def test_no_rule_body_is_stored_in_the_external_catalog(published, token):
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags,extension")
    stored = httpx.Response(200, json=table).text
    assert MASK_PATTERN not in stored
    assert RLS_FILTER not in stored
    assert "current_setting" not in stored
