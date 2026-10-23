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

from provisa.api.metadata_export.openmetadata import CLASSIFICATION
from tests.integration.metadata_export_server import (
    CLASS_TERM,
    GLOSSARY_TERM,
    MASK_PATTERN,
    ORDERS_AMOUNT_DESCRIPTION,
    PRODUCT,
    PRODUCT_FIELDS,
    RLS_FILTER,
    SCHEMA,
    SOURCE,
    VIEW,
    publish,
    published_server,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_openmetadata,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_openmetadata_e2e"
ORDERS_FQN = f"{SOURCE}.default.{SCHEMA}.orders"
TOTALS_FQN = f"__derived__.default.views.{VIEW}"


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
    """The production publish (REQ-2119): a Provisa server saves OpenMetadata as its target and
    publishes over the admin API."""
    async with published_server(ORG) as client:
        result = await publish(
            client,
            {
                "provider": "openmetadata",
                "endpoint": _base_url(),
                "auth_mode": "bearer",
                "token": token,
                "timeout_seconds": 60,
            },
        )
    assert result["ok"], result["errors"]
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
    assert published["result"]["published"]["table"] == 3
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags")
    columns = {c["name"]: c for c in table["columns"]}
    assert set(columns) == {"id", "customer_id", "amount", "region", "ssn", "margin"}
    # dataType is OpenMetadata's own enum; the source type rides in dataTypeDisplay so nothing
    # about the real column type is lost in translation.
    assert columns["amount"]["dataTypeDisplay"] == "numeric"
    assert columns["amount"]["description"] == ORDERS_AMOUNT_DESCRIPTION


async def test_governance_signals_arrive_as_classification_tags(published, token):
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags")

    def _classified(tags: list[dict]) -> set[str]:
        return {t["tagFQN"] for t in tags if t["source"] == "Classification"}

    assert _classified(table["tags"]) == {f"{CLASSIFICATION}.rls_restricted"}
    columns = {c["name"]: c for c in table["columns"]}
    assert {t["tagFQN"] for t in columns["ssn"]["tags"]} == {f"{CLASSIFICATION}.masked"}
    assert {t["tagFQN"] for t in columns["margin"]["tags"]} == {
        f"{CLASSIFICATION}.visibility_restricted"
    }
    assert not columns["id"].get("tags")


async def test_column_lineage_of_the_derived_view_is_queryable(published, token):
    assert published["result"]["published"]["lineage"] == 1
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
    assert [o["name"] for o in sales["owners"]] == ["data_owner"]


async def test_the_catalog_is_never_handed_a_path_to_the_source(published, token):
    """OpenMetadata must reach the data only through Provisa — a real connection config would
    give it an ungoverned route to the warehouse."""
    service = await _get(token, f"/api/v1/services/databaseServices/name/{SOURCE}")
    config = service["connection"]["config"]
    assert config["type"] == "CustomDatabase"
    assert "password" not in config and "hostPort" not in config


async def test_no_rule_body_is_stored_in_the_external_catalog(published, token):
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags,extension")
    stored = httpx.Response(200, json=table).text
    assert MASK_PATTERN not in stored
    assert RLS_FILTER not in stored
    assert "current_setting" not in stored


async def test_the_data_product_holds_its_members(published, token):
    """REQ-1634, REQ-2118: OpenMetadata's own data product, in its domain, with every member."""
    product = await _get(token, f"/api/v1/dataProducts/name/{PRODUCT}", fields="assets,domains")
    assert [d["name"] for d in product["domains"]] == ["sales"]
    assert {a["fullyQualifiedName"] for a in product["assets"]} == {
        ORDERS_FQN,
        f"{SOURCE}.default.{SCHEMA}.customers",
        TOTALS_FQN,
    }
    assert product["description"].startswith(PRODUCT_FIELDS["purpose"])
    assert f"Limitations: {PRODUCT_FIELDS['limitations']}" in product["description"]


async def test_each_member_carries_its_odcs_contract(published, token):
    """REQ-2118: the member's ODCS 3.1 contract, imported onto its table, read back out of
    OpenMetadata as ODCS."""
    orders = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}")
    contract = await _get(
        token, "/api/v1/dataContracts/entity", entityId=orders["id"], entityType="table"
    )
    odcs = await _get(token, f"/api/v1/dataContracts/{contract['id']}/odcs")
    assert odcs["apiVersion"] == "v3.1.0"
    assert odcs["version"] == PRODUCT_FIELDS["version"]
    (schema,) = odcs["schema"]
    columns = {p["name"]: p for p in schema["properties"]}
    assert set(columns) == {"id", "customer_id", "amount", "region", "ssn", "margin"}
    assert columns["amount"]["description"] == ORDERS_AMOUNT_DESCRIPTION
    assert MASK_PATTERN not in httpx.Response(200, json=odcs).text


async def test_provisa_assigns_its_terms_as_automated_glossary_labels(published, token):
    """REQ-1389 (amended): the Class term labels the table it grounds and the Property term its
    column, each as an Automated label -- what OpenMetadata shows as a system assignment."""
    table = await _get(token, f"/api/v1/tables/name/{ORDERS_FQN}", fields="columns,tags")
    glossary = f"provisa_{ORG}"

    def _assigned(tags: list[dict]) -> set[tuple[str, str]]:
        return {(t["tagFQN"], t["labelType"]) for t in tags if t["source"] == "Glossary"}

    assert _assigned(table["tags"]) == {(f"{glossary}.Classes.{CLASS_TERM}", "Automated")}
    columns = {c["name"]: c for c in table["columns"]}
    assert _assigned(columns["customer_id"].get("tags") or []) == {
        (f"{glossary}.Properties.{GLOSSARY_TERM}", "Automated")
    }


async def test_each_term_sits_in_its_roles_group_and_carries_its_role(published, token):
    """REQ-2141: the Class term beneath the Classes group term, with provisaRole set."""
    term = await _get(
        token,
        f"/api/v1/glossaryTerms/name/provisa_{ORG}.Classes.{CLASS_TERM}",
        fields="extension,parent",
    )
    assert term["parent"]["fullyQualifiedName"] == f"provisa_{ORG}.Classes"
    assert term["extension"]["provisaRole"] == "class"
