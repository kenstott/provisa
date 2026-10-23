# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b5d20-7c41-4f96-a2e8-1b9d0f6c4a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2119 e2e: a Provisa server publishes to a real DataHub through the production path.

The test starts DataHub (GMS with its MySQL, its search index and the stack's Kafka), saves it
as the org's target and presses Publish now over the admin API, then reads every aspect back
out of GMS's own REST API: what passes is what DataHub stored.
"""

# Requirements: REQ-2119, REQ-1069, REQ-1634

from __future__ import annotations

import asyncio
import json
import os
import time
from urllib.parse import quote

import httpx
import pytest

from provisa.api.metadata_export.datahub import (
    PROVISA_ACTOR,
    data_product_urn,
    glossary_term_urn,
)
from tests.integration.metadata_export_server import (
    CLASS_TERM,
    DOMAIN,
    GLOSSARY_DEFINITION,
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
    pytest.mark.requires_datahub,
    pytest.mark.asyncio(loop_scope="session"),
]

ORG = "mdx_datahub_e2e"


def _gms() -> str:
    return f"http://localhost:{os.environ['DATAHUB_GMS_PORT']}"


def _dataset(table: str, schema: str = SCHEMA, source: str = SOURCE) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:provisa,{source}.{schema}.{table},PROD)"


async def _aspect(urn: str, aspect: str) -> dict:
    """One aspect of ``urn`` as GMS stores it. Its search and graph indices are written by the
    change-log consumer after the ingest returns, so the read waits for the aspect to appear."""
    url = f"{_gms()}/aspects/{quote(urn, safe='')}"
    deadline = time.monotonic() + 60
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            response = await client.get(url, params={"aspect": aspect, "version": 0})
            if response.status_code != 404 or time.monotonic() >= deadline:
                break
            await asyncio.sleep(1)
    response.raise_for_status()
    (value,) = response.json()["aspect"].values()
    return value


async def _the_publish_reports_what_it_sent(published: dict) -> None:
    assert published["provider"] == "datahub"
    assert published["published"]["table"] == 3
    assert published["published"]["data_product"] == 1


async def _each_member_table_and_its_columns_land(published: dict) -> None:
    orders = await _aspect(_dataset("orders"), "datasetProperties")
    assert orders["description"] == "Orders placed"
    assert orders["customProperties"]["provisaUri"].endswith("/tables/orders")
    schema = await _aspect(_dataset("orders"), "schemaMetadata")
    fields = {f["fieldPath"]: f for f in schema["fields"]}
    assert set(fields) == {"id", "customer_id", "amount", "region", "ssn", "margin"}
    assert fields["amount"]["description"] == ORDERS_AMOUNT_DESCRIPTION
    customers = await _aspect(_dataset("customers"), "schemaMetadata")
    assert {f["fieldPath"] for f in customers["fields"]} == {"id", "name"}


async def _governance_arrives_as_tags_without_any_rule_body(published: dict) -> None:
    schema = await _aspect(_dataset("orders"), "schemaMetadata")
    tags = {f["fieldPath"]: [t["tag"] for t in f["globalTags"]["tags"]] for f in schema["fields"]}
    assert tags["ssn"] == ["urn:li:tag:provisa_masked"]
    assert tags["margin"] == ["urn:li:tag:provisa_visibility_restricted"]
    assert tags["id"] == []
    table_tags = await _aspect(_dataset("orders"), "globalTags")
    assert [t["tag"] for t in table_tags["tags"]] == ["urn:li:tag:provisa_rls_restricted"]
    stored = json.dumps([schema, table_tags])
    assert MASK_PATTERN not in stored
    assert RLS_FILTER not in stored


async def _the_steward_owns_each_member(published: dict) -> None:
    ownership = await _aspect(_dataset("orders"), "ownership")
    assert [o["owner"] for o in ownership["owners"]] == ["urn:li:corpuser:data_owner"]


async def _the_data_product_holds_its_members_and_fields(published: dict) -> None:
    urn = data_product_urn(ORG, PRODUCT)
    product = await _aspect(urn, "dataProductProperties")
    assert product["name"] == PRODUCT_FIELDS["name"]
    assert product["customProperties"]["provisaDomain"] == DOMAIN
    # REQ-2118: every ODPS field, under its ODPS name.
    odps = {k: v for k, v in product["customProperties"].items() if k.startswith("odps.")}
    assert odps == {
        "odps.purpose": PRODUCT_FIELDS["purpose"],
        "odps.ownerRole": PRODUCT_FIELDS["owner_role"],
        "odps.teamRole": PRODUCT_FIELDS["team_role"],
        "odps.limitations": PRODUCT_FIELDS["limitations"],
        "odps.usage": PRODUCT_FIELDS["usage"],
        "odps.version": PRODUCT_FIELDS["version"],
        "odps.status": PRODUCT_FIELDS["status"],
        "odps.sla": PRODUCT_FIELDS["sla"],
        "odps.support": PRODUCT_FIELDS["support"],
        "odps.supportContact": PRODUCT_FIELDS["support_contact"],
        "odps.custom.cost_center": "CC-42",
    }
    assert {a["destinationUrn"] for a in product["assets"]} == {
        _dataset("orders"),
        _dataset("customers"),
        _dataset(VIEW, schema="views", source="__derived__"),
    }
    owners = await _aspect(urn, "ownership")
    assert [o["owner"] for o in owners["owners"]] == ["urn:li:corpuser:data_owner"]


async def _the_view_carries_its_column_lineage(published: dict) -> None:
    lineage = await _aspect(_dataset(VIEW, schema="views", source="__derived__"), "upstreamLineage")
    assert [u["dataset"] for u in lineage["upstreams"]] == [_dataset("orders")]
    downstream = {
        tuple(edge["downstreams"]): edge["upstreams"] for edge in lineage["fineGrainedLineages"]
    }
    net = f"urn:li:schemaField:({_dataset(VIEW, schema='views', source='__derived__')},net)"
    assert downstream[(net,)] == [f"urn:li:schemaField:({_dataset('orders')},amount)"]


async def _the_glossary_term_lands_under_the_org_node(published: dict) -> None:
    term = await _aspect(glossary_term_urn(ORG, GLOSSARY_TERM), "glossaryTermInfo")
    assert term["definition"] == GLOSSARY_DEFINITION
    assert term["parentNode"] == f"urn:li:glossaryNode:provisa.{ORG}"


async def _provisa_assigns_its_terms_attributed_to_its_actor(published: dict) -> None:
    """REQ-1389 (amended): the Class term on the dataset it grounds, the Property term on its
    field, each association attributed to Provisa's actor."""
    dataset_terms = await _aspect(_dataset("orders"), "glossaryTerms")
    assert [(t["urn"], t["attribution"]["actor"]) for t in dataset_terms["terms"]] == [
        (glossary_term_urn(ORG, CLASS_TERM), PROVISA_ACTOR)
    ]
    schema = await _aspect(_dataset("orders"), "schemaMetadata")
    fields = {f["fieldPath"]: f for f in schema["fields"]}
    assert [t["urn"] for t in fields["customer_id"]["glossaryTerms"]["terms"]] == [
        glossary_term_urn(ORG, GLOSSARY_TERM)
    ]


# One test, one DataHub: the heavy-service fixture starts GMS for a test and removes it after, so
# every check reads the catalog the one production publish wrote.
async def test_datahub_holds_what_the_production_publish_sent():
    async with published_server(ORG) as client:
        published = await publish(client, {"provider": "datahub", "endpoint": _gms()})
    assert published["ok"], published["errors"]
    for check in (
        _the_publish_reports_what_it_sent,
        _each_member_table_and_its_columns_land,
        _governance_arrives_as_tags_without_any_rule_body,
        _the_steward_owns_each_member,
        _the_data_product_holds_its_members_and_fields,
        _the_view_carries_its_column_lineage,
        _the_glossary_term_lands_under_the_org_node,
        _provisa_assigns_its_terms_attributed_to_its_actor,
    ):
        await check(published)
