# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1d9f27-6b03-4e58-8c72-d5e0139ab846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1069 e2e: a governed config reaches a real OpenLineage server as datasets and lineage.

Marquez is the OpenLineage reference implementation, so what it accepts is what the standard
says. Reading the assets back out of Marquez — rather than asserting on the payload the adapter
built — is the only way this proves interoperability instead of self-consistency.
"""

# Requirements: REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import os
from urllib.parse import quote

import httpx
import pytest

from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.openlineage import PRODUCER
from provisa.core.models import MetadataExportConfig
from tests.integration.metadata_export_fixture import (
    MASK_PATTERN,
    ORG_ID,
    RLS_FILTER,
    governed_snapshot,
)

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_marquez,
    pytest.mark.asyncio(loop_scope="session"),
]

NAMESPACE = f"provisa://{ORG_ID}"


def _base_url() -> str:
    return f"http://localhost:{os.environ['MARQUEZ_PORT']}"


def _export_config() -> MetadataExportConfig:
    return MetadataExportConfig(
        enabled=True, provider="openlineage", endpoint=_base_url(), timeout_seconds=30
    )


@pytest.fixture(scope="module")
async def published() -> dict:
    """Publish once, then let every assertion read the same server state back."""
    export = metadata_export(_export_config())
    await export.health()
    result = await export.publish(governed_snapshot())
    assert result.ok, [e.message for e in result.errors]
    return {"result": result}


async def _get(path: str, **params) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{_base_url()}{path}", params=params or None)
    response.raise_for_status()
    return response.json()


def _ns_path(suffix: str) -> str:
    # The namespace is org-scoped and contains ``://``, so it has to be percent-encoded whole —
    # an unencoded slash would split the path and address a different Marquez resource.
    return f"/api/v1/namespaces/{quote(NAMESPACE, safe='')}{suffix}"


async def test_publish_registers_every_governed_table_as_a_dataset(published):
    assert published["result"].published["dataset"] == 3
    datasets = await _get(_ns_path("/datasets"), limit=100)
    by_name = {d["name"]: d for d in datasets["datasets"]}
    assert {"wh.public.orders", "wh.public.customers", "wh.public.order_totals"} <= set(by_name)
    orders = by_name["wh.public.orders"]
    fields = {f["name"]: f for f in orders["fields"]}
    assert fields["amount"]["type"] == "numeric"
    assert fields["amount"]["description"] == "Order total in cents"


async def test_marquez_accepts_the_column_lineage_of_the_derived_view(published):
    assert published["result"].published["lineage"] == 1
    job = await _get(_ns_path("/jobs/wh.public.order_totals"))
    assert [i["name"] for i in job["inputs"]] == ["wh.public.orders"]
    assert [o["name"] for o in job["outputs"]] == ["wh.public.order_totals"]

    node_id = f"dataset:{NAMESPACE}:wh.public.order_totals"
    graph = await _get("/api/v1/column-lineage", nodeId=node_id, depth=2)
    # Marquez resolved the facet into its own per-field graph, which is the proof: it only
    # builds these nodes for columns it recognises on both ends of the edge.
    edges = {
        (edge["origin"], edge["destination"])
        for node in graph["graph"]
        for edge in node.get("inEdges", [])
    }
    assert (
        f"datasetField:{NAMESPACE}:wh.public.order_totals:net",
        f"datasetField:{NAMESPACE}:wh.public.orders:amount",
    ) in edges, graph
    net = next(n for n in graph["graph"] if n["id"].endswith("order_totals:net"))
    assert net["data"]["fieldType"] == "numeric"


async def test_the_steward_and_the_governance_signals_survive_the_round_trip(published):
    """The facets Marquez stores back are what a downstream consumer actually reads."""
    orders = await _get(_ns_path("/datasets/wh.public.orders"))
    facets = orders["facets"]
    assert facets["ownership"]["owners"] == [{"name": "data-steward", "type": "STEWARD"}]
    signals = {s["signal"]: s for s in facets["provisa_governance"]["signals"]}
    assert set(signals) == {"masked", "rls_restricted", "visibility_restricted"}
    assert signals["masked"]["asset"] == "wh.public.orders.ssn"
    assert signals["masked"]["exemptRoles"] == ["admin"]
    assert signals["visibility_restricted"]["asset"] == "wh.public.orders.margin"
    assert facets["provisa_governance"]["_producer"] == PRODUCER


async def test_no_rule_body_is_ever_stored_in_the_external_catalog(published):
    """REQ-1071: the catalog learns THAT a column is masked, never the pattern that masks it —
    the pattern describes the shape of the withheld value."""
    datasets = await _get(_ns_path("/datasets"), limit=100)
    stored = httpx.Response(200, json=datasets).text
    assert MASK_PATTERN not in stored
    assert RLS_FILTER not in stored
    assert "current_setting" not in stored
