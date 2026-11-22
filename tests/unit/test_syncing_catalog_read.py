# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8e1a75-3b96-4d20-8f17-6a2d9e0c5b14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247, REQ-2122: a provider reads a linked catalog's values back.

OpenMetadata and DataHub read the description, column descriptions, owners, tags and glossary links
of each table the entry is published, and each Provisa term's definition, from their stubbed HTTP
APIs, through the same checked client the publish uses. What a wrong read gets wrong silently:
reading Provisa's own governance tags as catalog tags, reading a term by its vendor address instead
of its Provisa name, preferring the ingested description over the one a person edited, answering an
unsupported provider with an empty read, and turning a refused read into no values.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

from urllib.parse import unquote

import httpx
import pytest

from provisa.api.metadata_export import sync_values as sv
from provisa.api.metadata_export.datahub import DataHubExport, dataset_urn, glossary_term_urn
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    DomainAsset,
    GlossaryTermAsset,
    MetadataSnapshot,
    ModelTag,
    OwnerRef,
    TableAsset,
)
from provisa.api.metadata_export.openmetadata import OpenMetadataExport, to_term_plans
from provisa.api.catalog_import.reader import CatalogReader
from provisa.api.metadata_export.registry import metadata_export
from provisa.core.models import MetadataExportTarget

TABLE = AssetRef(AssetKind.TABLE, ("wh", "public", "orders"))
COL_ID = AssetRef(AssetKind.COLUMN, ("wh", "public", "orders", "id"))
COL_TOTAL = AssetRef(AssetKind.COLUMN, ("wh", "public", "orders", "total"))
GROUP = "wh.public.orders"


def _snapshot() -> MetadataSnapshot:
    return MetadataSnapshot(
        org_id="acme",
        domains=[
            DomainAsset(
                id="sales",
                description="Sales",
                steward=OwnerRef(id="alice", kind="steward"),
                pending=False,
            )
        ],
        tables=[
            TableAsset(
                ref=TABLE,
                name="orders",
                source_id="wh",
                domain_id="sales",
                description="Orders placed",
                columns=[
                    ColumnAsset(ref=COL_ID, name="id", data_type="integer", description="Order id"),
                    ColumnAsset(
                        ref=COL_TOTAL, name="total", data_type="numeric", description="Total"
                    ),
                ],
            )
        ],
        model_tags=[ModelTag(tag_id="gold", is_system=False, asset=TABLE)],
        glossary_terms=[
            GlossaryTermAsset(
                term_id=7,
                name="Order",
                definition="A customer's purchase",
                is_abstract=False,
                deprecated=False,
                status="approved",
                grounding="grounded",
                tables=(TABLE,),
                role="class",
            )
        ],
    )


def _target(provider: str) -> MetadataExportTarget:
    return MetadataExportTarget(
        id="cat", provider=provider, endpoint="https://catalog.example", timeout_seconds=5
    )


@pytest.fixture
def egress(monkeypatch):
    """Count the checked client's hooks being asked for: the read is held to the same egress
    rules as the publish (SEC-H6/SEC-H6b)."""
    from provisa.security import sandbox_egress

    calls: list[int] = []
    real = sandbox_egress.endpoint_event_hooks

    def _hooks():
        calls.append(1)
        return real()

    monkeypatch.setattr(sandbox_egress, "endpoint_event_hooks", _hooks)
    return calls


def _serve(monkeypatch, routes: dict[str, object]) -> list[str]:
    """Answer each GET by the first route whose key the decoded URL ends with: a dict is served
    as JSON, an int as that status. A URL no route names is a 404."""
    seen: list[str] = []

    async def _get(self, url, params=None, headers=None):
        seen.append(url)
        request = httpx.Request("GET", url)
        path = unquote(url)
        for key, answer in routes.items():
            if path.endswith(key):
                if isinstance(answer, int):
                    return httpx.Response(answer, text="refused", request=request)
                return httpx.Response(200, json=answer, request=request)
        return httpx.Response(404, text="not found", request=request)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return seen


# --- OpenMetadata ------------------------------------------------------------------------------


def _om_term_fqn(snapshot: MetadataSnapshot) -> str:
    plans, _ = to_term_plans(snapshot)
    return next(p.fqn for p in plans if p.term.term_id == 7)


@pytest.mark.asyncio
async def test_openmetadata_reads_the_linked_assets_values(monkeypatch, egress):
    snapshot = _snapshot()
    term_fqn = _om_term_fqn(snapshot)
    _serve(
        monkeypatch,
        {
            "/api/v1/tables/name/wh.default.public.orders": {
                "description": "Orders, as the catalog says",
                "owners": [{"type": "user", "name": "bob"}, {"type": "user", "name": "alice"}],
                "tags": [
                    {"tagFQN": "Provisa.rls_restricted", "source": "Classification"},
                    {"tagFQN": "PII.Sensitive", "source": "Classification"},
                    {"tagFQN": term_fqn, "source": "Glossary"},
                    {"tagFQN": "Finance.Revenue", "source": "Glossary"},
                ],
                "columns": [
                    {"name": "id", "description": "Order id"},
                    {
                        "name": "total",
                        "tags": [{"tagFQN": "PII.Money", "source": "Classification"}],
                    },
                ],
            },
            f"/api/v1/glossaryTerms/name/{term_fqn}": {"description": "A purchase"},
        },
    )
    values = await OpenMetadataExport(_target("openmetadata")).read(snapshot)
    got = {key: v.value for key, v in values.items()}
    assert got == {
        (sv.DESCRIPTION, GROUP): "Orders, as the catalog says",
        (sv.OWNER, GROUP): ("alice", "bob"),
        # Provisa's governance classification is not a catalog value; the steward's tag is.
        (sv.TAG, GROUP): ("PII.Sensitive",),
        # Provisa's term by its name; another glossary's by its FQN.
        (sv.GLOSSARY_LINK, GROUP): ("Finance.Revenue", "Order"),
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.id"): "Order id",
        (sv.TAG, "wh.public.orders.id"): (),
        (sv.GLOSSARY_LINK, "wh.public.orders.id"): (),
        # No description in the entity is the empty one.
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.total"): "",
        (sv.TAG, "wh.public.orders.total"): ("PII.Money",),
        (sv.GLOSSARY_LINK, "wh.public.orders.total"): (),
        (sv.TERM_DEFINITION, "Order"): "A purchase",
    }
    assert all(v.group == GROUP for k, v in values.items() if k[0] != sv.TERM_DEFINITION)
    assert egress, "the read must use the checked client the publish uses"


@pytest.mark.asyncio
async def test_openmetadata_asset_it_does_not_hold_has_no_values(monkeypatch, egress):
    _serve(monkeypatch, {})
    assert await OpenMetadataExport(_target("openmetadata")).read(_snapshot()) == {}


@pytest.mark.asyncio
async def test_openmetadata_refused_read_raises(monkeypatch, egress):
    _serve(monkeypatch, {"/api/v1/tables/name/wh.default.public.orders": 403})
    with pytest.raises(httpx.HTTPStatusError):
        await OpenMetadataExport(_target("openmetadata")).read(_snapshot())


# --- DataHub -----------------------------------------------------------------------------------


def _aspect(record: str, value: dict) -> dict:
    return {"aspect": {record: value}}


@pytest.mark.asyncio
async def test_datahub_reads_the_linked_assets_values(monkeypatch, egress):
    snapshot = _snapshot()
    urn = dataset_urn(TABLE)
    provisa_term = glossary_term_urn("acme", "Order")
    _serve(
        monkeypatch,
        {
            f"{urn}?aspect=datasetProperties&version=0": _aspect(
                "com.linkedin.dataset.DatasetProperties", {"description": "Orders placed"}
            ),
            # A description a person edited in DataHub is what DataHub shows.
            f"{urn}?aspect=editableDatasetProperties&version=0": _aspect(
                "com.linkedin.dataset.EditableDatasetProperties", {"description": "Edited orders"}
            ),
            f"{urn}?aspect=schemaMetadata&version=0": _aspect(
                "com.linkedin.schema.SchemaMetadata",
                {
                    "fields": [
                        {
                            "fieldPath": "id",
                            "description": "Order id",
                            "globalTags": {"tags": [{"tag": "urn:li:tag:provisa_masked"}]},
                        },
                        {"fieldPath": "total", "description": "Total"},
                    ]
                },
            ),
            f"{urn}?aspect=editableSchemaMetadata&version=0": _aspect(
                "com.linkedin.schema.EditableSchemaMetadata",
                {
                    "editableSchemaFieldInfo": [
                        {
                            "fieldPath": "total",
                            "description": "Total in USD",
                            "glossaryTerms": {"terms": [{"urn": provisa_term}]},
                        }
                    ]
                },
            ),
            f"{urn}?aspect=ownership&version=0": _aspect(
                "com.linkedin.common.Ownership",
                {"owners": [{"owner": "urn:li:corpuser:carol"}]},
            ),
            f"{urn}?aspect=globalTags&version=0": _aspect(
                "com.linkedin.common.GlobalTags",
                {
                    "tags": [
                        {"tag": "urn:li:tag:provisa_rls_restricted"},
                        {"tag": "urn:li:tag:provisa_gold"},
                        {"tag": "urn:li:tag:Certified"},
                    ]
                },
            ),
            f"{urn}?aspect=glossaryTerms&version=0": _aspect(
                "com.linkedin.common.GlossaryTerms",
                {"terms": [{"urn": provisa_term}, {"urn": "urn:li:glossaryTerm:Other.Sale"}]},
            ),
            f"{provisa_term}?aspect=glossaryTermInfo&version=0": _aspect(
                "com.linkedin.glossary.GlossaryTermInfo", {"definition": "A customer's purchase"}
            ),
        },
    )
    values = await DataHubExport(_target("datahub")).read(snapshot)
    got = {key: v.value for key, v in values.items()}
    assert got == {
        (sv.DESCRIPTION, GROUP): "Edited orders",
        (sv.OWNER, GROUP): ("carol",),
        # Governance tags left out; a registry tag by its Provisa id; a person's by its name.
        (sv.TAG, GROUP): ("Certified", "gold"),
        (sv.GLOSSARY_LINK, GROUP): ("Order", "urn:li:glossaryTerm:Other.Sale"),
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.id"): "Order id",
        (sv.TAG, "wh.public.orders.id"): (),
        (sv.GLOSSARY_LINK, "wh.public.orders.id"): (),
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.total"): "Total in USD",
        (sv.TAG, "wh.public.orders.total"): (),
        (sv.GLOSSARY_LINK, "wh.public.orders.total"): ("Order",),
        (sv.TERM_DEFINITION, "Order"): "A customer's purchase",
    }
    assert egress, "the read must use the checked client the publish uses"


@pytest.mark.asyncio
async def test_datahub_dataset_it_does_not_hold_has_no_values(monkeypatch, egress):
    _serve(monkeypatch, {})
    assert await DataHubExport(_target("datahub")).read(_snapshot()) == {}


@pytest.mark.asyncio
async def test_datahub_refused_read_raises(monkeypatch, egress):
    _serve(monkeypatch, {"aspect=datasetProperties&version=0": 500})
    with pytest.raises(httpx.HTTPStatusError):
        await DataHubExport(_target("datahub")).read(_snapshot())


# --- every other provider ----------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["atlas", "collibra", "atlan", "openlineage"])
async def test_a_provider_that_does_not_read_is_not_a_catalog_reader(provider):
    assert not isinstance(metadata_export(_target(provider)), CatalogReader)


def test_provisa_values_cover_every_kind_on_every_asset():
    got = {key: v.value for key, v in sv.provisa_values(_snapshot()).items()}
    assert got == {
        (sv.DESCRIPTION, GROUP): "Orders placed",
        (sv.OWNER, GROUP): ("alice",),
        (sv.TAG, GROUP): ("gold",),
        (sv.GLOSSARY_LINK, GROUP): ("Order",),
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.id"): "Order id",
        (sv.TAG, "wh.public.orders.id"): (),
        (sv.GLOSSARY_LINK, "wh.public.orders.id"): (),
        (sv.COLUMN_DESCRIPTION, "wh.public.orders.total"): "Total",
        (sv.TAG, "wh.public.orders.total"): (),
        (sv.GLOSSARY_LINK, "wh.public.orders.total"): (),
        (sv.TERM_DEFINITION, "Order"): "A customer's purchase",
    }
