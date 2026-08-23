# Copyright (c) 2026 Kenneth Stott
# Canary: f6075c9a-8dad-4215-afbc-3995424f6246
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1387: business-glossary publishing to OpenMetadata stays inside the Provisa glossary.

Terms publish into a Provisa-owned glossary (``provisa_<org>``) and nowhere else: no other
glossary is read, written, or deleted, and a term's absence from the snapshot never deletes
it. Each upsert's entity UUID is captured as a binding keyed by the term's semantic URI; on
a later publish a changed FQN is rebound by PATCH — glossary terms, unlike tables, allow
rename and re-parenting — so steward curation stays on the SAME entity. The four edge types
land on OpenMetadata's own constructs, the deprecated flag on ``entityStatus``, and
term-to-asset assignment is never written (HUMAN-OWNED, REQ-1389).
"""

# Requirements: REQ-1387, REQ-1389

from __future__ import annotations

import json as jsonlib

import httpx
import pytest

from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
)
from provisa.api.metadata_export.openmetadata import (
    GLOSSARY_DISPLAY_NAME,
    OpenMetadataExport,
)
from provisa.core.models import MetadataExportConfig

GLOSSARY = "provisa_acme"

# Steward curation living on a Provisa term inside OpenMetadata: the publish must carry it.
STEWARD_TERM_TAG = {
    "tagFQN": "PII.Sensitive",
    "source": "Classification",
    "labelType": "Manual",
    "state": "Confirmed",
}
STEWARD_REVIEWERS = [{"id": "9c1d8e70-0000-0000-0000-000000000001", "type": "user"}]


def _term(
    term_id: int,
    name: str,
    definition: str | None = "Means something.",
    deprecated: bool = False,
    refs: tuple[AssetRef, ...] = (),
) -> GlossaryTermAsset:
    return GlossaryTermAsset(
        term_id=term_id,
        name=name,
        definition=definition,
        is_abstract=True,
        deprecated=deprecated,
        refs=refs,
        semantic_uri=f"provisa://acme/terms/{name}",
    )


def _snapshot(terms, edges=()) -> MetadataSnapshot:
    return MetadataSnapshot(org_id="acme", glossary_terms=list(terms), glossary_edges=list(edges))


def _export_config() -> MetadataExportConfig:
    return MetadataExportConfig(
        enabled=True,
        provider="openmetadata",
        endpoint="https://catalog.example/",
        timeout_seconds=5,
    )


def _mock_transport(monkeypatch, live_terms: dict[str, dict] | None = None):
    """Record every call the exporter makes; no network.

    PUTs answer with a deterministic id (``uuid-<name>``) and the FQN the body implies;
    a term GET answers ``live_terms[fqn]`` when present and 404 otherwise; PATCH and
    DELETE are recorded so a test can assert exactly what was (never) touched.
    """
    calls: list[tuple[str, str, object]] = []
    live = live_terms or {}

    async def _put(self, url, json=None, headers=None):
        calls.append(("PUT", url, json))
        if url.endswith("/glossaries"):
            body = {"id": "gid-1", "fullyQualifiedName": json["name"]}
        else:
            prefix = json.get("parent", json["glossary"])
            body = {"id": f"uuid-{json['name']}", "fullyQualifiedName": f"{prefix}.{json['name']}"}
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    async def _get(self, url, params=None, headers=None):
        calls.append(("GET", url, params))
        request = httpx.Request("GET", url)
        fqn = url.rsplit("/name/", 1)[-1]
        if fqn in live:
            return httpx.Response(200, json=live[fqn], request=request)
        return httpx.Response(404, request=request)

    async def _patch(self, url, content=None, headers=None):
        calls.append(("PATCH", url, jsonlib.loads(content)))
        return httpx.Response(200, json={}, request=httpx.Request("PATCH", url))

    async def _delete(self, url, params=None, headers=None):
        calls.append(("DELETE", url, None))
        return httpx.Response(200, request=httpx.Request("DELETE", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    monkeypatch.setattr(httpx.AsyncClient, "patch", _patch)
    monkeypatch.setattr(httpx.AsyncClient, "delete", _delete)
    return calls


def _glossary_puts(calls) -> list[dict]:
    return [body for method, url, body in calls if method == "PUT" and url.endswith("/glossaries")]


def _term_puts(calls) -> list[dict]:
    return [
        body for method, url, body in calls if method == "PUT" and url.endswith("/glossaryTerms")
    ]


def _patches(calls) -> list[tuple[str, list]]:
    return [(url, body) for method, url, body in calls if method == "PATCH"]


@pytest.mark.asyncio
async def test_glossary_is_upserted_idempotently_under_the_org_stable_name(monkeypatch):
    calls = _mock_transport(monkeypatch)
    exporter = OpenMetadataExport(_export_config())
    first = await exporter.publish(_snapshot([_term(1, "Customer")]))
    second = await exporter.publish(_snapshot([_term(1, "Customer")]))
    assert first.ok and second.ok
    # One PUT per publish, always addressed by the same per-org name: the upsert is the
    # idempotency, not a create-if-missing dance.
    assert (
        _glossary_puts(calls)
        == [
            {
                "name": GLOSSARY,
                "displayName": GLOSSARY_DISPLAY_NAME,
                "description": "Business vocabulary Provisa publishes from its governed semantic layer.",
            }
        ]
        * 2
    )
    assert first.published["glossary"] == 1


@pytest.mark.asyncio
async def test_term_put_lands_in_the_provisa_glossary_and_captures_the_binding(monkeypatch):
    calls = _mock_transport(monkeypatch)
    result = await OpenMetadataExport(_export_config()).publish(_snapshot([_term(1, "Customer")]))
    assert result.ok
    (body,) = _term_puts(calls)
    assert body["glossary"] == GLOSSARY
    assert body["name"] == "Customer"
    assert body["description"] == "Means something."
    assert result.published["glossary_term"] == 1
    # REQ-1389: the vendor's own id, keyed by the canonical Provisa URN.
    assert result.bindings["provisa://acme/terms/Customer"] == (
        "uuid-Customer",
        f"{GLOSSARY}.Customer",
    )


@pytest.mark.asyncio
async def test_renamed_term_is_rebound_by_patch_on_the_stored_uuid(monkeypatch):
    calls = _mock_transport(monkeypatch)
    exporter = OpenMetadataExport(_export_config())
    # Last publish landed this URN under the old vendor name.
    exporter.stored_bindings = {"provisa://acme/terms/Customer": ("uuid-old", f"{GLOSSARY}.Cust")}
    result = await exporter.publish(_snapshot([_term(1, "Customer")]))
    assert result.ok
    # Glossary terms allow rename via PATCH (unlike tables): the stored UUID is patched,
    # so the fqn-keyed PUT that follows lands on the SAME entity — no succession.
    assert _patches(calls) == [
        (
            "https://catalog.example/api/v1/glossaryTerms/uuid-old",
            [{"op": "replace", "path": "/name", "value": "Customer"}],
        )
    ]
    assert result.bindings["provisa://acme/terms/Customer"] == (
        "uuid-Customer",
        f"{GLOSSARY}.Customer",
    )


@pytest.mark.asyncio
async def test_reparented_term_is_moved_by_patch_using_the_new_parents_uuid(monkeypatch):
    calls = _mock_transport(monkeypatch)
    exporter = OpenMetadataExport(_export_config())
    # Dog used to sit at the glossary root; the snapshot now nests it under Animal.
    exporter.stored_bindings = {"provisa://acme/terms/Dog": ("uuid-old", f"{GLOSSARY}.Dog")}
    result = await exporter.publish(
        _snapshot(
            [_term(1, "Animal"), _term(2, "Dog")],
            [GlossaryTermEdge(from_term_id=2, to_term_id=1, rel_type="KIND_OF")],
        )
    )
    assert result.ok
    assert _patches(calls) == [
        (
            "https://catalog.example/api/v1/glossaryTerms/uuid-old",
            [
                {
                    "op": "add",
                    "path": "/parent",
                    "value": {"id": "uuid-Animal", "type": "glossaryTerm"},
                }
            ],
        )
    ]


@pytest.mark.asyncio
async def test_unbound_vendor_terms_are_never_touched_and_nothing_is_deleted(monkeypatch):
    calls = _mock_transport(monkeypatch)
    exporter = OpenMetadataExport(_export_config())
    # A term published earlier that the snapshot no longer carries: its binding is stored,
    # but absence is never a delete — removal inside the catalog is a steward's call.
    exporter.stored_bindings = {
        "provisa://acme/terms/Legacy": ("uuid-legacy", f"{GLOSSARY}.Legacy")
    }
    result = await exporter.publish(_snapshot([_term(1, "Customer")]))
    assert result.ok
    assert not [call for call in calls if call[0] == "DELETE"]
    assert not [call for call in calls if "uuid-legacy" in call[1]]
    # Every write is scoped to the Provisa glossary's collections; no other glossary is
    # addressed anywhere.
    for method, url, body in calls:
        assert "/api/v1/glossar" in url
        if method == "PUT" and url.endswith("/glossaryTerms"):
            assert body["glossary"] == GLOSSARY


@pytest.mark.asyncio
async def test_kind_of_becomes_the_parent_child_hierarchy_parents_first(monkeypatch):
    calls = _mock_transport(monkeypatch)
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot(
            [_term(2, "Dog"), _term(1, "Animal")],
            [GlossaryTermEdge(from_term_id=2, to_term_id=1, rel_type="KIND_OF")],
        )
    )
    assert result.ok
    animal, dog = _term_puts(calls)
    assert animal["name"] == "Animal" and "parent" not in animal
    assert dog["name"] == "Dog"
    assert dog["parent"] == f"{GLOSSARY}.Animal"
    assert result.bindings["provisa://acme/terms/Dog"] == (
        "uuid-Dog",
        f"{GLOSSARY}.Animal.Dog",
    )


@pytest.mark.asyncio
async def test_synonym_related_and_part_of_edges_map_to_native_fields(monkeypatch):
    calls = _mock_transport(monkeypatch)
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot(
            [_term(1, "Client"), _term(2, "Customer"), _term(3, "Order"), _term(4, "OrderLine")],
            [
                GlossaryTermEdge(from_term_id=1, to_term_id=2, rel_type="SYNONYM_OF"),
                GlossaryTermEdge(from_term_id=1, to_term_id=3, rel_type="RELATED_TO"),
                GlossaryTermEdge(from_term_id=4, to_term_id=3, rel_type="PART_OF"),
            ],
        )
    )
    assert result.ok
    puts = _term_puts(calls)
    first_pass = {body["name"]: body for body in puts[:4]}
    # SYNONYM_OF: OpenMetadata models a synonym as an alternative NAME.
    assert first_pass["Client"]["synonyms"] == ["Customer"]
    # RELATED_TO and PART_OF ride relatedTerms, deferred until every endpoint exists.
    assert all("relatedTerms" not in body for body in puts[:4])
    second_pass = {body["name"]: body for body in puts[4:]}
    assert second_pass["Client"]["relatedTerms"] == [f"{GLOSSARY}.Order"]
    assert second_pass["OrderLine"]["relatedTerms"] == [f"{GLOSSARY}.Order"]
    assert set(second_pass) == {"Client", "OrderLine"}


@pytest.mark.asyncio
async def test_deprecated_flag_lands_as_entity_status(monkeypatch):
    calls = _mock_transport(monkeypatch)
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot([_term(1, "Legacy", deprecated=True)])
    )
    assert result.ok
    assert _patches(calls) == [
        (
            "https://catalog.example/api/v1/glossaryTerms/uuid-Legacy",
            [{"op": "add", "path": "/entityStatus", "value": "Deprecated"}],
        )
    ]


@pytest.mark.asyncio
async def test_undeprecated_term_moves_back_to_approved_and_steward_status_is_left_alone(
    monkeypatch,
):
    live = {
        f"{GLOSSARY}.Customer": {"id": "uuid-Customer", "entityStatus": "Deprecated"},
        f"{GLOSSARY}.Draft": {"id": "uuid-Draft", "entityStatus": "In Review"},
    }
    calls = _mock_transport(monkeypatch, live)
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot([_term(1, "Customer"), _term(2, "Draft")])
    )
    assert result.ok
    # Provisa authors only the Deprecated transition; "In Review" is the stewards'
    # workflow and stays untouched.
    assert _patches(calls) == [
        (
            "https://catalog.example/api/v1/glossaryTerms/uuid-Customer",
            [{"op": "replace", "path": "/entityStatus", "value": "Approved"}],
        )
    ]


@pytest.mark.asyncio
async def test_merge_carries_steward_tags_reviewers_and_foreign_related_terms(monkeypatch):
    live = {
        f"{GLOSSARY}.Customer": {
            "id": "uuid-Customer",
            "entityStatus": "Approved",
            "tags": [STEWARD_TERM_TAG],
            "reviewers": STEWARD_REVIEWERS,
            "relatedTerms": [
                {"fullyQualifiedName": "Business Glossary.Party"},
                {"fullyQualifiedName": f"{GLOSSARY}.Stale"},
            ],
        }
    }
    calls = _mock_transport(monkeypatch, live)
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot([_term(1, "Customer", definition="A paying party.")])
    )
    assert result.ok
    (body,) = _term_puts(calls)
    # Provisa authors the description; steward curation rides through; a stale in-glossary
    # relatedTerm tracks the snapshot (which has no edges) and is gone.
    assert body["description"] == "A paying party."
    assert body["tags"] == [STEWARD_TERM_TAG]
    assert body["reviewers"] == STEWARD_REVIEWERS
    assert body["relatedTerms"] == ["Business Glossary.Party"]


@pytest.mark.asyncio
async def test_term_to_asset_assignment_is_never_written(monkeypatch):
    calls = _mock_transport(monkeypatch)
    ref = AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "customer_id"))
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot([_term(1, "Customer", refs=(ref,))])
    )
    assert result.ok
    # REQ-1389: term-to-asset assignment is HUMAN-OWNED. The ref never leaves the model —
    # no table/column endpoint is touched and no payload names the referenced asset.
    assert all("/api/v1/glossar" in url for _, url, _ in calls)
    assert "customer_id" not in jsonlib.dumps([body for _, _, body in calls if body is not None])


@pytest.mark.asyncio
async def test_term_without_definition_publishes_the_required_empty_description(monkeypatch):
    calls = _mock_transport(monkeypatch)
    result = await OpenMetadataExport(_export_config()).publish(
        _snapshot([_term(1, "Customer", definition=None)])
    )
    assert result.ok
    (body,) = _term_puts(calls)
    # CreateGlossaryTerm requires the field; an undefined term must not be withheld.
    assert body["description"] == ""
