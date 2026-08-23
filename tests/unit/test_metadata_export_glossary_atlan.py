# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2f9a14-5e6b-4d38-9a70-1b84d3c6f2e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1387: native glossary publishing on Atlan, under the ownership constraint.

Every term lands in the one Provisa-owned glossary; the stored binding is the ownership
record — a bound term updates in place by guid, an unbound vendor item is never touched,
and nothing is ever deleted. Transport is monkeypatched at the httpx.AsyncClient method
level, the same approach as the DataHub merge tests in test_metadata_export_vendors.py.
"""

# Requirements: REQ-1387, REQ-1389

from __future__ import annotations

import httpx
import pytest

from provisa.api.metadata_export.atlan import (
    GLOSSARY_NAME,
    GLOSSARY_TYPE,
    TERM_DEPRECATION_STATUS,
    TERM_TYPE,
    AtlanExport,
)
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
)
from provisa.core.models import MetadataExportConfig

ORG = "org-1"
GLOSSARY_QN = f"provisa/{ORG}/glossary"
GLOSSARY_PROBE_ROUTE = f"/api/meta/entity/uniqueAttribute/type/{GLOSSARY_TYPE}"


def _export(bindings: dict[str, tuple[str, str]] | None = None) -> AtlanExport:
    export = AtlanExport(
        MetadataExportConfig(
            enabled=True,
            provider="atlan",
            endpoint="https://tenant.atlan.com",
            token="t",
            timeout_seconds=5,
        )
    )
    if bindings is not None:
        export.stored_bindings = bindings
    return export


def _term(
    term_id: int,
    name: str,
    *,
    definition: str | None = "a definition",
    deprecated: bool = False,
    refs: tuple[AssetRef, ...] = (),
) -> GlossaryTermAsset:
    return GlossaryTermAsset(
        term_id=term_id,
        name=name,
        definition=definition,
        is_abstract=not refs,
        deprecated=deprecated,
        refs=refs,
        semantic_uri=f"provisa://{ORG}/terms/{name}",
    )


def _snapshot(
    terms: list[GlossaryTermAsset], edges: list[GlossaryTermEdge] | None = None
) -> MetadataSnapshot:
    # Terms only: with no sources or tables the asset publish sends nothing, so every
    # request the fake sees is glossary traffic.
    return MetadataSnapshot(org_id=ORG, glossary_terms=terms, glossary_edges=edges or [])


class _FakeAtlan:
    """Atlan's glossary surface: the container probe, per-guid reads, the bulk route.

    ``glossary_guid`` of None answers the container probe with 404 (nothing to reuse);
    ``live_terms`` maps guid -> the entity body a per-guid GET returns.
    """

    def __init__(
        self, glossary_guid: str | None = None, live_terms: dict[str, dict] | None = None
    ) -> None:
        self.glossary_guid = glossary_guid
        self.live_terms = live_terms or {}
        self.posted: list[dict] = []
        self.fetched: list[str] = []
        self.deleted: list[str] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = self

        async def _get(self, url, params=None, headers=None):
            fake.fetched.append(url)
            request = httpx.Request("GET", url)
            if GLOSSARY_PROBE_ROUTE in url:
                if fake.glossary_guid is None:
                    return httpx.Response(404, request=request)
                body = {
                    "entity": {
                        "guid": fake.glossary_guid,
                        "attributes": {"qualifiedName": GLOSSARY_QN, "name": GLOSSARY_NAME},
                    }
                }
                return httpx.Response(200, json=body, request=request)
            guid = url.rsplit("/", 1)[-1]
            if guid in fake.live_terms:
                return httpx.Response(200, json={"entity": fake.live_terms[guid]}, request=request)
            return httpx.Response(404, request=request)

        async def _post(self, url, json=None, headers=None):
            fake.posted.append(json)
            assignments = {
                entity["guid"]: f"srv{abs(int(entity['guid']))}"
                for entity in json["entities"]
                if entity["guid"].startswith("-")
            }
            return httpx.Response(
                200, json={"guidAssignments": assignments}, request=httpx.Request("POST", url)
            )

        async def _delete(self, url, headers=None):
            fake.deleted.append(url)
            return httpx.Response(200, request=httpx.Request("DELETE", url))

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)
        monkeypatch.setattr(httpx.AsyncClient, "post", _post)
        monkeypatch.setattr(httpx.AsyncClient, "delete", _delete)


def _bulk_entities(fake: _FakeAtlan) -> list[dict]:
    assert len(fake.posted) == 1
    return fake.posted[0]["entities"]


# --- Container idempotency ------------------------------------------------------------------


async def test_first_publish_creates_the_provisa_glossary_and_anchors_the_term_in_it(
    monkeypatch,
):
    fake = _FakeAtlan(glossary_guid=None)
    fake.install(monkeypatch)
    result = await _export().publish(_snapshot([_term(1, "customer")]))
    assert result.ok
    entities = _bulk_entities(fake)
    glossary, term = entities
    assert glossary["typeName"] == GLOSSARY_TYPE
    assert glossary["attributes"]["qualifiedName"] == GLOSSARY_QN
    assert glossary["attributes"]["name"] == GLOSSARY_NAME
    assert term["typeName"] == TERM_TYPE
    # The anchor names the container by its in-batch placeholder guid.
    assert term["relationshipAttributes"]["anchor"] == {
        "typeName": GLOSSARY_TYPE,
        "guid": glossary["guid"],
    }
    assert result.published == {"glossary": 1, "term": 1}


async def test_an_existing_glossary_is_reused_never_recreated(monkeypatch):
    fake = _FakeAtlan(glossary_guid="g-1")
    fake.install(monkeypatch)
    result = await _export().publish(_snapshot([_term(1, "customer")]))
    assert result.ok
    entities = _bulk_entities(fake)
    assert [entity["typeName"] for entity in entities] == [TERM_TYPE]
    assert entities[0]["relationshipAttributes"]["anchor"]["guid"] == "g-1"
    assert result.published == {"term": 1}


# --- Term create and binding capture --------------------------------------------------------


async def test_term_create_captures_the_binding_by_semantic_uri(monkeypatch):
    fake = _FakeAtlan(glossary_guid="g-1")
    fake.install(monkeypatch)
    result = await _export().publish(_snapshot([_term(1, "customer")]))
    assert result.ok
    (entity,) = _bulk_entities(fake)
    assert entity["attributes"]["qualifiedName"] == f"customer@{GLOSSARY_QN}"
    assert entity["attributes"]["longDescription"] == "a definition"
    # REQ-1389: userDescription belongs to humans in Atlan's UI — never written.
    assert "userDescription" not in entity["attributes"]
    # The placeholder resolved through guidAssignments IS the ownership record.
    assert result.bindings[f"provisa://{ORG}/terms/customer"] == (
        "srv1",
        f"customer@{GLOSSARY_QN}",
    )


# --- Rename via the stored binding ----------------------------------------------------------


async def test_a_stored_binding_updates_the_same_vendor_term_in_place(monkeypatch):
    """The binding's guid is the identity: the vendor-side name-key no longer matches the
    term's name, and the publish still UPDATES that entity — read-merged, so the steward's
    userDescription survives and the live qualifiedName is kept verbatim."""
    live = {
        "guid": "t-1",
        "attributes": {
            "qualifiedName": f"cust@{GLOSSARY_QN}",
            "name": "cust",
            "longDescription": "old definition",
            "userDescription": "steward note",
        },
        "relationshipAttributes": {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "g-1"}},
    }
    fake = _FakeAtlan(glossary_guid="g-1", live_terms={"t-1": live})
    fake.install(monkeypatch)
    uri = f"provisa://{ORG}/terms/customer"
    export = _export(bindings={uri: ("t-1", f"cust@{GLOSSARY_QN}")})
    result = await export.publish(_snapshot([_term(1, "customer", definition="new definition")]))
    assert result.ok
    (entity,) = _bulk_entities(fake)
    assert entity["guid"] == "t-1"
    assert entity["attributes"]["qualifiedName"] == f"cust@{GLOSSARY_QN}"
    assert entity["attributes"]["name"] == "customer"
    assert entity["attributes"]["longDescription"] == "new definition"
    assert entity["attributes"]["userDescription"] == "steward note"
    assert result.bindings[uri] == ("t-1", f"cust@{GLOSSARY_QN}")


# --- Ownership: unbound and external items untouched, never a delete ------------------------


async def test_unbound_vendor_items_are_untouched_and_nothing_is_ever_deleted(monkeypatch):
    """An external term in the catalog is never read or written; a term absent from the
    snapshot but present in the bindings is simply not sent — never deleted."""
    external = {
        "guid": "ext-9",
        "attributes": {"qualifiedName": "revenue@someone-elses-glossary", "name": "revenue"},
        "relationshipAttributes": {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "other-g"}},
    }
    fake = _FakeAtlan(glossary_guid="g-1", live_terms={"ext-9": external})
    fake.install(monkeypatch)
    export = _export(bindings={f"provisa://{ORG}/terms/retired": ("t-retired", "retired@x")})
    result = await export.publish(_snapshot([_term(1, "customer")]))
    assert result.ok
    assert fake.deleted == []
    assert not any(url.endswith("/ext-9") or url.endswith("/t-retired") for url in fake.fetched)
    assert {entity["guid"] for entity in _bulk_entities(fake)} == {"-1"}


async def test_a_bound_term_anchored_in_a_foreign_glossary_is_refused_not_modified(
    monkeypatch,
):
    foreign = {
        "guid": "f-1",
        "attributes": {"qualifiedName": f"customer@{GLOSSARY_QN}", "name": "customer"},
        "relationshipAttributes": {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "other-g"}},
    }
    fake = _FakeAtlan(glossary_guid="g-1", live_terms={"f-1": foreign})
    fake.install(monkeypatch)
    export = _export(bindings={f"provisa://{ORG}/terms/customer": ("f-1", "customer@x")})
    result = await export.publish(_snapshot([_term(1, "customer")]))
    assert not result.ok
    assert "outside the Provisa glossary" in result.errors[0].message
    # Nothing to send once the only term is refused — the foreign entity is never written.
    assert fake.posted == []
    assert fake.deleted == []


# --- Term-to-term relations -----------------------------------------------------------------


async def test_every_provisa_relation_maps_to_its_nearest_atlan_term_relation(monkeypatch):
    fake = _FakeAtlan(glossary_guid="g-1")
    fake.install(monkeypatch)
    terms = [_term(n, name) for n, name in enumerate(("client", "party", "buyer", "order"), 1)]
    edges = [
        GlossaryTermEdge(from_term_id=1, to_term_id=2, rel_type="KIND_OF"),
        GlossaryTermEdge(from_term_id=1, to_term_id=3, rel_type="SYNONYM_OF"),
        GlossaryTermEdge(from_term_id=1, to_term_id=4, rel_type="RELATED_TO"),
        GlossaryTermEdge(from_term_id=2, to_term_id=3, rel_type="PART_OF"),
    ]
    result = await _export().publish(_snapshot(terms, edges))
    assert result.ok
    by_name = {e["attributes"]["name"]: e for e in _bulk_entities(fake)}
    guid_of = {name: by_name[name]["guid"] for name in by_name}
    client_rels = by_name["client"]["relationshipAttributes"]
    assert client_rels["isA"] == [{"typeName": TERM_TYPE, "guid": guid_of["party"]}]
    assert client_rels["synonyms"] == [{"typeName": TERM_TYPE, "guid": guid_of["buyer"]}]
    assert client_rels["seeAlso"] == [{"typeName": TERM_TYPE, "guid": guid_of["order"]}]
    # PART_OF: Atlas's glossary model has no term-to-term meronymy, so it rides seeAlso.
    assert by_name["party"]["relationshipAttributes"]["seeAlso"] == [
        {"typeName": TERM_TYPE, "guid": guid_of["buyer"]}
    ]


# --- Human-owned surfaces -------------------------------------------------------------------


async def test_term_refs_never_become_asset_assignments(monkeypatch):
    """REQ-1389: term-to-asset assignment is HUMAN-OWNED; refs stay Provisa-internal."""
    fake = _FakeAtlan(glossary_guid="g-1")
    fake.install(monkeypatch)
    ref = AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "cust_id"))
    result = await _export().publish(_snapshot([_term(1, "customer", refs=(ref,))]))
    assert result.ok
    (entity,) = _bulk_entities(fake)
    assert set(entity["relationshipAttributes"]) == {"anchor"}
    assert "assignedEntities" not in entity["relationshipAttributes"]
    assert "cust_id" not in str(entity)


# --- Deprecation convention -----------------------------------------------------------------


async def test_deprecation_rides_certificate_status(monkeypatch):
    fake = _FakeAtlan(glossary_guid="g-1")
    fake.install(monkeypatch)
    result = await _export().publish(_snapshot([_term(1, "legacy", deprecated=True)]))
    assert result.ok
    (entity,) = _bulk_entities(fake)
    assert entity["attributes"]["certificateStatus"] == TERM_DEPRECATION_STATUS
    assert "Provisa registry" in entity["attributes"]["certificateStatusMessage"]


async def test_only_provisas_own_deprecation_marker_is_cleared(monkeypatch):
    """Un-deprecating clears the DEPRECATED Provisa wrote; a human-set status survives."""
    was_deprecated = {
        "guid": "t-1",
        "attributes": {
            "qualifiedName": f"legacy@{GLOSSARY_QN}",
            "name": "legacy",
            "certificateStatus": TERM_DEPRECATION_STATUS,
            "certificateStatusMessage": "Deprecated in the Provisa registry.",
        },
        "relationshipAttributes": {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "g-1"}},
    }
    human_verified = {
        "guid": "t-2",
        "attributes": {
            "qualifiedName": f"customer@{GLOSSARY_QN}",
            "name": "customer",
            "certificateStatus": "VERIFIED",
        },
        "relationshipAttributes": {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "g-1"}},
    }
    fake = _FakeAtlan(
        glossary_guid="g-1", live_terms={"t-1": was_deprecated, "t-2": human_verified}
    )
    fake.install(monkeypatch)
    export = _export(
        bindings={
            f"provisa://{ORG}/terms/legacy": ("t-1", f"legacy@{GLOSSARY_QN}"),
            f"provisa://{ORG}/terms/customer": ("t-2", f"customer@{GLOSSARY_QN}"),
        }
    )
    result = await export.publish(_snapshot([_term(1, "legacy"), _term(2, "customer")]))
    assert result.ok
    by_name = {e["attributes"]["name"]: e for e in _bulk_entities(fake)}
    assert "certificateStatus" not in by_name["legacy"]["attributes"]
    assert "certificateStatusMessage" not in by_name["legacy"]["attributes"]
    assert by_name["customer"]["attributes"]["certificateStatus"] == "VERIFIED"
