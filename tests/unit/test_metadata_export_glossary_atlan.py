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

from dataclasses import replace

import httpx
import pytest

from provisa.api.metadata_export.atlan import (
    ATLAN_ASSIGNMENT_ATTRIBUTES,
    GLOSSARY_NAME,
    GLOSSARY_TYPE,
    TERM_DEPRECATION_STATUS,
    TERM_TYPE,
    AtlanExport,
)
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
    SourceAsset,
    TableAsset,
)
from provisa.api.metadata_export.atlas import ASSIGNMENT_SOURCE, SEMANTIC_ASSIGNMENT
from provisa.api.metadata_export.provider import PublishResult
from provisa.core.models import MetadataExportTarget

ORG = "org-1"
GLOSSARY_QN = f"provisa/{ORG}/glossary"
GLOSSARY_PROBE_ROUTE = f"/api/meta/entity/uniqueAttribute/type/{GLOSSARY_TYPE}"


def _export(bindings: dict[str, tuple[str, str]] | None = None) -> AtlanExport:
    export = AtlanExport(
        MetadataExportTarget(
            id="catalog",
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
    snapshot but present in the bindings is read only for Provisa's own assignments to
    withdraw, and is never deleted."""
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
    assert not any(url.endswith("/ext-9") for url in fake.fetched)
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


# --- Term-to-asset assignment (REQ-1389, amended) -------------------------------------------

ORDERS = AssetRef(kind=AssetKind.TABLE, parts=("wh", "public", "orders"))
AMOUNT = AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "amount"))
ORDERS_URI = f"provisa://{ORG}/sales/tables/orders"
AMOUNT_URI = f"{ORDERS_URI}#field:amount"


def _with_table(terms: list[GlossaryTermAsset]) -> MetadataSnapshot:
    column = ColumnAsset(
        ref=AMOUNT, name="amount", data_type="numeric", description="", semantic_uri=AMOUNT_URI
    )
    table = TableAsset(
        ref=ORDERS,
        name="orders",
        source_id="wh",
        domain_id=None,
        description="",
        columns=[column],
        semantic_uri=ORDERS_URI,
    )
    source = SourceAsset(
        ref=AssetRef(kind=AssetKind.SOURCE, parts=("wh",)),
        id="wh",
        source_type="postgresql",
        description="",
    )
    return MetadataSnapshot(org_id=ORG, sources=[source], tables=[table], glossary_terms=terms)


def _published_assets(monkeypatch) -> None:
    """The asset bulk already ran: its guids are what the assignments address."""

    async def _entities(self, entities, snapshot):
        result = PublishResult(provider_name="atlan")
        result.bindings[ORDERS_URI] = ("orders-guid", "orders")
        result.bindings[AMOUNT_URI] = ("amount-guid", "amount")
        return result

    monkeypatch.setattr(AtlanExport, "_publish_entities", _entities)


def _provisa_assignment(type_name: str, guid: str) -> dict:
    return {
        "typeName": type_name,
        "guid": guid,
        "relationshipAttributes": {
            "typeName": SEMANTIC_ASSIGNMENT,
            "attributes": {"source": ASSIGNMENT_SOURCE},
        },
    }


async def test_a_class_term_is_assigned_to_its_table_and_a_property_to_its_column(monkeypatch):
    """Provisa appends its assignments in the term's own bulk entry, attributed by source,
    without replacing the term's assignment list."""
    _published_assets(monkeypatch)
    fake = _FakeAtlan(glossary_guid="g-1")
    fake.install(monkeypatch)
    order = replace(_term(1, "order"), role="class", tables=(ORDERS,))
    amount = replace(_term(2, "amount"), role="property", refs=(AMOUNT,))
    result = await _export().publish(_with_table([order, amount, _term(3, "party")]))
    assert result.ok, result.errors
    by_name = {e["attributes"]["name"]: e for e in _bulk_entities(fake)}
    assert by_name["order"]["appendRelationshipAttributes"] == {
        "assignedEntities": [
            {
                "typeName": "Table",
                "guid": "orders-guid",
                "relationshipType": SEMANTIC_ASSIGNMENT,
                "relationshipAttributes": {
                    "typeName": SEMANTIC_ASSIGNMENT,
                    "attributes": ATLAN_ASSIGNMENT_ATTRIBUTES,
                },
            }
        ]
    }
    (column,) = by_name["amount"]["appendRelationshipAttributes"]["assignedEntities"]
    assert (column["typeName"], column["guid"]) == ("Column", "amount-guid")
    assert "appendRelationshipAttributes" not in by_name["party"]
    assert all(
        "relationshipAttributes" not in e or "assignedEntities" not in e["relationshipAttributes"]
        for e in _bulk_entities(fake)
    )


async def test_human_assignments_stand_and_provisas_stale_ones_are_removed(monkeypatch):
    """A steward's assignment is never removed or duplicated; Provisa's own assignment to an
    asset the term no longer grounds is removed, and a term that left the snapshot loses
    only Provisa's assignments."""
    _published_assets(monkeypatch)
    anchor = {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "g-1"}}
    live = {
        "t-order": {
            "guid": "t-order",
            "attributes": {"qualifiedName": f"order@{GLOSSARY_QN}", "name": "order"},
            "relationshipAttributes": {
                **anchor,
                "assignedEntities": [
                    {"typeName": "Table", "guid": "orders-guid"},  # a steward's
                    _provisa_assignment("Table", "old-guid"),
                ],
            },
        },
        "t-margin": {
            "guid": "t-margin",
            "attributes": {"qualifiedName": f"margin@{GLOSSARY_QN}", "name": "margin"},
            "relationshipAttributes": {
                **anchor,
                "assignedEntities": [_provisa_assignment("Column", "amount-guid")],
            },
        },
    }
    fake = _FakeAtlan(glossary_guid="g-1", live_terms=live)
    fake.install(monkeypatch)
    export = _export(
        bindings={
            f"provisa://{ORG}/terms/order": ("t-order", "order@x"),
            f"provisa://{ORG}/terms/margin": ("t-margin", "margin@x"),
        }
    )
    order = replace(_term(1, "order"), role="class", tables=(ORDERS,))
    result = await export.publish(_with_table([order]))
    assert result.ok, result.errors
    by_guid = {e["guid"]: e for e in _bulk_entities(fake)}
    assert "appendRelationshipAttributes" not in by_guid["t-order"]
    assert by_guid["t-order"]["removeRelationshipAttributes"] == {
        "assignedEntities": [{"typeName": "Table", "guid": "old-guid"}]
    }
    assert by_guid["t-margin"]["removeRelationshipAttributes"] == {
        "assignedEntities": [{"typeName": "Column", "guid": "amount-guid"}]
    }
    # The departed term is sent with its live attributes, unchanged.
    assert by_guid["t-margin"]["attributes"] == live["t-margin"]["attributes"]
    assert fake.deleted == []


async def test_every_term_carries_its_role_and_a_stewards_attributes_stand(monkeypatch):
    """REQ-2141: the role rides additionalAttributes under provisaRole; a steward's entries in
    the map stand."""
    live = {
        "t-order": {
            "guid": "t-order",
            "attributes": {
                "qualifiedName": f"order@{GLOSSARY_QN}",
                "name": "order",
                "additionalAttributes": {"owner_team": "finance"},
            },
            "relationshipAttributes": {"anchor": {"typeName": GLOSSARY_TYPE, "guid": "g-1"}},
        }
    }
    fake = _FakeAtlan(glossary_guid="g-1", live_terms=live)
    fake.install(monkeypatch)
    export = _export(bindings={f"provisa://{ORG}/terms/order": ("t-order", "order@x")})
    order = replace(_term(1, "order"), role="class")
    result = await export.publish(_snapshot([order, _term(2, "party")]))
    assert result.ok, result.errors
    by_name = {e["attributes"]["name"]: e["attributes"] for e in _bulk_entities(fake)}
    assert by_name["order"]["additionalAttributes"] == {
        "owner_team": "finance",
        "provisaRole": "class",
    }
    assert by_name["party"]["additionalAttributes"] == {"provisaRole": "concept"}
