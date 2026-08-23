# Copyright (c) 2026 Kenneth Stott
# Canary: 3233488b-c2ac-4b63-bd52-2612a288611f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1387: the DataHub adapter publishes the term graph as a native business glossary.

Everything lands inside one Provisa-owned ``glossaryNode`` per org; a term URN derives from
the term's name, so a rename mints a new URN and the old bound URN is deprecated toward its
successor (REQ-1389 succession). Term-to-asset assignment stays human-owned: no
``glossaryTerms`` aspect is ever written on a dataset, and no delete is ever emitted.
"""

# Requirements: REQ-1387, REQ-1389

from __future__ import annotations

import json

import httpx

from provisa.api.metadata_export.datahub import (
    DataHubExport,
    glossary_node_urn,
    glossary_term_urn,
    to_proposals,
)
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    ColumnAsset,
    GlossaryTermAsset,
    GlossaryTermEdge,
    MetadataSnapshot,
    TableAsset,
)
from provisa.core.models import MetadataExportConfig

ORG = "acme"
NODE_URN = glossary_node_urn(ORG)


def _term(term_id: int, name: str, *, deprecated: bool = False) -> GlossaryTermAsset:
    return GlossaryTermAsset(
        term_id=term_id,
        name=name,
        definition=f"definition of {name}",
        is_abstract=False,
        deprecated=deprecated,
        experts=("alice",) if name == "customer" else (),
        semantic_uri=f"provisa://{ORG}/terms/{name}",
    )


def _snapshot(
    terms: list[GlossaryTermAsset], edges: list[GlossaryTermEdge] | None = None
) -> MetadataSnapshot:
    """A snapshot with one published table, so the dataset aspects publish alongside the
    glossary and the human-owned assignment boundary is observable."""
    table_ref = AssetRef(kind=AssetKind.TABLE, parts=("wh", "public", "orders"))
    column_ref = AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "cust_id"))
    return MetadataSnapshot(
        org_id=ORG,
        tables=[
            TableAsset(
                ref=table_ref,
                name="orders",
                source_id="wh",
                domain_id=None,
                description="orders",
                columns=[
                    ColumnAsset(ref=column_ref, name="cust_id", data_type="integer", description="")
                ],
                semantic_uri=f"provisa://{ORG}/sales/tables/orders",
            )
        ],
        glossary_terms=terms,
        glossary_edges=edges or [],
    )


def _export() -> DataHubExport:
    return DataHubExport(
        MetadataExportConfig(
            enabled=True, provider="datahub", endpoint="https://gms.example", timeout_seconds=5
        )
    )


async def _publish(export: DataHubExport, snapshot: MetadataSnapshot, monkeypatch):
    posted: list[dict] = []

    async def _post(self, url, json=None, headers=None):
        posted.append(json)
        return httpx.Response(200, request=httpx.Request("POST", url))

    async def _get(self, url, headers=None):
        return httpx.Response(404, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", _post)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return await export.publish(snapshot), posted


# --- Proposal shape -------------------------------------------------------------------------


def test_terms_publish_under_the_orgs_own_provisa_node():
    proposals = to_proposals(_snapshot([_term(1, "customer")]))
    node = next(p for p in proposals if p.aspect_name == "glossaryNodeInfo")
    assert node.entity_type == "glossaryNode"
    assert node.urn == NODE_URN == f"urn:li:glossaryNode:provisa.{ORG}"
    assert node.aspect["name"] == f"Provisa ({ORG})"
    term = next(p for p in proposals if p.aspect_name == "glossaryTermInfo")
    assert term.entity_type == "glossaryTerm"
    assert term.urn == glossary_term_urn(ORG, "customer")
    assert term.aspect["name"] == "customer"
    assert term.aspect["definition"] == "definition of customer"
    assert term.aspect["parentNode"] == NODE_URN
    assert term.aspect["customProperties"]["provisaUri"] == f"provisa://{ORG}/terms/customer"
    assert term.aspect["customProperties"]["provisaTermId"] == "1"
    assert term.aspect["customProperties"]["provisaExperts"] == "alice"
    # The node is proposed before any term referencing it as parentNode.
    assert proposals.index(node) < proposals.index(term)


def test_a_snapshot_without_terms_emits_no_glossary_aspects():
    proposals = to_proposals(_snapshot([]))
    assert not [p for p in proposals if p.entity_type in {"glossaryNode", "glossaryTerm"}]


def test_a_deprecated_term_carries_datahubs_native_deprecation_aspect():
    proposals = to_proposals(_snapshot([_term(1, "legacy_margin", deprecated=True)]))
    deprecation = next(
        p for p in proposals if p.aspect_name == "deprecation" and p.entity_type == "glossaryTerm"
    )
    assert deprecation.urn == glossary_term_urn(ORG, "legacy_margin")
    assert deprecation.aspect["deprecated"] is True


# --- Edge mapping ---------------------------------------------------------------------------


def test_edges_map_onto_the_native_related_terms_fields():
    terms = [_term(1, "customer"), _term(2, "party"), _term(3, "address"), _term(4, "client")]
    edges = [
        GlossaryTermEdge(from_term_id=1, to_term_id=2, rel_type="KIND_OF"),
        GlossaryTermEdge(from_term_id=3, to_term_id=1, rel_type="PART_OF"),
        GlossaryTermEdge(from_term_id=1, to_term_id=3, rel_type="RELATED_TO"),
        GlossaryTermEdge(from_term_id=1, to_term_id=4, rel_type="SYNONYM_OF"),
    ]
    related = {
        p.urn: p.aspect
        for p in to_proposals(_snapshot(terms, edges))
        if p.aspect_name == "glossaryRelatedTerms"
    }
    customer = related[glossary_term_urn(ORG, "customer")]
    # KIND_OF: the subtype inherits (is-a) the supertype.
    assert customer["isRelatedTerms"] == [glossary_term_urn(ORG, "party")]
    # PART_OF is inverted onto the whole: the container "has" the part.
    assert customer["hasRelatedTerms"] == [glossary_term_urn(ORG, "address")]
    # RELATED_TO and SYNONYM_OF both ride relatedTerms; synonym is emitted symmetrically.
    assert customer["relatedTerms"] == [
        glossary_term_urn(ORG, "address"),
        glossary_term_urn(ORG, "client"),
    ]
    assert related[glossary_term_urn(ORG, "client")]["relatedTerms"] == [
        glossary_term_urn(ORG, "customer")
    ]
    # No aspect for a term with no edges pointing at it in a mapped direction.
    assert glossary_term_urn(ORG, "party") not in related


# --- Ownership boundaries -------------------------------------------------------------------


def test_no_dataset_ever_gets_a_glossary_terms_aspect():
    """Term-to-asset assignment is human-owned (REQ-1389): the adapter maps refs into
    nothing — a curated assignment in DataHub is never overwritten or duplicated."""
    ref = AssetRef(kind=AssetKind.COLUMN, parts=("wh", "public", "orders", "cust_id"))
    terms = [
        GlossaryTermAsset(
            term_id=1,
            name="customer",
            definition="buyer",
            is_abstract=False,
            deprecated=False,
            refs=(ref,),
            semantic_uri=f"provisa://{ORG}/terms/customer",
        )
    ]
    for proposal in to_proposals(_snapshot(terms)):
        assert proposal.aspect_name != "glossaryTerms"
        if proposal.aspect_name == "schemaMetadata":
            assert "glossaryTerms" not in json.dumps(proposal.aspect)


def test_every_glossary_proposal_is_an_upsert_in_the_provisa_namespace_never_a_delete():
    terms = [_term(1, "customer"), _term(2, "party", deprecated=True)]
    edges = [GlossaryTermEdge(from_term_id=1, to_term_id=2, rel_type="KIND_OF")]
    glossary = [
        p
        for p in to_proposals(_snapshot(terms, edges))
        if p.entity_type in {"glossaryNode", "glossaryTerm"}
    ]
    assert glossary
    for proposal in glossary:
        assert proposal.payload()["proposal"]["changeType"] == "UPSERT"
        assert proposal.urn.startswith(
            (f"urn:li:glossaryNode:provisa.{ORG}", f"urn:li:glossaryTerm:provisa.{ORG}.")
        )


# --- Bindings and rename succession ---------------------------------------------------------


async def test_publish_captures_each_terms_urn_as_its_binding(monkeypatch):
    result, _ = await _publish(_export(), _snapshot([_term(7, "customer")]), monkeypatch)
    assert result.ok
    assert result.published["glossary_node"] == 1
    assert result.published["glossary_term"] == 1
    assert result.bindings[f"provisa://{ORG}/terms/customer"] == (
        glossary_term_urn(ORG, "customer"),
        "term:7",
    )


async def test_a_renamed_term_deprecates_its_old_urn_toward_the_successor(monkeypatch):
    """The URN is immutable identity; the stable term id in the binding is what ties the
    old URN to the renamed term (REQ-1389 succession, the dataset rebind pattern)."""
    export = _export()
    old_urn = glossary_term_urn(ORG, "customer")
    export.stored_bindings = {f"provisa://{ORG}/terms/customer": (old_urn, "term:7")}
    result, posted = await _publish(export, _snapshot([_term(7, "client")]), monkeypatch)
    assert result.ok
    deprecations = [
        p["proposal"]
        for p in posted
        if p["proposal"]["aspectName"] == "deprecation" and p["proposal"]["entityUrn"] == old_urn
    ]
    assert len(deprecations) == 1
    aspect = json.loads(deprecations[0]["aspect"]["value"])
    assert aspect["deprecated"] is True
    assert glossary_term_urn(ORG, "client") in aspect["note"]
    assert f"provisa://{ORG}/terms/client" in aspect["note"]
    # The new URN publishes as usual and the binding moves to the new URI, same stable id.
    assert result.bindings[f"provisa://{ORG}/terms/client"] == (
        glossary_term_urn(ORG, "client"),
        "term:7",
    )


async def test_an_unrenamed_term_and_a_foreign_urn_are_never_touched(monkeypatch):
    """A stored binding outside the org's Provisa namespace is skipped, not deprecated —
    the adapter never modifies a glossary entity it does not own (REQ-1387)."""
    export = _export()
    foreign = "urn:li:glossaryTerm:Classification.Sensitive"
    export.stored_bindings = {
        f"provisa://{ORG}/terms/customer": (glossary_term_urn(ORG, "customer"), "term:7"),
        f"provisa://{ORG}/terms/renamed-foreign": (foreign, "term:8"),
    }
    result, posted = await _publish(
        export, _snapshot([_term(7, "customer"), _term(8, "vendor")]), monkeypatch
    )
    assert result.ok
    assert not [p for p in posted if p["proposal"]["aspectName"] == "deprecation"]
    assert foreign not in {p["proposal"]["entityUrn"] for p in posted}
