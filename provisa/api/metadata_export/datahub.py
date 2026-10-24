# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2c9a15-4e83-4b60-96af-0d51e7c3b842
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DataHub adapter (REQ-1069).

DataHub does not take entities; it takes *aspects*. One asset is a URN, and everything known
about it is a set of independently-versioned aspect documents attached to that URN — schema,
ownership, tags, upstream lineage. Each is proposed separately through
``/aspects?action=ingestProposal``.

That shape is why this is not an Atlas subclass. It also gives the mapping a property the
others lack: an aspect Provisa does not own is never touched, so a description curated in
DataHub outside the ``datasetProperties`` aspect survives a republish.

The mapping:

* a table is a ``dataset`` URN on the ``provisa`` platform, carrying ``datasetProperties``
  and ``schemaMetadata`` (its columns, with types and descriptions);
* a domain steward is an ``ownership`` aspect naming a ``corpuser`` URN (REQ-609);
* a governance signal is a ``globalTags`` aspect entry, and the tag itself is a ``tag`` URN
  with its own ``tagProperties`` (REQ-1071);
* column lineage is an ``upstreamLineage`` aspect carrying both the table-level upstreams and
  the ``fineGrainedLineages`` DataHub uses for column-level edges (REQ-1070).

Single-writer merge (REQ-1389): human edits made in the DataHub UI land in the editable*
aspects — ``editableDatasetProperties``, ``editableSchemaMetadata`` — which this adapter never
writes, so descriptions and per-field documentation curated in DataHub already survive every
publish. The one aspect where human and Provisa authorship collide is ``globalTags``: a tag a
steward attaches in the UI lands in the same aspect this adapter UPSERTs (an aspect UPSERT is a
whole-document replace). ``_merge_global_tags`` closes that gap by reading the live aspect and
carrying every non-``provisa_``-prefixed tag through unchanged, while the ``provisa_`` tags are
set to exactly what the snapshot says (stale ones drop out).

URN-canonical rebind (REQ-1389) is NOT implementable on DataHub: a dataset URN is an immutable
identifier derived from the physical fqn — GMS has no rename/re-key API — so a re-platformed
table necessarily mints a new dataset URN. What the stored binding buys instead: the exporter
captures each table's dataset URN at publish time, and when the binding shows the URN changed
(physical re-address) it publishes the new URN as usual AND sets the ``deprecation`` aspect on
the OLD bound URN — a note naming the successor URN and the canonical Provisa URN — so the
lingering entity is visibly superseded rather than a silent duplicate. The Provisa URN is
still published as the ``provisaUri`` customProperty (and ``externalUrl``).

Business glossary (REQ-1387): terms publish as native ``glossaryTerm`` entities under one
Provisa-owned ``glossaryNode`` per org (``urn:li:glossaryNode:provisa.<org>``), so everything
this adapter writes in the glossary sits inside a namespace it visibly owns — a term or node
outside that namespace is never proposed, and no delete is ever emitted. Term-to-asset
assignment (REQ-1389, amended): Provisa assigns a Class term to the datasets it grounds and a
Property term to its schema fields, each association attributed to Provisa's actor. A field's
terms ride ``schemaMetadata``, which Provisa writes whole (a steward's field terms land in
``editableSchemaMetadata``, never written here); a dataset's ride the shared ``glossaryTerms``
aspect, read-merged so every association not attributed to Provisa stands. The typed term
edges map onto ``glossaryRelatedTerms``:

* ``KIND_OF``   → ``isRelatedTerms`` on the subtype (DataHub's "Inherits" / is-a relation);
* ``PART_OF``   → ``hasRelatedTerms`` on the WHOLE (DataHub's "Contains" / has-a relation,
  which points from container to part — the edge is inverted to fit);
* ``RELATED_TO`` → ``relatedTerms`` on the source (DataHub's "Related Terms");
* ``SYNONYM_OF`` → ``relatedTerms`` on BOTH endpoints — DataHub has no synonym field, and
  emitting the closest relation symmetrically is what preserves the symmetry a synonym means.

A glossaryTerm URN is immutable identity like a dataset URN, and it derives from the term's
name-based semantic URI — so a RENAME in Provisa mints a new term URN. The stored binding
carries the stable Provisa term id, and when it shows the URN changed the old bound URN gets
the ``deprecation`` aspect naming its successor, the same succession pattern datasets use.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071, REQ-1387, REQ-1389

from __future__ import annotations

import json
from hashlib import sha256

from dataclasses import dataclass, replace
from urllib.parse import quote
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.provider import (
    AssetError,
    AssetRefStub,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.model import junction_payload
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        AssetRef,
        GlossaryTermAsset,
        MetadataSnapshot,
        TableAsset,
    )

# The data platform every published dataset is attributed to. Provisa is the governed access
# path, and a consumer resolving one of these URNs has to arrive back at Provisa rather than at
# a federated source it has no policy-enforced route to.
PLATFORM = "provisa"

# DataHub environments are a fixed enum (PROD, DEV, …). Everything Provisa governs is
# production data regardless of which environment Provisa itself runs in — the distinction
# DataHub is drawing is about the data, not about the publisher.
FABRIC = "PROD"

TAG_PREFIX = "provisa_"

# REQ-1389: the actor every Provisa-made term assignment is attributed to. The attribution is the
# ownership test: an association attributed to anyone else is a person's, and stands.
PROVISA_ACTOR = "urn:li:corpuser:provisa"


def dataset_urn(ref: AssetRef) -> str:
    return _dataset_urn_for(ref.fqn())


def _dataset_urn_for(fqn: str) -> str:
    """The dataset URN for an already-rendered fqn — used where the address is derived from a
    column ref rather than held as a table ref."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{fqn},{FABRIC})"


def _tag_urn(signal: str) -> str:
    return f"urn:li:tag:{TAG_PREFIX}{signal}"


def glossary_node_urn(org_id: str) -> str:
    """The one Provisa-owned glossary node per org — the ownership boundary (REQ-1387)."""
    return f"urn:li:glossaryNode:{PLATFORM}.{org_id}"


# REQ-2141: each ontology role groups its terms under its own child node of the org's node.
ROLE_NODES = {"class": "Classes", "property": "Properties", "concept": "Concepts"}


def role_node_urn(org_id: str, role: str) -> str:
    """The child node of the org's Provisa node that holds the terms of one role (REQ-2141)."""
    return f"{glossary_node_urn(org_id)}.{ROLE_NODES[role].lower()}"


def glossary_term_urn(org_id: str, name: str) -> str:
    """A term URN inside the org's Provisa node. Name-derived, so a rename mints a new URN."""
    return f"urn:li:glossaryTerm:{PLATFORM}.{org_id}.{name}"


def data_product_urn(org_id: str, product_id: str) -> str:  # REQ-1634
    """DataHub ships a native ``dataProduct`` entity — id-derived, stable across renames since
    the Provisa ``id`` (unlike the display name) never changes for a product's lifetime."""
    return f"urn:li:dataProduct:{PLATFORM}.{org_id}.{product_id}"


def _field_path(ref: AssetRef) -> str:
    """The column name inside its dataset, which is how DataHub addresses a schema field."""
    return ref.parts[-1]


@dataclass(frozen=True)
class AspectProposal:  # REQ-1069
    """One aspect of one entity, and the Provisa asset a rejection is reported against."""

    asset: Any
    kind: str
    entity_type: str
    urn: str
    aspect_name: str
    aspect: dict[str, Any]

    def payload(self) -> dict[str, Any]:
        return {
            "proposal": {
                "entityType": self.entity_type,
                "entityUrn": self.urn,
                "changeType": "UPSERT",
                "aspectName": self.aspect_name,
                "aspect": {
                    "contentType": "application/json",
                    "value": json.dumps(self.aspect),
                },
            }
        }


def _tags_for(snapshot: MetadataSnapshot, asset_fqn: str) -> list[dict[str, str]]:
    governance = [
        {"tag": _tag_urn(tag.signal.value)}
        for tag in snapshot.governance_tags
        if tag.asset.fqn() == asset_fqn
    ]
    # REQ-1378: registry tags publish through the same prefixed tag namespace.
    registry = [
        {"tag": _tag_urn(tag.tag_id)}
        for tag in snapshot.model_tags
        if tag.asset is not None and tag.asset.fqn() == asset_fqn
    ]
    return governance + registry


def _tag_definitions(snapshot: MetadataSnapshot) -> list[AspectProposal]:
    """A ``tag`` entity per signal, so the tags on the datasets resolve to something named.

    Each carries the rules imposing it in its description — never a rule body (REQ-1071).
    """
    by_signal: dict[str, set[str]] = {}
    for tag in snapshot.governance_tags:
        by_signal.setdefault(tag.signal.value, set()).add(tag.rule_id)
    return [
        AspectProposal(
            asset=AssetRefStub(_tag_urn(signal)),
            kind="tag",
            entity_type="tag",
            urn=_tag_urn(signal),
            aspect_name="tagProperties",
            aspect={
                "name": f"{TAG_PREFIX}{signal}",
                "description": (
                    f"Provisa enforces a {signal.replace('_', ' ')} policy here. "
                    f"Rules: {', '.join(sorted(rule_ids))}."
                ),
            },
        )
        for signal, rule_ids in sorted(by_signal.items())
    ] + [
        # REQ-1378: a tag entity per registry tag, so dataset/field references resolve.
        AspectProposal(
            asset=AssetRefStub(_tag_urn(tag_id)),
            kind="tag",
            entity_type="tag",
            urn=_tag_urn(tag_id),
            aspect_name="tagProperties",
            aspect={
                "name": f"{TAG_PREFIX}{tag_id}",
                "description": f"Provisa registry tag {tag_id!r}.",
            },
        )
        for tag_id in sorted({tag.tag_id for tag in snapshot.model_tags if tag.asset is not None})
    ]


def _term_association(org_id: str, term: GlossaryTermAsset) -> dict[str, Any]:
    """One Provisa-made term assignment (REQ-1389), unstamped: ``publish`` stamps its time, the
    live one when the same assignment already stands, so a steady state writes no new version."""
    return {
        "urn": glossary_term_urn(org_id, term.name),
        "actor": PROVISA_ACTOR,
        "attribution": {"actor": PROVISA_ACTOR, "sourceDetail": {"provisaUri": term.semantic_uri}},
    }


def _assignments(snapshot: MetadataSnapshot) -> dict[str, list[dict[str, Any]]]:
    """Each published table's or column's Provisa term assignments, by its fqn (REQ-1389): a
    Class term on the tables it grounds, a Property term on its columns."""
    by_asset: dict[str, list[dict[str, Any]]] = {}
    for term in sorted(snapshot.glossary_terms, key=lambda t: t.name):
        for ref in term.assigned_to():
            by_asset.setdefault(ref.fqn(), []).append(_term_association(snapshot.org_id, term))
    return by_asset


def _field_terms(assigned: list[dict[str, Any]]) -> dict[str, Any]:
    if not assigned:
        return {}
    return {"glossaryTerms": {"terms": assigned, "auditStamp": {"actor": PROVISA_ACTOR}}}


def _schema_metadata(
    table: TableAsset, snapshot: MetadataSnapshot, assigned: dict[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    return {
        "schemaName": table.ref.fqn(),
        "platform": f"urn:li:dataPlatform:{PLATFORM}",
        "version": 0,
        "hash": "",
        "platformSchema": {"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
        "fields": [
            {
                "fieldPath": column.name,
                "description": column.description,
                "nativeDataType": column.data_type,
                # Provisa's types come from federated sources and do not map onto DataHub's
                # closed type union without inventing a correspondence. The native type is
                # exact, so it is what a reader is given; `nativeDataType` is the field DataHub
                # provides for exactly that.
                "type": {"type": {"com.linkedin.schema.NullType": {}}},
                # Column-level governance rides in the schema aspect, because DataHub attaches
                # a field's tags there rather than on a separate entity — a masked column is
                # visible as masked in the same view that shows its type (REQ-1071).
                "globalTags": {"tags": _tags_for(snapshot, column.ref.fqn())},
                # REQ-1389: Provisa's Property assignments, in the aspect Provisa writes whole.
                **_field_terms(assigned.get(column.ref.fqn(), [])),
            }
            for column in table.columns
        ],
    }


def _ownership(steward_id: str) -> dict[str, Any]:
    """The owner Provisa authors, attributed to Provisa's actor so a read-merge tells it from the
    owners people add in DataHub (REQ-1389)."""
    return {
        "owners": [
            {
                "owner": f"urn:li:corpuser:{steward_id}",
                "type": "DATAOWNER",
                "attribution": {"time": 0, "actor": PROVISA_ACTOR},
            }
        ],
        "lastModified": {"time": 0, "actor": PROVISA_ACTOR},
    }


def _lineage_aspects(snapshot: MetadataSnapshot) -> list[AspectProposal]:
    """One ``upstreamLineage`` per derived table, table-level and column-level together.

    DataHub keeps the two in one aspect: ``upstreams`` is what a lineage graph traverses, and
    ``fineGrainedLineages`` is the column detail. Publishing only the first would discard the
    column-level derivation that is the point of REQ-1070.
    """
    table_by_fqn = {table.ref.fqn(): table for table in snapshot.tables}
    by_downstream: dict[str, list[Any]] = {}
    for edge in snapshot.lineage:
        by_downstream.setdefault(".".join(edge.downstream.parts[:-1]), []).append(edge)

    proposals: list[AspectProposal] = []
    for downstream_fqn, edges in by_downstream.items():
        downstream = table_by_fqn[downstream_fqn]
        upstream_fqns = sorted({".".join(edge.upstream.parts[:-1]) for edge in edges})
        proposals.append(
            AspectProposal(
                asset=downstream.ref,
                kind="lineage",
                entity_type="dataset",
                urn=dataset_urn(downstream.ref),
                aspect_name="upstreamLineage",
                aspect={
                    "upstreams": [
                        {
                            "dataset": dataset_urn(table_by_fqn[fqn].ref),
                            "type": "TRANSFORMED",
                        }
                        for fqn in upstream_fqns
                    ],
                    "fineGrainedLineages": [
                        {
                            "upstreamType": "FIELD_SET",
                            "downstreamType": "FIELD",
                            "upstreams": [
                                f"urn:li:schemaField:"
                                f"({dataset_urn(table_by_fqn['.'.join(edge.upstream.parts[:-1])].ref)},"
                                f"{_field_path(edge.upstream)})"
                            ],
                            "downstreams": [
                                f"urn:li:schemaField:"
                                f"({dataset_urn(downstream.ref)},{_field_path(edge.downstream)})"
                            ],
                            "transformOperation": ", ".join(edge.transforms),
                            "confidenceScore": 1.0,
                        }
                        for edge in edges
                    ],
                },
            )
        )
    return proposals


_ROLE_DEFINITIONS = {
    "class": "Terms whose instances are the rows of the tables they ground.",
    "property": "Terms that name the values of the columns they ground.",
    "concept": "Terms grounded in no table or column, related to the terms that are.",
}


def _glossary_proposals(snapshot: MetadataSnapshot) -> list[AspectProposal]:  # REQ-1387
    """The term graph as native glossary aspects, all inside the org's Provisa node.

    Every URN emitted here is minted by ``glossary_node_urn`` / ``glossary_term_urn`` — the
    adapter never addresses a glossary entity outside its own namespace, and it emits only
    UPSERTs, never deletes. Each term sits in the child node of its ontology role --
    Classes, Properties or Concepts -- and carries the role as ``provisaRole`` (REQ-2141).

    Edge mapping (module docstring documents the reasoning): ``KIND_OF`` →
    ``isRelatedTerms`` on the subtype; ``PART_OF`` → ``hasRelatedTerms`` on the whole (edge
    inverted — DataHub's has-a points container→part); ``RELATED_TO`` → ``relatedTerms`` on
    the source; ``SYNONYM_OF`` → ``relatedTerms`` on both endpoints. DataHub's glossary
    model has no typed fields beyond is/has/related-Terms, so the remaining six enum
    members fall back to the untyped link: symmetric types (``ANTONYM_OF``) write
    ``relatedTerms`` on both endpoints like ``SYNONYM_OF``; directional types
    (``VALID_VALUE_OF``, ``DERIVED_FROM``, ``REPLACES``, ``PREFERRED_TERM_FOR``,
    ``TRANSLATION_OF``) write ``relatedTerms`` on the source only, like ``RELATED_TO``.
    """
    if not snapshot.glossary_terms:
        return []
    node_urn = glossary_node_urn(snapshot.org_id)
    proposals = [
        AspectProposal(
            asset=AssetRefStub(node_urn),
            kind="glossary_node",
            entity_type="glossaryNode",
            urn=node_urn,
            aspect_name="glossaryNodeInfo",
            aspect={
                "name": f"Provisa ({snapshot.org_id})",
                "definition": (
                    "Business glossary published and owned by Provisa for org "
                    f"{snapshot.org_id!r}. Terms in this node are managed in Provisa, "
                    "grouped by their role in the ontology."
                ),
            },
        )
    ]
    for role in sorted({term.role for term in snapshot.glossary_terms}):
        role_urn = role_node_urn(snapshot.org_id, role)
        proposals.append(
            AspectProposal(
                asset=AssetRefStub(role_urn),
                kind="glossary_node",
                entity_type="glossaryNode",
                urn=role_urn,
                aspect_name="glossaryNodeInfo",
                aspect={
                    "name": ROLE_NODES[role],
                    "definition": _ROLE_DEFINITIONS[role],
                    "parentNode": node_urn,
                },
            )
        )
    term_by_id = {term.term_id: term for term in snapshot.glossary_terms}
    related: dict[int, dict[str, list[str]]] = {}

    def _add(term_id: int, field: str, other_id: int) -> None:
        urn = glossary_term_urn(snapshot.org_id, term_by_id[other_id].name)
        related.setdefault(term_id, {}).setdefault(field, []).append(urn)

    for edge in snapshot.glossary_edges:
        if edge.rel_type == "KIND_OF":
            _add(edge.from_term_id, "isRelatedTerms", edge.to_term_id)
        elif edge.rel_type == "PART_OF":
            _add(edge.to_term_id, "hasRelatedTerms", edge.from_term_id)
        elif edge.rel_type == "RELATED_TO":
            _add(edge.from_term_id, "relatedTerms", edge.to_term_id)
        elif edge.rel_type in ("SYNONYM_OF", "ANTONYM_OF"):
            _add(edge.from_term_id, "relatedTerms", edge.to_term_id)
            _add(edge.to_term_id, "relatedTerms", edge.from_term_id)
        elif edge.rel_type in (
            "VALID_VALUE_OF",
            "DERIVED_FROM",
            "REPLACES",
            "PREFERRED_TERM_FOR",
            "TRANSLATION_OF",
        ):
            _add(edge.from_term_id, "relatedTerms", edge.to_term_id)
        else:
            raise ValueError(f"unknown glossary edge type {edge.rel_type!r}")

    for term in snapshot.glossary_terms:
        urn = glossary_term_urn(snapshot.org_id, term.name)
        custom = {
            "provisaUri": term.semantic_uri,
            # REQ-1389: the stable Provisa term id rides the binding so the NEXT publish can
            # recognize a renamed term (new name-derived URN) and deprecate the old URN.
            "provisaTermId": str(term.term_id),
            "provisaRole": term.role,  # REQ-2141
        }
        if term.experts:
            custom["provisaExperts"] = ", ".join(term.experts)
        proposals.append(
            AspectProposal(
                asset=AssetRefStub(term.semantic_uri),
                kind="glossary_term",
                entity_type="glossaryTerm",
                urn=urn,
                aspect_name="glossaryTermInfo",
                aspect={
                    "name": term.name,
                    # GlossaryTermInfo.definition is a required string in DataHub's model
                    # (REQ-1387) — a Provisa term without a definition publishes it empty.
                    "definition": term.definition or "",
                    "parentNode": role_node_urn(snapshot.org_id, term.role),
                    "termSource": "INTERNAL",
                    "customProperties": custom,
                },
            )
        )
        if term.in_deprecation:
            deprecation: dict[str, Any] = {
                "deprecated": True,
                # REQ-2144: the curator's stated reason, when they gave one.
                "note": term.status_reason or "Deprecated in the Provisa glossary.",
                "actor": "urn:li:corpuser:provisa",
            }
            if term.retire_on:
                # decommissionTime is DataHub's native removal date (epoch millis).
                retire = datetime.fromisoformat(term.retire_on).replace(tzinfo=timezone.utc)
                deprecation["decommissionTime"] = int(retire.timestamp() * 1000)
            proposals.append(
                AspectProposal(
                    asset=AssetRefStub(term.semantic_uri),
                    kind="glossary_term_deprecation",
                    entity_type="glossaryTerm",
                    urn=urn,
                    aspect_name="deprecation",
                    aspect=deprecation,
                )
            )
        # REQ-1389: proposed for every term, so an edge Provisa withdrew leaves; ``publish``
        # keeps the relations a person made to terms outside the Provisa glossary.
        proposals.append(
            AspectProposal(
                asset=AssetRefStub(term.semantic_uri),
                kind="glossary_edges",
                entity_type="glossaryTerm",
                urn=urn,
                aspect_name="glossaryRelatedTerms",
                aspect=related.get(term.term_id, {}),
            )
        )
    return proposals


def _data_product_proposals(snapshot: MetadataSnapshot) -> list[AspectProposal]:  # REQ-1634
    """A data product as DataHub's own native ``dataProduct`` entity.

    ``assets`` names the member datasets by their dataset URN — DataHub's Data Product feature
    is built exactly for this grouping, unlike the domain/data-product retrofits the other
    adapters need. Ownership rides ``ownership`` the same way a table's steward does; the
    product's own domain rides as a ``customProperties`` entry (``provisaDomain``) rather than
    an association to a native ``domain`` entity, since this adapter does not publish domains
    as entities of their own.
    """
    proposals: list[AspectProposal] = []
    for product in snapshot.data_products:
        urn = data_product_urn(snapshot.org_id, product.id)
        proposals.append(
            AspectProposal(
                asset=product.ref,
                kind="data_product",
                entity_type="dataProduct",
                urn=urn,
                aspect_name="dataProductProperties",
                aspect={
                    "name": product.name,
                    "description": product.description,
                    "customProperties": {
                        "provisaDomain": product.domain_id,
                        "provisaUri": product.semantic_uri,  # REQ-1385
                        # REQ-2118: every ODPS field, under its ODPS name.
                        **{f"odps.{k}": v for k, v in product.odps_fields().items()},
                    },
                    "externalUrl": product.semantic_uri,
                    "assets": [
                        {"destinationUrn": _dataset_urn_for(member.fqn())}
                        for member in product.members
                    ],
                },
            )
        )
        if product.owner is not None:
            proposals.append(
                AspectProposal(
                    asset=product.ref,
                    kind="data_product_ownership",
                    entity_type="dataProduct",
                    urn=urn,
                    aspect_name="ownership",
                    aspect=_ownership(product.owner.id),
                )
            )
    return proposals


def _assertion_urn(assertion: Any) -> str:  # REQ-1443
    """A stable assertion URN, derived from what the assertion IS.

    DataHub mints assertion ids; Provisa has none to send, so the id is a digest over the
    observed asset, the check type and the check's own text. Republishing an unchanged contract
    therefore addresses the same assertion entity — a random id would fork a new one on every
    reconcile and leave the previous ones behind as orphans.
    """
    identity = "|".join(
        (
            assertion.asset.fqn(),
            assertion.checker,
            assertion.check_type,
            assertion.definition,
            assertion.results_table.fqn(),
        )
    )
    return f"urn:li:assertion:{PLATFORM}-{sha256(identity.encode()).hexdigest()[:32]}"


def _assertion_proposals(snapshot: MetadataSnapshot) -> list[AspectProposal]:  # REQ-1443
    """Contract checks as DataHub ``assertion`` entities on the datasets they observe.

    ``operator`` is ``_NATIVE_`` and ``logic`` carries the check's authored text: DataHub's
    standard operator enum cannot express a Soda threshold or a GX expectation without
    reinterpreting it, and an approximate operator would tell a consumer the assertion tests
    something other than what runs. ``nativeType`` names the check type as the checker spells it.
    """
    proposals: list[AspectProposal] = []
    for assertion in snapshot.assertions:
        is_column = len(assertion.asset.parts) == 4
        dataset = _dataset_urn_for(".".join(assertion.asset.parts[:3]))
        info: dict[str, Any] = {
            "type": "DATASET",
            "description": f"{assertion.checker} {assertion.check_type}",
            "datasetAssertion": {
                "dataset": dataset,
                "scope": "DATASET_COLUMN" if is_column else "DATASET_ROWS",
                "fields": [f"urn:li:schemaField:({dataset},{assertion.asset.parts[-1]})"]
                if is_column
                else [],
                "operator": "_NATIVE_",
                "nativeType": assertion.check_type,
                "logic": assertion.definition,
            },
            "customProperties": {
                "provisaChecker": assertion.checker,
                "provisaSeverity": assertion.severity,
                # Where the outcomes land — the pointer from the assertion back to the rows.
                "provisaResultsTable": assertion.results_table.fqn(),
                # REQ-1443: the last scan's state in full, including the ones no run event can
                # carry — never_run, error, skipped — so an unexecuted check reads as unexecuted
                # rather than as an assertion with no results yet.
                "provisaOutcome": assertion.outcome.status,
            },
        }
        urn = _assertion_urn(assertion)
        proposals.append(
            AspectProposal(
                asset=assertion.asset,
                kind="assertion",
                entity_type="assertion",
                urn=urn,
                aspect_name="assertionInfo",
                aspect=info,
            )
        )
        # REQ-1443: the last scan's verdict as DataHub's own run event, so the assertion shows
        # as passing or failing rather than merely declared. A check that reached no verdict —
        # never run, errored, skipped — emits none: DataHub's result type is the outcome itself,
        # and its customProperties already carry the state on the assertion.
        run_event = _assertion_run_event(assertion, urn)
        if run_event is not None:
            proposals.append(
                AspectProposal(
                    asset=assertion.asset,
                    kind="assertion_run",
                    entity_type="assertion",
                    urn=urn,
                    aspect_name="assertionRunEvent",
                    aspect=run_event,
                )
            )
    return proposals


# REQ-1443: Provisa's outcomes as DataHub assertion results. A warn is FAILURE because the
# checker raised it on a breached threshold; severity rides customProperties, where a consumer
# can see how loudly the author asked to be told.
_ASSERTION_RESULTS: dict[str, str] = {"pass": "SUCCESS", "fail": "FAILURE", "warn": "FAILURE"}


def _assertion_run_event(assertion: Any, urn: str) -> dict[str, Any] | None:  # REQ-1443
    """One check's last scan as an ``assertionRunEvent``, or None when it reached no verdict.

    ``runId`` is the scan's own id and ``timestampMillis`` its own time — the run this reports is
    the checker's, not the publish, so re-publishing the same scan restates one run instead of
    inventing a second.
    """
    result_type = _ASSERTION_RESULTS.get(assertion.outcome.status)
    if result_type is None:
        return None
    scan_time = assertion.outcome.scan_time
    assert scan_time is not None  # a verdict implies the scan that reached it
    result: dict[str, Any] = {"type": result_type, "nativeResults": {}}
    if assertion.outcome.metric_value is not None:
        result["actualAggValue"] = assertion.outcome.metric_value
    if assertion.outcome.failed_rows is not None:
        result["rowCount"] = assertion.outcome.failed_rows
    return {
        "timestampMillis": int(scan_time.timestamp() * 1000),
        "runId": assertion.outcome.scan_id,
        "assertionUrn": urn,
        "asserteeUrn": _dataset_urn_for(".".join(assertion.asset.parts[:3])),
        "status": "COMPLETE",
        "result": result,
    }


def to_proposals(snapshot: MetadataSnapshot) -> list[AspectProposal]:
    """The snapshot as DataHub aspect proposals.

    Tags are defined before the datasets that reference them: a ``globalTags`` entry pointing at
    a tag URN with no ``tagProperties`` renders in DataHub as an unnamed tag.
    """
    proposals = _tag_definitions(snapshot)
    assigned = _assignments(snapshot)  # REQ-1389
    domain_by_id = {domain.id: domain for domain in snapshot.domains}
    relationships_by_source: dict[str, list[Any]] = {}
    for edge in snapshot.relationships:
        relationships_by_source.setdefault(edge.source.fqn(), []).append(edge)
    edge_tags: dict[str, list[str]] = {}
    for tag in snapshot.model_tags:
        if tag.relationship_id is not None:
            edge_tags.setdefault(tag.relationship_id, []).append(tag.tag_id)
    # REQ-1375: 'deprecated' maps to DataHub's native deprecation aspect, not just a tag —
    # that is the construct DataHub consumers already surface in their UI.
    deprecated_tables = {
        tag.asset.fqn(): tag
        for tag in snapshot.model_tags
        if tag.tag_id == "deprecated" and tag.asset is not None and len(tag.asset.parts) == 3
    }

    for table in snapshot.tables:
        urn = dataset_urn(table.ref)
        custom: dict[str, str] = {}
        if table.domain_id:
            custom["provisaDomain"] = table.domain_id
        edges = relationships_by_source.get(table.ref.fqn(), [])
        if edges:
            custom["provisaRelationships"] = json.dumps(
                {
                    "approved": [
                        {
                            "id": edge.id,
                            "uri": edge.semantic_uri,  # REQ-1385
                            "target": edge.target.fqn() if edge.target is not None else None,
                            "sourceColumn": edge.source_column,
                            "targetColumn": edge.target_column,
                            "cardinality": edge.cardinality,
                            "alias": edge.alias,
                            "owner": edge.owner.id if edge.owner is not None else None,
                            "version": edge.version,
                            "needsReview": edge.needs_review,
                            # REQ-1586: kind plus, on a junction-backed edge, the associative table it traverses.
                            **junction_payload(edge),
                            # REQ-1378: relationship registry tags ride the relationship
                            # record — DataHub has no edge entity to tag natively here.
                            **(
                                {"tags": sorted(edge_tags[edge.id])} if edge.id in edge_tags else {}
                            ),
                        }
                        for edge in edges
                    ]
                }
            )
        custom["provisaUri"] = table.semantic_uri  # REQ-1385: also the clickable externalUrl
        properties: dict[str, Any] = {
            # REQ-1385: the presented name is the business name; qualifiedName is the binding.
            "name": table.aliases[0] if table.aliases else table.name,
            "qualifiedName": table.ref.fqn(),
            "description": table.description,
            "customProperties": custom,
            # REQ-1385: DataHub renders externalUrl as the asset's outbound link — the
            # dereference path back to the governed definition.
            "externalUrl": table.semantic_uri,
        }
        if table.aliases:
            custom["provisaAliases"] = ", ".join(table.aliases)
        proposals.append(
            AspectProposal(
                asset=table.ref,
                kind="table",
                entity_type="dataset",
                urn=urn,
                aspect_name="datasetProperties",
                aspect=properties,
            )
        )
        proposals.append(
            AspectProposal(
                asset=table.ref,
                kind="schema",
                entity_type="dataset",
                urn=urn,
                aspect_name="schemaMetadata",
                aspect=_schema_metadata(table, snapshot, assigned),
            )
        )
        if table.ref.fqn() in deprecated_tables:
            _dep = deprecated_tables[table.ref.fqn()]
            _aspect: dict[str, Any] = {
                "deprecated": True,
                # The steward's stated reason (required at assignment) — never boilerplate.
                "note": _dep.reason or "Tagged 'deprecated' in the Provisa registry.",
            }
            if _dep.expires_on:
                # decommissionTime is DataHub's native removal date (epoch millis).
                _epoch = datetime.fromisoformat(_dep.expires_on).replace(tzinfo=timezone.utc)
                _aspect["decommissionTime"] = int(_epoch.timestamp() * 1000)
            proposals.append(
                AspectProposal(
                    asset=table.ref,
                    kind="deprecation",
                    entity_type="dataset",
                    urn=urn,
                    aspect_name="deprecation",
                    aspect=_aspect,
                )
            )
        # REQ-1389: Provisa's Class assignments on this dataset -- proposed even when empty, so
        # an assignment Provisa withdrew leaves; ``publish`` merges in the live aspect.
        proposals.append(
            AspectProposal(
                asset=table.ref,
                kind="glossary_assignment",
                entity_type="dataset",
                urn=urn,
                aspect_name="glossaryTerms",
                aspect={
                    "terms": assigned.get(table.ref.fqn(), []),
                    "auditStamp": {"actor": PROVISA_ACTOR},
                },
            )
        )
        table_tags = _tags_for(snapshot, table.ref.fqn())
        if table_tags:
            proposals.append(
                AspectProposal(
                    asset=table.ref,
                    kind="tags",
                    entity_type="dataset",
                    urn=urn,
                    aspect_name="globalTags",
                    aspect={"tags": table_tags},
                )
            )
        # A table whose domain has no steward publishes without an ownership aspect. DataHub
        # would otherwise show an owner that does not exist, which is worse than showing none
        # (REQ-609).
        if table.domain_id:
            domain = domain_by_id[table.domain_id]
            if domain.steward is not None:
                proposals.append(
                    AspectProposal(
                        asset=table.ref,
                        kind="ownership",
                        entity_type="dataset",
                        urn=urn,
                        aspect_name="ownership",
                        aspect=_ownership(domain.steward.id),
                    )
                )

    proposals.extend(_lineage_aspects(snapshot))
    proposals.extend(_glossary_proposals(snapshot))  # REQ-1387
    proposals.extend(_data_product_proposals(snapshot))  # REQ-1634
    proposals.extend(_assertion_proposals(snapshot))  # REQ-1443
    return proposals


@register_provider
class DataHubExport(MetadataExport):  # REQ-1069
    """Publish a snapshot to DataHub as aspect proposals."""

    provider_name = "datahub"

    ingest_path = "/aspects?action=ingestProposal"
    aspects_path = "/aspects"
    health_path = "/config"
    # REQ-1389: read-merge the live globalTags aspect before proposing ours, so tags a steward
    # attached in the DataHub UI survive the whole-document aspect UPSERT. Analogous to Atlas's
    # classification_merge; off in tests that exercise the plain replace path.
    tag_merge = True

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _merge_global_tags(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        proposal: AspectProposal,
        result: PublishResult,
    ) -> AspectProposal | None:
        """Read-merge the dataset's live ``globalTags`` into the proposal (REQ-1389).

        The live aspect is read from the same GMS endpoint the ingest posts to
        (``GET /aspects/<urlencoded urn>?aspect=globalTags&version=0``, same auth headers).
        Every live tag whose urn is NOT ``provisa_``-prefixed was attached by a human and is
        carried through verbatim; the ``provisa_`` tags become exactly the snapshot's, so a
        stale one drops out. A failed read refuses the aspect, reported by name -- proposed
        blind it would replace the people's tags -- and the publish continues.
        """
        route = f"{self.aspects_path}/{quote(proposal.urn, safe='')}?aspect=globalTags&version=0"
        try:
            response = await client.get(self._url(route), headers=headers)
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message=(
                        f"globalTags: not written: the live read the people's tags are kept "
                        f"from failed ({exc})"
                    ),
                )
            )
            return None
        if response.status_code == 404:
            # No live aspect: nothing human-attached to preserve.
            return proposal
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message=(
                        f"globalTags: not written: the live read the people's tags are kept "
                        f"from failed (HTTP {response.status_code})"
                    ),
                )
            )
            return None
        # GMS wraps the aspect value in its record-class key
        # ({"aspect": {"com.linkedin.common.GlobalTags": {"tags": [...]}}}).
        live_tags: list[dict[str, Any]] = []
        for value in (response.json().get("aspect") or {}).values():
            if isinstance(value, dict) and isinstance(value.get("tags"), list):
                live_tags = value["tags"]
                break
        human = [
            entry
            for entry in live_tags
            if not str(entry.get("tag", "")).startswith(f"urn:li:tag:{TAG_PREFIX}")
        ]
        return replace(proposal, aspect={"tags": human + proposal.aspect["tags"]})

    async def _live_aspect(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        proposal: AspectProposal,
        result: PublishResult,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Whether the proposal's aspect could be read (a failure is reported here), and its
        live value, None when there is none."""
        route = (
            f"{self.aspects_path}/{quote(proposal.urn, safe='')}"
            f"?aspect={proposal.aspect_name}&version=0"
        )
        try:
            response = await client.get(self._url(route), headers=headers)
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message=f"{proposal.aspect_name}: live aspect read failed ({exc})",
                )
            )
            return False, None
        if response.status_code == 404:
            return True, None
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message=(
                        f"{proposal.aspect_name}: live aspect read failed "
                        f"(HTTP {response.status_code})"
                    ),
                )
            )
            return False, None
        # GMS wraps the value in its record-class key ({"aspect": {"com.linkedin...": {...}}}).
        for value in (response.json().get("aspect") or {}).values():
            if isinstance(value, dict):
                return True, value
        return True, None

    @staticmethod
    def _stamped(
        ours: list[dict[str, Any]], live: list[dict[str, Any]], now_ms: int
    ) -> list[dict[str, Any]]:
        """Provisa's associations with their times: an assignment already standing keeps the
        time it was made, a new one is made now (REQ-1389)."""
        made = {
            entry["urn"]: entry["attribution"]["time"]
            for entry in live
            if (entry.get("attribution") or {}).get("actor") == PROVISA_ACTOR
        }
        return [
            {
                **entry,
                "attribution": {**entry["attribution"], "time": made.get(entry["urn"], now_ms)},
            }
            for entry in ours
        ]

    async def _merge_glossary_terms(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        proposal: AspectProposal,
        result: PublishResult,
        now_ms: int,
    ) -> AspectProposal | None:
        """Read-merge a dataset's live ``glossaryTerms`` into Provisa's assignments (REQ-1389).

        An association not attributed to Provisa's actor is a person's and stands; where a
        person assigned the same term, Provisa adds none beside it. Provisa's own track the
        snapshot, so a withdrawn one leaves. Nothing is written when the live aspect cannot be
        read -- writing blind would replace the people's assignments -- or when nothing would
        change.
        """
        read, live = await self._live_aspect(client, headers, proposal, result)
        if not read:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message="glossaryTerms: not written, so the live assignments stand",
                )
            )
            return None
        live_terms = (live or {}).get("terms") or []
        human = [
            entry
            for entry in live_terms
            if (entry.get("attribution") or {}).get("actor") != PROVISA_ACTOR
        ]
        held = {entry["urn"] for entry in human}
        ours = [entry for entry in proposal.aspect["terms"] if entry["urn"] not in held]
        terms = human + self._stamped(ours, live_terms, now_ms)
        if terms == live_terms:
            return None
        return replace(
            proposal,
            aspect={"terms": terms, "auditStamp": {"actor": PROVISA_ACTOR, "time": now_ms}},
        )

    async def _merge_ownership(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        proposal: AspectProposal,
        result: PublishResult,
        now_ms: int,
    ) -> AspectProposal | None:
        """Read-merge a dataset's live ``ownership`` into Provisa's owner (REQ-1389).

        An owner attributed to Provisa's actor is Provisa's and tracks the snapshot; any other is
        a person's and stands. An owner Provisa wrote before owners carried an attribution has
        none, in an aspect Provisa last modified: it is Provisa's too. Nothing is written when the
        live aspect cannot be read -- written blind it would replace the people's owners.
        """
        read, live = await self._live_aspect(client, headers, proposal, result)
        if not read:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message="ownership: not written, so the live owners stand",
                )
            )
            return None
        live_owners = (live or {}).get("owners") or []
        provisa_last = ((live or {}).get("lastModified") or {}).get("actor") == PROVISA_ACTOR

        def _provisas(owner: dict[str, Any]) -> bool:
            actor = (owner.get("attribution") or {}).get("actor")
            return actor == PROVISA_ACTOR or (actor is None and provisa_last)

        wanted = {(o["owner"], o.get("type")): o for o in proposal.aspect["owners"]}
        held = {(o["owner"], o.get("type")) for o in live_owners if not _provisas(o)}
        owners: list[dict[str, Any]] = []
        for live_owner in live_owners:
            key = (live_owner["owner"], live_owner.get("type"))
            if not _provisas(live_owner):
                owners.append(live_owner)  # a person's: it stands, where it stands
            elif key in wanted and key not in held:
                time = (live_owner.get("attribution") or {}).get("time") or now_ms
                owners.append(
                    {**wanted.pop(key), "attribution": {"actor": PROVISA_ACTOR, "time": time}}
                )
        for key, owner in wanted.items():
            if key not in held:
                owners.append({**owner, "attribution": {"actor": PROVISA_ACTOR, "time": now_ms}})
        if owners == live_owners:
            return None
        return replace(
            proposal,
            aspect={"owners": owners, "lastModified": {"time": now_ms, "actor": PROVISA_ACTOR}},
        )

    async def _merge_related_terms(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        proposal: AspectProposal,
        result: PublishResult,
        provisa_prefix: str,
    ) -> AspectProposal | None:
        """Read-merge a term's live ``glossaryRelatedTerms`` (REQ-1389): the relations among the
        org's Provisa terms are Provisa's and track the snapshot; a relation a person made to a
        term outside the Provisa glossary stands. Nothing is written when the live aspect cannot
        be read, or when nothing would change."""
        read, live = await self._live_aspect(client, headers, proposal, result)
        if not read:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message="glossaryRelatedTerms: not written, so the live relations stand",
                )
            )
            return None
        live = live or {}
        merged: dict[str, list[str]] = {}
        for field in sorted(set(live) | set(proposal.aspect)):
            if not isinstance(live.get(field, []), list):
                continue
            foreign = [u for u in live.get(field) or [] if not str(u).startswith(provisa_prefix)]
            ours = [u for u in proposal.aspect.get(field, []) if u not in foreign]
            if foreign or ours:
                merged[field] = foreign + ours
        current = {k: v for k, v in live.items() if isinstance(v, list) and v}
        if merged == current:
            return None
        return replace(proposal, aspect=merged)

    async def _stamp_field_terms(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        proposal: AspectProposal,
        result: PublishResult,
        now_ms: int,
    ) -> AspectProposal:
        """Stamp the schema fields' Provisa assignments (REQ-1389), keeping the time of each
        one that already stands so an unchanged schema is an unchanged aspect."""
        fields = proposal.aspect["fields"]
        if not any("glossaryTerms" in f for f in fields):
            return proposal
        live_fields: dict[str, list[dict[str, Any]]] = {}
        if self.tag_merge:
            _, live = await self._live_aspect(client, headers, proposal, result)
            for live_field in (live or {}).get("fields") or []:
                terms = (live_field.get("glossaryTerms") or {}).get("terms") or []
                live_fields[live_field["fieldPath"]] = terms
        stamped = []
        for f in fields:
            if "glossaryTerms" in f:
                f = {
                    **f,
                    "glossaryTerms": {
                        "terms": self._stamped(
                            f["glossaryTerms"]["terms"], live_fields.get(f["fieldPath"], []), now_ms
                        ),
                        "auditStamp": {"actor": PROVISA_ACTOR, "time": now_ms},
                    },
                }
            stamped.append(f)
        return replace(proposal, aspect={**proposal.aspect, "fields": stamped})

    def _rebind_deprecations(self, proposals: list[AspectProposal]) -> list[AspectProposal]:
        """Deprecation aspects for the OLD URNs of physically re-addressed tables (REQ-1389).

        A dataset URN is immutable identity, so a re-address mints a new URN; the stored
        binding names the old one, which is marked deprecated with a pointer to its
        successor and the canonical Provisa URN — visibly superseded, never a silent
        duplicate.
        """
        deprecations: list[AspectProposal] = []
        for proposal in proposals:
            if proposal.aspect_name != "datasetProperties":
                continue
            uri = proposal.aspect["customProperties"]["provisaUri"]
            stored = self._bindings.get(uri)
            if stored is None or stored[0] == proposal.urn:
                continue
            old_urn = stored[0]
            deprecations.append(
                AspectProposal(
                    asset=proposal.asset,
                    kind="rebind_deprecation",
                    entity_type="dataset",
                    urn=old_urn,
                    aspect_name="deprecation",
                    aspect={
                        "deprecated": True,
                        "note": (
                            f"Physically re-addressed by Provisa; superseded by "
                            f"{proposal.urn}. Provisa URI: {uri}."
                        ),
                        "actor": "urn:li:corpuser:provisa",
                    },
                )
            )
        return deprecations

    def _term_rename_deprecations(self, snapshot: MetadataSnapshot) -> list[AspectProposal]:
        """Deprecation aspects for the OLD URNs of renamed glossary terms (REQ-1387/1389).

        A glossaryTerm URN derives from the term's name, so a rename mints a new URN; the
        stored binding carries the stable Provisa term id (``term:<id>`` as the physical
        key), which is how the old URN is matched to its successor. Only URNs inside the
        org's own Provisa node are ever touched — a foreign glossary URN in a corrupted
        binding is skipped rather than deprecated — and nothing is ever deleted.
        """
        term_by_id = {term.term_id: term for term in snapshot.glossary_terms}
        namespace = f"urn:li:glossaryTerm:{PLATFORM}.{snapshot.org_id}."
        deprecations: list[AspectProposal] = []
        for old_uri, (old_urn, physical_key) in self._bindings.items():
            if not physical_key.startswith("term:"):
                continue
            term = term_by_id.get(int(physical_key.removeprefix("term:")))
            if term is None:
                continue
            successor = glossary_term_urn(snapshot.org_id, term.name)
            if successor == old_urn or not old_urn.startswith(namespace):
                continue
            deprecations.append(
                AspectProposal(
                    asset=AssetRefStub(old_uri),
                    kind="glossary_rename_deprecation",
                    entity_type="glossaryTerm",
                    urn=old_urn,
                    aspect_name="deprecation",
                    aspect={
                        "deprecated": True,
                        "note": (
                            f"Renamed in the Provisa glossary; superseded by {successor}. "
                            f"Provisa URI: {term.semantic_uri}."
                        ),
                        "actor": "urn:li:corpuser:provisa",
                    },
                )
            )
        return deprecations

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        headers = self._headers()
        proposals = to_proposals(snapshot)
        proposals.extend(self._rebind_deprecations(proposals))
        proposals.extend(self._term_rename_deprecations(snapshot))  # REQ-1387
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        provisa_prefix = glossary_term_urn(snapshot.org_id, "")
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            for proposal in proposals:
                if (
                    self.tag_merge
                    and proposal.aspect_name == "globalTags"
                    and proposal.entity_type == "dataset"
                ):
                    merged_tags = await self._merge_global_tags(client, headers, proposal, result)
                    if merged_tags is None:
                        continue
                    proposal = merged_tags
                if (
                    self.tag_merge
                    and proposal.aspect_name == "ownership"
                    and proposal.entity_type == "dataset"
                ):
                    # REQ-1389: Provisa's owner beside the people's.
                    merged_owners = await self._merge_ownership(
                        client, headers, proposal, result, now_ms
                    )
                    if merged_owners is None:
                        continue
                    proposal = merged_owners
                if self.tag_merge and proposal.aspect_name == "glossaryRelatedTerms":
                    merged_related = await self._merge_related_terms(
                        client, headers, proposal, result, provisa_prefix
                    )
                    if merged_related is None:
                        continue
                    proposal = merged_related
                if proposal.aspect_name == "glossaryTerms" and proposal.entity_type == "dataset":
                    # REQ-1389: Provisa's term assignments beside the people's.
                    if self.tag_merge:
                        merged = await self._merge_glossary_terms(
                            client, headers, proposal, result, now_ms
                        )
                    elif proposal.aspect["terms"]:
                        merged = replace(
                            proposal,
                            aspect={
                                "terms": self._stamped(proposal.aspect["terms"], [], now_ms),
                                "auditStamp": {"actor": PROVISA_ACTOR, "time": now_ms},
                            },
                        )
                    else:
                        merged = None
                    if merged is None:
                        continue
                    proposal = merged
                if proposal.aspect_name == "schemaMetadata":
                    proposal = await self._stamp_field_terms(
                        client, headers, proposal, result, now_ms
                    )
                # One request per aspect, because that is DataHub's unit of ingestion. A
                # rejected aspect is reported against its asset and the rest still publish —
                # a table whose lineage aspect is refused is still a published table.
                response = await client.post(
                    self._url(self.ingest_path), json=proposal.payload(), headers=headers
                )
                if response.status_code >= 400:
                    result.errors.append(
                        AssetError(
                            asset=proposal.asset,
                            message=(
                                f"{proposal.aspect_name}: HTTP {response.status_code}: "
                                f"{response.text[:300]}"
                            ),
                        )
                    )
                    continue
                result.published[proposal.kind] = result.published.get(proposal.kind, 0) + 1
                if proposal.aspect_name == "datasetProperties" and proposal.kind == "table":
                    # REQ-1389: the URN IS the vendor id — capture it keyed by the Provisa
                    # URN so a later publish detects a physical re-address.
                    result.bindings[proposal.aspect["customProperties"]["provisaUri"]] = (
                        proposal.urn,
                        proposal.aspect["qualifiedName"],
                    )
                if proposal.aspect_name == "glossaryTermInfo":
                    # REQ-1387/1389: the term URN is the vendor id — captured keyed by the
                    # term's Provisa URI, with the stable term id as the physical key so a
                    # later publish detects a rename (name-derived URN change).
                    custom = proposal.aspect["customProperties"]
                    result.bindings[custom["provisaUri"]] = (
                        proposal.urn,
                        f"term:{custom['provisaTermId']}",
                    )
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(self._url(self.health_path), headers=self._headers())
        response.raise_for_status()
