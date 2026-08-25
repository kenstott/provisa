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
assignment is HUMAN-OWNED (REQ-1389): this adapter never writes a ``glossaryTerms`` aspect on
a dataset or schema field. The typed term edges map onto ``glossaryRelatedTerms``:

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
    from provisa.api.metadata_export.model import AssetRef, MetadataSnapshot, TableAsset

# The data platform every published dataset is attributed to. Provisa is the governed access
# path, and a consumer resolving one of these URNs has to arrive back at Provisa rather than at
# a federated source it has no policy-enforced route to.
PLATFORM = "provisa"

# DataHub environments are a fixed enum (PROD, DEV, …). Everything Provisa governs is
# production data regardless of which environment Provisa itself runs in — the distinction
# DataHub is drawing is about the data, not about the publisher.
FABRIC = "PROD"

TAG_PREFIX = "provisa_"


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


def glossary_term_urn(org_id: str, name: str) -> str:
    """A term URN inside the org's Provisa node. Name-derived, so a rename mints a new URN."""
    return f"urn:li:glossaryTerm:{PLATFORM}.{org_id}.{name}"


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


def _schema_metadata(table: TableAsset, snapshot: MetadataSnapshot) -> dict[str, Any]:
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
            }
            for column in table.columns
        ],
    }


def _ownership(steward_id: str) -> dict[str, Any]:
    return {
        "owners": [
            {
                "owner": f"urn:li:corpuser:{steward_id}",
                "type": "DATAOWNER",
            }
        ],
        "lastModified": {"time": 0, "actor": "urn:li:corpuser:provisa"},
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


def _glossary_proposals(snapshot: MetadataSnapshot) -> list[AspectProposal]:  # REQ-1387
    """The term graph as native glossary aspects, all inside the org's Provisa node.

    Every URN emitted here is minted by ``glossary_node_urn`` / ``glossary_term_urn`` — the
    adapter never addresses a glossary entity outside its own namespace, and it emits only
    UPSERTs, never deletes. Datasets and schema fields get NO ``glossaryTerms`` aspect from
    this adapter: term-to-asset assignment is human-owned (REQ-1389).

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
                    f"{snapshot.org_id!r}. Terms in this node are managed in Provisa; "
                    "term-to-asset assignment is curated in DataHub."
                ),
            },
        )
    ]
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
                    "parentNode": node_urn,
                    "termSource": "INTERNAL",
                    "customProperties": custom,
                },
            )
        )
        if term.deprecated:
            proposals.append(
                AspectProposal(
                    asset=AssetRefStub(term.semantic_uri),
                    kind="glossary_term_deprecation",
                    entity_type="glossaryTerm",
                    urn=urn,
                    aspect_name="deprecation",
                    aspect={
                        "deprecated": True,
                        "note": "Deprecated in the Provisa glossary.",
                        "actor": "urn:li:corpuser:provisa",
                    },
                )
            )
        if term.term_id in related:
            proposals.append(
                AspectProposal(
                    asset=AssetRefStub(term.semantic_uri),
                    kind="glossary_edges",
                    entity_type="glossaryTerm",
                    urn=urn,
                    aspect_name="glossaryRelatedTerms",
                    aspect=related[term.term_id],
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
                aspect=_schema_metadata(table, snapshot),
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
    ) -> AspectProposal:
        """Read-merge the dataset's live ``globalTags`` into the proposal (REQ-1389).

        The live aspect is read from the same GMS endpoint the ingest posts to
        (``GET /aspects/<urlencoded urn>?aspect=globalTags&version=0``, same auth headers).
        Every live tag whose urn is NOT ``provisa_``-prefixed was attached by a human and is
        carried through verbatim; the ``provisa_`` tags become exactly the snapshot's, so a
        stale one drops out. A read failure never aborts the publish: the proposal goes out
        with only Provisa's tags and the merge failure is reported as an AssetError — the
        clobber is surfaced, never silent.
        """
        route = f"{self.aspects_path}/{quote(proposal.urn, safe='')}?aspect=globalTags&version=0"
        try:
            response = await client.get(self._url(route), headers=headers)
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message=(
                        f"globalTags merge: live aspect read failed ({exc}); published "
                        "Provisa tags only — human-attached tags may be overwritten"
                    ),
                )
            )
            return proposal
        if response.status_code == 404:
            # No live aspect: nothing human-attached to preserve.
            return proposal
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=proposal.asset,
                    message=(
                        f"globalTags merge: live aspect read failed (HTTP "
                        f"{response.status_code}); published Provisa tags only — "
                        "human-attached tags may be overwritten"
                    ),
                )
            )
            return proposal
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
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            for proposal in proposals:
                if (
                    self.tag_merge
                    and proposal.aspect_name == "globalTags"
                    and proposal.entity_type == "dataset"
                ):
                    proposal = await self._merge_global_tags(client, headers, proposal, result)
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
