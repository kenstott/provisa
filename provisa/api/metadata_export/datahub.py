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
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import json

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.provider import (
    AssetError,
    AssetRefStub,
    MetadataExport,
    PublishResult,
)
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
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{ref.fqn()},{FABRIC})"


def _tag_urn(signal: str) -> str:
    return f"urn:li:tag:{TAG_PREFIX}{signal}"


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
        for tag_id in sorted(
            {tag.tag_id for tag in snapshot.model_tags if tag.asset is not None}
        )
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
                            # REQ-1378: relationship registry tags ride the relationship
                            # record — DataHub has no edge entity to tag natively here.
                            **(
                                {"tags": sorted(edge_tags[edge.id])}
                                if edge.id in edge_tags
                                else {}
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
    return proposals


@register_provider
class DataHubExport(MetadataExport):  # REQ-1069
    """Publish a snapshot to DataHub as aspect proposals."""

    provider_name = "datahub"

    ingest_path = "/aspects?action=ingestProposal"
    health_path = "/config"

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        headers = self._headers()
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            for proposal in to_proposals(snapshot):
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
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(self._url(self.health_path), headers=self._headers())
        response.raise_for_status()
