# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7a4e60-92fd-4d3b-a0b1-5e8f2c94d713
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Apache Atlas adapter, and with it Microsoft Purview (REQ-1069).

Purview's ingestion API is Atlas-API-compatible: the same ``/api/atlas/v2`` routes, the same
entity envelope, the same RDBMS type model. The only difference is how a request authenticates
— Purview takes an Entra ID client-credentials token, which ``auth_mode: entra`` obtains here
(REQ-1068). One adapter therefore covers both, and the endpoint decides which one is talked to.

The mapping is the Atlas RDBMS model, because it is the one type set both products ship:

* a Provisa source becomes an ``rdbms_instance``, and one ``rdbms_db`` under it;
* a table becomes an ``rdbms_table`` and its columns ``rdbms_column``, linked to their parents
  by the relationship attributes Atlas defines between those types;
* a governance signal becomes an Atlas *classification* on the asset, carrying the signal and
  the rule id but never the rule body (REQ-1071);
* an approved relationship and a domain become entity attributes rather than Atlas structures:
  Atlas models a foreign key it discovered, and a steward-approved join is not that;
* column lineage becomes a ``Process`` entity per derived table, with ``inputs``/``outputs``
  and the compiled transform recorded on it (REQ-1070).

Every entity carries a stable ``qualifiedName``, which is the key Atlas upserts on — that is
what makes the scheduled reconcile (REQ-1072) idempotent.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import base64
import json

from dataclasses import dataclass, field
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
    from provisa.api.metadata_export.model import DomainAsset, MetadataSnapshot, TableAsset

# Every Provisa-projected classification is prefixed, so a catalog admin can tell governance
# Provisa enforces from a classification curated inside Atlas.
CLASSIFICATION_PREFIX = "provisa_"

# Atlas addresses an entity by qualifiedName, and its convention is <path>@<cluster>. The
# cluster name is the qualifier for "which deployment does this asset live in"; Provisa's
# governed config is the deployment, so every published name ends in it.
CLUSTER = "provisa"

# Where a table's domain and approved relationships are published. Both are Provisa concepts
# Atlas has no type for, and an attribute Atlas has not declared on a type is dropped rather
# than refused — so they ride in `userDescription`, which every Atlas `Asset` declares, as one
# JSON document. Purview's schema is Atlas's, so the same field carries them there.
GOVERNANCE_ATTRIBUTE = "userDescription"

# The Entra scope Purview's data-plane accepts. Purview does not take a Graph token; this is
# the resource id its Atlas endpoint authenticates against.
PURVIEW_SCOPE = "https://purview.azure.net/.default"


@dataclass
class AtlasEntity:  # REQ-1069
    """One Atlas entity, and the Provisa asset it reports failures against.

    ``guid`` is the negative placeholder Atlas requires on a create: a bulk request refers to
    its own not-yet-created entities by these, and the server swaps in real guids.
    """

    asset: Any
    kind: str
    type_name: str
    attributes: dict[str, Any]
    guid: str
    classifications: list[dict[str, Any]] = field(default_factory=list)
    relationships: dict[str, Any] = field(default_factory=dict)

    def payload(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "typeName": self.type_name,
            "guid": self.guid,
            "attributes": self.attributes,
        }
        if self.classifications:
            body["classifications"] = self.classifications
        if self.relationships:
            body["relationshipAttributes"] = self.relationships
        return body


def _qualified(*parts: str) -> str:
    return f"{'.'.join(parts)}@{CLUSTER}"


def _instance_qn(source_id: str) -> str:
    return _qualified(source_id)


def _db_qn(source_id: str) -> str:
    return _qualified(source_id, "default")


def _table_qn(table: TableAsset) -> str:
    return _qualified(*table.ref.parts)


def _column_qn(table: TableAsset, column_name: str) -> str:
    return _qualified(*table.ref.parts, column_name)


def _classification_name(signal: str) -> str:
    return f"{CLASSIFICATION_PREFIX}{signal}"


def _classifications(snapshot: MetadataSnapshot, asset_fqn: str) -> list[dict[str, Any]]:
    """The governance classifications on one asset, with the rule that imposes each.

    The attributes name the rule and the roles it applies to. They never carry the rule body:
    a mask pattern published beside the masked column is the bypass instruction (REQ-1071).
    """
    return [
        {
            "typeName": _classification_name(tag.signal.value),
            "attributes": {
                "ruleId": tag.rule_id,
                "restrictedRoles": list(tag.restricted_roles),
                "exemptRoles": list(tag.exempt_roles),
            },
        }
        for tag in snapshot.governance_tags
        if tag.asset.fqn() == asset_fqn
    ]


def classification_defs(snapshot: MetadataSnapshot) -> list[dict[str, Any]]:
    """Typedefs for the signals the snapshot actually carries.

    Atlas rejects a classification whose typedef does not exist, so these are registered before
    the entities that reference them.
    """
    return [
        {
            "name": _classification_name(signal),
            "description": f"Provisa enforces a {signal.replace('_', ' ')} policy here.",
            "superTypes": [],
            "attributeDefs": [
                {
                    "name": "ruleId",
                    "typeName": "string",
                    "isOptional": True,
                    "cardinality": "SINGLE",
                    "isUnique": False,
                    "isIndexable": False,
                },
                {
                    "name": "restrictedRoles",
                    "typeName": "array<string>",
                    "isOptional": True,
                    "cardinality": "SET",
                    "isUnique": False,
                    "isIndexable": False,
                },
                {
                    "name": "exemptRoles",
                    "typeName": "array<string>",
                    "isOptional": True,
                    "cardinality": "SET",
                    "isUnique": False,
                    "isIndexable": False,
                },
            ],
        }
        for signal in sorted({tag.signal.value for tag in snapshot.governance_tags})
    ]


def _governance_document(
    snapshot: MetadataSnapshot, table: TableAsset, domain: DomainAsset | None
) -> str | None:
    """The domain and the approved relationships of one table, as a JSON document.

    Returns None when the table has neither, so a table without them publishes with the field
    absent rather than with an empty structure a reader would have to interpret.
    """
    body: dict[str, Any] = {}
    if domain is not None:
        # REQ-609: a domain with no steward publishes as pending. Naming a placeholder owner
        # would report accountability nobody holds.
        body["domain"] = {
            "id": domain.id,
            "description": domain.description,
            "steward": domain.steward.id if domain.steward is not None else None,
            "pending": domain.pending,
        }
    edges = [edge for edge in snapshot.relationships if edge.source.fqn() == table.ref.fqn()]
    if edges:
        body["approvedRelationships"] = [
            {
                "id": edge.id,
                "target": edge.target.fqn() if edge.target is not None else None,
                "sourceColumn": edge.source_column,
                "targetColumn": edge.target_column,
                "cardinality": edge.cardinality,
                "alias": edge.alias,
                "owner": edge.owner.id if edge.owner is not None else None,
                "version": edge.version,
                "needsReview": edge.needs_review,
            }
            for edge in edges
        ]
    if not body:
        return None
    return json.dumps(body)


def to_entities(snapshot: MetadataSnapshot) -> list[AtlasEntity]:
    """The snapshot as Atlas entities, parents before the children that reference them.

    A bulk request is resolved in order, and a child naming a parent guid that appears later is
    rejected — so the order is the contract rather than a preference.
    """
    entities: list[AtlasEntity] = []
    guid = _GuidCounter()
    db_guids: dict[str, str] = {}
    table_guids: dict[str, str] = {}

    for source in snapshot.sources:
        instance_guid = guid.next()
        entities.append(
            AtlasEntity(
                asset=source.ref,
                kind="instance",
                type_name="rdbms_instance",
                guid=instance_guid,
                attributes={
                    "qualifiedName": _instance_qn(source.id),
                    "name": source.id,
                    "description": source.description,
                    # What the asset is federated FROM, recorded as the platform rather than as
                    # a connection: Atlas is never handed a route to the source, because
                    # Provisa is the only path on which governance is enforced.
                    "rdbms_type": source.source_type,
                    "platform": "provisa",
                    "protocol": "provisa",
                },
            )
        )
        db_guid = guid.next()
        db_guids[source.id] = db_guid
        entities.append(
            AtlasEntity(
                asset=source.ref,
                kind="database",
                type_name="rdbms_db",
                guid=db_guid,
                attributes={
                    "qualifiedName": _db_qn(source.id),
                    "name": "default",
                    "description": source.description,
                },
                relationships={"instance": {"guid": instance_guid}},
            )
        )

    domain_by_id = {domain.id: domain for domain in snapshot.domains}
    for table in snapshot.tables:
        source_id = table.ref.parts[0]
        table_guid = guid.next()
        table_guids[table.ref.fqn()] = table_guid
        attributes: dict[str, Any] = {
            "qualifiedName": _table_qn(table),
            "name": table.name,
            "description": table.description,
        }
        if table.aliases:
            attributes["displayName"] = table.aliases[0]
        # A domain Provisa governs but the snapshot did not carry is a builder fault, not a
        # condition to paper over: publishing the table with no domain would report it as
        # ungoverned, so the lookup is direct and raises.
        domain = domain_by_id[table.domain_id] if table.domain_id else None
        if domain is not None and domain.steward is not None:
            # Atlas's own accountability field, so the steward shows where a catalog reader
            # already looks for an owner rather than only inside Provisa's JSON document.
            attributes["owner"] = domain.steward.id
        governance_doc = _governance_document(snapshot, table, domain)
        if governance_doc is not None:
            attributes[GOVERNANCE_ATTRIBUTE] = governance_doc
        entities.append(
            AtlasEntity(
                asset=table.ref,
                kind="table",
                type_name="rdbms_table",
                guid=table_guid,
                attributes=attributes,
                classifications=_classifications(snapshot, table.ref.fqn()),
                relationships={"db": {"guid": db_guids[source_id]}},
            )
        )
        for column in table.columns:
            column_attributes: dict[str, Any] = {
                "qualifiedName": _column_qn(table, column.name),
                "name": column.name,
                "description": column.description,
                "data_type": column.data_type,
            }
            if column.aliases:
                column_attributes["displayName"] = column.aliases[0]
            entities.append(
                AtlasEntity(
                    asset=column.ref,
                    kind="column",
                    type_name="rdbms_column",
                    guid=guid.next(),
                    attributes=column_attributes,
                    classifications=_classifications(snapshot, column.ref.fqn()),
                    relationships={"table": {"guid": table_guid}},
                )
            )

    entities.extend(_lineage_entities(snapshot, guid, table_guids))
    return entities


class _GuidCounter:
    """Negative placeholder guids, which is how a bulk request refers to its own entities."""

    def __init__(self) -> None:
        self._next = -1

    def next(self) -> str:
        value = str(self._next)
        self._next -= 1
        return value


def _lineage_entities(
    snapshot: MetadataSnapshot, guid: _GuidCounter, table_guids: dict[str, str]
) -> list[AtlasEntity]:
    """One ``Process`` per derived table, carrying its inputs, output and transforms.

    Atlas models lineage as a process between datasets rather than as an edge, and it has no
    column-level lineage type — so the column pairs and the compiled transform ride on the
    process as a JSON document, which keeps the derivation explainable instead of reducing it
    to a table-to-table dependency.
    """
    table_by_prefix = {table.ref.fqn(): table for table in snapshot.tables}
    by_downstream: dict[str, list[Any]] = {}
    for edge in snapshot.lineage:
        by_downstream.setdefault(".".join(edge.downstream.parts[:-1]), []).append(edge)

    entities: list[AtlasEntity] = []
    for downstream_prefix, edges in by_downstream.items():
        downstream = table_by_prefix[downstream_prefix]
        upstreams = sorted({".".join(edge.upstream.parts[:-1]) for edge in edges})
        entities.append(
            AtlasEntity(
                asset=downstream.ref,
                kind="lineage",
                type_name="Process",
                guid=guid.next(),
                attributes={
                    "qualifiedName": _qualified(*downstream.ref.parts, "derivation"),
                    "name": f"{downstream.ref.fqn()} derivation",
                    "description": json.dumps(
                        [
                            {
                                "from": edge.upstream.fqn(),
                                "to": edge.downstream.fqn(),
                                "transforms": list(edge.transforms),
                            }
                            for edge in edges
                        ]
                    ),
                    # By guid rather than by qualifiedName. Atlas resolves a referenced entity
                    # against what is already committed, not against the rest of the same
                    # request, so naming a table this publish is creating gets ATLAS-404 —
                    # the placeholder guid is the only reference that resolves inside a batch.
                    "inputs": [{"guid": table_guids[prefix]} for prefix in upstreams],
                    "outputs": [{"guid": table_guids[downstream_prefix]}],
                },
            )
        )
    return entities


@register_provider
class AtlasExport(MetadataExport):  # REQ-1069
    """Publish a snapshot to Apache Atlas, or to Microsoft Purview's Atlas-compatible API."""

    provider_name = "atlas"

    # The route prefix and the type-registration route. Both are attributes so a vendor whose
    # API is Atlas-shaped but differently mounted (Atlan) is a subclass rather than a fork.
    entity_bulk_path = "/api/atlas/v2/entity/bulk"
    typedefs_path = "/api/atlas/v2/types/typedefs"
    classificationdef_path = "/api/atlas/v2/types/classificationdef/name"
    health_path = "/api/atlas/v2/types/typedefs/headers"

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    async def _bearer(self, client: httpx.AsyncClient) -> str:
        """The credential this publish authenticates with.

        ``entra`` exchanges the client-credentials grant for a Purview data-plane token; the
        other modes carry a credential the administrator already configured.
        """
        if self._config.auth_mode == "entra":
            response = await client.post(
                f"https://login.microsoftonline.com/{self._config.entra_tenant_id}"
                "/oauth2/v2.0/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._config.entra_client_id,
                    "client_secret": self._config.entra_client_secret.get_secret_value(),
                    "scope": PURVIEW_SCOPE,
                },
            )
            response.raise_for_status()
            return response.json()["access_token"]
        return self._config.token.get_secret_value() or self._config.api_key.get_secret_value()

    async def _headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._config.auth_mode == "basic":
            # Apache Atlas's own default authentication. Its API answers a bearer token with
            # 401, so a deployment against stock Atlas configures this mode and the credential
            # is sent as the HTTP basic pair the server expects.
            secret = (
                self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
            )
            pair = base64.b64encode(f"{self._config.username}:{secret}".encode()).decode()
            headers["Authorization"] = f"Basic {pair}"
            return headers
        bearer = await self._bearer(client)
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        return headers

    async def _register_classifications(
        self, client: httpx.AsyncClient, defs: list[dict[str, Any]], headers: dict[str, str]
    ) -> None:
        """Create the classification typedefs the entities reference.

        A typedef that already exists is left alone rather than updated: Atlas answers a POST
        of an existing typedef with 409, and an update would overwrite whatever a catalog admin
        has since added to it.
        """
        missing = []
        for definition in defs:
            probe = await client.get(
                self._url(f"{self.classificationdef_path}/{definition['name']}"),
                headers=headers,
            )
            if probe.status_code == 404:
                missing.append(definition)
            elif probe.status_code >= 400:
                probe.raise_for_status()
        if not missing:
            return
        response = await client.post(
            self._url(self.typedefs_path),
            json={"classificationDefs": missing},
            headers=headers,
        )
        response.raise_for_status()

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        return await self._publish_entities(to_entities(snapshot), snapshot)

    async def _publish_entities(
        self, entities: list[AtlasEntity], snapshot: MetadataSnapshot
    ) -> PublishResult:
        """Send already-mapped entities.

        Split from ``publish`` so a subclass whose catalog is Atlas-shaped but differently
        typed (Atlan) replaces the mapping without restating the transport, the classification
        registration or the error reporting.
        """
        result = PublishResult(provider_name=self.provider_name)
        defs = classification_defs(snapshot)
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            headers = await self._headers(client)
            if defs:
                try:
                    await self._register_classifications(client, defs, headers)
                except httpx.HTTPError as exc:
                    # The assets can be published without their classifications, but sending a
                    # classification the target does not know fails the entity carrying it. The
                    # governance projection is dropped and reported once, naming the reason,
                    # rather than failing every governed asset (REQ-1071).
                    result.errors.append(
                        AssetError(
                            asset=AssetRefStub("classificationDefs"),
                            message=(
                                f"classification typedefs: {exc}. Governance signals were not "
                                "published; every asset was."
                            ),
                        )
                    )
                    for entity in entities:
                        entity.classifications = []
            response = await client.post(
                self._url(self.entity_bulk_path),
                json={"entities": [entity.payload() for entity in entities]},
                headers=headers,
            )
            if response.status_code >= 400:
                # Atlas resolves a bulk request as one unit, so a rejection is about the batch
                # rather than about one entity. Reporting it against each asset would claim
                # per-asset diagnosis the response does not contain.
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(self.entity_bulk_path),
                        message=f"HTTP {response.status_code}: {response.text[:500]}",
                    )
                )
                return result
        for entity in entities:
            result.published[entity.kind] = result.published.get(entity.kind, 0) + 1
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            headers = await self._headers(client)
            response = await client.get(self._url(self.health_path), headers=headers)
        response.raise_for_status()
