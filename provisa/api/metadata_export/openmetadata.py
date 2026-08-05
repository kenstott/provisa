# Copyright (c) 2026 Kenneth Stott
# Canary: 4e83c9a1-70d6-4b25-8f19-6a2c5d0e7b34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OpenMetadata adapter — the standards-first asset target (REQ-1069).

OpenMetadata's ingestion API is entity-shaped rather than event-shaped, so the snapshot maps
onto its hierarchy: a database service per Provisa source, a database and schema under it, a
table per governed table with its columns inline, and a domain per Provisa domain. Every PUT
is an upsert keyed by ``fullyQualifiedName``, which is what makes the scheduled reconcile
(REQ-1072) idempotent — republishing an unchanged snapshot is a no-op on the target.

Provisa's own concepts map onto OpenMetadata's:

* a domain steward becomes the entity's ``owner``;
* a governance signal becomes a ``tag`` under the ``Provisa`` classification, carrying the
  signal and the rule id but never the rule body (REQ-1071);
* an approved relationship becomes a table-level ``extension`` entry, because OpenMetadata
  models constraints it discovered, not joins a steward approved, and forcing one into
  ``tableConstraints`` would assert a physical foreign key that may not exist.

Column lineage goes to ``/api/v1/lineage`` in OpenMetadata's own ``columnsLineage`` shape —
the same edges the OpenLineage adapter emits, addressed by FQN instead of namespace+name.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import json

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_egress.provider import (
    AssetError,
    AssetRefStub,
    MetadataEgress,
    PublishResult,
)
from provisa.api.metadata_egress.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_egress.model import (
        AssetRef,
        ColumnAsset,
        MetadataSnapshot,
        TableAsset,
    )

# The classification every Provisa-projected tag lives under, so a catalog admin can tell
# governance Provisa enforces from tags applied inside OpenMetadata.
CLASSIFICATION = "Provisa"

# OpenMetadata addresses a table as service.database.schema.table. Provisa's source maps to
# the service, and the source's own name repeats as the database: Provisa federates a source
# as one logical database, and inventing a second level would address a database that the
# governed config never names.
_DEFAULT_DATABASE = "default"

# OpenMetadata stores an email on every user. A Provisa steward is an identity in Provisa's own
# directory rather than a mailbox, so an id that is not already an address is qualified with the
# RFC 2606 reserved domain — a synthesized address under a real domain would reach somebody.
_STEWARD_EMAIL_DOMAIN = "provisa.invalid"

# The custom property approved relationships are stored under, and its value shape. It is a
# `string` holding a JSON array rather than OpenMetadata's own `table-cp` property, which caps a
# table at three columns — an approved relationship carries eight fields, so a table-cp form
# would have to drop five of them.
RELATIONSHIP_PROPERTY = "provisaRelationships"


@dataclass(frozen=True)
class Entity:  # REQ-1069
    """One upsert: which OpenMetadata collection, what body, and the asset it is about."""

    asset: AssetRef
    path: str
    kind: str
    body: dict[str, Any]
    # The steward this entity is owned by. OpenMetadata addresses an owner by the UUID it
    # assigned the user, which only the server knows, so :meth:`publish` substitutes the id it
    # got back when it upserted that user.
    owned_by: str | None = None


def _service_fqn(source_id: str) -> str:
    return source_id


def _database_fqn(source_id: str) -> str:
    return f"{source_id}.{_DEFAULT_DATABASE}"


def _schema_fqn(table: TableAsset) -> str:
    source_id, schema_name, _ = table.ref.parts
    return f"{_database_fqn(source_id)}.{schema_name}"


def _table_fqn(table: TableAsset) -> str:
    return f"{_schema_fqn(table)}.{table.ref.parts[-1]}"


def _column_fqn(table: TableAsset, column_name: str) -> str:
    return f"{_table_fqn(table)}.{column_name}"


def _steward_email(steward_id: str) -> str:
    """The address OpenMetadata stores for a steward, derived from the Provisa identity.

    An id that is already an address is published as it stands; anything else is qualified
    with the reserved domain (see ``_STEWARD_EMAIL_DOMAIN``).
    """
    if "@" in steward_id:
        return steward_id
    return f"{steward_id}@{_STEWARD_EMAIL_DOMAIN}"


def _tag_label(tag_fqn: str) -> dict[str, Any]:
    # ``Automated``: the label came from an upstream system, not from a human curating it in
    # OpenMetadata, so a catalog admin can tell Provisa-enforced governance from local edits.
    return {
        "tagFQN": tag_fqn,
        "source": "Classification",
        "labelType": "Automated",
        "state": "Confirmed",
    }


def _governance_tag_fqns(snapshot: MetadataSnapshot, asset_fqn: str) -> list[str]:
    return sorted(
        {
            f"{CLASSIFICATION}.{tag.signal.value}"
            for tag in snapshot.governance_tags
            if tag.asset.fqn() == asset_fqn
        }
    )


def _classification_entities(snapshot: MetadataSnapshot) -> list[Entity]:
    """The classification and one tag per signal actually present.

    Tags are created before the assets that reference them; OpenMetadata rejects a tag label
    whose tag does not exist, so seeding them is what keeps a first publish from failing on
    every governed table at once.
    """
    signals = sorted({tag.signal.value for tag in snapshot.governance_tags})
    if not signals:
        return []
    root = AssetRefStub(CLASSIFICATION)
    entities = [
        Entity(
            asset=root,
            path="/api/v1/classifications",
            kind="classification",
            body={
                "name": CLASSIFICATION,
                "description": "Governance Provisa enforces on the upstream federated data.",
            },
        )
    ]
    entities.extend(
        Entity(
            asset=AssetRefStub(f"{CLASSIFICATION}.{signal}"),
            path="/api/v1/tags",
            kind="tag",
            body={
                "classification": CLASSIFICATION,
                "name": signal,
                "description": f"Provisa enforces a {signal.replace('_', ' ')} policy here.",
            },
        )
        for signal in signals
    )
    return entities


def _column_body(
    snapshot: MetadataSnapshot, table: TableAsset, column: ColumnAsset
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": column.name,
        # OpenMetadata requires a dataType from its own enum; the source type is preserved
        # verbatim in dataTypeDisplay so nothing about the column's real type is lost.
        "dataType": "UNKNOWN",
        "dataTypeDisplay": column.data_type,
        "description": column.description,
    }
    if column.aliases:
        body["displayName"] = column.aliases[0]
    tag_fqns = _governance_tag_fqns(snapshot, _to_provisa_fqn(table, column.name))
    if tag_fqns:
        body["tags"] = [_tag_label(fqn) for fqn in tag_fqns]
    return body


def _to_provisa_fqn(table: TableAsset, column_name: str) -> str:
    """A column's Provisa address, which is how governance tags are keyed."""
    return f"{table.ref.fqn()}.{column_name}"


def _table_entity(snapshot: MetadataSnapshot, table: TableAsset) -> Entity:
    body: dict[str, Any] = {
        "name": table.ref.parts[-1],
        "databaseSchema": _schema_fqn(table),
        "description": table.description,
        "columns": [_column_body(snapshot, table, column) for column in table.columns],
    }
    if table.aliases:
        body["displayName"] = table.aliases[0]
    tag_fqns = _governance_tag_fqns(snapshot, table.ref.fqn())
    if tag_fqns:
        body["tags"] = [_tag_label(fqn) for fqn in tag_fqns]
    if table.domain_id:
        body["domain"] = table.domain_id
    edges = [edge for edge in snapshot.relationships if edge.source.fqn() == table.ref.fqn()]
    if edges:
        body["extension"] = {
            RELATIONSHIP_PROPERTY: json.dumps(
                [
                    {
                        "id": edge.id,
                        "target": edge.target.fqn() if edge.target is not None else None,
                        "sourceColumn": edge.source_column,
                        "targetColumn": edge.target_column,
                        "cardinality": edge.cardinality,
                        "owner": edge.owner.id if edge.owner is not None else None,
                        "version": edge.version,
                        "needsReview": edge.needs_review,
                    }
                    for edge in edges
                ]
            )
        }
    return Entity(asset=table.ref, path="/api/v1/tables", kind="table", body=body)


def to_entities(snapshot: MetadataSnapshot) -> list[Entity]:
    """The snapshot as OpenMetadata upserts, in dependency order.

    Classifications and tags, then services, databases and schemas, then domains, then tables.
    Each level references the one before it by FQN, and OpenMetadata rejects a reference it
    cannot resolve — so the order is the contract, not a preference.
    """
    entities: list[Entity] = _classification_entities(snapshot)

    for source in snapshot.sources:
        entities.append(
            Entity(
                asset=source.ref,
                path="/api/v1/services/databaseServices",
                kind="service",
                body={
                    "name": _service_fqn(source.id),
                    "serviceType": "CustomDatabase",
                    "description": source.description,
                    # The connection is Provisa's, not the catalog's: OpenMetadata must never
                    # be handed credentials to the underlying source, because Provisa is the
                    # only path on which governance is enforced.
                    "connection": {
                        "config": {
                            "type": "CustomDatabase",
                            "sourcePythonClass": "provisa.metadata_egress",
                        }
                    },
                },
            )
        )
        entities.append(
            Entity(
                asset=source.ref,
                path="/api/v1/databases",
                kind="database",
                body={"name": _DEFAULT_DATABASE, "service": _service_fqn(source.id)},
            )
        )

    for schema_fqn, table in {_schema_fqn(t): t for t in snapshot.tables}.items():
        entities.append(
            Entity(
                asset=table.ref,
                path="/api/v1/databaseSchemas",
                kind="schema",
                body={
                    "name": schema_fqn.rsplit(".", 1)[-1],
                    "database": _database_fqn(table.ref.parts[0]),
                },
            )
        )

    # Stewards become OpenMetadata users before the domains that own them: an owner is an
    # entity reference the server resolves by UUID, so a domain naming a user the catalog has
    # never heard of is rejected outright.
    for steward_id in sorted({d.steward.id for d in snapshot.domains if d.steward is not None}):
        entities.append(
            Entity(
                asset=AssetRefStub(steward_id),
                path="/api/v1/users",
                kind="user",
                body={
                    "name": steward_id,
                    "displayName": steward_id,
                    "email": _steward_email(steward_id),
                },
            )
        )

    for domain in snapshot.domains:
        body: dict[str, Any] = {
            "name": domain.id,
            "domainType": "Aggregate",
            "description": domain.description,
        }
        # REQ-609: an unstewarded domain publishes with no owner and a description that says
        # so. Assigning a placeholder owner would report accountability nobody holds.
        if domain.steward is None:
            body["description"] = (
                f"{domain.description}\n\n(No designated steward — governance pending.)"
            )
        entities.append(
            Entity(
                asset=AssetRefStub(domain.id),
                path="/api/v1/domains",
                kind="domain",
                body=body,
                owned_by=domain.steward.id if domain.steward is not None else None,
            )
        )

    entities.extend(_table_entity(snapshot, table) for table in snapshot.tables)
    return entities


def to_lineage_requests(snapshot: MetadataSnapshot) -> list[Entity]:
    """Column lineage as OpenMetadata ``AddLineage`` requests, one per (upstream, downstream).

    Grouped by table pair: OpenMetadata models the edge between two tables and hangs the
    column pairs off it, so emitting one request per column would overwrite the previous
    column's edge rather than adding to it.

    The two endpoints are named here by ``fullyQualifiedName``. OpenMetadata's ``AddLineage``
    resolves an edge by the entity's UUID, which only the server knows, so :meth:`publish`
    substitutes the id it got back when it upserted that table.
    """
    by_table: dict[tuple[str, str], list[dict[str, list[str] | str]]] = {}
    downstream_by_key: dict[tuple[str, str], AssetRef] = {}
    table_by_prefix = {table.ref.fqn(): table for table in snapshot.tables}

    for edge in snapshot.lineage:
        up_prefix = ".".join(edge.upstream.parts[:-1])
        down_prefix = ".".join(edge.downstream.parts[:-1])
        upstream_table = table_by_prefix[up_prefix]
        downstream_table = table_by_prefix[down_prefix]
        key = (_table_fqn(upstream_table), _table_fqn(downstream_table))
        by_table.setdefault(key, []).append(
            {
                "fromColumns": [_column_fqn(upstream_table, edge.upstream.parts[-1])],
                "toColumn": _column_fqn(downstream_table, edge.downstream.parts[-1]),
                # The transform Provisa resolved from the compiled SQL, which is what makes
                # this lineage explainable rather than a bare dependency claim.
                "function": " | ".join(edge.transforms),
            }
        )
        downstream_by_key[key] = edge.downstream

    return [
        Entity(
            asset=downstream_by_key[(up_fqn, down_fqn)],
            path="/api/v1/lineage",
            kind="lineage",
            body={
                "edge": {
                    "fromEntity": {"fullyQualifiedName": up_fqn, "type": "table"},
                    "toEntity": {"fullyQualifiedName": down_fqn, "type": "table"},
                    "lineageDetails": {"columnsLineage": columns},
                }
            },
        )
        for (up_fqn, down_fqn), columns in by_table.items()
    ]


@register_provider
class OpenMetadataEgress(MetadataEgress):  # REQ-1069
    """Publish a snapshot to an OpenMetadata server's ingestion API."""

    provider_name = "openmetadata"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    @staticmethod
    def _resolve_edge(body: dict[str, Any], table_ids: dict[str, str]) -> str | None:
        """Fill both endpoints' ``id`` in place; return the first FQN that has no id yet."""
        for side in ("fromEntity", "toEntity"):
            ref = body["edge"][side]
            entity_id = table_ids.get(ref["fullyQualifiedName"])
            if entity_id is None:
                return ref["fullyQualifiedName"]
            ref["id"] = entity_id
        return None

    async def _register_relationship_property(self, client: httpx.AsyncClient) -> None:
        """Declare the custom property the relationship extension is stored under.

        OpenMetadata rejects an extension field the entity type does not declare, so the
        property has to exist before the first table that carries one. Both the type ids are
        the server's, so they are read rather than assumed; the PUT itself is an upsert.
        """
        ids: dict[str, str] = {}
        for name, category in (("table", "entity"), ("string", "field")):
            response = await client.get(
                self._url(f"/api/v1/metadata/types/name/{name}"),
                params={"category": category},
                headers=self._headers(),
            )
            response.raise_for_status()
            ids[name] = response.json()["id"]
        response = await client.put(
            self._url(f"/api/v1/metadata/types/{ids['table']}"),
            json={
                "name": RELATIONSHIP_PROPERTY,
                "description": (
                    "Relationships a Provisa steward approved on this table, as a JSON array."
                ),
                "propertyType": {"id": ids["string"], "type": "type"},
            },
            headers=self._headers(),
        )
        response.raise_for_status()

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        requests = [*to_entities(snapshot), *to_lineage_requests(snapshot)]
        # fullyQualifiedName -> server-assigned UUID, harvested from the table upserts and
        # consumed by the lineage requests that follow them.
        table_ids: dict[str, str] = {}
        # steward id -> server-assigned UUID, harvested from the user upserts and consumed by
        # the domains they steward.
        user_ids: dict[str, str] = {}
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            if any("extension" in entity.body for entity in requests):
                try:
                    await self._register_relationship_property(client)
                except httpx.HTTPError as exc:
                    # The relationships cannot be published without the property, but the tables
                    # they hang off can. Sending the extension anyway would fail every one of
                    # those tables over an obstacle that only affects the relationship payload,
                    # so the extension is dropped and the loss is reported here — once, naming
                    # the property and the reason, rather than once per table.
                    result.errors.append(
                        AssetError(
                            asset=AssetRefStub(RELATIONSHIP_PROPERTY),
                            message=(
                                f"custom property {RELATIONSHIP_PROPERTY}: {exc}. Approved "
                                "relationships were not published; every other asset was."
                            ),
                        )
                    )
                    for entity in requests:
                        entity.body.pop("extension", None)
            for entity in requests:
                if entity.owned_by is not None:
                    owner_id = user_ids.get(entity.owned_by)
                    if owner_id is None:
                        # REQ-609: the steward's user upsert failed, and its own error is
                        # already in the result. Publishing the domain anyway would report it
                        # as unstewarded, which is a governance claim rather than a gap.
                        result.errors.append(
                            AssetError(
                                asset=entity.asset,
                                message=(
                                    f"{entity.kind} {entity.asset.fqn()}: steward "
                                    f"{entity.owned_by!r} was not upserted, so ownership "
                                    "cannot be published"
                                ),
                            )
                        )
                        continue
                    entity.body["owners"] = [{"id": owner_id, "type": "user"}]
                if entity.kind == "lineage":
                    unresolved = self._resolve_edge(entity.body, table_ids)
                    if unresolved is not None:
                        # The table upsert this edge depends on failed, and its own error is
                        # already in the result. Sending the edge anyway would add a second,
                        # misleading failure about lineage rather than about the table.
                        result.errors.append(
                            AssetError(
                                asset=entity.asset,
                                message=(
                                    f"lineage {entity.asset.fqn()}: endpoint {unresolved!r} "
                                    "was not upserted, so the edge cannot be addressed"
                                ),
                            )
                        )
                        continue
                response = await client.put(
                    self._url(entity.path), json=entity.body, headers=self._headers()
                )
                if entity.kind == "table" and response.status_code < 400:
                    body = response.json()
                    table_ids[body["fullyQualifiedName"]] = body["id"]
                if entity.kind == "user" and response.status_code < 400:
                    user_ids[entity.body["name"]] = response.json()["id"]
                if response.status_code >= 400:
                    result.errors.append(
                        AssetError(
                            asset=entity.asset,
                            message=(
                                f"{entity.kind} {entity.asset.fqn()}: "
                                f"HTTP {response.status_code}: {response.text[:500]}"
                            ),
                        )
                    )
                    continue
                result.published[entity.kind] = result.published.get(entity.kind, 0) + 1
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(
                self._url("/api/v1/system/version"), headers=self._headers()
            )
        response.raise_for_status()
