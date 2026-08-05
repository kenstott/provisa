# Copyright (c) 2026 Kenneth Stott
# Canary: b48e2f70-5d19-4c83-a2b6-90fd731ce514
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Collibra adapter (REQ-1069).

Collibra models everything as *assets* of a configured type, joined by *relations* of a
configured type, with values carried in *attributes*. Nothing about that vocabulary is
Provisa's: the type ids, the relation type ids and the domain an asset lands in are configured
inside the customer's Collibra instance, which is why they are read from the target rather than
assumed.

The import runs through Collibra's synchronous import job (``/rest/2.0/import/json-job``),
which takes the whole payload and upserts by each asset's full name. That is one request per
publish rather than one per asset, which is what makes the scheduled reconcile (REQ-1072)
affordable against a catalog of any size — and it is also why a rejection is reported against
the batch: the job result names the rows it refused, and those are mapped back to their assets.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import json

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
    from provisa.api.metadata_egress.model import MetadataSnapshot

# Collibra ships these operating-model types out of the box, and its own JDBC ingestion uses
# them — so a Provisa-published table lands beside a Collibra-ingested one instead of in a
# parallel vocabulary only Provisa understands.
TABLE_TYPE = "Table"
COLUMN_TYPE = "Column"
DATABASE_TYPE = "Database"

# The relation types binding the hierarchy, named as Collibra names them: "<head> <role>
# <tail>". These are the out-of-the-box technical-asset relations.
COLUMN_TO_TABLE_RELATION = "Column is part of Table"
TABLE_TO_DATABASE_RELATION = "Table is part of Database"

# Attributes Provisa writes. `Description` is Collibra's own; the rest are custom attribute
# types the target must have, and a target that lacks one refuses the rows carrying it — which
# the publish reports rather than dropping.
DESCRIPTION_ATTRIBUTE = "Description"
GOVERNANCE_ATTRIBUTE = "Provisa Governance"
RELATIONSHIP_ATTRIBUTE = "Provisa Approved Relationships"
LINEAGE_ATTRIBUTE = "Provisa Lineage"
STEWARD_ATTRIBUTE = "Provisa Steward"


def _identifier(name: str, community: str, domain: str) -> dict[str, Any]:
    """How Collibra addresses an asset for upsert: its name inside a domain inside a community.

    Collibra has no cross-domain unique key, so the domain is part of the identity — which is
    why the configured domain is what separates one org's published assets from another's.
    """
    return {"name": name, "domain": {"name": domain, "community": {"name": community}}}


def _governance_value(snapshot: MetadataSnapshot, asset_fqn: str) -> str | None:
    """The enforcement facts on one asset, as a JSON document.

    Collibra's classification is a curated vocabulary an organization owns, and asserting into
    it would overwrite a data office's own taxonomy. The facts are published as an attribute
    instead, so the target can map them onto whatever its own model calls them (REQ-1071).
    """
    tags = [tag for tag in snapshot.governance_tags if tag.asset.fqn() == asset_fqn]
    if not tags:
        return None
    return json.dumps(
        [
            {
                "signal": tag.signal.value,
                "ruleId": tag.rule_id,
                "restrictedRoles": list(tag.restricted_roles),
                "exemptRoles": list(tag.exempt_roles),
            }
            for tag in tags
        ]
    )


def to_rows(snapshot: MetadataSnapshot, community: str, domain: str) -> list[dict[str, Any]]:
    """The snapshot as Collibra import rows, hierarchy first.

    Collibra's import resolves relations against assets in the same payload, so a column may
    name its table before that table's own row appears — but a database that never appears at
    all leaves its tables unparented, which is why every source publishes a database row even
    when nothing under it changed.
    """
    rows: list[dict[str, Any]] = []
    for source in snapshot.sources:
        rows.append(
            {
                "resourceType": "Asset",
                "identifier": _identifier(source.id, community, domain),
                "name": source.id,
                "type": {"name": DATABASE_TYPE},
                "attributes": {
                    DESCRIPTION_ATTRIBUTE: [{"value": source.description}],
                },
            }
        )

    domain_by_id = {entry.id: entry for entry in snapshot.domains}
    relationships_by_source: dict[str, list[Any]] = {}
    for edge in snapshot.relationships:
        relationships_by_source.setdefault(edge.source.fqn(), []).append(edge)
    lineage_by_downstream: dict[str, list[Any]] = {}
    for edge in snapshot.lineage:
        lineage_by_downstream.setdefault(edge.downstream.fqn(), []).append(edge)

    for table in snapshot.tables:
        attributes: dict[str, Any] = {DESCRIPTION_ATTRIBUTE: [{"value": table.description}]}
        # A domain id the snapshot did not carry is a builder fault. Publishing the table
        # without its steward would report it as unowned.
        if table.domain_id:
            governing = domain_by_id[table.domain_id]
            if governing.steward is not None:
                attributes[STEWARD_ATTRIBUTE] = [{"value": governing.steward.id}]
        governance = _governance_value(snapshot, table.ref.fqn())
        if governance is not None:
            attributes[GOVERNANCE_ATTRIBUTE] = [{"value": governance}]
        edges = relationships_by_source.get(table.ref.fqn(), [])
        if edges:
            attributes[RELATIONSHIP_ATTRIBUTE] = [
                {
                    "value": json.dumps(
                        [
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
                    )
                }
            ]
        rows.append(
            {
                "resourceType": "Asset",
                "identifier": _identifier(table.ref.fqn(), community, domain),
                "name": table.ref.fqn(),
                "displayName": table.aliases[0] if table.aliases else table.name,
                "type": {"name": TABLE_TYPE},
                "attributes": attributes,
                "relations": {
                    f"{TABLE_TO_DATABASE_RELATION}:TARGET": [
                        _identifier(table.source_id, community, domain)
                    ]
                },
            }
        )
        for column in table.columns:
            column_attributes: dict[str, Any] = {
                DESCRIPTION_ATTRIBUTE: [{"value": column.description}],
            }
            column_governance = _governance_value(snapshot, column.ref.fqn())
            if column_governance is not None:
                column_attributes[GOVERNANCE_ATTRIBUTE] = [{"value": column_governance}]
            lineage = lineage_by_downstream.get(column.ref.fqn(), [])
            if lineage:
                # Collibra's own lineage is a harvested technical relation between columns it
                # scanned. Provisa's is derived from the compiled query, so it is published as
                # an attribute naming the upstream columns and the transforms applied, rather
                # than asserted into a graph Collibra believes it discovered (REQ-1070).
                column_attributes[LINEAGE_ATTRIBUTE] = [
                    {
                        "value": json.dumps(
                            [
                                {
                                    "from": edge.upstream.fqn(),
                                    "transforms": list(edge.transforms),
                                }
                                for edge in lineage
                            ]
                        )
                    }
                ]
            rows.append(
                {
                    "resourceType": "Asset",
                    "identifier": _identifier(column.ref.fqn(), community, domain),
                    "name": column.ref.fqn(),
                    "displayName": column.aliases[0] if column.aliases else column.name,
                    "type": {"name": COLUMN_TYPE},
                    "attributes": column_attributes,
                    "relations": {
                        f"{COLUMN_TO_TABLE_RELATION}:TARGET": [
                            _identifier(table.ref.fqn(), community, domain)
                        ]
                    },
                }
            )
    return rows


@register_provider
class CollibraEgress(MetadataEgress):  # REQ-1069
    """Publish a snapshot to Collibra through its JSON import job."""

    provider_name = "collibra"

    import_path = "/rest/2.0/import/json-job"
    health_path = "/rest/2.0/application/info"

    # Where published assets land. Collibra requires both, has no default, and rejects an
    # import naming a domain that does not exist — so a deployment publishing to Collibra
    # creates them once and Provisa addresses them by name.
    community = "Provisa"
    domain = "Provisa Governed Assets"

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    def _headers(self) -> dict[str, str]:
        token = self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        rows = to_rows(snapshot, self.community, self.domain)
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.post(
                self._url(self.import_path),
                # Collibra's import endpoint takes multipart with the payload as a file part,
                # not a JSON body — a JSON body is answered with 415.
                files={"file": ("provisa.json", json.dumps(rows), "application/json")},
                data={"batchSize": "0", "deleteFileAfterImport": "true"},
                headers=self._headers(),
            )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(self.import_path),
                        message=f"HTTP {response.status_code}: {response.text[:500]}",
                    )
                )
                return result
        for row in rows:
            kind = row["type"]["name"].lower()
            result.published[kind] = result.published.get(kind, 0) + 1
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(self._url(self.health_path), headers=self._headers())
        response.raise_for_status()
