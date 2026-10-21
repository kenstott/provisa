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

REQ-1389 — import-job merge semantics (verified against Collibra's own documentation, not
assumed):

* Attributes the payload does NOT mention are untouched. "If the resource exists with
  properties different from the ones defined in the input, the Import API replaces or creates
  the properties that are provided in the input and performs no action on the other existing
  properties." (https://developer.collibra.com/api/guides/import-api/import-commands.md)
  Steward-added attributes, tags, terms and comments Provisa never sends therefore survive
  every publish and reconcile.
* Attributes the payload DOES mention are set-replaced, including multi-value ones: "If the
  resource exists with properties that have multiple values, for example multi-value
  attributes or tags, the values provided in the input file are updated and the rest of the
  existing values are deleted." (same page). Every attribute type this adapter sends —
  ``Description`` and the ``Provisa *`` custom types — is Provisa-authored, so full
  replacement of those types IS the REQ-1389 drift correction, not a violation of it. No
  human-owned attribute type may ever be added to a row here: mentioning one would delete the
  steward-entered values of that type.
* Relations follow the same scoping. With the job default ``relationsAction=REPLACE``, the
  related-asset list is the complete final set per anchor asset and relation type — existing
  relations of a *mentioned* type not in the list are deleted — while relation types the
  payload never names (glossary assignments, hand-drawn relations) are untouched.
  (https://developer.collibra.com/api/guides/import-api.md, ``relationsAction`` /
  ``attributesAction``: REPLACE | ADD_OR_IGNORE, both defaulting to REPLACE.) This adapter
  only names the physical hierarchy relations (column→table, table→database), which are
  Provisa-owned, so REPLACE is correct there too. Both actions are pinned explicitly in the
  job form so the contract does not ride on a remote default.

URN rebind (REQ-1385/REQ-1389): the import job's only identity is the asset full name inside
its domain — the physical FQN — and the job itself has no rename operation. The rebind
therefore happens BEFORE the job: the exporter captures each table's asset UUID at publish
time (``catalog_bindings``), and when the stored binding shows the name changed it renames
the live asset first — ``PATCH /rest/2.0/assets/{id}`` with the new name — so the name-keyed
import that follows matches the SAME asset, enrichment intact, instead of creating a
duplicate. The canonical business identity is still published on every table as the
``Provisa URI`` attribute.

REQ-1387 — business glossary, and the containment argument that keeps REPLACE away from a
steward's glossaries. The paragraphs above establish that steward glossary items survive
because Provisa never mentions them; publishing terms natively means Provisa now DOES send
Business Term rows and term-to-term relations, so the argument is re-derived rather than
inherited:

* CONTAINMENT: Provisa publishes terms only into its own Glossary-type domain — community
  ``Provisa``, domain ``Provisa Glossary`` — created idempotently (find by name, create when
  absent, domain type looked up by name from the target) before the first term row is
  imported. Every glossary row's identifier and every term-to-term relation endpoint is an
  asset inside that domain. REPLACE's reach is per *mentioned* asset and *mentioned* relation
  type: the four term relation types this adapter names are set-replaced only on Provisa's
  own terms, so a hand-drawn relation of the same type between two steward terms is anchored
  on unmentioned assets and survives, and steward glossary assets — never mentioned — are
  untouched entirely.
* Every term row mentions all four term relation types, with an empty final set where the
  snapshot carries no edge of that type. The edge set anchored on a Provisa term is
  Provisa-authored (REQ-1387 ownership), so full replacement of it IS the drift correction,
  exactly as it is for the ``Provisa *`` attribute types above.
* Term-to-asset assignments are HUMAN-OWNED (REQ-1389): this adapter never writes an
  assignment relation, so the assignment relation type is never mentioned and steward
  assignments survive — the same never-mentioned argument the existing exclusions rest on.
* Nothing is deleted for absence: a term missing from the snapshot is simply not mentioned,
  and no asset lacking a stored Provisa binding is ever PATCHed.
* Deprecation is published as the asset's status (``Obsolete`` when deprecated, ``Accepted``
  otherwise) rather than a custom attribute: status is a property of an asset wholly inside
  Provisa's own glossary domain, so setting it collides with no steward workflow, and it is
  the marker Collibra's own UI badges.
* Ownership is the binding: each term row carries the ``Provisa URI`` attribute
  (``provisa://<org>/terms/<name>``), so terms ride the same UUID capture and
  rename-by-stored-UUID-before-import path tables do (REQ-1385/REQ-1389 above).
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071, REQ-1385, REQ-1387, REQ-1389

from __future__ import annotations

import json

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
from provisa.api.metadata_export.reader import (
    AssetMetadata,
    CatalogAsset,
    CatalogReader,
    register_reader,
)

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import MetadataSnapshot

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

# REQ-1634: Collibra's Data Product Ops module ships a "Data Product" asset type — unverified
# against a live target, same caveat atlan.py documents for its own DataProduct type guess. A
# product does not own its member tables (removing the product must never delete a table), so
# the relation is named as a grouping, not the containment vocabulary "is part of" uses above.
DATA_PRODUCT_TYPE = "Data Product"
DATA_PRODUCT_TABLE_RELATION = "Data Product groups Table"
DOMAIN_ATTRIBUTE = "Provisa Domain"

# Attributes Provisa writes. `Description` is Collibra's own; the rest are custom attribute
# types the target must have, and a target that lacks one refuses the rows carrying it — which
# the publish reports rather than dropping.
DESCRIPTION_ATTRIBUTE = "Description"
URI_ATTRIBUTE = "Provisa URI"  # REQ-1385: business-identity address
GOVERNANCE_ATTRIBUTE = "Provisa Governance"
RELATIONSHIP_ATTRIBUTE = "Provisa Approved Relationships"
LINEAGE_ATTRIBUTE = "Provisa Lineage"
STEWARD_ATTRIBUTE = "Provisa Steward"
# REQ-1443: the checks a registered contract makes about this asset, as a JSON document on the
# asset the checker OBSERVES. Collibra's own Data Quality Rule assets live in a rulebook a data
# office curates, and publishing rows into it would put Provisa-authored rules inside a
# steward-owned domain — the containment argument in the module docstring holds only while
# Provisa mentions its own assets. So the checks ride a Provisa-authored attribute instead, the
# same choice governance and lineage make above, and the target maps them onto its rulebook.
DATA_QUALITY_ATTRIBUTE = "Provisa Data Quality"

# REQ-1387: Collibra's own glossary vocabulary. Business Term and the Glossary domain type are
# out-of-the-box, as are the term-to-term relation types below — named as Collibra names them,
# "<head> <role> <tail>", the same convention as the physical hierarchy relations above. A
# target lacking one refuses the rows carrying it, which the publish reports.
GLOSSARY_TERM_TYPE = "Business Term"
GLOSSARY_DOMAIN_TYPE = "Glossary"
DEFINITION_ATTRIBUTE = "Definition"
DEPRECATED_STATUS = "Obsolete"
ACTIVE_STATUS = "Accepted"

# The closed Provisa edge enum mapped to Collibra's closest native term-to-term relations.
# Only Provisa's own terms are ever mentioned — see the module docstring's containment
# argument. Collibra ships no out-of-the-box relation type for VALID_VALUE_OF, DERIVED_FROM,
# REPLACES, PREFERRED_TERM_FOR, TRANSLATION_OF, or ANTONYM_OF, so each collapses onto the
# generic "is related to" rather than asserting a native type Collibra doesn't have (the same
# conservative choice atlas.py makes with seeAlso for its own unmapped types).
TERM_RELATION_TYPES = {
    "SYNONYM_OF": "Business Term is synonym of Business Term",
    "RELATED_TO": "Business Term is related to Business Term",
    "KIND_OF": "Business Term is a type of Business Term",
    "PART_OF": "Business Term is part of Business Term",
    "VALID_VALUE_OF": "Business Term is related to Business Term",
    "DERIVED_FROM": "Business Term is related to Business Term",
    "REPLACES": "Business Term is related to Business Term",
    "PREFERRED_TERM_FOR": "Business Term is related to Business Term",
    "TRANSLATION_OF": "Business Term is related to Business Term",
    "ANTONYM_OF": "Business Term is related to Business Term",
}


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


def _data_quality_value(snapshot: MetadataSnapshot, asset_fqn: str) -> str | None:  # REQ-1443
    """The contract checks made about one asset, as a JSON document.

    ``resultsTable`` names where the outcomes land, so a reader of the observed asset can reach
    the scan output; that table publishes as an ordinary table row alongside.
    """
    assertions = [a for a in snapshot.assertions if a.asset.fqn() == asset_fqn]
    if not assertions:
        return None
    return json.dumps(
        [
            {
                "checker": assertion.checker,
                "checkType": assertion.check_type,
                "definition": assertion.definition,
                "severity": assertion.severity,
                "resultsTable": assertion.results_table.fqn(),
                # REQ-1443: the most recent scan's verdict, carried in the same Provisa-authored
                # attribute rather than written into a steward-curated rulebook.
                "outcome": assertion.outcome.as_document(),
            }
            for assertion in assertions
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
        attributes: dict[str, Any] = {
            DESCRIPTION_ATTRIBUTE: [{"value": table.description}],
            URI_ATTRIBUTE: [{"value": table.semantic_uri}],  # REQ-1385
        }
        # A domain id the snapshot did not carry is a builder fault. Publishing the table
        # without its steward would report it as unowned.
        if table.domain_id:
            governing = domain_by_id[table.domain_id]
            if governing.steward is not None:
                attributes[STEWARD_ATTRIBUTE] = [{"value": governing.steward.id}]
        governance = _governance_value(snapshot, table.ref.fqn())
        if governance is not None:
            attributes[GOVERNANCE_ATTRIBUTE] = [{"value": governance}]
        data_quality = _data_quality_value(snapshot, table.ref.fqn())  # REQ-1443
        if data_quality is not None:
            attributes[DATA_QUALITY_ATTRIBUTE] = [{"value": data_quality}]
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
                                # REQ-1586: kind plus, on a junction-backed edge, the associative table it traverses.
                                **junction_payload(edge),
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
            column_data_quality = _data_quality_value(snapshot, column.ref.fqn())  # REQ-1443
            if column_data_quality is not None:
                column_attributes[DATA_QUALITY_ATTRIBUTE] = [{"value": column_data_quality}]
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
    for product in snapshot.data_products:  # REQ-1634
        attributes: dict[str, Any] = {
            DESCRIPTION_ATTRIBUTE: [{"value": product.description}],
            URI_ATTRIBUTE: [{"value": product.semantic_uri}],  # REQ-1385
            DOMAIN_ATTRIBUTE: [{"value": product.domain_id}],
        }
        if product.owner is not None:
            attributes[STEWARD_ATTRIBUTE] = [{"value": product.owner.id}]
        rows.append(
            {
                "resourceType": "Asset",
                "identifier": _identifier(product.ref.fqn(), community, domain),
                "name": product.ref.fqn(),
                "displayName": product.name,
                "type": {"name": DATA_PRODUCT_TYPE},
                "attributes": attributes,
                "relations": {
                    f"{DATA_PRODUCT_TABLE_RELATION}:TARGET": [
                        _identifier(member.fqn(), community, domain) for member in product.members
                    ]
                },
            }
        )
    return rows


def glossary_rows(
    snapshot: MetadataSnapshot, community: str, glossary_domain: str
) -> list[dict[str, Any]]:  # REQ-1387
    """The term graph as Collibra import rows, contained in Provisa's own glossary domain.

    Every identifier and every relation endpoint is inside ``glossary_domain`` — that
    containment is the whole safety argument for sending term relations under REPLACE (module
    docstring). Each row mentions all four term relation types, empty where the snapshot has
    no edge of that type: the edge set anchored on a Provisa term is Provisa-authored, so the
    empty set is the correct final set, not a deletion of someone else's work.
    """
    name_by_id = {term.term_id: term.name for term in snapshot.glossary_terms}
    relations_by_term: dict[int, dict[str, list[dict[str, Any]]]] = {}
    for edge in snapshot.glossary_edges:
        key = f"{TERM_RELATION_TYPES[edge.rel_type]}:TARGET"
        relations_by_term.setdefault(edge.from_term_id, {}).setdefault(key, []).append(
            _identifier(name_by_id[edge.to_term_id], community, glossary_domain)
        )
    rows: list[dict[str, Any]] = []
    for term in snapshot.glossary_terms:
        attributes: dict[str, Any] = {
            URI_ATTRIBUTE: [{"value": term.semantic_uri}],  # REQ-1385: binding key
        }
        if term.definition is not None:
            # Provisa-authored on Provisa's own term (REQ-1389), so set-replacement of the
            # Definition type on THIS asset is the drift correction.
            attributes[DEFINITION_ATTRIBUTE] = [{"value": term.definition}]
        term_relations = relations_by_term.get(term.term_id, {})
        rows.append(
            {
                "resourceType": "Asset",
                "identifier": _identifier(term.name, community, glossary_domain),
                "name": term.name,
                "displayName": term.name,
                "type": {"name": GLOSSARY_TERM_TYPE},
                # Deprecation marker: Collibra status (module docstring).
                "status": {"name": DEPRECATED_STATUS if term.deprecated else ACTIVE_STATUS},
                "attributes": attributes,
                "relations": {
                    f"{relation}:TARGET": term_relations.get(f"{relation}:TARGET", [])
                    for relation in TERM_RELATION_TYPES.values()
                },
            }
        )
    return rows


@register_provider
class CollibraExport(MetadataExport):  # REQ-1069
    """Publish a snapshot to Collibra through its JSON import job."""

    provider_name = "collibra"

    import_path = "/rest/2.0/import/json-job"
    health_path = "/rest/2.0/application/info"

    # Where published assets land. Collibra requires both, has no default, and rejects an
    # import naming a domain that does not exist — so a deployment publishing to Collibra
    # creates them once and Provisa addresses them by name.
    community = "Provisa"
    domain = "Provisa Governed Assets"
    # REQ-1387: the Glossary-type domain terms land in. Unlike the asset domain above it is
    # created idempotently by the publish itself — the containment argument requires the
    # domain to exist and to be Provisa's own before any term row is imported.
    glossary_domain = "Provisa Glossary"

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    def _headers(self) -> dict[str, str]:
        token = self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _row_uri(row: dict[str, Any]) -> str | None:
        """The Provisa URN a row carries, when it carries one (tables do)."""
        values = row.get("attributes", {}).get(URI_ATTRIBUTE)
        return values[0]["value"] if values else None

    async def _rename_rebound_assets(
        self,
        client: httpx.AsyncClient,
        rows: list[dict[str, Any]],
        result: PublishResult,
    ) -> None:
        """Rename physically re-addressed assets BEFORE the import job (REQ-1389).

        The import upserts by full name; a stored binding whose name differs from the
        outgoing row means the same Provisa asset re-addressed. Renaming the live asset by
        its stored UUID first makes the name-keyed import match the SAME asset — enrichment
        intact — instead of minting a duplicate. A failed rename is reported and the import
        proceeds: it then creates a new asset, which the error names.
        """
        for row in rows:
            uri = self._row_uri(row)
            if uri is None:
                continue
            stored = self._bindings.get(uri)
            if stored is None or stored[1] == row["name"]:
                continue
            asset_id, old_name = stored
            response = await client.patch(
                self._url(f"/rest/2.0/assets/{asset_id}"),
                json={"name": row["name"], "displayName": row["displayName"]},
                headers=self._headers(),
            )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(row["name"]),
                        message=(
                            f"rebind rename of {old_name!r} (asset {asset_id}) to "
                            f"{row['name']!r} failed (HTTP {response.status_code}: "
                            f"{response.text[:300]}); the import will create a new asset "
                            "and the old one keeps its enrichment"
                        ),
                    )
                )

    async def _capture_bindings(
        self,
        client: httpx.AsyncClient,
        rows: list[dict[str, Any]],
        result: PublishResult,
    ) -> None:
        """Record each URI-carrying asset's Collibra UUID after a successful import (REQ-1389).

        Only assets with no stored binding — or whose name changed — are looked up, so a
        steady-state reconcile costs zero extra requests. The lookup is by exact full name;
        an asset the import just accepted but the lookup cannot find is reported, not
        skipped silently.
        """
        for row in rows:
            uri = self._row_uri(row)
            if uri is None:
                continue
            stored = self._bindings.get(uri)
            if stored is not None and stored[1] == row["name"]:
                result.bindings[uri] = stored
                continue
            response = await client.get(
                self._url("/rest/2.0/assets"),
                params={"name": row["name"], "nameMatchMode": "EXACT"},
                headers=self._headers(),
            )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(row["name"]),
                        message=(
                            f"binding capture lookup failed (HTTP {response.status_code}); "
                            "the vendor id for this asset was not recorded"
                        ),
                    )
                )
                continue
            matches = [
                r for r in response.json().get("results", []) if r.get("name") == row["name"]
            ]
            if not matches:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(row["name"]),
                        message=(
                            "binding capture found no asset by this name after a successful "
                            "import; the vendor id was not recorded"
                        ),
                    )
                )
                continue
            result.bindings[uri] = (matches[0]["id"], row["name"])

    async def _find_by_name(
        self, client: httpx.AsyncClient, path: str, params: dict[str, str], name: str
    ) -> str | None:
        """The id of the resource at ``path`` exactly named ``name``, or None if absent.

        Collibra's list endpoints match by prefix, so the exact filter is applied here; a
        failed listing raises rather than reading as absence — creating over an unreadable
        target is exactly the blind write this adapter never performs.
        """
        response = await client.get(self._url(path), params=params, headers=self._headers())
        response.raise_for_status()
        for entry in response.json().get("results", []):
            if entry.get("name") == name:
                return entry["id"]
        return None

    async def _ensure_glossary_domain(
        self, client: httpx.AsyncClient, result: PublishResult
    ) -> bool:
        """Idempotently create the Provisa community and Glossary-type domain (REQ-1387).

        Find-by-name first, create only when absent, domain type resolved from the target by
        name rather than a hardcoded UUID (Collibra ids are instance configuration). Returns
        False — with the failure reported, never swallowed — when the domain cannot be
        guaranteed, and the publish then withholds the term rows: importing into a domain
        that may not exist would fail the whole batch, tables included.
        """
        try:
            community_id = await self._find_by_name(
                client, "/rest/2.0/communities", {"name": self.community}, self.community
            )
            if community_id is None:
                response = await client.post(
                    self._url("/rest/2.0/communities"),
                    json={"name": self.community},
                    headers=self._headers(),
                )
                response.raise_for_status()
                community_id = response.json()["id"]
            domain_id = await self._find_by_name(
                client,
                "/rest/2.0/domains",
                {"name": self.glossary_domain, "communityId": community_id},
                self.glossary_domain,
            )
            if domain_id is None:
                type_id = await self._find_by_name(
                    client,
                    "/rest/2.0/domainTypes",
                    {"name": GLOSSARY_DOMAIN_TYPE},
                    GLOSSARY_DOMAIN_TYPE,
                )
                if type_id is None:
                    result.errors.append(
                        AssetError(
                            asset=AssetRefStub(self.glossary_domain),
                            message=(
                                f"the target has no {GLOSSARY_DOMAIN_TYPE!r} domain type, so "
                                "the Provisa glossary domain cannot be created; glossary "
                                "terms were not published"
                            ),
                        )
                    )
                    return False
                response = await client.post(
                    self._url("/rest/2.0/domains"),
                    json={
                        "name": self.glossary_domain,
                        "communityId": community_id,
                        "typeId": type_id,
                    },
                    headers=self._headers(),
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as error:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(self.glossary_domain),
                    message=(
                        f"ensuring the Provisa glossary domain failed (HTTP "
                        f"{error.response.status_code}: {error.response.text[:300]}); "
                        "glossary terms were not published"
                    ),
                )
            )
            return False
        return True

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        rows = to_rows(snapshot, self.community, self.domain)
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            # REQ-1387: terms ride the same import job, contained in Provisa's own glossary
            # domain — created first, since the import rejects rows naming an absent domain.
            if snapshot.glossary_terms and await self._ensure_glossary_domain(client, result):
                rows = rows + glossary_rows(snapshot, self.community, self.glossary_domain)
            # REQ-2122: picked tables publish onto the picked assets; locked ones keep their
            # catalog's descriptions.
            rows = await self._retarget(client, snapshot, rows, result)
            # REQ-1389: rebind by stored UUID happens BEFORE the name-keyed import.
            await self._rename_rebound_assets(client, rows, result)
            response = await client.post(
                self._url(self.import_path),
                # Collibra's import endpoint takes multipart with the payload as a file part,
                # not a JSON body — a JSON body is answered with 415.
                files={"file": ("provisa.json", json.dumps(rows), "application/json")},
                # REQ-1389: REPLACE is pinned rather than inherited from the job default.
                # Its scope is only what the payload mentions — Provisa-authored attribute
                # types and the physical hierarchy relations — so it is the drift
                # correction; everything unmentioned (steward attributes, tags, terms,
                # other relation types) is untouched by the job. See the module docstring
                # for the cited semantics.
                data={
                    "batchSize": "0",
                    "deleteFileAfterImport": "true",
                    "attributesAction": "REPLACE",
                    "relationsAction": "REPLACE",
                },
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
            # REQ-1389: capture the vendor's own asset UUIDs for the assets just imported.
            await self._capture_bindings(client, rows, result)
        for row in rows:
            kind = row["type"]["name"].lower()
            result.published[kind] = result.published.get(kind, 0) + 1
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(self._url(self.health_path), headers=self._headers())
        response.raise_for_status()

    async def _asset_identifier(self, client: httpx.AsyncClient, asset_id: str) -> dict[str, Any]:
        """A live asset's import identifier: its name, its domain and the domain's community."""
        asset = await self._get_json(client, f"/rest/2.0/assets/{asset_id}")
        domain = await self._get_json(client, f"/rest/2.0/domains/{asset['domain']['id']}")
        return {
            "name": asset["name"],
            "domain": {"name": domain["name"], "community": {"name": domain["community"]["name"]}},
            "_type": asset["type"]["name"],
        }

    async def _get_json(self, client: httpx.AsyncClient, path: str, **params: Any) -> Any:
        response = await client.get(self._url(path), params=params, headers=self._headers())
        response.raise_for_status()
        return response.json()

    async def _retarget(
        self,
        client: httpx.AsyncClient,
        snapshot: MetadataSnapshot,
        rows: list[dict[str, Any]],
        result: PublishResult,
    ) -> list[dict[str, Any]]:
        """Publish picked tables onto the assets stewards picked, and keep a locked table's
        descriptions as the catalog holds them (REQ-2122).

        A picked table's own row and its columns' rows are not imported. The picked asset takes
        the table's Provisa address and, unless the table is locked here, its description; the
        picked asset's columns take the descriptions of the table's columns of the same name; a
        data product groups the picked asset in place of a table of the export's own. A locked
        table's own rows are sent without their Description, which the import then leaves as
        the catalog holds it.
        """
        by_fqn = {t.ref.fqn(): t for t in snapshot.tables}
        picked = {fqn: t for fqn, t in by_fqn.items() if t.semantic_uri in self.picked}
        locked = {fqn for fqn, t in by_fqn.items() if t.semantic_uri in self.locked}
        if not picked and not locked:
            return rows
        targets: dict[str, dict[str, Any]] = {}
        added: list[dict[str, Any]] = []
        for fqn, table in picked.items():
            ref, _name = self.picked[table.semantic_uri]
            try:
                identifier = await self._asset_identifier(client, ref)
                type_name = identifier.pop("_type")
                reader = CollibraReader(self._config)
                meta = await reader.read(ref)
            except (httpx.HTTPError, KeyError) as exc:
                result.errors.append(
                    AssetError(asset=table.ref, message=f"the picked asset {ref}: {exc}")
                )
                continue
            targets[fqn] = identifier
            result.bindings[table.semantic_uri] = (ref, identifier["name"])
            attributes: dict[str, Any] = {URI_ATTRIBUTE: [{"value": table.semantic_uri}]}
            if fqn not in locked:
                attributes[DESCRIPTION_ATTRIBUTE] = [{"value": table.description}]
            added.append(
                {
                    "resourceType": "Asset",
                    "identifier": identifier,
                    "type": {"name": type_name},
                    "attributes": attributes,
                }
            )
            if fqn in locked:
                continue
            ours = {c.name.lower(): c.description for c in table.columns}
            prefix = identifier["name"]
            separator = ">" if ">" in prefix else "."
            for name in meta.columns:
                if name.lower() not in ours:
                    continue
                added.append(
                    {
                        "resourceType": "Asset",
                        "identifier": {**identifier, "name": f"{prefix}{separator}{name}"},
                        "type": {"name": COLUMN_TYPE},
                        "attributes": {DESCRIPTION_ATTRIBUTE: [{"value": ours[name.lower()]}]},
                    }
                )
        set_aside = {
            json.dumps(_identifier(fqn, self.community, self.domain), sort_keys=True)
            for fqn in picked
        }
        kept: list[dict[str, Any]] = []
        for row in rows:
            key = json.dumps(row["identifier"], sort_keys=True)
            parent = (row.get("relations") or {}).get(f"{COLUMN_TO_TABLE_RELATION}:TARGET") or []
            if key in set_aside or any(json.dumps(p, sort_keys=True) in set_aside for p in parent):
                continue
            row_fqn = row["name"]
            owner_fqn = ".".join(row_fqn.split(".")[:3])
            if row_fqn in locked or (row["type"]["name"] == COLUMN_TYPE and owner_fqn in locked):
                row = {
                    **row,
                    "attributes": {
                        k: v for k, v in row["attributes"].items() if k != DESCRIPTION_ATTRIBUTE
                    },
                }
            grouped = (row.get("relations") or {}).get(f"{DATA_PRODUCT_TABLE_RELATION}:TARGET")
            if grouped:
                row = {
                    **row,
                    "relations": {
                        **row["relations"],
                        f"{DATA_PRODUCT_TABLE_RELATION}:TARGET": [
                            targets.get(m["name"], m) for m in grouped
                        ],
                    },
                }
            kept.append(row)
        return kept + added


#: The public ids of the operating model's types a reader follows: a table's Description, the
#: relation a column has to its table, and the one a Business Term has to the asset it describes.
DESCRIPTION_PUBLIC_ID = "Description"
COLUMN_OF_TABLE_PUBLIC_ID = "ColumnIsPartOfTable"
TERM_REPRESENTS_PUBLIC_ID = "BusinessAssetRepresentsDataAsset"


def _collibra_physical(name: str) -> tuple[str, ...]:
    """A Collibra table's full name as its path's parts. Collibra's own ingestion separates the
    parts with ``>`` (``server>db>schema>table``); an imported one may use ``.``."""
    separator = ">" if ">" in name else "."
    return tuple(part for part in name.split(separator) if part)


@register_reader
class CollibraReader(CatalogReader):  # REQ-2122
    """Reads a Collibra instance's Table assets over its REST API: their Description attributes,
    the columns that are part of them, their tags, the business terms that represent them, and
    the people responsible for them."""

    provider_name = "collibra"

    def _url(self, path: str) -> str:
        return f"{self._catalog.endpoint.rstrip('/')}{path}"

    def _headers(self) -> dict[str, str]:
        token = self._catalog.token.get_secret_value() or self._catalog.api_key.get_secret_value()
        return {"Authorization": f"Bearer {token}"} if token else {}

    def asset_url(self, ref: str, name: str) -> str | None:
        del name
        return self._url(f"/asset/{ref}")

    async def _get(self, client: httpx.AsyncClient, path: str, **params: Any) -> Any:
        response = await client.get(self._url(path), params=params, headers=self._headers())
        response.raise_for_status()
        return response.json()

    async def _type_id(self, client: httpx.AsyncClient, kind: str, public_id: str) -> str:
        """The id of the operating model's type with ``public_id`` (``attributeTypes`` or
        ``relationTypes``): list filters take ids, and ids differ between instances."""
        found = await self._get(client, f"/rest/2.0/{kind}/publicId/{public_id}")
        return found["id"]

    async def _description(self, client: httpx.AsyncClient, asset_id: str, type_id: str) -> str:
        attributes = await self._get(
            client, "/rest/2.0/attributes", assetId=asset_id, typeIds=type_id
        )
        values = [a.get("value") or "" for a in attributes.get("results", [])]
        return values[0] if values else ""

    async def _tags(self, client: httpx.AsyncClient, asset_id: str) -> tuple[str, ...]:
        return tuple(
            t["name"] for t in await self._get(client, f"/rest/2.0/assets/{asset_id}/tags")
        )

    async def _terms(
        self, client: httpx.AsyncClient, asset_id: str, type_id: str
    ) -> tuple[str, ...]:
        relations = await self._get(
            client, "/rest/2.0/relations", targetId=asset_id, relationTypeId=type_id, limit=1000
        )
        return tuple(r["source"]["name"] for r in relations.get("results", []))

    def _asset(self, entry: dict[str, Any], description: str) -> CatalogAsset:
        name = entry.get("name") or entry.get("displayName") or entry["id"]
        return CatalogAsset(
            ref=entry["id"],
            name=name,
            physical=_collibra_physical(name),
            description=description,
            url=self.asset_url(entry["id"], name),
        )

    async def search(self, text: str, limit: int) -> list[CatalogAsset]:
        async with httpx.AsyncClient(timeout=self._catalog.timeout_seconds) as client:
            found = await self._get(
                client,
                "/rest/2.0/assets",
                name=text,
                nameMatchMode="ANYWHERE",
                typePublicIds="Table",
                limit=limit,
            )
            return [self._asset(entry, "") for entry in found.get("results", [])]

    async def read(self, ref: str) -> AssetMetadata:
        async with httpx.AsyncClient(timeout=self._catalog.timeout_seconds) as client:
            described = await self._type_id(client, "attributeTypes", DESCRIPTION_PUBLIC_ID)
            part_of = await self._type_id(client, "relationTypes", COLUMN_OF_TABLE_PUBLIC_ID)
            represents = await self._type_id(client, "relationTypes", TERM_REPRESENTS_PUBLIC_ID)
            entry = await self._get(client, f"/rest/2.0/assets/{ref}")
            description = await self._description(client, ref, described)
            parts = await self._get(
                client, "/rest/2.0/relations", targetId=ref, relationTypeId=part_of, limit=1000
            )
            columns: dict[str, str] = {}
            column_tags: dict[str, tuple[str, ...]] = {}
            column_terms: dict[str, tuple[str, ...]] = {}
            for relation in parts.get("results", []):
                column = relation["source"]
                # A column's full name ends with its own name, after the table's path.
                name = _collibra_physical(column["name"])[-1]
                columns[name] = await self._description(client, column["id"], described)
                column_tags[name] = await self._tags(client, column["id"])
                column_terms[name] = await self._terms(client, column["id"], represents)
            responsible = await self._get(
                client, "/rest/2.0/responsibilities", resourceIds=ref, limit=100
            )
            owners = []
            for responsibility in responsible.get("results", []):
                owner = responsibility.get("owner") or {}
                if owner.get("resourceType") == "User":
                    user = await self._get(client, f"/rest/2.0/users/{owner['id']}")
                    owners.append(
                        " ".join(p for p in (user.get("firstName"), user.get("lastName")) if p)
                        or user.get("userName")
                    )
                elif owner.get("resourceType") == "UserGroup":
                    group = await self._get(client, f"/rest/2.0/userGroups/{owner['id']}")
                    owners.append(group.get("name"))
            return AssetMetadata(
                asset=self._asset(entry, description),
                description=description,
                columns=columns,
                owners=tuple(o for o in owners if o),
                tags=await self._tags(client, ref),
                terms=await self._terms(client, ref, represents),
                column_tags=column_tags,
                column_terms=column_terms,
            )
