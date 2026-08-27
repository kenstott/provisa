# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1b8d47-6c02-4ae9-9d15-7b204ec8a361
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Atlan adapter (REQ-1069).

Atlan is built on Atlas, and its ingestion API keeps the Atlas entity envelope — so this is a
subclass over the same mapping rather than a second implementation. Three things differ, and
they are the whole of this module:

* the routes are mounted under ``/api/meta`` instead of ``/api/atlas``;
* an Atlan entity is typed by Atlan's own type set (``Table``, ``Column``, ``Database``,
  ``Connection``) rather than by Atlas's RDBMS types;
* an Atlan asset must name the ``connectionQualifiedName`` it belongs to, and its
  qualifiedName is rooted at that connection rather than suffixed with a cluster name.

Governance signals stay Atlas classifications, which Atlan calls tags but transports
identically (REQ-1071).

The business glossary (REQ-1387) publishes natively: Atlan carries the Atlas glossary model
(``AtlasGlossary`` / ``AtlasGlossaryTerm``) over the same entity envelope, so terms ride the
same bulk route into one Provisa-owned glossary.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071, REQ-1387, REQ-1389

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

from provisa.api.metadata_export.atlas import (
    AtlasEntity,
    AtlasExport,
    _GuidCounter,
    to_entities,
)
from provisa.api.metadata_export.provider import AssetError, AssetRefStub, PublishResult
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import GlossaryTermAsset, MetadataSnapshot

# Atlas type name -> Atlan type name. Atlan rejects an entity whose typeName is not in its own
# type set, and it does not ship the Atlas RDBMS types.
TYPE_MAP = {
    "rdbms_instance": "Connection",
    "rdbms_db": "Database",
    "rdbms_table": "Table",
    "rdbms_column": "Column",
    "Process": "Process",
}

# Every Atlan asset declares which source system it came from. Provisa is the governed access
# path, so that is what the assets are attributed to rather than the federated source.
CONNECTOR_NAME = "provisa"

# REQ-1387: every Provisa term publishes into ONE Provisa-owned glossary, addressed by a
# stable per-org qualifiedName. No other glossary is ever read, written or deleted.
GLOSSARY_TYPE = "AtlasGlossary"
TERM_TYPE = "AtlasGlossaryTerm"
GLOSSARY_NAME = "Provisa Glossary"

# REQ-1387: Provisa's closed term-relation enum, mapped onto the nearest native
# AtlasGlossaryTerm relationship attribute (Atlan transports the Atlas glossary model):
#   KIND_OF    -> isA       the from-term declares the to-term among the terms it is a
#                           kind of; Atlas maintains the inverse (`classifies`) itself;
#   SYNONYM_OF -> synonyms  symmetric on both ends, exactly as in Provisa;
#   RELATED_TO -> seeAlso   Atlas's undirected related-terms relation;
#   PART_OF    -> seeAlso   Atlas's glossary model has no meronymy relation between terms
#                           (containment exists only toward categories), so the nearest
#                           term-to-term relation is the related-terms one;
#   VALID_VALUE_OF -> validValuesFor  written from the value term, matching Atlas's own end;
#   PREFERRED_TERM_FOR -> preferredToTerms  written from the preferred term;
#   TRANSLATION_OF -> translatedTerms  written from the translation, listing the original(s);
#   ANTONYM_OF -> antonyms  symmetric, either endpoint may write it;
#   REPLACES   -> seeAlso   Atlas's replacementTerms/replacedBy pair is written from the
#                           deprecated term, the opposite of this map's `from` side, so using
#                           it here would assert the reverse of REPLACES; seeAlso avoids the
#                           false claim;
#   DERIVED_FROM -> seeAlso  no lineage-flavored term relation exists in this model.
TERM_RELATION_MAP = {
    "KIND_OF": "isA",
    "SYNONYM_OF": "synonyms",
    "RELATED_TO": "seeAlso",
    "PART_OF": "seeAlso",
    "VALID_VALUE_OF": "validValuesFor",
    "PREFERRED_TERM_FOR": "preferredToTerms",
    "TRANSLATION_OF": "translatedTerms",
    "ANTONYM_OF": "antonyms",
    "REPLACES": "seeAlso",
    "DERIVED_FROM": "seeAlso",
}

# REQ-1389 field ownership on glossary terms: the definition rides ``longDescription`` —
# Provisa-owned, replaced on every publish (a definition removed in Provisa clears it).
# ``userDescription`` is the field Atlan's UI hands to humans, so it is NEVER written; a
# steward's note on a Provisa term survives every publish. Deprecation convention:
# ``certificateStatus = "DEPRECATED"`` — the construct Atlan consumers already surface,
# matching this adapter's model-tag mapping — and Provisa clears only a DEPRECATED it
# wrote; any other status is human curation and passes through the read-merge untouched.
TERM_DEPRECATION_STATUS = "DEPRECATED"
TERM_DEPRECATION_MESSAGE = "Deprecated in the Provisa registry."


@register_provider
class AtlanExport(AtlasExport):  # REQ-1069
    """Publish a snapshot to Atlan over its Atlas-shaped ingestion API.

    REQ-1443: contract checks ride the inherited Atlas governance document rather than Atlan's
    own data-quality rules. That surface is addressed through routes this adapter has not
    verified — the same reason ``_canonicalize_identity`` and ``classification_merge`` are off
    here — so the checks publish where every Atlan consumer can already read them.
    """

    provider_name = "atlan"

    entity_bulk_path = "/api/meta/entity/bulk"
    typedefs_path = "/api/meta/types/typedefs"
    classificationdef_path = "/api/meta/types/classificationdef/name"
    unique_attr_path = "/api/meta/entity/uniqueAttribute/type"
    entity_guid_path = "/api/meta/entity/guid"
    health_path = "/api/meta/types/typedefs/headers"
    # Atlan's per-guid classification routes are unverified; REQ-1389 merge stays off here.
    classification_merge = False

    async def _ensure_type_system(self, client, headers) -> None:
        """Atlan publishes through its own built-in types (REQ-1388's documented caveat:
        custom types get reduced native UI treatment there), so no provisa_* typedefs."""

    async def _canonicalize_identity(self, client, headers, entities) -> None:
        """Atlan's search route for the URN-canonical rebind is unverified; off until then."""

    def _connection_qn(self, snapshot: MetadataSnapshot) -> str:
        """Atlan's root address for everything this org publishes.

        Atlan's convention is ``<tenant>/<connector>/<name>``; the org id is what separates one
        Provisa tenant's assets from another's inside a shared Atlan workspace.
        """
        return f"default/{CONNECTOR_NAME}/{snapshot.org_id}"

    def _atlan_entities(self, snapshot: MetadataSnapshot) -> list[AtlasEntity]:
        connection_qn = self._connection_qn(snapshot)
        entities = to_entities(snapshot)
        # REQ-1375: 'deprecated' maps to Atlan's native certificate status — the construct
        # Atlan consumers already surface — in addition to the inherited classification.
        deprecated_fqns = {
            tag.asset.fqn(): tag
            for tag in snapshot.model_tags
            if tag.tag_id == "deprecated" and tag.asset is not None
        }
        for entity in entities:
            # A type Atlan has no equivalent for would be published under a name its API
            # rejects. Mapping is total, so an unmapped type is a wiring fault and raises here
            # rather than reaching the wire.
            entity.type_name = TYPE_MAP[entity.type_name]
            entity.attributes["connectorName"] = CONNECTOR_NAME
            if entity.kind != "instance":
                entity.attributes["connectionQualifiedName"] = connection_qn
            if entity.asset is not None and entity.asset.fqn() in deprecated_fqns:
                _dep = deprecated_fqns[entity.asset.fqn()]
                entity.attributes["certificateStatus"] = "DEPRECATED"
                # The steward's stated reason (required at assignment) plus the planned
                # removal date, in the message Atlan renders beside the status.
                _msg = _dep.reason or "Tagged 'deprecated' in the Provisa registry."
                if _dep.expires_on:
                    _msg = f"{_msg} (removal: {_dep.expires_on})"
                entity.attributes["certificateStatusMessage"] = _msg
        return entities

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        result = await self._publish_entities(self._atlan_entities(snapshot), snapshot)
        if snapshot.glossary_terms:
            await self._publish_atlan_glossary(snapshot, result)
        return result

    # --- REQ-1387: business glossary --------------------------------------------------------

    def _glossary_qn(self, org_id: str) -> str:
        """The Provisa glossary's stable per-org address — the idempotency key.

        The container is re-found by this qualifiedName on every publish rather than by a
        stored binding: the binding store prunes to the snapshot's asset URIs, and the
        container is a vendor-side structure, not a snapshot asset.
        """
        return f"provisa/{org_id}/glossary"

    async def _glossary_guid(
        self, client: httpx.AsyncClient, headers: dict[str, str], qn: str
    ) -> str | None:
        """The live guid of the Provisa glossary, or None when this publish must create it."""
        probe = await client.get(
            self._url(f"{self.unique_attr_path}/{GLOSSARY_TYPE}"),
            params={"attr:qualifiedName": qn},
            headers=headers,
        )
        if probe.status_code == 404:
            return None
        probe.raise_for_status()
        return probe.json()["entity"]["guid"]

    @staticmethod
    def _apply_deprecation(attributes: dict, term: GlossaryTermAsset) -> None:
        """The documented convention: DEPRECATED asserted while the term is deprecated,
        and cleared only when the live value is the one Provisa itself writes."""
        if term.deprecated:
            attributes["certificateStatus"] = TERM_DEPRECATION_STATUS
            attributes["certificateStatusMessage"] = TERM_DEPRECATION_MESSAGE
        elif attributes.get("certificateStatus") == TERM_DEPRECATION_STATUS:
            # Bulk updates replace the full attribute map, so dropping the pair clears it.
            del attributes["certificateStatus"]
            attributes.pop("certificateStatusMessage", None)

    def _new_term_entity(
        self, term: GlossaryTermAsset, glossary_guid: str, glossary_qn: str, guid: str
    ) -> AtlasEntity:
        attributes = {
            "qualifiedName": f"{term.name}@{glossary_qn}",
            "name": term.name,
            # REQ-1389: longDescription is the Provisa-owned definition field;
            # userDescription belongs to humans in Atlan's UI and is never written.
            "longDescription": term.definition or "",
            "provisaUri": term.semantic_uri,
        }
        self._apply_deprecation(attributes, term)
        return AtlasEntity(
            asset=AssetRefStub(term.name),
            kind="term",
            type_name=TERM_TYPE,
            guid=guid,
            attributes=attributes,
            # REQ-1389: term-to-asset assignment is HUMAN-OWNED — ``assignedEntities`` is
            # never written; ``term.refs`` stay Provisa-internal.
            relationships={"anchor": {"typeName": GLOSSARY_TYPE, "guid": glossary_guid}},
        )

    async def _bound_term_entity(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        term: GlossaryTermAsset,
        binding: tuple[str, str],
        glossary_guid: str,
        result: PublishResult,
    ) -> AtlasEntity | None:
        """The read-merged update for a term Provisa already owns (REQ-1387/1389).

        The binding's guid is the identity, so a Provisa-side rename UPDATES the same
        vendor term; the live qualifiedName is kept verbatim. A bound term whose live
        anchor is not the Provisa glossary is refused, never modified (ownership rule).
        """
        live_guid, _physical_key = binding
        fetch = await client.get(self._url(f"{self.entity_guid_path}/{live_guid}"), headers=headers)
        if fetch.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(term.name),
                    message=f"glossary term fetch failed: HTTP {fetch.status_code}",
                )
            )
            return None
        live = fetch.json()["entity"]
        anchor = (live.get("relationshipAttributes") or {}).get("anchor") or {}
        if anchor.get("guid") != glossary_guid:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(term.name),
                    message="bound term is anchored outside the Provisa glossary; refusing "
                    "to modify it",
                )
            )
            return None
        # Read-merge: start from the live object, overlay only Provisa-authored fields —
        # userDescription and every other human field survive verbatim.
        attributes = {**(live.get("attributes") or {})}
        attributes["name"] = term.name
        attributes["longDescription"] = term.definition or ""
        attributes["provisaUri"] = term.semantic_uri
        self._apply_deprecation(attributes, term)
        return AtlasEntity(
            asset=AssetRefStub(term.name),
            kind="term",
            type_name=TERM_TYPE,
            guid=live_guid,
            attributes=attributes,
            relationships={"anchor": {"typeName": GLOSSARY_TYPE, "guid": glossary_guid}},
        )

    async def _publish_atlan_glossary(
        self, snapshot: MetadataSnapshot, result: PublishResult
    ) -> None:
        """Publish the term graph into the Provisa-owned glossary (REQ-1387).

        Named apart from ``AtlasExport._publish_glossary`` rather than overriding it: Atlan's
        glossary API takes a different route and a different payload, so this opens its own client
        and headers and shares no parameters with the base. ``publish`` here is overridden too, so
        the base's call site never reaches this method.

        Every term lands in the ONE glossary Provisa creates and owns. A vendor-side term
        is only ever addressed through a stored Provisa binding, nothing outside the
        Provisa glossary is read or written, and nothing is ever deleted — a term absent
        from the snapshot is simply not sent (its vendor copy is left standing).
        """
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            headers = await self._headers(client)
            glossary_qn = self._glossary_qn(snapshot.org_id)
            guid = _GuidCounter()
            entities: list[AtlasEntity] = []
            glossary_guid = await self._glossary_guid(client, headers, glossary_qn)
            if glossary_guid is None:
                glossary_guid = guid.next()
                entities.append(
                    AtlasEntity(
                        asset=AssetRefStub(GLOSSARY_NAME),
                        kind="glossary",
                        type_name=GLOSSARY_TYPE,
                        guid=glossary_guid,
                        attributes={
                            "qualifiedName": glossary_qn,
                            "name": GLOSSARY_NAME,
                            "shortDescription": "Business vocabulary governed by Provisa.",
                        },
                    )
                )
            by_term_id: dict[int, AtlasEntity] = {}
            for term in snapshot.glossary_terms:
                binding = self._bindings.get(term.semantic_uri)
                if binding is None:
                    entity = self._new_term_entity(term, glossary_guid, glossary_qn, guid.next())
                else:
                    entity = await self._bound_term_entity(
                        client, headers, term, binding, glossary_guid, result
                    )
                    if entity is None:
                        continue
                by_term_id[term.term_id] = entity
                entities.append(entity)
            for edge in snapshot.glossary_edges:
                source = by_term_id.get(edge.from_term_id)
                target = by_term_id.get(edge.to_term_id)
                if source is None or target is None:
                    # The refused endpoint already reported its own error above.
                    continue
                source.relationships.setdefault(TERM_RELATION_MAP[edge.rel_type], []).append(
                    {"typeName": TERM_TYPE, "guid": target.guid}
                )
            await self._send_glossary(client, headers, entities, result)

    async def _send_glossary(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        entities: list[AtlasEntity],
        result: PublishResult,
    ) -> None:
        if not entities:
            return
        response = await client.post(
            self._url(self.entity_bulk_path),
            json={"entities": [entity.payload() for entity in entities]},
            headers=headers,
        )
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub("glossary"),
                    message=f"HTTP {response.status_code}: {response.text[:500]}",
                )
            )
            return
        # REQ-1387/1389: the captured guid IS the ownership record — the next publish
        # addresses the same vendor term through it, which is what makes renames update
        # in place and unbound vendor items untouchable.
        assignments = response.json().get("guidAssignments") or {}
        for entity in entities:
            uri = entity.attributes.get("provisaUri")
            if uri:
                resolved = assignments.get(entity.guid, entity.guid)
                if not resolved.startswith("-"):
                    result.bindings[uri] = (resolved, entity.attributes["qualifiedName"])
            result.published[entity.kind] = result.published.get(entity.kind, 0) + 1
