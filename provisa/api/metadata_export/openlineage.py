# Copyright (c) 2026 Kenneth Stott
# Canary: 8d15f0b7-2a63-45ce-91f4-c30e6b8d7291
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""OpenLineage adapter — the standards-first lineage target (REQ-1069).

Two event shapes carry the snapshot, matching what the spec says each is for:

* a **DatasetEvent** per table, which registers the dataset and its columns with no run
  attached. Emitting base tables as the output of a fabricated job would claim a producing
  process that does not exist, so the static event is the only correct shape for them.
* a **RunEvent** per derived table, whose ``columnLineage`` facet carries the per-column
  derivations REQ-1070 resolved from the compiled view SQL.

Facets that the spec standardises — ``schema``, ``documentation``, ``ownership``,
``columnLineage`` — are used as standardised. Provisa knows two things OpenLineage has no
standard facet for, approved relationships and governance signals, and those ride in custom
``provisa_*`` facets, which the spec provides for exactly this and which a consumer that does
not understand them ignores rather than mis-reads.

Run identity is derived (uuid5 over the job name), not random: republishing an unchanged
snapshot must address the same run, or every reconcile would fork a new lineage history.
"""

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.provider import (
    AssetError,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        AssetRef,
        GovernanceTag,
        MetadataSnapshot,
        RelationshipEdge,
        TableAsset,
    )

# Identifies Provisa as the emitter in every event and every custom facet, per the spec.
PRODUCER = "https://github.com/provisa-dev/provisa"
SPEC_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
FACET_URL = f"{SPEC_URL}#/definitions"

# The event's own type, named in its ``schemaURL``. A consumer deduces which of the spec's event
# shapes it received from this anchor — Marquez rejects a bare spec URL as a RunEvent with a null
# run — so each event must point at its own ``$defs`` entry, not at the document.
DATASET_EVENT_URL = f"{SPEC_URL}#/$defs/DatasetEvent"
RUN_EVENT_URL = f"{SPEC_URL}#/$defs/RunEvent"

# uuid5 seed. Fixed so a run id depends only on the job it belongs to.
_RUN_NAMESPACE = uuid.UUID("6f0d2b1a-9c74-4e58-8b3d-0a5e1f742c96")


def _namespace(org_id: str) -> str:
    """The OpenLineage namespace for one org's datasets.

    Org-scoped rather than global: two orgs may both govern a table called ``public.orders``
    in a source called ``wh``, and a shared namespace would merge them into one dataset.
    """
    return f"provisa://{org_id}"


def _dataset_name(table: TableAsset) -> str:
    return ".".join(table.ref.parts)


def _custom_facet(**payload: Any) -> dict[str, Any]:
    return {"_producer": PRODUCER, "_schemaURL": FACET_URL, **payload}


def _schema_facet(table: TableAsset) -> dict[str, Any]:
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"{FACET_URL}/SchemaDatasetFacet",
        "fields": [
            {"name": column.name, "type": column.data_type, "description": column.description}
            for column in table.columns
        ],
    }


def _ownership_facet(owner_id: str) -> dict[str, Any]:
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"{FACET_URL}/OwnershipDatasetFacet",
        "owners": [{"name": owner_id, "type": "STEWARD"}],
    }


def _governance_facet(tags: list[GovernanceTag]) -> dict[str, Any]:
    """Governance signals for one asset and its columns.

    ``rule_id`` identifies the rule; the rule body is not in the snapshot to begin with
    (REQ-1071), so there is nothing here to withhold.
    """
    return _custom_facet(
        signals=[
            {
                "asset": tag.asset.fqn(),
                "signal": tag.signal.value,
                "ruleId": tag.rule_id,
                "restrictedRoles": list(tag.restricted_roles),
                "exemptRoles": list(tag.exempt_roles),
            }
            for tag in tags
        ]
    )


def _relationships_facet(edges: list[RelationshipEdge]) -> dict[str, Any]:
    return _custom_facet(
        relationships=[
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
            }
            for edge in edges
        ]
    )


def _assertion_column(table: TableAsset, assertion: Any) -> str | None:
    """The column an assertion names, or None when it is about the whole dataset.

    A column assertion's ref is the table's parts plus the column; a table-level one is the
    table's parts exactly.
    """
    return (
        assertion.asset.parts[-1]
        if len(assertion.asset.parts) > len(table.ref.parts)
        else None
    )


def _spec_data_quality_facet(
    table: TableAsset, assertions: list[Any]
) -> dict[str, Any] | None:  # REQ-1443
    """The spec's ``dataQualityAssertions`` facet: the most recent scan's verdicts.

    Its entries require a ``success`` boolean, which is a run outcome — and Provisa has one,
    because a results table is that scan's rows and the export reads the latest back. A check
    whose last run neither passed nor failed (error, skipped) and one that has never run carry no
    boolean, so they are absent HERE and reported in the Provisa facet instead: inventing a
    ``success`` for them would state a verdict that was never reached. The facet is omitted
    entirely when no assertion on this dataset has one.
    """
    entries = [
        {
            "assertion": assertion.check_type,
            "success": assertion.outcome.success,
            "column": _assertion_column(table, assertion),
        }
        for assertion in assertions
        if assertion.outcome.success is not None
    ]
    if not entries:
        return None
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"{FACET_URL}/DataQualityAssertionsDatasetFacet",
        "assertions": entries,
    }


def _data_quality_facet(table: TableAsset, assertions: list[Any]) -> dict[str, Any]:  # REQ-1443
    """The checks a contract makes about one dataset and its columns, and how each last fared.

    Carries what the spec facet has no room for: the checker and its dialect's own severity, the
    authored definition, the dataset the outcomes land in, and the last run's status in full —
    including ``never_run`` for a contract registered but not yet scanned, and the error/skipped
    states that reached no verdict. A consumer that reads only ``dataQualityAssertions`` sees the
    passes and failures; one that reads this too can tell an unchecked column from a clean one.
    """
    return _custom_facet(
        assertions=[
            {
                "assertion": assertion.check_type,
                "column": _assertion_column(table, assertion),
                "checker": assertion.checker,
                "definition": assertion.definition,
                "severity": assertion.severity,
                "resultsTable": ".".join(assertion.results_table.parts),
                "outcome": assertion.outcome.as_document(),
            }
            for assertion in assertions
        ]
    )


def _dataset_facets(
    snapshot: MetadataSnapshot,
    table: TableAsset,
    domain_owner: dict[str, str],
) -> dict[str, Any]:
    facets: dict[str, Any] = {"schema": _schema_facet(table)}
    # REQ-1385: the stable business-identity address — the cross-catalog join key a lineage
    # consumer stitches runs with.
    facets["provisa_uri"] = _custom_facet(uri=table.semantic_uri)
    if table.description:
        facets["documentation"] = {
            "_producer": PRODUCER,
            "_schemaURL": f"{FACET_URL}/DocumentationDatasetFacet",
            "description": table.description,
        }
    # REQ-609: a table inherits its accountable party from its domain's steward. A domain with
    # no steward publishes no ownership facet — the DomainAsset already carries ``pending``,
    # and naming a placeholder owner here would report accountability that nobody holds.
    steward = domain_owner.get(table.domain_id) if table.domain_id else None
    if steward is not None:
        facets["ownership"] = _ownership_facet(steward)
    if table.domain_id:
        facets["provisa_domain"] = _custom_facet(domainId=table.domain_id)

    asset_prefix = table.ref.fqn()
    tags = [
        tag
        for tag in snapshot.governance_tags
        if tag.asset.fqn() == asset_prefix or tag.asset.fqn().startswith(f"{asset_prefix}.")
    ]
    if tags:
        facets["provisa_governance"] = _governance_facet(tags)

    edges = [edge for edge in snapshot.relationships if edge.source.fqn() == asset_prefix]
    if edges:
        facets["provisa_relationships"] = _relationships_facet(edges)

    # REQ-1443: the assertions publish on the dataset they OBSERVE, so a consumer asking what is
    # checked about this table asks the table, not the table the outcomes land in.
    assertions = [
        assertion
        for assertion in snapshot.assertions
        if assertion.asset.fqn() == asset_prefix
        or assertion.asset.fqn().startswith(f"{asset_prefix}.")
    ]
    if assertions:
        facets["provisa_data_quality"] = _data_quality_facet(table, assertions)
        spec_facet = _spec_data_quality_facet(table, assertions)
        if spec_facet is not None:
            facets["dataQualityAssertions"] = spec_facet
    return facets


def _column_lineage_facet(
    namespace: str, table: TableAsset, snapshot: MetadataSnapshot
) -> dict[str, Any] | None:
    """The spec's ``columnLineage`` facet, keyed by output column.

    ``transformations`` carries the operation Provisa resolved from the compiled SQL. The
    spec's ``type`` distinguishes a value-carrying derivation from an INDIRECT one (a filter
    or join key); every edge here comes from a projected expression, so all are DIRECT.
    """
    downstream_prefix = f"{table.ref.fqn()}."
    fields: dict[str, Any] = {}
    for edge in snapshot.lineage:
        if not edge.downstream.fqn().startswith(downstream_prefix):
            continue
        upstream_parts = edge.upstream.parts
        entry = fields.setdefault(edge.downstream.parts[-1], {"inputFields": []})
        entry["inputFields"].append(
            {
                "namespace": namespace,
                "name": ".".join(upstream_parts[:-1]),
                "field": upstream_parts[-1],
                "transformations": [
                    {"type": "DIRECT", "subtype": "TRANSFORMATION", "description": transform}
                    for transform in edge.transforms
                ],
            }
        )
    if not fields:
        return None
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"{FACET_URL}/ColumnLineageDatasetFacet",
        "fields": fields,
    }


@dataclass(frozen=True)
class ExportEvent:  # REQ-1069
    """One OpenLineage event and the asset it is about.

    The asset travels WITH the payload rather than being recovered by re-walking the snapshot:
    a per-asset failure has to name the asset the catalog actually rejected, and a second walk
    that drifted out of step would name the wrong one.
    """

    asset: AssetRef
    kind: str  # "dataset" | "lineage"
    payload: dict[str, Any]


def _run_id(job_name: str) -> str:
    return str(uuid.uuid5(_RUN_NAMESPACE, job_name))


def to_events(snapshot: MetadataSnapshot, *, event_time: datetime) -> list[ExportEvent]:
    """The full snapshot as OpenLineage events, in the order they must be sent.

    Dataset events come first so a lineage event never references a dataset the target has not
    seen. ``event_time`` is passed in rather than read from the clock so the mapping stays a
    pure function of the snapshot — the caller stamps it once for the whole publish.
    """
    namespace = _namespace(snapshot.org_id)
    stamp = event_time.astimezone(UTC).isoformat()
    domain_owner = {
        domain.id: domain.steward.id for domain in snapshot.domains if domain.steward is not None
    }

    facets_by_name = {
        _dataset_name(table): _dataset_facets(snapshot, table, domain_owner)
        for table in snapshot.tables
    }

    events: list[ExportEvent] = []
    for table in snapshot.tables:
        events.append(
            ExportEvent(
                asset=table.ref,
                kind="dataset",
                payload={
                    "eventTime": stamp,
                    "producer": PRODUCER,
                    "schemaURL": DATASET_EVENT_URL,
                    "dataset": {
                        "namespace": namespace,
                        "name": _dataset_name(table),
                        "facets": facets_by_name[_dataset_name(table)],
                    },
                },
            )
        )

    for table in snapshot.tables:
        lineage = _column_lineage_facet(namespace, table, snapshot)
        if lineage is None:
            continue
        inputs = sorted(
            {
                ".".join(edge.upstream.parts[:-1])
                for edge in snapshot.lineage
                if edge.downstream.fqn().startswith(f"{table.ref.fqn()}.")
            }
        )
        job_name = _dataset_name(table)
        events.append(
            ExportEvent(
                asset=table.ref,
                kind="lineage",
                payload={
                    "eventType": "COMPLETE",
                    "eventTime": stamp,
                    "producer": PRODUCER,
                    "schemaURL": RUN_EVENT_URL,
                    "run": {"runId": _run_id(job_name)},
                    "job": {"namespace": namespace, "name": job_name},
                    # Every dataset named by the run repeats its facets. A run event replaces the
                    # facets of the datasets it names, so sending the output with columnLineage
                    # alone would erase the schema the dataset event registered — and a consumer
                    # that resolves column lineage against known columns then has none to
                    # resolve against.
                    "inputs": [
                        {
                            "namespace": namespace,
                            "name": name,
                            # An upstream outside the governed set has no facets to repeat; it is
                            # still named, because dropping the input would lose the edge itself.
                            **({"facets": facets_by_name[name]} if name in facets_by_name else {}),
                        }
                        for name in inputs
                    ],
                    "outputs": [
                        {
                            "namespace": namespace,
                            "name": _dataset_name(table),
                            "facets": {
                                **facets_by_name[_dataset_name(table)],
                                "columnLineage": lineage,
                            },
                        }
                    ],
                },
            )
        )
    return events


@register_provider
class OpenLineageExport(MetadataExport):  # REQ-1069
    """Publish a snapshot to any OpenLineage-compatible endpoint (Marquez, and others)."""

    provider_name = "openlineage"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = self._config.api_key.get_secret_value()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _lineage_url(self) -> str:
        return f"{self._config.endpoint.rstrip('/')}/api/v1/lineage"

    async def publish(self, snapshot: MetadataSnapshot) -> PublishResult:
        events = to_events(snapshot, event_time=datetime.now(UTC))
        result = PublishResult(provider_name=self.provider_name)
        url = self._lineage_url()
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            for event in events:
                response = await client.post(url, json=event.payload, headers=self._headers())
                if response.status_code >= 400:
                    # Per-asset rather than aborting: a catalog that rejects one dataset must
                    # not cost the publish the other 199 (REQ-1068 PublishResult).
                    result.errors.append(
                        AssetError(
                            asset=event.asset,
                            message=f"HTTP {response.status_code}: {response.text[:500]}",
                        )
                    )
                    continue
                result.published[event.kind] = result.published.get(event.kind, 0) + 1
        return result

    async def health(self) -> None:
        url = f"{self._config.endpoint.rstrip('/')}/api/v1/namespaces"
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(url, headers=self._headers())
        response.raise_for_status()
