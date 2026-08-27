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

# Requirements: REQ-1068, REQ-1069, REQ-1070, REQ-1071, REQ-1387, REQ-1389

# REQ-1389 — vendor-id tracking and succession on physical re-address. OpenMetadata's
# upsert is keyed on ``fullyQualifiedName`` (service.database.schema.table), and PATCH
# cannot rename or re-parent a table: the documented PATCH-updatable fields are
# "description, displayName, owners, tags, retentionPeriod, columns, domain, and extension"
# — ``name`` and ``databaseSchema`` are not among them
# (https://docs.open-metadata.org/v1.12.x/api-reference/data-assets/tables/update), and the
# server's ``EntityRepository.restorePatchAttributes`` silently restores ``name`` unless the
# entity type opts into ``renameAllowed`` — which ``TableRepository`` does not
# (https://github.com/open-metadata/OpenMetadata/blob/main/openmetadata-service/src/main/
# java/org/openmetadata/service/jdbi3/EntityRepository.java). A PATCH-by-id rebind is
# therefore impossible; the fallback is SUCCESSION: the exporter captures each table's
# entity UUID at publish time, and when the stored binding shows the FQN changed it reads
# the OLD entity by that UUID, carries its human enrichment (steward tags, extension,
# owners and the carried PUT-body fields) onto the NEW entity's first publish, and marks
# the old entity's description as superseded — pointing at the new FQN and the Provisa URN.
# Publish still never deletes.

# REQ-1387 — business-glossary publishing. Terms publish ONLY into a Provisa-owned
# glossary (name ``provisa_<org>``, created idempotently via PUT /v1/glossaries); no other
# glossary is ever read, written, or deleted, and a term's absence from the snapshot never
# deletes it — removal inside the catalog is a steward's call. Unlike tables,
# ``GlossaryTermRepository`` sets ``renameAllowed = true`` and its updater moves name and
# parent together (``updateNameAndParent`` / ``updateParent``,
# https://github.com/open-metadata/OpenMetadata/blob/main/openmetadata-service/src/main/
# java/org/openmetadata/service/jdbi3/GlossaryTermRepository.java), so a re-addressed term
# is REBOUND by PATCHing the stored UUID instead of the table adapter's succession.
# ``CreateGlossaryTerm`` carries no status field, so the deprecated flag lands as a PATCH
# of ``/entityStatus`` after the upsert. Term-to-asset assignment is HUMAN-OWNED
# (REQ-1389) and is never written.

from __future__ import annotations

import json

from dataclasses import dataclass
from hashlib import sha256
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
        ColumnAsset,
        DataQualityAssertion,
        GlossaryTermAsset,
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

# REQ-1389: PUT-body fields of a table the exporter never authors. A PUT replaces the fields
# sent — and clears the ones it does not send — so anything live under these keys would be
# destroyed by a publish that stayed silent about them. They are carried through verbatim
# from the live entity; Provisa never writes them of its own accord.
_CARRIED_TABLE_FIELDS = (
    "owners",
    "tableType",
    "tableConstraints",
    "tablePartition",
    "retentionPeriod",
    "certification",
    "lifeCycle",
)

# REQ-1387: how the Provisa-owned glossary presents itself in the catalog. The NAME is the
# stable per-org key the upsert is addressed by; the display name is what a catalog user
# sees.
GLOSSARY_DISPLAY_NAME = "Provisa Glossary"

# REQ-1387: PUT-body fields of a glossary term the exporter never authors — reviewers,
# tags, owners, references, style and custom extensions are steward curation inside
# OpenMetadata, carried through verbatim so the fqn-keyed upsert cannot wipe them
# (same single-writer discipline as ``_CARRIED_TABLE_FIELDS``, REQ-1389).
_CARRIED_TERM_FIELDS = ("tags", "reviewers", "owners", "references", "style", "extension")

# REQ-1443: how each Provisa checker names itself in OpenMetadata's TestPlatform enum. The
# enum already carries both, so a published test case reports the engine that actually runs it
# instead of implying OpenMetadata executes it.
_TEST_PLATFORMS = {"soda": "Soda", "great_expectations": "GreatExpectations"}

_KIND_OF = "KIND_OF"
_SYNONYM_OF = "SYNONYM_OF"


def _glossary_name(org_id: str) -> str:
    return f"provisa_{org_id}"


@dataclass(frozen=True)
class Entity:  # REQ-1069
    """One upsert: which OpenMetadata collection, what body, and the asset it is about."""

    # The union AssetError.asset carries, for the same reason: an upsert may be about a
    # target-side object that is not a Provisa asset (a classification, a tag, a service).
    asset: AssetRef | AssetRefStub
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
        # REQ-1385: OpenMetadata renders sourceUrl as the asset's outbound link — the
        # dereference path back to the governed definition.
        "sourceUrl": table.semantic_uri,
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
                        "uri": edge.semantic_uri,  # REQ-1385
                        "target": edge.target.fqn() if edge.target is not None else None,
                        "sourceColumn": edge.source_column,
                        "targetColumn": edge.target_column,
                        "cardinality": edge.cardinality,
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
    return Entity(asset=table.ref, path="/api/v1/tables", kind="table", body=body)


# REQ-1443: Provisa's scan outcomes in OpenMetadata's own vocabulary. A warn maps to Failed, not
# Success — the checker raised it because a threshold was breached, and the severity is the
# author's volume knob, not a verdict. error/skipped/never_run are absent on purpose: they reached
# no verdict, and OpenMetadata's Aborted would claim a run that did not reach one here.
_TEST_CASE_STATUSES: dict[str, str] = {"pass": "Success", "fail": "Failed", "warn": "Failed"}


def _test_definition_name(assertion: DataQualityAssertion, entity_type: str) -> str:
    """The TestDefinition a check publishes under (REQ-1443).

    Keyed by checker, check type and entity type together: OpenMetadata's own definitions
    (``columnValuesToBeNotNull`` and the rest) describe tests OpenMetadata runs, and reusing one
    would say the catalog executes a check the checker actually executes elsewhere. A definition
    is COLUMN- or TABLE-scoped in OpenMetadata, so a check type used at both levels needs one of
    each rather than a single definition that lies about half its uses.
    """
    return f"provisa_{assertion.checker}_{assertion.check_type}_{entity_type.lower()}"


def _entity_type_of(assertion: DataQualityAssertion, table: TableAsset) -> str:
    """``COLUMN`` when the assertion's ref carries a column beyond the table's parts."""
    return "COLUMN" if len(assertion.asset.parts) > len(table.ref.parts) else "TABLE"


def _test_suite_name(table: TableAsset) -> str:
    return f"{_table_fqn(table)}.testSuite"


def _entity_link(assertion: DataQualityAssertion, table: TableAsset) -> str:
    table_link = f"<#E::table::{_table_fqn(table)}"
    if _entity_type_of(assertion, table) == "COLUMN":
        return f"{table_link}::columns::{assertion.asset.parts[-1]}>"
    return f"{table_link}>"


def _data_quality_entities(snapshot: MetadataSnapshot) -> list[Entity]:
    """Contract checks as OpenMetadata TestDefinitions, basic TestSuites and TestCases.

    Published on the OBSERVED table — the one the checker scans — because that is where a
    consumer asks whether a column is checked. The results table publishes as an ordinary
    table alongside; the link back to it rides each test case's description, since
    ``CreateTestCase`` carries no field for a second asset reference.

    Order is the contract, as it is for classifications: a test case names a definition and a
    suite by FQN, and OpenMetadata rejects a reference it cannot resolve. A basic suite (one
    bound to a single table) rather than a logical one, because a logical suite takes its
    members through a separate endpoint while a basic suite owns the tests on its table.

    ``testPlatforms`` names the checker itself — OpenMetadata's enum already has ``Soda`` and
    ``GreatExpectations`` — so the catalog reports which engine runs the test rather than
    implying OpenMetadata does.
    """
    tables = {table.ref.fqn(): table for table in snapshot.tables}
    definitions: dict[str, Entity] = {}
    suites: dict[str, Entity] = {}
    cases: list[Entity] = []
    results: list[Entity] = []
    for assertion in snapshot.assertions:
        # The observed table is a published table by construction: the builder emits an
        # assertion only when both ends survive the Data Product filter.
        table = tables[".".join(assertion.asset.parts[:3])]
        entity_type = _entity_type_of(assertion, table)
        definition_name = _test_definition_name(assertion, entity_type)
        platform = _TEST_PLATFORMS[assertion.checker]
        definitions.setdefault(
            definition_name,
            Entity(
                asset=AssetRefStub(definition_name),
                path="/api/v1/dataQuality/testDefinitions",
                kind="test_definition",
                body={
                    "name": definition_name,
                    "displayName": f"{platform} {assertion.check_type}",
                    "description": (
                        f"A {assertion.check_type} check {platform} runs against Provisa. "
                        "The check's own text is on each test case."
                    ),
                    "entityType": entity_type,
                    "testPlatforms": [platform],
                    "parameterDefinition": [],
                },
            ),
        )
        suite_name = _test_suite_name(table)
        suites.setdefault(
            suite_name,
            Entity(
                asset=table.ref,
                path="/api/v1/dataQuality/testSuites/basic",
                kind="test_suite",
                body={
                    "name": suite_name,
                    "description": f"Data-quality contract Provisa registered over {table.ref.fqn()}.",
                    "basicEntityReference": _table_fqn(table),
                },
            ),
        )
        cases.append(
            Entity(
                asset=assertion.asset,
                path="/api/v1/dataQuality/testCases",
                kind="test_case",
                body={
                    "name": _test_case_name(assertion, table),
                    "entityLink": _entity_link(assertion, table),
                    "testDefinition": definition_name,
                    "testSuite": suite_name,
                    "description": (
                        f"{assertion.definition}\n\n"
                        f"Severity: {assertion.severity}. "
                        f"Outcomes land in {assertion.results_table.fqn()}."
                    ),
                    "parameterValues": [],
                },
            )
        )
        # REQ-1443: the last scan's verdict, on OpenMetadata's own test-result endpoint, so the
        # catalog's Data Quality tab shows the current state rather than only the test's
        # existence. A check that reached no verdict — never run, errored, skipped — posts no
        # result: OpenMetadata's own statuses are outcomes, and Aborted would claim a run that
        # either never happened or is still the checker's business to report.
        result_body = _test_case_result(assertion)
        if result_body is not None:
            case_fqn = _test_case_fqn(assertion, table)
            results.append(
                Entity(
                    asset=assertion.asset,
                    path=f"/api/v1/dataQuality/testCases/{case_fqn}/testCaseResult",
                    kind="test_case_result",
                    body=result_body,
                )
            )
    # Results last: a result addresses its test case by FQN, and OpenMetadata rejects one it
    # cannot resolve — the same ordering contract the cases have with their definitions.
    return [*definitions.values(), *suites.values(), *cases, *results]


def _test_case_fqn(assertion: DataQualityAssertion, table: TableAsset) -> str:
    """How OpenMetadata addresses a test case: its entity's FQN, then the case name."""
    name = _test_case_name(assertion, table)
    if _entity_type_of(assertion, table) == "COLUMN":
        return f"{_column_fqn(table, assertion.asset.parts[-1])}.{name}"
    return f"{_table_fqn(table)}.{name}"


def _test_case_result(assertion: DataQualityAssertion) -> dict[str, Any] | None:
    """One check's last verdict as a ``CreateTestCaseResult``, or None when it reached none.

    ``timestamp`` is epoch milliseconds, which is how OpenMetadata timestamps every result, and
    it is the scan's own time rather than the publish time — a result stamped now would claim the
    check was verified by this export.
    """
    outcome = assertion.outcome
    status = _TEST_CASE_STATUSES.get(outcome.status)
    if status is None:
        return None
    assert outcome.scan_time is not None  # a verdict implies the scan that reached it
    result_text = f"{assertion.checker} reported {outcome.status} (severity {assertion.severity})"
    if outcome.failed_rows is not None:
        result_text += f"; {outcome.failed_rows} failing rows"
    return {
        "timestamp": int(outcome.scan_time.timestamp() * 1000),
        "testCaseStatus": status,
        "result": result_text,
        "testResultValue": (
            [{"name": assertion.check_type, "value": str(outcome.metric_value)}]
            if outcome.metric_value is not None
            else []
        ),
    }


def _test_case_name(assertion: DataQualityAssertion, table: TableAsset) -> str:
    """A per-suite unique name, derived from what the check is about rather than its position.

    An index would renumber every case below a check the steward removed from the contract, so
    the next publish would address a different test case with each name.
    """
    scope = (
        assertion.asset.parts[-1]
        if _entity_type_of(assertion, table) == "COLUMN"
        else table.ref.parts[-1]
    )
    digest = sha256(
        "|".join([assertion.checker, assertion.check_type, scope, assertion.definition]).encode()
    ).hexdigest()[:12]
    return f"provisa_{assertion.check_type}_{scope}_{digest}"


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
                            "sourcePythonClass": "provisa.metadata_export",
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
    # REQ-1443: after the tables, because a test case's entityLink addresses the table it
    # observes and OpenMetadata resolves that link at creation time.
    entities.extend(_data_quality_entities(snapshot))
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


@dataclass(frozen=True)
class TermPlan:  # REQ-1387
    """One glossary-term upsert: its target FQN, phase-1 body, and deferred relatedTerms."""

    term: GlossaryTermAsset
    fqn: str
    body: dict[str, Any]
    related: tuple[str, ...]


def to_term_plans(snapshot: MetadataSnapshot) -> tuple[list[TermPlan], list[AssetError]]:
    """The published term graph as OpenMetadata glossary-term upserts, parents first.

    The four edge types map onto OpenMetadata's own constructs (REQ-1387):

    * ``KIND_OF`` becomes the glossary's parent-child hierarchy — the native "is a kind
      of" construct. The tree holds one parent, so a term asserting several keeps the
      lowest-id target as its tree position and the rest publish as ``relatedTerms``,
      dropping no asserted edge.
    * ``SYNONYM_OF`` becomes ``synonyms`` — OpenMetadata models a synonym as an
      alternative NAME, not a term link, so the target term's name is what publishes.
    * ``RELATED_TO``, ``PART_OF``, and the six newer types (``VALID_VALUE_OF``,
      ``DERIVED_FROM``, ``REPLACES``, ``PREFERRED_TERM_FOR``, ``TRANSLATION_OF``,
      ``ANTONYM_OF``) all become ``relatedTerms`` — OpenMetadata's glossary model has no
      construct for enumerations, lineage, deprecation, preference, translation, or
      antonymy at the term-relation level, so the untyped term link is deliberately the
      closest native fit for all of them, the same choice already made for RELATED_TO and
      PART_OF. They are deferred to a second pass because both endpoints must exist first.

    A ``KIND_OF`` cycle cannot be a tree: the term closing the loop publishes at the
    glossary root and the cycle is reported rather than silently rewired.
    """
    by_id = {term.term_id: term for term in snapshot.glossary_terms}
    gname = _glossary_name(snapshot.org_id)
    kind_of: dict[int, list[int]] = {}
    related_ids: dict[int, list[int]] = {}
    synonym_ids: dict[int, list[int]] = {}
    for edge in snapshot.glossary_edges:
        if edge.rel_type == _KIND_OF:
            kind_of.setdefault(edge.from_term_id, []).append(edge.to_term_id)
        elif edge.rel_type == _SYNONYM_OF:
            synonym_ids.setdefault(edge.from_term_id, []).append(edge.to_term_id)
        else:  # RELATED_TO | PART_OF | VALID_VALUE_OF | DERIVED_FROM | REPLACES |
            # PREFERRED_TERM_FOR | TRANSLATION_OF | ANTONYM_OF — all fall to relatedTerms
            related_ids.setdefault(edge.from_term_id, []).append(edge.to_term_id)
    parent: dict[int, int] = {}
    for from_id, targets in kind_of.items():
        ordered = sorted(targets)
        parent[from_id] = ordered[0]
        if ordered[1:]:
            related_ids.setdefault(from_id, []).extend(ordered[1:])
    errors: list[AssetError] = []
    for term_id in sorted(parent):
        seen: set[int] = set()
        cursor = term_id
        while cursor in parent:
            if cursor in seen:
                errors.append(
                    AssetError(
                        asset=AssetRefStub(by_id[cursor].name),
                        message=(
                            f"glossary term {by_id[cursor].name!r}: KIND_OF cycle — "
                            "published at the glossary root instead of nested"
                        ),
                    )
                )
                del parent[cursor]
                break
            seen.add(cursor)
            cursor = parent[cursor]

    def _fqn(term_id: int) -> str:
        chain = [by_id[term_id].name]
        cursor = term_id
        while cursor in parent:
            cursor = parent[cursor]
            chain.append(by_id[cursor].name)
        return ".".join([gname, *reversed(chain)])

    def _depth(term_id: int) -> int:
        depth = 0
        cursor = term_id
        while cursor in parent:
            cursor = parent[cursor]
            depth += 1
        return depth

    plans: list[TermPlan] = []
    for term in sorted(snapshot.glossary_terms, key=lambda t: (_depth(t.term_id), t.term_id)):
        body: dict[str, Any] = {
            "glossary": gname,
            "name": term.name,
            # OpenMetadata requires ``description`` on CreateGlossaryTerm (a schema-required
            # field), so a term with no definition publishes an empty one rather than being
            # withheld — REQ-1387, the vendor schema mandates the value.
            "description": term.definition or "",
        }
        if term.term_id in parent:
            body["parent"] = _fqn(parent[term.term_id])
        synonyms = sorted(by_id[target].name for target in synonym_ids.get(term.term_id, []))
        if synonyms:
            body["synonyms"] = synonyms
        plans.append(
            TermPlan(
                term=term,
                fqn=_fqn(term.term_id),
                body=body,
                related=tuple(sorted(_fqn(target) for target in related_ids.get(term.term_id, []))),
            )
        )
    return plans, errors


@register_provider
class OpenMetadataExport(MetadataExport):  # REQ-1069
    """Publish a snapshot to an OpenMetadata server's ingestion API."""

    provider_name = "openmetadata"

    # REQ-1389: read-merge the live table before every table PUT. A PUT replaces the fields
    # sent, and stewards tag and enrich the SAME entity inside OpenMetadata — without the
    # merge, every publish clobbers their work. Same gate shape as Atlas's
    # ``classification_merge``: a class attribute, so a test (or an Atlas-shaped subclass)
    # can switch the pass off without touching config.
    tag_merge = True

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

    @staticmethod
    def _merged_tags(
        live_tags: list[dict[str, Any]] | None, our_tags: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        """Provisa's tags to match the snapshot, everyone else's preserved (REQ-1389).

        Ownership is the classification namespace: a label under ``Provisa.`` was authored
        here and is added/removed to track the snapshot; any other label was applied by a
        steward inside OpenMetadata and is carried through untouched.
        """
        steward = [
            tag
            for tag in live_tags or []
            if not str(tag.get("tagFQN", "")).startswith(f"{CLASSIFICATION}.")
        ]
        return [*(our_tags or []), *steward]

    async def _merge_live_table(
        self, client: httpx.AsyncClient, entity: Entity, result: PublishResult
    ) -> None:
        """Overlay the live table's human-owned state onto the outgoing PUT body (REQ-1389).

        Preserved: steward tag labels at table and column level, extension keys other than
        Provisa's relationship property, and the PUT-body fields the exporter never authors
        (``_CARRIED_TABLE_FIELDS``). Overwritten: everything Provisa authored — description,
        columns, ``Provisa.*`` tags, the relationship extension. A failed read must not
        abort the publish: the PUT proceeds with Provisa's own body and the merge failure is
        reported against the asset.
        """
        fqn = f"{entity.body['databaseSchema']}.{entity.body['name']}"
        try:
            response = await client.get(
                self._url(f"/api/v1/tables/name/{fqn}"),
                params={"fields": "tags,columns,extension,owners"},
                headers=self._headers(),
            )
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=entity.asset,
                    message=(
                        f"table {entity.asset.fqn()}: live read for the tag merge failed "
                        f"({exc}); published Provisa-authored fields only, which may drop "
                        "steward tags on this table"
                    ),
                )
            )
            return
        if response.status_code == 404:
            # First publish of this table: nothing live, nothing human to preserve.
            return
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=entity.asset,
                    message=(
                        f"table {entity.asset.fqn()}: live read for the tag merge failed "
                        f"(HTTP {response.status_code}); published Provisa-authored fields "
                        "only, which may drop steward tags on this table"
                    ),
                )
            )
            return
        self._overlay_live_table(entity, response.json())

    def _overlay_live_table(self, entity: Entity, live: dict[str, Any]) -> None:
        """Carry one live table's human-owned state onto the outgoing PUT body (REQ-1389)."""
        entity.body["tags"] = self._merged_tags(live.get("tags"), entity.body.get("tags"))
        live_columns = {c.get("name"): c for c in live.get("columns") or []}
        for column in entity.body["columns"]:
            live_column = live_columns.get(column["name"])
            if live_column is None:
                continue
            column["tags"] = self._merged_tags(live_column.get("tags"), column.get("tags"))
        # Extension keys Provisa does not own are somebody's custom properties; only the
        # relationship property tracks the snapshot (including its removal).
        live_extension = {
            k: v for k, v in (live.get("extension") or {}).items() if k != RELATIONSHIP_PROPERTY
        }
        if live_extension:
            entity.body["extension"] = {**live_extension, **entity.body.get("extension", {})}
        for field in _CARRIED_TABLE_FIELDS:
            if field not in entity.body and live.get(field) is not None:
                entity.body[field] = live[field]

    async def _succeed_rebound_table(
        self,
        client: httpx.AsyncClient,
        entity: Entity,
        stored: tuple[str, str],
        new_fqn: str,
        result: PublishResult,
    ) -> None:
        """Succession for a physically re-addressed table (REQ-1389).

        The stored binding says this Provisa asset was last published under a different FQN.
        PATCH-by-id cannot rename or re-parent a table (see the module docstring for the
        cited OpenMetadata docs and server source), so the old entity — read by its stored
        UUID, immune to the name change — donates its human enrichment to the new entity's
        first publish, and its own description is marked superseded with a pointer to the
        new FQN and the Provisa URN. Any failure here is reported and the publish proceeds
        with Provisa's own body: a lost carry-forward must never block the asset itself.
        """
        old_id, old_fqn = stored
        uri = entity.body["sourceUrl"]
        try:
            response = await client.get(
                self._url(f"/api/v1/tables/{old_id}"),
                params={"fields": "tags,columns,extension,owners"},
                headers=self._headers(),
            )
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=entity.asset,
                    message=(
                        f"table {entity.asset.fqn()}: rebind read of predecessor "
                        f"{old_fqn!r} (id {old_id}) failed ({exc}); published without its "
                        "steward enrichment"
                    ),
                )
            )
            return
        if response.status_code == 404:
            # The predecessor is gone from the catalog: nothing to carry, nothing to mark.
            return
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=entity.asset,
                    message=(
                        f"table {entity.asset.fqn()}: rebind read of predecessor "
                        f"{old_fqn!r} (id {old_id}) failed (HTTP {response.status_code}); "
                        "published without its steward enrichment"
                    ),
                )
            )
            return
        live = response.json()
        self._overlay_live_table(entity, live)
        note = (
            f"[Superseded by Provisa] This table was physically re-addressed and is now "
            f"published as {new_fqn}. Provisa URI: {uri}."
        )
        patch = [
            {
                "op": "replace" if live.get("description") is not None else "add",
                "path": "/description",
                "value": note,
            }
        ]
        try:
            deprecate = await client.patch(
                self._url(f"/api/v1/tables/{old_id}"),
                content=json.dumps(patch),
                headers={**self._headers(), "Content-Type": "application/json-patch+json"},
            )
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=entity.asset,
                    message=(
                        f"table {entity.asset.fqn()}: superseded note on predecessor "
                        f"{old_fqn!r} failed ({exc})"
                    ),
                )
            )
            return
        if deprecate.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=entity.asset,
                    message=(
                        f"table {entity.asset.fqn()}: superseded note on predecessor "
                        f"{old_fqn!r} failed (HTTP {deprecate.status_code})"
                    ),
                )
            )

    async def _read_live_term(
        self, client: httpx.AsyncClient, fqn: str, result: PublishResult
    ) -> dict[str, Any] | None:
        """The live term, for the enrichment merge — None when it does not exist yet.

        Same single-writer discipline as the table merge (REQ-1389): a PUT replaces the
        fields sent, and tags/reviewers on a Provisa term are steward work inside
        OpenMetadata. A failed read is reported and the PUT proceeds with Provisa's own
        body rather than blocking the term.
        """
        try:
            response = await client.get(
                self._url(f"/api/v1/glossaryTerms/name/{fqn}"),
                params={"fields": "tags,reviewers,owners,references,style,relatedTerms,extension"},
                headers=self._headers(),
            )
        except httpx.HTTPError as exc:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(fqn),
                    message=(
                        f"glossary_term {fqn}: live read for the enrichment merge failed "
                        f"({exc}); published Provisa-authored fields only, which may drop "
                        "steward curation on this term"
                    ),
                )
            )
            return None
        if response.status_code == 404:
            # First publish of this term: nothing live, nothing human to preserve.
            return None
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(fqn),
                    message=(
                        f"glossary_term {fqn}: live read for the enrichment merge failed "
                        f"(HTTP {response.status_code}); published Provisa-authored fields "
                        "only, which may drop steward curation on this term"
                    ),
                )
            )
            return None
        return response.json()

    async def _rebind_term(
        self,
        client: httpx.AsyncClient,
        plan: TermPlan,
        stored: tuple[str, str],
        gname: str,
        term_ids: dict[str, str],
        result: PublishResult,
    ) -> None:
        """PATCH-rebind a re-addressed term to its new name/parent (REQ-1387, REQ-1389).

        The stored binding says this Provisa term was last published under a different
        FQN. Unlike tables, glossary terms allow rename and re-parenting via PATCH
        (``renameAllowed = true`` — see the module note), so the stored UUID is patched to
        the new position and the fqn-keyed PUT that follows lands on the SAME entity,
        enrichment intact — no succession. A failed rebind is reported and the PUT
        proceeds, creating a fresh term; the predecessor is left untouched, because
        publish never deletes.
        """
        old_id, old_fqn = stored
        ops: list[dict[str, Any]] = []
        if old_fqn.rsplit(".", 1)[-1] != plan.term.name:
            ops.append({"op": "replace", "path": "/name", "value": plan.term.name})
        old_parent = old_fqn.rsplit(".", 1)[0]
        new_parent = plan.fqn.rsplit(".", 1)[0]
        if old_parent != new_parent:
            if new_parent == gname:
                ops.append({"op": "remove", "path": "/parent"})
            else:
                parent_id = term_ids.get(new_parent)
                if parent_id is None:
                    result.errors.append(
                        AssetError(
                            asset=AssetRefStub(plan.fqn),
                            message=(
                                f"glossary_term {plan.fqn}: cannot re-parent — parent "
                                f"{new_parent!r} was not upserted"
                            ),
                        )
                    )
                    return
                ops.append(
                    {
                        "op": "add" if old_parent == gname else "replace",
                        "path": "/parent",
                        "value": {"id": parent_id, "type": "glossaryTerm"},
                    }
                )
        if not ops:
            return
        response = await client.patch(
            self._url(f"/api/v1/glossaryTerms/{old_id}"),
            content=json.dumps(ops),
            headers={**self._headers(), "Content-Type": "application/json-patch+json"},
        )
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(plan.fqn),
                    message=(
                        f"glossary_term {plan.fqn}: rebind of predecessor {old_fqn!r} "
                        f"(id {old_id}) failed (HTTP {response.status_code}); the upsert "
                        "will create a fresh term without its steward curation"
                    ),
                )
            )

    async def _sync_term_status(
        self,
        client: httpx.AsyncClient,
        plan: TermPlan,
        term_id: str,
        live_status: str | None,
        result: PublishResult,
    ) -> None:
        """Project the deprecated flag onto ``entityStatus`` (REQ-1387).

        ``CreateGlossaryTerm`` carries no status field, so the flag lands as a PATCH after
        the upsert. Provisa authors only the Deprecated transition: a deprecated term is
        marked ``Deprecated``, and a term no longer deprecated that the catalog still
        shows as ``Deprecated`` moves to ``Approved``. Any other status is the stewards'
        review workflow and is never touched.
        """
        if plan.term.deprecated:
            if live_status == "Deprecated":
                return
            value = "Deprecated"
        else:
            if live_status != "Deprecated":
                return
            value = "Approved"
        ops = [
            {
                "op": "replace" if live_status is not None else "add",
                "path": "/entityStatus",
                "value": value,
            }
        ]
        response = await client.patch(
            self._url(f"/api/v1/glossaryTerms/{term_id}"),
            content=json.dumps(ops),
            headers={**self._headers(), "Content-Type": "application/json-patch+json"},
        )
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(plan.fqn),
                    message=(
                        f"glossary_term {plan.fqn}: entityStatus {value!r} failed "
                        f"(HTTP {response.status_code})"
                    ),
                )
            )

    async def _publish_glossary(
        self, client: httpx.AsyncClient, snapshot: MetadataSnapshot, result: PublishResult
    ) -> None:
        """Publish the term graph into the Provisa-owned glossary (REQ-1387).

        Everything here is scoped to the ``provisa_<org>`` glossary: no other glossary is
        read, written, or deleted, and a term's absence from the snapshot never deletes it.
        Term-to-asset assignment (tagging tables/columns with terms) is HUMAN-OWNED
        (REQ-1389) and is never written — ``GlossaryTermAsset.refs`` stays out of the
        payload by design.
        """
        if not snapshot.glossary_terms:
            return
        gname = _glossary_name(snapshot.org_id)
        response = await client.put(
            self._url("/api/v1/glossaries"),
            json={
                "name": gname,
                "displayName": GLOSSARY_DISPLAY_NAME,
                "description": (
                    "Business vocabulary Provisa publishes from its governed semantic layer."
                ),
            },
            headers=self._headers(),
        )
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(gname),
                    message=(
                        f"glossary {gname}: HTTP {response.status_code}: "
                        f"{response.text[:500]}. No terms were published."
                    ),
                )
            )
            return
        result.published["glossary"] = result.published.get("glossary", 0) + 1
        plans, plan_errors = to_term_plans(snapshot)
        result.errors.extend(plan_errors)
        # fqn -> server-assigned UUID and live entity, harvested in the first pass and
        # consumed by rebinds and the relatedTerms pass.
        term_ids: dict[str, str] = {}
        live_by_fqn: dict[str, dict[str, Any]] = {}
        for plan in plans:
            stored = self._bindings.get(plan.term.semantic_uri)
            if stored is not None and stored[1] != plan.fqn:
                await self._rebind_term(client, plan, stored, gname, term_ids, result)
            live = await self._read_live_term(client, plan.fqn, result)
            if live is not None:
                live_by_fqn[plan.fqn] = live
                for field in _CARRIED_TERM_FIELDS:
                    if live.get(field) is not None:
                        plan.body[field] = live[field]
            # relatedTerms a steward linked OUTSIDE the Provisa glossary are theirs and
            # ride through; inside it, the snapshot's edges are the source of truth and
            # land in the second pass.
            foreign_related = [
                ref["fullyQualifiedName"]
                for ref in (live or {}).get("relatedTerms") or []
                if not str(ref["fullyQualifiedName"]).startswith(f"{gname}.")
            ]
            if foreign_related:
                plan.body["relatedTerms"] = foreign_related
            response = await client.put(
                self._url("/api/v1/glossaryTerms"), json=plan.body, headers=self._headers()
            )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(plan.fqn),
                        message=(
                            f"glossary_term {plan.fqn}: HTTP {response.status_code}: "
                            f"{response.text[:500]}"
                        ),
                    )
                )
                continue
            body = response.json()
            term_ids[plan.fqn] = body["id"]
            # REQ-1389: capture the vendor's own id for this term, keyed by the canonical
            # Provisa URN, so the next publish can rebind by identity.
            result.bindings[plan.term.semantic_uri] = (body["id"], body["fullyQualifiedName"])
            result.published["glossary_term"] = result.published.get("glossary_term", 0) + 1
            await self._sync_term_status(
                client, plan, body["id"], (live or {}).get("entityStatus"), result
            )
        # Second pass: relatedTerms, once every endpoint exists — OpenMetadata rejects a
        # reference it cannot resolve, and the edges can be cyclic.
        for plan in plans:
            if not plan.related or plan.fqn not in term_ids:
                continue
            targets = [fqn for fqn in plan.related if fqn in term_ids]
            for missing in (fqn for fqn in plan.related if fqn not in term_ids):
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(plan.fqn),
                        message=(
                            f"glossary_term {plan.fqn}: related term {missing!r} was not "
                            "upserted, so the edge cannot be addressed"
                        ),
                    )
                )
            if not targets:
                continue
            body = {
                **plan.body,
                "relatedTerms": [*plan.body.get("relatedTerms", []), *targets],
            }
            response = await client.put(
                self._url("/api/v1/glossaryTerms"), json=body, headers=self._headers()
            )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(plan.fqn),
                        message=(
                            f"glossary_term {plan.fqn}: relatedTerms HTTP "
                            f"{response.status_code}: {response.text[:500]}"
                        ),
                    )
                )

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
                if entity.kind == "table":
                    fqn = f"{entity.body['databaseSchema']}.{entity.body['name']}"
                    stored = self._bindings.get(entity.body["sourceUrl"])
                    if stored is not None and stored[1] != fqn:
                        # REQ-1389: the stored binding says the physical FQN changed —
                        # succession by stored UUID, since PATCH cannot rename a table.
                        await self._succeed_rebound_table(client, entity, stored, fqn, result)
                    if self.tag_merge:
                        # REQ-1389: a PUT replaces the fields sent, so the live entity's
                        # human-owned state is read and carried through first.
                        await self._merge_live_table(client, entity, result)
                response = await client.put(
                    self._url(entity.path), json=entity.body, headers=self._headers()
                )
                if entity.kind == "table" and response.status_code < 400:
                    body = response.json()
                    table_ids[body["fullyQualifiedName"]] = body["id"]
                    # REQ-1389: capture the vendor's own id for this asset, keyed by the
                    # canonical Provisa URN, so the next publish can rebind by identity.
                    result.bindings[entity.body["sourceUrl"]] = (
                        body["id"],
                        body["fullyQualifiedName"],
                    )
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
            # REQ-1387: business-glossary terms, scoped to the Provisa-owned glossary.
            await self._publish_glossary(client, snapshot, result)
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(
                self._url("/api/v1/system/version"), headers=self._headers()
            )
        response.raise_for_status()
