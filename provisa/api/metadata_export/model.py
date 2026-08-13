# Copyright (c) 2026 Kenneth Stott
# Canary: efd43be2-01dc-4903-9259-48246805d571
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Vendor-neutral metadata model published to external catalogs (REQ-1070).

Every adapter maps FROM this model. None of them reads ``ProvisaConfig`` directly — that
indirection is what lets OpenLineage, OpenMetadata, Atlas, DataHub, Atlan and Collibra
share one definition of what Provisa knows (REQ-1069).
"""

# Requirements: REQ-1069, REQ-1070, REQ-1071

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class AssetKind(str, Enum):  # REQ-1070
    """The asset granularities Provisa publishes."""

    SOURCE = "source"
    TABLE = "table"
    COLUMN = "column"
    DOMAIN = "domain"


@dataclass(frozen=True)
class AssetRef:  # REQ-1070
    """A catalog-independent address for one published asset.

    ``parts`` is the hierarchy from the outermost container inward — ``(source, table)`` for
    a table, ``(source, table, column)`` for a column. Adapters render it into whatever
    their catalog calls a fully-qualified name.
    """

    kind: AssetKind
    parts: tuple[str, ...]

    def fqn(self, separator: str = ".") -> str:
        return separator.join(self.parts)


@dataclass
class OwnerRef:  # REQ-609, REQ-1070
    """Accountable party for an asset — a domain steward or a relationship's owner."""

    id: str
    kind: str  # "steward" | "relationship_owner"


@dataclass
class DomainAsset:  # REQ-609, REQ-1070
    """A governed domain.

    ``steward`` is None only for a domain that has not been assigned one. REQ-609 forbids
    such a domain from serving governed data, so it publishes as ``pending`` rather than
    being dropped — the catalog should show the gap, not hide it.
    """

    id: str
    description: str
    steward: OwnerRef | None
    pending: bool
    # REQ-1385: stable business-identity address (provisa:// scheme); the physical ref
    # exports alongside as the binding, so re-platforming preserves catalog identity.
    semantic_uri: str = ""


@dataclass
class ColumnAsset:  # REQ-1070
    ref: AssetRef
    name: str
    data_type: str
    description: str
    aliases: tuple[str, ...] = ()
    semantic_uri: str = ""  # REQ-1385: <table uri>#field:<business name>


@dataclass
class TableAsset:  # REQ-1070
    ref: AssetRef
    name: str
    source_id: str
    domain_id: str | None
    description: str
    aliases: tuple[str, ...] = ()
    columns: list[ColumnAsset] = field(default_factory=list)
    semantic_uri: str = ""  # REQ-1385: provisa://<org>/<domain path>/tables/<business name>


@dataclass
class SourceAsset:  # REQ-1070
    ref: AssetRef
    id: str
    source_type: str
    description: str
    semantic_uri: str = ""  # REQ-1385: provisa://<org>/sources/<source id>


@dataclass
class RelationshipEdge:  # REQ-019, REQ-020, REQ-1070
    """An approved relationship, carrying its defining steward and version (REQ-020)."""

    id: str
    source: AssetRef
    target: AssetRef | None  # None for a computed (function-target) relationship
    source_column: str
    target_column: str
    cardinality: str
    alias: str | None
    owner: OwnerRef | None
    version: int
    needs_review: bool
    # REQ-1385: a relationship is a navigational field of its source concept —
    # <source table uri>#rel:<alias> (registry id when the edge has no alias).
    semantic_uri: str = ""


@dataclass
class LineageEdge:  # REQ-939, REQ-942, REQ-1070
    """One column-level derivation.

    Derived from compiled queries and the MV DAG, not from a scanner — which is why
    ``transforms`` can name the operations applied rather than asserting a bare dependency.
    """

    upstream: AssetRef
    downstream: AssetRef
    transforms: tuple[str, ...] = ()


class GovernanceSignal(str, Enum):  # REQ-1071
    """The enforcement facts projected outward as catalog tags."""

    MASKED = "masked"
    RLS_RESTRICTED = "rls_restricted"
    VISIBILITY_RESTRICTED = "visibility_restricted"


@dataclass
class GovernanceTag:  # REQ-039, REQ-040, REQ-1071
    """An enforcement fact about one asset.

    Carries THAT the asset is governed and which rule governs it — never the rule body. A
    mask pattern or an RLS predicate is policy, and publishing it to an external catalog
    would put the bypass instructions next to the restricted asset.
    """

    asset: AssetRef
    signal: GovernanceSignal
    rule_id: str
    # Roles the restriction applies to, and roles exempt from it (``unmasked_to`` for masking).
    # Naming who is restricted and who is not is an access fact the catalog needs; the rule
    # that computes the value is not. Both are recorded because neither implies the other —
    # an RLS rule names the role it filters, a mask names the roles it spares.
    restricted_roles: tuple[str, ...] = ()
    exempt_roles: tuple[str, ...] = ()


@dataclass
class ModelTag:  # REQ-1375, REQ-1377, REQ-1378
    """A steward-assigned registry tag on one asset or relationship edge.

    Exactly one of ``asset`` / ``relationship_id`` is set: assets (source/table/column)
    become vendor classifications, while relationship edges cannot carry classifications
    on Atlas and ride the governance document instead (REQ-1378 asymmetry).
    """

    tag_id: str
    is_system: bool
    asset: AssetRef | None = None
    relationship_id: str | None = None
    reason: str | None = None  # required for 'deprecated' at the mutation layer
    expires_on: str | None = None  # ISO date; planned removal for 'deprecated'


@dataclass
class DataQualityOutcome:  # REQ-1443
    """What the most recent scan observed about one check.

    The results table is scan history (``scan_time`` is the watermark, so scans append), which
    makes the rows of the maximum ``scan_time`` the last execution — a fact the export reads back
    and publishes, so a catalog shows whether the check currently passes rather than only that it
    exists.

    ``status`` is the checker's own word (pass | fail | warn | error | skipped) plus ``never_run``
    for a contract registered but not yet scanned. That state is published, not omitted: "this
    column has a null check that has never executed" is precisely what a consumer needs to know,
    and dropping it would render an unrun check identical to a passing one. ``scan_id`` and
    ``scan_time`` are empty exactly when ``status`` is ``never_run``, since there is no scan to
    name.
    """

    status: str
    scan_id: str = ""
    scan_time: datetime | None = None
    metric_value: float | None = None
    failed_rows: int | None = None

    NEVER_RUN = "never_run"

    @property
    def ran(self) -> bool:
        return self.status != DataQualityOutcome.NEVER_RUN

    @property
    def success(self) -> bool | None:
        """The boolean an OpenLineage assertion entry requires, or None when there is none.

        ``warn`` is not a success: the checker raised it because a threshold WAS breached, and
        only its severity is milder. Reporting it as passing would hide the breach behind the
        author's choice of how loudly to complain about it. ``error`` and ``skipped`` reached no
        verdict at all, and ``never_run`` never started, so all three are None rather than a
        failure that did not happen.
        """
        if self.status == "pass":
            return True
        if self.status in ("fail", "warn"):
            return False
        return None

    def as_document(self) -> dict[str, object]:
        """The outcome as the JSON object the Provisa-authored carriers embed.

        One shape for every catalog that has no native run-result surface (the Atlas governance
        document, the Collibra attribute, the OpenLineage Provisa facet), so a consumer reading
        Provisa's own carrier reads the same fields wherever it finds them.
        """
        return {
            "status": self.status,
            "scanId": self.scan_id,
            "scanTime": self.scan_time.isoformat() if self.scan_time is not None else None,
            "metricValue": self.metric_value,
            "failedRows": self.failed_rows,
        }


@dataclass
class DataQualityAssertion:  # REQ-1443
    """One check a registered contract makes about a governed asset.

    Published on the OBSERVED asset — the table or column the checker scans — not on the table
    the outcomes land in. Every target catalog models it that way (OpenMetadata TestCase,
    DataHub Assertion, Collibra Data Quality Rule, Atlan DQ, the OpenLineage
    ``dataQualityAssertions`` facet): a consumer asks "is this column checked for nulls?" of the
    column, and hanging the answer off the results table would put it where nobody looks.

    ``results_table`` names where the outcomes DO land, so the two halves stay navigable in both
    directions; that table publishes as an ordinary table alongside, carrying the derived
    ``data_quality`` tag. ``definition`` is the check's own authored text in the checker's
    dialect — the same string :func:`provisa.dq.contract.contract_checks` reads back — because a
    normalized summary would describe a check the checker does not run.
    """

    asset: AssetRef
    checker: str  # soda | great_expectations
    check_type: str
    definition: str
    # fail | warn. Soda carries this as threshold.level; a GX expectation has no warn level at
    # all, so every GX assertion publishes as 'fail' — the dialect's own semantics, not a default.
    severity: str
    results_table: AssetRef
    # The last scan's verdict for this check, read back out of ``results_table``. Always present:
    # a check the scan never covered carries a ``never_run`` outcome rather than None, so every
    # adapter publishes a state instead of leaving the consumer to guess at an absence.
    outcome: DataQualityOutcome = field(
        default_factory=lambda: DataQualityOutcome(status=DataQualityOutcome.NEVER_RUN)
    )


@dataclass
class GlossaryTermAsset:  # REQ-1387
    """One business-glossary term: the normalized vocabulary entry over the semantic layer.

    ``refs`` lists the published columns that mean this concept; a rooted term whose refs
    all point at withheld tables is withheld with them (the data_product filter rule), while
    an abstract term publishes on the strength of its edges alone.
    """

    term_id: int
    name: str
    definition: str | None
    is_abstract: bool
    deprecated: bool
    refs: tuple[AssetRef, ...] = ()
    experts: tuple[str, ...] = ()
    semantic_uri: str = ""  # REQ-1385: provisa://<org>/terms/<term name>


@dataclass
class GlossaryTermEdge:  # REQ-1387
    """A typed relationship between two published terms (closed enum set)."""

    from_term_id: int
    to_term_id: int
    rel_type: str  # KIND_OF | RELATED_TO | PART_OF | SYNONYM_OF | VALID_VALUE_OF |
    # DERIVED_FROM | REPLACES | PREFERRED_TERM_FOR | TRANSLATION_OF | ANTONYM_OF


@dataclass
class MetadataSnapshot:  # REQ-1070
    """Everything Provisa publishes about one org at one moment.

    The same structure serves both publish paths (REQ-1072): a full snapshot for the scheduled
    reconcile, and a snapshot narrowed to the changed assets for the event path.
    """

    org_id: str
    sources: list[SourceAsset] = field(default_factory=list)
    domains: list[DomainAsset] = field(default_factory=list)
    tables: list[TableAsset] = field(default_factory=list)
    relationships: list[RelationshipEdge] = field(default_factory=list)
    lineage: list[LineageEdge] = field(default_factory=list)
    governance_tags: list[GovernanceTag] = field(default_factory=list)
    model_tags: list[ModelTag] = field(default_factory=list)  # REQ-1377/1378
    glossary_terms: list[GlossaryTermAsset] = field(default_factory=list)  # REQ-1387
    glossary_edges: list[GlossaryTermEdge] = field(default_factory=list)  # REQ-1387
    assertions: list[DataQualityAssertion] = field(default_factory=list)  # REQ-1443

    def columns(self) -> list[ColumnAsset]:
        return [column for table in self.tables for column in table.columns]

    def asset_count(self) -> dict[str, int]:
        return {
            AssetKind.SOURCE.value: len(self.sources),
            AssetKind.DOMAIN.value: len(self.domains),
            AssetKind.TABLE.value: len(self.tables),
            AssetKind.COLUMN.value: len(self.columns()),
        }
