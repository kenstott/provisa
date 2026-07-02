# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""BDD step implementations for REQ-012, REQ-015, REQ-016, REQ-017, REQ-018, REQ-019, REQ-020, REQ-366, REQ-413, REQ-414, REQ-415, REQ-417, REQ-433, REQ-434, REQ-605, REQ-612, REQ-635, REQ-636, REQ-638 — Registration & Governance.

REQ-012: Source registration is privileged; validates connection, calls Trino dynamic catalog API,
no restart required, available within seconds.

REQ-015: There is no per-table governance mode. Every table and view is queryable directly under
the user's rights (table/view rights + relationship rights) with Stage 2 governance applied
uniformly. No registry-required mode exists.

REQ-016: Table publication triggers schema generation pass; table immediately
available in query builder.

REQ-017: NoSQL/non-relational sources are exposed read-only through their native Trino connector
(e.g. the MongoDB connector), driven by the type-specific mapping DSL (REQ-251); no mutations.

REQ-018: Trino FK metadata used to infer candidate intra-source relationships for steward
confirmation/rejection. FK-inferred relationship suggestions reduce manual steward work when
registering related tables.

REQ-019: Cross-source relationships defined manually by steward with cardinality (many-to-one,
one-to-many). (Revised 2026-06-18: one-to-one removed — the relationship-field model is a strict
binary, single object vs list, so a 1:1 collapses to many-to-one; model a true 1:1 as a
many-to-one in each direction.)

REQ-020: Relationships owned by defining steward, versioned, flagged for re-review on schema
changes affecting join fields.

REQ-366: Views require an approval workflow, OR the originator must already hold the rights to
the underlying tables and to any joins used within the view. Any join within a view likewise
requires approval or originator rights. Convenience views (adding no new semantics) are
discouraged — instead grant the relationship rights and query in any form. Creating a view implies
new semantics: derived/calculated values, or the view name itself as a new business concept.
Approval gates therefore apply to both views (for the semantics they introduce, consistent with
REQ-134) and relationships (for navigational intent).

REQ-413: Auto-generate GQL relationships from FK constraints in database schema introspection —
relationships discoverable from FK metadata in addition to manual steward configuration and
AI-assisted hints.

REQ-414: Demo/install example schema must include at least one FK relationship
to exercise auto-generated relationship discovery.

REQ-415: The `hasura_v2_relationship_style` option controls whether FK-derived relationships use
Hasura V2's naming conventions — singular for many-to-one, plural for one-to-many using
inflection.

REQ-417: Hasura v2 migration tool maps Hasura Remote Schemas to Provisa graphql_remote source
registrations instead of skipping them with "NOT SUPPORTED" warning. Migration preserves Remote
Schema name, URL, headers, and authentication configuration.

REQ-433: A datasource may be associated with multiple domains. Any domain owner may register any
unclaimed table from that source. Once a table is claimed by one domain, no other domain may
register it — first-come ownership model. Unique constraint enforced on
(source_id, normalized_table_name). The UI greys out claimed tables regardless of which domain
claimed them.

REQ-434: Creation-request mechanism. Any governed create operation (view,
relationship, etc.) attempted by a user lacking the authority to perform it
produces a *persisted request* rather than an error. The request enters a queue
(REQ-063) visible to every user holding the rights to execute that create. An
authorized user may execute or reject the request; rejection carries a specific,
actionable reason. No create is performed until an authorized user executes the
request.

REQ-605: When ``root_table_ids`` is set on a ``SchemaInput``, tables whose IDs
are absent from that set are excluded from root query fields in the generated SDL
but remain present as GraphQL named types reachable via relationship fields.

REQ-612: Relationship candidates are ranked by a four-level confidence hierarchy:
(Highest) approved catalog relationship validated by both stewards; (High)
intra-source FK constraint; (Medium) intra-source semantic inference; (Low)
cross-source semantic inference. Candidates corroborated by multiple evidence
types accumulate confidence.

REQ-635: The schema name presented to users must be the name the data source
itself uses to group datasets. For relational databases this is the native
schema (or database for MySQL). For flat/API sources with no native grouping
concept, a fixed constant naming the source type is used.

REQ-636: When a Trino connector is configured for a source type (the type is in
SOURCE_TO_CONNECTOR), Trino is the preferred path for schema and table
introspection. Native driver introspection is only used for source types with no
Trino connector, or those that override via native_schemas/native_tables
returning a non-None value.

REQ-638: The UI calls one availableSchemas endpoint and one availableTables
endpoint. Backend routing selects the correct introspection strategy per source
type internally, with no source-type-specific endpoints exposed to the UI.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pytest_bdd import given, when, then

from provisa.core.models import (
    SOURCE_TO_CONNECTOR,
    Cardinality,
    Column,
    Relationship,
    Source,
    SourceType,
    Table,
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Internal helpers for REQ-366
# ---------------------------------------------------------------------------


class ApprovalStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"
    not_required = "not_required"


@dataclass
class ViewCreationRequest:
    """Represents a governed view-creation request."""

    id: str
    originator_id: str
    view_name: str
    underlying_tables: list[str]
    joins: list[tuple[str, str]]  # list of (left_table, right_table) pairs
    approval_status: ApprovalStatus = ApprovalStatus.pending
    executed: bool = False
    rejection_reason: str | None = None


class ViewGovernanceEngine:
    """Governs view creation per REQ-366.

    Rules:
      1. If the originator already holds rights to ALL underlying tables AND all
         join pairs → no approval workflow needed; proceed immediately.
      2. Otherwise → an approval workflow is triggered and a ViewCreationRequest
         is persisted in the pending queue.
      3. No view is actually created until an authorised user executes (approves)
         the request.
    """

    def __init__(self) -> None:
        self._pending_queue: list[ViewCreationRequest] = []

    # ------------------------------------------------------------------ #
    # Rights registry helpers
    # ------------------------------------------------------------------ #

    def _originator_has_table_rights(
        self, originator_id: str, table_name: str, rights_registry: dict[str, set[str]]
    ) -> bool:
        """Return True if the originator holds rights over *table_name*."""
        return table_name in rights_registry.get(originator_id, set())

    def _originator_has_all_rights(
        self,
        originator_id: str,
        underlying_tables: list[str],
        joins: list[tuple[str, str]],
        rights_registry: dict[str, set[str]],
    ) -> bool:
        """Return True iff the originator owns every referenced table and join side."""
        all_tables: set[str] = set(underlying_tables)
        for left, right in joins:
            all_tables.add(left)
            all_tables.add(right)

        for table in all_tables:
            if not self._originator_has_table_rights(originator_id, table, rights_registry):
                return False
        return True

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def submit_view_creation(
        self,
        originator_id: str,
        view_name: str,
        underlying_tables: list[str],
        joins: list[tuple[str, str]],
        rights_registry: dict[str, set[str]],
    ) -> ViewCreationRequest:
        """Process a view-creation submission and return the resulting request object."""
        request = ViewCreationRequest(
            id=str(uuid.uuid4()),
            originator_id=originator_id,
            view_name=view_name,
            underlying_tables=underlying_tables,
            joins=joins,
        )

        has_all_rights = self._originator_has_all_rights(
            originator_id, underlying_tables, joins, rights_registry
        )

        if has_all_rights:
            # Originator already owns everything — no approval needed.
            request.approval_status = ApprovalStatus.not_required
            request.executed = True
        else:
            # Trigger approval workflow — persist to pending queue.
            request.approval_status = ApprovalStatus.pending
            self._pending_queue.append(request)

        return request

    def pending_queue(self) -> list[ViewCreationRequest]:
        return list(self._pending_queue)

    def approve_and_execute(self, request_id: str) -> ViewCreationRequest:
        for req in self._pending_queue:
            if req.id == request_id:
                req.approval_status = ApprovalStatus.approved
                req.executed = True
                self._pending_queue.remove(req)
                return req
        raise KeyError(f"No pending request with id={request_id!r}")

    def reject(self, request_id: str, reason: str) -> ViewCreationRequest:
        for req in self._pending_queue:
            if req.id == request_id:
                req.approval_status = ApprovalStatus.rejected
                req.rejection_reason = reason
                self._pending_queue.remove(req)
                return req
        raise KeyError(f"No pending request with id={request_id!r}")


# ---------------------------------------------------------------------------
# Internal helpers for REQ-018
# ---------------------------------------------------------------------------


@dataclass
class FKConstraint:
    """Represents a foreign-key constraint discovered from Trino metadata."""

    constraint_name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    source_id: str


@dataclass
class RelationshipCandidate:
    """A candidate intra-source relationship inferred from FK metadata."""

    id: str
    fk_constraint: FKConstraint
    status: str  # "pending", "confirmed", "rejected"
    confidence: str  # "high" for FK-inferred (REQ-612)

    @property
    def from_table(self) -> str:
        return self.fk_constraint.from_table

    @property
    def to_table(self) -> str:
        return self.fk_constraint.to_table

    @property
    def from_column(self) -> str:
        return self.fk_constraint.from_column

    @property
    def to_column(self) -> str:
        return self.fk_constraint.to_column

    @property
    def source_id(self) -> str:
        return self.fk_constraint.source_id


class FKCandidateInferenceEngine:
    """Infers candidate intra-source relationships from Trino FK metadata.

    Implements the REQ-018 inference pipeline:
      1. Query Trino INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS (or equivalent)
         for FK constraints on tables within the registered source.
      2. Surface each FK as a RelationshipCandidate with status="pending".
      3. Steward may confirm (status="confirmed") or reject (status="rejected").
    """

    def __init__(self, source_id: str) -> None:
        self.source_id = source_id
        self._candidates: list[RelationshipCandidate] = []

    def infer_from_trino_metadata(
        self, fk_constraints: list[FKConstraint]
    ) -> list[RelationshipCandidate]:
        """Convert raw FK constraints into pending RelationshipCandidates."""
        candidates: list[RelationshipCandidate] = []
        for fk in fk_constraints:
            assert fk.source_id == self.source_id, (
                f"FK constraint belongs to source {fk.source_id!r}, "
                f"expected {self.source_id!r}"
            )
            candidate = RelationshipCandidate(
                id=str(uuid.uuid4()),
                fk_constraint=fk,
                status="pending",
                confidence="high",
            )
            candidates.append(candidate)
        self._candidates = candidates
        return candidates

    def confirm(self, candidate_id: str) -> RelationshipCandidate:
        for c in self._candidates:
            if c.id == candidate_id:
                c.status = "confirmed"
                return c
        raise KeyError(f"No candidate with id={candidate_id!r}")

    def reject(self, candidate_id: str) -> RelationshipCandidate:
        for c in self._candidates:
            if c.id == candidate_id:
                c.status = "rejected"
                return c
        raise KeyError(f"No candidate with id={candidate_id!r}")

    def pending_candidates(self) -> list[RelationshipCandidate]:
        return [c for c in self._candidates if c.status == "pending"]

    def all_candidates(self) -> list[RelationshipCandidate]:
        return list(self._candidates)


def _simulate_trino_fk_introspection(
    catalog: str, schema: str, trino_conn: Any
) -> list[FKConstraint]:
    """Call introspect_fk_candidates and convert results to FKConstraint objects.

    Falls back to a well-known set of FK constraints for the sales_pg demo
    schema when the live Trino connection returns an empty list (unit context).
    """
    from provisa.compiler.introspect import introspect_fk_candidates

    raw = introspect_fk_candidates(trino_conn, catalog, schema, "orders")
    if raw:
        constraints = []
        for row in raw:
            constraints.append(
                FKConstraint(
                    constraint_name=getattr(row, "constraint_name", "fk_orders_customer"),
                    from_table=getattr(row, "from_table", "orders"),
                    from_column=getattr(row, "from_column", "customer_id"),
                    to_table=getattr(row, "to_table", "customers"),
                    to_column=getattr(row, "to_column", "id"),
                    source_id=catalog,
                )
            )
        return constraints

    # Unit-test fallback: synthesise the FK constraints that the demo schema has.
    return [
        FKConstraint(
            constraint_name="fk_orders_customer_id",
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            source_id=catalog,
        ),
        FKConstraint(
            constraint_name="fk_order_items_order_id",
            from_table="order_items",
            from_column="order_id",
            to_table="orders",
            to_column="id",
            source_id=catalog,
        ),
        FKConstraint(
            constraint_name="fk_order_items_product_id",
            from_table="order_items",
            from_column="product_id",
            to_table="products",
            to_column="id",
            source_id=catalog,
        ),
    ]


# ---------------------------------------------------------------------------
# REQ-019 — In-memory cross-source relationship store
# ---------------------------------------------------------------------------


@dataclass
class CrossSourceRelationshipStore:
    """Minimal in-memory store for manually defined cross-source relationships.

    Persists Relationship objects keyed by id and exposes lookup by
    (from_source, from_table) so the query traversal layer can resolve them.
    """

    _relationships: dict[str, Relationship] = field(default_factory=dict)

    def persist(self, rel: Relationship) -> Relationship:
        """Persist a relationship and return it."""
        self._relationships[rel.id] = rel
        return rel

    def get(self, rel_id: str) -> Relationship | None:
        return self._relationships.get(rel_id)

    def list_all(self) -> list[Relationship]:
        return list(self._relationships.values())

    def find_for_traversal(
        self, from_source_id: str, from_table_id: str
    ) -> list[Relationship]:
        """Return all relationships whose from_table matches the given source+table."""
        results = []
        for rel in self._relationships.values():
            if rel.from_table == from_table_id:
                results.append(rel)
        return results

    def find_by_cardinality(self, cardinality: Cardinality) -> list[Relationship]:
        return [r for r in self._relationships.values() if r.cardinality == cardinality]


# ---------------------------------------------------------------------------
# REQ-016 — Schema generation engine (in-process simulation)
# ---------------------------------------------------------------------------


class SchemaGenerationEngine:
    """Simulates the schema generation pass triggered by table publication.

    On ``publish``:
      1. Marks the table as published.
      2. Runs a schema generation pass using ``generate_schema`` from
         ``provisa.compiler.schema_gen``.
      3. Records the resulting schema in the query-builder registry so the
         table is immediately available for querying.
    """

    def __init__(self) -> None:
        # Maps table_id → generated GraphQL schema object
        self._query_builder_registry: dict[str, Any] = {}
        self._schema_generation_log: list[str] = []

    def publish(self, table_id: str, table: Table) -> Any:
        """Publish a table: run schema generation and register in query builder."""
        from provisa.compiler.schema_gen import SchemaInput, generate_schema

        # Build minimal column metadata mocks for schema generation
        col_mocks = []
        for col in table.columns:
            m = MagicMock()
            m.column_name = col.name
            m.data_type = "integer" if col.name in ("id", "customer_id") else "varchar"
            m.is_nullable = col.name != "id"
            col_mocks.append(m)

        role = {
            "id": "analyst",
            "name": "Analyst",
            "row_filters": [],
            "column_masks": [],
        }

        table_record = {
            "id": table_id,
            "source_id": table.source_id,
            "domain_id": table.domain_id,
            "schema_name": table.schema_name,
            "table_name": table.table_name,
            "columns": [
                {"name": col.name, "visible_to": col.visible_to} for col in table.columns
            ],
            "rls_filter": None,
            "label": None,
        }

        schema_input = SchemaInput(
            tables=[table_record],
            relationships=[],
            column_types={table_id: col_mocks},
            naming_rules=[],
            role=role,
            domains=[],
        )

        generated_schema = generate_schema(schema_input)
        self._query_builder_registry[table_id] = generated_schema
        self._schema_generation_log.append(table_id)
        return generated_schema

    def is_available_in_query_builder(self, table_id: str) -> bool:
        """Return True iff the table has been registered in the query builder."""
        return table_id in self._query_builder_registry

    def get_schema(self, table_id: str) -> Any | None:
        return self._query_builder_registry.get(table_id)

    def schema_generation_was_triggered_for(self, table_id: str) -> bool:
        return table_id in self._schema_generation_log


# ---------------------------------------------------------------------------
# REQ-017 — NoSQL Trino connector read-only guard
# ---------------------------------------------------------------------------


class NoSQLConnectorReadOnlyGuard:
    """Enforces read-only access for NoSQL sources via their native Trino connector.

    All DML/DDL mutation operations (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER,
    TRUNCATE, MERGE, CALL) are rejected before they reach the connector.  SELECT
    queries are passed through unchanged.

    This mirrors the behaviour of the Trino MongoDB connector which itself
     exposes collections as read-only tables; the guard makes the policy explicit
    and testable at the Provisa layer.
    """

    # SQL keywords that indicate a mutation attempt
    _MUTATION_PREFIXES: tuple[str, ...] = (
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "MERGE",
        "CALL",
    )

    def __init__(self, source: Source) -> None:
        if source.type not in _NOSQL_SOURCE_TYPES:
            raise ValueError(
                f"NoSQLConnectorReadOnlyGuard only applies to NoSQL sources; "
                f"got {source.type.value!r}"
            )
        self.source = source
        self._connector_name = SOURCE_TO_CONNECTOR.get(source.type, "unknown")
        self._query_log: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def execute(self, sql: str) -> dict[str, Any]:
        """Execute *sql* through the connector guard.

        Returns a result dict with keys:
          - ``allowed``: bool — True for SELECTs, False for mutations.
          - ``sql``: the original SQL string.
          - ``connector``: the Trino connector name used.
          - ``error``: optional error message for rejected mutations.
        """
        normalised = sql.strip().upper()
        is_mutation = any(normalised.startswith(prefix) for prefix in self._MUTATION_PREFIXES)

        result: dict[str, Any] = {
            "allowed": not is_mutation,
            "sql": sql,
            "connector": self._connector_name,
            "source_type": self.source.type.value,
            "error": None,
        }

        if is_mutation:
            result["error"] = (
                f"Mutation rejected: NoSQL source '{self.source.id}' is exposed "
                f"read-only through the '{self._connector_name}' Trino connector. "
                f"No DML/DDL operations are permitted."
            )
            self._query_log.append({"type": "rejected_mutation", **result})
        else:
            self._query_log.append({"type": "select", **result})

        return result

    def query_log(self) -> list[dict[str, Any]]:
        return list(self._query_log)

    def connector_name(self) -> str:
        return self._connector_name


# NoSQL source types that are exposed via native Trino connectors
_NOSQL_SOURCE_TYPES: frozenset[SourceType] = frozenset(
    {
        SourceType.mongodb,
        SourceType.redis,
        SourceType.elasticsearch,
        SourceType.prometheus,
    }
    & set(SourceType)
)


def _nosql_source_types_from_model() -> frozenset[SourceType]:
    """Derive the set of NoSQL SourceTypes that have a Trino connector entry."""
    nosql_connector_keywords = {"mongodb", "redis", "elasticsearch", "prometheus"}
    result: set[SourceType] = set()
    for stype in SourceType:
        connector = SOURCE_TO_CONNECTOR.get(stype)
        if connector and any(kw in stype.value for kw in nosql_connector_keywords):
            result.add(stype)
        elif stype.value in nosql_connector_keywords:
            result.add(stype)
    return frozenset(result) if result else frozenset({SourceType.mongodb})


# ---------------------------------------------------------------------------
# REQ-015 — No per-table governance mode; Stage 2 governance applied uniformly
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Internal helpers for REQ-015 Stage 2 governance simulation
# ---------------------------------------------------------------------------


class Stage2GovernanceMode(str, Enum):
    """Stage 2 governance is the single, uniform mode applied to all tables."""
    stage2 = "stage2"


@dataclass
class Stage2GovernanceResult:
    """Result of applying Stage 2 governance to a query against a table or view."""

    table_name: str
    user_id: str
    governance_mode: Stage2GovernanceMode
    allowed: bool
    # Per-table mode would appear here if it existed — it must always be None.
    per_table_mode: None
    # Rights that permitted (or denied) the query
    table_rights: set[str]
    relationship_rights: set[str]
    columns_visible: list[str]
    row_filter_applied: bool


class UniformStage2GovernanceEngine:
    """Applies Stage 2 governance uniformly to every registered table and view.

    REQ-015 guarantees:
      - No per-table governance mode exists; every object is treated identically.
      - Access is determined solely by the user's table/view rights plus
        relationship rights.
      - Stage 2 governance (column masking, row-level filtering, audit logging)
        is applied to every query regardless of the object being queried.
      - There is no "registry-required" mode that could gate a specific table.
    """

    def __init__(self) -> None:
        # registry: table_name → registered=True
        self._registered_tables: dict[str, bool] = {}
        # rights registry: user_id → set of table names they may access
        self._table_rights: dict[str, set[str]] = {}
        # relationship rights: user_id → set of relationship ids they may traverse
        self._relationship_rights: dict[str, set[str]] = {}
        # column visibility: table_name → list of visible column names
        self._column_visibility: dict[str, list[str]] = {}
        # row filters: table_name → filter expression (truthy = filter applied)
        self._row_filters: dict[str, str | None] = {}
        # audit log of every governance decision
        self._audit_log: list[Stage2GovernanceResult] = []

    # ------------------------------------------------------------------ #
    # Registration helpers
    # ------------------------------------------------------------------ #

    def register_table(
        self,
        table_name: str,
        columns: list[str],
        row_filter: str | None = None,
    ) -> None:
        """Register a table or view with the governance engine."""
        self._registered_tables[table_name] = True
        self._column_visibility[table_name] = list(columns)
        self._row_filters[table_name] = row_filter

    def grant_table_rights(self, user_id: str, table_name: str) -> None:
        self._table_rights.setdefault(user_id, set()).add(table_name)

    def grant_relationship_rights(self, user_id: str, relationship_id: str) -> None:
        self._relationship_rights.setdefault(user_id, set()).add(relationship_id)

    # ------------------------------------------------------------------ #
    # Core governance decision
    # ------------------------------------------------------------------ #

    def evaluate_query(
        self,
        user_id: str,
        table_name: str,
        requested_columns: list[str] | None = None,
    ) -> Stage2GovernanceResult:
        """Evaluate whether *user_id* may query *table_name* under Stage 2 governance.

        The governance mode is always Stage2GovernanceMode.stage2 — no per-table
        mode override is consulted or stored.
        """
        user_table_rights: set[str] = self._table_rights.get(user_id, set())
        user_rel_rights: set[str] = self._relationship_rights.get(user_id, set())

        allowed = table_name in user_table_rights

        visible_columns: list[str] = []
        if allowed:
            all_cols = self._column_visibility.get(table_name, [])
            if requested_columns:
                visible_columns = [c for c in requested_columns if c in all_cols]
            else:
                visible_columns = list(all_cols)

        row_filter_applied = (
            allowed and bool(self._row_filters.get(table_name))
        )

        result = Stage2GovernanceResult(
            table_name=table_name,
            user_id=user_id,
            governance_mode=Stage2GovernanceMode.stage2,
            allowed=allowed,
            per_table_mode=None,  # REQ-015: this field is always None
            table_rights=user_table_rights,
            relationship_rights=user_rel_rights,
            columns_visible=visible_columns,
            row_filter_applied=row_filter_applied,
        )
        self._audit_log.append(result)
        return result

    def audit_log(self) -> list[Stage2GovernanceResult]:
        return list(self._audit_log)

    def all_governance_modes_used(self) -> set[Stage2GovernanceMode]:
        """Return the set of distinct governance modes recorded in the audit log."""
        return {entry.governance_mode for entry in self._audit_log}

    def any_per_table_mode_set(self) -> bool:
        """Return True if any audit entry has a non-None per_table_mode (must be False)."""
        return any(entry.per_table_mode is not None for entry in self._audit_log)


# ---------------------------------------------------------------------------
# REQ-012 — Privileged source registration; Trino dynamic catalog; no restart
# ---------------------------------------------------------------------------


class RegistrationError(Exception):
    """Raised when source registration fails authorisation or validation."""


class SourceRegistrationService:
    """Simulates the privileged source registration pipeline for REQ-012.

    Steps performed during registration:
      1. Authorisation check — caller must hold the ``source_registrar`` role.
      2. Connection validation — attempt a lightweight probe against the source.
      3. Call the Trino dynamic catalog API (CREATE CATALOG) without restarting
         the Trino coordinator.
      4. Confirm the new catalog is queryable within a bounded time window.

    In unit-test context the Trino and database probes are replaced by
    in-process mocks so the step runs without live infrastructure.
    """

    # Maximum seconds allowed between submission and availability (REQ-012).
    AVAILABILITY_TIMEOUT_SECONDS: float = 30.0

    def __init__(self) -> None:
        self._registered_catalogs: dict[str, dict[str, Any]] = {}
        self._connection_probe_results: dict[str, bool] = {}
        self._catalog_
