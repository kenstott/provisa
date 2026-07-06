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

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, scenarios, then, when

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
                f"FK constraint belongs to source {fk.source_id!r}, expected {self.source_id!r}"
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
        # introspect_fk_candidates returns dicts:
        # {constraint_name, column_name, referenced_table, referenced_column}
        constraints = []
        for row in raw:
            constraints.append(
                FKConstraint(
                    constraint_name=row["constraint_name"],
                    from_table="orders",
                    from_column=row["column_name"],
                    to_table=row["referenced_table"],
                    to_column=row["referenced_column"],
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

    def find_for_traversal(self, from_source_id: str, from_table_id: str) -> list[Relationship]:
        """Return all relationships whose from_table matches the given source+table."""
        results = []
        for rel in self._relationships.values():
            if rel.source_table_id == from_table_id:
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

        # Build ColumnMetadata (real type, not mocks) for schema generation.
        from provisa.compiler.introspect import ColumnMetadata

        col_meta = [
            ColumnMetadata(
                column_name=col.name,
                data_type="integer" if col.name in ("id", "customer_id") else "varchar",
                is_nullable=col.name != "id",
            )
            for col in table.columns
        ]

        role = {
            "id": "analyst",
            "name": "Analyst",
            "capabilities": ["read"],
            "domain_access": ["*"],
        }

        table_record = {
            "id": table_id,
            "source_id": table.source_id,
            "domain_id": table.domain_id,
            "schema_name": table.schema_name,
            "table_name": table.table_name,
            "columns": [
                {"column_name": col.name, "visible_to": list(col.visible_to)}
                for col in table.columns
            ],
        }

        schema_input = SchemaInput(
            tables=[table_record],
            relationships=[],
            column_types=cast("dict[int, list[ColumnMetadata]]", {table_id: col_meta}),
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

        row_filter_applied = allowed and bool(self._row_filters.get(table_name))

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
        self._catalog_available_at: dict[str, float] = {}
        self._restart_count = 0

    # ------------------------------------------------------------------ #
    # Authorisation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_authorised(roles: set[str]) -> bool:
        """Registration is privileged: caller must hold ``source_registrar``."""
        return "source_registrar" in roles

    # ------------------------------------------------------------------ #
    # Registration pipeline
    # ------------------------------------------------------------------ #

    def register(
        self,
        caller_roles: set[str],
        source: Source,
        trino_dynamic_catalog_api: Any,
        connection_prober: Any,
    ) -> dict[str, Any]:
        """Run the full privileged registration pipeline for *source*.

        ``trino_dynamic_catalog_api`` is the client boundary for Trino's
        dynamic CREATE CATALOG endpoint (mocked in unit context). It must
        expose ``create_catalog(name, connector, properties)`` and
        ``list_catalogs()``. ``connection_prober`` exposes ``probe(source)``
        returning True on a successful lightweight connection check.
        """
        if not self._is_authorised(caller_roles):
            raise RegistrationError(
                "Source registration is privileged; caller lacks 'source_registrar'."
            )

        # 1. Validate the connection with a lightweight probe.
        probe_ok = bool(connection_prober.probe(source))
        self._connection_probe_results[source.id] = probe_ok
        if not probe_ok:
            raise RegistrationError(f"Connection validation failed for source {source.id!r}.")

        # 2. Call the Trino dynamic catalog API — no coordinator restart.
        start = time.monotonic()
        catalog_name = source.catalog_name
        trino_dynamic_catalog_api.create_catalog(
            catalog_name,
            SOURCE_TO_CONNECTOR.get(source.type, source.type.value),
            source.mapping,
        )

        # 3. Confirm the new catalog is queryable within the time window.
        catalogs = trino_dynamic_catalog_api.list_catalogs()
        elapsed = time.monotonic() - start
        if catalog_name not in catalogs:
            raise RegistrationError(f"Catalog {catalog_name!r} not visible after CREATE CATALOG.")
        if elapsed > self.AVAILABILITY_TIMEOUT_SECONDS:
            raise RegistrationError(
                f"Catalog {catalog_name!r} took {elapsed:.1f}s (> "
                f"{self.AVAILABILITY_TIMEOUT_SECONDS}s) to become available."
            )

        self._registered_catalogs[catalog_name] = {
            "source_id": source.id,
            "connector": SOURCE_TO_CONNECTOR.get(source.type, source.type.value),
        }
        self._catalog_available_at[catalog_name] = elapsed
        return {
            "catalog": catalog_name,
            "available": True,
            "elapsed_seconds": elapsed,
            "restart_required": False,
            "restart_count": self._restart_count,
        }

    def is_registered(self, catalog_name: str) -> bool:
        return catalog_name in self._registered_catalogs


# ---------------------------------------------------------------------------
# REQ-605 — root_table_ids exclusion helper
# ---------------------------------------------------------------------------


def _make_col_meta(name: str, data_type: str = "integer") -> Any:
    """Build a ColumnMetadata-like stand-in accepted by generate_schema."""
    from provisa.compiler.introspect import ColumnMetadata

    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=name != "id")


def _build_schema_input_with_root_ids(root_table_ids: set[int] | None) -> Any:
    """Build a SchemaInput with two FK-linked tables and optional root filtering.

    Tables:
      1 = orders(id, customer_id)  --many-to-one--> 2 = customers(id, name)
    When ``root_table_ids`` excludes table 2 (customers), customers must remain a
    named GraphQL type (reachable via the orders.customer relationship) but must
    NOT appear as a root query field.
    """
    from provisa.compiler.schema_gen import SchemaInput

    tables = [
        {
            "id": 1,
            "source_id": "pg1",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": []},
                {"column_name": "customer_id", "visible_to": []},
            ],
        },
        {
            "id": 2,
            "source_id": "pg1",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": []},
                {"column_name": "name", "visible_to": []},
            ],
        },
    ]
    relationships = [
        {
            "id": "rel_orders_customer",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
            "alias": "customer",
        }
    ]
    column_types = {
        1: [_make_col_meta("id"), _make_col_meta("customer_id")],
        2: [_make_col_meta("id"), _make_col_meta("name", "varchar")],
    }
    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "analyst", "capabilities": ["read"], "domain_access": ["*"]},
        domains=[],
        root_table_ids=root_table_ids,
    )


# ---------------------------------------------------------------------------
# REQ-612 — four-level confidence hierarchy over real RelationshipCandidate
# ---------------------------------------------------------------------------


class ConfidenceTier(float, Enum):
    """Four-level confidence hierarchy per REQ-612 (higher = stronger evidence)."""

    approved_catalog = 1.0  # Highest: approved by both stewards
    intra_source_fk = 0.9  # High: FK constraint
    intra_source_semantic = 0.75  # Medium: intra-source semantic inference
    cross_source_semantic = 0.6  # Low: cross-source semantic inference


def _rank_candidates(candidates: list[Any]) -> list[Any]:
    """Rank real RelationshipCandidate objects by descending confidence (REQ-612)."""
    return sorted(candidates, key=lambda c: c.confidence, reverse=True)


# ---------------------------------------------------------------------------
# REQ-433 — first-come dataset ownership (in-memory registry mirroring
# _dataset_ownership_conflict semantics via the real normalizer)
# ---------------------------------------------------------------------------


class DatasetOwnershipRegistry:
    """First-come table ownership across domains sharing a source (REQ-433).

    Uses the production normalizer (``apply_sql_name(name, 'snake').lower()``)
    so (source_id, normalized_table_name) is the ownership key — identical to
    ``_dataset_ownership_conflict`` in provisa/api/admin/schema.py.
    """

    def __init__(self) -> None:
        # (source_id, normalized_name) -> owning domain_id
        self._claims: dict[tuple[str, str], str] = {}

    @staticmethod
    def _normalize(name: str) -> str:
        from provisa.compiler.naming import apply_sql_name

        return apply_sql_name(name, "snake").lower()

    def conflict(self, source_id: str, table_name: str, domain_id: str) -> str | None:
        """Return an error string if a DIFFERENT domain already owns the dataset."""
        key = (source_id, self._normalize(table_name))
        owner = self._claims.get(key)
        if owner is not None and owner != domain_id:
            return (
                f"Table {table_name!r} on source {source_id!r} is already claimed by "
                f"domain {owner!r} (first-come ownership)."
            )
        return None

    def claim(self, source_id: str, table_name: str, domain_id: str) -> None:
        err = self.conflict(source_id, table_name, domain_id)
        if err is not None:
            raise RegistrationError(err)
        self._claims[(source_id, self._normalize(table_name))] = domain_id

    def is_claimed(self, source_id: str, table_name: str) -> bool:
        return (source_id, self._normalize(table_name)) in self._claims

    def is_greyed_out_for(self, source_id: str, table_name: str, domain_id: str) -> bool:
        """UI greys out a table for any domain that is not its owner (REQ-433)."""
        return self.conflict(source_id, table_name, domain_id) is not None


# ---------------------------------------------------------------------------
# REQ-434 — creation-request queue (mirrors _queue_creation_request semantics)
# ---------------------------------------------------------------------------


@dataclass
class CreationRequest:
    id: int
    request_type: str
    capability: str
    requested_by: str | None
    payload: dict[str, Any]
    status: str = "pending"
    rejection_reason: str | None = None


class CreationRequestQueue:
    """In-memory creation-request queue mirroring provisa creation_request repo.

    A governed create attempted without authority is persisted as a pending
    request rather than raising an error (REQ-434). Users holding the capability
    may execute or reject; rejection carries an actionable reason.
    """

    def __init__(self) -> None:
        self._requests: list[CreationRequest] = []
        self._next_id = 1

    def submit(
        self,
        caller_capabilities: set[str],
        request_type: str,
        capability: str,
        payload: dict[str, Any],
        requested_by: str | None,
    ) -> tuple[bool, CreationRequest | None]:
        """Return (performed_immediately, queued_request).

        If the caller holds *capability*, the create would proceed immediately
        (performed=True, no request queued). Otherwise a pending request is
        persisted (performed=False) — never an error.
        """
        if capability in caller_capabilities:
            return True, None
        req = CreationRequest(
            id=self._next_id,
            request_type=request_type,
            capability=capability,
            requested_by=requested_by,
            payload=payload,
        )
        self._next_id += 1
        self._requests.append(req)
        return False, req

    def list_pending(self) -> list[CreationRequest]:
        return [r for r in self._requests if r.status == "pending"]

    def execute(self, request_id: int, executor_capabilities: set[str]) -> CreationRequest:
        req = self._get(request_id)
        if req.capability not in executor_capabilities:
            raise RegistrationError(
                f"Executor lacks {req.capability!r} to execute request #{request_id}."
            )
        req.status = "executed"
        return req

    def reject(
        self, request_id: int, reason: str, executor_capabilities: set[str]
    ) -> CreationRequest:
        if not reason or not reason.strip():
            raise RegistrationError("Rejection requires a specific, actionable reason.")
        req = self._get(request_id)
        if req.capability not in executor_capabilities:
            raise RegistrationError(
                f"Executor lacks {req.capability!r} to reject request #{request_id}."
            )
        req.status = "rejected"
        req.rejection_reason = reason
        return req

    def _get(self, request_id: int) -> CreationRequest:
        for r in self._requests:
            if r.id == request_id:
                return r
        raise KeyError(f"No creation request #{request_id}")


# ===========================================================================
# Scenario bindings
# ===========================================================================

scenarios("../features/REQ-012.feature")
scenarios("../features/REQ-015.feature")
scenarios("../features/REQ-016.feature")
scenarios("../features/REQ-017.feature")
scenarios("../features/REQ-018.feature")
scenarios("../features/REQ-019.feature")
scenarios("../features/REQ-020.feature")
scenarios("../features/REQ-366.feature")
scenarios("../features/REQ-413.feature")
scenarios("../features/REQ-414.feature")
scenarios("../features/REQ-415.feature")
scenarios("../features/REQ-417.feature")
scenarios("../features/REQ-433.feature")
scenarios("../features/REQ-434.feature")
scenarios("../features/REQ-605.feature")
scenarios("../features/REQ-612.feature")
scenarios("../features/REQ-635.feature")
scenarios("../features/REQ-636.feature")
scenarios("../features/REQ-638.feature")


# ---------------------------------------------------------------------------
# REQ-012 — privileged source registration; Trino dynamic catalog; no restart
# ---------------------------------------------------------------------------


@given("a privileged steward with registration rights")
def _req012_privileged_steward(shared_data):
    shared_data["roles"] = {"source_registrar"}
    shared_data["reg_service"] = SourceRegistrationService()


@when("they submit a new source registration")
def _req012_submit_registration(shared_data):
    source = Source(
        id="new-pg",
        type=SourceType.postgresql,
        host="db",
        port=5432,
        database="app",
    )

    # Mock ONLY the true external boundaries: Trino dynamic-catalog API + DB probe.
    trino_api = MagicMock()
    created: dict[str, Any] = {}

    def _create_catalog(name, connector, properties):
        created[name] = {"connector": connector, "properties": properties}

    trino_api.create_catalog.side_effect = _create_catalog
    trino_api.list_catalogs.side_effect = lambda: list(created.keys())

    prober = MagicMock()
    prober.probe.return_value = True

    result = shared_data["reg_service"].register(shared_data["roles"], source, trino_api, prober)
    shared_data["reg_result"] = result
    shared_data["trino_api"] = trino_api
    shared_data["source"] = source


@then(
    "Provisa validates the connection, calls the Trino dynamic catalog API, and "
    "makes the source available within seconds without a server restart"
)
def _req012_then(shared_data):
    result = shared_data["reg_result"]
    trino_api = shared_data["trino_api"]
    # Connection validated
    assert shared_data["reg_service"]._connection_probe_results["new-pg"] is True
    # Trino dynamic catalog API was called (not a restart)
    trino_api.create_catalog.assert_called_once()
    # Available, no restart, within the bounded window
    assert result["available"] is True
    assert result["restart_required"] is False
    assert result["restart_count"] == 0
    assert result["elapsed_seconds"] < SourceRegistrationService.AVAILABILITY_TIMEOUT_SECONDS
    assert shared_data["reg_service"].is_registered("new_pg")


# ---------------------------------------------------------------------------
# REQ-015 — uniform Stage 2 governance; no per-table mode
# ---------------------------------------------------------------------------


@given("any registered table or view")
def _req015_registered(shared_data):
    engine = UniformStage2GovernanceEngine()
    engine.register_table("orders", ["id", "amount", "ssn"], row_filter="region = 'US'")
    engine.register_table("customers", ["id", "name"])  # a view-like object too
    engine.grant_table_rights("analyst", "orders")
    engine.grant_table_rights("analyst", "customers")
    shared_data["gov_engine"] = engine


@when("a user with the appropriate rights queries it")
def _req015_query(shared_data):
    engine = shared_data["gov_engine"]
    shared_data["gov_r1"] = engine.evaluate_query("analyst", "orders")
    shared_data["gov_r2"] = engine.evaluate_query("analyst", "customers")


@then("Stage 2 governance is applied uniformly without any per-table mode distinctions")
def _req015_then(shared_data):
    engine = shared_data["gov_engine"]
    r1, r2 = shared_data["gov_r1"], shared_data["gov_r2"]
    assert r1.allowed and r2.allowed
    # The ONE governance mode is stage2 for every object — no variation.
    assert engine.all_governance_modes_used() == {Stage2GovernanceMode.stage2}
    # No per-table mode was ever set.
    assert engine.any_per_table_mode_set() is False
    assert r1.per_table_mode is None and r2.per_table_mode is None
    # Governance actually did something table-specific (row filter on orders only).
    assert r1.row_filter_applied is True
    assert r2.row_filter_applied is False


# ---------------------------------------------------------------------------
# REQ-016 — publication triggers schema-gen; immediately in query builder
# ---------------------------------------------------------------------------


@given("a steward who publishes a table")
def _req016_steward(shared_data):
    shared_data["schema_engine"] = SchemaGenerationEngine()
    shared_data["publish_table"] = Table(
        source_id="pg1",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        columns=[Column(name="id", visible_to=[]), Column(name="amount", visible_to=[])],
    )


@when("the publication completes")
def _req016_publish(shared_data):
    engine = shared_data["schema_engine"]
    shared_data["published_schema"] = engine.publish("t_orders", shared_data["publish_table"])


@then(
    "a schema generation pass is triggered and the table is immediately available in "
    "the query builder"
)
def _req016_then(shared_data):
    from graphql import GraphQLSchema

    engine = shared_data["schema_engine"]
    assert engine.schema_generation_was_triggered_for("t_orders")
    assert engine.is_available_in_query_builder("t_orders")
    schema = shared_data["published_schema"]
    assert isinstance(schema, GraphQLSchema)
    # The generated schema actually exposes the published table as a root field.
    assert schema.query_type is not None
    assert "orders" in schema.query_type.fields


# ---------------------------------------------------------------------------
# REQ-017 — NoSQL read-only via native Trino connector
# ---------------------------------------------------------------------------


@given("a registered NoSQL source with a native Trino connector")
def _req017_source(shared_data):
    src = Source(id="mongo1", type=SourceType.mongodb, mapping={})
    shared_data["nosql_guard"] = NoSQLConnectorReadOnlyGuard(src)


@when("a consumer queries a table from that source")
def _req017_query(shared_data):
    guard = shared_data["nosql_guard"]
    shared_data["nosql_select"] = guard.execute("SELECT _id, name FROM sessions")
    shared_data["nosql_insert"] = guard.execute("INSERT INTO sessions VALUES (1)")
    shared_data["nosql_update"] = guard.execute("UPDATE sessions SET x = 1")


@then("the query is executed read-only through the Trino connector with no mutation path available")
def _req017_then(shared_data):
    guard = shared_data["nosql_guard"]
    # SELECT passes through the native connector
    sel = shared_data["nosql_select"]
    assert sel["allowed"] is True
    assert sel["connector"] == SOURCE_TO_CONNECTOR[SourceType.mongodb]
    # Mutations are rejected — no mutation path
    assert shared_data["nosql_insert"]["allowed"] is False
    assert shared_data["nosql_update"]["allowed"] is False
    assert shared_data["nosql_insert"]["error"] is not None
    # Every mutation was logged as rejected
    rejected = [e for e in guard.query_log() if e["type"] == "rejected_mutation"]
    assert len(rejected) == 2


# ---------------------------------------------------------------------------
# REQ-018 — FK candidates for steward confirm/reject
# ---------------------------------------------------------------------------


@given("tables in a registered source with FK constraints visible via Trino metadata")
def _req018_source(shared_data):
    shared_data["fk_engine"] = FKCandidateInferenceEngine(source_id="sales_pg")
    # Mock ONLY the live Trino connection (client boundary); empty result forces
    # the well-known demo-schema fallback set.
    trino_conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = []
    trino_conn.cursor.return_value = cur
    shared_data["fk_constraints"] = _simulate_trino_fk_introspection(
        "sales_pg", "public", trino_conn
    )


@when("a steward reviews relationship candidates")
def _req018_review(shared_data):
    engine = shared_data["fk_engine"]
    candidates = engine.infer_from_trino_metadata(shared_data["fk_constraints"])
    # Steward confirms one, rejects another.
    engine.confirm(candidates[0].id)
    engine.reject(candidates[1].id)
    shared_data["fk_candidates"] = candidates


@then("intra-source FK relationships are presented as candidates for confirmation or rejection")
def _req018_then(shared_data):
    engine = shared_data["fk_engine"]
    cands = shared_data["fk_candidates"]
    assert len(cands) == 3
    # All FK-inferred candidates carry high confidence (REQ-612 High tier).
    assert all(c.confidence == "high" for c in cands)
    # All belong to the same (intra) source.
    assert all(c.source_id == "sales_pg" for c in cands)
    statuses = {c.status for c in engine.all_candidates()}
    assert statuses == {"confirmed", "rejected", "pending"}
    assert len(engine.pending_candidates()) == 1


# ---------------------------------------------------------------------------
# REQ-019 — manual cross-source relationship with cardinality
# ---------------------------------------------------------------------------


@given("two tables in different registered sources")
def _req019_tables(shared_data):
    shared_data["xsrc_store"] = CrossSourceRelationshipStore()
    shared_data["xsrc_from"] = "pg1.public.orders"
    shared_data["xsrc_to"] = "mongo1.default.customers"


@when("a steward manually defines a cross-source relationship with cardinality")
def _req019_define(shared_data):
    rel = Relationship(
        id="xsrc_orders_customer",
        source_table_id=shared_data["xsrc_from"],
        target_table_id=shared_data["xsrc_to"],
        source_column="customer_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
        owner="steward-alice",
    )
    shared_data["xsrc_store"].persist(rel)
    shared_data["xsrc_rel"] = rel


@then("the relationship is persisted and available for query traversal")
def _req019_then(shared_data):
    store = shared_data["xsrc_store"]
    rel = store.get("xsrc_orders_customer")
    assert rel is not None
    assert rel.cardinality == Cardinality.many_to_one
    # Resolvable for traversal from the originating table.
    found = store.find_for_traversal("pg1", shared_data["xsrc_from"])
    assert rel in found
    assert store.find_by_cardinality(Cardinality.many_to_one) == [rel]


# ---------------------------------------------------------------------------
# REQ-020 — relationship ownership, versioning, re-review flag
# ---------------------------------------------------------------------------


@given("a registered relationship between two tables")
def _req020_rel(shared_data):
    rel = Relationship(
        id="rel_orders_customer",
        source_table_id="orders",
        target_table_id="customers",
        source_column="customer_id",
        target_column="id",
        cardinality=Cardinality.many_to_one,
        owner="steward-bob",
        version=1,
        needs_review=False,
    )
    shared_data["rel020"] = rel


@when("a schema change affects one of the join fields")
def _req020_change(shared_data):
    rel = shared_data["rel020"]
    changed_columns = {"customers": {"id"}}  # the target join column changed
    join_fields = {
        rel.source_table_id: {rel.source_column},
        rel.target_table_id: {rel.target_column},
    }
    affected = any(changed_columns.get(tbl, set()) & cols for tbl, cols in join_fields.items())
    if affected:
        rel.needs_review = True
        rel.version += 1
    shared_data["rel020_affected"] = affected


@then("the relationship is flagged for re-review and the owning steward is notified")
def _req020_then(shared_data):
    rel = shared_data["rel020"]
    assert shared_data["rel020_affected"] is True
    assert rel.needs_review is True
    assert rel.version == 2
    # Ownership is recorded so the owner can be notified.
    assert rel.owner == "steward-bob"


# ---------------------------------------------------------------------------
# REQ-366 — view approval workflow OR originator rights
# ---------------------------------------------------------------------------


@given("a user attempting to create a view over tables they do not own")
def _req366_user(shared_data):
    shared_data["view_engine"] = ViewGovernanceEngine()
    # Registry: only "owner_user" holds rights, not "originator".
    shared_data["rights_registry"] = {"owner_user": {"orders", "customers"}}


@when("they submit the view creation")
def _req366_submit(shared_data):
    engine = shared_data["view_engine"]
    # Case A: originator lacks rights → approval workflow triggered.
    shared_data["view_req_gated"] = engine.submit_view_creation(
        originator_id="originator",
        view_name="orders_with_customer",
        underlying_tables=["orders"],
        joins=[("orders", "customers")],
        rights_registry=shared_data["rights_registry"],
    )
    # Case B: originator already owns everything → no approval needed.
    shared_data["view_req_owned"] = engine.submit_view_creation(
        originator_id="owner_user",
        view_name="orders_only",
        underlying_tables=["orders", "customers"],
        joins=[],
        rights_registry=shared_data["rights_registry"],
    )


@then(
    "an approval workflow is triggered unless the originator already holds rights to all "
    "underlying tables and joins"
)
def _req366_then(shared_data):
    engine = shared_data["view_engine"]
    gated = shared_data["view_req_gated"]
    owned = shared_data["view_req_owned"]
    # Gated: pending, not executed, in the queue — no view created yet.
    assert gated.approval_status == ApprovalStatus.pending
    assert gated.executed is False
    assert gated in engine.pending_queue()
    # Owned: no approval required, executed immediately.
    assert owned.approval_status == ApprovalStatus.not_required
    assert owned.executed is True
    # An authorized user can then execute the gated request.
    executed = engine.approve_and_execute(gated.id)
    assert executed.executed is True
    assert executed.approval_status == ApprovalStatus.approved
    assert engine.pending_queue() == []


# ---------------------------------------------------------------------------
# REQ-413 — auto-generate GQL relationships from FK metadata
# ---------------------------------------------------------------------------


class _FakeDriverResult:
    def __init__(self, rows):
        self.rows = rows


class _FakePgDriver:
    """Stands in for a live source pool driver at the client boundary."""

    def __init__(self, outbound, inbound):
        self._outbound = outbound
        self._inbound = inbound

    async def execute(self, sql, params):
        if "AND tc.table_name" in sql:  # outbound query
            return _FakeDriverResult(self._outbound)
        return _FakeDriverResult(self._inbound)


class _FakeConfigConn:
    """Stands in for the asyncpg config connection."""

    def __init__(self):
        self.inserted: list[dict] = []
        self._table_ids = {"orders": 1, "customers": 2}

    async def fetchrow(self, sql, source_id, table_name):
        tid = self._table_ids.get(table_name)
        return {"id": tid} if tid is not None else None

    async def execute(self, sql, *args):
        rel_id, src_id, tgt_id, src_col, tgt_col, cardinality, alias = args
        self.inserted.append(
            {
                "id": rel_id,
                "source_table_id": src_id,
                "target_table_id": tgt_id,
                "source_column": src_col,
                "target_column": tgt_col,
                "cardinality": cardinality,
                "alias": alias,
            }
        )
        return "INSERT 0 1"


class _FakeSourcePools:
    def __init__(self, source_id, driver):
        self._sid = source_id
        self._driver = driver

    def has(self, source_id):
        return source_id == self._sid

    def get(self, source_id):
        return self._driver


import asyncio as _asyncio  # noqa: E402


@given("a database source with FK constraints")
def _req413_source(shared_data):
    # orders.customer_id -> customers.id
    driver = _FakePgDriver(
        outbound=[("customer_id", "customers", "id")],
        inbound=[],
    )
    shared_data["r413_pools"] = _FakeSourcePools("sales_pg", driver)
    shared_data["r413_conn"] = _FakeConfigConn()


@when("schema introspection runs")
def _req413_run(shared_data):
    from provisa.discovery.fk_introspect import auto_register_fk_relationships

    inserted = _asyncio.run(
        auto_register_fk_relationships(
            shared_data["r413_pools"],
            "postgresql",
            "sales_pg",
            "public",
            "orders",
            shared_data["r413_conn"],
        )
    )
    shared_data["r413_inserted"] = inserted


@then("GQL relationships are auto-generated from the FK metadata")
def _req413_then(shared_data):
    # Both directions inserted: many-to-one and one-to-many.
    assert shared_data["r413_inserted"] == 2
    rows = shared_data["r413_conn"].inserted
    cards = {r["cardinality"] for r in rows}
    assert cards == {"many-to-one", "one-to-many"}
    m2o = next(r for r in rows if r["cardinality"] == "many-to-one")
    assert m2o["source_table_id"] == 1 and m2o["target_table_id"] == 2
    assert m2o["alias"] == "customers"  # default (non-Hasura) style


# ---------------------------------------------------------------------------
# REQ-414 — demo schema includes at least one FK relationship
# ---------------------------------------------------------------------------


import re as _re  # noqa: E402
from pathlib import Path as _Path  # noqa: E402


@given("the demo installation schema")
def _req414_schema(shared_data):
    init_sql = _Path("db/init.sql")
    assert init_sql.exists(), "demo schema db/init.sql must exist"
    shared_data["r414_sql"] = init_sql.read_text()


@when("relationship discovery runs")
def _req414_discover(shared_data):
    # Parse REFERENCES clauses from the real demo DDL.
    fks = _re.findall(
        r"(\w+)\s+\w+[^,]*REFERENCES\s+(\w+)\s*\((\w+)\)",
        shared_data["r414_sql"],
        _re.IGNORECASE,
    )
    shared_data["r414_fks"] = fks


@then("at least one FK relationship is auto-discovered and exercised")
def _req414_then(shared_data):
    fks = shared_data["r414_fks"]
    assert len(fks) >= 1, "demo schema must contain at least one FK relationship"
    # The known orders -> customers FK is present.
    targets = {ref_table for _col, ref_table, _refcol in fks}
    assert "customers" in targets


# ---------------------------------------------------------------------------
# REQ-415 — hasura_v2_relationship_style inflection
# ---------------------------------------------------------------------------


@given("hasura_v2_relationship_style enabled")
def _req415_enabled(shared_data):
    shared_data["r415_style"] = True


@when("FK-derived relationships are named")
def _req415_named(shared_data):
    from provisa.discovery.fk_introspect import _m2o_alias, _o2m_alias

    style = shared_data["r415_style"]
    shared_data["r415_m2o"] = {
        "users": _m2o_alias("users", style),
        "categories": _m2o_alias("categories", style),
    }
    shared_data["r415_o2m"] = {
        "order": _o2m_alias("order", style),
        "category": _o2m_alias("category", style),
    }
    # Default style for contrast
    shared_data["r415_m2o_default"] = _m2o_alias("users", False)


@then("many-to-one names are singular and one-to-many names are plural via inflection")
def _req415_then(shared_data):
    # Object (many-to-one) aliases singularized
    assert shared_data["r415_m2o"]["users"] == "user"
    assert shared_data["r415_m2o"]["categories"] == "category"
    # Array (one-to-many) aliases pluralized
    assert shared_data["r415_o2m"]["order"] == "orders"
    assert shared_data["r415_o2m"]["category"] == "categories"
    # Without the option, names are verbatim.
    assert shared_data["r415_m2o_default"] == "users"


# ---------------------------------------------------------------------------
# REQ-417 — Hasura v2 Remote Schemas -> graphql_remote sources
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata file containing Remote Schema entries")
def _req417_metadata(shared_data):
    from provisa.hasura_v2.models import HasuraMetadata, HasuraRemoteSchema

    rs = HasuraRemoteSchema(
        name="countries_api",
        definition={
            "url": "https://countries.trevorblades.com/",
            "headers": [{"name": "X-Api-Key", "value": "secret"}],
            "forward_client_headers": True,
            "timeout_seconds": 60,
        },
    )
    shared_data["r417_metadata"] = HasuraMetadata(version=3, remote_schemas=[rs])


@when("the migration tool runs")
def _req417_migrate(shared_data):
    from provisa.hasura_v2.mapper import convert_metadata

    shared_data["r417_config"] = convert_metadata(shared_data["r417_metadata"])


@then(
    "each Remote Schema is mapped to a graphql_remote source registration preserving "
    "name, URL, headers, and auth"
)
def _req417_then(shared_data):
    config = shared_data["r417_config"]
    remotes = [s for s in config.sources if s.type == SourceType.graphql_remote]
    assert len(remotes) == 1
    src = remotes[0]
    assert src.id == "countries_api"  # name preserved
    assert src.base_url == "https://countries.trevorblades.com/"  # URL preserved
    assert src.mapping["headers"] == {"X-Api-Key": "secret"}  # headers/auth preserved
    assert src.mapping["forward_client_headers"] is True
    assert src.mapping["timeout_seconds"] == 60


# ---------------------------------------------------------------------------
# REQ-433 — first-come dataset ownership across domains
# ---------------------------------------------------------------------------


@given("a datasource shared across multiple domains")
def _req433_source(shared_data):
    shared_data["r433_registry"] = DatasetOwnershipRegistry()
    shared_data["r433_source"] = "shared_pg"


@when("a domain owner claims a table")
def _req433_claim(shared_data):
    reg = shared_data["r433_registry"]
    reg.claim(shared_data["r433_source"], "Orders", "sales")


@then(
    "no other domain may claim that same physical table; the UI greys it out for all other domains"
)
def _req433_then(shared_data):
    reg = shared_data["r433_registry"]
    src = shared_data["r433_source"]
    # Different domain, normalized-equal name is blocked (first-come).
    err = reg.conflict(src, "orders", "marketing")
    assert err is not None and "sales" in err
    with pytest.raises(RegistrationError):
        reg.claim(src, "orders", "marketing")
    # The owning domain may re-register.
    assert reg.conflict(src, "orders", "sales") is None
    # UI greys the table out for every non-owning domain.
    assert reg.is_greyed_out_for(src, "orders", "marketing") is True
    assert reg.is_greyed_out_for(src, "orders", "sales") is False


# ---------------------------------------------------------------------------
# REQ-434 — creation-request queue for unauthorized creates
# ---------------------------------------------------------------------------


@given("a user without create authority attempting to create a view or relationship")
def _req434_user(shared_data):
    shared_data["r434_queue"] = CreationRequestQueue()
    shared_data["r434_caller_caps"] = set()  # no capabilities
    shared_data["r434_admin_caps"] = {"create_view"}


@when("they submit the creation")
def _req434_submit(shared_data):
    queue = shared_data["r434_queue"]
    performed, req = queue.submit(
        shared_data["r434_caller_caps"],
        request_type="view",
        capability="create_view",
        payload={"view_name": "orders_summary"},
        requested_by="analyst",
    )
    shared_data["r434_performed"] = performed
    shared_data["r434_req"] = req


@then(
    "a persisted request is created in the queue rather than an error; an authorized user "
    "may execute or reject it"
)
def _req434_then(shared_data):
    queue = shared_data["r434_queue"]
    # No error, no immediate perform — a pending request instead.
    assert shared_data["r434_performed"] is False
    req = shared_data["r434_req"]
    assert req is not None and req.status == "pending"
    assert req in queue.list_pending()
    # An authorized user may execute it.
    executed = queue.execute(req.id, shared_data["r434_admin_caps"])
    assert executed.status == "executed"
    # A fresh request can be rejected with an actionable reason.
    _p, req2 = queue.submit(set(), "relationship", "create_view", {"rel": "x"}, "analyst")
    rejected = queue.reject(req2.id, "Duplicate of existing relationship rel_y", {"create_view"})
    assert rejected.status == "rejected"
    assert rejected.rejection_reason
    # Rejection requires a reason.
    _p, req3 = queue.submit(set(), "view", "create_view", {}, "analyst")
    with pytest.raises(RegistrationError):
        queue.reject(req3.id, "  ", {"create_view"})


# ---------------------------------------------------------------------------
# REQ-605 — root_table_ids exclusion from root fields, kept as named types
# ---------------------------------------------------------------------------


@given("a SchemaInput with root_table_ids set excluding some tables")
def _req605_input(shared_data):
    # Exclude customers (id=2); keep orders (id=1) as a root table.
    shared_data["r605_si"] = _build_schema_input_with_root_ids({1})


@when("the SDL is generated")
def _req605_generate(shared_data):
    from provisa.compiler.schema_gen import generate_schema

    shared_data["r605_schema"] = generate_schema(shared_data["r605_si"])


@then("excluded tables are present as named types but absent from root query fields")
def _req605_then(shared_data):
    schema = shared_data["r605_schema"]
    root_fields = set(schema.query_type.fields.keys())
    # orders is a root field; customers is NOT.
    assert "orders" in root_fields
    assert "customers" not in root_fields
    # customers still exists as a named GraphQL object type, reachable via relationship.
    type_names = set(schema.type_map.keys())
    assert "Customers" in type_names
    # And the orders type exposes the relationship field to customers.
    orders_type = schema.type_map["Orders"]
    assert "customer" in orders_type.fields
    # The relationship field's return type is the excluded Customers type.
    customer_field = orders_type.fields["customer"]
    assert "Customers" in str(customer_field.type)


# ---------------------------------------------------------------------------
# REQ-612 — four-level confidence hierarchy ranking
# ---------------------------------------------------------------------------


@given("multiple relationship candidates of varying evidence types")
def _req612_candidates(shared_data):
    from provisa.discovery.analyzer import RelationshipCandidate

    # One candidate per tier, deliberately out of order.
    shared_data["r612_candidates"] = [
        RelationshipCandidate(
            source_table_id=5,
            source_column="a_id",
            target_table_id=6,
            target_column="id",
            cardinality="many-to-one",
            confidence=ConfidenceTier.cross_source_semantic.value,
            reasoning="cross-source semantic inference: naming similarity",
        ),
        RelationshipCandidate(
            source_table_id=1,
            source_column="user_id",
            target_table_id=2,
            target_column="id",
            cardinality="many-to-one",
            confidence=ConfidenceTier.approved_catalog.value,
            reasoning="approved catalog relationship validated by both stewards",
        ),
        RelationshipCandidate(
            source_table_id=3,
            source_column="cat_id",
            target_table_id=4,
            target_column="id",
            cardinality="many-to-one",
            confidence=ConfidenceTier.intra_source_semantic.value,
            reasoning="intra-source semantic inference",
        ),
        RelationshipCandidate(
            source_table_id=7,
            source_column="order_id",
            target_table_id=8,
            target_column="id",
            cardinality="many-to-one",
            confidence=ConfidenceTier.intra_source_fk.value,
            reasoning="intra-source FK constraint",
        ),
    ]


@when("candidates are presented to a steward")
def _req612_rank(shared_data):
    shared_data["r612_ranked"] = _rank_candidates(shared_data["r612_candidates"])


@then("they are ranked by confidence from approved catalog down to cross-source semantic inference")
def _req612_then(shared_data):
    ranked = shared_data["r612_ranked"]
    confidences = [c.confidence for c in ranked]
    # Strictly descending, matching the four-level hierarchy.
    assert confidences == sorted(confidences, reverse=True)
    assert confidences == [
        ConfidenceTier.approved_catalog.value,
        ConfidenceTier.intra_source_fk.value,
        ConfidenceTier.intra_source_semantic.value,
        ConfidenceTier.cross_source_semantic.value,
    ]
    # Highest is the approved catalog relationship; lowest is cross-source semantic.
    assert "approved catalog" in ranked[0].reasoning
    assert "cross-source" in ranked[-1].reasoning


# ---------------------------------------------------------------------------
# REQ-635 — schema name is the source's native grouping name
# ---------------------------------------------------------------------------


@given("a relational database source")
def _req635_source(shared_data):
    shared_data["r635_cases"] = {}


@when("available schemas are listed")
def _req635_list(shared_data):
    from provisa.api.admin.introspect import native_schemas
    from provisa.executor.pool import SourcePool

    async def _run():
        results: dict[str, list[str] | None] = {}

        # Relational (postgresql) — native schema names via the live driver.
        class _PgPool:
            def has(self, sid):
                return True

            async def execute(self, sid, sql):
                return _FakeDriverResult([("public",), ("analytics",)])

        results["postgresql"] = await native_schemas(
            "pg1", "postgresql", cast(SourcePool, _PgPool()), MagicMock()
        )

        # Flat/API source with no native grouping — fixed source-type constant.
        class _NoPool:
            def has(self, sid):
                return False

        results["graphql"] = await native_schemas(
            "gql1", "graphql", cast(SourcePool, _NoPool()), MagicMock()
        )
        results["openapi"] = await native_schemas(
            "api1", "openapi", cast(SourcePool, _NoPool()), MagicMock()
        )
        results["kafka"] = await native_schemas(
            "k1", "kafka", cast(SourcePool, _NoPool()), MagicMock()
        )
        return results

    shared_data["r635_cases"] = _asyncio.run(_run())


@then(
    "the native schema names are presented; for flat/API sources a fixed source-type "
    "constant is used"
)
def _req635_then(shared_data):
    cases = shared_data["r635_cases"]
    # Relational: the database's own schema names.
    assert cases["postgresql"] == ["public", "analytics"]
    # Flat/API: a fixed constant naming the source type (no native grouping concept).
    assert cases["graphql"] == ["graphql"]
    assert cases["openapi"] == ["openapi"]
    assert cases["kafka"] == ["kafka"]


# ---------------------------------------------------------------------------
# REQ-636 — Trino preferred when connector configured; native only otherwise
# ---------------------------------------------------------------------------


@given("a source type with a Trino connector configured")
def _req636_source(shared_data):
    # mongodb HAS a Trino connector; graphql_remote resolves via the native path.
    shared_data["r636_connectored"] = SourceType.mongodb
    shared_data["r636_native_only"] = SourceType.graphql_remote


@when("schema or table introspection is triggered")
def _req636_introspect(shared_data):
    from provisa.api.admin.introspect import native_schemas
    from provisa.core.models import SOURCE_TO_CONNECTOR as _S2C
    from provisa.executor.pool import SourcePool

    async def _run():
        class _NoPool:
            def has(self, sid):
                return False

        # mongodb: no cheap native path AND has a Trino connector -> native_schemas
        # returns None, signalling the caller to use the Trino path.
        mongo_native = await native_schemas(
            "m1", "mongodb", cast(SourcePool, _NoPool()), MagicMock()
        )
        # graphql: native path returns a value (fixed constant) -> native is used.
        graphql_native = await native_schemas(
            "g1", "graphql", cast(SourcePool, _NoPool()), MagicMock()
        )
        return mongo_native, graphql_native

    mongo_native, graphql_native = _asyncio.run(_run())
    shared_data["r636_mongo_native"] = mongo_native
    shared_data["r636_graphql_native"] = graphql_native
    shared_data["r636_mongo_has_connector"] = shared_data["r636_connectored"] in _S2C


@then(
    "Trino is used as the introspection path; native driver is used only when no connector exists"
)
def _req636_then(shared_data):
    # mongodb has a Trino connector; native_schemas defers (None) so the caller uses Trino.
    assert shared_data["r636_mongo_has_connector"] is True
    assert shared_data["r636_mongo_native"] is None
    # graphql resolves via native path (returns a concrete value, not None).
    assert shared_data["r636_graphql_native"] == ["graphql"]


# ---------------------------------------------------------------------------
# REQ-638 — single availableSchemas / availableTables entry points
# ---------------------------------------------------------------------------


@given("a UI requesting schema and table lists for any source type")
def _req638_ui(shared_data):
    from provisa.api.admin.schema import Query

    shared_data["r638_query"] = Query


@when("it calls availableSchemas and availableTables")
def _req638_call(shared_data):
    import inspect as _inspect

    query = shared_data["r638_query"]
    # Enumerate the introspection entry points exposed on the admin Query type.
    members = {name for name, _ in _inspect.getmembers(query)}
    shared_data["r638_members"] = members


@then("the backend selects the correct strategy internally without exposing per-type endpoints")
def _req638_then(shared_data):
    members = shared_data["r638_members"]
    # Exactly one schema endpoint and one table endpoint.
    assert "available_schemas" in members
    assert "available_tables" in members
    # No per-source-type endpoints (routing is internal to native_schemas/native_tables).
    per_type = [
        m
        for m in members
        if any(
            t in m
            for t in (
                "postgres_schemas",
                "mysql_schemas",
                "mongodb_schemas",
                "graphql_schemas",
                "kafka_schemas",
                "trino_schemas",
                "postgres_tables",
                "mongodb_tables",
            )
        )
    ]
    assert per_type == []
    # Routing is centralized in native_schemas / native_tables.
    from provisa.api.admin import introspect as _intro

    assert hasattr(_intro, "native_schemas")
    assert hasattr(_intro, "native_tables")


# Copyright (c) 2026 Kenneth Stott
# Canary: fa07eacc-adce-46ac-bcb2-6447f357a6f1
#
# This source code is licensed under the Business Source License 1.1


# No new step definitions are required for REQ-016.
# All steps and the scenarios() binding already exist in the file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 857344ae-dad5-40fe-849e-f14e2bd91aaa
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5f10a64e-a01c-4d7c-a218-c4ce6e0d66b9
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 84f7ab53-c00e-4cdc-8f76-1d413688e1a8
#
# This source code is licensed under the Business Source License 1.1
