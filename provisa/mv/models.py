# Copyright (c) 2026 Kenneth Stott
# Canary: 677d06ed-af4f-4d99-bbdd-2a773e8bb1bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialized view data models (REQ-081 through REQ-086)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

# Requirements: REQ-133, REQ-135, REQ-158, REQ-160, REQ-199, REQ-234, REQ-235


class MVStatus(str, Enum):  # REQ-160, REQ-199, REQ-235
    FRESH = "fresh"
    STALE = "stale"
    REFRESHING = "refreshing"
    DISABLED = "disabled"
    SKIPPED_SIZE = "skipped_size"


@dataclass(frozen=True)
class JoinPattern:  # REQ-158
    """Defines a JOIN pattern that an MV covers."""

    left_table: str  # table name
    left_column: str
    right_table: str  # table name
    right_column: str
    join_type: str = "left"  # left, inner


@dataclass(frozen=True)
class SDLConfig:  # REQ-653
    """Configuration for exposing an MV in the GraphQL SDL."""

    domain_id: str
    columns: list[dict] | None = None  # [{name, visible_to: [roles]}]


@dataclass
class MVDefinition:  # REQ-133, REQ-135, REQ-158, REQ-160, REQ-199, REQ-234, REQ-235
    """A materialized view definition."""

    id: str
    source_tables: list[str]  # table names referenced
    target_catalog: str
    target_schema: str
    target_table: str | None = None  # auto-generated if not specified
    tenant_id: str | None = None  # when set, Iceberg schema = f"{tenant_id}_mv"
    refresh_interval: int = 300  # seconds
    enabled: bool = True

    # Mode 1: Join-pattern (transparent optimization)
    join_pattern: JoinPattern | None = None

    # Mode 2: Custom SQL (optionally exposed in SDL)
    sql: str | None = None
    expose_in_sdl: bool = False
    sdl_config: SDLConfig | None = None

    # Aggregate MV routing (REQ-198/199)
    serves_aggregates: bool = False
    aggregate_columns: list[str] = field(default_factory=list)
    # REQ-882: predicate fragments this aggregate MV was pre-computed WITH. An aggregate
    # query may use this MV only when these are a SUBSET of the query's own filters (so the
    # MV is no more restrictive than the query); the query's extra filters are re-applied.
    filters: list[str] = field(default_factory=list)

    # Consistency tier (REQ-879, ADR 0001): "shared" = one coordinated copy in the
    # materialization store (snapshot-consistent; engages the refresh-coordination
    # catalog). "distributed" = per-instance materialization (eventually consistent —
    # requires a deterministic view AND a source that quiesces within a refresh cycle;
    # never converges for a never-settling high-churn source).
    consistency: str = "shared"

    # Lifecycle guards
    max_rows: int = 1_000_000
    orphan_grace_period: int = 86400  # 24h in seconds

    # REQ-881: refresh-time freshness gate. "ttl" (default) rebuilds every refresh_interval;
    # "probe" re-checks every loop ignoring TTL; "ttl_probe" probes only after the TTL floor.
    # For probe/ttl_probe, refresh skips the (expensive) rebuild when every source reports an
    # unchanged input token vs last_input_token (REQ-862 signals). Single-instance today; the
    # authoritative shared token store is REQ-879.
    freshness_mode: str = "ttl"

    # REQ-963 live-MV debounce (event-loop path). quiet<=0 → recompute-to-current fires immediately
    # (real-time). Otherwise a burst of upstream fan-ins collapses into one recompute at
    # min(last_change+quiet, first_change+max_delay); max_delay is the mandatory staleness-SLA cap.
    debounce_quiet: float = 0.0
    debounce_max_delay: float | None = None

    # REQ-965: the TWO independent operator-declared MV outcomes.
    #  - persist: how the recompute is applied to the MV's OWN store table (replace/append/upsert).
    #  - emit: the SET of downstream event shapes this MV may emit (subset of replace/append/delta);
    #    None keeps the default single-shape fan-out. Emission is demand-driven (pay-per-consumer).
    #  - consumes: which upstream shapes THIS MV subscribes to (drives a producer's demand routing);
    #    default {replace} = recompute-on-any-input-change.
    # REQ-970: primary_key is the operator-declared PK for the derived table (required for
    # persist=upsert / emit=delta when not inferable from a GROUP BY).
    persist: str = "replace"
    emit: list[str] | None = None
    consumes: list[str] = field(default_factory=lambda: ["replace"])
    primary_key: list[str] = field(default_factory=list)

    # Runtime state
    status: MVStatus = MVStatus.STALE
    last_refresh_at: float | None = None
    row_count: int | None = None
    last_error: str | None = None
    last_input_token: str | None = None  # REQ-881: source version token at last materialization

    def __post_init__(self):
        if self.target_table is None:
            self.target_table = f"mv_{self.id.replace('-', '_')}"
        if self.tenant_id is not None:
            self.target_schema = f"{self.tenant_id}_mv"

    @property
    def is_fresh(self) -> bool:  # REQ-483
        return self.status == MVStatus.FRESH

    def freshness_subject(self):  # REQ-859
        """This MV's refresh state as a FreshnessSubject for the unified predicate."""
        from provisa.freshness.adapters import StateSubject

        return StateSubject(refreshed_at=self.last_refresh_at, ok=self.last_error is None)

    def is_fresh_at(self, now: float) -> bool:
        """TTL-aware freshness (REQ-199).

        An MV is serveable only when its status is FRESH and its last refresh is within
        ``refresh_interval`` (its TTL). A FRESH MV whose TTL has elapsed is treated as stale
        so the query falls back to the live source rather than serving expired data. The TTL
        decision is delegated to the shared freshness module (REQ-859); the FRESH-status gate
        stays here as the orthogonal lifecycle guard.
        """
        if self.status != MVStatus.FRESH:
            return False
        from provisa.freshness import Ttl, evaluate

        return evaluate(self.freshness_subject(), Ttl(self.refresh_interval), now).is_fresh

    @property
    def is_join_pattern(self) -> bool:  # REQ-653
        return self.join_pattern is not None

    @property
    def is_custom_sql(self) -> bool:
        return self.sql is not None
