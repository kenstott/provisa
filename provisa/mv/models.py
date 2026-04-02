# Copyright (c) 2025 Kenneth Stott
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


class MVStatus(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    REFRESHING = "refreshing"
    DISABLED = "disabled"


@dataclass(frozen=True)
class JoinPattern:
    """Defines a JOIN pattern that an MV covers."""

    left_table: str  # table name
    left_column: str
    right_table: str  # table name
    right_column: str
    join_type: str = "left"  # left, inner


@dataclass(frozen=True)
class SDLConfig:
    """Configuration for exposing an MV in the GraphQL SDL."""

    domain_id: str
    governance: str = "pre-approved"
    columns: list[dict] | None = None  # [{name, visible_to: [roles]}]


@dataclass
class MVDefinition:
    """A materialized view definition."""

    id: str
    source_tables: list[str]  # table names referenced
    target_catalog: str
    target_schema: str
    target_table: str | None = None  # auto-generated if not specified
    refresh_interval: int = 300  # seconds
    enabled: bool = True

    # Mode 1: Join-pattern (transparent optimization)
    join_pattern: JoinPattern | None = None

    # Mode 2: Custom SQL (optionally exposed in SDL)
    sql: str | None = None
    expose_in_sdl: bool = False
    sdl_config: SDLConfig | None = None

    # Runtime state
    status: MVStatus = MVStatus.STALE
    last_refresh_at: float | None = None
    row_count: int | None = None
    last_error: str | None = None

    def __post_init__(self):
        if self.target_table is None:
            self.target_table = f"mv_{self.id.replace('-', '_')}"

    @property
    def is_fresh(self) -> bool:
        return self.status == MVStatus.FRESH

    @property
    def is_join_pattern(self) -> bool:
        return self.join_pattern is not None

    @property
    def is_custom_sql(self) -> bool:
        return self.sql is not None
