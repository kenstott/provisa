# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Scheduled re-embedding (REQ-428).

Plans how an embedding column / fallback cache is refreshed: incrementally re-embed
only the rows that changed, or fully rebuild when the model or source schema changed
(a model/dimension change invalidates every existing vector).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

# Requirements: REQ-428, REQ-429


class RefreshMode(str, Enum):
    INCREMENTAL = "incremental"
    FULL = "full"


@dataclass
class RefreshPlan:
    mode: RefreshMode
    pks: list = field(default_factory=list)  # rows to re-embed (all for FULL is left to the caller)
    reason: str = ""


def needs_full_rebuild(  # REQ-428, REQ-429
    old_model_id: str | None,
    new_model_id: str,
    old_dimensions: int | None,
    new_dimensions: int,
    schema_changed: bool = False,
) -> bool:
    """A model change, dimension change, or schema change requires a full rebuild."""
    if schema_changed:
        return True
    if old_model_id is not None and old_model_id != new_model_id:
        return True
    if old_dimensions is not None and old_dimensions != new_dimensions:
        return True
    return False


def plan_refresh(  # REQ-428
    changed_pks: list,
    new_model_id: str,
    new_dimensions: int,
    old_model_id: str | None = None,
    old_dimensions: int | None = None,
    schema_changed: bool = False,
) -> RefreshPlan:
    """Plan an incremental refresh of changed rows, or a full rebuild on model/schema change."""
    if needs_full_rebuild(
        old_model_id, new_model_id, old_dimensions, new_dimensions, schema_changed
    ):
        reason = "schema change" if schema_changed else "model/dimension change"
        return RefreshPlan(mode=RefreshMode.FULL, pks=[], reason=reason)
    return RefreshPlan(mode=RefreshMode.INCREMENTAL, pks=list(changed_pks), reason="changed rows")
