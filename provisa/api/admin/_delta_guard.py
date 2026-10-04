# Copyright (c) 2026 Kenneth Stott
# Canary: 65f62dd1-6bf9-433e-8dc7-3ff299a33262
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Refuse a table that declares a delta at save, until the delta apply path lands (REQ-874).

The declaration, validation and persistence of ``delta`` are in place but the apply path is not
wired into the replicator yet, so a declared delta would silently whole-rebuild. Registering or
updating such a table is refused by name until then. This guard is reverted when the apply path
lands (it is the mirror of config_loader._validate_delta's same refusal)."""

# Requirements: REQ-874

from __future__ import annotations

from typing import Any

from provisa.api.admin.types import MutationResult


def table_delta_refusal(model: Any) -> MutationResult | None:
    if getattr(model, "delta", None) is None:
        return None
    return MutationResult(
        success=False,
        message=f"table {model.table_name!r}: delta replication is not available yet (REQ-874)",
        code="schema.delta_not_available",
        params={"table": model.table_name},
    )
