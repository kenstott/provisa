# Copyright (c) 2026 Kenneth Stott
# Canary: 2c68de69-ad61-4ee4-bed2-093f20e6a1d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Refuse a table that declares a delta on a non-SQL source at save (REQ-874).

The delta apply path is a generated SQL delta run on the source's native pool, so it is defined
only for SQL source types. Registering or updating a table that declares ``delta`` on any other
source type is refused by name -- the admin-mutation mirror of ``config_loader._validate_delta``."""

# Requirements: REQ-874

from __future__ import annotations

from typing import Any

from provisa.api.admin.types import MutationResult


async def table_delta_refusal(conn: Any, model: Any) -> MutationResult | None:
    """A failing MutationResult when ``model`` declares ``delta`` and its source is not a SQL
    source a generated delta is defined for (REQ-874); None otherwise."""
    if getattr(model, "delta", None) is None:
        return None
    from provisa.core.repositories import source as source_repo
    from provisa.federation.delta import delta_source_supported

    source = await source_repo.get(conn, model.source_id)
    source_type = (source or {}).get("type")
    if source_type is not None and delta_source_supported(source_type):
        return None
    return MutationResult(
        success=False,
        message=f"table {model.table_name!r}: delta replication is only available for SQL sources, "
        f"not {source_type!r} (REQ-874)",
        code="schema.delta_not_sql_source",
        params={"table": model.table_name, "source": model.source_id, "type": source_type},
    )
