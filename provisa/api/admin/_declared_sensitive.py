# Copyright (c) 2026 Kenneth Stott
# Canary: 8e77c2b7-d7a8-415d-af0d-a7bbaa11e6b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Applying a source kind's declaration of sensitive columns at registration (REQ-1943).

Two steps of ``register_table``, both taken only for a table its source kind declares
(``provisa.core.declared_sensitive``) and only the first time it is registered:

- before the table is stored, the declared columns are hidden as declared, in place of whatever
  the registration carried for them; the hiding guard then asks no right for those columns,
  since the registrar did not choose them;
- once it is stored, each carries the sensitive tag, written by the one writer of tag
  assignments.

A table already registered is not touched: by then its declared columns are sensitive, and how
they are hidden is its steward's (``sensitive_data``).
"""

# Requirements: REQ-1943
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.core import declared_sensitive

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def hide_declared(conn: "Connection", source_type: str | None, model: Any) -> frozenset[str]:
    """Hide ``model``'s declared columns if this is its first registration, and answer which
    they were: empty for a table its source kind declares nothing of, or one already stored."""
    from provisa.api.admin._hiding_guard import stored_table

    declared = declared_sensitive.declared_for(source_type, model.table_name)
    if declared is None or await stored_table(conn, model) is not None:
        return frozenset()
    return declared_sensitive.hide(model.columns, declared)


async def tag_declared(
    conn: "Connection", table_id: int, source_type: str, columns: frozenset[str]
) -> Any:
    """Put the sensitive tag on each of ``columns`` of the table just stored. Answers the
    refusal of the first that could not be tagged, else None."""
    from provisa.api.admin.schema_mutation import store_tag_assignment
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import tag as tag_repo

    if not columns:
        return None
    tag_row = await tag_repo.get(conn, declared_sensitive.TAG)
    assert tag_row is not None, "the built-in sensitive tag is defined in code"
    for column in sorted(columns):
        refused = await store_tag_assignment(
            conn,
            TagAssignment(
                tag_id=declared_sensitive.TAG,
                object_type="column",
                table_id=table_id,
                column_name=column,
                reason=declared_sensitive.reason(source_type),
            ),
            tag_row,
        )
        if refused is not None:
            return refused
    return None
