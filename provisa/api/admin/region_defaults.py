# Copyright (c) 2026 Kenneth Stott
# Canary: 0b2becf6-f2a9-4987-8b12-467dc2c868aa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The region an object edited through the admin keeps (REQ-1921).

An admin edit rebuilds a table or a source from the form, which carries no region; saved as
built, the edit wrote NULL over the stored region — for a table, moving where its data lives.
"""

# Requirements: REQ-1921

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def kept_region(conn: "Connection", model) -> str | None:
    """The region an edit of a registered table keeps: the stored one. A table's region changes
    only through the configuration; an edit that rebuilt the table from the form wrote NULL over
    it — moving its data (its copies retired wherever it had been kept)."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables as t

    row = (
        await conn.execute_core(
            select(t.c.region).where(
                t.c.source_id == model.source_id,
                t.c.schema_name == model.schema_name,
                t.c.table_name == model.table_name,
            )
        )
    ).fetchone()
    if row is None:
        raise LookupError(
            f"table {model.source_id}/{model.schema_name}.{model.table_name} is not registered"
        )
    return row.region
