# Copyright (c) 2026 Kenneth Stott
# Canary: f6726109-1b33-4948-b4b9-e9947e99b319
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's nearest physical inputs (REQ-2126), for the server's own use.

Read from the control plane's table_inputs view, for the server's own use: a refresh's hold on
its inputs' failing checks. A product's detail finds its inputs on its reader's lineage instead,
cut to that reader's rights (REQ-2156).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SOURCE_TABLE = "source_table"
MATERIALIZED_VIEW = "materialized_view"


@dataclass(frozen=True)
class NearestInput:
    table_id: int
    relation: str  # domain.table, as lineage names it
    kind: str  # SOURCE_TABLE or MATERIALIZED_VIEW
    address: str  # source/schema/table, the form a checker contract names a dataset by


async def nearest_physical_inputs(conn: Any, table_id: int) -> list[NearestInput]:
    """The nearest physical inputs of the registered table ``table_id``: walking its lineage
    upstream through plain views, the first source table or materialized view on each path.
    ``conn`` is a control-plane connection."""
    from sqlalchemy import text

    rows = await conn.execute_core(
        text(
            "SELECT input_table_id, input_relation, input_kind, input_address "
            "FROM table_inputs WHERE table_id = :table_id ORDER BY input_relation"
        ).bindparams(table_id=table_id)
    )
    return [NearestInput(*row) for row in rows.fetchall()]
