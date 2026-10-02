# Copyright (c) 2026 Kenneth Stott
# Canary: 81863426-3643-4136-9a09-35207da43934
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The control plane's record of each replica (REQ-1912, REQ-826).

``replica_state`` holds one row per replica of a source table, keyed by the table's registered
identity ``(source_id, schema_name, table_name)``. It is the state every worker and instance
shares: a table promoted in one process is promoted for all of them.

This module owns the PROMOTED flag — the automatic-promotion decision. The replica build's own
state lives on the same row and is written by the build.
"""

# Requirements: REQ-1912, REQ-826, REQ-238

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_org import replica_state

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: A replica's key: the registered identity of its table.
ReplicaKey = tuple[str, str, str]


async def set_promoted(conn: "Connection", key: ReplicaKey, promoted: bool) -> None:
    """Record that the table ``key`` is (or is no longer) promoted. The one write site of the
    promoted flag."""
    source_id, schema_name, table_name = key
    values = {
        "source_id": source_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "promoted": promoted,
    }
    if promoted:
        values["promoted_at"] = datetime.now(UTC)
    await conn.upsert(
        replica_state,
        values,
        index_elements=["source_id", "schema_name", "table_name"],
    )


async def promoted_keys(conn: "Connection") -> frozenset[ReplicaKey]:
    """The tables currently promoted."""
    result = await conn.execute_core(
        select(
            replica_state.c.source_id, replica_state.c.schema_name, replica_state.c.table_name
        ).where(replica_state.c.promoted.is_(True))
    )
    return frozenset((r[0], r[1], r[2]) for r in result.fetchall())
