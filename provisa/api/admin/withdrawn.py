# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What is withdrawn, and who may see it (REQ-1921).

A table or source is withdrawn by one action and put back by another; while it is withdrawn it
serves no role and the lists that draw the model (the ERD's tables and relationships, the graph
schema) leave it out, and its relationships with it -- a relationship is withdrawn while either
end (or its junction) is withdrawn or retired, a derived state stored nowhere. Each such list
takes an include-withdrawn flag, honoured only for a person who may preview the withdrawn table:
one holding table registration reaching its domain, the right a draft's preview needs.
"""

# Requirements: REQ-1921

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The right whose reach decides who previews a withdrawn table (as a draft's preview).
PREVIEW_RIGHT = "table_registration"


async def withdrawn_table_domains(conn: "Connection") -> dict[int, str]:
    """Each withdrawn table in service (itself, or through its source), with its domain."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources as src

    rows = await conn.execute_core(
        select(rt.c.id, rt.c.domain_id).where(
            rt.c.retired.is_(False),
            or_(
                rt.c.withdrawn.is_(True),
                rt.c.source_id.in_(select(src.c.id).where(src.c.withdrawn.is_(True))),
            ),
        )
    )
    return {int(r.id): str(r.domain_id) for r in rows.fetchall()}


async def retired_table_ids(conn: "Connection") -> frozenset[int]:
    """The retired tables: an end retired withdraws a relationship as an end withdrawn does."""
    from provisa.core.schema_org import registered_tables as rt

    rows = await conn.execute_core(select(rt.c.id).where(rt.c.retired.is_(True)))
    return frozenset(int(r.id) for r in rows.fetchall())


def preview_reach(info: Any) -> frozenset[str] | None:
    """The domains whose withdrawn tables the GraphQL caller may preview: None for every domain,
    empty for none (capabilities.right_reach of :data:`PREVIEW_RIGHT`)."""
    from provisa.api.admin.capabilities import _identity_from_info, right_reach
    from provisa.api.app import state

    return right_reach(_identity_from_info(info), state, PREVIEW_RIGHT)


def previews(reach: frozenset[str] | None, domain_id: str) -> bool:
    """Whether a caller of ``reach`` (:func:`preview_reach`) may preview a table of ``domain_id``."""
    from provisa.security.rights import reaches_domain

    return reach is None or (bool(reach) and reaches_domain(sorted(reach), domain_id))


def edge_ends(row: Any) -> tuple[int, ...]:
    """The tables a relationship row joins: its two ends and its junction, when it has one."""
    ends = (row["source_table_id"], row["target_table_id"], row["via_table_id"])
    return tuple(int(t) for t in ends if t is not None)
