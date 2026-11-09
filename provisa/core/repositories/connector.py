# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3d6e14-7a2f-48c1-b5e0-3f81c2d7a946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The connectors an organization offers, in its model store (REQ-2221).

The ``connectors:`` block of a configuration, held in a table of the model that promotes with it,
so the model's branches decide where a version runs. A row carries the block's build settings
(``path``, ``build_command``) so a promoted block reads back whole; the server acts on none of
them, and the Sources API projects them away (:func:`offered`).
"""

# Requirements: REQ-2221

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core import model_change
from provisa.core.schema_org import connectors

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.core.models import ConnectorConfig

_BUILD_SETTINGS = ("path", "build_command")


def _row(found: Any) -> dict[str, Any]:
    return dict(found._mapping)


async def upsert(conn: "Connection", connector: "ConnectorConfig") -> None:
    """Add the connector, or replace its definition."""
    model_change.name("upsert", "connector", connector.name)
    values = connector.model_dump(mode="json")
    await conn.upsert(
        connectors,
        values,
        index_elements=["name"],
        update_columns=[k for k in values if k != "name"],
    )


async def list_all(conn: "Connection") -> list[dict[str, Any]]:
    """Every connector, whole, by name."""
    found = await conn.execute_core(select(connectors).order_by(connectors.c.name))
    return [_row(r) for r in found.fetchall()]


async def get(conn: "Connection", name: str) -> dict[str, Any] | None:
    """The connector ``name``, whole; None when the model has none of that name."""
    found = (
        await conn.execute_core(select(connectors).where(connectors.c.name == name))
    ).fetchone()
    return None if found is None else _row(found)


def public(row: dict[str, Any]) -> dict[str, Any]:
    """A connector as the Sources API returns it: never its build settings."""
    return {k: v for k, v in row.items() if k not in _BUILD_SETTINGS}


async def offered(conn: "Connection") -> list[dict[str, Any]]:
    """What Sources lists: every connector, without its build settings."""
    return [public(row) for row in await list_all(conn)]
