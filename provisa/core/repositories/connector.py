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

THE PLATFORM'S OWN BLOCK (REQ-2225). ``platform_connectors.yaml``, in the package, is a block every
organization is offered beside its own. The release build writes it; a checkout's is empty, so
Sources shows nothing of the platform until a release fills it. It is read here and never stored:
it is not the organization's model, so :func:`list_all` leaves it out and it does not promote. An
organization's own connector of the same name keeps that name, so what a release adds never changes
what a source of theirs runs.
"""

# Requirements: REQ-2221, REQ-2225

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from sqlalchemy import select

from provisa.core import model_change
from provisa.core.schema_org import connectors

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.core.models import ConnectorConfig

_BUILD_SETTINGS = ("path", "build_command")
_PLATFORM_FILE = Path(__file__).resolve().parent.parent / "platform_connectors.yaml"


def _row(found: Any) -> dict[str, Any]:
    return dict(found._mapping)


def _values(connector: "ConnectorConfig") -> dict[str, Any]:
    """The connector as a row of the table."""
    values = connector.model_dump(mode="json")
    # Each entry as the block writes it: what its file did not set is not stored as a null.
    for key, entries in (("requires", connector.requires), ("settings", connector.settings)):
        values[key] = [e.model_dump(mode="json", exclude_defaults=True) for e in entries]
    return values


def _platform() -> list[dict[str, Any]]:
    """The platform's connectors, as rows. A block that does not read is a broken package, refused
    by name rather than shown as no connectors."""
    from pydantic import TypeAdapter, ValidationError

    from provisa.core.models import ConnectorConfig

    raw = yaml.safe_load(_PLATFORM_FILE.read_text())
    if not isinstance(raw, dict) or "connectors" not in raw:
        raise ValueError(f"{_PLATFORM_FILE.name} has no connectors: block")
    try:
        entries = TypeAdapter(list[ConnectorConfig]).validate_python(raw["connectors"] or [])
    except ValidationError as exc:
        wrong = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
        raise ValueError(f"{_PLATFORM_FILE.name}: {wrong}") from None
    return [_values(entry) for entry in entries]


async def upsert(conn: "Connection", connector: "ConnectorConfig") -> None:
    """Add the connector, or replace its definition."""
    model_change.name("upsert", "connector", connector.name)
    values = _values(connector)
    await conn.upsert(
        connectors,
        values,
        index_elements=["name"],
        update_columns=[k for k in values if k != "name"],
    )


async def list_all(conn: "Connection") -> list[dict[str, Any]]:
    """Every connector of the organization's model, whole, by name."""
    found = await conn.execute_core(select(connectors).order_by(connectors.c.name))
    return [_row(r) for r in found.fetchall()]


async def get(conn: "Connection", name: str) -> dict[str, Any] | None:
    """The connector ``name``, whole: the organization's own, else the platform's; None when
    neither has that name."""
    found = (
        await conn.execute_core(select(connectors).where(connectors.c.name == name))
    ).fetchone()
    if found is not None:
        return _row(found)
    return next((row for row in _platform() if row["name"] == name), None)


def public(row: dict[str, Any]) -> dict[str, Any]:
    """A connector as the Sources API returns it: never its build settings."""
    return {k: v for k, v in row.items() if k not in _BUILD_SETTINGS}


async def offered(conn: "Connection") -> list[dict[str, Any]]:
    """What Sources lists: the organization's connectors and the platform's, by name, without
    their build settings."""
    own = await list_all(conn)
    names = {row["name"] for row in own}
    both = [*own, *(row for row in _platform() if row["name"] not in names)]
    return [public(row) for row in sorted(both, key=lambda row: row["name"])]
