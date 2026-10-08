# Copyright (c) 2026 Kenneth Stott
# Canary: 4a27265f-5c9e-4441-8a3f-f7b381aa9992
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each coordinator's system catalogs were last created from (REQ-1429).

One Trino coordinator serves every org, environment and worker of a deployment, and each of them
comes through catalog registration when it boots or builds a runtime. Registration refreshes a
catalog by dropping it first -- Trino cannot read a catalog's properties back -- and every
statement that names the catalog between the drop and the create fails. So a catalog is created
once and created again only when its spec changed, and this is where "the spec it was created
from" is kept: its hash, by coordinator address and catalog name.

The reads and writes are synchronous: registration runs on a boot thread and under the engine's
provisioning connection, off the event loop, holding the deployment's registration lock."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from provisa.core.schema_admin import engine_system_catalogs

if TYPE_CHECKING:
    from provisa.core.database import Database

TABLES = ("engine_system_catalogs",)

_t = engine_system_catalogs.c


class CatalogRecord:
    """The record, through the platform state store's handle."""

    def __init__(self, db: "Database") -> None:
        self._engine = db.engine

    def created_from(self, coordinator: str, name: str) -> str | None:
        """The hash of the spec ``coordinator``'s catalog ``name`` was last created from."""
        with self._engine.connect() as conn:
            row = conn.execute(
                select(_t.spec_hash).where(_t.coordinator == coordinator, _t.name == name)
            ).fetchone()
        return None if row is None else row[0]

    def record(self, coordinator: str, name: str, spec_hash: str) -> None:
        """``coordinator``'s catalog ``name`` was just created from the spec with this hash."""
        with self._engine.begin() as conn:
            conn.execute(
                delete(engine_system_catalogs).where(_t.coordinator == coordinator, _t.name == name)
            )
            conn.execute(
                engine_system_catalogs.insert().values(
                    coordinator=coordinator,
                    name=name,
                    spec_hash=spec_hash,
                    registered_at=datetime.now(UTC),
                )
            )
