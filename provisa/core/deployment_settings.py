# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4e6c15-0b73-4d28-8f51-e2c7d9b3a640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deployment-wide settings changed at runtime (REQ-165, REQ-1900).

`PUT /admin/settings` used to apply a deployment-wide scalar by writing ``os.environ`` in the
worker process that served the request. The other workers of the launch and every other instance
never saw the change, and a restart lost it.

A setting changed at runtime is now a row in the platform control plane (``deployment_settings``:
key, JSON value) — the one store every worker and instance reaches. Readers do not query it per
call: they resolve against a snapshot of the rows held in the process and refreshed at most once
per ``SNAPSHOT_TTL_SECONDS``, the same shape the debug-trace windows use
(``provisa/core/trace_scope.py``). So a change made through one worker is in force on every
other within that many seconds, on the worker that made it at once, and after a restart because
it never lived in a process.

PRECEDENCE, for a reader of one of these settings: the stored row, when there is one; else what
the process was started with (its environment variable, then the config file) — a stored row is
an operator's later, explicit change to exactly that.

A process that has not bound a control plane (a script, a unit test compiling a query) has no
stored settings and reads only what it was started with.
"""

# Requirements: REQ-165, REQ-1900

from __future__ import annotations

import json
import threading
import time
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from provisa.core.schema_admin import deployment_settings as _table

if TYPE_CHECKING:
    from provisa.core.database import Database

# How stale a reader's snapshot may be: a setting changed on another worker or instance takes
# effect here within this many seconds.
SNAPSHOT_TTL_SECONDS = 5.0

_monotonic = time.monotonic

_db: "Database | None" = None
# (when it was read, the rows).
_held: tuple[float, dict[str, Any]] | None = None
_refreshing = threading.Lock()


def bind(db: "Database") -> None:
    """Name the platform control plane this process reads its deployment settings from."""
    global _db, _held
    _db = db
    _held = None


def _load(db: "Database") -> dict[str, Any]:
    with db.engine.connect() as conn:
        rows = conn.execute(select(_table.c.key, _table.c.value)).fetchall()
    return {key: json.loads(value) for key, value in rows}


def _snapshot() -> dict[str, Any]:
    global _held
    db = _db
    if db is None:
        return {}
    held = _held
    if held is not None and _monotonic() - held[0] < SNAPSHOT_TTL_SECONDS:
        return held[1]
    # One thread refreshes; the others keep the snapshot they have rather than queueing on the
    # control plane. A thread with nothing to serve reads for itself.
    owns_refresh = _refreshing.acquire(blocking=False)
    if held is not None and not owns_refresh:
        return held[1]
    try:
        rows = _load(db)
        _held = (_monotonic(), rows)
        return rows
    finally:
        if owns_refresh:
            _refreshing.release()


def get(key: str) -> Any | None:
    """The stored value of ``key``, or ``None`` when no runtime change has been made to it."""
    return _snapshot().get(key)


def write(db: "Database", values: dict[str, Any], *, updated_by: str) -> None:
    """Store ``values`` (key -> JSON value), in one transaction. The writing process sees them at
    once; every other within ``SNAPSHOT_TTL_SECONDS``.

    A value of ``None`` CLEARS the setting: its row is removed and nothing is written in its
    place (REQ-1913) — ``get`` reads "no row" as "no stored value", so a JSON null is never stored.
    """
    global _held
    with db.engine.begin() as conn:
        for key, value in values.items():
            conn.execute(delete(_table).where(_table.c.key == key))
            if value is None:
                continue
            conn.execute(
                _table.insert().values(key=key, value=json.dumps(value), updated_by=updated_by)
            )
    _held = None


# --- the settings that used to be process environment writes -------------------------------------


def auto_track_fk() -> bool:
    """Whether registering a table also registers its foreign keys as relationships."""
    from provisa.core import settings_registry  # REQ-1913: declared in settings_catalog

    return settings_registry.value("relationships.auto_track_fk")
