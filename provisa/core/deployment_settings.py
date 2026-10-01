# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4e6c15-0b73-4d28-8f51-e2c7d9b3a640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deployment-wide settings changed at runtime (REQ-165, REQ-1900, REQ-1914).

`PUT /admin/settings` used to apply a deployment-wide scalar by writing ``os.environ`` in the
worker process that served the request. The other workers of the launch and every other instance
never saw the change, and a restart lost it.

A setting changed at runtime is now a row in the platform control plane (``deployment_settings``:
key, JSON value) — the one store every worker and instance reaches. Readers never query it: they
resolve against a snapshot of the rows held in the process. REQ-1914: the snapshot is loaded
together with the platform plane's ``settings`` config stamp, and the process's config watcher
(``provisa/core/config_watch.py``) reloads it when the stored stamp differs. So a change made
through one worker is in force on every other within the reload interval
(``config.reload_interval``), on the worker that made it at once, and after a restart because it
never lived in a process.

PRECEDENCE, for a reader of one of these settings: the stored row, when there is one; else what
the process was started with (its environment variable, then the config file) — a stored row is
an operator's later, explicit change to exactly that.

A process that has not bound a control plane (a script, a unit test compiling a query) has no
stored settings and reads only what it was started with.
"""

# Requirements: REQ-165, REQ-1900, REQ-1914

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from provisa.core import config_stamp
from provisa.core.schema_admin import deployment_settings as _table

if TYPE_CHECKING:
    from provisa.core.database import Database

_db: "Database | None" = None
# (the platform plane's ``settings`` stamp read just before the rows, the rows).
_held: tuple[int, dict[str, Any]] | None = None
_loading = threading.Lock()


def bind(db: "Database") -> None:
    """Name the platform control plane this process reads its deployment settings from."""
    global _db, _held
    _db = db
    _held = None


def _load(db: "Database") -> tuple[int, dict[str, Any]]:
    """The stamp, then the rows. In that order: a change stored between the two reads leaves the
    snapshot holding the older stamp, so the watcher reloads it once more — never the reverse,
    newer stamp over older rows, which nothing would correct."""
    stamp = config_stamp.read_sync(db)[config_stamp.SETTINGS]
    with db.engine.connect() as conn:
        rows = conn.execute(select(_table.c.key, _table.c.value)).fetchall()
    return stamp, {key: json.loads(value) for key, value in rows}


def _snapshot() -> dict[str, Any]:
    global _held
    db = _db
    if db is None:
        return {}
    held = _held
    if held is not None:
        return held[1]
    # Nothing loaded yet (first read after binding, or after this process's own write).
    with _loading:
        if _held is None:
            _held = _load(db)
        return _held[1]


def loaded_stamp() -> int | None:
    """The ``settings`` stamp this process's snapshot was loaded at; ``None`` when none is held."""
    held = _held
    return None if held is None else held[0]


def current_stamp() -> int | None:
    """:func:`loaded_stamp` of the snapshot a reader would get now, loading it if this process
    holds none (it dropped its own on a write). ``None`` only when no control plane is bound."""
    _snapshot()
    return loaded_stamp()


def reload() -> None:
    """Read the stored rows again (the config watcher, on a changed stamp)."""
    global _held
    db = _db
    if db is None:
        raise RuntimeError("no control plane is bound: there are no deployment settings to reload")
    with _loading:
        _held = _load(db)


def get(key: str) -> Any | None:
    """The stored value of ``key``, or ``None`` when no runtime change has been made to it."""
    return _snapshot().get(key)


def write(db: "Database", values: dict[str, Any], *, updated_by: str) -> None:
    """Store ``values`` (key -> JSON value), in one transaction — the one that also advances the
    platform plane's ``settings`` stamp (by trigger). The writing process sees them at once; every
    other when its config watcher next reads the stamp.

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
