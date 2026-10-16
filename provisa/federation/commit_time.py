# Copyright (c) 2026 Kenneth Stott
# Canary: 316f4947-f9bc-4649-aa1c-3093901f6c93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The commit time a change-data-capture landing carries (REQ-1163 as amended).

A table kept by change data capture lands, beside its own columns, ``source_committed_at``: when
its row's change was committed at the source, for a view with history to name as its source time.

- A table on the ``debezium`` signal: each change event's ``source.ts_ms``, the database commit
  time (the envelope's top-level ``ts_ms`` is the connector's processing time, not this).
- A PostgreSQL table on the ``native`` signal: PostgreSQL's own commit time of each row,
  ``pg_xact_commit_timestamp(xmin)``, read with the row when its replica is built. PostgreSQL keeps
  it only with ``track_commit_timestamp = on``; without it the column is empty.

The column is a registered column of the table, so every path that lands its rows -- the change
events, the full build, the startup reconcile -- names the same columns. A full build reads it
where the source can give it (PostgreSQL with commit timestamps) and leaves it empty elsewhere: a
row copied whole has no change event to take a commit time from.
"""

# Requirements: REQ-1163, REQ-929, REQ-261

from __future__ import annotations

import logging
import time
from typing import Any

log = logging.getLogger(__name__)

SOURCE_COMMITTED_AT = "source_committed_at"
_DESCRIPTION = "When the row's change was committed at the source"
#: How long a source's answer to whether it keeps commit timestamps is taken as it is.
_PROBE_TTL_S = 300.0
_probed: dict[str, tuple[float, bool]] = {}


def carries(source_type: str | None, signal: str | None) -> bool:
    """Whether a table of a ``source_type`` source on change ``signal`` lands its commit time."""
    from provisa.core.change_signal import PUSH_SOURCE_TYPES

    if signal == "debezium":
        return source_type not in PUSH_SOURCE_TYPES
    return signal == "native" and source_type == "postgresql"


def with_column(columns: list[dict]) -> list[dict]:
    """A carrying table's columns (``name``, ``visible_to``, ``is_primary_key`` each, as a config
    or a save gives them) with ``source_committed_at`` appended where missing, granted as the
    first key column is."""
    if any(c["name"] == SOURCE_COMMITTED_AT for c in columns):
        return columns
    key = next((c for c in columns if c.get("is_primary_key")), None)
    if key is None:
        return columns  # no key: no change can be landed by key, and none is (push_wiring)
    return columns + [
        {
            "name": SOURCE_COMMITTED_AT,
            "data_type": "timestamp",
            "visible_to": list(key.get("visible_to") or []),
            "description": _DESCRIPTION,
        }
    ]


def tracks_commit_time(source: Any) -> bool:
    """Whether a PostgreSQL ``source`` keeps commit timestamps (``track_commit_timestamp``),
    asked of the source at most once per ``_PROBE_TTL_S``. A source that cannot be reached is
    reported as not keeping them, and asked again at the next build, with the reason logged."""
    clock = time.monotonic()
    held = _probed.get(source.id)
    if held is not None and clock - held[0] < _PROBE_TTL_S:
        return held[1]
    from provisa.subscriptions.pg_logical import PgConnection

    try:
        with PgConnection.from_source(source).connect() as conn:
            (value,) = conn.execute("SELECT current_setting('track_commit_timestamp')").fetchone()
    except Exception as exc:  # allow-ble: an unreachable source fails its own build by name; here it only means the column cannot be filled now, and the probe is not remembered
        log.warning("source %s: cannot ask whether it keeps commit timestamps: %s", source.id, exc)
        return False
    keeps = str(value).lower() == "on"
    _probed[source.id] = (clock, keeps)
    return keeps


def source_expression(source: Any) -> str:
    """What a build's read of a carrying table selects as ``source_committed_at``, in the
    source's own SQL: PostgreSQL's commit time of the row when it keeps it, else NULL."""
    if getattr(source, "type", None) is not None and _type(source) == "postgresql":
        if tracks_commit_time(source):
            return f"pg_xact_commit_timestamp(xmin) AS {SOURCE_COMMITTED_AT}"
    return f"CAST(NULL AS TIMESTAMP) AS {SOURCE_COMMITTED_AT}"


def reads_commit_time(source: Any) -> bool:
    """Whether a build of a carrying table of ``source`` reads a commit time from the source --
    the row's own, which a read through another engine's attach of it cannot see."""
    return _type(source) == "postgresql" and tracks_commit_time(source)


def _type(source: Any) -> str:
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)
