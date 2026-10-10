# Copyright (c) 2026 Kenneth Stott
# Canary: 15afc4bc-a4b9-45fd-a26a-6d46bd91a531
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an operator sees of replica builds (REQ-1915): each replica's state, how its running or
last build copies, how far a running build has got and at what rate, and its last error."""

# Requirements: REQ-1915

from __future__ import annotations

from datetime import datetime
from typing import Any

from provisa.federation.replica_errors import WAITING
from provisa.federation.replica_state import retry_policy

RETIRED = "retired"


def _iso(at: datetime | None) -> str | None:
    return at.isoformat() if at is not None else None


def _next_attempt_at(record: Any) -> datetime | None:
    if record.build_state != "failed" or record.retired_at is not None or record.failed_at is None:
        return None
    return retry_policy().next_attempt_at(record.failed_at, record.failed_attempts)


def build_view(record: Any, now: datetime) -> dict:
    """One replica's record as the admin query returns it. ``rows_per_second`` is derived for a
    build that is running: the rows copied so far over the time since it started (None before
    its first progress write, and for a build that is not running). A replica the model no
    longer declares is shown as ``retired`` until it is dropped."""
    building = record.build_state == "building" and record.retired_at is None
    rate: float | None = None
    if building and record.build_started_at is not None and record.rows_copied:
        elapsed = (now - record.build_started_at).total_seconds()
        rate = record.rows_copied / elapsed if elapsed > 0 else None
    return {
        "source_id": record.key[0],
        "schema_name": record.key[1],
        "table_name": record.key[2],
        "state": RETIRED if record.retired_at is not None else record.build_state,
        "requested_reason": record.requested_reason,
        "method": record.build_method,
        "load_kind": record.load_kind,
        "started_at": _iso(record.build_started_at),
        "rows_copied": record.rows_copied,
        "rows_per_second": rate,
        "completed_at": _iso(record.completed_at),
        "next_refresh_at": _iso(record.next_refresh_at),
        # REQ-1350: the English text, and the code and params the UI renders in its own
        # language when the cause is one Provisa names.
        "last_error": record.last_error,
        "last_error_code": record.last_error_code,
        "last_error_params": record.last_error_params,
        "failed_attempts": record.failed_attempts,
        # REQ-1915: when a failed build is tried next (its wait grows with each failure in a
        # row). None unless the replica is failed.
        "next_attempt_at": _iso(_next_attempt_at(record)),
        "waiting_on": WAITING[record.waiting_on] if record.waiting_on is not None else None,
        "waiting_on_code": record.waiting_on,
        "feed_down_since": _iso(record.feed_down_since),
        "feed_error": record.feed_error,
        # REQ-874: how the last build refreshed a delta table. ``delta_skipped`` is the declared
        # reason this build was a whole rebuild instead of a delta (delta.SKIP_*), or None when a
        # delta was applied; ``delta_cursor`` is the stored watermark the next delta resumes from.
        "delta_skipped": record.delta_skipped,
        "delta_cursor": record.delta_cursor,
        # What the last completed build had to say of the copy it made: each a code and its
        # particulars, worded by the UI; empty when it had nothing to say.
        "build_notes": record.build_notes,
    }
