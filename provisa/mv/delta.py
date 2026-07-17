# Copyright (c) 2026 Kenneth Stott
# Canary: 4dd10510-55c3-4a5b-84bb-7f352cb5b686
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row-level MV delta capture + point-in-time reconstruction (REQ-877, REQ-878).

REQ-877 — opt-in per-MV ROW-LEVEL delta capture. On a refresh, the prior landed row set and the
freshly landed row set are diffed BY KEY into insert/update/delete events and appended to the
store's append-only ``mv_delta_ledger`` (store-independent — never stamped on the target table, the
same principle as ``mv_refresh_log``). Change detection is a row hash over the projection with
``delta_exclude_columns`` removed, so a change to only an ignored (volatile) column is not reported.
Each event carries the row VALUES (value-delta tier), which is what makes full-content reconstruction
possible for an RDB target that lacks native time-travel.

REQ-878 — point-in-time reconstruction folds the ledger to rebuild the view as-of a refresh version
N. The ledger is the temporal substrate; two fold DIRECTIONS reach the same as-of-N result:

  * FORWARD-FOLD (from base): the greatest event per key with ``refresh_version <= N``, dropping keys
    whose last such event is a delete. This is REQ-878's "windowed greatest-per-key query" — executed
    in the store, never a Python row replay.
  * REVERSE (from live): start from the live row set and correct only the keys touched AFTER N back to
    their as-of-N state — cheaper when few keys changed since N. Yields the identical as-of-N set.

Reconstruct against an unknown version fails loud (never a silent empty result).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import and_, distinct, func, insert, select

from provisa.core.schema_org import mv_delta_ledger as _ledger

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.mv.models import MVDefinition

_INSERT = "insert"
_UPDATE = "update"
_DELETE = "delete"


@dataclass(frozen=True)
class DeltaEvent:
    """One row-level change between two refresh snapshots."""

    change_type: str
    row_key: str
    old_hash: str | None
    new_hash: str | None
    old_values: dict | None
    new_values: dict | None


def _canonical(row: dict, exclude: frozenset[str]) -> str:
    projected = {k: v for k, v in row.items() if k not in exclude}
    return json.dumps(projected, sort_keys=True, default=str)


def _row_hash(row: dict, exclude: frozenset[str]) -> str:
    # md5 is a change-detection digest here, not a security primitive.
    return hashlib.md5(_canonical(row, exclude).encode(), usedforsecurity=False).hexdigest()


def _key_str(row: dict, key_cols: list[str]) -> str:
    # KeyError here is intentional: a row missing a declared delta_key column is a loud upstream
    # contract violation, never silently keyed on a partial identity.
    return json.dumps({k: row[k] for k in key_cols}, sort_keys=True, default=str)


def compute_deltas(
    prev: list[dict], curr: list[dict], key_cols: list[str], exclude: frozenset[str]
) -> list[DeltaEvent]:
    """Diff two row snapshots BY KEY into insert/update/delete events (hash over the non-excluded
    projection). A key present only in ``curr`` is an insert; only in ``prev`` a delete; in both with
    a differing hash an update; an unchanged hash yields no event."""
    prev_by = {_key_str(r, key_cols): r for r in prev}
    curr_by = {_key_str(r, key_cols): r for r in curr}
    events: list[DeltaEvent] = []
    for key, row in curr_by.items():
        new_hash = _row_hash(row, exclude)
        if key not in prev_by:
            events.append(DeltaEvent(_INSERT, key, None, new_hash, None, dict(row)))
            continue
        old_hash = _row_hash(prev_by[key], exclude)
        if old_hash != new_hash:
            events.append(
                DeltaEvent(_UPDATE, key, old_hash, new_hash, dict(prev_by[key]), dict(row))
            )
    for key, row in prev_by.items():
        if key not in curr_by:
            events.append(DeltaEvent(_DELETE, key, _row_hash(row, exclude), None, dict(row), None))
    return events


async def _next_version(store: Database, mv_id: str) -> int:
    async with store.acquire() as conn:
        result = await conn.execute_core(
            select(func.max(_ledger.c.refresh_version)).where(_ledger.c.mv_id == mv_id)
        )
        row = result.fetchone()
    current = row[0] if row is not None and row[0] is not None else 0
    return current + 1


async def capture_row_deltas(
    store: Database,
    mv: MVDefinition,
    prev_rows: list[dict],
    curr_rows: list[dict],
    *,
    definition_version: str | None = None,
    trace_id: str | None = None,
) -> int | None:
    """Diff ``prev_rows`` → ``curr_rows`` and APPEND the change events to the ledger under the next
    refresh version for this MV. No-op returning ``None`` when the MV did not opt in. A refresh that
    changed no rows records no events and does not consume a version number (its as-of state equals
    the prior version). Fails loud when opted in without a ``delta_key`` (no identity ⇒ no delta)."""
    if not mv.capture_row_deltas:
        return None
    if not mv.delta_key:
        raise ValueError(f"MV {mv.id}: capture_row_deltas requires a non-empty delta_key")
    exclude = frozenset(mv.delta_exclude_columns)
    events = compute_deltas(prev_rows, curr_rows, mv.delta_key, exclude)
    version = await _next_version(store, mv.id)
    if not events:
        return version
    payload = [
        {
            "mv_id": mv.id,
            "refresh_version": version,
            "definition_version": definition_version,
            "trace_id": trace_id,
            "change_type": e.change_type,
            "row_key": e.row_key,
            "old_hash": e.old_hash,
            "new_hash": e.new_hash,
            "old_values": e.old_values,
            "new_values": e.new_values,
        }
        for e in events
    ]
    async with store.acquire() as conn:
        await conn.execute_core(insert(_ledger).values(payload))
    return version


async def _known_versions(store: Database, mv_id: str) -> set[int]:
    async with store.acquire() as conn:
        result = await conn.execute_core(
            select(distinct(_ledger.c.refresh_version)).where(_ledger.c.mv_id == mv_id)
        )
        rows = result.fetchall()
    return {r[0] for r in rows}


async def _assert_known(store: Database, mv_id: str, version: int) -> None:
    if version not in await _known_versions(store, mv_id):
        raise ValueError(
            f"MV {mv_id}: cannot reconstruct as-of refresh version {version} — "
            f"no such version in the delta ledger"
        )


async def _forward_state(store: Database, mv_id: str, version: int) -> dict[str, dict]:
    """As-of-N state per key: the greatest event with ``refresh_version <= N`` per key, executed in
    the store as a windowed greatest-per-key query; keys whose last such event is a delete are absent.
    Returns ``{row_key: new_values}``."""
    ranked = (
        select(
            _ledger.c.row_key,
            _ledger.c.change_type,
            _ledger.c.new_values,
            func.row_number()
            .over(
                partition_by=_ledger.c.row_key,
                order_by=(_ledger.c.refresh_version.desc(), _ledger.c.id.desc()),
            )
            .label("rn"),
        )
        .where(and_(_ledger.c.mv_id == mv_id, _ledger.c.refresh_version <= version))
        .subquery()
    )
    stmt = select(ranked.c.row_key, ranked.c.change_type, ranked.c.new_values).where(
        ranked.c.rn == 1
    )
    async with store.acquire() as conn:
        result = await conn.execute_core(stmt)
        rows = result.fetchall()
    return {
        r._mapping["row_key"]: r._mapping["new_values"]
        for r in rows
        if r._mapping["change_type"] != _DELETE
    }


async def _keys_touched_after(store: Database, mv_id: str, version: int) -> set[str]:
    async with store.acquire() as conn:
        result = await conn.execute_core(
            select(distinct(_ledger.c.row_key)).where(
                and_(_ledger.c.mv_id == mv_id, _ledger.c.refresh_version > version)
            )
        )
        rows = result.fetchall()
    return {r[0] for r in rows}


async def reconstruct_forward(store: Database, mv: MVDefinition, version: int) -> list[dict]:
    """FORWARD-FOLD reconstruction (REQ-878): the view as-of refresh ``version``, folded from the
    ledger base. Fails loud on an unknown version."""
    await _assert_known(store, mv.id, version)
    state = await _forward_state(store, mv.id, version)
    return list(state.values())


async def reconstruct_reverse(
    store: Database, mv: MVDefinition, version: int, live_rows: list[dict]
) -> list[dict]:
    """REVERSE reconstruction (REQ-878): the view as-of refresh ``version``, derived from the LIVE
    row set by correcting only the keys touched after ``version`` back to their as-of-N state. Yields
    the identical set as ``reconstruct_forward``. Fails loud on an unknown version."""
    await _assert_known(store, mv.id, version)
    touched = await _keys_touched_after(store, mv.id, version)
    asof = await _forward_state(store, mv.id, version)
    live_keys = {_key_str(r, mv.delta_key) for r in live_rows}
    out: list[dict] = []
    for row in live_rows:
        key = _key_str(row, mv.delta_key)
        if key not in touched:
            out.append(dict(row))  # unchanged since N — the live row is the as-of-N row
        elif key in asof:
            out.append(asof[key])  # updated after N — restore its as-of-N value
        # else: inserted after N (no as-of-N state) — drop it
    # keys DELETED after N: present at N (in asof) and touched, but absent from live — resurrect them.
    for key, values in asof.items():
        if key in touched and key not in live_keys:
            out.append(values)
    return out
