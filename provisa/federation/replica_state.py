# Copyright (c) 2026 Kenneth Stott
# Canary: 81863426-3643-4136-9a09-35207da43934
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The state store's record of each replica (REQ-1920, REQ-1912, REQ-826, REQ-1915).

``replica_state`` holds one row per replica of a source table, keyed by the table's registered
identity ``(source_id, schema_name, table_name)``. It is STATE, not model: nothing declares it,
the runtime derives it while making the estate match the model, and every worker and node shares
it. A table promoted in one process is promoted for all of them; a build started on one node is
seen as running by every other.

This module is the state store for that table: the one place it is read and written. The build
runner, the read path, the admin panel and the triggers ask this module; a test fails the build
when anything else writes the table.

Two things live on the row:

- the PROMOTED flag, the automatic-promotion decision;
- the replica's BUILD: whether one is requested, running or failed, how far the running one has
  got, when the replica was last completed, when its next refresh is due, the last error and
  which process is building it. A build is REQUESTED by writing the row; nothing that requests
  a build copies anything. A second request for a replica whose build is already requested or
  running changes nothing: it joins that build. A replica EXISTS when ``completed_at`` is set,
  whatever ``build_state`` says, so a refresh that is requested, running or failed leaves the
  previous replica readable.

Because it is state:

- It is derived and discardable. Emptying the table loses no information. What it costs: every
  replica is requested again by the boot request and rebuilt once (the store's copy is not
  trusted without its record), a promoted table is read live until it crosses its threshold
  again, and each rebuilt replica ripples once to what depends on it because its content hash
  is gone. Nothing may depend on a row surviving: a missing row means "no replica, no build".
- A change to it never causes a model reload. Other processes learn of it through the
  replica-state stamp, which is separate from the model stamp.
- It is not part of a configuration export and is not copied to another environment
  (``provisa.core.env_classes`` classifies the table ``NEVER_RUNTIME``: never copied, it belongs
  to the environment that produced it).
"""

# Requirements: REQ-1920, REQ-1912, REQ-826, REQ-238, REQ-1915

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError

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


# -- the replica's build (REQ-1915) ----------------------------------------------------------

IDLE = "idle"
REQUESTED = "requested"
BUILDING = "building"
FAILED = "failed"

# Why a build was requested.
REASON_MODEL = "model"  # the model declares a replica that has no completed build
REASON_DEFINITION = "definition"  # the table's definition changed since the last build
REASON_HOT = "hot"
REASON_REFRESH = "refresh"
REASON_OPERATOR = "operator"
REASON_READ = "read"
REASONS = (
    REASON_MODEL,
    REASON_DEFINITION,
    REASON_HOT,
    REASON_REFRESH,
    REASON_OPERATOR,
    REASON_READ,
)

_t = replica_state.c


@dataclass(frozen=True)
class ReplicaRecord:
    """One replica's row, as read."""

    key: ReplicaKey
    build_state: str
    requested_at: datetime | None
    requested_reason: str | None
    build_started_at: datetime | None
    build_holder: str | None
    build_method: str | None
    rows_copied: int | None
    completed_at: datetime | None
    next_refresh_at: datetime | None
    content_hash: str | None
    built_store: str | None
    last_error: str | None
    failed_at: datetime | None
    waiting_on: str | None
    definition_hash: str | None
    built_columns: list | None
    model_stamp: int | None
    load_kind: str | None
    retired_at: datetime | None
    last_error_code: str | None = None
    last_error_params: dict | None = None
    failed_attempts: int = 0

    @property
    def exists(self) -> bool:
        """Whether a build of this replica has ever completed, in any store."""
        return self.completed_at is not None

    def exists_in(self, store: str) -> bool:
        """Whether there is a replica to read in the store ``store`` identifies. A record is one
        per table, not per engine: a replica built in another engine's store (the deployment
        was moved to a different engine or store) is not one this engine can read."""
        return self.completed_at is not None and self.built_store == store


_COLUMNS = (
    _t.source_id,
    _t.schema_name,
    _t.table_name,
    _t.build_state,
    _t.requested_at,
    _t.requested_reason,
    _t.build_started_at,
    _t.build_holder,
    _t.build_method,
    _t.rows_copied,
    _t.completed_at,
    _t.next_refresh_at,
    _t.content_hash,
    _t.built_store,
    _t.last_error,
    _t.failed_at,
    _t.waiting_on,
    _t.definition_hash,
    _t.built_columns,
    _t.model_stamp,
    _t.load_kind,
    _t.retired_at,
    _t.last_error_code,
    _t.last_error_params,
    _t.failed_attempts,
)


def _aware(value: datetime | None) -> datetime | None:
    # A control plane without a timezone-aware column type (SQLite) hands back a naive value;
    # every stamp here is written in UTC.
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _record(row: Any) -> ReplicaRecord:
    return ReplicaRecord(
        key=(row[0], row[1], row[2]),
        build_state=row[3],
        requested_at=_aware(row[4]),
        requested_reason=row[5],
        build_started_at=_aware(row[6]),
        build_holder=row[7],
        build_method=row[8],
        rows_copied=row[9],
        completed_at=_aware(row[10]),
        next_refresh_at=_aware(row[11]),
        content_hash=row[12],
        built_store=row[13],
        last_error=row[14],
        failed_at=_aware(row[15]),
        waiting_on=row[16],
        definition_hash=row[17],
        built_columns=row[18],
        model_stamp=row[19],
        load_kind=row[20],
        retired_at=_aware(row[21]),
        last_error_code=row[22],
        last_error_params=row[23],
        failed_attempts=row[24],
    )


def _is(key: ReplicaKey) -> Any:
    return and_(_t.source_id == key[0], _t.schema_name == key[1], _t.table_name == key[2])


class ReplicaBuilding(TimeoutError):
    """A read's deadline passed while the replica it needs was still being built."""

    def __init__(self, replica: str, running_for: float) -> None:
        self.replica = replica
        self.running_for = running_for
        super().__init__(
            f"the replica of {replica} is still being built (running for {running_for:.0f}s); "
            "the build continues, and a read succeeds once it has finished"
        )


class ReplicaBuildFailed(RuntimeError):
    """The build of a replica a read needs failed: the read fails, it never reads what the
    failed build left standing (REQ-1661)."""

    def __init__(self, replica: str, error: str | None) -> None:
        self.replica = replica
        self.error = error
        super().__init__(f"the replica of {replica} could not be built: {error}")


async def read(conn: "Connection", key: ReplicaKey) -> ReplicaRecord | None:
    """The record of the replica ``key``, or None when it has none."""
    row = (await conn.execute_core(select(*_COLUMNS).where(_is(key)))).fetchone()
    return _record(row) if row is not None else None


async def read_all(conn: "Connection") -> list[ReplicaRecord]:
    """Every replica's record."""
    return [_record(r) for r in (await conn.execute_core(select(*_COLUMNS))).fetchall()]


async def request_build(
    conn: "Connection",
    key: ReplicaKey,
    reason: str,
    *,
    retry_interval: float | None = None,
    model_stamp: int | None = None,
    now: datetime | None = None,
) -> bool:
    """Ask for a build of the replica ``key``. True when this call made the request; False when
    a build was already requested or running (the caller has joined it), when
    ``retry_interval`` holds it back, or when the replica is retired (the model no longer
    declares it: a retired replica is never rebuilt).

    ``model_stamp`` is the model stamp the asking process has loaded, recorded so that a node
    with an older model does not take the replica for one whose table is gone.

    ``retry_interval`` (seconds) is given by a caller that must not ask again too soon after a
    failure — a read: a build that failed less than that long ago is not requested again."""
    if reason not in REASONS:
        raise ValueError(f"unknown build reason {reason!r}; expected one of {REASONS}")
    at = now if now is not None else datetime.now(UTC)
    retriable = _t.build_state == FAILED
    if retry_interval is not None:
        retriable = and_(
            retriable,
            or_(_t.failed_at.is_(None), _t.failed_at <= at - timedelta(seconds=retry_interval)),
        )
    requested: dict[str, Any] = {
        "build_state": REQUESTED,
        "requested_at": at,
        "requested_reason": reason,
    }
    if model_stamp is not None:
        requested["model_stamp"] = model_stamp
    result = await conn.execute_core(
        update(replica_state)
        .where(_is(key), _t.retired_at.is_(None), or_(_t.build_state == IDLE, retriable))
        .values(**requested, waiting_on=None)
    )
    if (result.rowcount or 0) > 0:
        return True
    if (await conn.execute_core(select(_t.build_state).where(_is(key)))).fetchone() is not None:
        return False  # requested, building, failed too recently to ask again, or retired
    try:
        await conn.execute_core(
            replica_state.insert().values(
                source_id=key[0], schema_name=key[1], table_name=key[2], **requested
            )
        )
    except IntegrityError:
        return False  # another caller created the row in the same moment: its request stands
    return True


def _claimable(now: datetime, retry_interval: float) -> Any:
    """A record a runner may build: not retired, and requested; idle with a refresh due;
    building (whose builder may have died — only the runner that gets the replica's lock finds
    out); or failed at least ``retry_interval`` seconds ago (the runner tries a failed build
    again itself: a replica nobody reads would otherwise stay failed)."""
    due = and_(_t.build_state == IDLE, _t.next_refresh_at.is_not(None), _t.next_refresh_at <= now)
    retry = and_(
        _t.build_state == FAILED,
        _t.failed_at.is_not(None),
        _t.failed_at <= now - timedelta(seconds=retry_interval),
    )
    return and_(_t.retired_at.is_(None), or_(_t.build_state.in_((REQUESTED, BUILDING)), due, retry))


async def candidates(
    conn: "Connection", *, now: datetime, limit: int, retry_interval: float
) -> list[ReplicaKey]:
    """The replicas a runner may try to build (:func:`_claimable`), oldest request first."""
    result = await conn.execute_core(
        select(_t.source_id, _t.schema_name, _t.table_name)
        .where(_claimable(now, retry_interval))
        .order_by(_t.requested_at.asc().nulls_last(), _t.next_refresh_at.asc())
        .limit(limit)
    )
    return [(r[0], r[1], r[2]) for r in result.fetchall()]


async def claim(
    conn: "Connection", key: ReplicaKey, *, holder: str, now: datetime, retry_interval: float
) -> bool:
    """Move the row to ``building`` for ``holder``. Called only by the process that holds the
    replica's lock; False when the row is no longer a candidate (another runner completed it
    between this runner's selection and its lock, or the model stopped declaring it)."""
    result = await conn.execute_core(
        update(replica_state)
        .where(_is(key), _claimable(now, retry_interval))
        .values(
            build_state=BUILDING,
            build_started_at=now,
            build_holder=holder,
            rows_copied=0,
            waiting_on=None,
        )
    )
    return (result.rowcount or 0) > 0


async def record_started(
    conn: "Connection", key: ReplicaKey, *, method: str, load_kind: str
) -> None:
    """How the running build copies (its method, and whether the store takes a bulk stream or
    a row copy), written when the build starts so an operator sees it while it runs."""
    await conn.execute_core(
        update(replica_state)
        .where(_is(key), _t.build_state == BUILDING)
        .values(build_method=method, load_kind=load_kind)
    )


async def unclaim(conn: "Connection", key: ReplicaKey, *, waiting_on: str) -> None:
    """Put a claimed row back to ``requested`` because the build could not start, saying why."""
    await conn.execute_core(
        update(replica_state)
        .where(_is(key), _t.build_state == BUILDING)
        .values(build_state=REQUESTED, build_holder=None, waiting_on=waiting_on)
    )


async def set_waiting(conn: "Connection", keys: list[ReplicaKey], *, waiting_on: str) -> None:
    """Record why the requested builds ``keys`` were not started on this pass."""
    for key in keys:
        await conn.execute_core(
            update(replica_state)
            .where(_is(key), _t.build_state == REQUESTED)
            .values(waiting_on=waiting_on)
        )


async def record_progress(conn: "Connection", key: ReplicaKey, *, rows_copied: int) -> None:
    """The running build's rows copied so far."""
    await conn.execute_core(
        update(replica_state)
        .where(_is(key), _t.build_state == BUILDING)
        .values(rows_copied=rows_copied)
    )


async def record_completed(
    conn: "Connection",
    key: ReplicaKey,
    *,
    rows_copied: int,
    method: str,
    content_hash: str | None,
    store: str,
    next_refresh_at: datetime | None,
    now: datetime,
    definition_hash: str | None = None,
    built_columns: list | None = None,
) -> None:
    """The build finished and its table was swapped in, in the store ``store`` identifies.
    ``definition_hash`` and ``built_columns`` say what it was built from and which columns it
    has (None from a caller that does not track them: the next convergence asks again)."""
    # CALL SITE (replica-layout-2, Job 3): ``await mark_first_completion(conn, key)`` goes
    # here, in this function's transaction — it bumps the REPLICA stamp for a promoted row's
    # first completion. This function never touches the stamp itself.
    await conn.execute_core(
        update(replica_state)
        .where(_is(key))
        .values(
            build_state=IDLE,
            build_holder=None,
            rows_copied=rows_copied,
            build_method=method,
            completed_at=now,
            next_refresh_at=next_refresh_at,
            content_hash=content_hash,
            built_store=store,
            definition_hash=definition_hash,
            built_columns=built_columns,
            last_error=None,
            last_error_code=None,
            last_error_params=None,
            failed_at=None,
            failed_attempts=0,
            waiting_on=None,
        )
    )


async def record_failed(
    conn: "Connection",
    key: ReplicaKey,
    *,
    error: str,
    now: datetime,
    code: str | None = None,
    params: dict | None = None,
) -> None:
    """The build failed. The previous replica, if any, is untouched and stays readable.
    ``code`` and ``params`` name a cause Provisa knows (``replica_errors``). The count of
    failures in a row goes up by one; a completed build puts it back to none."""
    await conn.execute_core(
        update(replica_state)
        .where(_is(key))
        .values(
            build_state=FAILED,
            build_holder=None,
            last_error=error,
            last_error_code=code,
            last_error_params=params,
            failed_at=now,
            failed_attempts=_t.failed_attempts + 1,
        )
    )


# -- retiring a replica the model no longer declares (REQ-1915, REQ-1919) ----------------------


async def retire(conn: "Connection", key: ReplicaKey, *, now: datetime) -> bool:
    """Mark the replica ``key`` retired: the model no longer declares it. Nothing is dropped;
    the record is no longer built or refreshed. True when this call retired it."""
    result = await conn.execute_core(
        update(replica_state).where(_is(key), _t.retired_at.is_(None)).values(retired_at=now)
    )
    return (result.rowcount or 0) > 0


async def unretire(conn: "Connection", key: ReplicaKey) -> bool:
    """The model declares the replica ``key`` again before it was dropped: it keeps its table."""
    result = await conn.execute_core(
        update(replica_state).where(_is(key), _t.retired_at.is_not(None)).values(retired_at=None)
    )
    return (result.rowcount or 0) > 0


async def retired_before(conn: "Connection", before: datetime) -> list[ReplicaKey]:
    """The replicas retired at or before ``before``: those whose wait is over."""
    result = await conn.execute_core(
        select(_t.source_id, _t.schema_name, _t.table_name).where(
            _t.retired_at.is_not(None), _t.retired_at <= before
        )
    )
    return [(r[0], r[1], r[2]) for r in result.fetchall()]


async def forget(conn: "Connection", key: ReplicaKey) -> bool:
    """Remove the record of a retired replica whose table has been dropped. Only a record that
    is still retired goes: one the model declared again in the meantime stays."""
    result = await conn.execute_core(
        replica_state.delete().where(_is(key), _t.retired_at.is_not(None))
    )
    return (result.rowcount or 0) > 0
