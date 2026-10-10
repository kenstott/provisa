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

from provisa.core.read_refusal import ReadRefused

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.exc import IntegrityError

from provisa.core import config_stamp
from provisa.core.schema_org import replica_state

if TYPE_CHECKING:
    from collections.abc import Callable

    from provisa.federation.data_replicator import BuildNote

    from provisa.core.database import Connection

#: A replica's key: the registered identity of its table.
ReplicaKey = tuple[str, str, str]


async def set_promoted(conn: "Connection", key: ReplicaKey, promoted: bool) -> bool:
    """Record that the table ``key`` is (or is no longer) promoted. The one write site of the
    promoted flag. True when the flag changed.

    A change is one of the two transitions that move a table between its live read and its
    replica (the other is ``mark_first_completion``): it advances the replica-state stamp in
    the same transaction, so every process republishes its routes. Demotion takes the table
    out of ``serving_keys`` at once; its replica is left for the replicator to retire."""
    source_id, schema_name, table_name = key
    async with conn.transaction():
        row = (await conn.execute_core(select(replica_state.c.promoted).where(_is(key)))).fetchone()
        if (bool(row[0]) if row is not None else False) == promoted:
            return False
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
        await config_stamp.advance(conn, config_stamp.REPLICA)
    return True


async def promoted_keys(conn: "Connection") -> frozenset[ReplicaKey]:
    """The tables currently promoted."""
    result = await conn.execute_core(
        select(
            replica_state.c.source_id, replica_state.c.schema_name, replica_state.c.table_name
        ).where(replica_state.c.promoted.is_(True))
    )
    return frozenset((r[0], r[1], r[2]) for r in result.fetchall())


async def promotion(
    conn: "Connection", store: "Callable[[], str]"
) -> tuple[frozenset[ReplicaKey], frozenset[ReplicaKey]]:
    """``(promoted, serving)``: the tables that passed their Hot threshold (REQ-826), and those
    of them whose replica exists in the store ``store`` identifies
    (``replica_builds.store_identity``) — the ones whose reads go to their replica. A promoted
    table that is not serving is read live while its replica is built; a replica built in
    another engine's store is not one this engine can read."""
    result = await conn.execute_core(
        select(
            replica_state.c.source_id,
            replica_state.c.schema_name,
            replica_state.c.table_name,
            replica_state.c.completed_at,
            replica_state.c.built_store,
        ).where(replica_state.c.promoted.is_(True))
    )
    rows = result.fetchall()
    built = [r for r in rows if r[3] is not None]
    # ``store`` is asked only when a promoted table has a completed build to place: a deployment
    # with nothing promoted needs no store, and may have none (an engine that is not its own
    # store, with none configured) — its registry is read all the same.
    here = store() if built else None
    return (
        frozenset((r[0], r[1], r[2]) for r in rows),
        frozenset((r[0], r[1], r[2]) for r in built if r[4] == here),
    )


async def serving_keys(conn: "Connection", store: str) -> frozenset[ReplicaKey]:
    """The promoted tables served from their replica in ``store`` (see :func:`promotion`)."""
    return (await promotion(conn, lambda: store))[1]


async def mark_first_completion(conn: "Connection", key: ReplicaKey, store: str) -> bool:
    """Called by ``record_completed``, inside its transaction, BEFORE it writes the completion:
    whether this completion is the one that makes a PROMOTED table's replica readable in
    ``store`` for the first time — and if so, advance the replica-state stamp, so every process
    moves the table's reads onto the replica.

    False, and no stamp, for a table that is not promoted (Always and load-protected tables are
    addressed at their replica from the moment the setting is saved: nothing about their route
    changes when a build completes) and for every refresh after the first."""
    row = (
        await conn.execute_core(
            select(replica_state.c.promoted, replica_state.c.completed_at, _t.built_store).where(
                _is(key)
            )
        )
    ).fetchone()
    if row is None or not row[0]:
        return False
    if row[1] is not None and row[2] == store:
        return False
    await config_stamp.advance(conn, config_stamp.REPLICA)
    return True


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
REASON_WRITE = "write"  # REQ-1924: the table was just written through Provisa
#: The table's rows changed at its source. A build already running when one of these arrives
#: may have read past the change, so the replica is built again once that build completes.
CHANGE_REASONS = (REASON_REFRESH, REASON_WRITE)
REASONS = (
    REASON_MODEL,
    REASON_DEFINITION,
    REASON_HOT,
    REASON_REFRESH,
    REASON_OPERATOR,
    REASON_READ,
    REASON_WRITE,
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
    feed_down_since: datetime | None = None
    feed_error: str | None = None
    delta_cursor: Any = None
    delta_skipped: str | None = None
    #: What the last completed build had to say of the copy it made, when it had anything.
    #: What the last completed build had to say: [{"code", "params"}], empty when nothing.
    build_notes: list = field(default_factory=list)

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
    _t.feed_down_since,
    _t.feed_error,
    _t.delta_cursor,
    _t.delta_skipped,
    _t.build_notes,
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
        feed_down_since=_aware(row[25]),
        feed_error=row[26],
        delta_cursor=row[27],
        delta_skipped=row[28],
        # NULL is a replica no build has completed for, which has no notes.
        build_notes=row[29] if row[29] is not None else [],
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


class ReplicaBuildFailed(ReadRefused):
    """The build of a replica a read needs failed: the read is refused, naming the table and the
    build's own error; it never reads what the failed build left standing (REQ-1661). The
    statement is good and the server has not failed — the table cannot be built until what the
    error names is put right."""

    code = "query.replica_build_failed"

    def __init__(self, replica: str, error: str | None) -> None:
        self.replica = replica
        self.error = error
        reason = error or "the build recorded no error"
        self.params = {"table": replica, "reason": reason}
        super().__init__(f"the replica of {replica} could not be built: {reason}")


async def read(conn: "Connection", key: ReplicaKey) -> ReplicaRecord | None:
    """The record of the replica ``key``, or None when it has none."""
    row = (await conn.execute_core(select(*_COLUMNS).where(_is(key)))).fetchone()
    return _record(row) if row is not None else None


async def read_all(conn: "Connection") -> list[ReplicaRecord]:
    """Every replica's record."""
    return [_record(r) for r in (await conn.execute_core(select(*_COLUMNS))).fetchall()]


class Failure(NamedTuple):
    """One recorded failure: how many in a row, and whether it repeats the one before."""

    attempts: int
    repeat: bool


@dataclass(frozen=True)
class RetryPolicy:
    """How long a failed build waits before it is tried again (REQ-1915): the operator's
    ``replication.retry_interval``, doubled for each failure in a row, never past
    ``replication.retry_interval_max``. Nothing stops the retries; a completed build puts the
    count of failures back to none, and with it the wait.

    Computed from ``failed_attempts`` and ``failed_at`` alone: there is no stored "next attempt"
    and no second clock, so a changed setting is in force for the very next decision."""

    interval: float
    ceiling: float

    def __post_init__(self) -> None:
        # The two settings are refused as a pair where they are saved and where they are loaded
        # (settings_registry.check_pairs); a policy is never made from one that slipped past.
        if self.ceiling < self.interval:
            raise ValueError(
                f"retry ceiling {self.ceiling:g} s is below the retry interval {self.interval:g} s"
            )

    def wait(self, failed_attempts: int) -> float:
        """Seconds after its last failure before a build that has failed ``failed_attempts``
        times in a row is tried again."""
        if self.interval <= 0:
            return 0.0
        wait = self.interval
        for _ in range(max(failed_attempts, 1) - 1):
            if wait >= self.ceiling:
                break
            wait = min(wait * 2, self.ceiling)
        return float(wait)

    def next_attempt_at(self, failed_at: datetime, failed_attempts: int) -> datetime:
        return failed_at + timedelta(seconds=self.wait(failed_attempts))

    def _due(self, now: datetime) -> Any:
        """The SQL for "this failed build's wait has passed": one comparison per step of the
        doubling, which the ceiling keeps few, and one for every count at or past the ceiling."""
        steps: list[Any] = []
        attempts = 1
        while self.wait(attempts) < self.wait(attempts + 1):
            # The first step also takes a record with no failure counted yet.
            counted = (
                _t.failed_attempts <= attempts if attempts == 1 else _t.failed_attempts == attempts
            )
            waited = _t.failed_at <= now - timedelta(seconds=self.wait(attempts))
            steps.append(and_(counted, waited))
            attempts += 1
        longest = _t.failed_at <= now - timedelta(seconds=self.wait(attempts))
        steps.append(longest if attempts == 1 else and_(_t.failed_attempts >= attempts, longest))
        return or_(*steps)


def retry_policy() -> RetryPolicy:
    """The policy in force: the two operator settings, read now (both are live)."""
    from provisa.core import settings_registry  # noqa: PLC0415 -- settings load the catalog

    return RetryPolicy(
        interval=float(settings_registry.value("replication.retry_interval")),
        ceiling=float(settings_registry.value("replication.retry_interval_max")),
    )


async def request_build(
    conn: "Connection",
    key: ReplicaKey,
    reason: str,
    *,
    retry: RetryPolicy | None = None,
    model_stamp: int | None = None,
    now: datetime | None = None,
) -> bool:
    """Ask for a build of the replica ``key``. True when this call made the request; False when
    a build was already requested or running (the caller has joined it), when
    ``retry`` holds it back, or when the replica is retired (the model no longer declares it:
    a retired replica is never rebuilt).

    ``model_stamp`` is the model stamp the asking process has loaded, recorded so that a node
    with an older model does not take the replica for one whose table is gone.

    ``retry`` is given by a caller that must not ask again too soon after a failure — a read: a
    build whose wait (:class:`RetryPolicy`) has not passed is not requested again. A caller that
    gives none — an operator's own request — is tried at once, whatever the wait."""
    if reason not in REASONS:
        raise ValueError(f"unknown build reason {reason!r}; expected one of {REASONS}")
    at = now if now is not None else datetime.now(UTC)
    retriable = _t.build_state == FAILED
    if retry is not None:
        retriable = and_(retriable, or_(_t.failed_at.is_(None), retry._due(at)))
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
    if reason in CHANGE_REASONS:
        # The rows changed while a build is running: the build may have read past the change.
        # The request is kept on the row, and :func:`record_completed` leaves the replica
        # requested (a request made after the build started) instead of idle.
        await conn.execute_core(
            update(replica_state)
            .where(_is(key), _t.retired_at.is_(None), _t.build_state == BUILDING)
            .values(requested_at=at, requested_reason=reason)
        )
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


def _claimable(now: datetime, retry: RetryPolicy) -> Any:
    """A record a runner may build: not retired, and requested; idle with a refresh due;
    building (whose builder may have died — only the runner that gets the replica's lock finds
    out); or failed and its wait has passed (:class:`RetryPolicy` — the runner tries a failed
    build again itself: a replica nobody reads would otherwise stay failed). THE one place the
    wait of a failed build is decided for a runner; a read's request asks the same policy."""
    due = and_(_t.build_state == IDLE, _t.next_refresh_at.is_not(None), _t.next_refresh_at <= now)
    again = and_(_t.build_state == FAILED, _t.failed_at.is_not(None), retry._due(now))
    return and_(_t.retired_at.is_(None), or_(_t.build_state.in_((REQUESTED, BUILDING)), due, again))


async def candidates(
    conn: "Connection", *, now: datetime, limit: int, retry: RetryPolicy
) -> list[ReplicaKey]:
    """The replicas a runner may try to build (:func:`_claimable`), oldest request first."""
    result = await conn.execute_core(
        select(_t.source_id, _t.schema_name, _t.table_name)
        .where(_claimable(now, retry))
        .order_by(_t.requested_at.asc().nulls_last(), _t.next_refresh_at.asc())
        .limit(limit)
    )
    return [(r[0], r[1], r[2]) for r in result.fetchall()]


async def claim(
    conn: "Connection", key: ReplicaKey, *, holder: str, now: datetime, retry: RetryPolicy
) -> bool:
    """Move the row to ``building`` for ``holder``. Called only by the process that holds the
    replica's lock; False when the row is no longer a candidate (another runner completed it
    between this runner's selection and its lock, or the model stopped declaring it)."""
    result = await conn.execute_core(
        update(replica_state)
        .where(_is(key), _claimable(now, retry))
        .values(
            build_state=BUILDING,
            build_started_at=now,
            build_holder=holder,
            rows_copied=0,
            waiting_on=None,
        )
    )
    return (result.rowcount or 0) > 0


async def claim_sibling(conn: "Connection", key: ReplicaKey, *, holder: str, now: datetime) -> bool:
    """Move the row to ``building`` for ``holder`` because a build of another table of the
    same read is starting: the read gives this table's rows too, so it is built whatever its
    own state -- idle and not yet due, or failed and still waiting. Called only by the process
    that holds the replica's lock. False when the row is retired or gone. Its failure record
    is left as it is: the build's completion clears it, and a failed read adds one attempt."""
    result = await conn.execute_core(
        update(replica_state)
        .where(_is(key), _t.retired_at.is_(None))
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


async def record_feed(
    conn: "Connection", key: ReplicaKey, *, error: str | None, now: datetime
) -> None:
    """The state of the change-feed listener of the replica ``key``'s table (REQ-1861): down
    with the server's ``error`` (the time it went down is kept while it stays down), or watching
    (``error`` None). A replica with no record yet has none to show; convergence creates it."""
    values: dict[str, Any] = (
        {"feed_down_since": None, "feed_error": None}
        if error is None
        else {"feed_down_since": func.coalesce(_t.feed_down_since, now), "feed_error": error}
    )
    await conn.execute_core(update(replica_state).where(_is(key)).values(**values))


async def record_delta_applied(
    conn: "Connection", key: ReplicaKey, *, cursor: Any
) -> None:  # REQ-874
    """Record that a delta was applied: store the advanced cursor and clear ``delta_skipped``
    (this build was a delta, not a whole rebuild). ``cursor`` is max(cursor-field) over the
    applied rows (unchanged when the delta was empty)."""
    import json

    await conn.execute_core(
        update(replica_state)
        .where(_is(key))
        .values(delta_cursor=json.dumps(cursor, default=str), delta_skipped=None)
    )


async def record_whole_rebuild(
    conn: "Connection", key: ReplicaKey, *, skipped: str, cursor: Any
) -> None:  # REQ-874
    """Record that this build was a whole rebuild, not a delta: ``skipped`` is the declared
    reason (``delta.SKIP_*``, shown on the status line), and ``cursor`` sets the delta cursor to
    the rebuilt data's max cursor-field so the next delta resumes from it (None leaves it)."""
    import json

    values: dict[str, Any] = {"delta_skipped": skipped}
    if cursor is not None:
        values["delta_cursor"] = json.dumps(cursor, default=str)
    await conn.execute_core(update(replica_state).where(_is(key)).values(**values))


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
    notes: "tuple[BuildNote, ...]" = (),
) -> None:
    """The build finished and its table was swapped in, in the store ``store`` identifies.
    ``definition_hash`` and ``built_columns`` say what it was built from and which columns it
    has (None from a caller that does not track them: the next convergence asks again).
    ``note`` is what the build had to say of its copy; a build with nothing to say clears the
    note the one before it left.

    When this is the first replica of a promoted table in this store, the replica-state stamp
    advances with the completion (REQ-826, ``mark_first_completion``): the completion and the
    stamp are one transaction, so no process is told of a replica that was not recorded."""
    async with conn.transaction():
        await mark_first_completion(conn, key, store)
        await conn.execute_core(
            update(replica_state)
            .where(_is(key))
            .values(
                # A change reported after this build started (request_build) is not in it for
                # certain: the replica is served as built and is requested again.
                build_state=case(
                    (
                        and_(
                            _t.requested_reason.in_(CHANGE_REASONS),
                            _t.requested_at.is_not(None),
                            _t.build_started_at.is_not(None),
                            _t.requested_at > _t.build_started_at,
                        ),
                        REQUESTED,
                    ),
                    else_=IDLE,
                ),
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
                build_notes=[{"code": note.code, "params": note.params} for note in notes],
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
) -> "Failure":
    """The build failed. The previous replica, if any, is untouched and stays readable.
    ``code`` and ``params`` name a cause Provisa knows (``replica_errors``). The count of
    failures in a row goes up by one; a completed build puts it back to none.

    Returns the count of failures in a row including this one, and whether this failure repeats
    the one before it (the same error text, in an unbroken run of failures) — what the runner's
    log needs to say a repeat in one line."""
    before = (
        await conn.execute_core(
            select(_t.build_state, _t.failed_attempts, _t.last_error).where(_is(key))
        )
    ).fetchone()
    earlier = int(before[1] or 0) if before is not None else 0
    repeat = earlier > 0 and before is not None and before[2] == error
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
    return Failure(attempts=earlier + 1, repeat=repeat)


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
