# Copyright (c) 2026 Kenneth Stott
# Canary: dfb8dec5-e07e-44a0-a0ba-d5b46e83774b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Replicas converge to the declared model (REQ-1915, REQ-1919, REQ-1920).

Nothing calls "build this replica" when a table is saved, and nothing calls "delete this
replica" when one is removed. The model says which tables are served from a whole-table replica
on this engine; the state store says which replicas there are. :func:`converge_replicas`
compares the two, in every process that does background work, each time that process builds
its model (at boot, after its own change, and when another worker's change reaches it through
the model stamp, REQ-1914):

- a table that should have a replica and has no completed build in this store, or whose
  definition has changed since its last build, has a build requested;
- a replica the state store records whose table is no longer declared (table or source
  deleted, setting changed to Never, demoted) is RETIRED — marked, nothing dropped — and
  dropped by a later pass once every node has had time to reload its model and every statement
  already addressed at it has ended (:func:`drop_retired`, run by the build runner's pass).
  A table declared again before the drop is un-retired and keeps its replica.

So an admin save, a config-file load, an environment copy, a delete, a demotion and a boot are
one code path, on whichever node made the change or none of them, and the model store knows
nothing of replication.

What makes it safe with many nodes running it at once:

- it reads the state store once and the model from memory, and every write it makes is
  conditional, so running it twice changes nothing the second time;
- it only ever drops what the state store records, at the address the resolver gives; a table
  in the replicas schema with no record is never touched;
- a node retires a record only when the model it has loaded is at least as new as the one the
  record was requested at, so a node that has not yet seen a new table cannot take its replica
  for one whose table is gone;
- a build and a drop of one replica are exclusive (the replica's lock).
"""

# Requirements: REQ-1915, REQ-1919, REQ-1920

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from provisa.federation import replica_state
from provisa.federation.replica_state import ReplicaKey

log = logging.getLogger(__name__)

#: The columns of a source that are its identity: a change of any moves the table elsewhere.
#: (The same set a worker's per-source state is built from, without credentials.)
_SOURCE_IDENTITY = ("type", "host", "port", "database", "path")


def definition_hash(
    source: Any, address: Any, columns: list[tuple[str, str]], pk_columns: list[str]
) -> str:
    """What a replica is built from, as a digest: the address it is written at, its columns in
    order with their types, its key, and its source's connection identity. A model that gives a
    different digest than the last build recorded asks for a rebuild."""
    identity = {name: _plain(getattr(source, name, None)) for name in _SOURCE_IDENTITY}
    described = {
        "address": [address.schema, address.table],
        "columns": [[name, ir_type] for name, ir_type in columns],
        "key": list(pk_columns),
        "source": identity,
    }
    return hashlib.sha256(json.dumps(described, sort_keys=True).encode()).hexdigest()


def _plain(value: Any) -> Any:
    return getattr(value, "value", value)  # an enum's value, anything else as it is


def still_answers(built_columns: list | None, columns: list[tuple[str, str]]) -> bool:
    """Whether a replica with ``built_columns`` can answer anything the model can now ask of a
    table declaring ``columns``: every declared column is there with the same type. A column
    removed from the model, a changed key or a moved source leave the old rows usable until the
    rebuild swaps; a column added or retyped does not."""
    if built_columns is None:
        return False
    have = {(name, ir_type) for name, ir_type in built_columns}
    return all((name, ir_type) in have for name, ir_type in columns)


def _field(holder: Any, name: str) -> Any:
    return holder[name] if isinstance(holder, dict) else getattr(holder, name)


def whole_copy(source: Any, table: Any, engine: Any) -> bool:
    """Whether the replica of ``table`` is a whole copy of it, the kind a build makes. Two
    kinds of table are served from the store and never built whole:

    - one replicated row by row (REQ-1865, where the engine cannot attach its source): its rows
      are fetched by key when a statement asks for them, never ahead of one;
    - one with a parameter column: it is a function of its arguments, with no whole to copy.

    The one answer for convergence (no build is requested), the read backstop (no build is
    requested or awaited) and the build itself (it refuses). ``table`` is a registered table as
    the registry gives it, a row or a model; ``engine`` the runtime or the federation engine."""
    from provisa.federation.strategy import engine_attaches

    if any(_field(c, "native_filter_type") is not None for c in _field(table, "columns")):
        return False
    row_level = _field(table, "row_materialize") and not engine_attaches(
        engine, _plain(source.type)
    )
    return not row_level


def builds_here(home_region: str | None) -> bool:
    """Whether this node builds a replica of a table whose data lives in ``home_region`` (the
    region the table names — ``home_region``): the one question convergence and Hot
    promotion ask (REQ-1921/1922). A table naming no region is built in every region, each into
    its own store; a table naming one is built only by that region's nodes, and read from there by
    the org's other regions."""
    from provisa.core import process_region

    return home_region is None or home_region == process_region.region()


def home_region(registration: Any) -> str | None:
    """The region a registered table's data lives in: the one the table names, None for none
    (REQ-1921, "a table carries its own region"). Its source's region is only the admin form's
    default for a new table; it decides nothing about where copies live."""
    return registration["region"] if isinstance(registration, dict) else registration.region


@dataclass
class Converged:
    """What one pass did."""

    requested: list[ReplicaKey] = field(default_factory=list)
    retired: list[ReplicaKey] = field(default_factory=list)
    kept: list[ReplicaKey] = field(default_factory=list)  # un-retired: declared again
    not_serving: list[ReplicaKey] = field(default_factory=list)


#: The last convergence failure per org (None: the default org): when, on which replica, why.
#: Cleared by the next pass of that org that succeeds. Read by the admin build query.
last_error: dict[str | None, dict] = {}


async def converge_replicas(state: Any) -> Converged:
    """Bring the state store in line with what the model of the org ``state`` serves declares
    (see the module's docstring). Raises what fails; :func:`converge_logged` is the entry that
    records and logs a failure instead."""
    from provisa.federation import replica_builds
    from provisa.federation.replica_routing import landing_worklist, replica_tables
    from provisa.federation.replica_state_view import view_for

    engine = state.federation_engine.engine
    backend = engine.backend
    stamp = state.model_stamp
    store = replica_builds.store_identity(state)
    org_id = _org()

    served = [
        ((src.id, reg["schema_name"], reg["table_name"]), src, reg)
        for src, reg in await replica_tables(engine, state)
    ]
    declared = {key for key, _src, _reg in served}
    homes = {key: home_region(reg) for key, src, reg in served}
    # Of those, the ones a build makes: a row-level or parameterized table is declared (its
    # table at the resolver's address is never retired) and never built.
    whole = {key for key, src, reg in served if whole_copy(src, reg, engine)}
    # The tables of those whose columns are all resolved: what a build needs. One whose type
    # is not resolved yet is declared (never retired) and built once it is.
    shapes: dict[ReplicaKey, tuple[str, list[tuple[str, str]]]] = {}
    for src, schema_name, table_name, columns, pk_columns in await landing_worklist(engine, state):
        key = (src.id, schema_name, table_name)
        if key not in whole:
            continue
        address = backend.replica_address(
            state, source_id=src.id, schema_name=schema_name, table_name=table_name
        )
        shapes[key] = (definition_hash(src, address, columns, pk_columns), columns)

    done = Converged()
    now = datetime.now(UTC)
    view = view_for(state)
    async with state.tenant_db.acquire() as conn:
        records = {r.key: r for r in await replica_state.read_all(conn)}
        for key in sorted(declared):
            if not builds_here(homes[key]):
                continue
            record = records.get(key)
            if record is not None and record.retired_at is not None:
                if await replica_state.unretire(conn, key):
                    done.kept.append(key)
            shape = shapes.get(key)
            if shape is None:
                continue
            wanted_hash, columns = shape
            built = record is not None and record.exists_in(store)
            serving = built and still_answers(record.built_columns, columns)  # type: ignore[union-attr]
            # A standing replica that cannot answer the model is not read until a build of the
            # model's definition has completed (the read backstop asks the view).
            view.await_definition(org_id, key, wanted_hash if built and not serving else None)
            if built and not serving:
                done.not_serving.append(key)
            if built and record.definition_hash == wanted_hash:  # type: ignore[union-attr]
                continue
            reason = replica_state.REASON_DEFINITION if built else replica_state.REASON_MODEL
            if await replica_state.request_build(conn, key, reason, model_stamp=stamp, now=now):
                done.requested.append(key)
        for key, record in sorted(records.items()):
            # REQ-1922: a copy of a table declared but kept in another region (it named none,
            # or another, when this region built it) holds data this region may not keep: it
            # goes exactly as an undeclared one does.
            if (key in declared and builds_here(homes[key])) or record.retired_at is not None:
                continue
            if record.model_stamp is not None and (stamp is None or stamp < record.model_stamp):
                continue  # requested under a newer model than this node has loaded: not gone
            if await replica_state.retire(conn, key, now=now):
                done.retired.append(key)
    if done.requested:
        replica_builds.kick(org_id)
    return done


async def converge_logged(state: Any) -> None:
    """:func:`converge_replicas` for a caller that must not fail with it (the model build it
    runs after must not wait on, or be stopped by, a replica store or a source). A failure is
    logged at ERROR with the org and the cause, and kept as the org's last convergence error
    until a pass succeeds, so an operator sees that replicas are not converging."""
    org_id = _org()
    if getattr(state, "federation_engine", None) is None or state.tenant_db is None:
        return  # no engine or no tenant plane yet: there are no replicas to converge
    try:
        done = await converge_replicas(state)
    except Exception as exc:  # allow-ble: recorded for the operator and logged; the model build this runs after must not depend on a replica store (the rule model_reload.reconcile_sources follows)
        last_error[org_id] = {
            "at": datetime.now(UTC),
            "key": getattr(exc, "key", None),
            "cause": f"{type(exc).__name__}: {exc}",
        }
        log.error(
            "replicas of org %s did not converge (%s): %s",
            org_id or "default",
            ".".join(getattr(exc, "key", None) or ("-",)),
            exc,
            exc_info=exc,
        )
        return
    last_error.pop(org_id, None)
    if done.requested or done.retired or done.kept:
        log.info(
            "replicas converged: %d build(s) requested, %d retired, %d kept",
            len(done.requested),
            len(done.retired),
            len(done.kept),
        )


def _org() -> str | None:
    from provisa.core.request_context import current_org

    return current_org.get(None)


def drop_grace_seconds() -> float:
    """How long a retired replica stands before it is dropped: twice the model reload interval
    (every live node has reloaded and stopped routing to it) plus the longest request timeout
    (every statement already addressed at it has ended)."""
    from provisa.core import settings_registry

    longest = float(settings_registry.value("limits.request_timeout"))
    for seconds in settings_registry.value("limits.request_timeouts").values():
        if seconds is not None:
            longest = max(longest, float(seconds))
    return 2 * float(settings_registry.value("config.reload_interval")) + longest


async def drop_retired(state: Any, locks: Any, org_id: str) -> list[ReplicaKey]:
    """Drop every retired replica whose wait is over: its table in the store, then its record.
    ``locks`` is the process's :class:`~provisa.federation.replica_locks.BuildLocks`; a claim
    is opened only when something is due. A replica whose lock is held (a build finishing,
    another node's drop) is left for the next pass. Returns the replicas dropped."""
    before = datetime.now(UTC) - timedelta(seconds=drop_grace_seconds())
    async with state.tenant_db.acquire() as conn:
        due = await replica_state.retired_before(conn, before)
    if not due:
        return []
    dropped: list[ReplicaKey] = []
    claim = locks.claim()
    try:
        for key in due:
            if not claim.try_replica(org_id, key):
                continue
            try:
                await drop_replica_table(state, key)
                async with state.tenant_db.acquire() as conn:
                    # Only a record still retired goes: one declared again meanwhile keeps its
                    # row (its table is gone, so its next convergence asks for a build).
                    if await replica_state.forget(conn, key):
                        dropped.append(key)
            finally:
                claim.release_replica(org_id, key)
    finally:
        claim.close()
    if dropped:
        log.info(
            "replicas dropped (no longer declared, or kept in another region): %s",
            [".".join(k) for k in dropped],
        )
    return dropped


async def drop_replica_table(state: Any, key: ReplicaKey) -> None:
    """Remove the table of the replica ``key`` from this engine's store, at the address the
    resolver gives for it — never any other table."""
    from provisa.federation.replica_parties import StoreReadingEngine

    backend = state.federation_engine.engine.backend
    address = backend.replica_address(
        state, source_id=key[0], schema_name=key[1], table_name=key[2]
    )
    target = backend.replica_target(
        state,
        address=address,
        args=SimpleNamespace(columns=[], pk_columns=[]),
        engine=StoreReadingEngine(backend, state),
    )
    await target.drop()
