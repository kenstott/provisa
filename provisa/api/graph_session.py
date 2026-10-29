# Copyright (c) 2026 Kenneth Stott
# Canary: b97914cc-074c-4706-bb7c-2521bbafa040
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A principal's ephemeral graph session: one per environment, the same on every client
(REQ-2176).

Every tab of the Cypher page, every Bolt connection, MCP and every server worker a statement of a
principal reaches runs it in the principal's one graph session. Its tables live in the
environment's store under the principal's own schema (a session id fixed by the principal,
:func:`session_id`); what the session knows of them -- each table's shape and serials, its
generation and its ephemeral graph's index -- is one control-plane row per principal
(``graph_sessions``). :func:`session_for` reads the row for each statement under a lock -- shared
for a statement with no write clause (``parser.writes_in``), exclusive for one with -- and a
statement that changed the session writes it back with the generation advanced. Whichever worker
or connection a statement reaches therefore sees what every earlier statement made, and nothing is
lost when a connection, a tab or a worker goes.

It ends when the principal clears it (:func:`clear`), when it has gone unused for the retention
(``temp_tables.graph_session_retention_days``), or when the principal's rights change: the role
claims it holds, or any grant of those roles (:func:`rights`). A principal's rights are the union
of its roles; which one it acts as is no part of them. A SQL temporary table keeps its connection's
own session (REQ-615); only the graph session is the principal's.
"""

# Requirements: REQ-2176

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import time
from collections import Counter
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.compiler import temp_tables
from provisa.compiler.temp_tables import TempSession, TempTable
from provisa.core.schema_org import graph_sessions as rows
from provisa.ephemeral import store

log = logging.getLogger(__name__)


#: How stale a session's last-use mark may be before a read renews it. Its retention is counted
#: in days, so a read need not write the mark each time -- it then reads the row and nothing else.
_USE_MARKED_EVERY = timedelta(hours=1)


def session_id(user_id: str) -> str:
    """The principal's graph session id: its store schema is the same on every statement. Hex,
    as every session's id is: a session's store schema is known by it
    (``replica_address.temp_schema``, whose writes the replica guard admits by that form)."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:16]


@dataclass
class GraphSession:
    """The principal's graph session, for one statement."""

    user_id: str
    session: TempSession
    exclusive: bool
    # Set by a statement that wrote to it -- one whose rows changed in place leaves the shapes and
    # the graph's index as they were -- so its generation is advanced.
    wrote: bool = False
    # Set once it is written back; the statement's answer says so.
    changed: bool = False


# The graph session the statements of the current context belong to.
_bound: contextvars.ContextVar[GraphSession | None] = contextvars.ContextVar(
    "graph_session", default=None
)


def bound() -> GraphSession | None:
    return _bound.get()


# --- rights -------------------------------------------------------------------------------------


def role_ids(claims: list[str]) -> list[str]:
    """The roles named by role claims (``role`` or ``role:domain``)."""
    return sorted({c.partition(":")[0] for c in claims})


#: The principal of an unsecured deployment and of a local MCP client: whoever sends a request.
ANONYMOUS = "anonymous"


def claims_of(user_id: str, roles: Any, derived: Any) -> list[str]:
    """The roles a principal holds: its role claims and its derived roles. The anonymous
    principal holds none: an unsecured request names whichever role it acts as, and that is no
    change of rights -- its session ends only when cleared or past its retention."""
    if user_id == ANONYMOUS:
        return []
    return sorted({*(roles or []), *(derived or ())})


def principal(state: Any, identity: Any) -> tuple[str, list[str]]:
    """``identity``'s graph-session principal and the roles it holds, alike on every client: the
    derived roles the HTTP middleware bound with it, or, for a transport that authenticates its
    own caller, those its attributes give it (``auth.derived_roles.principal_of``)."""
    derived = identity.derived_roles
    if derived is None:
        from provisa.auth.derived_roles import principal_of

        derived = principal_of(state, identity)[1]
    return identity.user_id, claims_of(identity.user_id, identity.roles, derived)


def _sorted(items: Any) -> list:
    return sorted(items, key=lambda x: json.dumps(x, sort_keys=True, default=str))


def _role_grants(state: Any, role_id: str) -> dict[str, Any]:
    """Everything ``role_id`` is granted in this build, its inherited grants expanded: the
    columns it is served by the one rule (``security.visibility.visible_tables``)."""
    from provisa.security.visibility import visible_tables

    inputs = state.role_build_inputs
    tables = inputs["tables"]
    role = state.roles.get(role_id)
    served = [] if role is None else visible_tables(tables, role)
    return {
        "role": role,
        "columns": _sorted(
            [t["id"], c["column_name"]] for t in served for c in t.get("columns") or []
        ),
        "writes": _sorted(
            g for t in tables for g in t.get("write_grants") or [] if g.get("role") == role_id
        ),
        "rows": _sorted(r for r in inputs["rls_rules"] if r.get("role_id") == role_id),
        "metrics": sorted(
            m["name"] for m in inputs["metrics"] if role_id in (m.get("visible_to") or [])
        ),
    }


#: Where a build's inputs keep each role's grant fingerprint, made once per build: a role's grants
#: are a pure function of them, and walking every table's columns per statement grows with the model.
_PRINTS = "graph_session_role_prints"


def _role_print(state: Any, role_id: str) -> str:
    prints = state.role_build_inputs.setdefault(_PRINTS, {})
    held = prints.get(role_id)
    if held is None:
        held = prints[role_id] = hashlib.sha256(
            json.dumps(_role_grants(state, role_id), sort_keys=True, default=str).encode()
        ).hexdigest()
    return held


def rights(state: Any, claims: list[str]) -> str:
    """The fingerprint of a principal's rights in the build bound now: the role claims it holds
    and every grant of the roles they name."""
    held = {
        "claims": sorted(claims),
        "grants": {r: _role_print(state, r) for r in role_ids(claims)},
    }
    return hashlib.sha256(json.dumps(held, sort_keys=True).encode()).hexdigest()


async def _assigned(state: Any, user_ids: list[str]) -> dict[str, list[str]]:
    """The role claims the org assigns each of ``user_ids`` now, rendered as the auth middleware
    renders them into an identity's roles (a ``*`` domain is the bare role)."""
    from provisa.auth.role_grants import in_force
    from provisa.core.schema_org import user_role_assignments as t

    async with state.model_db.acquire() as conn:
        found = (
            await conn.execute_core(
                select(t.c.user_id, t.c.role_id, t.c.domain_id).where(
                    t.c.user_id.in_(user_ids), in_force()
                )
            )
        ).fetchall()
    out: dict[str, list[str]] = {}
    for r in found:
        out.setdefault(r.user_id, []).append(
            r.role_id if r.domain_id == "*" else f"{r.role_id}:{r.domain_id}"
        )
    return {u: sorted(c) for u, c in out.items()}


# --- the session's row ----------------------------------------------------------------------


def _session_of(user_id: str, row: Any) -> TempSession:
    session = TempSession(
        id=session_id(user_id),
        tables={
            t["name"]: TempTable(t["name"], [tuple(c) for c in t["columns"]], dict(t["serials"]))
            for t in row.tables
        },
        stored=row.stored,
        generation=row.generation,
        kept=True,
    )
    held = store.graph(session)
    loaded = store.graph_from_json(row.graph)
    for f in fields(held):
        setattr(held, f.name, getattr(loaded, f.name))
    return session


def _held(session: TempSession) -> dict[str, Any]:
    """What the row keeps of ``session``."""
    return {
        "tables": [
            {"name": t.name, "columns": [list(c) for c in t.columns], "serials": t.serials}
            for t in session.tables.values()
        ],
        "graph": store.graph_to_json(store.graph(session)),
        "stored": session.stored,
    }


def _first_generation(after: int = -1) -> int:
    """The generation a session begins at: the clock in microseconds, so one begun after a clear,
    a retention sweep or a change of rights (the row gone, or replaced) never repeats a generation
    its principal's earlier session had -- the session id is fixed, so ``id:generation`` keys what
    is kept for it. Never at or below ``after``, the generation a replaced row held."""
    return max(time.time_ns() // 1000, after + 1)


def _empty_row(
    user_id: str, claims: list[str], assigned: list[str], fingerprint: str, after: int = -1
) -> dict:
    return {
        "user_id": user_id,
        "generation": _first_generation(after),
        "used_at": datetime.now(UTC),
        "roles": sorted(claims),
        "assigned": assigned,
        "rights": fingerprint,
        **_held(TempSession(kept=True)),
    }


async def _end(state: Any, user_id: str, stored: bool) -> None:
    """Drop the principal's store schema, in the org environment bound now."""
    from provisa.pgwire import temp_exec

    if stored:
        await temp_exec.end(TempSession(id=session_id(user_id), stored=True, kept=True), state)


@asynccontextmanager
async def session_for(
    state: Any, user_id: str, claims: list[str], *, writes: bool
) -> AsyncGenerator[GraphSession]:
    """The principal's graph session in the org environment bound now, for one statement --
    ``writes``: the statement holds a write clause -- bound as the temporary-table session its
    statements run in. A statement made within it (a write's read) is in it already and is not
    bound again. A session whose principal's rights changed is ended and a new one begun."""
    outer = _bound.get()
    if outer is not None:
        yield outer
        return
    fingerprint = rights(state, claims)
    async with state.tenant_db.acquire() as conn:
        failure: BaseException | None = None
        async with conn.transaction():
            locking = select(rows).where(rows.c.user_id == user_id)
            row = (
                await conn.execute_core(
                    locking.with_for_update() if writes else locking.with_for_update(read=True)
                )
            ).first()
            if row is None or row.rights != fingerprint:
                # A principal's first session, or one whose rights changed: begun afresh, under
                # the exclusive lock (taken again over a shared one).
                assigned = (await _assigned(state, [user_id])).get(user_id, [])
                await conn.execute_core(
                    pg_insert(rows)
                    .values(**_empty_row(user_id, claims, assigned, fingerprint))
                    .on_conflict_do_nothing(index_elements=["user_id"])
                )
                row = (await conn.execute_core(locking.with_for_update())).one()
                if row.rights != fingerprint:
                    await _end(state, user_id, row.stored)
                    await conn.execute_core(
                        update(rows)
                        .where(rows.c.user_id == user_id)
                        .values(
                            **_empty_row(user_id, claims, assigned, fingerprint, row.generation)
                        )
                    )
                    row = (await conn.execute_core(locking)).one()
            held = GraphSession(user_id, _session_of(user_id, row), exclusive=writes)
            before = _held(held.session)
            token = _bound.set(held)
            temp = temp_tables.bind(held.session)
            try:
                yield held
            except BaseException as exc:  # noqa: BLE001 — written back first, then raised
                failure = exc
            finally:
                temp_tables.unbind(temp)
                _bound.reset(token)
            after = _held(held.session)
            if after != before or held.wrote:
                # What a failed statement landed before it failed is the session's too.
                assert held.exclusive, "a statement with no write clause changed the graph session"
                # Past every generation the statement passed through: each table it made or
                # dropped advanced it, and a plan it kept between was keyed by that one.
                generation = max(row.generation, held.session.generation) + 1
                await conn.execute_core(
                    update(rows)
                    .where(rows.c.user_id == user_id)
                    .values(**after, generation=generation, used_at=datetime.now(UTC))
                )
                held.changed = True
        if failure is not None:
            raise failure
        now = datetime.now(UTC)
        if not held.changed and row.used_at < now - _USE_MARKED_EVERY:
            # A read is a use too, marked after its shared lock is let go: readers holding it
            # together would each wait on the others' to write the mark.
            await conn.execute_core(
                update(rows).where(rows.c.user_id == user_id).values(used_at=now)
            )


# --- what it holds ------------------------------------------------------------------------------


async def _rows_of(state: Any, session: TempSession, name: str) -> list[dict[str, Any]]:
    from provisa.pgwire import temp_exec

    if name not in session.tables:
        return []
    names = [c for c, _ in session.tables[name].columns]
    return [dict(zip(names, r)) for r in await temp_exec.held_rows(state, session, name)]


async def contents(state: Any, session: TempSession) -> dict[str, Any]:
    """The session's labels, each with how many of its ephemeral nodes carry it and how many
    model nodes it tags; its relationship types, each with how many relationships it has; and
    whether it holds nothing."""
    g = store.graph(session)
    labelled = await _rows_of(state, session, store.NODE_LABELS)
    tagged = Counter(r["label"] for r in labelled if r[store.NODE_ID] not in g.node_keys)
    nodes = Counter(label for keys in g.node_keys.values() for label in keys)
    rels = Counter(r["type"] for r in await _rows_of(state, session, store.RELS))
    labels = sorted(g.labels | set(g.tag_types) | set(tagged))
    return {
        "labels": [
            {"label": x, "nodes": nodes[x], "tagged_model_nodes": tagged[x]} for x in labels
        ],
        "types": [{"type": t, "relationships": rels[t]} for t in sorted(g.types | set(rels))],
        "empty": g.empty and not session.tables,
    }


# --- its end ------------------------------------------------------------------------------------


async def clear(state: Any, user_id: str) -> None:
    """End the principal's graph session in the org environment bound now: its objects are
    dropped and its row deleted, and a later statement begins a new one. Called within one of
    the principal's statements (a Cypher procedure call), it empties the bound session, which
    that statement's exclusive lock writes back."""
    held = _bound.get()
    if held is not None and held.user_id == user_id:
        assert held.exclusive, "a graph session is cleared under its exclusive lock"
        from provisa.pgwire import temp_exec

        await temp_exec.end(held.session, state)
        return
    async with state.tenant_db.acquire() as conn:
        async with conn.transaction():
            row = (
                await conn.execute_core(
                    select(rows.c.stored).where(rows.c.user_id == user_id).with_for_update()
                )
            ).first()
            if row is None:
                return
            await _end(state, user_id, row.stored)
            await conn.execute_core(delete(rows).where(rows.c.user_id == user_id))


def _retained_since(now: datetime) -> datetime:
    from provisa.core import settings_registry

    days = int(settings_registry.value("temp_tables.graph_session_retention_days"))
    return now - timedelta(days=days)


async def sweep(state: Any, now: datetime) -> list[str]:
    """End, in the org environment bound now, each graph session unused for the retention or
    whose principal's rights changed since it was last used: a grant of its roles (this build),
    or the roles assigned to it. One a statement holds now is left for the next sweep. Returns
    the principals whose session was ended."""
    async with state.tenant_db.acquire() as conn:
        held = (
            await conn.execute_core(
                select(rows.c.user_id, rows.c.used_at, rows.c.roles, rows.c.assigned, rows.c.rights)
            )
        ).fetchall()
    if not held:
        return []
    assigned = await _assigned(state, [r.user_id for r in held])
    lapsed = [
        r.user_id
        for r in held
        if r.used_at < _retained_since(now)
        or assigned.get(r.user_id, []) != r.assigned
        or r.rights != rights(state, r.roles)
    ]
    ended: list[str] = []
    for user_id in lapsed:
        async with state.tenant_db.acquire() as conn:
            async with conn.transaction():
                row = (
                    await conn.execute_core(
                        select(rows.c.stored)
                        .where(rows.c.user_id == user_id)
                        .with_for_update(skip_locked=True)
                    )
                ).first()
                if row is None:
                    continue
                await _end(state, user_id, row.stored)
                await conn.execute_core(delete(rows).where(rows.c.user_id == user_id))
        ended.append(user_id)
    if ended:
        log.info("ended %d graph session(s): %s", len(ended), ", ".join(ended))
    return ended


async def keep(state: Any) -> None:
    """A worker's sweep of the graph sessions, in every org environment it holds a runtime of.
    Runs until the worker stops; any worker's sweep may end any principal's session."""
    import asyncio

    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.pgwire.temp_keeper import BEAT_SECONDS

    while True:
        try:
            now = datetime.now(UTC)
            for runtime in state.org_registry.runtimes():
                org, env = set_current_org(runtime.org_id), set_current_env(runtime.env)
                try:
                    await sweep(state, now)
                finally:
                    reset_current_env(env)
                    reset_current_org(org)
        except Exception:  # noqa: BLE001 — the sweep outlives a pass that failed; it says so
            log.exception("graph session sweep failed; it runs again")
        await asyncio.sleep(BEAT_SECONDS)
