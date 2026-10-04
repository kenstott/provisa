# Copyright (c) 2026 Kenneth Stott
# Canary: a59d39cf-19d2-426e-98a1-2cbbd23a1d39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every model change lands in its environment's branch as a commit (REQ-1524).

WHERE A CHANGE IS SEEN. In one place: the control-plane :class:`~provisa.core.database.Connection`.
Every statement that writes a projected table (:data:`provisa.core.env_deploy.PROJECTED`) through
a connection of an environment's model plane — a Core statement or raw SQL, by the table it
writes — is recorded here. A rule every writer has to remember is a rule the next writer forgets;
the connection is the one place every writer passes through.

WHEN IT IS COMMITTED. Once per change scope: an HTTP request (``ModelChangeMiddleware``), a
config load, a Hasura import, the reaper's sweep, a boot. When the outermost scope ends — or a
response is about to start, so the caller sees its answer after the commit — each plane written
in it is projected once, on a fresh connection, and committed by
:func:`provisa.core.env_repo.write_through`. One logical change is one commit; a write that
changed nothing projects to the same tree and makes no commit (REQ-1526).

WHAT IS NOT COMMITTED HERE. A path that commits for itself — environment create, merge, deploy,
undo and redo, the boot baseline, org creation — runs inside :func:`committed_by_caller`, where
what it writes is not recorded: a second projection would at best repeat its commit and, after an
undo, record a new one on top of the commit the environment was just moved back to.

A WRITE NO SCOPE OWNS IS A DEFECT. When a plane's repository is attached (the platform plane is
bound, :func:`attach`), a recorded write with no open scope raises :class:`ModelChangeOutsideScope`
rather than leaving a model change with no commit. With no repository attached — a bare control
plane, as in a unit test — there is no projection to keep, and nothing is recorded.
"""

# Requirements: REQ-1524, REQ-1526, REQ-1543

from __future__ import annotations

import contextvars
import logging
import re
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelPlane:
    """The environment a control-plane :class:`Database` holds the model of."""

    org_id: str
    env: str


class ModelChangeOutsideScope(RuntimeError):
    """A model write with no change scope open to commit it (REQ-1524)."""

    def __init__(self, plane: ModelPlane, table: str) -> None:
        super().__init__(
            f"{table} of {plane.org_id}/{plane.env} was written outside a model change scope; "
            "every model change commits (REQ-1524), so the surface that wrote it opens one "
            "(provisa.core.model_change.scope)"
        )


class ModelWriteToAnotherEnvironment(RuntimeError):
    """A write, through one environment's connection, to another environment's model."""

    def __init__(self, plane: ModelPlane, schema: str, table: str) -> None:
        super().__init__(
            f"{schema}.{table} was written through the connection of {plane.org_id}/{plane.env}; "
            "only a path that commits for itself writes another environment's model "
            "(provisa.core.model_change.committed_by_caller)"
        )


@dataclass
class _Scope:
    label: str
    actor: Callable[[], str | None]
    discard: bool = False
    # Every plane written in the scope, and what was written: (verb, table) in order.
    written: dict[tuple[ModelPlane, int], tuple["Database", list[tuple[str, str]]]] = field(
        default_factory=dict
    )
    names: list[str] = field(default_factory=list)
    closed: bool = False


_SCOPE: contextvars.ContextVar[_Scope | None] = contextvars.ContextVar(
    "provisa_model_change_scope", default=None
)

_platform: "Database | None" = None

# What commits a plane, and which tables a projection carries: handed in by :func:`attach`, so this
# module (which the control-plane connection imports) depends on neither the repository nor the
# projection.
_Commit = Callable[..., Awaitable[str | None]]
_commit: _Commit | None = None
_projected_tables: frozenset[str] = frozenset()


def attach(admin_db: "Database", *, commit: _Commit, projected: frozenset[str]) -> None:
    """Attach the platform plane, which holds every environment's position and drift, with the
    commit (``env_repo.write_through``) and the projected tables (``env_deploy.PROJECTED``). From
    here on a model write must be owned by a scope."""
    global _platform, _commit, _projected_tables
    _platform, _commit, _projected_tables = admin_db, commit, projected


def unbind() -> None:
    """Leave the current context's change. Detached work (a background worker) outlives the change
    its spawner was in, which closes when the spawner finishes; a write the worker made inside it
    would then find it closed. Detached work that writes the model opens a change of its own."""
    _SCOPE.set(None)


def detach() -> None:
    """The process's platform plane is gone (shutdown)."""
    global _platform, _commit
    _platform, _commit = None, None


def _projected() -> frozenset[str]:
    return _projected_tables


def record(
    database: "Database", plane: ModelPlane, verb: str, table: str, schema: str | None
) -> None:
    """Record that ``table`` was written through ``database``, the model plane of ``plane``.
    Called by the connection after a write that changed a row."""
    if _platform is None or table not in _projected():
        return
    current = _SCOPE.get()
    if current is not None and current.discard:
        return
    if schema is not None and schema != database.search_path:
        raise ModelWriteToAnotherEnvironment(plane, schema, table)
    if current is None or current.closed:
        raise ModelChangeOutsideScope(plane, table)
    _database, writes = current.written.setdefault((plane, id(database)), (database, []))
    writes.append((verb, table))


def name(action: str, kind: str, ident: str | int) -> None:
    """Name the change a writer is making, for the commit's message: ``upsert relationship
    orders-to-customers``. Outside a scope, or in one that commits nothing, it says nothing."""
    current = _SCOPE.get()
    if current is not None and not current.discard:
        current.names.append(f"{action} {kind} {ident}")


def label(text: str) -> None:
    """Name the scope's change when no writer names it: a GraphQL operation's name."""
    current = _SCOPE.get()
    if current is not None and not current.discard:
        current.label = text


def _system() -> str | None:
    return None


@asynccontextmanager
async def scope(text: str, actor: Callable[[], str | None] = _system) -> AsyncGenerator[_Scope]:
    """One logical model change. A scope opened inside another joins it: the outermost scope is
    the one change, and it is committed when it ends — whether or not its body raised, because
    the projection records the model as it IS, and a statement that committed before the raise
    changed it."""
    outer = _SCOPE.get()
    if outer is not None and not outer.closed:
        yield outer
        return
    current = _Scope(label=text, actor=actor)
    token = _SCOPE.set(current)
    try:
        yield current
    finally:
        _SCOPE.reset(token)
        await flush(current)
        current.closed = True


@asynccontextmanager
async def committed_by_caller() -> AsyncGenerator[None]:
    """A path that writes the model and commits it itself (environment create, merge, deploy,
    undo, redo, the boot baseline, org creation). What it writes is not recorded."""
    token = _SCOPE.set(_Scope(label="", actor=_system, discard=True))
    try:
        yield
    finally:
        _SCOPE.reset(token)


@asynccontextmanager
async def layout() -> AsyncGenerator[None]:
    """Laying out an org's schema (``schema.sql``: its DDL and the backfills that keep its own
    columns whole, such as ``tag_assignments.base_tag_id``) is not a change of the model: it is
    the same for every org and environment and changes nothing anyone authored. What it writes is
    not recorded — a backfill run by a whole script reports no row count, so recording it would
    count every boot of every org as a change (REQ-1524)."""
    token = _SCOPE.set(_Scope(label="", actor=_system, discard=True))
    try:
        yield
    finally:
        _SCOPE.reset(token)


def _message(current: _Scope, writes: list[tuple[str, str]]) -> str:
    if len(current.names) == 1:
        return current.names[0]
    if current.names:
        shown = current.names[:20]
        more = len(current.names) - len(shown)
        body = "\n".join(f"- {n}" for n in shown) + (f"\n- and {more} more" if more else "")
        return f"{current.label}\n\n{body}"
    tables = sorted({table for _verb, table in writes})
    return f"{current.label} ({', '.join(tables)})"


async def flush(current: _Scope) -> None:
    """Commit every plane the scope wrote, once each, and forget them. Never raises: a projection
    that does not land marks its environment drifted (``write_through``)."""
    if not current.written or _platform is None or _commit is None:
        current.written.clear()
        current.names.clear()
        return
    from provisa.core.environments import org_schema

    written = list(current.written.items())
    current.written.clear()
    actor = current.actor()
    for (plane, _db_id), (database, writes) in written:
        message = _message(current, writes)
        async with database.acquire() as conn:
            sha = await _commit(
                conn,
                _platform,
                plane.org_id,
                plane.env,
                org_schema(plane.org_id, plane.env),
                message,
                actor,
            )
        if sha is not None:
            log.debug("committed %s/%s as %s", plane.org_id, plane.env, sha)
    current.names.clear()


_WRITE = re.compile(
    r"""^\s*(?P<verb>INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+
        (?:(?P<schema>"[^"]+"|\w+)\s*\.\s*)?(?P<table>"[^"]+"|\w+)""",
    re.IGNORECASE | re.VERBOSE,
)


def raw_target(sql: str) -> tuple[str, str, str | None] | None:
    """The (verb, table, schema) a raw SQL write names, or None when ``sql`` is not one."""
    match = _WRITE.match(sql)
    if match is None:
        return None
    verb = match.group("verb").split()[0].lower()
    schema = match.group("schema")
    return (
        verb,
        match.group("table").strip('"'),
        schema.strip('"') if schema is not None else None,
    )


def core_target(stmt: Any) -> tuple[str, str, str | None] | None:
    """The (verb, table, schema) a SQLAlchemy Core statement writes, or None when it does not."""
    from sqlalchemy.sql.dml import Delete, Insert, Update

    for kind, verb in ((Insert, "insert"), (Update, "update"), (Delete, "delete")):
        if isinstance(stmt, kind):
            table = stmt.table
            return verb, str(getattr(table, "name", "")), getattr(table, "schema", None)
    return None


def commits_itself(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mark an async function as a path that commits the model itself: its body runs inside
    :func:`committed_by_caller`."""
    import functools

    @functools.wraps(fn)
    async def _wrapped(*args: Any, **kwargs: Any) -> Any:
        async with committed_by_caller():
            return await fn(*args, **kwargs)

    return _wrapped
