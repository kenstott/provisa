# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1d9b77-6c02-4e15-9f83-2d70bb4c5e91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The acting principal an audit row is written for.

REQ-074/REQ-1386: ``query_audit_log`` records WHO ran a statement and over WHICH protocol. The one
query pipeline (:mod:`provisa.pgwire._pipeline`) is handed a role, never a user — every surface
authenticates its caller at its own boundary. This module is the single channel by which that
boundary hands the acting principal down to the pipeline, so the audit write lives in the pipeline
(one place, every surface) rather than being re-implemented per transport.

A context with no identity bound is a statement with no acting principal — startup seeding, schema
rebuilds, materialization refreshes. Those are not user queries and are not audited; the pipeline
checks :func:`current_audit_identity` for None and writes nothing. Every caller-facing surface
binds one, and the surface-coverage test asserts that.

A deployment with no auth provider authenticates nobody, but its requests are still requests:
every surface attributes them to :data:`ANONYMOUS_USER` (pgwire and Bolt to the principal name
the client supplied, when it supplied one), so a transport is audited with or without auth.
"""

# Requirements: REQ-074, REQ-1386

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field


@dataclass(frozen=True)
class AuditIdentity:
    """The authenticated caller behind the statements run in this context.

    user_id  the authenticated principal (audit log's ``user_id``)
    surface  the protocol the statement arrived on — the audit log's ``source``, which
             ``surface_mix``/``query_health`` group by (e.g. ``http``, ``pgwire``, ``flight``,
             ``grpc``, ``mcp``, ``bolt``)
    """

    user_id: str
    surface: str


# The principal an unauthenticated deployment's requests are attributed to — the ``anonymous``
# dev identity the HTTP auth middleware binds in its no-auth mode.
ANONYMOUS_USER = "anonymous"

_current_identity: ContextVar[AuditIdentity | None] = ContextVar(
    "provisa_audit_identity", default=None
)


def set_audit_identity(identity: AuditIdentity) -> Token:
    """Bind the acting principal for this context. Returns a token for :func:`reset_audit_identity`."""
    return _current_identity.set(identity)


def reset_audit_identity(token: Token) -> None:
    _current_identity.reset(token)


def current_audit_identity() -> AuditIdentity | None:
    """The bound principal, or None when nothing user-initiated is running in this context."""
    return _current_identity.get()


@contextmanager
def audit_identity_scope(user_id: str, surface: str):
    """Bind ``user_id``/``surface`` for the duration of the block."""
    token = _current_identity.set(AuditIdentity(user_id=user_id, surface=surface))
    try:
        yield
    finally:
        _current_identity.reset(token)


async def with_audit_identity(user_id: str, surface: str, coro):
    """Await ``coro`` with the acting principal bound INSIDE it.

    Surfaces that govern on the main event loop from a worker thread (pgwire, Flight SQL, gRPC)
    submit via ``run_coroutine_threadsafe``, which does not carry ContextVars across the thread
    boundary — the identity set on the worker thread is invisible to the loop-side coroutine that
    calls :func:`provisa.audit.pipeline.begin_audit`. Wrapping the coroutine binds it where it runs.
    """
    token = _current_identity.set(AuditIdentity(user_id=user_id, surface=surface))
    try:
        return await coro
    finally:
        _current_identity.reset(token)


# ---------------------------------------------------------------------------------------------
# What a request's own terminal reports for its audit row.
#
# A surface that executes at its own terminal (``/data/graphql``) writes one row per REQUEST, and
# the facts that row needs — the route each field was answered by, the rows returned, whether a
# statement of the request already wrote a row of its own — are produced deeper in the call than
# the place the row is written. They are noted here, on an object bound for the request.
# ---------------------------------------------------------------------------------------------


@dataclass
class RequestAudit:
    routes: list[str] = field(default_factory=list)
    rows: int = 0
    # Rows already written for statements this request ran (an action field's governed statement).
    statements_audited: int = 0

    def route(self) -> str | None:
        """The route the request was answered by; several, when its fields took different ones."""
        return ",".join(sorted(set(self.routes))) or None


_current_request_audit: ContextVar[RequestAudit | None] = ContextVar(
    "provisa_request_audit", default=None
)


def bind_request_audit() -> RequestAudit:
    """Bind a fresh :class:`RequestAudit` in the current context and return it. Called by the
    request's own handler, whose context ends with the request; every task the request spawns
    shares the object (a task copies the context, not what it holds)."""
    outcome = RequestAudit()
    _current_request_audit.set(outcome)
    return outcome


def note_request_route(route: str) -> None:
    outcome = _current_request_audit.get()
    if outcome is not None:
        outcome.routes.append(route)


def note_request_rows(rows: int) -> None:
    outcome = _current_request_audit.get()
    if outcome is not None:
        outcome.rows += rows


def note_statement_audited() -> None:
    """A statement of the current request wrote its own audit row."""
    outcome = _current_request_audit.get()
    if outcome is not None:
        outcome.statements_audited += 1
