# Copyright (c) 2026 Kenneth Stott
# Canary: f1a2b3c4-d5e6-7890-1234-56789abcdef0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Authorization required for every EngineRuntime.execute_engine call (REQ-1760).

No caller may pass a bare SQL string to execute_engine with no accountability — not even
an internal/privileged one. Every call names what authorizes it, checked against a
DIFFERENT test depending on the type:

  - GovernedPlanAuth: a query response. Re-checks the SAME pipeline stamp
    require_governed_plan (REQ-1176) already verified upstream of a _Plan-based call —
    execute_engine itself never trusts a caller's self-report, even one that already
    passed the Plan-level gate.
  - SystemAuth: the system's own internal work (MV refresh, cache warming, catalog
    introspection) — the Trusted Subsystem pattern: the system authenticates as itself,
    not as an impersonated user. Proven by a token minted only at genuine internal call
    sites (mint_system_token), never by request-handling code. When `expected_sql` is
    set (a landing execution such as MV refresh), the SQL actually executed must match
    it exactly, so a system token can never be reused to run a different query than the
    one that was reviewed and registered — this is what closes MV registration as a
    privilege-escalation surface (REQ-1756 makes refresh privileged by design; this
    makes "privileged" mean "runs exactly what was registered," not "runs anything").

Where the physical target itself has native row-security enforcement (e.g. Postgres RLS/
BYPASSRLS), that is a second, useful layer — but most sources here (NoSQL connectors,
API-based sources, JDBC-via-driver sources) have no such feature, so this module is the
one mechanism that generalizes across every target.
"""

from __future__ import annotations

import secrets
from collections import deque
from dataclasses import dataclass
from typing import Union

_ISSUED_SYSTEM_TOKENS: deque[str] = deque(maxlen=4096)
_ISSUED_SYSTEM_SET: set[str] = set()


def mint_system_token() -> str:
    """Issue a token proving the SYSTEM ITSELF — not a caller impersonating it — is
    making this call. Call this only from genuine internal call sites (a scheduled
    job, MV refresh, cache warming, catalog introspection), never from request-handling
    code reachable by an external caller."""
    token = secrets.token_hex(32)
    if len(_ISSUED_SYSTEM_TOKENS) == _ISSUED_SYSTEM_TOKENS.maxlen:
        _ISSUED_SYSTEM_SET.discard(_ISSUED_SYSTEM_TOKENS[0])
    _ISSUED_SYSTEM_TOKENS.append(token)
    _ISSUED_SYSTEM_SET.add(token)
    return token


def _system_token_is_valid(token: str) -> bool:
    return bool(token) and token in _ISSUED_SYSTEM_SET


@dataclass(frozen=True)
class GovernedPlanAuth:
    """A query response already verified by require_governed_plan (REQ-1176)."""

    stamp: str


@dataclass(frozen=True)
class SystemAuth:
    """The system's own internal work. `reason` is a short human-readable purpose,
    recorded for audit/logging. `expected_sql`, when set, pins this token to exactly
    one statement — required for any landing execution (e.g. MV refresh) so a system
    token cannot be reused to run a different query than the one it was minted for."""

    token: str
    reason: str
    expected_sql: str | None = None


ExecutionAuthorization = Union[GovernedPlanAuth, SystemAuth]


def verify_execution_authorization(auth: ExecutionAuthorization, sql: str) -> None:
    """Raise PermissionError unless ``auth`` legitimately authorizes executing ``sql``."""
    if isinstance(auth, GovernedPlanAuth):
        from provisa.pgwire._pipeline import stamp_is_valid

        if not stamp_is_valid(auth.stamp):
            raise PermissionError(
                "execute_engine refused: invalid/missing governed-plan stamp — every "
                "query response must be produced by the one governed pipeline "
                "(_govern_and_route / _govern_and_route_compiled)"
            )
        return

    if isinstance(auth, SystemAuth):
        if not _system_token_is_valid(auth.token):
            raise PermissionError(
                "execute_engine refused: invalid/missing system authorization token — "
                "only a genuine internal call site may mint one"
            )
        if auth.expected_sql is not None and sql != auth.expected_sql:
            raise PermissionError(
                f"execute_engine refused: SQL does not match the authorized definition "
                f"for {auth.reason!r} — a system token cannot be reused to run a "
                "different query than the one it was minted for"
            )
        return

    # Statically unreachable given ExecutionAuthorization's declared type, but Python does not
    # enforce type hints at runtime — a caller ignoring them must still be refused, not trusted.
    raise PermissionError(  # pyright: ignore[reportUnreachable]
        f"execute_engine refused: unrecognized authorization {auth!r}"
    )
