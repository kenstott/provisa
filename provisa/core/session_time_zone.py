# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1f0c7a-93b4-4d26-a8e1-6b7d20c4f915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The TimeZone the statement in flight runs in, for whatever runs it (REQ-2171). pgwire binds it
around each statement of a session not on UTC; every other surface leaves it unbound, and an unbound
statement runs on UTC as it always did.

It is told to an engine on the statement's own cursor, connection or request and never on anything
another request shares. Each engine reads a different set of names, so each takes the spelling it
reads. A zone an engine reads no spelling of is refused where the answer could depend on it."""

# Requirements: REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass
from functools import wraps
from typing import ParamSpec, TypeVar

from provisa.core.pg_error import PostgresError


_P = ParamSpec("_P")
_R = TypeVar("_R")


@dataclass(frozen=True)
class StatementZone:
    """A session's zone in the spellings engines read. ``name`` is a zone of the tz database,
    ``offset`` a fixed offset such as +05:30, and ``posix`` the zone as PostgreSQL itself reads it.
    None: not expressible that way. ``shown`` names it in a refusal and in a cache key."""

    shown: str
    name: str | None = None
    offset: str | None = None
    posix: str | None = None


current_zone: ContextVar[StatementZone | None] = ContextVar("current_zone", default=None)

#: What a statement must spell for its answer to depend on the zone: reading an instant's parts or
#: its text, or the clock. The cost of matching one that does not depend on it is a refusal.
_ZONE_MATTERS = re.compile(
    r"::|\b(?:TRY_)?CAST\b|\bEXTRACT\b|\bDATE\w*|\bTO_\w+|\bSTRFTIME\b|\bFORMAT\w*|\bTIME\w*"
    r"|\bNOW\b|\bCURRENT_\w+|\bLOCALTIME\w*|\bAGE\b|\bMAKE_\w+|\bAT\s+TIME\s+ZONE\b",
    re.IGNORECASE,
)


def bind_zone(zone: StatementZone | None) -> Token[StatementZone | None]:
    return current_zone.set(zone)


def reset_zone(token: Token[StatementZone | None]) -> None:
    current_zone.reset(token)


def carrying_zone(run: Callable[_P, _R]) -> Callable[_P, _R]:
    """``run`` for another thread: ``run_in_executor`` does not carry a contextvar. Only the zone
    goes with it, so what such a thread does of the request's other contextvars does not change."""
    zone = current_zone.get()

    @wraps(run)
    def in_zone(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        token = bind_zone(zone)
        try:
            return run(*args, **kwargs)
        finally:
            reset_zone(token)

    return in_zone


def zone_cache_key() -> str | None:
    """What keeps an answer given in one zone from being served in another: None on UTC, so an
    entry written by a session on UTC or by another surface is the same entry as before."""
    zone = current_zone.get()
    return None if zone is None else zone.shown


def told_as(sql: str, spelling: Callable[[StatementZone], str | None], engine: str) -> str | None:
    """The zone to tell ``engine`` for ``sql``, in the ``spelling`` it reads: None when nothing is
    to be told. A zone the engine reads no spelling of is refused if ``sql`` could depend on it."""
    zone = current_zone.get()
    if zone is None:
        return None
    told = spelling(zone)
    if told is None and _ZONE_MATTERS.search(sql):
        raise PostgresError(
            "0A000", f"{engine} cannot run this statement in the session TimeZone {zone.shown}"
        )
    return told
