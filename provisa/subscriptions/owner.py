# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3f6c21-7e48-4b15-8d02-c6e1b5f7a943
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A client-started sink or subscription stream ends with its owner's role (REQ-812).

The owner is the principal whose request started it, and the roles that request acted as. Each is
re-read from where every sign-in's roles are read (``provisa.auth.held_assignments``) at most every
:data:`RECHECK` while it runs -- before the next change is served and while no change comes -- so a
role removed in the control plane, expired (REQ-2077), or dropped from a token's scopes ends it on
whichever process of whichever node it runs on. Nothing has to be told: the control plane is read.
"""

# Requirements: REQ-812

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

# How long a sink or stream may run on a role check: the most a removed role is outlived by.
RECHECK = 30.0

T = TypeVar("T")


class OwnerRoleLost(PermissionError):
    """The principal that started a sink or stream no longer holds the role it acts as."""

    code = "subscribe.owner_role_lost"

    def __init__(self, roles: tuple[str, ...]) -> None:
        named = ", ".join(roles)
        super().__init__(
            f"The subscription ended: its owner no longer holds the role it was started under ({named})"
        )
        self.roles = roles
        self.params = {"roles": named}


@dataclass(frozen=True)
class Owner:
    """Who started a sink or stream: the identity, the org it was bound to and the roles it acts
    as (a meta-role's members, else the one role)."""

    identity: Any
    org_id: str | None
    roles: tuple[str, ...]

    @property
    def user_id(self) -> str:
        return self.identity.user_id


def owner_of(raw_request: Any, state: Any, role_id: str) -> Owner:
    """The owner of what ``raw_request`` starts, acting as ``role_id``. The auth middleware sets the
    identity and the bound org on every request, secured or not."""
    from provisa.security.meta_role import acting_roles

    return Owner(
        identity=raw_request.state.identity,
        org_id=raw_request.state.active_org_id,
        roles=acting_roles(state, role_id),
    )


async def holds(state: Any, owner: Owner) -> bool:
    """Whether ``owner`` still holds every role it acts as."""
    from provisa.auth.bearer import auth_active

    if not auth_active(state, "subscriptions"):
        # REQ-535: with no auth provider every request is the anonymous dev principal acting as the
        # roles its own request names; no assignment grants them, so none can be taken away.
        return True
    from provisa.auth.held_assignments import held_assignments

    held = {a.role_id for a in await held_assignments(state, owner.identity, owner.org_id)}
    return set(owner.roles) <= held


async def ensure_held(state: Any, owner: Owner) -> None:
    """Raise :class:`OwnerRoleLost` unless ``owner`` still holds every role it acts as."""
    if not await holds(state, owner):
        raise OwnerRoleLost(owner.roles)


async def bounded_by_owner(
    changes: AsyncIterator[T],
    check: Callable[[], Awaitable[None]],
    every: float | None = None,
) -> AsyncIterator[T]:
    """``changes``, while ``check`` passes; what it raises (:func:`ensure_held`'s
    :class:`OwnerRoleLost`) ends the iteration.

    ``check`` runs at most once per ``every`` seconds, before a change is passed on and while none
    arrives, so a sink or stream with no changes ends as surely as a busy one. ``every`` unset is
    :data:`RECHECK`."""
    period = RECHECK if every is None else every
    iterator = changes.__aiter__()
    pending: asyncio.Future[T] = asyncio.ensure_future(iterator.__anext__())
    checked = time.monotonic()
    try:
        while True:
            wait = max(0.0, period - (time.monotonic() - checked))
            done, _ = await asyncio.wait({pending}, timeout=wait)
            if time.monotonic() - checked >= period:
                await check()
                checked = time.monotonic()
            if not done:
                continue
            try:
                item = pending.result()
            except StopAsyncIteration:
                return
            yield item
            pending = asyncio.ensure_future(iterator.__anext__())
    finally:
        if not pending.done():
            pending.cancel()
            try:
                await pending
            except (asyncio.CancelledError, StopAsyncIteration):
                pass
