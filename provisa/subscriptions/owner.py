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

Where the roles are the sign-in's own claims (``auth.assignments_source: claims``) the claims an
identity was signed in with are not read again by ``held_assignments``, so the sign-in itself is
(:func:`_sign_in_now`): a provider that can say what the identity holds now
(``AuthProvider.rereads``: basic, simple, LDAP, Firebase) is asked; one that cannot (OAuth/OIDC,
Keycloak, SAML: a signed token or assertion, with no back channel to ask again) is trusted only
until the sign-in's token expires, and then the sink or stream ends.
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


class OwnerRightEnded(PermissionError):
    """The right a sink or stream was started under has ended; ``code`` and ``params`` tell the
    client why."""

    code: str
    params: dict[str, str]


class OwnerRoleLost(OwnerRightEnded):
    """The principal that started a sink or stream no longer holds the role it acts as."""

    code = "subscribe.owner_role_lost"

    def __init__(self, roles: tuple[str, ...]) -> None:
        named = ", ".join(roles)
        super().__init__(
            f"The subscription ended: its owner no longer holds the role it was started under ({named})"
        )
        self.roles = roles
        self.params = {"roles": named}


class OwnerSignInExpired(OwnerRightEnded):
    """The sign-in a sink or stream was started under expired, and its provider cannot be asked
    what the identity holds now."""

    code = "subscribe.owner_sign_in_expired"

    def __init__(self) -> None:
        super().__init__(
            "The subscription ended: the sign-in it was started under has expired; "
            "sign in again and start it again"
        )
        self.params = {}


def _now() -> float:
    """The wall clock a token's ``exp`` is read against."""
    return time.time()


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
    from provisa.auth.held_assignments import held_assignments, token_scopes
    from provisa.core.assignments_source import CLAIMS

    identity = owner.identity
    # The default is held_assignments' own: the source every sign-in's roles are read from.
    if (
        state.auth_config.get("assignments_source", CLAIMS) == CLAIMS
        and token_scopes(identity) is None
    ):
        identity = await _sign_in_now(state, owner)
    held = {a.role_id for a in await held_assignments(state, identity, owner.org_id)}
    return set(owner.roles) <= held


async def _sign_in_now(state: Any, owner: Owner) -> Any:
    """``owner``'s identity as its sign-in stands now, where its roles are its claims: read again
    from a provider that can be asked; else the identity it signed in with, until its token's
    ``exp``, after which :class:`OwnerSignInExpired`. An account its provider no longer signs in
    holds no role (:class:`OwnerRoleLost`)."""
    identity = owner.identity
    if "superuser" in identity.raw_claims:
        # REQ-125: the break-glass identity is no provider's: its roles are the configuration's,
        # held while the configured superuser is this account.
        from provisa.auth.superuser import resolve_superuser_config

        configured = resolve_superuser_config(state.auth_config.get("superuser"))
        if configured is None or configured["username"] != identity.user_id:
            raise OwnerRoleLost(owner.roles)
        return identity
    from provisa.auth.wiring import build_auth_provider

    provider = build_auth_provider(state.auth_config, admin_pool=getattr(state, "admin_db", None))
    if provider.rereads:
        try:
            return await provider.reread(identity)
        except ValueError as gone:
            # A provider raises ValueError for an account that no longer signs in, as sign-in does.
            raise OwnerRoleLost(owner.roles) from gone
    if _now() >= identity.raw_claims["exp"]:
        raise OwnerSignInExpired()
    return identity


async def ensure_held(state: Any, owner: Owner) -> None:
    """Raise :class:`OwnerRoleLost` unless ``owner`` still holds every role it acts as, or
    :class:`OwnerSignInExpired` once a sign-in that cannot be read again has expired."""
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
