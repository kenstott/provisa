# Copyright (c) 2026 Kenneth Stott
# Canary: 67a879a8-4103-4a1b-b49a-9fc6a74c3e4b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A person's effective rows recomputed in a named org, after a role grant there changed
(REQ-2219).

A membership, an invitation, an org's creation or a person's removal writes role grants into an
org that is often not the one the request is bound to (an auto-join at sign-in, a platform
administrator acting on another org, a claim that acts in no org). The rows are computed from that
org's prod runtime, so it is bound here for the computation, and written to its model store.
"""

# Requirements: REQ-2219

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Connection


@asynccontextmanager
async def _prod_store(org_id: str) -> AsyncIterator["Connection"]:
    """A connection to ``org_id``'s prod model store, with the org and its prod runtime bound."""
    from provisa.api.app import ensure_org_runtime
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    rt = await ensure_org_runtime(org_id)
    if rt.model_db is None:
        raise RuntimeError(f"org {org_id!r} has no model store to record effective rows in")
    org_token = set_current_org(org_id)
    env_token = set_current_env(None)
    try:
        async with rt.model_db.acquire() as conn:
            yield conn
    finally:
        reset_current_env(env_token)
        reset_current_org(org_token)


async def record_people(org_id: str, principals: Iterable[str]) -> None:
    """Recompute each of ``principals``' effective rows in ``org_id``'s prod model, after a role
    grant there was written or removed."""
    from provisa.api.app import state
    from provisa.entitlements import effective_store

    people = sorted(set(principals))
    if not people:
        return
    async with _prod_store(org_id) as conn:
        for principal in people:
            await effective_store.write_person(conn, state, principal)


async def record_removed_account(removed: dict) -> None:
    """After an account's removal (``removed``, as ``org_membership.remove_account`` returns it):
    in every org it left, every list held for it, Provisa's and the identity provider's, is ended
    in the store and its rows rewritten to its tombstone, the identity provider's are dropped from
    the runtime, and its effective rows are closed and rewritten to its tombstone. Nothing is
    recomputed for it: a recompute could give it a role by default. With nothing in the store
    naming it, no later model build lists it among the people it knows or gives it a role
    (REQ-2219)."""
    from provisa.api.app import state
    from provisa.auth.middleware import forget_person_idp
    from provisa.entitlements import effective_store, store

    user_id = removed["deleted"]
    for org_id in removed["left_orgs"]:
        async with _prod_store(org_id) as conn:
            await store.remove_person(conn, user_id, removed["tombstone"])
            state.idp_entitlements = {
                p: lists for p, lists in state.idp_entitlements.items() if p != user_id
            }
            await effective_store.remove_person(conn, user_id, removed["tombstone"])
    forget_person_idp(user_id)
