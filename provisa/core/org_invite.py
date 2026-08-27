# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1c9f2e-77b6-4d31-9e08-51ad3c0b6742
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an invitation admits, and how many times (REQ-1594, REQ-1595, REQ-1596).

An invitation used to be one person, once: ``used_at`` was NULL or it was not, and five call sites
each said so in their own words. That shape cannot express the two things the product now needs.

THE OPEN INVITE (REQ-1594). A "Try it Out" link on the marketing site is addressed to nobody and
redeemed by everybody. It is the SAME object as the addressed invitation -- an org, a role, an
expiry, a creator to attribute it to, a revoke door -- differing only in how many redemptions it
admits. So the count became a column instead of the shape becoming a second feature: ``max_uses``
defaults to 1, which is exactly the invitation that already existed, and NULL means unlimited.
``used_at``/``used_by`` remain, now meaning the LAST redemption rather than the only one.

WHAT THE REDEEMER IS GIVEN (REQ-1595). ``env_policy`` decides. ``none`` is the ordinary invitation:
the member is served by the org. ``per_visitor`` mints a fresh environment per redemption and gives
it ``env_ttl_seconds`` to live -- the sandbox, where a stranger gets a real model on real machinery
and nothing they do outlives the hour. ``shared`` seats every redeemer in the one environment
``env_name`` names -- a branded portal an org publishes to people it has never met.

WHY THE VISITOR IS PINNED (REQ-1596). An environment is only a container if it is the only one the
member can be served by. The membership carries ``env_name``, and a pinned member naming nothing is
served that environment rather than prod -- otherwise the sandbox would be a place the visitor
could simply decline to go.
"""

# Requirements: REQ-1594, REQ-1595, REQ-1596

from __future__ import annotations

import uuid

from sqlalchemy import func

from provisa.core.schema_admin import org_invites

#: The ordinary invitation: membership in the org, served by the org (REQ-1595).
ENV_POLICY_NONE = "none"
#: A fresh environment per redemption, expiring ``env_ttl_seconds`` after it is minted.
ENV_POLICY_PER_VISITOR = "per_visitor"
#: Every redeemer seated in the one environment the invite names.
ENV_POLICY_SHARED = "shared"

ENV_POLICIES = (ENV_POLICY_NONE, ENV_POLICY_PER_VISITOR, ENV_POLICY_SHARED)

#: What a ``per_visitor`` environment is named after. Kept short because the name has 32 characters
#: to live in (``provisa.core.environments.MAX_ENV_NAME``) and the random half needs most of them.
SANDBOX_ENV_PREFIX = "sandbox_"


#: Everything a redemption path has to know about the invitation it is redeeming: who it admits to
#: which org as what, whether it is still live, and what the redeemer is to be given. One tuple
#: because both redemption paths (``/register`` and ``/redeem-invite``) decide from the same facts,
#: and a column added to one select but not the other is a difference in what the two admit.
INVITE_REDEMPTION_COLUMNS = (
    org_invites.c.org_id,
    org_invites.c.role_id,
    org_invites.c.expires_at,
    org_invites.c.uses,
    org_invites.c.max_uses,
    org_invites.c.env_policy,
    org_invites.c.env_ttl_seconds,
    org_invites.c.env_name,
)


def unspent():
    """The clause selecting invitations that still have a redemption left in them.

    ``max_uses IS NULL`` is unlimited, so it is unspent whatever ``uses`` has reached. Written once
    here because every gate -- my-invites, the invite preview, both redemption paths, and revoke --
    has to agree on it; five hand-written copies of a two-branch predicate is how they stop agreeing.
    """
    return org_invites.c.max_uses.is_(None) | (org_invites.c.uses < org_invites.c.max_uses)


def is_spent(invite: dict) -> bool:
    """Whether ``invite`` (a row carrying ``uses`` and ``max_uses``) has no redemption left.

    The Python counterpart of :func:`unspent`, for the paths that fetch the row and then decide --
    they need to tell "already used" apart from "expired" in the error they raise, which a WHERE
    clause cannot do.
    """
    max_uses = invite["max_uses"]
    return max_uses is not None and invite["uses"] >= max_uses


def spend(token: str, user_id: str):
    """The statement recording one redemption of ``token`` by ``user_id``.

    ``uses = uses + 1`` in SQL rather than a read-modify-write, and :func:`unspent` repeated in the
    WHERE: two people clicking the same link at once both read ``uses`` before either wrote it, and
    an in-Python increment would let both through the ceiling. The caller checks the returned row --
    no row means someone else took the last redemption between the read and here.
    """
    return (
        org_invites.update()
        .where(org_invites.c.token == token, unspent())
        .values(uses=org_invites.c.uses + 1, used_at=func.now(), used_by=user_id)
        .returning(org_invites.c.token)
    )


def sandbox_env_name() -> str:
    """A name for one visitor's environment, unique per redemption (REQ-1595).

    Random rather than derived from the redeemer: an environment name appears in a schema name, and
    deriving it from a user id would put an identity into the database's own namespace. Random also
    means a visitor who redeems twice gets two environments, which is what "per visitor" promises --
    the second visit is not handed the first one's leftovers.
    """
    return f"{SANDBOX_ENV_PREFIX}{uuid.uuid4().hex[:12]}"
