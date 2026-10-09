# Copyright (c) 2026 Kenneth Stott
# Canary: 0a6f3e82-9d17-4c5b-b2e4-51c8d7a90f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a deployment reads its users' role assignments from, and when that is refused.

``auth.assignments_source`` is ``claims`` (the roles a user's sign-in carries) or ``provisa``
(the assignments Provisa holds, per org). A multi-tenant deployment grants roles itself — an
invitation, auto-join, the org_admin of a new org — and holds them per org; one that read
assignments from sign-in claims would ignore every one of those grants. That combination is
refused wherever a configuration is taken in: the model file at load, the deployment at boot,
and a settings save that would produce it. A single-tenant deployment may read claims.
"""

from __future__ import annotations

CLAIMS = "claims"
PROVISA = "provisa"
ASSIGNMENTS_SOURCES: tuple[str, ...] = (CLAIMS, PROVISA)
_NO_PROVIDER = (None, "", "none")


class ClaimsWithMultitenancy(ValueError):
    """A multi-tenant deployment configured to read role assignments from sign-in claims."""

    def __init__(self) -> None:
        super().__init__(
            "multitenancy: true cannot be combined with auth.assignments_source: claims -- a "
            "multi-tenant deployment grants roles itself, per org (invitations, auto-join, an "
            "org's own admin), and sign-in claims would override every one of them. Set "
            "auth.assignments_source: provisa."
        )


def require_assignments_source(multitenancy: bool, auth: dict | None) -> None:
    """Refuse (:class:`ClaimsWithMultitenancy`) a multi-tenant configuration whose ``auth``
    block reads assignments from claims — stated, or by leaving the setting at its default.

    With no auth provider nobody signs in and no assignment is read, so there is nothing to
    refuse; a single-tenant deployment is never refused.
    """
    if not multitenancy or not auth or auth.get("provider") in _NO_PROVIDER:
        return
    if auth.get("assignments_source", CLAIMS) == CLAIMS:
        raise ClaimsWithMultitenancy()
