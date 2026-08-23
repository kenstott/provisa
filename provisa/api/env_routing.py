# Copyright (c) 2026 Kenneth Stott
# Canary: 9f8ea78b-6572-466c-b41a-6b261bd12a6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which environment a request is served by (REQ-1487, REQ-1488, REQ-1529).

A request names an environment in the ``x-provisa-env`` header, and one that names none is served
by ``prod``. That is the whole selection rule; what this module adds is that the name is CHECKED
before anything is bound to it.

WHY IT IS CHECKED AND NOT TRUSTED. Binding an environment picks the schema every repository query
reads and the runtime whose source pools and bindings answer it. An unchecked name would reach
:func:`provisa.core.db.init_schema` through the runtime build, which creates the schema it is given
-- so a typo would not fail, it would silently CREATE a nameless empty environment and serve it.
The environment must already exist in the org's ``environments`` table, and the org is the
authenticated one: an environment is a copy of ONE org's model, so naming another org's
environment is not a lookup that can succeed.

WHY IT REFUSES RATHER THAN FALLING BACK TO PROD. An unknown environment served as prod is the worst
available answer: a caller who believed they were writing to a branch would write to production and
get a success. A name that cannot be honoured is a 404.

WHO MAY BE SERVED BY ONE (REQ-1573). Naming a non-prod environment needs the ``environment_switch``
right. The check belongs HERE, at the selection, because the environment is bound before any route
is reached — so one gate covers HTTP, GraphQL, SQL and the wire protocols at once, and a surface
added later inherits it rather than needing to remember it. prod needs no right: it is what a
request naming nothing is served by, and refusing it would refuse every request.
"""

# Requirements: REQ-1487, REQ-1488, REQ-1529, REQ-1573

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.core.environments import PROD, is_env_name

if TYPE_CHECKING:
    from provisa.core.database import Database

#: The header carrying the selection (REQ-1487). An environment is not a DNS label and not a path
#: segment: the same URL serves every environment, and the header says which.
ENV_HEADER = "x-provisa-env"


#: The right to be served by an environment other than prod (REQ-1573).
SWITCH_CAPABILITY = "environment_switch"


class EnvironmentSelectionError(Exception):
    """The request named an environment that cannot be served, with the reason it cannot."""


class EnvironmentRightError(Exception):
    """The request named an environment the caller holds no right to be served by (REQ-1573).

    Distinct from :class:`EnvironmentSelectionError` because the answers differ: that one means the
    environment does not exist (404), this one means it does and you may not have it (403). Telling
    an analyst their branch name is unknown would be a lie about the org's model.
    """


def may_switch(capabilities: set[str]) -> bool:
    """Whether a principal holding ``capabilities`` may be served by a non-prod environment.

    The platform administrator bypasses this as they bypass every capability gate (REQ-1297).
    """
    from provisa.security.rights import has_platform_bypass

    return SWITCH_CAPABILITY in capabilities or has_platform_bypass(capabilities)


def env_header_value(headers) -> str | None:
    """The environment a raw ASGI header list names, or ``None`` if it names none.

    Accepts the ASGI ``[(b"name", b"value"), ...]`` list because the org routing middleware is
    plain ASGI (it has no ``Request``); an empty header is the same as an absent one, since a client
    that sends the header blank has named nothing.
    """
    for name, value in headers:
        if name.lower() == ENV_HEADER.encode():
            decoded = value.decode("latin-1").strip()
            return decoded or None
    return None


async def select_environment(
    admin_db: "Database | None",
    org_id: str,
    requested: str | None,
    capabilities: set[str] | None = None,
) -> str:
    """The environment ``org_id`` is to be served in, refusing a name it cannot be served in.

    ``None`` and ``prod`` both give ``prod`` without a lookup: prod exists for every org from
    creation (REQ-1487) and cannot be deleted, so there is nothing a query could tell us.
    """
    if requested is None or requested == PROD:
        return PROD
    if admin_db is None:
        # Not a selection failure and so not a 404: the environments table lives on the admin plane,
        # and a request naming an environment before that plane is bound reached a server that
        # cannot answer the question at all.
        raise RuntimeError(
            f"cannot honour {ENV_HEADER}={requested!r}: no admin control plane is bound"
        )
    if not is_env_name(requested):
        raise EnvironmentSelectionError(f"{requested!r} is not a legal environment name")
    # REQ-1573: the right is checked before the lookup, so a caller who may not switch learns
    # nothing about which environments the org has. ``None`` is dev/no-auth mode — the same
    # exemption every other capability gate makes (provisa.api.admin.capabilities), and the reason
    # it is None rather than an empty set: an unauthenticated deployment has no roles to read, which
    # is not the same as a principal whose roles carry nothing.
    if capabilities is not None and not may_switch(capabilities):
        raise EnvironmentRightError(
            f"being served by environment {requested!r} requires the "
            f"{SWITCH_CAPABILITY!r} capability"
        )
    from provisa.core.env_store import get_env

    if await get_env(admin_db, org_id, requested) is None:
        raise EnvironmentSelectionError(f"org {org_id!r} has no environment named {requested!r}")
    return requested
