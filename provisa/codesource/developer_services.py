# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1c7a3e-9b42-4f6e-a0d8-2e7f4c9b1a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's services to a signed-in developer's code, outside Provisa (REQ-2106, amended
2026-11-22; REQ-2112).

The VS Code extension sets ``PROVISA_SERVICES`` to
``<instance>/admin/orgs/<org>/code-sources/<source>/services/<personal access token>``; the
module appends ``/<service>/<call>`` and sends no header of its own, so the token is part of the
address, as a worker's is. A call is answered exactly as a worker's is
(:func:`provisa.codesource.services.answer_grant`), under the grant the source's worker gets, and
as the role the token acts as.

It is refused, by name, when the token is unknown, revoked, suspended or expired; when it is
another organization's; when its owner holds no role there or not ``source_registration`` (what
puts a source in dev mode and declares its secrets); and when the source is not one of the org's
code sources in dev mode. Every call the token reaches is in the org's administrative trail: the
developer, the token's label, the source, the service and call, and the answer's status. A secret
read is also audited as a worker's is.
"""

# Requirements: REQ-2106, REQ-2112
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.codesource.services import ServiceGrant, ServiceRefused

#: The audit trail's action for one services call a developer's token made.
DEVELOPER_CALL = "code_source.services.developer_call"


@dataclass(frozen=True)
class Developer:
    """Who a call is made by: the token's owner, the token's label and the role it acts as."""

    user_id: str
    token_name: str
    role: str


async def resolve(
    state: Any, org_id: str, source_id: str, token: str
) -> tuple[ServiceGrant, Developer]:
    """The grant ``token`` reaches ``source_id`` of ``org_id`` with, and who holds it; refused by
    name otherwise. Called with ``org_id`` bound as the current org; its runtime is built only
    once the token is seen to be one of its own. A refusal of the source is audited."""
    from provisa.auth.pat import PersonalAccessTokenStore, TokenRefused, is_personal_access_token

    if not is_personal_access_token(token):
        raise ServiceRefused(401, "the services address carries no personal access token")
    try:
        identity = await PersonalAccessTokenStore(state.admin_db).resolve(token)
    except TokenRefused as refused:
        raise ServiceRefused(401, str(refused)) from None
    token_name = str(identity.raw_claims["token_name"])
    if identity.active_org_id != org_id:
        raise ServiceRefused(
            403,
            f"the personal access token {token_name!r} is another organization's; it reaches "
            f"no source of {org_id!r}",
        )
    from provisa.api.app import ensure_org_runtime

    await ensure_org_runtime(org_id)
    developer = Developer(identity.user_id, token_name, await _role(state, identity, org_id))
    try:
        return await _grant(state, org_id, source_id, developer), developer
    except ServiceRefused as refused:
        await audit(org_id, source_id, developer, "", "", refused.status)
        raise


async def _role(state: Any, identity: Any, org_id: str) -> str:
    """The role the token acts as, as on every transport's sign-in, once it is seen to carry
    ``source_registration``."""
    from provisa.auth.held_assignments import sign_in_roles
    from provisa.security.rights import Capability, capabilities_for_claims

    name = identity.raw_claims["token_name"]
    try:
        role, roles = await sign_in_roles(state, identity, org_id)
    except PermissionError as refused:
        raise ServiceRefused(
            403, f"the personal access token {name!r} acts as no role in {org_id!r}: {refused}"
        ) from None
    if Capability.SOURCE_REGISTRATION.value not in capabilities_for_claims(roles, state.roles):
        raise ServiceRefused(
            403,
            f"the personal access token {name!r} does not carry source registration in "
            f"{org_id!r}, which a dev-mode source's services need",
        )
    return role


async def _grant(state: Any, org_id: str, source_id: str, developer: Developer) -> ServiceGrant:
    """The grant a worker of ``source_id`` is given (provisa.codesource.source.granted), acting
    as the developer's role; refused unless the source is a code source in dev mode."""
    from provisa.codesource import remote_sources
    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.languages import LANGUAGES
    from provisa.codesource.source import config_of
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    if not found:
        raise ServiceRefused(404, f"{org_id!r} has no source {source_id!r}")
    source = found[0]
    if source.type.value not in LANGUAGES:
        raise ServiceRefused(403, f"the source {source_id!r} is not a code source")
    try:
        if remote_sources.mode_of(source.mapping) != remote_sources.DEV:
            raise ServiceRefused(
                403,
                f"the source {source_id!r} is not in dev mode; its services are its workers' only",
            )
        config = config_of(source.type.value, source.mapping)
    except BundleRefused as refused:
        raise ServiceRefused(409, f"the source {source_id!r} cannot be used: {refused}") from None
    return ServiceGrant(
        org_id=org_id,
        source_id=source_id,
        secrets=config.secrets,
        admin_db=state.admin_db,
        llm_cap_usd=config.llm_cap_usd,
        settings=config.settings,
        egress=tuple(state.udf_egress_allowlist),
        role=developer.role,
    )


async def audit(
    org_id: str, source_id: str, developer: Developer, service: str, call: str, status: int
) -> None:
    """One call in the org's trail: the developer, the token's label, the source, the service and
    call (none for a call refused before its service is reached), and the answer's status; the
    time is the row's own. Never a value."""
    from provisa.api.admin.orgs_router import _org_model_db
    from provisa.core.org_membership import record_admin_action

    await record_admin_action(
        await _org_model_db(org_id),
        org_id=org_id,
        action=DEVELOPER_CALL,
        actor_id=developer.user_id,
        subject_id="/".join(part for part in (service, call) if part) or source_id,
        detail={
            "source": source_id,
            "service": service,
            "token": developer.token_name,
            "role": developer.role,
            "status": status,
        },
    )
