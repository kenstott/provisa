# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The central redaction rules (REQ-2087): what applies to the acting org, the org's own rules
(``org_settings``), and the deployment's (``platform_settings``)."""

# Requirements: REQ-2087

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import (
    has_platform_settings,
    require_org_settings,
    require_platform_settings,
)
from provisa.api.errors import ApiError
from provisa.api.redaction_load import load_deployment_rules, refresh_org
from provisa.security import redaction

router = APIRouter()


class RuleBody(BaseModel):
    name: str
    pattern: str | None = None
    reference: str | None = None


class RulesBody(BaseModel):
    rules: list[RuleBody]


def _parsed(body: RulesBody, source: str) -> list[dict]:
    raw = [r.model_dump() for r in body.rules]
    try:
        rules = redaction.parse_rules(raw, source)
    except redaction.RuleInvalid as exc:
        raise ApiError(400, "redaction.rule_invalid", str(exc), rule=exc.rule, why=exc.why) from exc
    return [{"name": r.name, "pattern": r.pattern, "reference": r.reference} for r in rules]


def _updated_by(request: Request) -> str:
    identity = getattr(request.state, "identity", None)
    return getattr(identity, "user_id", "anonymous")


async def _described(request: Request, org_id: str) -> dict:
    return {
        **redaction.described(org_id),
        "can_edit_deployment": has_platform_settings(request),
    }


@router.get("/admin/redaction")
async def get_redaction(request: Request) -> dict:
    """Every rule that applies to the acting org, by where it is set; the automatic rule's
    secrets by name only, never a value."""
    require_org_settings(request)
    return await _described(request, require_active_org_id(request))


@router.put("/admin/redaction")
async def put_org_redaction(body: RulesBody, request: Request) -> dict:
    """Replace the acting org's own rules."""
    require_org_settings(request)
    from provisa.api.app import state
    from provisa.core.org_settings import write_org_overrides

    org_id = require_active_org_id(request)
    rules = _parsed(body, "org")
    assert state.model_db is not None
    await write_org_overrides(
        state.model_db,
        {redaction.ORG_BLOCK: {"rules": rules} if rules else None},
        updated_by=_updated_by(request),
    )
    await refresh_org(org_id, every_environment=True)
    return await _described(request, org_id)


@router.put("/admin/redaction/deployment")
async def put_deployment_redaction(body: RulesBody, request: Request) -> dict:
    """Replace the deployment's rules, which apply in every org."""
    require_platform_settings(request)
    from provisa.api.app import state
    from provisa.core import deployment_settings

    rules = _parsed(body, "deployment")
    assert state.admin_db is not None
    deployment_settings.write(
        state.admin_db,
        {redaction.DEPLOYMENT_KEY: rules or None},
        updated_by=_updated_by(request),
    )
    await load_deployment_rules()
    return await _described(request, require_active_org_id(request))
