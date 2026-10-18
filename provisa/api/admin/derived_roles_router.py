# Copyright (c) 2026 Kenneth Stott
# Canary: cc9bd83c-205d-4559-b495-325d375354af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.


"""The derived roles an org declares (REQ-2057, REQ-2078, REQ-2079).

Each declared attribute is a rightless parent role, and each value a member's attributes give it a
value role ``<attribute>__<value>``. The org's own list (org_settings ``derived_roles``) replaces
the deployment config's: its first change copies the deployment's list and changes the copy. Each
entry carries a required description (REQ-2080). Read with the values each attribute has been seen
with and what names it; gated by ``user_management``, as roles are.
"""

# Requirements: REQ-2057, REQ-2078, REQ-2079, REQ-2080

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel, ValidationError

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.security.rights import Capability

router = APIRouter(prefix="/admin/derived-roles", tags=["admin"])

ORG = "org"
DEPLOYMENT = "deployment"


class DescriptionBody(BaseModel):
    description: str


def _db(state):
    if state.model_db is None:
        raise ApiError(503, "derived_roles.database_not_connected", "Database not connected")
    return state.model_db


@router.get("")
async def list_derived_roles(request: Request) -> dict:
    """Each declared derived role: its attribute, description, the values seen and what names it;
    and whether the list is the org's own or the deployment's."""
    require_capability_request(request, Capability.USER_MANAGEMENT.value)
    from provisa.api.app import state
    from provisa.auth.derived_roles import declared_derived_role_entries
    from provisa.core.repositories import derived_role as repo

    entries = declared_derived_role_entries(state)
    out = []
    async with _db(state).acquire() as conn:
        for e in entries:
            out.append(
                {
                    "attribute": e.attribute,
                    "description": e.description,
                    "values": await repo.values_seen(conn, e.attribute),
                    "uses": [
                        {"kind": d.ref.kind, "name": d.name, "via": list(d.via)}
                        for d in await repo.uses(conn, e.attribute)
                    ],
                }
            )
    source = ORG if "derived_roles" in state.settings_overrides else DEPLOYMENT
    return {"roles": out, "source": source}


async def _store(request: Request, entries: list[dict]) -> None:
    from provisa.api.app import state
    from provisa.core.org_settings import read_org_overrides, write_org_overrides

    model_db = _db(state)
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        model_db, {"derived_roles": entries}, updated_by=getattr(identity, "user_id", "anonymous")
    )
    # A request's derived roles are read off the bound runtime: refreshed here, as settings are.
    state.settings_overrides = await read_org_overrides(model_db)


@router.put("/{attribute}")
async def put_derived_role(attribute: str, body: DescriptionBody, request: Request) -> dict:
    """Declare a derived role, or change its description."""
    require_capability_request(request, Capability.USER_MANAGEMENT.value)
    from provisa.api.app import state
    from provisa.auth.attributes import attribute_name_problem, normalize_name
    from provisa.auth.derived_roles import declared_derived_role_entries
    from provisa.core.models import DerivedRole

    problem = attribute_name_problem(attribute)
    if problem is not None:
        code, message = problem
        shown = normalize_name(attribute) if code == "name_reserved" else attribute
        raise ApiError(422, f"derived_roles.{code}", message, name=shown)
    try:
        entry = DerivedRole(attribute=attribute, description=body.description)
    except ValidationError:
        raise ApiError(
            422,
            "derived_roles.description_missing",
            "A derived role needs a description of what it is for",
        ) from None
    kept = [e for e in declared_derived_role_entries(state) if e.attribute != entry.attribute]
    await _store(request, [*(e.model_dump() for e in kept), entry.model_dump()])
    return entry.model_dump()


@router.delete("/{attribute}")
async def delete_derived_role(attribute: str, request: Request) -> dict:
    """Stop deriving roles from an attribute."""
    require_capability_request(request, Capability.USER_MANAGEMENT.value)
    from provisa.api.app import state
    from provisa.auth.attributes import normalize_name
    from provisa.auth.derived_roles import declared_derived_role_entries

    name = normalize_name(attribute)
    entries = declared_derived_role_entries(state)
    if not any(e.attribute == name for e in entries):
        raise ApiError(404, "derived_roles.not_found", f"No derived role from {name}", name=name)
    await _store(request, [e.model_dump() for e in entries if e.attribute != name])
    return {"deleted": name}
