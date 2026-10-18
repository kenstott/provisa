# Copyright (c) 2026 Kenneth Stott
# Canary: 9b52d32a-fa44-480b-b136-63e406070c23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the column editor shows about a mask list as it is being edited (REQ-2059): the entries
no reader reaches, whether readers it does not list read the column unmasked, and the entry a
reader acting as chosen roles and derived roles reads it through. It refuses nothing; the list is
saved with the table. Gated by ``masking_config``, the right that edits masks."""

# Requirements: REQ-2059

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import require_capability_request
from provisa.core.models import MaskEntry
from provisa.security.rights import Capability

router = APIRouter(prefix="/admin/masks", tags=["admin"])


class MasksCheckBody(BaseModel):
    masks: list[MaskEntry]
    roles: list[str] | None = None


def chain_of(state: Any, role_id: str) -> list[str]:
    """A role's chain as the org's model has it: a role and its ancestors; a derived role
    ``t__v`` and its parent ``t`` (REQ-2057); any other id -- an attribute's parent, or a role
    with no ancestors -- itself alone."""
    from provisa.auth.derived_roles import SEPARATOR, declared_derived_roles

    if role_id in state.role_chains:
        return state.role_chains[role_id]
    attribute, sep, _value = role_id.partition(SEPARATOR)
    if sep and attribute in declared_derived_roles(state):
        return [role_id, attribute]
    return [role_id]


@router.post("/check")
async def check_masks(body: MasksCheckBody, request: Request) -> dict:
    require_capability_request(request, Capability.MASKING_CONFIG.value)
    from provisa.api.app import state
    from provisa.security.masking import masks_report

    return masks_report(
        [e.model_dump() for e in body.masks], lambda r: chain_of(state, r), body.roles
    )
