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

# Requirements: REQ-2059, REQ-2142

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import require_capability_request
from provisa.security.rights import Capability

router = APIRouter(prefix="/admin/masks", tags=["admin"])


class _CheckedEntry(BaseModel):
    """A mask entry as it is being edited: its mask is read as given, so a regex that is not yet
    universal is reported (``patterns``) rather than refused (REQ-2142)."""

    to: list[str]
    mask: dict[str, Any] | None = None


class MasksCheckBody(BaseModel):
    masks: list[_CheckedEntry]
    roles: list[str] | None = None


def _verdict(entry: int, field: str, exc: Any) -> dict:
    return {
        "entry": entry,
        "field": field,
        "ok": False,
        "construct": exc.construct,
        "position": exc.position,
        "message": str(exc),
    }


def pattern_report(masks: list[dict]) -> dict:
    """Each regex entry's pattern and replacement that is not universal (``patterns``), and each
    alternation engines may choose between differently (``pattern_warnings``) -- REQ-2142."""
    from provisa.security.universal_regex import NotUniversal, check, warnings

    refused, warned = [], []
    for i, entry in enumerate(masks):
        mask = entry["mask"]
        if mask is None or mask.get("type") != "regex":
            continue
        try:
            parsed, _ = check(mask.get("pattern") or "", mask.get("replace") or "")
        except NotUniversal as exc:
            refused.append(_verdict(i, getattr(exc, "field"), exc))
            continue
        warned += [_verdict(i, "pattern", w) for w in warnings(parsed)]
    return {"patterns": refused, "pattern_warnings": warned}


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

    masks = [e.model_dump() for e in body.masks]
    return {
        **masks_report(masks, lambda r: chain_of(state, r), body.roles),
        **pattern_report(masks),
    }


@router.get("/presets")
async def mask_presets(request: Request) -> list[dict]:
    """The regex masks the column editor offers ready-made (REQ-2142)."""
    require_capability_request(request, Capability.MASKING_CONFIG.value)
    from provisa.security.mask_presets import PRESETS

    return PRESETS
