# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7e9a42-5d3b-4f86-b0a7-6e2d8c4f1b59
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statement text behind a row of the ops `queries` report (REQ-1910, REQ-689).

The report is a view over ``query_audit_log`` and exposes the statement's hash, never its text:
the text is stored encrypted and a view cannot decrypt it. One statement's text is read here, by
its audit row id, through the audit log's own read path (``provisa.audit.query_log.read_query_text``)
and gated on ``view_governance`` — the right to see an org's security posture, which the
statements its members ran are part of.
"""

# Requirements: REQ-1910, REQ-689

from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.audit.query_log import read_query_text
from provisa.encryption.runtime import encryption_service
from provisa.security.rights import Capability

router = APIRouter()


def _tenant_pool():
    """The acting org's control plane — where its query_audit_log lives."""
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


@router.get("/admin/audit/queries/{audit_id}/text")
async def read_statement_text(request: Request, audit_id: int) -> dict:
    require_capability_request(request, Capability.VIEW_GOVERNANCE.value)
    text = await read_query_text(_tenant_pool(), audit_id, encryption_service())
    if text is None:
        raise ApiError(404, "audit.statement_not_found", f"No audited statement {audit_id}")
    return {"id": audit_id, "query_text": text}
