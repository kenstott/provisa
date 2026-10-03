# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1c8a62-9d37-4b05-8e21-7a6d3c0b9e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The authorization for catalog SQL an admin handler runs on the engine (REQ-1760).

The handler is behind the admin gate (each one is proven gated by test_admin_routes_gated), and
what it runs is the system's own catalog work, so it carries the system's authorization, pinned
to the one statement. The statement is recorded with the acting admin — the authenticated user and
the role they act as — so the record shows who had the catalog read."""

from __future__ import annotations

import time
from typing import Any

from provisa.federation.execution_auth import SystemAuth, system_auth


def admin_engine_auth(reason: str, sql: str, state: Any) -> SystemAuth:
    """The system's authorization for ``sql``, with the acting admin recorded against it. With
    no authenticated caller bound there is no admin to record, and the call is refused."""
    from provisa.audit.context import current_audit_identity
    from provisa.audit.pipeline import PendingAudit, enqueue_audit
    from provisa.core.request_context import current_acting_role

    identity = current_audit_identity()
    role = current_acting_role.get()
    if identity is None or not role:
        raise PermissionError(f"{reason}: catalog SQL runs for an acting admin, and none is bound")
    enqueue_audit(
        PendingAudit(
            user_id=identity.user_id,
            surface=identity.surface,
            role_id=role,
            query_text=sql,
            table_ids=[],
            started=time.monotonic(),
            model_stamp=state.model_stamp,
            enforced={"system": reason},
        ),
        200,
        state,
        route="engine",
    )
    return system_auth(reason, expected_sql=sql)


async def run_admin_catalog_sql(state: Any, engine: Any, sql: str, reason: str) -> Any:
    """Run an admin handler's catalog ``sql`` on ``engine`` under :func:`admin_engine_auth`."""
    return await engine.execute_engine(sql, authorization=admin_engine_auth(reason, sql, state))
