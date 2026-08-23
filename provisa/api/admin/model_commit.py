# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1c6b28-93ad-4e57-8c02-71d9ea3b5c60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every model change lands in the environment's branch as a commit (REQ-1524).

WHY A SCHEMA EXTENSION AND NOT FIFTY CALL SITES. The carried classes of REQ-1489 are written by
some fifty admin mutations, and a rule that has to be remembered at fifty sites is a rule that will
be missed at the fifty-first. The extension is the one place every mutation passes through, so a
mutation added tomorrow is projected without anybody adding a line for it.

WHY IT PROJECTS THE WHOLE MODEL RATHER THAN THE ROWS THAT CHANGED. A mutation does not report what
it touched, and a diff assembled from what it *appears* to touch would be a second, weaker model of
the model. Projection is deterministic (REQ-1526), so an unchanged model produces a byte-identical
tree and :func:`provisa.core.env_repo.commit_files` writes no commit at all -- the filter is the
tree comparison, which cannot be wrong about what changed.

WHY IT RUNS AFTER THE OPERATION AND NEVER BEFORE. The commit records what the model IS, not what
somebody asked it to become: a mutation that raised leaves nothing to record. An operation whose
result carries errors is skipped for the same reason -- strawberry reports a resolver failure there
rather than raising out of execution.

WHY A FAILURE HERE IS SILENT. REQ-1524 makes the repository a projection and never an authority, so
a projection that does not land marks the environment drifted and leaves the model change standing.
That decision lives in :func:`provisa.core.env_repo.write_through`; this module only has to not
undo it.
"""

# Requirements: REQ-1487, REQ-1489, REQ-1524, REQ-1526

from __future__ import annotations

import logging
from typing import Any

from strawberry.extensions import SchemaExtension

from provisa.api.org_runtime import active_env, current_org
from provisa.core.env_repo import write_through
from provisa.core.environments import org_schema

log = logging.getLogger(__name__)


def _actor(context: Any) -> str | None:
    """The user the commit is authored by, or ``None`` for an act no user signed.

    ``None`` is not a missing value to be filled in: REQ-1524 has the system author stand in for a
    change no member made, and :data:`provisa.core.env_repo.SYSTEM_AUTHOR` is where that is decided.
    """
    request = (
        context.get("request") if isinstance(context, dict) else getattr(context, "request", None)
    )
    identity = getattr(getattr(request, "state", None), "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    return None if user_id in (None, "anonymous") else user_id


def _org_id() -> str | None:
    """The org whose repository this change belongs in.

    The routing middleware binds ``current_org`` only when the request is for a non-default org or a
    non-prod environment (see ``_OrgRoutingMiddleware``); a default-org prod request is served with
    it unbound, and ``state.org_id`` is the org it names. That is the routing rule read back, not a
    default invented here. ``None`` means no org plane exists yet -- a server still starting.
    """
    from provisa.api.app import state

    return current_org.get() or getattr(state, "org_id", None)


class ModelCommitExtension(SchemaExtension):
    """Commit the projected model after every admin mutation that completed (REQ-1524)."""

    async def on_operation(self):
        yield
        execution_context = self.execution_context
        if execution_context.operation_type.value.lower() != "mutation":
            return
        result = execution_context.result
        if result is None or result.errors:
            return
        await self._commit(execution_context.operation_name or "mutation")

    async def _commit(self, message: str) -> None:
        from provisa.api.app import state

        org_id = _org_id()
        if org_id is None or state.admin_db is None or state.tenant_db is None:
            return
        env = active_env()
        actor = _actor(self.execution_context.context)
        try:
            async with state.tenant_db.acquire() as conn:
                sha = await write_through(
                    conn, state.admin_db, org_id, env, org_schema(org_id, env), message, actor
                )
        except Exception:  # noqa: BLE001 — REQ-1524: the projection never fails the change it observes
            log.exception("could not project %s/%s after %s", org_id, env, message)
            return
        if sha is not None:
            log.debug("committed %s/%s as %s", org_id, env, sha)
