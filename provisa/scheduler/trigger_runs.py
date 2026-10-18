# Copyright (c) 2026 Kenneth Stott
# Canary: 53362452-7474-4a03-bc65-06460a307a30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One run of a trigger, and its record (REQ-1003, REQ-2054).

Every trigger runs here, whatever fired it -- its schedule, or a change or event-loop event of the
table it is on: in the org whose model holds it, as the principal ``scheduler:<id>``, its target
dispatched as any call of it is (a SQL statement through the one pipeline as its role, a command
through the one governed dispatch as its role, a webhook posted). Each run's end is kept on the
trigger's ``trigger_runs`` row: when it ran, whether it succeeded, was refused or failed, why, and
what fired it. A refused or failed run is logged too.
"""

# Requirements: REQ-1003, REQ-2054

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import httpx

log = logging.getLogger(__name__)

SUCCEEDED = "succeeded"
REFUSED = "refused"
FAILED = "failed"


def _refused(exc: BaseException) -> bool:
    """A run its admission or governance refused, rather than one that broke."""
    from provisa.api.errors import ApiError
    from provisa.scheduler.trigger_sql import TriggerSqlRefused

    if isinstance(exc, ApiError):
        return exc.status_code < 500
    return isinstance(exc, (PermissionError, TriggerSqlRefused))


async def post_webhook(url: str, trigger_id: str, args: dict) -> None:  # REQ-216, REQ-209
    """Post a trigger's webhook: the trigger's id and its arguments, as JSON. A response that is
    not a success raises."""
    from provisa.core.secrets import resolve_secrets

    # The URL is kept as written; an environment or secret reference in it is resolved here.
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(resolve_secrets(url), json={"trigger_id": trigger_id, **args})
        resp.raise_for_status()
    log.info("Trigger %s: webhook %s returned %s", trigger_id, url, resp.status_code)


async def _run_command(trigger: dict, given: dict, state: Any) -> None:
    """The trigger's command, as its role, given those of the trigger's arguments it declares
    (in the order it declares them); one the firing did not carry is given None."""
    from provisa.api.data.action_exec import invoke_command
    from provisa.auth.derived_roles import service_acting_role

    fn = state.tracked_functions.get(trigger["command"])
    names = [a["name"] for a in fn["arguments"]] if fn is not None else []
    # The node's own work: the trigger's role with the node's region derived role (REQ-2057).
    await invoke_command(
        trigger["command"],
        {n: given.get(n) for n in names},
        state,
        service_acting_role(state, trigger["role"]),
    )


async def _named_webhook_url(trigger: dict, state: Any) -> str:
    from sqlalchemy import select

    from provisa.core.schema_org import tracked_webhooks

    async with state.model_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(tracked_webhooks.c.url).where(
                    tracked_webhooks.c.name == trigger["webhook_name"]
                )
            )
        ).fetchone()
    if row is None:
        raise LookupError(
            f"Trigger {trigger['id']!r}: the org has no tracked webhook {trigger['webhook_name']!r}"
        )
    return row[0]


async def run_trigger(trigger: dict, org_id: str, *, given: dict, fired_by: str | None) -> str:
    """Run ``trigger`` (its row) once in ``org_id``, with the arguments its firing ``given``, and
    record how it ended. Returns the outcome. Never raises: a scheduled job and a change's
    background run have nobody to raise to, so the record and the log say it."""
    from provisa.api.app import state
    from provisa.audit.context import audit_identity_scope
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    error: str | None = None
    try:
        try:
            with audit_identity_scope(f"scheduler:{trigger['id']}", "scheduler"):
                if trigger["kind"] == "sql":
                    from provisa.scheduler.jobs import _execute_sql

                    await _execute_sql(trigger["sql"], trigger["id"], trigger["role"], org_id)
                elif trigger["kind"] == "command":
                    await _run_command(trigger, given, state)
                else:
                    url = trigger["url"] or await _named_webhook_url(trigger, state)
                    await post_webhook(url, trigger["id"], {**trigger["args"], **given})
            outcome = SUCCEEDED
        except Exception as exc:  # allow-ble: a trigger's run ends however its target ends; the record and the log keep why, and the scheduler or the change that fired it carries on
            error = str(exc) or type(exc).__name__
            outcome = REFUSED if _refused(exc) else FAILED
            log.error("Trigger %s (%s): %s: %s", trigger["id"], fired_by, outcome, error)
        await record(state, trigger["id"], outcome, error, fired_by)
        return outcome
    finally:
        reset_current_org(token)


async def record(
    state: Any, trigger_id: str, outcome: str, error: str | None, fired_by: str | None
) -> None:
    """Keep the trigger's last run on its ``trigger_runs`` row."""
    from provisa.core.schema_org import trigger_runs

    async with state.tenant_db.acquire() as conn:
        await conn.upsert(
            trigger_runs,
            {
                "trigger_id": trigger_id,
                "ran_at": datetime.now(UTC),
                "outcome": outcome,
                "error": error,
                "fired_by": fired_by,
            },
            index_elements=["trigger_id"],
            update_columns=["ran_at", "outcome", "error", "fired_by"],
        )


async def last_runs(state: Any) -> dict[str, dict]:
    """Each trigger's last run, by trigger id: ``ran_at``, ``outcome``, ``error``, ``fired_by``."""
    from sqlalchemy import select

    from provisa.core.schema_org import trigger_runs

    async with state.tenant_db.acquire() as conn:
        rows = (await conn.execute_core(select(trigger_runs))).fetchall()
    return {r.trigger_id: dict(r._mapping) for r in rows}
