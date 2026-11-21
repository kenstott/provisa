# Copyright (c) 2026 Kenneth Stott
# Canary: 1061a629-2d71-4909-bbd1-22d6f5929754
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A call of a statement command (REQ-2001): its statement through the one pipeline
(``_govern_and_route`` then ``_execute_plan``) as the caller's role, so it is governed as any
statement the caller sends; its arguments bound in declared order, never in the text. What the
statement declares is :mod:`provisa.core.statement_declaration`."""

# Requirements: REQ-2001

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from provisa.api.errors import ApiError
from provisa.core.statement_declaration import StatementCommandRefused, declared

#: What the run in this context found, for its provenance record (action_exec): the models its
#: statement embedded with, and the rows it affected or answered. Set by :func:`run`.
RUN_FACTS: ContextVar[dict[str, Any] | None] = ContextVar("statement_run_facts", default=None)


async def run(fn: dict, args: dict, state: Any, role_id: str | None) -> list[dict]:
    """One call of a statement command: its statement through the one pipeline as the caller's
    role, the call's arguments bound in declared order (REQ-2001)."""
    from provisa.executor.serialize import result_records
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    if role_id is None:
        raise StatementCommandRefused(
            fn["name"], "a statement command runs as a role; none is bound"
        )
    broken = fn["contract"]["broken"]
    if broken is not None:
        # Its statement no longer resolves against the model: a table or column it reads or
        # writes was renamed or dropped since it was registered.
        raise ApiError(
            409,
            "functions.statement_broken",
            f"command {fn['name']!r} is broken: {broken}",
            name=fn["name"],
            reason=broken,
        )
    names = [a["name"] for a in fn["arguments"]]
    found = declared(fn["name"], fn["binding"], names)
    params = [args[n] for n in names]
    plan = await _govern_and_route(found.sql, role_id, params=params or None)
    result = await _execute_plan(plan, state)
    # The write's count, as pgwire's command tag reads it (server._write_tag); a read's rows.
    count = result.rowcount if result.rowcount is not None else len(result.rows)
    RUN_FACTS.set(
        {
            "rows": count,
            "embed": list(plan.embedded.audit or ()) if plan.embedded is not None else [],
        }
    )
    if found.kind == "mutation" and not found.returns_rows:
        return [{"rows_affected": count}]
    return result_records(result)
