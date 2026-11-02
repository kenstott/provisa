# Copyright (c) 2026 Kenneth Stott
# Canary: 1585f50d-ff61-48c6-95fc-ed0b4c433142
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A plan as it crosses from a control node to a data node, and the identity it is asked for
(REQ-1916).

The control node governs and routes a statement exactly as a node in ``every`` mode does, then
:func:`export_plan` makes the plan self-contained -- what the plan derives lazily from the model
(the engine form a landed result needs, the result's typed columns, the audit record's resolved
tables and enforcement) is derived now, on the node that holds the model -- and seals it. A data
node's :func:`import_plan` verifies the seal, puts back what the plan reads lazily, and admits the
plan to this process's terminals (``_pipeline.adopt_verified_plan``): a plan the deployment key
did not seal is never run.

A plan reads no rows to be made. One that would carry rows (a response-cache hit) is never
exported: a control node plans for a data node with ``serve_cached`` off, and the data node's
terminal reads the cache itself. A plan of a kind this slice does not carry across is refused by
name (:class:`PlanNotPortable`), never run some other way.

The identity is the request's context as the surface bound it: the org and environment, the
acting role and its session variables, the audit principal, the request's transport and, for a
pgwire catalog statement, what the catalog is answered for. The control node governs inside
exactly that context (:func:`bound_identity`).
"""

# Requirements: REQ-1916, REQ-1914

from __future__ import annotations

import dataclasses
import pickle  # nosec B403 - only ever loads a payload whose seal verified (planes.signing)
import time
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.planes.signing import seal, unseal

if TYPE_CHECKING:
    from provisa.pgwire._pipeline import _Plan

PLAN = "plan"
REQUEST = "plan request"


class PlanNotPortable(RuntimeError):
    """A plan of a kind a data node is not yet given (REQ-1916): refused by name, not run."""

    code = "plane.plan_not_portable"

    def __init__(self, what: str) -> None:
        self.what = what
        super().__init__(
            f"this statement plans to {what}, which a data node does not run yet; "
            "send it to a node in every mode"
        )


#: Plan fields a data node does not run in this slice, with what each one means.
_NOT_CARRIED = {
    "two_step": "insert the rows of a SELECT in two steps",
    "code_write": "write through a code table's command",
    "staged": "stage a column transform's CTEs",
    "kept": "keep a mutation in a Reversible environment's change log",
    "temp": "write a temporary table of the session",
    "embedded": "embed texts after the read",
    "columns": "apply a column transform after the read",
    "scratch": "read scratch tables landed while it was planned",
}


def _identity_vars() -> tuple[ContextVar[Any], ...]:
    from provisa.audit import context as audit_context
    from provisa.core import limits, meta_rls, request_context
    from provisa.pgwire import catalog_route

    return (
        request_context.current_org,
        request_context.current_env,
        request_context.current_session_vars,
        request_context.current_acting_role,
        audit_context._current_identity,  # noqa: SLF001 - the principal the surface bound
        meta_rls._current_tenant,  # noqa: SLF001
        limits._request_transport,  # noqa: SLF001
        catalog_route.current_request,
    )


def capture_identity() -> dict[str, Any]:
    """The request's identity as the surface bound it in this context, by variable name."""
    return {var.name: var.get() for var in _identity_vars()}


@contextmanager
def bound_identity(identity: dict[str, Any]) -> Generator[None]:
    """Run the block in ``identity``: every variable it names bound to its value."""
    by_name = {var.name: var for var in _identity_vars()}
    unknown = set(identity) - set(by_name)
    if unknown:
        raise ValueError(f"a plan request names identity this node does not know: {unknown}")
    tokens = [(by_name[name], by_name[name].set(value)) for name, value in identity.items()]
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def seal_request(op: str, identity: dict[str, Any], args: tuple, kwargs: dict[str, Any]) -> bytes:
    return seal(REQUEST, pickle.dumps((op, identity, args, kwargs)))


def open_request(sealed: bytes) -> tuple[str, dict[str, Any], tuple, dict[str, Any]]:
    return pickle.loads(unseal(REQUEST, sealed))  # nosec B301 - the seal verified first


class _FixedResultShape:
    """A plan's result columns as the control node derived them (``_pipeline._KeptResultShape``
    derives them from the model, which a data node does not consult)."""

    __slots__ = ("_read", "_wire_typed")

    def __init__(self, read: list[tuple[str, str]] | None, wire_typed: list | None) -> None:
        self._read = read
        self._wire_typed = wire_typed

    def now(self) -> list[tuple[str, str]] | None:
        return self._read

    async def __call__(self) -> list[tuple[str, str]] | None:
        return self._read

    def wire_typed(self) -> list[tuple[str, str]] | None:
        return self._wire_typed


@dataclass
class PortablePlan:
    """What a control node answers for a plan request."""

    plan: Any  # _Plan, with nothing lazy left on it
    # The statement's engine form for a landed result (``_Plan.engine_landing``), derived now.
    engine_landing_sql: str | None
    result_shape: list[tuple[str, str]] | None
    wire_typed_shape: list | None
    has_result_shape: bool
    # How long the statement's audit record had been open when the plan was sealed: the data
    # node's record starts that long before it arrived, on the data node's own clock.
    audit_age_s: float | None


async def export_plan(plan: "_Plan") -> bytes:
    """``plan``, governed on this node, made self-contained and sealed for a data node. Its audit
    record keeps the model stamp it was governed under (REQ-1914)."""
    from provisa.pgwire._pipeline import require_governed_plan

    require_governed_plan(plan)
    if plan.cache_hit is not None:
        raise RuntimeError(
            "a plan for a data node carried a response-cache hit: a control node never reads rows"
        )
    for name, what in _NOT_CARRIED.items():
        if getattr(plan, name):
            raise PlanNotPortable(what)
    landing = await plan.engine_landing() if plan.engine_landing is not None else None
    shape = wire_typed = None
    if plan.result_shape is not None:
        shape = await plan.result_shape()
        wire_typed = plan.result_shape.wire_typed()
    audit = plan.audit
    audit_age = None
    if audit is not None:
        audit_age = time.monotonic() - audit.started
        audit = dataclasses.replace(
            audit,
            table_ids=tuple(audit.table_ids() if callable(audit.table_ids) else audit.table_ids),
            enforced=audit.enforced() if callable(audit.enforced) else audit.enforced,
        )
    carried = dataclasses.replace(
        plan,
        engine_landing=None,
        result_shape=None,
        loop_run=None,
        stamp=None,
        audit=audit,
    )
    portable = PortablePlan(
        plan=carried,
        engine_landing_sql=landing,
        result_shape=shape,
        wire_typed_shape=wire_typed,
        has_result_shape=plan.result_shape is not None,
        audit_age_s=audit_age,
    )
    return seal(PLAN, pickle.dumps(portable))


def import_plan(sealed: bytes) -> "_Plan":
    """The plan a control node sealed, admitted to this process's terminals; refused by name
    (``PlanForged``, ``PlanStale``) when the deployment key did not seal it or it is too old."""
    from provisa.pgwire._pipeline import adopt_verified_plan

    portable: PortablePlan = pickle.loads(unseal(PLAN, sealed))  # nosec B301 - seal verified
    plan = portable.plan
    if portable.engine_landing_sql is not None:
        landing_sql = portable.engine_landing_sql

        async def _landing() -> str:
            return landing_sql

        plan.engine_landing = _landing
    if portable.has_result_shape:
        plan.result_shape = _FixedResultShape(portable.result_shape, portable.wire_typed_shape)
    if plan.audit is not None and portable.audit_age_s is not None:
        plan.audit = dataclasses.replace(
            plan.audit, started=time.monotonic() - portable.audit_age_s
        )
    return adopt_verified_plan(plan)
