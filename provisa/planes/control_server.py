# Copyright (c) 2026 Kenneth Stott
# Canary: 5d76fa79-5869-4bcb-b75e-5ab9de37a584
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A control node's plan listener: the other end of the plan facade (REQ-1916).

An Arrow Flight server that answers actions and nothing else -- no stream, no ticket, so no row
can leave it. Each action is one sealed request (:mod:`provisa.planes.portable`) and one sealed
answer:

* ``govern_and_route`` / ``govern_and_route_compiled``: the plan half of the one pipeline, run
  here exactly as a node in ``every`` mode runs it, inside the identity the data node's surface
  bound, and answered as a sealed self-contained plan. It is planned with the response cache
  unread (``serve_cached`` off): a hit would be rows.
* ``describe``: a pgwire statement's result columns and parameter types.
* ``report``: the audit facts of statements a data node ran (:mod:`provisa.planes.audit_report`),
  written here as the rows of the audit log.
* ``ping``: the data node's launch check.

A request that is not sealed under the deployment key, or is too old, is refused by name before
anything in it is read.
"""

# Requirements: REQ-1916

from __future__ import annotations

import logging
from typing import Any

from pyarrow import flight

from provisa.planes import portable
from provisa.planes.facade import seal_answer

log = logging.getLogger(__name__)

#: How long one action may take on this node: a plan is governance and routing, never a read.
_ACTION_TIMEOUT_S = 120.0


async def _plan(op: str, args: tuple, kwargs: dict[str, Any]) -> bytes:
    from provisa.pgwire import _pipeline

    if op == "govern_and_route":
        plan = await _pipeline._govern_and_route_here(*args, **kwargs)
    else:
        plan = await _pipeline._govern_and_route_compiled_here(*args, **kwargs)
    return await portable.export_plan(plan)


async def _describe(sql: str, role_id: str, *, as_of: str | None) -> Any:
    from provisa.pgwire._pipeline import _Described, _describe_pgwire_statement_here

    described = await _describe_pgwire_statement_here(sql, role_id, as_of=as_of)
    # The governed statement stays here; the data node's Execute asks for its plan.
    return _Described(described.shape, None, described.parameters)


async def _serve(op: str, identity: dict[str, Any], args: tuple, kwargs: dict[str, Any]) -> Any:
    from provisa.api.app import ensure_serving_runtime

    if op == "ping":
        return "pong"
    if op == "report":
        from provisa.planes.audit_report import record_reported

        await record_reported(*args)
        return None
    with portable.bound_identity(identity):
        org_id = identity.get("current_org")
        if org_id is not None:
            await ensure_serving_runtime(org_id, identity.get("current_env"))
        if op in ("govern_and_route", "govern_and_route_compiled"):
            return await _plan(op, args, kwargs)
        if op == "describe":
            return await _describe(*args, **kwargs)
    raise ValueError(f"unknown plane action {op!r}")


def answer(action_type: str, body: bytes) -> bytes:
    """The sealed answer to one sealed request: run on the handler's thread, on a connection loop
    checked out for it (REQ-1882), as every other transport's request is."""
    from provisa.core.connection_loop import connection_loop

    try:
        op, identity, args, kwargs = portable.open_request(body)
        if op != action_type:
            raise PermissionError(f"plane action {action_type!r} carried a {op!r} request")
        with connection_loop() as cl:
            value = cl.run(_serve(op, identity, args, kwargs), timeout=_ACTION_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 - every failure is the caller's answer, by name
        if not isinstance(exc, (PermissionError, ValueError)):
            log.exception("plane action %s failed", action_type)
        return seal_answer(False, exc)
    return seal_answer(True, value)


class PlaneServer(flight.FlightServerBase):
    """The control node's plan listener."""

    def do_action(self, context: Any, action: flight.Action) -> list[flight.Result]:
        return [flight.Result(answer(action.type, action.body.to_pybytes()))]

    def list_actions(self, context: Any) -> list[tuple[str, str]]:
        return [
            ("govern_and_route", "a raw statement's signed plan"),
            ("govern_and_route_compiled", "a compiled statement's signed plan"),
            ("describe", "a pgwire statement's result columns"),
            ("report", "audit facts of statements a data node ran"),
            ("ping", "whether this control node answers"),
        ]


def start(port: int) -> PlaneServer:
    """Start the plan listener on ``port`` (all interfaces: data nodes run on other hosts, and
    every request is sealed)."""
    from provisa.api.flight.relay import serve_in_background

    server = PlaneServer(location=f"grpc://0.0.0.0:{port}")  # nosec B104 - see above
    serve_in_background(server)
    log.info("plan listener for data nodes on port %d", server.port)
    return server
