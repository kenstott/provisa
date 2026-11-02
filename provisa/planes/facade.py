# Copyright (c) 2026 Kenneth Stott
# Canary: 93c7517c-5666-4bdf-a97c-7a837864bbc9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The plan facade's remote side: a data node asking its control node (REQ-1916).

``_pipeline._govern_and_route`` and ``_govern_and_route_compiled`` are the facade: in ``data``
mode each hands its arguments here, and the plan comes back from the control node named at launch
(``provisa run --mode data --control host:port``, ``PROVISA_CONTROL``), sealed under the
deployment key. The request carries the identity the surface bound (``portable.capture_identity``)
so the control node governs as the same principal, in the same org, under the same role.

A data node whose control node cannot be reached does not plan some other way: the request fails,
naming the control node (:class:`ControlUnreachable`), and a data node whose control node does not
answer at launch does not start (:func:`require_control`).
"""

# Requirements: REQ-1916

from __future__ import annotations

import asyncio
import os
import pickle  # nosec B403 - only ever loads a payload whose seal verified (planes.signing)
import threading
from typing import TYPE_CHECKING, Any

from provisa.planes import portable
from provisa.planes.signing import seal, unseal

if TYPE_CHECKING:
    from provisa.pgwire._pipeline import _Described, _Plan

CONTROL_ENV = "PROVISA_CONTROL"
PLANE_PORT_ENV = "PROVISA_PLANE_PORT"
ANSWER = "plane answer"

#: A plan call with no request deadline bound waits this long for its control node.
_DEFAULT_TIMEOUT_S = 60.0


class ControlUnreachable(ConnectionError):
    """A data node could not reach its control node (REQ-1916)."""

    code = "plane.control_unreachable"

    def __init__(self, address: str, why: str) -> None:
        self.address = address
        self.why = why
        super().__init__(f"this data node cannot reach its control node at {address}: {why}")

    def __reduce__(self) -> tuple:
        return (type(self), (self.address, self.why))


class RemoteRefusal(RuntimeError):
    """A control node refused or failed a plan request with an error that does not cross the
    socket as itself; its type and text are kept."""

    def __init__(self, kind: str, message: str) -> None:
        self.kind = kind
        self.message = message
        super().__init__(f"{kind}: {message}")

    def __reduce__(self) -> tuple:
        return (type(self), (self.kind, self.message))


def control_address() -> str:
    """The control node this data node plans with, as launched (``host:port``)."""
    address = os.environ.get(CONTROL_ENV, "").strip()
    if not address:
        raise ControlUnreachable("(none)", f"a data node is started with --control ({CONTROL_ENV})")
    return address


_client_lock = threading.Lock()
_client: Any = None


def _flight_client() -> Any:
    global _client
    with _client_lock:
        if _client is None:
            from pyarrow import flight

            _client = flight.FlightClient(f"grpc://{control_address()}")
        return _client


def seal_answer(ok: bool, value: Any) -> bytes:
    """The control node's answer to one call: its value, or the error it raised."""
    if not ok:
        try:
            pickle.loads(pickle.dumps(value))  # nosec B301 - our own bytes, never received
        except Exception:  # noqa: BLE001 - any error that does not survive the round trip
            value = RemoteRefusal(type(value).__name__, str(value))
    return seal(ANSWER, pickle.dumps((ok, value)))


def call(op: str, args: tuple, kwargs: dict[str, Any], *, timeout: float | None = None) -> Any:
    """Send ``op`` to the control node and return its answer, or raise the error it raised there.
    Blocking: the async callers run it off their loop."""
    from pyarrow import flight

    request = portable.seal_request(op, portable.capture_identity(), args, kwargs)
    options = flight.FlightCallOptions(timeout=timeout or _DEFAULT_TIMEOUT_S)
    address = control_address()
    try:
        results = list(_flight_client().do_action(flight.Action(op, request), options=options))
    except flight.FlightUnavailableError as exc:
        raise ControlUnreachable(address, str(exc)) from exc
    except flight.FlightTimedOutError as exc:
        raise ControlUnreachable(address, f"no answer in {options.timeout}s") from exc
    if len(results) != 1:
        raise RuntimeError(f"control node at {address} answered {op} with {len(results)} results")
    ok, value = pickle.loads(unseal(ANSWER, results[0].body.to_pybytes()))  # nosec B301
    if not ok:
        raise value
    return value


def _remaining() -> float | None:
    from provisa.core import request_deadline

    deadline = request_deadline.current()
    return None if deadline is None else max(deadline.remaining(), 0.001)


async def _call(op: str, args: tuple, kwargs: dict[str, Any]) -> Any:
    # The identity is read in the caller's context, which ``to_thread`` runs the call in a copy of.
    return await asyncio.to_thread(call, op, args, kwargs, timeout=_remaining())


async def plan_remotely(op: str, args: tuple, kwargs: dict[str, Any]) -> "_Plan":
    """A plan from the control node for ``op`` (``govern_and_route`` or
    ``govern_and_route_compiled``), admitted to this process's terminals. What the control node
    found to say about the statement is said here too, in the request's own warning channel."""
    from provisa.core.statement_warnings import collecting

    plan = portable.import_plan(await _call(op, args, kwargs))
    with collecting() as found:
        found.extend(plan.warnings)
    return plan


async def describe_remotely(sql: str, role_id: str, *, as_of: str | None) -> "_Described":
    """A pgwire statement described by the control node: its columns and parameters, and no
    governed statement, so its Execute asks for its plan (``govern_pgwire_plan``)."""
    return await _call("describe", (sql, role_id), {"as_of": as_of})


def report(facts: list[dict[str, Any]]) -> None:
    """Hand the control node the audit facts of finished statements (blocking)."""
    call("report", (facts,), {})


def require_control() -> None:
    """A data node's launch check: the deployment key is held, and the control node answers.
    Raises ``LaunchRefused`` naming what is missing."""
    from provisa.core.process_region import LaunchRefused
    from provisa.planes import signing

    try:
        signing._key()  # noqa: SLF001 - derived now, so a node without the key does not start
    except RuntimeError as exc:
        raise LaunchRefused(f"a data node signs and verifies plans: {exc}") from exc
    try:
        answered = call("ping", (), {}, timeout=10.0)
    except (ControlUnreachable, PermissionError) as exc:
        raise LaunchRefused(str(exc)) from exc
    if answered != "pong":
        raise LaunchRefused(f"the control node at {control_address()} answered {answered!r}")
