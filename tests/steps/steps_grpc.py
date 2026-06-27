# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pytest-bdd step implementations for REQ-617 — gRPC role selection metadata."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.grpc.server import _get_role

scenarios("../features/REQ-617.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _make_context(metadata: tuple) -> MagicMock:
    """Build a mock gRPC servicer context whose invocation_metadata returns `metadata`."""
    context = MagicMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata.return_value = metadata
    return context


def _status_code_of(exc: BaseException):
    """Best-effort extraction of the gRPC StatusCode from an AbortError-like exception."""
    code = getattr(exc, "code", None)
    if callable(code):
        try:
            return code()
        except Exception:
            pass
    for arg in getattr(exc, "args", ()):  # AbortError(StatusCode, details)
        if isinstance(arg, grpc.StatusCode):
            return arg
    return code


@given(
    "a gRPC caller omitting or providing an unrecognised x-provisa-role metadata key"
)
def caller_without_role(shared_data):
    # Omit the x-provisa-role key entirely. Other benign metadata may be present.
    metadata = (
        ("user-agent", "grpc-python/1.0"),
        ("content-type", "application/grpc"),
    )
    shared_data["context"] = _make_context(metadata)
    # Sanity: the role metadata really is absent before the RPC is processed.
    assert "x-provisa-role" not in dict(metadata)


@when("the RPC is received")
def rpc_received(shared_data):
    context = shared_data["context"]
    shared_data["error"] = None
    shared_data["role"] = None
    try:
        shared_data["role"] = _get_role(context)
    except grpc.aio.AbortError as exc:  # type: ignore[attr-defined]
        shared_data["error"] = exc
    except Exception as exc:  # pragma: no cover - defensive
        shared_data["error"] = exc


@then("the call is rejected with UNAUTHENTICATED")
def call_rejected_unauthenticated(shared_data):
    error = shared_data["error"]
    assert error is not None, (
        "Expected the RPC to be rejected, but no exception was raised "
        f"(role resolved to {shared_data.get('role')!r})"
    )
    assert shared_data["role"] is None, "Role must not be resolved for a rejected call"
    code = _status_code_of(error)
    assert code == grpc.StatusCode.UNAUTHENTICATED, (
        f"Expected UNAUTHENTICATED, got {code!r} from {error!r}"
    )
