# Copyright (c) 2026 Kenneth Stott
# Canary: d72ccee7-bd5b-4e71-9c85-6af8e3214486
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1904: mid-stream gRPC status codes, and the health service's exemption from auth."""

from __future__ import annotations

import grpc
import pytest

from provisa.grpc.auth import AuthInterceptor
from provisa.grpc.server import _status_for_exception


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (TimeoutError("slow"), grpc.StatusCode.DEADLINE_EXCEEDED),
        (PermissionError("no"), grpc.StatusCode.PERMISSION_DENIED),
        (ConnectionError("down"), grpc.StatusCode.UNAVAILABLE),
        (OSError("io"), grpc.StatusCode.UNAVAILABLE),
        (ValueError("bad"), grpc.StatusCode.INVALID_ARGUMENT),
        (RuntimeError("boom"), grpc.StatusCode.INTERNAL),
        (KeyError("k"), grpc.StatusCode.INTERNAL),
    ],
)
def test_a_mid_stream_failure_maps_to_a_specific_status_not_unknown(exc, code) -> None:
    assert _status_for_exception(exc) is code


def test_a_timeout_is_not_read_as_a_generic_os_error() -> None:
    # TimeoutError subclasses OSError; the more specific mapping must win.
    assert isinstance(TimeoutError(), OSError)
    assert _status_for_exception(TimeoutError()) is grpc.StatusCode.DEADLINE_EXCEEDED


class _State:
    """Auth active and high-security on: everything but health must be refused."""

    auth_config = {"provider": "oidc", "default_role": "analyst"}
    auth_middleware_active = True
    security_high = True
    multitenancy = False
    admin_db = None
    roles: dict = {}


class _Details:
    def __init__(self, method: str) -> None:
        self.method = method
        self.invocation_metadata: list = []


def test_the_health_check_reaches_its_servicer_with_no_credential_even_in_high_security() -> None:
    sentinel = object()
    seen: list = []

    def continuation(details):
        seen.append(details.method)
        return sentinel

    handler = AuthInterceptor(_State()).intercept_service(
        continuation, _Details("/grpc.health.v1.Health/Check")
    )
    assert handler is sentinel
    assert seen == ["/grpc.health.v1.Health/Check"]


def test_a_data_rpc_with_no_credential_is_still_refused_before_any_servicer_runs() -> None:
    seen: list = []

    def continuation(details):
        seen.append(details.method)
        return object()

    AuthInterceptor(_State()).intercept_service(
        continuation, _Details("/provisa.Provisa/QueryOrders")
    )
    assert seen == []
