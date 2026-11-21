# Copyright (c) 2026 Kenneth Stott
# Canary: 1f005b86-7e4f-44ef-a9e7-b2b47e970cb4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An httpx failure turned into a client-facing message names its kind and address, never ending
at a colon: ``str()`` of a read timeout and of some connect errors is empty."""

from __future__ import annotations

import socket
import ssl

import httpx
import pytest

from provisa.api.errors import ApiError
from provisa.core.http_failure import describe_http_failure

_URL = "https://api.anthropic.com/v1/models"


def _request() -> httpx.Request:
    return httpx.Request("GET", _URL)


def _raised_from(exc: httpx.HTTPError, cause: BaseException) -> httpx.HTTPError:
    """``exc`` as httpx raises it: mapped from ``cause`` (``raise mapped from cause``)."""
    try:
        try:
            raise cause
        except BaseException as inner:
            raise exc from inner
    except httpx.HTTPError as outer:
        return outer


def test_read_timeout_with_empty_text_names_kind_and_address():
    exc = httpx.ReadTimeout("", request=_request())
    assert str(exc) == ""
    assert describe_http_failure(exc) == f"timed out waiting for a response from GET {_URL}"


def test_connect_error_with_empty_text_names_kind_and_address():
    exc = httpx.ConnectError("", request=_request())
    assert str(exc) == ""
    assert describe_http_failure(exc) == f"could not connect to GET {_URL}"


def test_connect_timeout():
    exc = httpx.ConnectTimeout("", request=_request())
    assert describe_http_failure(exc) == f"timed out connecting to GET {_URL}"


def test_connection_refused_read_from_the_os_error():
    exc = _raised_from(
        httpx.ConnectError("", request=_request()), ConnectionRefusedError(61, "Connection refused")
    )
    assert describe_http_failure(exc) == (
        f"the connection to GET {_URL} was refused ([Errno 61] Connection refused)"
    )


def test_name_not_resolved_read_from_the_os_error():
    exc = _raised_from(
        httpx.ConnectError("", request=_request()),
        socket.gaierror(8, "nodename nor servname provided, or not known"),
    )
    assert describe_http_failure(exc).startswith(f"the host name of GET {_URL} did not resolve (")


def test_tls_failure_read_from_the_ssl_error():
    exc = _raised_from(
        httpx.ConnectError("", request=_request()), ssl.SSLError(1, "CERTIFICATE_VERIFY_FAILED")
    )
    assert describe_http_failure(exc).startswith(f"the TLS handshake with GET {_URL} failed (")


def test_egress_proxy_refusing_the_address():
    exc = httpx.ProxyError("403 Forbidden", request=_request())
    assert describe_http_failure(exc) == (
        f"the egress proxy refused the connection to GET {_URL} (403 Forbidden)"
    )


def test_status_error_keeps_httpx_text():
    request = _request()
    response = httpx.Response(401, request=request)
    exc = httpx.HTTPStatusError(
        "Client error '401 Unauthorized'", request=request, response=response
    )
    assert describe_http_failure(exc) == "Client error '401 Unauthorized'"


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ReadTimeout("", request=_request()),
        httpx.ConnectError("", request=_request()),
    ],
    ids=["read-timeout", "connect-error"],
)
async def test_model_listing_failure_says_why(exc):
    """The Discover relationships dialog showed "anthropic model listing is unreachable:"."""
    from provisa.api.admin.ai_models_router import _run_model_listing

    async def fetch() -> list[str]:
        raise exc

    with pytest.raises(ApiError) as caught:
        await _run_model_listing("anthropic", fetch)
    detail = caught.value.detail
    assert not detail.rstrip().endswith(":"), detail
    assert _URL in detail
    assert detail.startswith("anthropic model listing is unreachable: ")
