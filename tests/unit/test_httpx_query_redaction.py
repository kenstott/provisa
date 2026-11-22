# Copyright (c) 2026 Kenneth Stott
# Canary: 3b19983a-5235-4f05-97eb-0297b152be30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""httpx logs every request URL at INFO; a credential in the query string must not reach a log."""

from __future__ import annotations

import logging
import subprocess
import sys

import httpx
import pytest

FAKE_KEY = "fake-key-0000"  # a fixed fake value, never a real credential


@pytest.fixture
def httpx_records(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.INFO, logger="httpx")
    return caplog


def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={})


def _request_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == "httpx"]


def test_sync_request_logs_url_without_query(httpx_records: pytest.LogCaptureFixture) -> None:
    with httpx.Client(transport=httpx.MockTransport(_ok)) as client:
        client.get(f"https://api.example.test/v1/items?api_key={FAKE_KEY}&page=2")
    lines = _request_lines(httpx_records)
    assert lines, "httpx logged no request line"
    assert all(FAKE_KEY not in line for line in lines), lines
    assert all("api_key" not in line and "?" not in line for line in lines), lines
    assert any("https://api.example.test/v1/items" in line for line in lines), lines
    assert all(FAKE_KEY not in r.getMessage() for r in httpx_records.records)


async def test_async_request_logs_url_without_query(
    httpx_records: pytest.LogCaptureFixture,
) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(_ok)) as client:
        await client.get(f"https://api.example.test/spec.json?token={FAKE_KEY}")
    lines = _request_lines(httpx_records)
    assert lines and all(FAKE_KEY not in line for line in lines), lines
    assert any("https://api.example.test/spec.json" in line for line in lines), lines


def test_header_credential_unaffected(httpx_records: pytest.LogCaptureFixture) -> None:
    seen: dict[str, str] = {}

    def capture(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers["Authorization"]
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    with httpx.Client(transport=httpx.MockTransport(capture)) as client:
        client.get(
            "https://api.example.test/v1/items?api_key=" + FAKE_KEY,
            headers={"Authorization": f"Bearer {FAKE_KEY}"},
        )
    # The request itself is untouched: the header and the query still go to the source.
    assert seen["auth"] == f"Bearer {FAKE_KEY}"
    assert seen["url"].endswith(f"api_key={FAKE_KEY}")
    lines = _request_lines(httpx_records)
    assert lines and all(FAKE_KEY not in line for line in lines), lines


def test_url_without_query_logged_unchanged(httpx_records: pytest.LogCaptureFixture) -> None:
    with httpx.Client(transport=httpx.MockTransport(_ok)) as client:
        client.get("https://api.example.test/v1/items")
    lines = _request_lines(httpx_records)
    assert lines == ['HTTP Request: GET https://api.example.test/v1/items "HTTP/1.1 200 OK"']


def test_installed_by_importing_provisa_in_a_fresh_process() -> None:
    """Every Provisa process (server, workers, CLI) imports the package first: the filter is on
    the httpx logger from then on, whatever the configured level."""
    code = (
        "import logging, httpx, provisa\n"
        "from provisa.observability.http_log_redaction import URL_QUERY_FILTER\n"
        "assert URL_QUERY_FILTER in logging.getLogger('httpx').filters\n"
        "records = []\n"
        "class H(logging.Handler):\n"
        "    def emit(self, r): records.append(r.getMessage())\n"
        "logging.getLogger('httpx').addHandler(H())\n"
        "logging.getLogger('httpx').setLevel(logging.INFO)\n"
        "t = httpx.MockTransport(lambda r: httpx.Response(200))\n"
        f"httpx.Client(transport=t).get('https://h.test/p?api_key={FAKE_KEY}')\n"
        "print(records)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert "https://h.test/p" in out
    assert FAKE_KEY not in out
