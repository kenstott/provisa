# Copyright (c) 2026 Kenneth Stott
# Canary: a68a5ce1-6352-4233-9b60-748c5dd1b5e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Python's provisa_services (REQ-2106) against the contract both language modules keep
(tests/fixtures/code_services_contract.json): each call sends the request the contract names to
$PROVISA_SERVICES, naming the call it serves, and returns or raises what the contract says for
the answer -- here from a services server answering in-process. With no $PROVISA_SERVICES every
call raises, naming its service."""

# Requirements: REQ-2106

from __future__ import annotations

import importlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from provisa.python.source import HOST

pytestmark = pytest.mark.unit

CONTRACT = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "code_services_contract.json").read_text()
)
TOKEN = "tok-123"


@pytest.fixture
def ps(monkeypatch):
    monkeypatch.syspath_prepend(str(HOST.parent / "services"))
    module = importlib.import_module("provisa_services")
    yield module
    sys.modules.pop("provisa_services", None)


class _Services:
    """$PROVISA_SERVICES answered in-process: each request kept, each answered as told."""

    def __init__(self) -> None:
        self.seen: list[dict] = []
        self.answer: dict = {}
        served = self

        class Handler(BaseHTTPRequestHandler):
            def _serve(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                served.seen.append(
                    {
                        "method": self.command,
                        "path": self.path.split("/", 2)[2],  # after /<token>/
                        "token": self.path.split("/", 2)[1],
                        "body": json.loads(raw) if raw else None,
                        "call": self.headers.get("X-Provisa-Call"),
                        "traceparent": self.headers.get("traceparent"),
                    }
                )
                answer = served.answer
                body = answer["body"]
                data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
                self.send_response(answer["status"])
                self.send_header("Content-Type", answer["type"])
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_PUT = do_DELETE = _serve  # noqa: N815 -- http.server's names

            def log_message(self, format, *args) -> None:  # noqa: A002 -- the base method's name
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}/{TOKEN}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.server.shutdown()


@pytest.fixture
def services(monkeypatch):
    served = _Services()
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PROVISA_SERVICES", served.base)
    yield served
    served.stop()


@pytest.mark.parametrize("case", CONTRACT["cases"], ids=[c["name"] for c in CONTRACT["cases"]])
def test_each_call_sends_what_the_contract_names_and_returns_what_it_says(ps, services, case):
    services.answer = case["answer"]
    if "raises" in case:
        with pytest.raises(ps.ServiceError) as raised:
            eval(case["py"], {"ps": ps})  # noqa: S307 -- the contract's own call
        assert raised.value.service == case["raises"]["service"]
        assert raised.value.status == case["raises"]["status"]
        assert case["raises"]["message"] in str(raised.value)
    else:
        assert eval(case["py"], {"ps": ps}) == case["returns"]  # noqa: S307
    (sent,) = services.seen
    assert (sent["method"], sent["path"], sent["body"]) == (
        case["request"]["method"],
        case["request"]["path"],
        case["request"]["body"],
    )
    assert sent["token"] == TOKEN


def test_a_call_names_the_call_it_serves_and_its_trace(ps, services):
    services.answer = {"status": 200, "type": "application/json", "body": {}}
    traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    ps._bind(7, traceparent)
    ps.log("x")
    (sent,) = services.seen
    assert (sent["call"], sent["traceparent"]) == ("7", traceparent)


def test_with_no_services_every_call_raises_naming_its_service(ps, monkeypatch):
    monkeypatch.delenv("PROVISA_SERVICES", raising=False)
    with pytest.raises(ps.ServiceError) as raised:
        ps.cache.get("rates")
    assert raised.value.service == "cache" and raised.value.status == 0
    assert "PROVISA_SERVICES" in str(raised.value)
