# Copyright (c) 2026 Kenneth Stott
# Canary: de0bc726-d884-417a-a608-d051192be1b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""In the browser (Pyodide) a read goes to /data/query as the page's signed-in user, read from
the page's auth bridge, and arrives as an Arrow stream unless pyarrow is absent. The browser's
``js`` and ``pyodide.http`` modules are doubles here."""

import json
import sys
import types

import pandas as pd
import pyarrow as pa
import pytest

import provisa_client
from provisa_client import browser

ARROW = "application/vnd.apache.arrow.stream"


def _arrow(df: pd.DataFrame) -> bytes:
    sink = pa.BufferOutputStream()
    table = pa.Table.from_pandas(df, preserve_index=False)
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class _Bytes:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def to_bytes(self) -> bytes:
        return self.data


class _Uint8Array:
    @staticmethod
    def new(value):
        return _Bytes(value.data if isinstance(value, _Bytes) else bytes(value))


class _Answer:
    """What the double server answers: status, content type, body."""

    status = 200
    content_type = ARROW
    body = b""


class _XMLHttpRequest:
    sent: list = []

    @staticmethod
    def new():
        return _XMLHttpRequest()

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.responseType = ""

    def open(self, method, url, is_async):
        self.method, self.url, self.is_async = method, url, is_async

    def setRequestHeader(self, name, value):
        self.headers[name] = value

    def send(self, body=None):
        self.body = body.to_bytes() if body is not None else None
        self.responseText = _Answer.body.decode(errors="replace")
        _XMLHttpRequest.sent.append(self)
        self.status = _Answer.status
        self.response = _Bytes(_Answer.body)

    def getResponseHeader(self, name):
        assert name == "Content-Type"
        return _Answer.content_type


class _Bridge:
    def __init__(self, auth) -> None:
        self.auth = auth

    def to_py(self):
        return dict(self.auth)


class _FetchResponse:
    def __init__(self) -> None:
        self.status = _Answer.status
        self.headers = {"content-type": _Answer.content_type}

    async def bytes(self):
        return _Answer.body


@pytest.fixture
def page(monkeypatch):
    """A notebook kernel's Web Worker: Pyodide, the page's bridge, XMLHttpRequest and pyfetch."""
    js = types.ModuleType("js")
    js.XMLHttpRequest = _XMLHttpRequest  # type: ignore[attr-defined]
    js.Uint8Array = _Uint8Array  # type: ignore[attr-defined]
    js.provisaAuth = _Bridge(  # type: ignore[attr-defined]
        {"baseUrl": "", "role": "analyst_eu", "bearer": "tok", "org": "acme", "env": None}
    )
    fetched: list = []

    async def pyfetch(url, **kwargs):
        fetched.append((url, kwargs))
        return _FetchResponse()

    http = types.ModuleType("pyodide.http")
    http.pyfetch = pyfetch  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "js", js)
    monkeypatch.setitem(sys.modules, "pyodide", types.ModuleType("pyodide"))
    monkeypatch.setitem(sys.modules, "pyodide.http", http)
    monkeypatch.setattr(sys, "platform", "emscripten")
    _XMLHttpRequest.sent = []
    _Answer.status, _Answer.content_type = 200, ARROW
    _Answer.body = _arrow(pd.DataFrame({"region": ["EU"], "email": ["***@example.com"]}))
    return types.SimpleNamespace(js=js, fetched=fetched)


def test_a_read_goes_to_the_page_server_as_the_bridges_user_and_arrives_as_arrow(page):
    df = provisa_client.query("SELECT region, email FROM shop.orders")
    assert df.to_dict("records") == [{"region": "EU", "email": "***@example.com"}]
    [xhr] = _XMLHttpRequest.sent
    assert (xhr.method, xhr.url, xhr.is_async, xhr.responseType) == (
        "POST",
        "/data/query",
        False,
        "arraybuffer",
    )
    assert xhr.headers == {
        "Content-Type": "application/json",
        "Accept": ARROW,
        "X-Provisa-Role": "analyst_eu",
        "Authorization": "Bearer tok",
        "X-Org-Provisa": "acme",
    }
    assert json.loads(xhr.body) == {"query": "SELECT region, email FROM shop.orders"}


def test_a_role_given_to_the_read_acts_in_place_of_the_pages(page):
    provisa_client.query("SELECT 1", role="auditor")
    assert _XMLHttpRequest.sent[0].headers["X-Provisa-Role"] == "auditor"


def test_the_bridge_is_read_on_every_read_so_a_rotated_token_is_sent(page):
    provisa_client.query("SELECT 1")
    page.js.provisaAuth = _Bridge(
        {
            "baseUrl": "https://p.example",
            "role": "analyst_eu",
            "bearer": "rotated",
            "org": None,
            "env": "staging",
        }
    )
    provisa_client.query("SELECT 1")
    second = _XMLHttpRequest.sent[1]
    assert second.url == "https://p.example/data/query"
    assert second.headers["Authorization"] == "Bearer rotated"
    assert "X-Org-Provisa" not in second.headers
    assert second.headers["X-Provisa-Env"] == "staging"
    assert "X-Provisa-Env" not in _XMLHttpRequest.sent[0].headers


def test_without_pyarrow_json_is_asked_for_and_each_json_shape_is_a_dataframe(page, monkeypatch):
    monkeypatch.setattr(browser, "arrow_available", lambda: False)
    _Answer.content_type = "application/json"
    _Answer.body = json.dumps({"data": {"orders": [{"id": 1}, {"id": 2}]}}).encode()
    assert provisa_client.query("{ orders { id } }")["id"].tolist() == [1, 2]
    assert _XMLHttpRequest.sent[0].headers["Accept"] == "application/json"
    _Answer.body = json.dumps({"columns": ["n"], "rows": [[3]], "type": "cypher"}).encode()
    assert provisa_client.query("MATCH (n) RETURN n").to_dict("records") == [{"n": 3}]


def test_a_refusal_and_graphql_errors_raise_with_the_servers_answer(page):
    _Answer.status, _Answer.content_type = 403, "application/json"
    _Answer.body = b'{"error":"forbidden","detail":"unknown role"}'
    with pytest.raises(RuntimeError, match="HTTP 403: .*unknown role"):
        provisa_client.query("SELECT 1")
    _Answer.status = 200
    _Answer.body = json.dumps({"data": None, "errors": [{"message": "no field"}]}).encode()
    with pytest.raises(RuntimeError, match="no field"):
        provisa_client.query("{ nope }")


def test_a_page_that_has_not_handed_over_its_user_is_refused(page):
    del page.js.provisaAuth
    with pytest.raises(RuntimeError, match="no provisaAuth"):
        provisa_client.query("SELECT 1")


def test_a_synchronous_read_on_the_pages_main_thread_is_refused(page):
    page.js.document = object()
    with pytest.raises(RuntimeError, match="Web Worker"):
        provisa_client.query("SELECT 1")


async def test_an_awaited_read_goes_over_pyfetch(page):
    df = await provisa_client.aquery("SELECT region FROM shop.orders", role="auditor")
    assert df["region"].tolist() == ["EU"]
    [(url, kwargs)] = page.fetched
    assert url == "/data/query"
    assert kwargs["method"] == "POST"
    assert kwargs["headers"]["X-Provisa-Role"] == "auditor"
    assert json.loads(kwargs["body"]) == {"query": "SELECT region FROM shop.orders"}


def test_sign_in_in_the_browser_reports_the_pages_user_role_org_and_environment(page):
    _Answer.content_type = "application/json"
    _Answer.body = json.dumps(
        {
            "user_id": "u1",
            "display_name": "Ana",
            "email": "ana@example.com",
            "active_org_id": "acme",
        }
    ).encode()
    who = provisa_client.sign_in()
    [xhr] = _XMLHttpRequest.sent
    assert (xhr.method, xhr.url) == ("GET", "/auth/me")
    assert xhr.headers["Authorization"] == "Bearer tok"
    assert repr(who) == (
        "Signed in to this page as Ana <ana@example.com>, role analyst_eu, org acme, "
        "environment prod"
    )


def test_sign_in_in_the_browser_takes_no_settings_and_reports_a_refusal(page):
    with pytest.raises(TypeError, match="page's signed-in user"):
        provisa_client.sign_in(url="https://elsewhere")
    _Answer.status, _Answer.content_type, _Answer.body = 401, "application/json", b"expired"
    with pytest.raises(RuntimeError, match="HTTP 401: expired"):
        provisa_client.sign_in()


def test_a_bridge_without_the_environment_is_refused(page):
    page.js.provisaAuth = _Bridge({"baseUrl": "", "role": None, "bearer": None, "org": None})
    with pytest.raises(RuntimeError, match="lacks \\['env'\\]"):
        provisa_client.query("SELECT 1")
