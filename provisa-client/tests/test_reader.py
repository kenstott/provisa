# Copyright (c) 2026 Kenneth Stott
# Canary: 0e1c6244-ce02-4b60-a629-a5d5e9ab3fb1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On a desktop Python, query() reads through ProvisaClient over the connection's transport, as
the connection's role unless the read names another; the package imports without Arrow Flight."""

import subprocess
import sys

import httpx
import pandas as pd
import pytest
import respx

import provisa_client
from provisa_client import client as client_mod
from provisa_client import reader


class _Client:
    made: list = []

    def __init__(self, url, **kwargs) -> None:
        _Client.made.append((url, kwargs))

    def flight_df(self, text, variables):
        return pd.DataFrame({"via": ["flight"], "text": [text]})

    def query_df(self, text, variables):
        return pd.DataFrame({"via": ["http"], "text": [text]})


@pytest.fixture
def desktop(monkeypatch, tmp_path):
    monkeypatch.setattr(client_mod, "ProvisaClient", _Client)
    monkeypatch.setattr(reader, "_session", None)
    monkeypatch.setattr("provisa_client.credentials.PROFILE_PATH", tmp_path / "absent")
    for key in ("PROFILE", "TOKEN", "USERNAME", "PASSWORD", "ORG", "FLIGHT_PORT", "TRANSPORT"):
        monkeypatch.delenv(f"PROVISA_{key}", raising=False)
    monkeypatch.setenv("PROVISA_URL", "https://p.example")
    monkeypatch.setenv("PROVISA_ROLE", "analyst")
    _Client.made = []


def test_a_read_resolves_the_connection_and_goes_over_flight(desktop):
    df = provisa_client.query("SELECT 1")
    assert df.to_dict("records") == [{"via": "flight", "text": "SELECT 1"}]
    [(url, kwargs)] = _Client.made
    assert url == "https://p.example"
    assert kwargs == {"token": None, "role": "analyst", "org": None, "flight_port": 8815}


@respx.mock
def test_a_read_names_its_own_role_and_http_is_the_transport_when_asked(desktop):
    respx.get("https://p.example/auth/me").mock(
        return_value=httpx.Response(200, json={"user_id": "u1"})
    )
    provisa_client.sign_in(transport="http", token="provisa_pat_x")
    df = provisa_client.query("{ orders { id } }", role="auditor")
    assert df["via"].tolist() == ["http"]
    assert _Client.made[0][1]["role"] == "auditor"
    assert _Client.made[0][1]["token"] == "provisa_pat_x"


async def test_an_awaited_read_answers_the_same(desktop):
    df = await provisa_client.aquery("SELECT 2")
    assert df["text"].tolist() == ["SELECT 2"]


@respx.mock
def test_sign_in_with_no_arguments_reads_the_environment_and_says_as_whom(desktop):
    me = respx.get("https://p.example/auth/me").mock(
        return_value=httpx.Response(
            200,
            json={"user_id": "u1", "display_name": "u1", "email": None, "active_org_id": "acme"},
        )
    )
    who = provisa_client.sign_in()
    assert me.calls.last.request.headers["x-provisa-role"] == "analyst"
    assert repr(who) == (
        "Signed in to https://p.example as u1, role analyst, org acme, environment prod"
    )
    provisa_client.query("SELECT 1")
    assert reader._session is not None and _Client.made[0][0] == "https://p.example"


@respx.mock
def test_a_sign_in_the_server_refuses_raises(desktop):
    respx.get("https://p.example/auth/me").mock(return_value=httpx.Response(401, text="no"))
    with pytest.raises(PermissionError, match="HTTP 401"):
        provisa_client.sign_in(token="provisa_pat_bad")


def test_the_connection_is_resolved_once_for_every_read(desktop, monkeypatch):
    calls = []
    real = reader.__dict__.get("_session")
    assert real is None
    import provisa_client.credentials as credentials

    original = credentials.resolve
    monkeypatch.setattr(credentials, "resolve", lambda **kw: calls.append(kw) or original(**kw))
    provisa_client.query("SELECT 1")
    provisa_client.query("SELECT 2")
    assert len(calls) == 1


def test_the_package_imports_and_reads_without_arrow_flight():
    """Pyodide's pyarrow has no Flight: importing the package must not load it."""
    code = (
        "import sys; sys.modules['pyarrow.flight'] = None\n"
        "import provisa_client\n"
        "assert callable(provisa_client.query) and callable(provisa_client.aquery)\n"
        "assert 'provisa_client.client' not in sys.modules\n"
        "try:\n"
        "    provisa_client.ProvisaClient\n"
        "except ImportError:\n"
        "    print('flight-only names need flight')\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "flight-only names need flight"


def test_every_export_resolves():
    assert sorted(provisa_client.__all__) == sorted(provisa_client._EXPORTS)
    for name in provisa_client.__all__:
        assert getattr(provisa_client, name) is not None
