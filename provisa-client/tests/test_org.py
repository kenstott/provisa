# Copyright (c) 2026 Kenneth Stott
# Canary: 73c141c8-d95d-4485-8c2d-41c5690458ed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1235: every client entry point can name the org its requests are for.

A multi-tenant deployment refuses a request that names no org. Over HTTP the org is named by
the ``X-Org-Provisa`` header; an Arrow Flight ticket names it in ``org``. When no org is given
nothing is sent, which is what a single-tenant deployment expects.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import respx
from sqlalchemy.engine import URL

from provisa_client import ProvisaClient
from provisa_client.adbc import AdbcCursor, adbc_connect
from provisa_client.dbapi import connect
from provisa_client.sqlalchemy_dialect import ProvisaDialect

BASE = "http://localhost:8001"


def _login_ok():
    respx.post(f"{BASE}/auth/login").mock(
        return_value=httpx.Response(200, json={"access_token": "tok"})
    )


class TestProvisaClient:
    def test_http_requests_name_the_org(self):
        headers = ProvisaClient(BASE, token="tok", org="acme")._http_headers()
        assert headers["X-Org-Provisa"] == "acme"

    def test_flight_tickets_name_the_org(self):
        ticket = ProvisaClient(BASE, token="tok", org="acme")._flight_ticket("{ a { id } }", None)
        assert json.loads(ticket.ticket)["org"] == "acme"

    def test_nothing_is_sent_when_no_org_is_given(self):
        client = ProvisaClient(BASE, token="tok")
        assert "X-Org-Provisa" not in client._http_headers()
        assert "org" not in json.loads(client._flight_ticket("{ a { id } }", None).ticket)


class TestDbapi:
    @respx.mock
    def test_requests_name_the_org(self):
        _login_ok()
        conn = connect(BASE, username="u", password="p", org="acme")
        assert conn._headers()["X-Org-Provisa"] == "acme"

    @respx.mock
    def test_nothing_is_sent_when_no_org_is_given(self):
        _login_ok()
        conn = connect(BASE, username="u", password="p")
        assert "X-Org-Provisa" not in conn._headers()


class TestAdbc:
    @respx.mock
    def test_tickets_name_the_org(self):
        _login_ok()
        with patch("pyarrow.flight.connect", return_value=MagicMock()):
            conn = adbc_connect(BASE, user="u", password="p", org="acme")
        ticket = AdbcCursor(connection=conn)._build_ticket("SELECT 1")
        assert json.loads(ticket.ticket)["org"] == "acme"

    @respx.mock
    def test_nothing_is_sent_when_no_org_is_given(self):
        _login_ok()
        with patch("pyarrow.flight.connect", return_value=MagicMock()):
            conn = adbc_connect(BASE, user="u", password="p")
        ticket = AdbcCursor(connection=conn)._build_ticket("SELECT 1")
        assert "org" not in json.loads(ticket.ticket)


class TestSqlAlchemyUrl:
    def _args(self, **query):
        url = URL.create(
            "provisa+http", username="u", password="p", host="localhost", port=8001, query=query
        )
        return ProvisaDialect().create_connect_args(url)[1]

    def test_the_org_is_read_from_the_url(self):
        assert self._args(org="acme")["org"] == "acme"

    def test_no_org_in_the_url_names_none(self):
        assert "org" not in self._args()
