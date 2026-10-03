# Copyright (c) 2026 Kenneth Stott
# Canary: 9d68b2d6-c1fb-4273-b74a-0149abb2fdaf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A deployment configured with an auth provider authenticates on every surface it serves.

The server is started the way a deployment starts it: the provider is in its config file and
nothing else is set. Each surface is then offered a credential the provider does not know.
"""

from __future__ import annotations

import uuid

import httpx
import psycopg2
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.e2e]

_ORG = f"authsurf{uuid.uuid4().hex[:8]}"
_JWT_SECRET = "configured-provider-surfaces-signing-secret-48b!"
_USER = "surface-user"
_PASSWORD = "surface-user-password"


@pytest_asyncio.fixture(scope="module")
async def server():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    srv = IsolatedServer(
        _ORG,
        engine="trino",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config="tests/fixtures/sample_config.yaml",
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "default_role": "analyst"},
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        await drop_org_schema(_ORG)


def _pg_connect(srv, user: str, password: str):
    return psycopg2.connect(
        host="127.0.0.1",
        port=srv.pgwire_port,
        dbname="provisa",
        user=user,
        password=password,
        connect_timeout=30,
    )


class TestHttp:
    def test_the_deployment_reports_its_provider(self, server):
        resp = httpx.get(f"{server.base_url}/auth/provider-type", timeout=30)
        assert resp.json() == {"provider": "basic"}

    def test_a_request_without_a_credential_is_refused(self, server):
        resp = httpx.get(f"{server.base_url}/admin/config", timeout=30)
        assert resp.status_code == 401

    def test_login_is_served_and_refuses_an_unknown_account(self, server):
        resp = httpx.post(
            f"{server.base_url}/auth/login",
            json={"username": "nobody", "password": "nothing"},
            timeout=30,
        )
        assert resp.status_code == 401, resp.text


class TestPgwire:
    @pytest.mark.parametrize("role", ["org_admin", "analyst"])
    def test_a_role_name_with_any_password_is_refused(self, server, role):
        with pytest.raises(psycopg2.OperationalError):
            _pg_connect(server, role, "not-a-credential").close()

    def test_an_unknown_account_is_refused(self, server):
        with pytest.raises(psycopg2.OperationalError):
            _pg_connect(server, "nobody", "nothing").close()


class TestBolt:
    @pytest.mark.parametrize("principal", ["org_admin", "analyst", "nobody"])
    def test_an_unknown_credential_is_refused(self, server, principal):
        import neo4j
        from neo4j.exceptions import Neo4jError, ServiceUnavailable

        driver = neo4j.GraphDatabase.driver(
            f"bolt://127.0.0.1:{server.bolt_port}", auth=(principal, "not-a-credential")
        )
        try:
            with pytest.raises((Neo4jError, ServiceUnavailable)):
                driver.verify_connectivity()
        finally:
            driver.close()


class TestFlight:
    """Arrow Flight authenticates a bearer credential carried in the ticket."""

    def _get(self, server, token: str | None):
        import json

        import pyarrow.flight as flight

        ticket = {"query": "{ __typename }"}
        if token is not None:
            ticket["token"] = token
        client = flight.connect(f"grpc://127.0.0.1:{server.flight_port}")
        try:
            client.do_get(flight.Ticket(json.dumps(ticket).encode())).read_all()
        finally:
            client.close()

    @pytest.mark.parametrize("token", [None, "not-a-credential", "provisa_pat_not_issued"])
    def test_an_unknown_credential_is_refused(self, server, token):
        import pyarrow.flight as flight

        with pytest.raises(flight.FlightUnauthenticatedError):
            self._get(server, token)


class TestGrpc:
    """gRPC validates the bearer before any method runs. An unknown method answers
    UNIMPLEMENTED only to a caller whose credential holds, so UNAUTHENTICATED here means the
    credential itself was refused."""

    @pytest.mark.parametrize("metadata", [[], [("authorization", "Bearer not-a-credential")]])
    def test_an_unknown_credential_is_refused(self, server, metadata):
        import grpc

        channel = grpc.insecure_channel(f"127.0.0.1:{server.grpc_port}")
        try:
            call = channel.unary_unary("/provisa.Probe/Nothing")
            with pytest.raises(grpc.RpcError) as refused:
                call(b"", metadata=metadata, timeout=30)
            assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
        finally:
            channel.close()
