# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1d4e72-5b39-4fa6-b0d2-9e7a3f6c1d48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: gRPC answers on a deployment with auth on (REQ-273, REQ-1263, REQ-1266).

One real isolated server configured with an auth provider, over a Postgres source table this
module creates. A caller signs in over HTTP, presents the token over gRPC, is described its service by
reflection and reads the rows, so the interceptor, the org binding and the query pipeline are all
the real ones. A caller with no
credential, or a rejected one, reads nothing.
"""

# Requirements: REQ-273, REQ-1263, REQ-1266

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import bcrypt
import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "grpc_authenticated_query_e2e"
_SCHEMA = "grpc_authenticated_query_e2e_src"
_ROLE = "org_admin"
_USER = "grpc-user"
_PASSWORD = "grpc-user-password"
_ROWS = [(1, "east"), (2, "west"), (3, "north")]


async def _pg(statements: list[tuple[str, list]]) -> None:
    import asyncpg

    conn = await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )
    try:
        for sql, rows in statements:
            if rows:
                await conn.executemany(sql, rows)
            else:
                await conn.execute(sql)
    finally:
        await conn.close()


def _seed_source() -> None:
    asyncio.run(
        _pg(
            [
                (f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', []),
                (f'CREATE SCHEMA "{_SCHEMA}"', []),
                (
                    f'CREATE TABLE "{_SCHEMA}".orders (order_id integer PRIMARY KEY, '
                    "region varchar)",
                    [],
                ),
                (f'INSERT INTO "{_SCHEMA}".orders VALUES ($1, $2)', _ROWS),
            ]
        )
    )


def _config(work: Path) -> Path:
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    pg_source = next(s for s in base["sources"] if s["id"] == "sales-pg")
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{**pg_source, "id": "aq-pg"}]
    cfg["domains"] = [{"id": "sales-analytics", "description": "gRPC authenticated query e2e"}]
    cfg["tables"] = [
        {
            "source_id": "aq-pg",
            "domain_id": "sales-analytics",
            "schema": _SCHEMA,
            "table": "orders",
            "columns": [
                {"name": "order_id", "data_type": "integer", "visible_to": [_ROLE]},
                {"name": "region", "data_type": "varchar", "visible_to": [_ROLE]},
            ],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _auth() -> dict:
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "grpc-authenticated-query-e2e-signing-secret!",
        "simple": {
            "users": [
                {
                    "username": _USER,
                    "password_hash": bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode(),
                    "roles": [],
                }
            ]
        },
        "role_mapping": [],
        "default_role": _ROLE,
    }


class _Client:
    """A signed-in caller's token and a native client for the orders type, built from what server
    reflection describes to that caller."""

    def __init__(self, srv) -> None:
        import grpc
        from google.protobuf import descriptor_pb2
        from google.protobuf.descriptor_pool import DescriptorPool
        from google.protobuf.message_factory import GetMessageClass
        from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

        from provisa.grpc.descriptor import SERVICE_NAME

        self.srv = srv
        login = httpx.post(
            f"{srv.base_url}/auth/login",
            json={"username": _USER, "password": _PASSWORD},
            timeout=30,
        )
        login.raise_for_status()
        self.token = login.json()["access_token"]
        channel = grpc.insecure_channel(f"127.0.0.1:{srv.grpc_port}")
        try:
            (described,) = reflection_pb2_grpc.ServerReflectionStub(channel).ServerReflectionInfo(
                iter([reflection_pb2.ServerReflectionRequest(file_containing_symbol=SERVICE_NAME)]),
                metadata=(("authorization", f"Bearer {self.token}"),),
                timeout=120,
            )
        finally:
            channel.close()
        pool = DescriptorPool()
        # The file comes first and the files it imports after it: added imports first.
        for raw in reversed(described.file_descriptor_response.file_descriptor_proto):
            pool.Add(descriptor_pb2.FileDescriptorProto.FromString(raw))
        service = pool.FindServiceByName(SERVICE_NAME)
        self.service = service.full_name
        self.query = next(
            m for m in service.methods if m.name.startswith("Query") and m.name.endswith("Orders")
        )
        self.req_cls = GetMessageClass(self.query.input_type)
        self.row_cls = GetMessageClass(self.query.output_type)

    def rows(self, metadata: tuple) -> list:
        import grpc

        channel = grpc.insecure_channel(f"127.0.0.1:{self.srv.grpc_port}")
        try:
            rpc = channel.unary_stream(
                f"/{self.service}/{self.query.name}",
                request_serializer=self.req_cls.SerializeToString,
                response_deserializer=self.row_cls.FromString,
            )
            return list(rpc(self.req_cls(limit=10), metadata=metadata, timeout=120))
        finally:
            channel.close()


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("grpc_authenticated_query")
    _seed_source()
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        await_grpc=True,
        config=str(_config(work)),
        control_plane="postgres",
        auth=_auth(),
    )
    try:
        srv.start(timeout=240)
        yield _Client(srv)
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))
        asyncio.run(_pg([(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', [])]))


def test_an_authenticated_query_answers(client):
    rows = client.rows((("authorization", f"Bearer {client.token}"),))
    assert sorted((r.order_id, r.region) for r in rows) == _ROWS


def test_a_call_with_no_credential_is_refused(client):
    import grpc

    with pytest.raises(grpc.RpcError) as refused:
        client.rows(())
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED


def test_a_rejected_credential_is_refused(client):
    import grpc

    with pytest.raises(grpc.RpcError) as refused:
        client.rows((("authorization", "Bearer not-a-credential"),))
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert refused.value.details() == "credential rejected"
