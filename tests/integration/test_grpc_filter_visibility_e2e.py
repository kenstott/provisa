# Copyright (c) 2026 Kenneth Stott
# Canary: 1d6f8b32-9e4a-4c57-a3b0-7f2e5c9d8a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a hidden column's values cannot be probed through a gRPC filter (REQ-1860, #131).

One real isolated server over a Postgres table whose ``amount`` column is visible to ``org_admin``
only. The native server serves one wire proto, whose ``{Type}Filter`` message carries every
column, so ``analyst`` can set ``filter.amount``. If that predicate ran, the rows returned — and
their count — would say whether a row with that amount exists: the hidden value, confirmed or
refuted. Both RPCs that take a filter (``Query{Type}`` and ``Query{Type}GroupBy``) must answer
INVALID_ARGUMENT naming the field, identically for a value that exists and one that does not.
"""

# Requirements: REQ-1860

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "grpc_filter_vis_e2e"
_SCHEMA = "grpc_filter_vis_e2e_src"
_ADMIN = "org_admin"
_NARROW = "analyst"  # cannot read ``amount``
_ROWS = [(1, 10.5, "east"), (2, 20.25, "west"), (3, 30.75, "east")]
_PRESENT, _ABSENT = 10.5, 999.0  # an amount some row has, and one no row has


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


def _config(work: Path) -> Path:
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    pg_source = next(s for s in base["sources"] if s["id"] == "sales-pg")
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{**pg_source, "id": "fv-pg"}]
    cfg["domains"] = [{"id": "sales-analytics", "description": "gRPC filter visibility e2e"}]
    cfg["tables"] = [
        {
            "source_id": "fv-pg",
            "domain_id": "sales-analytics",
            "schema": _SCHEMA,
            "table": "orders",
            "enable_aggregates": True,
            "enable_group_by": True,
            "columns": [
                {"name": "order_id", "data_type": "integer", "visible_to": [_ADMIN, _NARROW]},
                {"name": "amount", "data_type": "numeric", "visible_to": [_ADMIN]},
                {"name": "region", "data_type": "varchar", "visible_to": [_ADMIN, _NARROW]},
            ],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


class _Client:
    """A native gRPC client compiled from the published ``.proto`` (no reflection)."""

    def __init__(self, srv) -> None:
        import grpc
        from google.protobuf.message_factory import GetMessageClass

        from tests.grpc_proto_client import role_descriptor_pool

        _pool, svc = role_descriptor_pool(srv.base_url, _ADMIN)
        self._service = svc.full_name
        self._query = next(
            m for m in svc.methods if m.name.startswith("Query") and m.name.endswith("Orders")
        )
        self._group_by = next(
            m
            for m in svc.methods
            if m.name.startswith("Query") and m.name.endswith("OrdersGroupBy")
        )
        self._classes = GetMessageClass
        self.channel = grpc.insecure_channel(f"127.0.0.1:{srv.grpc_port}")

    def _call(self, method, request, role: str) -> list:
        rpc = self.channel.unary_stream(
            f"/{self._service}/{method.name}",
            request_serializer=type(request).SerializeToString,
            response_deserializer=self._classes(method.output_type).FromString,
        )
        return list(rpc(request, metadata=(("x-provisa-role", role),), timeout=120))

    def query(self, role: str, **filter_) -> list:
        request = self._classes(self._query.input_type)()
        for name, value in filter_.items():
            setattr(request.filter, name, value)
        return self._call(self._query, request, role)

    def group_by_region(self, role: str, **filter_) -> dict[str, int]:
        request = self._classes(self._group_by.input_type)(by=["region"], funcs=["count"])
        for name, value in filter_.items():
            setattr(request.filter, name, value)
        rows = self._call(self._group_by, request, role)
        return {json.loads(r.group_key)["region"]: r.aggregate.count for r in rows}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    asyncio.run(
        _pg(
            [
                (f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', []),
                (f'CREATE SCHEMA "{_SCHEMA}"', []),
                (
                    f'CREATE TABLE "{_SCHEMA}".orders '
                    "(order_id integer PRIMARY KEY, amount numeric, region varchar)",
                    [],
                ),
                (f'INSERT INTO "{_SCHEMA}".orders VALUES ($1, $2, $3)', _ROWS),
            ]
        )
    )
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        await_grpc=True,
        config=str(_config(tmp_path_factory.mktemp("grpc_filter_vis"))),
        control_plane="postgres",
    )
    grpc_client = None
    try:
        srv.start(timeout=240)
        grpc_client = _Client(srv)
        yield grpc_client
    finally:
        if grpc_client is not None:
            grpc_client.channel.close()
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))
        asyncio.run(_pg([(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', [])]))


def _rejection(call) -> tuple:
    import grpc

    with pytest.raises(grpc.RpcError) as failure:
        call()
    return failure.value.code(), failure.value.details()


def test_a_role_that_reads_the_column_filters_on_it(client):
    """The control: the predicate works, and its row count follows the value."""
    assert [r.order_id for r in client.query(_ADMIN, amount=_PRESENT)] == [1]
    assert client.query(_ADMIN, amount=_ABSENT) == []
    assert client.group_by_region(_ADMIN, amount=_PRESENT) == {"east": 1}


def test_query_filter_on_a_hidden_column_is_rejected_whatever_the_value(client):
    import grpc

    present = _rejection(lambda: client.query(_NARROW, amount=_PRESENT))
    absent = _rejection(lambda: client.query(_NARROW, amount=_ABSENT))

    assert present[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert "amount" in present[1]
    assert present == absent  # nothing distinguishes a value that exists from one that does not


def test_group_by_filter_on_a_hidden_column_is_rejected_whatever_the_value(client):
    import grpc

    present = _rejection(lambda: client.group_by_region(_NARROW, amount=_PRESENT))
    absent = _rejection(lambda: client.group_by_region(_NARROW, amount=_ABSENT))

    assert present[0] == grpc.StatusCode.INVALID_ARGUMENT
    assert "amount" in present[1]
    assert present == absent


def test_the_role_still_filters_on_the_columns_it_reads(client):
    assert sorted(r.order_id for r in client.query(_NARROW, region="east")) == [1, 3]
    assert client.group_by_region(_NARROW, region="east") == {"east": 2}
