# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2b6d41-7a3c-4e19-9d5b-6c1a0f8e2b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1359 cross-surface contract: aggregate/group-by parity across every consumption surface.

Before REQ-1359, SQL/GraphQL/Cypher spoke ``enable_aggregates``/``enable_group_by`` natively
while gRPC/JSON:API/REST silently fell back to raw ``SELECT ... LIMIT n`` — the same logical
"count of orders per region" question returned an aggregated answer on three surfaces and raw,
un-aggregated rows on the other three. This harness pins the fix: one grouped-count query, issued
identically against ``tests/fixtures/sample_config.yaml``'s ``orders`` table (which this REQ turned
on ``enable_aggregates``/``enable_group_by`` for), must agree across every surface REQ-1359 touches,
plus GraphQL as the reference implementation the other three now match:

  * pgwire  — ground truth: ``SELECT region, COUNT(*) FROM orders GROUP BY region`` over the
              Provisa pgwire endpoint (still the governed pipeline, just SQL in).
  * graphql — the ``{field}_group_by(by: [region])`` root field schema_gen exposes natively;
              the reference implementation JSON:API/REST/gRPC now synthesize query text against.
  * jsonapi — ``?groupBy=region&aggregate=count`` (REQ-1359: synthesizes GraphQL text, was raw rows).
  * rest    — same query params, REST's flat response shape (REQ-1359, was raw rows).
  * grpc    — ``Query{Type}GroupBy`` streaming RPC (REQ-1359, was raw rows / didn't exist).

Cypher/Bolt is intentionally excluded: its aggregation always routes through the Trino engine tier,
and the ``sales-pg`` fixture source's ``host: localhost`` (the direct-driver address, see
``sample_config.yaml``) isn't reachable from Trino's coordinator container in this harness — a
pre-existing split-network gap in ``_TrinoJdbcConnector`` (``provisa/federation/trino_connectors.py``)
that predates and is unrelated to REQ-1359.
"""

from __future__ import annotations

import json

import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration]

_ORG = "protocol_aggregate_parity"
_ROLE = "org_admin"


def _region_counts(rows: list[tuple[str, int]]) -> dict[str, int]:
    return dict(sorted(rows))


# --------------------------------------------------------------------------- #
# Per-surface readers — each returns {region: count} for "orders grouped by region".
# --------------------------------------------------------------------------- #
def _read_pgwire(srv: IsolatedServer) -> dict[str, int]:
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=_ROLE, password="provisa"
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT region, COUNT(*) FROM orders GROUP BY region")
        return _region_counts([(r[0], int(r[1])) for r in cur.fetchall()])
    finally:
        conn.close()


def _discover_group_by_field(srv: IsolatedServer) -> str:
    """schema_gen names the root field ``{field}GroupBy`` (apollo) or ``{field}_group_by`` (snake)
    depending on the org's active naming convention — discover which via introspection rather than
    hardcoding, so this test doesn't silently stop covering the surface if the convention changes."""
    import httpx

    with httpx.Client(base_url=srv.base_url, timeout=60.0) as c:
        resp = c.post(
            "/data/graphql",
            json={"query": "{ __schema { queryType { fields { name } } } }"},
            headers={"x-provisa-role": _ROLE},
        )
    assert resp.status_code == 200, f"introspection: HTTP {resp.status_code} {resp.text[:300]}"
    names = {f["name"] for f in resp.json()["data"]["__schema"]["queryType"]["fields"]}
    candidates = [
        n for n in names if "orders" in n.lower() and "groupby" in n.lower().replace("_", "")
    ]
    assert candidates, f"no orders group-by root field in schema; fields={sorted(names)}"
    return candidates[0]


def _read_graphql(srv: IsolatedServer, gb_field: str) -> dict[str, int]:
    import httpx

    query = f"{{ {gb_field}(by: [region]) {{ groupKey aggregate {{ count }} }} }}"
    with httpx.Client(base_url=srv.base_url, timeout=60.0) as c:
        resp = c.post("/data/graphql", json={"query": query}, headers={"x-provisa-role": _ROLE})
    assert resp.status_code == 200, f"graphql: HTTP {resp.status_code} {resp.text[:300]}"
    body = resp.json()
    assert not body.get("errors"), f"graphql: {body.get('errors')}"
    rows = body["data"][gb_field]
    return _region_counts([(r["groupKey"]["region"], r["aggregate"]["count"]) for r in rows])


def _read_jsonapi(srv: IsolatedServer) -> dict[str, int]:
    import httpx

    with httpx.Client(base_url=srv.base_url, timeout=60.0) as c:
        resp = c.get(
            "/data/jsonapi/sales-analytics/orders",
            params={"groupBy": "region", "aggregate": "count"},
            headers={"x-provisa-role": _ROLE},
        )
    assert resp.status_code == 200, f"jsonapi: HTTP {resp.status_code} {resp.text[:300]}"
    rows = resp.json()["data"]
    return _region_counts(
        [
            (r["attributes"]["groupKey"]["region"], r["attributes"]["aggregate"]["count"])
            for r in rows
        ]
    )


def _read_rest(srv: IsolatedServer) -> dict[str, int]:
    import httpx

    with httpx.Client(base_url=srv.base_url, timeout=60.0) as c:
        resp = c.get(
            "/data/rest/sales-analytics/orders",
            params={"groupBy": "region", "aggregate": "count"},
            headers={"x-provisa-role": _ROLE},
        )
    assert resp.status_code == 200, f"rest: HTTP {resp.status_code} {resp.text[:300]}"
    rows = resp.json()["data"]
    return _region_counts([(r["groupKey"]["region"], r["aggregate"]["count"]) for r in rows])


def _read_grpc(srv: IsolatedServer) -> dict[str, int]:
    import grpc
    from google.protobuf.descriptor_pool import DescriptorPool
    from google.protobuf.message_factory import GetMessageClass
    from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
        ProtoReflectionDescriptorDatabase,
    )

    channel = grpc.insecure_channel(f"127.0.0.1:{srv.grpc_port}")
    try:
        db = ProtoReflectionDescriptorDatabase(channel)
        pool = DescriptorPool(db)
        svc_name = next(s for s in db.get_services() if s.endswith("Service"))
        svc = pool.FindServiceByName(svc_name)
        method = next(
            m
            for m in svc.methods
            if m.name.startswith("Query") and m.name.endswith("GroupBy") and "rder" in m.name
        )
        req_cls = GetMessageClass(method.input_type)
        resp_cls = GetMessageClass(method.output_type)
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        responses = list(
            rpc(req_cls(by=["region"]), metadata=(("x-provisa-role", _ROLE),), timeout=60)
        )
    finally:
        channel.close()
    rows = [
        (json.loads(m.group_key)["region"], m.aggregate.count)  # type: ignore[attr-defined]
        for m in responses
    ]
    return _region_counts(rows)


# --------------------------------------------------------------------------- #
# Fixture — one server exposing every surface, over sample_config.yaml's orders table.
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def server():
    srv = IsolatedServer(
        _ORG,
        enable_pgwire=True,
        await_grpc=True,
        config="tests/fixtures/sample_config.yaml",
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        import asyncio

        asyncio.run(drop_org_schema(_ORG))


# --------------------------------------------------------------------------- #
# The contract.
# --------------------------------------------------------------------------- #
def test_orders_grouped_by_region_agrees_across_every_surface(server):
    expected = _read_pgwire(server)
    assert expected, "pgwire ground truth returned no groups — seed data missing?"

    gb_field = _discover_group_by_field(server)

    results = {
        "graphql": _read_graphql(server, gb_field),
        "jsonapi": _read_jsonapi(server),
        "rest": _read_rest(server),
        "grpc": _read_grpc(server),
    }
    for surface, rows in results.items():
        assert rows == expected, f"{surface}: grouped counts diverged from pgwire ground truth"
