# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a14-3f8b-4d65-b0e1-5a9d2c7f4e83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the per-request response-cache opt-in reaches the plan on every transport (REQ-544).

One real isolated server (DuckDB engine, Arrow Flight, Bolt, gRPC, HTTP; the stack's Redis as the
response cache) over a SQLite source. Per surface: a HINTED read writes exactly one entry under
this org, and the next identical read is served from it — proven by planting a marker value in
the stored rows and reading it back; an UNHINTED read writes nothing.

Surfaces: GraphQL ``@cached`` over Flight; Cypher ``// @provisa cache=true`` over Flight, HTTP
(``/data/cypher``) and Bolt; gRPC ``x-provisa-cache: true`` call metadata.
"""

from __future__ import annotations

import json

import pytest

from tests.integration.test_raw_sql_response_cache_e2e import (
    _ROLE,
    _entry_keys,
    _new_key,
    _redis,
    _start_server,
    _stop_server,
)

pytestmark = [pytest.mark.integration]

_ORG = "cache_opt_in_surfaces"
_MARK = 4242


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = _start_server(
        tmp_path_factory.mktemp("cacheoptin"),
        _ORG,
        {},
        enable_bolt=True,
        await_grpc=True,
    )
    try:
        yield srv
    finally:
        _stop_server(srv, _ORG)


def _plant(key: bytes) -> None:
    """Replace the first column of every stored row with _MARK (same kind and schema)."""
    import pyarrow as pa

    from provisa.cache.codec import decode_cache_payload, encode_cache_payload

    r = _redis()
    payload = decode_cache_payload(r.get(key))
    entry = payload["data"]
    if entry["kind"] == "rows":
        entry["rows"] = [[_MARK, *row[1:]] for row in entry["rows"]]
    else:
        table = pa.ipc.open_stream(entry["ipc"]).read_all()
        first = pa.array([_MARK] * table.num_rows, type=table.schema.field(0).type)
        table = table.set_column(0, table.schema.field(0), first)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        entry["ipc"] = sink.getvalue().to_pybytes()
    r.set(key, encode_cache_payload(payload), keepttl=True)


def _assert_opt_in(srv, read, hinted: str, plain: str) -> None:
    """hinted: MISS writes one entry, the planted HIT is served; plain: no entry at all."""
    before = _entry_keys(srv.org_id)
    miss = read(hinted)
    assert _MARK not in miss
    _plant(_new_key(srv.org_id, before))
    assert _MARK in read(hinted)  # only a cache HIT returns the planted value
    before = _entry_keys(srv.org_id)
    assert _MARK not in read(plain)
    assert _MARK not in read(plain)
    assert _entry_keys(srv.org_id) == before


def _flight_first_column(srv, query: str) -> list:
    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{srv.flight_port}")
    try:
        ticket = flight.Ticket(json.dumps({"query": query, "role": _ROLE}).encode())
        return client.do_get(ticket).read_all().column(0).to_pylist()
    finally:
        client.close()


def _http_cypher_first_column(srv, query: str) -> list:
    import httpx

    resp = httpx.post(
        f"{srv.base_url}/data/cypher",
        json={"query": query, "params": {}},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    first = body["columns"][0]
    return [row[first] if isinstance(row, dict) else row[0] for row in body["rows"]]


def _bolt_first_column(srv, query: str) -> list:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{srv.bolt_port}", auth=(_ROLE, ""))
    try:
        with driver.session() as sess:
            return [record[0] for record in sess.run(query)]
    finally:
        driver.close()


def _grpc_ids(srv, metadata: tuple) -> list:
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(srv.base_url, _ROLE)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and "vents" in m.name
        and not m.name.endswith(("Aggregate", "GroupBy", "Batch"))
    )
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    channel = grpc.insecure_channel(f"127.0.0.1:{srv.grpc_port}")
    try:
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        messages = list(rpc(req_cls(), metadata=(("x-provisa-role", _ROLE), *metadata), timeout=60))
    finally:
        channel.close()
    return [m.id for m in messages]  # type: ignore[attr-defined]


def test_graphql_cached_over_flight(server):
    _assert_opt_in(
        server,
        lambda q: _flight_first_column(server, q),
        "query @cached { rc__events { id } }",
        "query { rc__events { id } }",
    )


def test_cypher_hint_over_flight(server):
    _assert_opt_in(
        server,
        lambda q: _flight_first_column(server, q),
        "// @provisa cache=true\nMATCH (e:events) RETURN e.id AS id ORDER BY id",
        "MATCH (e:events) RETURN e.id AS id ORDER BY id",
    )


def test_cypher_hint_over_http(server):
    _assert_opt_in(
        server,
        lambda q: _http_cypher_first_column(server, q),
        "// @provisa cache=true\nMATCH (e:events) RETURN e.id AS id ORDER BY id DESC",
        "MATCH (e:events) RETURN e.id AS id ORDER BY id DESC",
    )


def test_cypher_hint_over_bolt(server):
    _assert_opt_in(
        server,
        lambda q: _bolt_first_column(server, q),
        "// @provisa cache_ttl=60\nMATCH (e:events) RETURN e.id AS id, e.ts AS ts ORDER BY id",
        "MATCH (e:events) RETURN e.id AS id, e.ts AS ts ORDER BY id",
    )


def test_grpc_metadata_hint(server):
    _assert_opt_in(
        server,
        lambda md: _grpc_ids(server, md),
        (("x-provisa-cache", "true"),),  # type: ignore[arg-type]
        (),  # type: ignore[arg-type]
    )
