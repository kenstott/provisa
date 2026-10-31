# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The proof's servers are as its contract says, and a refusal by design is not a failure."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import httpx
import jwt
import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import deployment_fixture as fx  # noqa: E402
import local_proof as lp  # noqa: E402
import lookup  # noqa: E402
import optimistic_load as ol  # noqa: E402
from run_benchmark import GraphqlTransport, joined_rows  # noqa: E402
from server_answer import checked  # noqa: E402


def _response(status: int, body: str) -> httpx.Response:
    return httpx.Response(status, text=body, request=httpx.Request("POST", "http://x/data/graphql"))


def test_a_failed_response_keeps_the_servers_answer() -> None:
    body = '{"detail":"nope","code":"data.row_level_filter_required","params":{"table":"t"}}'
    with pytest.raises(httpx.HTTPStatusError) as caught:
        checked(_response(400, body))
    assert body in str(caught.value)
    assert "400" in str(caught.value)
    assert caught.value.response.text == body


def test_a_good_response_passes_through() -> None:
    resp = _response(200, "{}")
    assert checked(resp) is resp


def test_the_brief_of_a_refusal_keeps_its_reason() -> None:
    with pytest.raises(httpx.HTTPStatusError) as caught:
        checked(_response(500, "x" * 300 + " the reason"))
    assert "the reason" in ol._brief(caught.value)  # noqa: SLF001
    assert len(ol._brief(RuntimeError("y" * 500))) < 200  # noqa: SLF001


class _Post:
    def __init__(self, resp: httpx.Response) -> None:
        self._resp = resp

    def post(self, *_a: Any, **_k: Any) -> httpx.Response:
        return self._resp


def test_the_matrix_transport_reports_the_body() -> None:
    t = GraphqlTransport("http://x")
    t._client = _Post(_response(400, '{"code":"data.row_level_filter_required"}'))  # noqa: SLF001
    with pytest.raises(httpx.HTTPStatusError, match="row_level_filter_required"):
        t.run_graphql("query { a }", {}, no_cache=True)


def test_the_load_client_reports_the_body() -> None:
    client = ol.GraphqlClient.__new__(ol.GraphqlClient)
    client._cred = None  # noqa: SLF001
    client._http = _Post(_response(400, '{"code":"data.row_level_filter_required"}'))  # noqa: SLF001
    query = ol.Query(id="q", category="c", description="d", graphql="{ a }")
    with pytest.raises(httpx.HTTPStatusError, match="row_level_filter_required"):
        client.call(query, "org_admin")


def test_a_nested_answer_counts_as_the_join_does() -> None:
    order = {
        "orderId": 1,
        "orderEvents": [{"eventType": "a"}, {"eventType": "b"}, {"eventType": "c"}],
        "orderDoc": {"status": "s"},
    }
    assert joined_rows(order) == 3


def test_a_row_with_no_child_is_dropped_as_an_inner_join_drops_it() -> None:
    assert joined_rows({"orderId": 1, "orderEvents": [], "orderDoc": {"status": "s"}}) == 0


def test_a_flat_row_and_a_group_key_are_one_row() -> None:
    assert joined_rows({"orderId": 1, "amount": None}) == 1
    assert (
        joined_rows({"groupKey": ["east", 2024], "aggregate": {"count": 4, "sum": {"a": 1}}}) == 1
    )


def test_a_nested_graphql_answer_is_counted_flat() -> None:
    body = {"data": {"pb__orders": [{"orderId": 1, "orderEvents": [{"t": 1}, {"t": 2}]}]}}
    t = GraphqlTransport("http://x")
    t._client = _Post(
        httpx.Response(  # noqa: SLF001
            200, json=body, request=httpx.Request("POST", "http://x/data/graphql")
        )
    )
    assert t.run_graphql("query { a }", {}, no_cache=True)[0] == 2


def test_the_token_carries_the_claim_the_simple_provider_reads() -> None:
    claims = jwt.decode(
        lp.proof_token(4_000_000_000),
        lp.JWT_SECRET,
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    assert claims["username"] == lp.AUTH_USER
    assert claims["roles"] == ["org_admin"]


def test_the_auth_config_names_a_default_role() -> None:
    cfg = lp.auth_config({"auth": {"provider": "none"}, "sources": []}, "hash")
    assert cfg["auth"]["default_role"] == "org_admin"
    assert cfg["auth"]["provider"] == "simple"
    assert cfg["auth"]["simple"]["users"][0]["password_hash"] == "hash"
    assert cfg["sources"] == []


def test_the_server_opens_the_mcp_listener_the_contract_dials() -> None:
    assert lp._mcp_env({"mcp": 21009})["PROVISA_MCP_PORT"] == "21009"  # noqa: SLF001


def test_the_contract_dials_the_leased_mcp_port() -> None:
    class Server:
        pgwire_port = bolt_port = flight_port = grpc_port = 1
        base_url = "http://x"

    raw = lp.proof_contract(Server(), "p", mcp_port=21009)
    assert raw["deployment"]["endpoints"]["mcp"]["url"] == "http://127.0.0.1:21009/mcp"


def test_the_replica_ttl_reaches_the_tables_of_the_source() -> None:
    cfg = lp.replica_config(yaml.safe_load((lp.PERF / "config.yaml").read_text()))
    tables = [t for t in cfg["tables"] if t["source_id"] == "bench-clickhouse"]
    assert tables
    assert {t["cache_ttl"] for t in tables} == {60}
    other = next(t for t in cfg["tables"] if t["source_id"] == "bench-mongodb")
    assert other["cache_ttl"] == 300


TEXT = "Table 'bench_order_node' is replicated row by row because it is large"


@pytest.mark.parametrize(
    "row",
    [
        {
            "error": "HTTPStatusError: 400",
            "response_body": '{"code":"data.row_level_filter_required"}',
        },
        {"error": f"InternalError_: {TEXT}"},
        {"error": f"_InactiveRpcError: details = {TEXT}"},
    ],
)
def test_the_row_level_refusal_is_by_design(row: dict[str, Any]) -> None:
    assert lp.refused_by_design(row, True)


def test_the_refusal_of_a_table_not_row_level_is_a_failure() -> None:
    assert not lp.refused_by_design({"error": TEXT}, False)


def test_another_error_is_a_failure() -> None:
    assert not lp.refused_by_design({"error": "HTTPStatusError: 500 boom"}, True)


def test_a_token_has_no_cell_on_pgwire() -> None:
    assert not lp.credential_reaches("token", "pgwire")
    assert lp.credential_reaches("password", "pgwire")
    assert lp.credential_reaches("token", "bolt")


class _Cursor:
    def __init__(self, sql: str) -> None:
        self.sql = sql

    def fetchall(self) -> list[Any]:
        return []

    def fetchone(self) -> tuple[int]:
        return (0,)


class _Conn:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def execute(self, sql: Any, params: Any = None) -> _Cursor:
        self.sent.append(str(sql))
        return _Cursor(str(sql))

    def close(self) -> None:
        pass


def test_verify_routes_sends_no_unfiltered_read_of_a_row_level_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import psycopg
    import replication
    import setup_contract as sc

    class Server:
        pgwire_port = bolt_port = flight_port = grpc_port = 1
        base_url = "http://x"

    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://x")
    raw = lp.proof_contract(Server(), "p", mcp_port=21009)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    unbound = sc.load_setup(path, known_transports=list(ol.TRANSPORTS))
    resolved = lookup.resolve(*sc.identities(unbound), fx.build())
    bound = sc.load_setup(
        path, known_transports=list(ol.TRANSPORTS), deployment=resolved, verify_replication=False
    )
    conn = _Conn()
    monkeypatch.setattr(psycopg, "connect", lambda **_k: conn)
    monkeypatch.setattr(replication, "verify_routes", lambda *a, **k: {"verified": 0})
    result = lp.verify_routes(bound, resolved, ol.endpoints_from_setup(bound), raw)
    assert result["verification"] == {"verified": 0}
    neo4j = [s for s in conn.sent if "bench_order_node" in s]
    assert neo4j, conn.sent
    assert all("WHERE" in s for s in neo4j)


def test_each_deployment_has_its_own_grpc_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    import run_benchmark

    class Transport:
        def __init__(self, host: str, port: int, *_a: Any) -> None:
            self.port = port

        def available(self) -> bool:
            return True

    monkeypatch.setattr(run_benchmark, "GrpcTransport", Transport)
    monkeypatch.setattr(ol, "_grpc_shared", {})

    def ep(port: int) -> Any:
        return SimpleNamespace(
            grpc_host="h", grpc_port=port, http_base_url="x", role="r", credential=None
        )

    first = ol._grpc_transport(ep(1))  # noqa: SLF001
    assert ol._grpc_transport(ep(1)) is first  # noqa: SLF001
    assert ol._grpc_transport(ep(2)).port == 2  # noqa: SLF001
