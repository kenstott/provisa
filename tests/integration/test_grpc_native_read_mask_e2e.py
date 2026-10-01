# Copyright (c) 2026 Kenneth Stott
# Canary: 5d9e2b76-1f4a-4c83-a0b7-6e3c8d1f9a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the request's ``read_mask`` selects the columns the QUERY reads (REQ-803).

One real isolated server (``main:app`` plus the test-only driver SQL tap) over a Postgres source
table this module creates in the stack's Postgres. A native gRPC client compiled from the role's
published ``.proto`` (no reflection) and the HTTP gRPC proxy each send ``read_mask`` and the test
checks both ends of the request:

- the response carries only the masked fields; every other field stays at its proto default;
- the statement the source driver / engine receives selects only the masked columns — the mask
  is applied to the query, never by stripping fields off a full-row read;
- a path that is not a field the role can read is INVALID_ARGUMENT (HTTP 400) naming the path,
  and no statement reaches the source;
- a dotted sub-path into a JSON-valued column selects that column and restricts its value to the
  named keys — one semantics on the native RPC and on the proxy;
- the request's ``filter`` reaches the source as a WHERE on both surfaces, and a filter on a
  field the role cannot read is rejected by name on both;
- no mask returns every field;
- two different masks sent back-to-back each get their own fields (they never share a kept plan).
"""

# Requirements: REQ-803

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "grpc_read_mask_e2e"
_SCHEMA = "grpc_read_mask_e2e_src"
_ROLE = "org_admin"
_NARROW_ROLE = "analyst"  # cannot read ``amount``
_COLUMNS = ("order_id", "amount", "region", "status", "order_meta")
_ORDERS = [
    (1, 10.5, "east", "open"),
    (2, 20.25, "west", "open"),
    (3, 30.75, "east", "closed"),
    (4, 40.5, "north", "open"),
    (5, 50.25, "south", "closed"),
]


def _meta(order_id: int) -> dict:
    return {
        "source_id": f"s{order_id}",
        "created_by": f"u{order_id}",
        "tags": {"a": order_id, "b": -order_id},
    }


_ROWS = [(*order, json.dumps(_meta(order[0]))) for order in _ORDERS]


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
                    "amount numeric, region varchar, status varchar, order_meta jsonb)",
                    [],
                ),
                (f'INSERT INTO "{_SCHEMA}".orders VALUES ($1, $2, $3, $4, $5)', _ROWS),
            ]
        )
    )


def _config(work: Path) -> Path:
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    pg_source = next(s for s in base["sources"] if s["id"] == "sales-pg")
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{**pg_source, "id": "rm-pg"}]
    cfg["domains"] = [{"id": "sales-analytics", "description": "gRPC read_mask e2e"}]
    cfg["tables"] = [
        {
            "source_id": "rm-pg",
            "domain_id": "sales-analytics",
            "schema": _SCHEMA,
            "table": "orders",
            "enable_aggregates": True,
            "enable_group_by": True,
            "columns": [
                {"name": "order_id", "data_type": "integer", "visible_to": [_ROLE, _NARROW_ROLE]},
                {"name": "amount", "data_type": "numeric", "visible_to": [_ROLE]},
                {"name": "region", "data_type": "varchar", "visible_to": [_ROLE, _NARROW_ROLE]},
                {"name": "status", "data_type": "varchar", "visible_to": [_ROLE, _NARROW_ROLE]},
                {"name": "order_meta", "data_type": "jsonb", "visible_to": [_ROLE, _NARROW_ROLE]},
            ],
        }
    ]
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


class _Server:
    """The isolated server, its driver SQL tap, and a native gRPC client for the orders type."""

    def __init__(self, srv, tap: Path) -> None:
        from google.protobuf.message_factory import GetMessageClass

        from tests.grpc_proto_client import role_descriptor_pool

        self.srv = srv
        self.tap = tap
        _pool, svc = role_descriptor_pool(srv.base_url, _ROLE)
        self.service = svc.full_name
        self.query = next(
            m for m in svc.methods if m.name.startswith("Query") and m.name.endswith("Orders")
        )
        self.batch = next(
            m for m in svc.methods if m.name.startswith("Query") and m.name.endswith("OrdersBatch")
        )
        self.group_by = next(
            m
            for m in svc.methods
            if m.name.startswith("Query") and m.name.endswith("OrdersGroupBy")
        )
        self.type_name = self.query.name[len("Query") :]
        self.req_cls = GetMessageClass(self.query.input_type)
        self.row_cls = GetMessageClass(self.query.output_type)
        self.batch_cls = GetMessageClass(self.batch.output_type)

    def _statement_records(self) -> list[dict]:
        records = [json.loads(line) for line in self.tap.read_text().splitlines() if line]
        return [r for r in records if r["terminal"] in ("source", "engine")]

    def statements(self) -> list[str]:
        """Every statement a source driver or the engine has received, oldest first."""
        return [r["sql"] for r in self._statement_records()]

    def bound(self, seen: int, limit: int) -> list[tuple[str, list[str]]]:
        """``(statement, bound values)`` received since ``seen`` for this request's row limit —
        the limit is itself a bound value, which is how a request's statements are told apart."""
        return [
            (r["sql"], r["params"])
            for r in self._statement_records()[seen:]
            if repr(limit) in r["params"]
        ]

    def _rpc(self, method, resp_cls, request, role: str) -> list:
        import grpc

        channel = grpc.insecure_channel(f"127.0.0.1:{self.srv.grpc_port}")
        try:
            rpc = channel.unary_stream(
                f"/{self.service}/{method.name}",
                request_serializer=self.req_cls.SerializeToString,
                response_deserializer=resp_cls.FromString,
            )
            return list(rpc(request, metadata=(("x-provisa-role", role),), timeout=120))
        finally:
            channel.close()

    def request(self, limit: int, paths: list[str], filter_: dict | None = None):
        request = self.req_cls(limit=limit)
        request.read_mask.paths.extend(paths)
        for name, value in (filter_ or {}).items():
            setattr(request.filter, name, value)
        return request

    def rows(
        self, limit: int, paths: list[str], role: str = _ROLE, filter_: dict | None = None
    ) -> list:
        return self._rpc(self.query, self.row_cls, self.request(limit, paths, filter_), role)

    def batched_rows(self, limit: int, paths: list[str]) -> list:
        batches = self._rpc(self.batch, self.batch_cls, self.request(limit, paths), _ROLE)
        return [row for batch in batches for row in batch.rows]

    def group_by_counts(self, by: str, filter_: dict, role: str = _ROLE) -> dict[str, int]:
        """Native ``Query{Type}GroupBy``: the ``by`` value → row count of each group."""
        import grpc
        from google.protobuf.message_factory import GetMessageClass

        req_cls = GetMessageClass(self.group_by.input_type)
        request = req_cls(by=[by], funcs=["count"])
        for name, value in filter_.items():
            setattr(request.filter, name, value)
        channel = grpc.insecure_channel(f"127.0.0.1:{self.srv.grpc_port}")
        try:
            rpc = channel.unary_stream(
                f"/{self.service}/{self.group_by.name}",
                request_serializer=req_cls.SerializeToString,
                response_deserializer=GetMessageClass(self.group_by.output_type).FromString,
            )
            rows = list(rpc(request, metadata=(("x-provisa-role", role),), timeout=120))
        finally:
            channel.close()
        return {json.loads(r.group_key)[by]: r.aggregate.count for r in rows}

    def proxy_group_by(self, by: str, filter_: dict, role: str = _ROLE) -> httpx.Response:
        return httpx.post(
            f"{self.srv.base_url}/data/grpc/{self.type_name}GroupBy",
            headers={"x-provisa-role": role},
            json={"by": [by], "funcs": ["count"], "filter": filter_},
            timeout=120,
        )

    def proxy(
        self,
        limit: int,
        paths: list[str],
        role: str = _ROLE,
        filter_: dict | None = None,
        cache: bool = False,
    ) -> httpx.Response:
        body: dict = {"limit": limit, "read_mask": {"paths": paths}}
        if filter_ is not None:
            body["filter"] = filter_
        headers = {"x-provisa-role": role}
        if cache:
            headers["x-provisa-cache"] = "true"  # REQ-544: the per-request cache opt-in
        return httpx.post(
            f"{self.srv.base_url}/data/grpc/{self.type_name}",
            headers=headers,
            json=body,
            timeout=120,
        )


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("grpc_read_mask")
    tap = work / "driver_sql.jsonl"
    tap.touch()
    _seed_source()
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        await_grpc=True,
        config=str(_config(work)),
        control_plane="postgres",
        app="tests.integration.driver_sql_tap_app:app",
        env={"PROVISA_TEST_DRIVER_SQL_TAP": str(tap)},
    )
    try:
        srv.start(timeout=240)
        yield _Server(srv, tap)
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))
        asyncio.run(_pg([(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE', [])]))


def _set_fields(message) -> set[str]:
    return {field.name for field, _value in message.ListFields()}


def _read_statements(server: _Server, seen: int, limit: int) -> list[str]:
    """The statements received since ``seen`` that carry this request's row limit."""
    return [sql for sql, _params in server.bound(seen, limit)]


def _assert_selects_only(statements: list[str], masked: set[str]) -> None:
    assert statements, "no statement for this request reached a source driver or the engine"
    for sql in statements:
        named = {c for c in _COLUMNS if re.search(rf"\b{c}\b", sql)}
        assert named == masked, f"statement names columns {sorted(named)}: {sql}"


def test_masked_fields_only_and_the_source_reads_only_those_columns(server):
    seen = len(server.statements())
    rows = server.rows(7411, ["order_id", "amount"])

    assert len(rows) == len(_ROWS)
    assert all(_set_fields(r) == {"order_id", "amount"} for r in rows)
    assert sorted((r.order_id, r.amount) for r in rows) == [(o[0], o[1]) for o in _ORDERS]
    # unset → proto default
    assert all(r.region == "" and r.status == "" and r.order_meta == "" for r in rows)
    _assert_selects_only(_read_statements(server, seen, 7411), {"order_id", "amount"})


def test_no_mask_returns_every_field(server):
    seen = len(server.statements())
    rows = server.rows(7412, [])

    assert len(rows) == len(_ROWS)
    assert all(_set_fields(r) == set(_COLUMNS) for r in rows)
    _assert_selects_only(_read_statements(server, seen, 7412), set(_COLUMNS))


def test_unknown_path_is_invalid_argument_naming_the_path(server):
    import grpc

    seen = len(server.statements())
    with pytest.raises(grpc.RpcError) as failure:
        server.rows(7413, ["order_id", "no_such_field"])

    assert failure.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "no_such_field" in failure.value.details()
    assert _read_statements(server, seen, 7413) == []


def test_a_column_the_role_cannot_read_is_an_unknown_path(server):
    import grpc

    seen = len(server.statements())
    with pytest.raises(grpc.RpcError) as failure:
        server.rows(7414, ["order_id", "amount"], role=_NARROW_ROLE)

    assert failure.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "amount" in failure.value.details()
    assert _read_statements(server, seen, 7414) == []


def test_two_masks_back_to_back_each_get_their_own_fields(server):
    """Same type, role and limit — only the mask differs, so only the mask can tell the plans
    apart. The first mask is sent again last: it must not be answered from the second's plan."""
    for paths in (["order_id", "amount"], ["region", "status"], ["order_id", "amount"]):
        seen = len(server.statements())
        rows = server.rows(7415, paths)

        assert len(rows) == len(_ROWS)
        assert all(_set_fields(r) == set(paths) for r in rows), paths
        _assert_selects_only(_read_statements(server, seen, 7415), set(paths))


def _json_value(value) -> dict:
    """A JSON-valued column travels as JSON text (the proto field is ``string``)."""
    assert isinstance(value, str), value
    return json.loads(value)


def test_json_sub_path_restricts_the_value_and_selects_its_column(server):
    seen = len(server.statements())
    rows = server.rows(7419, ["order_id", "order_meta.source_id"])

    assert len(rows) == len(_ROWS)
    assert all(_set_fields(r) == {"order_id", "order_meta"} for r in rows)
    assert sorted((r.order_id, _json_value(r.order_meta)["source_id"]) for r in rows) == [
        (o[0], f"s{o[0]}") for o in _ORDERS
    ]
    assert all(_json_value(r.order_meta) == {"source_id": f"s{r.order_id}"} for r in rows)
    _assert_selects_only(_read_statements(server, seen, 7419), {"order_id", "order_meta"})


def test_json_sub_paths_nest_and_combine(server):
    rows = server.rows(7420, ["order_id", "order_meta.created_by", "order_meta.tags.a"])

    assert len(rows) == len(_ROWS)
    assert all(
        _json_value(r.order_meta) == {"created_by": f"u{r.order_id}", "tags": {"a": r.order_id}}
        for r in rows
    )


def test_sub_path_into_a_column_that_is_not_json_is_invalid_argument(server):
    import grpc

    seen = len(server.statements())
    with pytest.raises(grpc.RpcError) as failure:
        server.rows(7421, ["region.code"])

    assert failure.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "region.code" in failure.value.details()
    assert _read_statements(server, seen, 7421) == []


def test_http_proxy_json_sub_path_matches_the_native_rpc(server):
    paths = ["order_id", "order_meta.source_id"]
    seen = len(server.statements())
    response = server.proxy(7422, paths)

    assert response.status_code == 200, response.text
    proxied = {r["order_id"]: _json_value(r["order_meta"]) for r in response.json()}
    assert all(set(r) == {"order_id", "order_meta"} for r in response.json())
    _assert_selects_only(_read_statements(server, seen, 7422), {"order_id", "order_meta"})

    native = {r.order_id: _json_value(r.order_meta) for r in server.rows(7422, paths)}
    assert proxied == native == {o[0]: {"source_id": f"s{o[0]}"} for o in _ORDERS}


def test_batch_rpc_honors_the_mask(server):
    seen = len(server.statements())
    rows = server.batched_rows(7416, ["status"])

    assert len(rows) == len(_ROWS)
    assert all(_set_fields(r) == {"status"} for r in rows)
    _assert_selects_only(_read_statements(server, seen, 7416), {"status"})


def test_http_proxy_applies_the_mask_to_the_query(server):
    seen = len(server.statements())
    response = server.proxy(7417, ["order_id", "region"])

    assert response.status_code == 200, response.text
    rows = response.json()
    assert len(rows) == len(_ROWS)
    assert all(set(r) == {"order_id", "region"} for r in rows)
    _assert_selects_only(_read_statements(server, seen, 7417), {"order_id", "region"})


def test_http_proxy_unknown_path_is_a_400_naming_the_path(server):
    seen = len(server.statements())
    response = server.proxy(7418, ["order_id", "no_such_field"])

    assert response.status_code == 400, response.text
    assert "no_such_field" in response.text
    assert _read_statements(server, seen, 7418) == []


# -- filter: one lowering for the native RPC and the proxy ---------------------------------------


def _assert_filters_region(bound: list[tuple[str, list[str]]]) -> None:
    """The source received ``region = <placeholder>`` with 'east' bound to it — the value travels
    as a bound parameter, never as text in the statement."""
    assert bound, "no statement for this request reached a source driver or the engine"
    for sql, params in bound:
        assert re.search(r"WHERE\s.*\bregion\b\"?\s*=\s*[$%@?]", sql, re.S), sql
        assert "east" not in sql, sql
        assert "'east'" in params, (sql, params)


def test_http_proxy_filter_reaches_the_source_as_a_where(server):
    seen = len(server.statements())
    response = server.proxy(7423, [], filter_={"region": "east"})

    assert response.status_code == 200, response.text
    assert sorted(r["order_id"] for r in response.json()) == [1, 3]
    assert all(r["region"] == "east" for r in response.json())
    _assert_filters_region(server.bound(seen, 7423))


def test_http_proxy_filter_matches_the_native_rpc(server):
    """Same filter and mask on both surfaces: the same rows, and the same statement at the source."""
    paths, filter_ = ["order_id", "status"], {"region": "east", "status": "open"}

    seen = len(server.statements())
    native = server.rows(7424, paths, filter_=filter_)
    native_statements = server.bound(seen, 7424)

    seen = len(server.statements())
    response = server.proxy(7424, paths, filter_=filter_)
    proxy_statements = server.bound(seen, 7424)

    assert response.status_code == 200, response.text
    assert [(r.order_id, r.status) for r in native] == [(1, "open")]
    assert [(r["order_id"], r["status"]) for r in response.json()] == [(1, "open")]
    _assert_filters_region(native_statements)
    assert proxy_statements == native_statements


def test_filter_on_a_field_that_does_not_exist_is_a_400_naming_the_field(server):
    seen = len(server.statements())
    response = server.proxy(7425, [], filter_={"no_such_field": "x"})

    assert response.status_code == 400, response.text
    assert "no_such_field" in response.text
    assert _read_statements(server, seen, 7425) == []


def test_filter_on_a_column_the_role_cannot_read_is_rejected_on_both_surfaces(server):
    import grpc

    seen = len(server.statements())
    with pytest.raises(grpc.RpcError) as failure:
        server.rows(7426, [], role=_NARROW_ROLE, filter_={"amount": 10.5})
    response = server.proxy(7426, [], role=_NARROW_ROLE, filter_={"amount": 10.5})

    assert failure.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "amount" in failure.value.details()
    assert response.status_code == 400, response.text
    assert "amount" in response.text
    assert _read_statements(server, seen, 7426) == []


def test_http_proxy_group_by_filter_matches_the_native_rpc(server):
    """``Query{Type}GroupBy`` with a filter: the proxy groups the same filtered rows as the native
    RPC, and the predicate is in the statement the source receives on both."""
    open_by_region = {"east": 1, "west": 1, "north": 1}  # orders 1, 2, 4; 3 and 5 are closed

    seen = len(server.statements())
    native = server.group_by_counts("region", {"status": "open"})
    native_statements = [s for s in server.statements()[seen:] if "GROUP BY" in s]

    seen = len(server.statements())
    response = server.proxy_group_by("region", {"status": "open"})
    proxy_statements = [s for s in server.statements()[seen:] if "GROUP BY" in s]

    assert response.status_code == 200, response.text
    proxied = {r["group_key"]["region"]: r["aggregate"]["count"] for r in response.json()}
    assert native == open_by_region
    assert proxied == open_by_region
    assert native_statements and all(
        re.search(r"WHERE\s.*\bstatus\b", sql, re.S) for sql in native_statements
    )
    assert proxy_statements == native_statements


def test_http_proxy_group_by_filter_value_must_be_a_scalar(server):
    response = server.proxy_group_by("region", {"status": ["open"]})

    assert response.status_code == 400, response.text
    assert "status" in response.text


def test_group_by_filter_on_a_column_the_role_cannot_read_is_invalid_argument(server):
    """GitHub issue 131, on ``Query{Type}GroupBy``: the same refusal as ``Query{Type}``."""
    import grpc

    with pytest.raises(grpc.RpcError) as failure:
        server.group_by_counts("region", {"amount": 10.5}, role=_NARROW_ROLE)

    assert failure.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "amount" in failure.value.details()


@pytest.mark.parametrize(
    ("filter_", "field"),
    [
        ({"order_id": "1"}, "order_id"),  # integer column, text value
        ({"amount": "10.5"}, "amount"),  # numeric column, text value
        ({"region": 5}, "region"),  # text column, number value
        ({"order_id": True}, "order_id"),  # a boolean is not a number
    ],
)
def test_http_proxy_filter_value_of_the_wrong_type_is_a_400_naming_the_field(
    server, filter_, field
):
    seen = len(server.statements())
    response = server.proxy(7427, [], filter_=filter_)

    assert response.status_code == 400, response.text
    assert field in response.text
    assert _read_statements(server, seen, 7427) == []


@pytest.mark.parametrize("rpc", ["query", "group_by"])
def test_http_proxy_filter_on_a_hidden_column_is_refused_whatever_the_value(server, rpc):
    """GitHub issue 131 on the HTTP proxy, which reads the body filter: ``analyst`` cannot read
    ``amount``, so a filter on it is HTTP 400 ``data.invalid_filter_field`` naming the field —
    the identical answer for an amount some row has and one no row has, so nothing about the
    hidden value can be read off the response — and no statement reaches the source."""

    def call(amount: float) -> httpx.Response:
        if rpc == "query":
            return server.proxy(7428, [], role=_NARROW_ROLE, filter_={"amount": amount})
        return server.proxy_group_by("region", {"amount": amount}, role=_NARROW_ROLE)

    seen = len(server.statements())
    present, absent = call(10.5), call(999.0)  # order 1's amount; no order's amount

    for response in (present, absent):
        assert response.status_code == 400, response.text
        assert response.json()["code"] == "data.invalid_filter_field", response.text
        assert "amount" in response.text
    assert present.json() == absent.json()
    assert server.statements()[seen:] == []

    # The control: a role that reads the column filters on it, and the count follows the value.
    if rpc == "query":
        assert [r["order_id"] for r in server.proxy(7429, [], filter_={"amount": 10.5}).json()] == [
            1
        ]
        assert server.proxy(7429, [], filter_={"amount": 999.0}).json() == []


def test_http_proxy_hinted_request_is_a_cache_hit_the_second_time(server):
    """REQ-544/REQ-1897 on the proxy: an opted-in request runs at the source once; the repeat is
    answered from the response cache and no statement reaches the source. Another filter value is
    another entry."""
    paths, east = ["order_id", "region"], {"region": "east"}

    seen = len(server.statements())
    miss = server.proxy(7430, paths, filter_=east, cache=True)
    assert miss.status_code == 200, miss.text
    assert len(server.bound(seen, 7430)) == 1

    seen = len(server.statements())
    hit = server.proxy(7430, paths, filter_=east, cache=True)
    assert hit.json() == miss.json()
    assert sorted(r["order_id"] for r in hit.json()) == [1, 3]
    assert server.statements()[seen:] == []

    seen = len(server.statements())
    west = server.proxy(7430, paths, filter_={"region": "west"}, cache=True)
    assert [r["order_id"] for r in west.json()] == [2]
    assert len(server.bound(seen, 7430)) == 1

    seen = len(server.statements())
    unhinted = server.proxy(7430, paths, filter_=east)
    assert unhinted.json() == miss.json()
    assert len(server.bound(seen, 7430)) == 1  # no opt-in: runs at the source
