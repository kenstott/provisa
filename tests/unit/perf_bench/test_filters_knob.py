# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: number of filters (equality predicates on filterable columns, values drawn from
each column's declared domain)."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_mix  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402
from queries import OPTIMISTIC  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)
TRANSPORT = "graphql"  # any: the base request reads Postgres orders on all of them
FILTERABLE = {"order_id", "customer_id", "region", "status"}
DOMAINS: dict[str, Any] = {
    "order_id": range(1, 20_000_001),
    "customer_id": range(1, 2_500_001),
    "region": ["NA", "EMEA", "APAC", "LATAM"],
    "status": ["open", "shipped", "delivered", "cancelled", "returned"],
}


def _raw() -> dict[str, Any]:
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _filters(raw: dict[str, Any], p: float, dist: dict[str, Any]) -> None:
    raw["knobs"]["filters"] = {"probability": p, "distribution": dist}


def _const(n: int) -> dict[str, Any]:
    return {"kind": "constant", "value": n}


def _setup(tmp_path: Path, mutate: Any = None) -> contract_model.Setup:
    raw = _raw()
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _orders_col(raw: dict[str, Any], name: str) -> dict[str, Any]:
    cols = raw["sources"]["bench-postgresql"]["tables"][0]["columns"]
    return next(c for c in cols if c["name"] == name)


# ------------------------------------------------------------------ the contract


def test_perf_contract_declares_domains_for_the_filterable_columns(tmp_path: Path) -> None:
    table = sc.base_table(_setup(tmp_path), "graphql")[1]
    assert {c.name for c in table.columns if c.filterable} == FILTERABLE
    assert all(c.domain is not None for c in table.columns if c.filterable)
    assert all(c.domain is None for c in table.columns if not c.filterable)


@pytest.mark.parametrize(
    "domain,msg",
    [
        ({"kind": "set", "values": []}, "values: at least one value"),
        ({"kind": "set", "values": ["a", "a"]}, "values: duplicate value 'a'"),
        ({"kind": "set", "values": [True]}, "must be a string or number"),
        ({"kind": "int_range", "min": 5, "max": 1}, "min must be <= max"),
        ({"kind": "int_range", "min": "a", "max": 1}, "value must be an integer"),
        ({"kind": "weird"}, "unknown domain kind 'weird'"),
        ({"kind": "set"}, "set requires 'values'"),
        ({"kind": "set", "values": ["a"], "x": 1}, "unknown key 'x'"),
    ],
)
def test_malformed_domain(tmp_path: Path, domain: dict, msg: str) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _orders_col(raw, "region")["domain"] = domain

    with pytest.raises(contract_model.SetupError, match=msg):
        _setup(tmp_path, mutate)


def test_filters_knob_is_no_longer_refused(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _filters(r, 0.5, _const(2)))
    assert setup.knobs.distributions["filters"].probability == 0.5


def test_filters_may_not_exceed_the_filterable_columns(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError,
        match="knobs.filters.distribution: draws up to 5 filters, a usable table has 4",
    ):
        _setup(tmp_path, lambda r: _filters(r, 1.0, {"kind": "uniform", "min": 0, "max": 5}))


def test_filters_need_a_domain_on_every_column_they_could_use(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        del _orders_col(raw, "status")["domain"]
        _filters(raw, 1.0, _const(4))

    with pytest.raises(
        contract_model.SetupError, match="draws up to 4 filters, a usable table has 3"
    ):
        _setup(tmp_path, mutate)


def test_transport_override_range_is_checked(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["rest"] = {
            "knobs": {"filters": {"probability": 1, "distribution": _const(9)}}
        }

    with pytest.raises(
        contract_model.SetupError, match="transports.rest.knobs.filters.distribution"
    ):
        _setup(tmp_path, mutate)


# ------------------------------------------------------------------ the draw


def test_no_knob_means_no_filter(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(_setup(tmp_path), "pgwire", "s", cacheable=True)
    assert {gen.next().filters for _ in range(50)} == {()}


def test_filters_are_distinct_filterable_columns_with_values_from_their_domain(
    tmp_path: Path,
) -> None:
    gen = request_mix.RequestGenerator(
        _setup(tmp_path, lambda r: _filters(r, 1.0, _const(3))), "pgwire", "s", cacheable=True
    )
    seen_cols: Counter[str] = Counter()
    for _ in range(400):
        filters = gen.next().filters
        cols = [c for c, _ in filters]
        assert len(cols) == len(set(cols)) == 3 and set(cols) <= FILTERABLE
        for col, value in filters:
            assert value in DOMAINS[col]
            seen_cols[col] += 1
    assert set(seen_cols) == FILTERABLE  # every filterable column gets used


def test_count_follows_the_distribution_and_probability(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(
        _setup(tmp_path, lambda r: _filters(r, 0.5, {"kind": "uniform", "min": 1, "max": 4})),
        "graphql",
        "s",
        cacheable=True,
    )
    counts = Counter(len(gen.next().filters) for _ in range(2000))
    assert set(counts) == {0, 1, 2, 3, 4}
    assert 900 < counts[0] < 1100


def test_same_seed_same_sequence(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _filters(r, 1.0, {"kind": "uniform", "min": 1, "max": 4}))
    a = request_mix.RequestGenerator(setup, "grpc", "x/1/2", cacheable=True)
    b = request_mix.RequestGenerator(setup, "grpc", "x/1/2", cacheable=True)
    assert [a.next() for _ in range(100)] == [b.next() for _ in range(100)]


def test_filters_do_not_change_the_other_knobs_draws(tmp_path: Path) -> None:
    def fields(raw: dict[str, Any]) -> None:
        raw["knobs"]["fields"] = {
            "probability": 1,
            "distribution": {"kind": "uniform", "min": 1, "max": 5},
        }
        raw["knobs"]["cache"]["probability"] = 0.5

    def both(raw: dict[str, Any]) -> None:
        fields(raw)
        _filters(raw, 1.0, _const(2))

    a = request_mix.RequestGenerator(_setup(tmp_path, fields), "grpc", "s", cacheable=True)
    b = request_mix.RequestGenerator(_setup(tmp_path, both), "grpc", "s", cacheable=True)
    sa = [a.next() for _ in range(100)]
    sb = [b.next() for _ in range(100)]
    assert [(s.columns, s.cached) for s in sa] == [(s.columns, s.cached) for s in sb]


def test_per_transport_override(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["rest"] = {
            "knobs": {"filters": {"probability": 1, "distribution": _const(2)}}
        }

    setup = _setup(tmp_path, mutate)
    assert (
        len(request_mix.RequestGenerator(setup, "rest", "s", cacheable=False).next().filters) == 2
    )
    assert request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True).next().filters == ()


# ------------------------------------------------------------------ the text each transport sends


def _spec(filters: tuple, cached: bool = False) -> request_mix.RequestSpec:
    return request_mix.RequestSpec("bench-postgresql", "orders", ("order_id",), 1, cached, filters)


def test_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), TRANSPORT, cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_render_two_filters(tmp_path: Path) -> None:
    q = request_render.Renderer(_setup(tmp_path)).render(
        _spec((("order_id", 7), ("region", "NA")), True)
    )
    assert q.sql == (
        "-- @provisa cache=true\nSELECT order_id FROM perf_bench.orders "
        "WHERE order_id = 7 AND region = 'NA' LIMIT 1"
    )
    assert q.cypher == (
        '// @provisa cache=true\nMATCH (o:PerfBench:Orders) WHERE o.orderId = 7 AND o.region = "NA" '
        "RETURN o.orderId AS order_id LIMIT 1"
    )
    assert q.graphql == (
        'query @cached { pb__orders(limit: 1, where: {orderId: {eq: 7}, region: {eq: "NA"}}) '
        "{ orderId } }"
    )
    assert q.grpc is not None and q.grpc["filter"] == {"order_id": 7, "region": "NA"}
    assert q.rest is not None and json.loads(q.rest["params"]["filter"]) == [
        {"field": "orderId", "comparator": "eq", "value": 7},
        {"field": "region", "comparator": "eq", "value": "NA"},
    ]
    assert q.jsonapi is not None
    assert q.jsonapi["params"]["filter[order_id]"] == 7
    assert q.jsonapi["params"]["filter[region]"] == "NA"


def test_sql_string_literals_are_escaped(tmp_path: Path) -> None:
    q = request_render.Renderer(_setup(tmp_path)).render(_spec((("region", "N'A"),)))
    assert "region = 'N''A'" in q.sql


def test_no_filter_adds_no_where_or_filter_keys(tmp_path: Path) -> None:
    q = request_render.Renderer(_setup(tmp_path)).render(_spec(()))
    assert "WHERE" not in q.sql and "WHERE" not in q.cypher and "where" not in q.graphql
    assert q.grpc is not None and "filter" not in q.grpc
    assert q.rest is not None and "filter" not in q.rest["params"]
    assert q.jsonapi is not None and not [k for k in q.jsonapi["params"] if k.startswith("filter")]


# ------------------------------------------------------------------ the gRPC client


class _Msg:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)
        self.read_mask = SimpleNamespace(paths=[])
        self.filter = SimpleNamespace(
            copied=None, CopyFrom=lambda m: setattr(self.filter, "copied", m)
        )

    SerializeToString = FromString = None  # noqa: N815

    def ListFields(self) -> list[int]:  # noqa: N802
        return [1]


def test_grpc_client_sends_the_filter(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sent: list[Any] = []

    class FakeTransport:
        def _resolve_message_class(self, name: str) -> Any:
            return _Msg

        _channel = SimpleNamespace(
            unary_stream=lambda path, **kw: (
                lambda req, metadata: sent.append((req, metadata)) or [req]
            )
        )

    monkeypatch.setattr(ol, "_grpc_shared", FakeTransport())
    setup = _setup(tmp_path)
    ep = SimpleNamespace(
        role="org_admin", grpc_host="h", grpc_port=1, http_base_url="x", credential=None
    )
    client = ol.GrpcClient(ep)  # type: ignore[arg-type]
    client.call(request_render.Renderer(setup).render(_spec((("order_id", 7),))), "org_admin")
    client.call(request_render.Renderer(setup).render(_spec(())), "org_admin")
    with_filter, plain = sent[0][0], sent[1][0]
    assert with_filter.filter.copied.order_id == 7
    assert plain.filter.copied is None


# ------------------------------------------------------------------ the report


def _rec(latency: float, n_filters: int) -> ol.Record:
    filters = tuple((c, 1) for c in ("order_id", "customer_id", "region", "status")[:n_filters])
    return (latency, _spec(filters), None, 1, 1)


def test_summary_by_filters() -> None:
    out = ol.summarize_requests([_rec(0.001, 0)] * 5 + [_rec(0.004, 2)] * 5, window_s=1.0)
    assert list(out["by_filters"]) == ["0", "2"]
    assert out["by_filters"]["2"]["p50_ms"] == 4.0
    assert out["by_filters"]["0"]["requests"] == 5
