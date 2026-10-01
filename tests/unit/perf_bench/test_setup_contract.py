# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the benchmark setup data contract — schema, validation, zero-knob parity."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_checks  # noqa: E402
import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import optimistic_load  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402
from queries import OPTIMISTIC  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(optimistic_load.TRANSPORTS)
TRANSPORT = "graphql"


def _raw() -> dict[str, Any]:
    # a JSON round trip un-shares the YAML anchors, so a test edits one knob
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _load(
    raw: dict[str, Any], tmp_path: Path, env: dict[str, str] | None = None
) -> contract_model.Setup:
    path = tmp_path / "setup.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(
        path,
        environ=ENV if env is None else env,
        known_transports=TRANSPORTS,
        deployment=fx.build(),
    )


def _fails(
    raw: dict[str, Any], tmp_path: Path, message: str, env: dict[str, str] | None = None
) -> None:
    with pytest.raises(contract_model.SetupError) as exc:
        _load(raw, tmp_path, env)
    assert message in str(exc.value)


# ---------------------------------------------------------------- the perf stack's own contract


def test_perf_contract_loads_with_every_knob_zero(tmp_path: Path) -> None:
    setup = sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )
    assert setup.name == "perf-stack"
    assert list(setup.transports) == TRANSPORTS
    assert setup.load.steps == optimistic_load.DEFAULT_STEPS
    assert setup.load.window_s == 20.0
    for name, knob in setup.knobs.distributions.items():
        assert knob.probability == 0, name
    assert [r.role for r in setup.roles] == ["org_admin"]


def test_zero_knob_query_equals_optimistic() -> None:
    setup = sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )
    q = request_render.build_query(setup, TRANSPORT, cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


# ---------------------------------------------------------------- errors name the field


def test_unknown_top_level_key(tmp_path: Path) -> None:
    raw = _raw()
    raw["bogus"] = 1
    _fails(raw, tmp_path, "unknown key 'bogus'")


def test_unknown_knob_key(tmp_path: Path) -> None:
    raw = _raw()
    raw["knobs"]["fieldz"] = raw["knobs"]["fields"]
    _fails(raw, tmp_path, "knobs: unknown key 'fieldz'")


def test_missing_required_field(tmp_path: Path) -> None:
    raw = _raw()
    del raw["seed"]
    _fails(raw, tmp_path, "seed: required")


def test_secret_in_credentials_is_not_a_key(tmp_path: Path) -> None:
    raw = _raw()
    raw["deployment"]["credentials"] = {"mode": "env", "env": "X", "password": "s3cret"}
    _fails(raw, tmp_path, "deployment.credentials: unknown key 'password'")


def test_credentials_env_must_be_set(tmp_path: Path) -> None:
    raw = _raw()
    raw["deployment"]["credentials"] = {"mode": "env", "kind": "token", "env": "BENCH_TOKEN"}
    _fails(raw, tmp_path, "BENCH_TOKEN")
    assert _load(raw, tmp_path, {**ENV, "BENCH_TOKEN": "x"}).credentials.env == "BENCH_TOKEN"


def test_endpoint_env_unset(tmp_path: Path) -> None:
    _fails(_raw(), tmp_path, "PROVISA_HTTP_BASE_URL", env={})


@pytest.mark.parametrize(
    "dist,message",
    [
        ({"kind": "bimodal"}, "unknown distribution kind 'bimodal'"),
        ({"kind": "constant"}, "knobs.fields.distribution: constant requires 'value'"),
        ({"kind": "uniform", "min": 5, "max": 2}, "min must be <= max"),
        ({"kind": "uniform", "min": 1, "max": 2, "extra": 1}, "unknown key 'extra'"),
        ({"kind": "zipf", "exponent": 0, "max": 5}, "exponent must be > 0"),
        ({"kind": "geometric", "p": 1.5, "max": 5}, "p must be in (0, 1]"),
        ({"kind": "constant", "value": True}, "value must be an integer"),
        ({"kind": "constant", "value": -1}, "value must be >= 0"),
    ],
)
def test_malformed_distribution(tmp_path: Path, dist: dict, message: str) -> None:
    raw = _raw()
    raw["knobs"]["fields"]["distribution"] = dist
    _fails(raw, tmp_path, message)


def test_probability_range(tmp_path: Path) -> None:
    raw = _raw()
    raw["knobs"]["rows"]["probability"] = 1.2
    _fails(raw, tmp_path, "knobs.rows.probability: must be between 0 and 1")


def test_source_weights_must_sum_to_one(tmp_path: Path) -> None:
    raw = _raw()
    raw["knobs"]["source_weights"]["bench-clickhouse"] = 0.5
    _fails(raw, tmp_path, "knobs.source_weights: weights sum to 1.5, must sum to 1")


def test_source_weights_must_name_every_source(tmp_path: Path) -> None:
    raw = _raw()
    del raw["knobs"]["source_weights"]["bench-neo4j"]
    _fails(raw, tmp_path, "knobs.source_weights: missing source 'bench-neo4j'")


def test_role_weights_must_sum_to_one(tmp_path: Path) -> None:
    raw = _raw()
    raw["deployment"]["roles"][0]["weight"] = 0.4
    _fails(raw, tmp_path, "deployment.roles: weights sum to 0.4, must sum to 1")


def test_unknown_transport(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["carrier_pigeon"] = {}
    _fails(raw, tmp_path, "transports: unknown transport 'carrier_pigeon'")


def test_base_column_must_be_declared(tmp_path: Path) -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["tables"][0]["base_column"] = "nope"
    _fails(raw, tmp_path, "base_column 'nope' is not a declared column")


def test_join_must_name_declared_tables_and_columns(tmp_path: Path) -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["joins"] = [
        {"left": "orders.order_id", "right": "ghost.order_id"}
    ]
    _fails(raw, tmp_path, "joins[0].right: unknown table 'ghost'")


# ---------------------------------------------------------------- knobs not implemented yet


def test_knob_set_is_not_implemented(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # the refusal mechanism, independent of which knobs are built today
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["knobs"]["filters"] = {
        "probability": 0.3,
        "distribution": {"kind": "uniform", "min": 1, "max": 5},
    }
    _fails(raw, tmp_path, "not implemented yet: knobs.filters")


def test_transport_override_set_is_not_implemented(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["transports"]["pgwire"] = {
        "knobs": {"filters": {"probability": 1, "distribution": {"kind": "constant", "value": 2}}}
    }
    _fails(raw, tmp_path, "not implemented yet: transports.pgwire.knobs.filters")


def test_transport_override_unknown_knob(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["pgwire"] = {"knobs": {"ttl_seconds": {}}}
    _fails(raw, tmp_path, "transports.pgwire.knobs: unknown key 'ttl_seconds'")


def test_two_weighted_sources_not_implemented(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["knobs"]["source_weights"] = {
        "bench-postgresql": 0.5,
        "bench-clickhouse": 0.5,
        "bench-mongodb": 0.0,
        "bench-neo4j": 0.0,
    }
    _fails(raw, tmp_path, "not implemented yet: knobs.source_weights")


def test_non_auto_route_not_implemented(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["knobs"]["route"]["bench-postgresql"] = {"mode": "direct", "federated_probability": 0}
    _fails(raw, tmp_path, "not implemented yet: knobs.route.bench-postgresql")


def test_open_loop_not_implemented(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["load"] = {
        "mode": "open_loop",
        "rate_per_s": 100,
        "connections": 8,
        "window_s": 20,
        "processes": 2,
        "profile_requests": 0,
        "idle_window_s": 0,
    }
    _fails(raw, tmp_path, "not implemented yet: load.mode open_loop")


def test_role_mix_not_implemented(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["deployment"]["roles"] = [
        {"role": "org_admin", "weight": 0.5},
        {"role": "analyst", "weight": 0.5},
    ]
    _fails(raw, tmp_path, "not implemented yet: deployment.roles (role mix)")


def test_base_is_the_row_count_alone(tmp_path: Path) -> None:
    raw = _raw()
    raw["base"]["fields"] = 2
    _fails(raw, tmp_path, "base: unknown key 'fields'")


def test_base_rows_is_what_a_request_with_no_row_knob_asks_for(tmp_path: Path) -> None:
    raw = _raw()
    raw["base"]["rows"] = 5
    setup = _load(raw, tmp_path)
    q = request_render.build_query(setup, "graphql", cached=False)
    assert q.sql.endswith("LIMIT 5") and "limit: 5" in q.graphql


def test_base_rows_must_be_positive(tmp_path: Path) -> None:
    raw = _raw()
    raw["base"]["rows"] = 0
    _fails(raw, tmp_path, "base.rows: value must be >= 1")


# ---------------------------------------------------------------- names come from the deployment


def test_a_join_the_deployment_does_not_register_is_refused(tmp_path: Path) -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["joins"] = [
        {"left": "orders.order_id", "right": "orders.customer_id"}
    ]
    _fails(raw, tmp_path, "is not a registered relationship in the deployment")


def test_a_table_the_deployment_does_not_register_is_refused(tmp_path: Path) -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["tables"][0]["table"] = "ghost"
    raw["sources"]["bench-postgresql"]["joins"] = []
    raw["cross_source_joins"] = []
    _fails(raw, tmp_path, "bench-postgresql/public.ghost is not registered in the deployment")


def test_a_column_the_deployment_does_not_register_is_refused(tmp_path: Path) -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["tables"][0]["columns"].append({"name": "nope"})
    _fails(raw, tmp_path, "column 'nope' of bench-postgresql/public.orders is not registered")


def test_the_contract_carries_no_transport_names() -> None:
    raw = _raw()
    for src in raw["sources"].values():
        for table in src["tables"]:
            assert set(table) == {"schema", "table", "weight", "base_column", "columns"}
            assert all(set(c) <= {"name", "domain"} for c in table["columns"])
        assert all(set(j) == {"left", "right"} for j in src["joins"])
    assert all(set(j) == {"left", "right"} for j in raw["cross_source_joins"])


def test_without_a_deployment_the_setup_is_unbound() -> None:
    setup = sc.load_setup(PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS)
    table = sc.base_table_unbound(setup)
    assert table.sql is None and table.cypher_label is None and table.columns[0].names == {}


def test_the_deployment_fills_every_spelling() -> None:
    setup = sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )
    orders = setup.sources["bench-postgresql"].tables[0]
    assert (orders.sql, orders.graphql_field, orders.cypher_label, orders.cypher_var) == (
        "perf_bench.orders",
        "pb__orders",
        "PerfBench:Orders",
        "o",
    )
    assert orders.column("order_id").names["graphql"] == "orderId"
    (items,) = setup.sources["bench-postgresql"].joins
    assert (items.graphql_field, items.cypher_rel) == ("orderItems", "HAS_ITEM")
    assert [(j.right_source, j.graphql_field, j.cypher_rel) for j in setup.cross_source_joins] == [
        ("bench-clickhouse", "orderEvents", "HAS_EVENT"),
        ("bench-mongodb", "orderDoc", "HAS_DOC"),
    ]


def test_resolved_names_can_be_given_back(tmp_path: Path) -> None:
    import lookup

    unbound = sc.load_setup(PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS)
    resolved = lookup.resolve(*sc.identities(unbound), fx.build())
    path = tmp_path / "resolved-names.json"
    lookup.write_resolved(resolved, path)
    again = sc.load_setup(
        PERF_CONTRACT,
        environ=ENV,
        known_transports=TRANSPORTS,
        deployment=lookup.read_resolved(path),
    )
    assert again == sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=resolved
    )


def test_a_join_written_the_other_way_round_is_oriented_as_registered(tmp_path: Path) -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["joins"] = [
        {"left": "order_items.order_id", "right": "orders.order_id"}
    ]
    (j,) = _load(raw, tmp_path).sources["bench-postgresql"].joins
    assert (j.left_table, j.right_table, j.graphql_field, j.cypher_rel) == (
        "orders",
        "order_items",
        "orderItems",
        "HAS_ITEM",
    )
