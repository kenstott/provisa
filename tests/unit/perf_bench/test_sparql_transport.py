# Copyright (c) 2026 Kenneth Stott
# Canary: 9bc74e68-a0c1-48f2-8cca-ca3be893ae4c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1911, REQ-2135: the bench's SPARQL transport, its own dialect beside every other.

Its names come from the deployment's SPARQL vocabulary (a table's class, a column's predicate, an
edge type's predicate), its requests are rendered from the same spec every transport's are, and
it joins. It has no route hint and no per-request cache opt-in.

FIXTURE: the deployment's answers are recorded shapes (deployment_fixture.py); the requests
against a real server are test_bench_features_e2e's."""

from __future__ import annotations

import dataclasses
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import lookup  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_render  # noqa: E402
from test_joins_knob import _setup, _spec  # noqa: E402

B = fx.PUBLIC_URL
ORDERS = lookup.TableIdentity("bench-postgresql", "public", "orders", ("order_id", "region"))
ITEMS = lookup.TableIdentity("bench-postgresql", "public", "order_items", ("item_id", "order_id"))


def test_sparql_is_its_own_transport_and_joins() -> None:
    assert contract_model.TRANSPORT_LANGUAGE["sparql"] == "sparql"
    assert "sparql" in contract_model.JOIN_TRANSPORTS
    # no per-request cache opt-in exists on the endpoint
    assert ol.TRANSPORTS["sparql"][1] is None
    assert ol.CLIENTS["sparql"] is ol.SparqlHttpClient


def test_a_table_its_columns_and_a_join_are_named_by_the_vocabulary() -> None:
    r = lookup.resolve(
        [ORDERS, ITEMS], [lookup.JoinIdentity(ORDERS, "order_id", ITEMS, "order_id")], fx.build()
    )
    orders = r.tables[ORDERS.key]
    assert orders.sparql_class == f"{B}/vocab/perf-bench/orders"
    assert orders.columns["order_id"]["sparql"] == f"{B}/vocab/perf-bench/orders#order_id"
    (join,) = r.joins.values()
    assert join.sparql_rel == f"{B}/vocab/rel/HAS_ITEM"


def test_a_table_sparql_does_not_expose_has_no_sparql_names() -> None:
    names = lookup.resolve([ORDERS], [], fx.build(drop={"orders": {"sparql"}})).tables[ORDERS.key]
    assert names.sparql_class is None
    assert all("sparql" not in spelled for spelled in names.columns.values())
    assert names.cypher_label == "PerfBench:Orders"  # the others are untouched


def test_a_request_is_a_select_over_the_tables_class(tmp_path: Path) -> None:
    q = request_render.Renderer(_setup(tmp_path)).render(
        _spec("orders", (), columns=("order_id", "region"), filters=(("region", "us-east"),))
    )
    assert q.sparql == (
        f"SELECT ?order_id ?region WHERE {{ ?t0 a <{B}/vocab/perf-bench/orders> . "
        f"OPTIONAL {{ ?t0 <{B}/vocab/perf-bench/orders#order_id> ?order_id }} "
        f"OPTIONAL {{ ?t0 <{B}/vocab/perf-bench/orders#region> ?region }} "
        f'?t0 <{B}/vocab/perf-bench/orders#region> ?f0 . FILTER(?f0 = "us-east") }} LIMIT 1'
    )


def test_a_join_follows_the_edge_predicate(tmp_path: Path) -> None:
    step = contract_model.JoinStep(
        0,
        "same",
        "bench-postgresql",
        "order_items",
        "order_id",
        "order_id",
        True,
        "orderItems",
        "HAS_ITEM",
        sparql_rel=f"{B}/vocab/rel/HAS_ITEM",
    )
    q = request_render.Renderer(_setup(tmp_path)).render(_spec("orders", (step,)))
    assert q.sparql == (
        f"SELECT ?order_id ?order_items_item_id WHERE {{ ?t0 a <{B}/vocab/perf-bench/orders> . "
        f"OPTIONAL {{ ?t0 <{B}/vocab/perf-bench/orders#order_id> ?order_id }} "
        f"?t0 <{B}/vocab/rel/HAS_ITEM> ?t1 . ?t1 a <{B}/vocab/perf-bench/order_items> . "
        f"OPTIONAL {{ ?t1 <{B}/vocab/perf-bench/order_items#item_id> ?order_items_item_id }} "
        "} LIMIT 1"
    )


def test_a_route_hint_is_not_sent_over_sparql(tmp_path: Path) -> None:
    spec = _spec("orders", ())
    routed = dataclasses.replace(spec, route="direct")
    assert request_render.Renderer(_setup(tmp_path)).render(routed).sparql is None


def test_an_answer_is_read_by_its_datatypes() -> None:
    xsd = "http://www.w3.org/2001/XMLSchema#"
    assert ol._sparql_value({"value": "7", "datatype": f"{xsd}integer"}) == 7  # noqa: SLF001
    assert ol._sparql_value({"value": "1.50", "datatype": f"{xsd}decimal"}) == Decimal("1.50")  # noqa: SLF001
    assert ol._sparql_value({"value": "true", "datatype": f"{xsd}boolean"}) is True  # noqa: SLF001
    assert ol._sparql_value({"value": "us-east"}) == "us-east"  # noqa: SLF001
    assert ol._sparql_value(None) is None  # noqa: SLF001
