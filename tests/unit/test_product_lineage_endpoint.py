# Copyright (c) 2026 Kenneth Stott
# Canary: 0b9e4d71-c2a6-4f38-8d15-6e7a3c9f2b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2156, REQ-1640, REQ-2126: a data product's lineage, read by any reader of the product.

GET /data-products/{product_id}/lineage answers the product's members, their lineage and its
nearest physical inputs, cut to what the caller reads: a hidden field leaves nothing behind, a
source field shows only behind a column the caller reads, and no governance right is asked.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import app as app_module
from provisa.api.admin import lineage_router
from provisa.api.admin.lineage_router import product_lineage
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.lineage.native import native_address, native_table, served

# orders (1, base) → v_orders (2, B) → mart (3, the product's member); mart_dq checks mart.
TABLES = [
    {"id": 1, "table_name": "orders", "source_id": "crm", "product_id": None, "dq_contract": None},
    {
        "id": 2,
        "table_name": "v_orders",
        "source_id": DERIVED_SOURCE_ID,
        "product_id": None,
        "dq_contract": None,
    },
    {
        "id": 3,
        "table_name": "mart",
        "source_id": DERIVED_SOURCE_ID,
        "product_id": "p1",
        "dq_contract": None,
    },
    {
        "id": 6,
        "table_name": "mart_dq",
        "source_id": "soda",
        "product_id": "p1",
        "dq_contract": "checks: []",
    },
]
for _t in TABLES:
    _t.update(domain_id="sales", alias=None, materialize=False)

VIEWS = [
    {
        "domain_id": "sales",
        "table_name": "v_orders",
        "view_sql": "SELECT amount, cost FROM sales.orders",
    },
    {
        "domain_id": "sales",
        "table_name": "mart",
        "view_sql": "SELECT amount, cost AS margin FROM sales.v_orders",
    },
]


def _column(table_id: int, table: str, column: str) -> dict:
    return {
        "table_id": table_id,
        "domain_id": "sales",
        "table_name": table,
        "column_name": column,
        "alias": None,
    }


COLUMNS = [
    _column(1, "orders", "amount"),
    _column(1, "orders", "cost"),
    _column(2, "v_orders", "amount"),
    _column(2, "v_orders", "cost"),
    _column(3, "mart", "amount"),
    _column(3, "mart", "margin"),
    _column(6, "mart_dq", "ok"),
]

ORDERS = {
    "schema_name": "public",
    "table_name": "orders",
    "columns": [
        {"column_name": "amount", "alias": None, "path": None, "native_filter_type": None},
        {"column_name": "cost", "alias": None, "path": None, "native_filter_type": None},
    ],
}


def _native():
    source = SimpleNamespace(id="crm", type="postgresql", path=None, federation_hints={})
    return native_table(
        "sales.orders",
        source,
        ORDERS,
        native_address("postgresql", ORDERS, source),
        served(False, None, None, None),
    )


def _context(served_cols: dict[int, list[str]]) -> SimpleNamespace:
    return SimpleNamespace(
        tables={tid: SimpleNamespace(table_id=tid) for tid in served_cols},
        physical_to_sql={(tid, c): c for tid, cols in served_cols.items() for c in cols},
    )


@pytest.fixture
def wired(monkeypatch):
    """analyst reads orders.amount and mart.amount: not v_orders (B), not orders.cost, not
    mart.margin. owner reads it all."""
    contexts = {
        "analyst": _context({1: ["amount"], 3: ["amount"], 6: ["ok"], 7: ["ok"]}),
        "owner": _context(
            {
                1: ["amount", "cost"],
                2: ["amount", "cost"],
                3: ["amount", "margin"],
                6: ["ok"],
                7: ["ok"],
            }
        ),
    }

    async def tables(state):
        return TABLES

    async def exists(state, product_id):
        return product_id == "p1"

    async def view_rows(state):
        return VIEWS

    async def columns(state):
        return COLUMNS

    async def natives(state):
        return [_native()]

    async def checkers(state):
        return CHECKERS

    monkeypatch.setattr(lineage_router, "_fetch_tables", tables)
    monkeypatch.setattr(lineage_router, "_product_exists", exists)
    monkeypatch.setattr(lineage_router, "_fetch_view_rows", view_rows)
    monkeypatch.setattr(lineage_router, "_fetch_registry_columns", columns)
    monkeypatch.setattr(lineage_router, "_native_tables", natives)
    monkeypatch.setattr(lineage_router, "_fetch_checkers", checkers)
    monkeypatch.setattr(app_module.state, "contexts", contexts, raising=False)


# mart_dq (6, p1's) checks mart.amount, a member: p1's promise. orders_dq (7, nobody's) checks
# orders.cost, an input: related, but only to a reader of orders.cost.
CHECKERS = [
    {
        "id": 6,
        "domain_id": "sales",
        "table_name": "mart_dq",
        "alias": None,
        "description": "Amounts are present",
        "product_id": "p1",
        "checked_table_id": 3,
        "checked_table": {"id": 3, "domain_id": "sales", "table_name": "mart", "alias": None},
        "rule_columns": ["amount"],
        "latest_outcome": "pass",
        "latest_run_at": None,
    },
    {
        "id": 7,
        "domain_id": "sales",
        "table_name": "orders_dq",
        "alias": None,
        "description": "Costs are positive",
        "product_id": None,
        "checked_table_id": 1,
        "checked_table": {"id": 1, "domain_id": "sales", "table_name": "orders", "alias": None},
        "rule_columns": ["cost"],
        "latest_outcome": "fail",
        "latest_run_at": None,
    },
]


def _as(role: str, identity: Any = None) -> Any:
    return SimpleNamespace(state=SimpleNamespace(identity=identity, role=role))


def _lineage(role: str) -> dict:
    return asyncio.run(product_lineage(_as(role), "p1"))


def test_a_caller_missing_b_reads_a_to_c(wired) -> None:
    out = _lineage("analyst")
    assert "v_orders" not in json.dumps(out)
    edges = {(e["source"], e["target"]): e for e in out["edges"]}
    assert edges[("sales.orders.amount", "sales.mart.amount")]["transform"] == ""


def test_a_member_column_the_caller_cannot_read_is_absent(wired) -> None:
    out = _lineage("analyst")
    assert "margin" not in json.dumps(out)
    assert out["outputs"] == ["sales.mart.amount"]
    owner = _lineage("owner")
    assert "sales.mart.margin" in owner["outputs"]


def test_a_source_field_shows_only_behind_a_column_the_caller_reads(wired) -> None:
    nodes = {n["id"]: n for n in _lineage("analyst")["nodes"]}
    native = nodes["native:sales.orders.amount"]
    assert (native["source_id"], native["source_schema"], native["source_table"]) == (
        "crm",
        "public",
        "orders",
    )
    assert "native:sales.orders.cost" not in nodes
    assert "native:sales.orders.cost" in {n["id"] for n in _lineage("owner")["nodes"]}


def test_a_checker_is_no_part_of_the_products_lineage(wired) -> None:
    out = _lineage("owner")
    # Its checks are the product's data quality (out["dq"]), never a node of its lineage.
    assert "mart_dq" not in json.dumps({k: out[k] for k in ("nodes", "edges", "inputs")})
    assert out["members"] == ["sales.mart"]


def test_the_inputs_are_the_nearest_physical_datasets_of_the_cut_graph(wired) -> None:
    (only,) = _lineage("analyst")["inputs"]
    assert (only["relation"], only["kind"], only["table_id"], only["product_id"]) == (
        "sales.orders",
        "source_table",
        1,
        None,
    )


def test_an_input_the_caller_cannot_read_is_no_input(wired, monkeypatch) -> None:
    monkeypatch.setitem(app_module.state.contexts, "analyst", _context({3: ["amount"]}))
    out = _lineage("analyst")
    assert out["inputs"] == []
    assert "orders" not in json.dumps(out)


def test_reading_it_asks_the_products_read_right_and_no_governance_one(wired, monkeypatch) -> None:
    from provisa.api.errors import ApiError

    reader = SimpleNamespace(user_id="u1", roles=["analyst"], acting_roles=["analyst"])
    monkeypatch.setattr(
        app_module.state,
        "roles",
        {"analyst": {"capabilities": ["data_product_read"], "domain_access": ["*"]}},
        raising=False,
    )
    out = asyncio.run(product_lineage(_as("analyst", reader), "p1"))
    assert "sales.mart.amount" in out["outputs"]
    monkeypatch.setattr(
        app_module.state,
        "roles",
        {"analyst": {"capabilities": ["query_development"], "domain_access": ["*"]}},
        raising=False,
    )
    with pytest.raises(ApiError) as refused:
        asyncio.run(product_lineage(_as("analyst", reader), "p1"))
    assert refused.value.status_code == 403


def test_an_unknown_product_is_not_found(wired) -> None:
    from provisa.api.errors import ApiError

    with pytest.raises(ApiError) as missing:
        asyncio.run(product_lineage(_as("analyst"), "nope"))
    assert missing.value.status_code == 404


def test_an_input_that_is_another_products_member_names_that_product(wired, monkeypatch) -> None:
    tables = [{**t, "product_id": "p0"} if t["id"] == 1 else t for t in TABLES]

    async def other(state):
        return tables

    monkeypatch.setattr(lineage_router, "_fetch_tables", other)
    (only,) = _lineage("analyst")["inputs"]
    assert (only["relation"], only["product_id"]) == ("sales.orders", "p0")


def _dq(role: str) -> dict[str, list[int]]:
    return {k: [e["checker_table_id"] for e in v] for k, v in _lineage(role)["dq"].items()}


def test_the_products_data_quality_is_classified_on_its_readers_lineage(wired) -> None:
    assert _dq("owner") == {"expectations": [], "promises": [6], "related": [7], "unattached": []}


def test_a_check_on_a_column_the_reader_cannot_read_is_in_no_list(wired) -> None:
    # The analyst reads orders.amount, not orders.cost: orders_dq is not theirs to see.
    assert _dq("analyst") == {"expectations": [], "promises": [6], "related": [], "unattached": []}


def test_a_role_without_a_request_reads_the_same_answer(wired) -> None:
    from provisa.api.admin.lineage_router import product_lineage_for

    out = asyncio.run(product_lineage_for(app_module.state, "p1", "analyst"))
    assert out is not None
    assert out["dq"] == _lineage("analyst")["dq"]
    assert out["natives"] == {}
    assert asyncio.run(product_lineage_for(app_module.state, "nope", "analyst")) is None


def test_a_consumer_one_hop_downstream_is_listed_cut_to_the_reader(wired, monkeypatch) -> None:
    """REQ-2160: the owner sees who reads the product, as far as they may read it."""
    tables = [
        *TABLES,
        {
            "id": 8,
            "table_name": "report",
            "source_id": DERIVED_SOURCE_ID,
            "product_id": "p2",
            "dq_contract": None,
            "domain_id": "sales",
            "alias": None,
            "materialize": True,
        },
    ]
    views = [
        *VIEWS,
        {"domain_id": "sales", "table_name": "report", "view_sql": "SELECT amount FROM sales.mart"},
    ]
    columns = [*COLUMNS, _column(8, "report", "amount")]

    async def _tables(state):
        return tables

    async def _views(state):
        return views

    async def _columns(state):
        return columns

    monkeypatch.setattr(lineage_router, "_fetch_tables", _tables)
    monkeypatch.setattr(lineage_router, "_fetch_view_rows", _views)
    monkeypatch.setattr(lineage_router, "_fetch_registry_columns", _columns)
    monkeypatch.setitem(
        app_module.state.contexts, "owner", _context({1: ["amount"], 3: ["amount"], 8: ["amount"]})
    )
    (only,) = _lineage("owner")["consumers"]
    assert (only["relation"], only["kind"], only["table_id"], only["product_id"]) == (
        "sales.report",
        "materialized_view",
        8,
        "p2",
    )
    # The analyst reads no column of report: no consumer.
    assert _lineage("analyst")["consumers"] == []
