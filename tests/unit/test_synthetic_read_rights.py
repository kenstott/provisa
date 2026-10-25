# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Synthetic datasets answer within the caller's grants (REQ-2152).

The right that opens the synthetic-data surfaces does not widen what its holder reads: a dataset
is defined over tables it is served, its relationships are those whose joined columns it is
served, and a dataset's report names only columns it is served.

The model: ``orders`` (7, served) refers to ``customers`` (8, served) and to ``payroll`` (9, not
served).
"""

# Requirements: REQ-2152

from __future__ import annotations

import types
from typing import Any, cast

import pytest

import provisa.api.app as appmod
from provisa.api.admin import synthetic_router
from provisa.api.errors import ApiError

pytestmark = pytest.mark.asyncio

# The served tables and columns are stubbed, so the request carries nothing.
_REQUEST = cast(Any, None)


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[Any]:
        return self._rows


class _Conn:
    """Answers each statement with the next queued result."""

    def __init__(self, results: list[list[Any]]) -> None:
        self._results = list(results)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def execute_core(self, stmt):
        return _Result(self._results.pop(0))


def _db(monkeypatch, results: list[list[Any]]) -> None:
    conn = _Conn(results)
    monkeypatch.setattr(
        appmod.state, "model_db", types.SimpleNamespace(acquire=lambda: conn), raising=False
    )


@pytest.fixture(autouse=True)
def _rights(monkeypatch):
    async def _served(conn, request, table_id):
        return {7: {"id", "customer_id"}, 8: {"id"}}.get(table_id, set())

    async def _readable(conn, reader, rel_id):
        return rel_id == "orders-customers"

    async def _served_of(conn, scope, table_id):
        return await _served(conn, None, table_id)

    monkeypatch.setattr(synthetic_router, "require_capability_request", lambda request, cap: None)
    monkeypatch.setattr("provisa.api.admin.read_rights.served_columns_request", _served)
    monkeypatch.setattr("provisa.api.admin.read_rights.served_columns_of", _served_of)
    monkeypatch.setattr(
        "provisa.api.admin.catalog_scope.catalog_scope",
        lambda info: types.SimpleNamespace(whole=False),
    )
    monkeypatch.setattr("provisa.api.admin.read_rights.relationship_readable", _readable)
    monkeypatch.setattr("provisa.api.admin.read_rights.reader_for_request", lambda request: None)


async def test_a_dataset_over_a_table_the_caller_is_not_served_is_refused(monkeypatch):
    _db(monkeypatch, [])
    monkeypatch.setattr("provisa.synthetic.run.store_schema", lambda name: f"syn_{name}")
    body = synthetic_router.DatasetIn(
        seed=1,
        scale=1.0,
        tables=[
            synthetic_router.TableIn(tableId=7, profileEnv="prod", runId="r"),
            synthetic_router.TableIn(tableId=9, profileEnv="prod", runId="r"),
        ],
    )
    with pytest.raises(ApiError) as refused:
        await synthetic_router.define_dataset(_REQUEST, "demo", body)
    assert refused.value.code == "synthetic.table_not_found"


async def test_only_the_relationships_whose_columns_the_caller_is_served_are_offered(
    monkeypatch,
):
    _db(monkeypatch, [])

    async def _tables(conn):
        return [
            {"id": 7, "table_name": "orders"},
            {"id": 8, "table_name": "customers"},
            {"id": 9, "table_name": "payroll"},
        ]

    async def _relationships(conn):
        def rel(rel_id, target):
            return {
                "id": rel_id,
                "source_table_id": 7,
                "source_column": "customer_id",
                "target_table_id": target,
                "target_column": "id",
                "via_table_id": None,
                "cardinality": "many-to-one",
            }

        return [rel("orders-customers", 8), rel("orders-payroll", 9)]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _tables)
    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_relationships", _relationships)
    offered = await synthetic_router.relationships(_REQUEST)
    assert [r["id"] for r in offered] == ["orders-customers"]


async def test_a_report_names_only_the_columns_the_caller_is_served(monkeypatch):
    def row(table, column):
        return types.SimpleNamespace(
            _mapping={
                "table_name": table,
                "column_name": column,
                "measure": "mean",
                "source_value": 1.0,
                "synthetic_value": 1.0,
                "delta": 0.0,
                "note": None,
            }
        )

    report = [
        row("orders", "id"),
        row("orders", "secret"),
        row("orders", None),
        row("payroll", "salary"),
    ]
    _db(monkeypatch, [[(None,)], report, [(7, "orders"), (9, "payroll")]])
    monkeypatch.setattr(
        "provisa.api.admin.profiler_checks_router._physical_names",
        lambda tid: {7: {"id": "id", "secret": "secret"}, 9: {"salary": "salary"}}[tid],
    )
    out = await synthetic_router.dataset_report(_REQUEST, "demo")
    assert [(r["table"], r["column"]) for r in out] == [("orders", "id"), ("orders", None)]
