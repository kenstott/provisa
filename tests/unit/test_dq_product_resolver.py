# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a7c15-2d4b-4f8e-a6c1-9b0d5e7f2a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1443 clause 10: a checker table's product_id is the SCANNED table's, resolved at read time.

The admin GraphQL type answers product_id from the row for every ordinary table and, for a checker
table, from the table its contract dataset resolves to — never from its own row, which registration
keeps empty.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api.admin.types import RegisteredTableType
from tests.helpers import dq_contexts

CONTRACT = "dataset: provisa/sales/orders\nchecks:\n  - row_count:\n"


async def _resolve(table: RegisteredTableType) -> str | None:
    # strawberry types the field as its return value; call the resolver the way the schema does.
    return await cast(Any, table).product_id()


class _Rows:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return None if self._row is None else SimpleNamespace(_mapping=self._row)


class _Conn:
    def __init__(self, row):
        self.row = row
        self.statements: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute_core(self, stmt):
        self.statements.append(stmt)
        return _Rows(self.row)


def _table(**overrides: Any) -> RegisteredTableType:
    fields: dict[str, Any] = dict(
        id=2,
        source_id="dq",
        domain_id="sales",
        schema_name="quality",
        table_name="orders_scans",
        alias=None,
        description=None,
        cache_ttl=None,
        prefer_materialized=None,
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        gql_naming_convention=None,
        watermark_column=None,
        columns=[],
    )
    fields.update(overrides)
    return RegisteredTableType(**fields)


@pytest.fixture
def _state(monkeypatch):
    conn = _Conn({"product_id": "orders-product"})
    st = SimpleNamespace(
        contexts=dq_contexts(
            (1, "wh", "sales", "sales", "orders"), (2, "dq", "sales", "quality", "orders_scans")
        ),
        source_types={"dq": "soda", "wh": "postgresql"},
        tenant_db=SimpleNamespace(acquire=lambda: conn),
    )
    monkeypatch.setattr("provisa.api.app.state", st, raising=False)
    return conn


@pytest.mark.asyncio
async def test_an_ordinary_table_answers_with_its_own_row(_state):
    table = _table(
        id=1, source_id="wh", schema_name="sales", table_name="orders", stored_product_id="p1"
    )
    assert await _resolve(table) == "p1"
    assert _state.statements == []  # nothing to resolve — the row is the answer


@pytest.mark.asyncio
async def test_a_checker_table_inherits_the_scanned_tables_product(_state):
    table = _table(dq_contract=CONTRACT, stored_product_id=None)
    assert await _resolve(table) == "orders-product"
    assert len(_state.statements) == 1  # one lookup: the target's row


@pytest.mark.asyncio
async def test_a_checker_table_never_answers_from_its_own_row(_state):
    # Registration refuses a stored product_id on a checker table; even if one were present the
    # derivation wins, so the two surfaces cannot disagree.
    _state.row = {"product_id": None}
    table = _table(dq_contract=CONTRACT, stored_product_id="stale")
    assert await _resolve(table) is None


@pytest.mark.asyncio
async def test_a_target_that_is_not_registered_fails_loud(_state):
    _state.row = None
    table = _table(dq_contract=CONTRACT)
    with pytest.raises(ValueError, match="is not registered"):
        await _resolve(table)
