# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6b3e9a-2d7f-4a1e-8b5c-9e2d4f7a0c36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1679: a function's or webhook's response is governed like a table's rows."""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
import sqlglot

from provisa.api.data.action_governance import (
    contract_columns,
    govern_action_rows,
    synthetic_table_id,
)
from provisa.api.errors import ApiError
from provisa.compiler.rls import RLSContext
from provisa.security.inheritance import materialize_rls, role_chains


class _Engine:
    """The federation engine as the governance step sees it: DuckDB in process."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    def transpile_physical(self, pg_sql: str) -> str:
        return sqlglot.transpile(pg_sql, read="postgres", write="duckdb")[0]

    async def execute_engine(self, sql: str, params=None, **_):
        self.sql.append(sql)
        cur = duckdb.connect().execute(sql)
        return SimpleNamespace(column_names=[d[0] for d in cur.description], rows=cur.fetchall())


CONTRACT = [
    {"name": "id", "type": "integer"},
    {"name": "region", "type": "varchar"},
    {
        "name": "ssn",
        "type": "varchar",
        "mask_type": "constant",
        "mask_value": "***",
        "unmasked_to": ["org_admin"],
    },
    {"name": "amount", "type": "double", "visible_to": ["org_admin"]},
]
ROWS = [
    {"id": 1, "region": "east", "ssn": "111", "amount": 10.0},
    {"id": 2, "region": "west", "ssn": "222", "amount": 20.0},
    {"id": 3, "region": "east", "ssn": "333", "amount": 30.0},
]
FN = {"name": "customer_lookup", "domain_id": "sales", "output_columns": CONTRACT}


def _state(rls_by_role: dict[str, RLSContext] | None = None, roles: dict | None = None):
    roles = roles or {
        "analyst": {
            "id": "analyst",
            "capabilities": ["full_results"],
            "session_vars": {"region": "east"},
        },
        "org_admin": {"id": "org_admin", "capabilities": ["admin", "full_results"]},
        "junior": {
            "id": "junior",
            "capabilities": ["full_results"],
            "session_vars": {"region": "east"},
        },
    }
    return SimpleNamespace(
        roles=roles,
        role_chains={
            "analyst": ["analyst"],
            "org_admin": ["org_admin"],
            "junior": ["junior", "analyst"],
        },
        rls_contexts=rls_by_role or {},
        federation_engine=_Engine(),
    )


class TestContract:
    def test_output_columns_win(self):
        assert contract_columns(FN)[0]["name"] == "id"

    def test_return_schema_array_of_objects_is_a_contract(self):
        fn = {
            "name": "f",
            "return_schema": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}, "ok": {"type": "boolean"}},
                },
            },
        }
        assert contract_columns(fn) == [
            {"name": "id", "type": "integer"},
            {"name": "ok", "type": "boolean"},
        ]

    def test_json_string_column_is_read(self):
        assert contract_columns(
            {"name": "f", "output_columns": '[{"name":"x","type":"varchar"}]'}
        ) == [{"name": "x", "type": "varchar"}]

    def test_scalar_return_has_no_contract(self):
        assert contract_columns({"name": "f", "returns": "String"}) is None

    def test_synthetic_id_is_negative_and_stable(self):
        assert synthetic_table_id("a") < 0
        assert synthetic_table_id("a") == synthetic_table_id("a")
        assert synthetic_table_id("a") != synthetic_table_id("b")


@pytest.mark.asyncio
class TestGovern:
    async def test_no_contract_passes_through(self):
        rows, enf = await govern_action_rows(
            [{"n": 1}], {"name": "f", "returns": "Int"}, "analyst", _state()
        )
        assert rows == [{"n": 1}] and enf is None

    async def test_off_contract_row_refused(self):
        with pytest.raises(ApiError) as exc:
            await govern_action_rows([{"id": 1, "extra": 2}], FN, "analyst", _state())
        assert exc.value.status_code == 502

    async def test_rls_mask_visibility_for_analyst(self):
        st = _state(
            {
                "analyst": RLSContext(
                    rules={},
                    domain_rules={},
                    action_rules={"customer_lookup": "region = current_setting('provisa.region')"},
                )
            }
        )
        rows, enf = await govern_action_rows(ROWS, FN, "analyst", st)
        assert [r["id"] for r in rows] == [1, 3]
        assert all(r["ssn"] == "***" for r in rows)
        assert "amount" not in rows[0]
        assert enf.rls_filters_applied == ["region = current_setting('provisa.region')"]
        assert enf.columns_excluded == ["amount"]
        assert enf.masking_applied == ["ssn -> constant"]

    async def test_domain_rule_covers_the_action(self):
        st = _state({"analyst": RLSContext(rules={}, domain_rules={"sales": "id > 2"})})
        rows, _ = await govern_action_rows(ROWS, FN, "analyst", st)
        assert [r["id"] for r in rows] == [3]

    async def test_admin_sees_everything_unmasked(self):
        rows, enf = await govern_action_rows(ROWS, FN, "org_admin", _state())
        assert len(rows) == 3 and rows[0]["ssn"] == "111" and rows[0]["amount"] == 10.0
        assert enf.masking_applied == [] and enf.columns_excluded == []

    async def test_missing_session_var_denies_all(self):
        st = _state(
            {
                "analyst": RLSContext(
                    rules={},
                    domain_rules={},
                    action_rules={"customer_lookup": "region = current_setting('provisa.tenant')"},
                )
            }
        )
        rows, _ = await govern_action_rows(ROWS, FN, "analyst", st)
        assert rows == []

    async def test_inherited_action_rule_reaches_child(self):
        rules = [
            {
                "id": 1,
                "role_id": "analyst",
                "table_id": None,
                "domain_id": None,
                "action_name": "customer_lookup",
                "filter_expr": "region = 'west'",
            }
        ]
        chains = role_chains([{"id": "analyst"}, {"id": "junior", "parent_role_id": "analyst"}])
        out = materialize_rls(rules, [], chains, [FN])
        from provisa.compiler.rls import build_rls_context

        junior = build_rls_context(out, "junior")
        assert junior.action_rules == {"customer_lookup": "region = 'west'"}
        st = _state({"junior": junior})
        rows, _ = await govern_action_rows(ROWS, FN, "junior", st)
        assert [r["id"] for r in rows] == [2]
