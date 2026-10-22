# Copyright (c) 2026 Kenneth Stott
# Canary: 44aed8a8-15ba-49d2-9ad1-27aef011b760
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship feeds a code table's parameters from its source table (REQ-2099).

A statement that joins a code table to the source of a relationship whose ``parameters`` map its
parameters to the source's columns reads that source through the one pipeline, as the statement's
role, and calls the code once per distinct set of values: the role's row rules choose which rows
are scored and its masks shape the values the code is given. A read of the code table without the
source is refused for its parameters, as before."""

# Requirements: REQ-2099
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from provisa.federation.replica_address import ReplicaRoutes

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.executor.result import QueryResult
from provisa.mongodb.pipeline_table import PipelineRead
from provisa.security.masking import MaskingRule, MaskType
from tests.helpers import governed_for_role

pytestmark = pytest.mark.unit

CUSTOMERS, RISK = 7, 8
_CUSTOMERS = [
    (1, "ann@example.com", "EU", "pro", 12),
    (2, "bob@example.com", "NA", "basic", 3),
    (3, "cy@example.com", "EU", "pro", 12),  # the same features as ann's, but another customer
    (4, "di@example.com", "EU", "team", 40),
]


def _param(name: str, data_type: str, required: bool) -> dict:
    return {
        "column_name": f"_nf_{name}",
        "data_type": data_type,
        "native_filter_type": "query_arg",
        "native_filter_required": required,
        "required_by_steward": False,
        "path": None,
    }


def _output(name: str, data_type: str) -> dict:
    return {"column_name": name, "data_type": data_type, "native_filter_type": None, "path": None}


def _risk_table() -> dict:
    return {
        "id": RISK,
        "source_id": "model",
        "schema_name": "model",
        "table_name": "churn_risk",
        "cache_ttl": 0,
        "columns": [
            _param("customer_id", "integer", True),
            _param("email", "varchar", True),
            _param("plan", "varchar", True),
            _param("tenure_months", "integer", True),
            _param("as_of", "date", False),
            _output("customer_id", "integer"),
            _output("score", "double"),
        ],
    }


def _meta(table_id: int, name: str, source_id: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        domain_id="public",
        table_name=name,
    )


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "customers": _meta(CUSTOMERS, "customers", "pg"),
        "churn_risk": _meta(RISK, "churn_risk", "model"),
    }
    ctx.joins = {}
    return ctx


def _state() -> SimpleNamespace:
    engine = MagicMock()
    engine.land_source_table = AsyncMock()
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"model": "python", "pg": "postgresql"},
        replica_routes=ReplicaRoutes(),
        tables=[_risk_table()],
        relationships=[
            {
                "id": "customer-risk",
                "source_table_id": CUSTOMERS,
                "target_table_id": RISK,
                "parameters": {
                    "customer_id": "id",
                    "email": "email",
                    "plan": "plan",
                    "tenure_months": "tenure_months",
                },
            }
        ],
        federation_engine=engine,
        source_cache={},
        response_cache_default_ttl=300,
    )


_JOINED = (
    'SELECT "c"."id", "r"."score" FROM "public"."customers" AS "c" '
    'JOIN "public"."churn_risk" AS "r" ON "r"."customer_id" = "c"."id"'
)


async def _scored(
    sql: str, table_ids: list[int], *, rls=None, masking_rules=None, nf_args=None
) -> tuple[list, dict, list]:
    """Run the materialize stage on ``sql`` as the analyst, the feed's read governed as the
    pipeline governs it and answered from the customers above. Returns the argument sets the code
    was called with, what the stage put in for the engine, and the governed feed statements."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    ctx = _ctx()
    governed: list[str] = []
    called: list = []

    async def govern(statement, role_id, *, session_vars=None):
        assert role_id == "analyst"
        governed.append(
            governed_for_role(statement, ctx, role_id, rls=rls, masking_rules=masking_rules)
        )
        return governed[-1]

    async def execute(plan):
        db = duckdb.connect()
        db.execute(
            "CREATE TABLE customers (id INTEGER, email VARCHAR, region VARCHAR, plan VARCHAR, "
            "tenure_months INTEGER)"
        )
        db.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?)", _CUSTOMERS)
        cursor = db.execute(plan.replace('"public".', ""))
        return QueryResult(
            rows=sorted(cursor.fetchall()), column_names=[d[0] for d in cursor.description]
        )

    async def read_each(state, table, argument_sets):
        called.extend(argument_sets)
        return PipelineRead(rows=[], columns=[("customer_id", "bigint"), ("score", "double")])

    with (
        patch("provisa.pgwire._pipeline._govern_and_route", side_effect=govern),
        patch("provisa.pgwire._pipeline._execute_plan", side_effect=execute),
        patch("provisa.codesource.table.read_each", side_effect=read_each),
        patch("provisa.api_source.engine_cache.cache_location"),
    ):
        rewrites, inlined, _ = await _materialize_api_to_engine_cache(
            sql,
            _state(),
            nf_args=nf_args,
            table_ids=table_ids,
            ctx=ctx,
            role_id="analyst",
        )
    return called, {**rewrites, **inlined}, governed


async def test_each_customer_is_scored_with_its_own_features():
    called, put_in, governed = await _scored(_JOINED, [CUSTOMERS, RISK])
    assert called == [
        {"customer_id": 1, "email": "ann@example.com", "plan": "pro", "tenure_months": 12},
        {"customer_id": 2, "email": "bob@example.com", "plan": "basic", "tenure_months": 3},
        {"customer_id": 3, "email": "cy@example.com", "plan": "pro", "tenure_months": 12},
        {"customer_id": 4, "email": "di@example.com", "plan": "team", "tenure_months": 40},
    ]
    # One read of the source's columns, named for the parameters they feed.
    assert governed == [
        'SELECT DISTINCT "id" AS "customer_id", "email" AS "email", "plan" AS "plan", '
        '"tenure_months" AS "tenure_months" FROM "public"."customers"'
    ]
    assert set(put_in) == {"churn_risk"}


async def test_the_statements_own_arguments_reach_every_call():
    called, _, _ = await _scored(_JOINED, [CUSTOMERS, RISK], nf_args={"as_of": "2026-10-01"})
    assert [c["as_of"] for c in called] == ["2026-10-01"] * 4


async def test_a_row_rule_limits_which_customers_are_scored():
    called, _, _ = await _scored(
        _JOINED, [CUSTOMERS, RISK], rls=RLSContext(rules={CUSTOMERS: "region = 'EU'"})
    )
    assert [c["customer_id"] for c in called] == [1, 3, 4]


async def test_a_masked_value_reaches_the_code_masked():
    mask = MaskingRule(mask_type=MaskType.regex, pattern="^[^@]+", replace="***")
    called, _, _ = await _scored(
        _JOINED,
        [CUSTOMERS, RISK],
        masking_rules={(CUSTOMERS, "analyst"): {"email": (mask, "varchar")}},
    )
    assert [c["email"] for c in called] == ["***@example.com"] * 4


async def test_a_read_without_the_source_is_refused_before_the_code_runs():
    with pytest.raises(ValueError, match=r"requires the parameter\(s\)"):
        await _scored('SELECT "score" FROM "public"."churn_risk"', [RISK])
