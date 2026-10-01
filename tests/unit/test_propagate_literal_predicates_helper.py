# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8d2f4a-1c7e-4a39-9d6b-0e3f8a2c7b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pipeline's one REQ-1880 helper: eligibility from the registry + connector capability, and
bind parameters carried only on engines whose runtime binds them by number."""

# Requirements: REQ-1880

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.pgwire._pipeline import propagate_literal_predicates

_SQL = (
    'SELECT "t0"."order_id", (SELECT max("t2"."status") FROM "perf_bench"."order_docs" AS "t2" '
    'WHERE "t2"."order_id" = "t0"."order_id") AS "d" FROM "perf_bench"."orders" AS "t0" '
    'WHERE "t0"."order_id" >= $1 AND "t0"."order_id" <= $2'
)


def _state(engine_name: str):
    caps = {
        "mongodb": SimpleNamespace(predicate_pushdown=True, join_pushdown=False),
        "postgresql": SimpleNamespace(predicate_pushdown=True, join_pushdown=True),
    }
    return SimpleNamespace(
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(name=engine_name),
            connector_pushdown=lambda source_type: caps[source_type],
        )
    )


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    from provisa.core.models import Column, Table

    def col(name, t):
        return Column(name=name, visible_to=["org_admin"], data_type=t)

    tables = [
        Table(
            source_id="pg",
            domain_id="d",
            schema_name="perf_bench",
            table_name="orders",
            columns=[col("order_id", "integer")],
        ),
        Table(
            source_id="mongo",
            domain_id="d",
            schema_name="perf_bench",
            table_name="order_docs",
            columns=[col("order_id", "integer"), col("status", "varchar")],
        ),
    ]
    sources = [
        SimpleNamespace(id="pg", type=SimpleNamespace(value="postgresql")),
        SimpleNamespace(id="mongo", type=SimpleNamespace(value="mongodb")),
    ]

    async def _t(state):
        return tables

    async def _s(state):
        return sources

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _t)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _s)


@pytest.mark.parametrize("engine_name", ["duckdb", "postgres"])
def test_a_numbered_param_bound_propagates_on_a_by_number_engine(engine_name):
    out = asyncio.run(propagate_literal_predicates(_SQL, _state(engine_name)))
    assert '"t2"."order_id" >= $1' in out and '"t2"."order_id" <= $2' in out


@pytest.mark.parametrize("engine_name", ["trino", "sqlalchemy", "clickhouse", "snowflake"])
def test_a_param_bound_is_never_copied_on_a_positional_binder(engine_name):
    assert asyncio.run(propagate_literal_predicates(_SQL, _state(engine_name))) == _SQL


def test_a_query_with_no_join_or_subquery_is_returned_untouched():
    sql = 'SELECT "t0"."order_id" FROM "perf_bench"."orders" AS "t0" WHERE "t0"."order_id" = 1'
    assert asyncio.run(propagate_literal_predicates(sql, _state("duckdb"))) == sql
