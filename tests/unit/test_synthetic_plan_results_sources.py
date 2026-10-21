# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A synthetic environment generates the operator's data only (REQ-1942): the tables of a Data
Profiler or a data-quality checker hold Provisa's results about the environment's tables, so the
model's generation plan leaves them out, as it leaves out the built-in sources'. The profiling demo
registers its profiler's results as tables, and its synthetic environment was refused because
nothing had profiled them."""

# Requirements: REQ-1942

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from provisa.synthetic import env_model

pytestmark = pytest.mark.unit


def _table(table_id: int, source_id: str, name: str) -> dict:
    return {
        "id": table_id,
        "source_id": source_id,
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": "id", "data_type": "integer"}],
    }


def test_the_results_sources_are_the_profiler_and_every_checker():
    assert env_model.results_source_types() == {"data_profiler", "soda", "great_expectations"}


def test_the_plan_leaves_out_a_profiler_s_and_a_checker_s_tables(monkeypatch):
    from provisa.api.admin import db_queries
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.synthetic import run

    tables = [
        _table(1, "crm", "customers"),
        _table(2, "harbor-profiler", "customers_1_profile_runs"),
        _table(3, "quality", "customers_checks"),
    ]
    types = {"crm": "postgresql", "harbor-profiler": "data_profiler", "quality": "soda"}
    measured = {"runId": "r1", "runTime": datetime(2026, 10, 21, tzinfo=UTC)}
    planned_for: list = []

    async def fetch_tables(_conn):
        return tables

    async def source_types(_conn):
        return types

    async def profile_runs_in(_conn, env, offered):
        planned_for.append(sorted(t["id"] for t in offered))
        if env != "prod":
            return []
        return [{"tableId": 1, "runs": [{**measured, "origin": "measured", "env": "prod"}]}]

    async def nothing(*_args, **_kwargs):
        return []

    async def no_commands(_conn, _sources):
        return {}

    async def no_generation(_conn):
        return None

    monkeypatch.setattr(db_queries, "fetch_tables", fetch_tables)
    monkeypatch.setattr(db_queries, "fetch_relationships", nothing)
    monkeypatch.setattr(env_model, "_source_types", source_types)
    monkeypatch.setattr(run, "profile_runs_in", profile_runs_in)
    monkeypatch.setattr(run, "uncovered_columns", nothing)
    monkeypatch.setattr(env_model, "source_commands", no_commands)
    monkeypatch.setattr(env_model, "last_generation", no_generation)
    state = SimpleNamespace(
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(replica_store_backend=lambda: "duckdb")
        )
    )
    token = set_current_org("demo")
    try:
        plan = asyncio.run(env_model.model_plan(state, object(), "prod", "synthetic"))
    finally:
        reset_current_org(token)
    assert [t["tableId"] for t in plan["tables"]] == [1]
    assert [s["id"] for s in plan["sources"]] == ["crm"]
    assert planned_for == [[1], [1]]
    assert plan["ready"] is True
