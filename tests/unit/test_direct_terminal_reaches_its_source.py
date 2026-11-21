# Copyright (c) 2026 Kenneth Stott
# Canary: a75a79d8-c2c8-47d5-bbf8-bfe8ddd3e245
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DIRECT terminal runs a statement on its own source's connection, or not at all.

The org's model store serves the ``provisa-admin`` source (meta.* views, REQ-1919) and no other.
A DIRECT plan for any other source that this node holds no connection for is refused by name —
it is never handed to the model store, where a read would answer from the control plane and a
write would land in it.
"""

# Requirements: REQ-825, REQ-1919, REQ-031

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from provisa.api.errors import ApiError
from provisa.pgwire._pipeline import _Plan, _run_plan_terminal
from provisa.transpiler.metadata_settings import WITHOUT_JIT, enter_client_role
from provisa.transpiler.router import Route


class _ModelStore:
    """The org's model store: records every statement it is asked to run."""

    def __init__(self) -> None:
        self.ran: list[str] = []
        self.bound: list[tuple[tuple, bool]] = []
        self.entered: list[str] = []
        self.prepared: list[bool] = []

    @asynccontextmanager
    async def acquire(self):
        store = self

        class _Conn:
            @asynccontextmanager
            async def transaction(self):
                yield

            async def execute(self, sql):
                store.entered.append(sql)

            async def fetch_with_columns(self, sql, *args, bind_as_given=False, prepared=True):
                assert store.entered, "a client statement reached the store as the pool role"
                store.ran.append(sql)
                store.bound.append((args, bind_as_given))
                store.prepared.append(prepared)
                return ["n"], [(1,)]

        yield _Conn()


class _Pools:
    """Connections this node holds: none."""

    def has(self, _source_id: str) -> bool:
        return False


def _state(store: _ModelStore):
    return type(
        "_State",
        (),
        {
            "model_db": store,
            "source_pools": _Pools(),
            "source_types": {"cassandra_writes": "cassandra"},
            "federation_engine": None,
        },
    )()


def _plan(source_id: str, sql: str) -> _Plan:
    return _Plan(route=Route.DIRECT, sql=sql, source_id=source_id, dialect="postgres")


async def test_the_admin_source_is_served_by_the_model_store():
    store = _ModelStore()
    result = await _run_plan_terminal(_plan("provisa-admin", "SELECT 1 AS n"), _state(store))
    assert result.rows == [(1,)]
    assert store.ran == ["SELECT 1 AS n"]


async def test_a_statement_that_is_told_nothing_still_runs_as_the_client_role():
    """REQ-2190: the role is the terminal's, not the catalog settings': a read of a table of meta
    that is no catalog view has no settings and runs as the client role all the same."""
    store = _ModelStore()
    await _run_plan_terminal(_plan("provisa-admin", "SELECT 1 AS n"), _state(store))
    assert store.entered == [WITHOUT_JIT, enter_client_role()]


async def test_a_client_statement_is_run_once_neither_prepared_nor_jit_compiled():
    """A client's statement is its own text, its literals written into it: kept prepared on the
    pooled connection, its plan (tens of megabytes for a catalog statement) stays in the server
    backend; JIT compiled, a privilege function's call took minutes on the control plane."""
    store = _ModelStore()
    await _run_plan_terminal(_plan("provisa-admin", "SELECT 1 AS n"), _state(store))
    assert store.prepared == [False]
    assert WITHOUT_JIT in store.entered


async def test_the_admin_source_receives_the_statements_bound_values():
    """A pgwire client's statement over ops/meta tables keeps its values: they are bound, a list
    as an array, not dropped (a psycopg prepare then bound none: "bind message supplies 0
    parameters")."""
    store = _ModelStore()
    sql = (
        'SELECT table_name FROM "org_default"."queries" AS "queries" '
        "WHERE id > $1 AND domain_id = ANY($2)"
    )
    plan = _Plan(
        route=Route.DIRECT,
        sql=sql,
        source_id="provisa-admin",
        dialect="postgres",
        exec_params=[5, ["sales", "ops"]],
    )
    await _run_plan_terminal(plan, _state(store))
    assert store.ran == [sql]
    assert store.bound == [((5, ["sales", "ops"]), True)]


@pytest.mark.parametrize(
    "sql",
    [
        'INSERT INTO "provisa_pipeline"."gadgets" (id, name) VALUES (2, \'B\')',
        'SELECT id FROM "provisa_pipeline"."gadgets"',
    ],
)
async def test_another_source_with_no_connection_is_refused_and_the_model_store_untouched(sql):
    store = _ModelStore()
    with pytest.raises(ApiError) as refused:
        await _run_plan_terminal(_plan("cassandra_writes", sql), _state(store))
    assert (refused.value.status_code, refused.value.code) == (500, "data.no_direct_route")
    assert "'cassandra_writes'" in refused.value.detail
    assert store.ran == []
