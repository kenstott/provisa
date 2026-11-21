# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4d2a71-3e58-4b16-8f0a-5d7e1b2c6a39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed write led by WITH runs as one statement where its source takes one, in two steps
elsewhere (REQ-039, pgwire/keyed_writes.py): which sources take one (executor/writable.py), and
how the pipeline plans and runs the two steps. What the two steps write, run on PostgreSQL, is
tests/unit/test_write_admission.py's."""

# Requirements: REQ-039

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.executor.writable import WritePath, resolve_write_path, write_refused_forms
from tests.write_governance import grants_over

# --- which sources run a write led by WITH ----------------------------------------------------


@pytest.mark.parametrize("source_type", ["postgresql", "cockroachdb", "yugabytedb"])
def test_a_postgresql_kin_runs_a_write_led_by_with_as_one_statement(source_type):
    assert resolve_write_path(source_type) in (WritePath.NATIVE, WritePath.SQLALCHEMY)
    assert "with_write" not in write_refused_forms(source_type)


@pytest.mark.parametrize(
    "source_type",
    [
        "mysql",
        "mariadb",
        "tidb",
        "singlestore",
        "sqlite",
        "duckdb",
        "greenplum",
        "snowflake",
        "bigquery",
        "clickhouse",
        "databricks",
        "oracle",
        "redshift",
        "synapse",
        "fabric",
        "exasol",
        "hiveserver2",
        "athena",
        "qdrant",
        "sharepoint",
        "salesforce",
    ],
)
def test_every_other_writable_source_takes_it_in_two_steps(source_type):
    assert resolve_write_path(source_type) is not None
    assert "with_write" in write_refused_forms(source_type)


def test_the_engine_route_takes_it_in_two_steps(monkeypatch):
    from provisa.executor import writable

    monkeypatch.setattr(writable, "resolve_write_path", lambda *_a, **_k: WritePath.ENGINE)
    assert "with_write" in writable.write_refused_forms("postgresql", object())  # type: ignore[arg-type]


# --- the plan and its run ---------------------------------------------------------------------


def _gov(*, two_steps: bool) -> GovernanceContext:
    gov = GovernanceContext()
    gov.role_id = "writer"
    gov.can_write = True
    gov.table_map = {"sales.orders": 1}
    gov.all_columns = {1: [("id", "integer"), ("region", "varchar")]}
    gov.write_grants = {1: grants_over("writer", ("id", "region"))}
    gov.rls_rules = {1: "region = 'east'"}
    gov.pk_columns = {1: ["id"]}
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {1: True}
    gov.write_refused_forms = {1: frozenset({"with_write"} if two_steps else ())}
    return gov


_CTX = SimpleNamespace(tables={"sales.orders": SimpleNamespace(table_id=1, source_id="shop")})
_STATE = SimpleNamespace(source_dialects={"shop": "mysql"})


def _governed(sql: str, *, two_steps: bool) -> SimpleNamespace:
    gov = _gov(two_steps=two_steps)
    return SimpleNamespace(
        governed_semantic=apply_governance(sql, gov),
        role_id="writer",
        table_ids=(1,),
        gov_ctx=gov,
    )


async def _plan(sql: str, *, two_steps: bool, reads: list):
    from provisa.pgwire import _pipeline

    async def _read(tree):
        reads.append(tree)
        return SimpleNamespace(sources=frozenset({"shop"}), audit="its own")

    async def _write(_statement, _values):  # pragma: no cover - not reached while planning
        raise AssertionError("nothing is written while the plan is made")

    return await _pipeline._keyed_write_plan(
        _governed(sql, two_steps=two_steps),  # type: ignore[arg-type]
        1,
        params=None,
        audit="the write's",
        state=_STATE,
        read=_read,  # pyright: ignore[reportArgumentType]  # a fake plan
        route_write=_write,  # pyright: ignore[reportArgumentType]  # a fake plan
        ctx=_CTX,
    )


async def test_a_source_running_a_write_led_by_with_gets_no_two_step_plan():
    reads: list = []
    sql = "DELETE FROM sales.orders WHERE id IN (SELECT 1)"
    assert await _plan(sql, two_steps=False, reads=reads) is None
    assert reads == []


async def test_a_write_not_led_by_with_runs_as_it_is_on_any_source():
    reads: list = []
    assert await _plan("DELETE FROM sales.orders WHERE id = 1", two_steps=True, reads=reads) is None
    assert reads == []


async def test_a_keyed_write_on_a_two_step_source_plans_its_key_read():
    reads: list = []
    plan = await _plan(
        "DELETE FROM sales.orders WHERE id IN (SELECT 1)", two_steps=True, reads=reads
    )
    assert plan is not None
    two, read_plan, _route = plan.keyed_write
    assert [r.sql(dialect="postgres") for r in reads] == [two.read.sql(dialect="postgres")]
    assert read_plan.audit is None  # the statement's one record is the write's
    assert plan.audit == "the write's" and plan.writes_tables and plan.written_table_id == 1
    assert plan.source_id == "shop" and plan.dialect == "mysql"
    assert sqlglot.parse_one(plan.sql, read="postgres").args.get("with_") is not None


async def test_the_terminal_reads_the_keys_then_writes_by_key(monkeypatch):
    from provisa.pgwire import _pipeline

    governed = _governed(
        "UPDATE sales.orders SET region = 'east' WHERE id IN (SELECT id FROM sales.orders) "
        "RETURNING id",
        two_steps=True,
    )
    routed: list[tuple[str, list | None]] = []

    async def _read(_tree):
        return SimpleNamespace(sources=frozenset({"shop"}), audit=None)

    async def _write(statement, values):
        routed.append((statement.sql(dialect="postgres"), values))
        return SimpleNamespace(rows=len(values) - 1)

    plan = await _pipeline._keyed_write_plan(
        governed,  # type: ignore[arg-type]
        1,
        params=None,
        audit=None,
        state=_STATE,
        read=_read,  # pyright: ignore[reportArgumentType]  # a fake plan
        route_write=_write,  # pyright: ignore[reportArgumentType]  # a fake plan
        ctx=_CTX,
    )
    assert plan is not None

    async def _bound(_read_plan, _state):
        # the keys and new values: two rows to make 'east', one a second time
        return SimpleNamespace(
            materialize=lambda: SimpleNamespace(rows=[(1, "east"), (3, "east"), (1, "east")])
        )

    async def _terminal(write, _state):
        return SimpleNamespace(rows=[(i, "east") for i in range(write.rows)], rowcount=write.rows)

    monkeypatch.setattr(_pipeline, "_execute_plan_bound", _bound)
    monkeypatch.setattr(_pipeline, "_run_plan_terminal", _terminal)
    result = await _pipeline._run_keyed_write(plan, _STATE)
    assert len(routed) == 1  # one set of new values: one write, each key once
    statement, values = routed[0]
    assert values == ["east", 1, 3]
    assert "WITH" not in statement.upper() and '"orders"."id" IN ($2, $3)' in statement
    assert '"orders"."region" = \'east\'' in statement  # the read rule, in the write by key
    assert result.rowcount == 2
    assert result.column_names == ["id", "region"]  # the served columns of the rows written
