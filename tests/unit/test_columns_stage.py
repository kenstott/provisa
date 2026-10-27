# Copyright (c) 2026 Kenneth Stott
# Canary: 57867371-16e9-4d62-a65d-760b55c869fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column transform in a CTE's select list is a staged, selective step (REQ-2131): each CTE
holding one is classified, then run on its own, restricted only where that leaves the answer
unchanged, and the statement reads its rows from a table.

Equivalence is shown on DuckDB, the transform's stand-in a SQL expression of its inputs: each
statement answers the same staged as written, and the staged step enriches only the rows the
statement can use."""

# Requirements: REQ-2131

from __future__ import annotations

import re

import duckdb
import pytest
import sqlglot

from provisa.codesource.columns_fold import fold
from provisa.codesource.columns_stage import classify, holds, stage

pytestmark = pytest.mark.unit

ENRICH = {
    "name": "enrich",
    "arguments": [{"name": "v", "type": "text"}],
    "output_columns": [{"name": "score", "type": "integer"}],
}
TRANSFORMS = {"enrich": ENRICH}
STAND_IN = "length(note) AS score"  # what enrich(note) gives, as SQL


def _classified(sql: str, cte: str = "c", computed=frozenset({"score"})):
    return classify(sqlglot.parse_one(sql, read="postgres"), cte, set(computed))


def _known(sql: str) -> dict[str, str]:
    return {c.sql: c.known for c in _classified(sql).conditions}


# -- classified before it is staged -----------------------------------------------------------------


def test_a_condition_on_literals_is_known_in_advance():
    got = _classified("WITH c AS (SELECT id, enrich(note) FROM t) SELECT * FROM c WHERE c.id > 3")
    assert got.restricted == "filter"
    assert [(c.sql, c.known) for c in got.conditions] == [("c.id > 3", "advance")]
    assert got.pushed == ("c.id > 3",)


def test_a_bound_parameter_and_a_session_variable_are_known_in_advance():
    assert _known(
        "WITH c AS (SELECT id, region, enrich(note) FROM t) SELECT * FROM c "
        "WHERE c.id = $1 AND c.region = current_setting('provisa.region')"
    ) == {"c.id = $1": "advance", "c.region = CURRENT_SETTING('provisa.region')": "advance"}


def test_a_join_key_is_known_from_the_other_relation():
    got = _classified(
        "WITH c AS (SELECT id, enrich(note) FROM t) "
        "SELECT * FROM c JOIN orders AS o ON c.id = o.customer_id WHERE o.total > 100"
    )
    assert got.restricted == "join"
    assert [(c.sql, c.known) for c in got.conditions] == [("c.id = o.customer_id", "join")]
    assert got.join is not None
    assert (got.join.column, got.join.other, got.join.key) == ("id", "o", "o.customer_id")
    assert got.join.conditions == ("o.total > 100",)


def test_an_exists_is_a_join_known_from_its_relation():
    got = _classified(
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT * FROM c "
        "WHERE EXISTS (SELECT 1 FROM orders AS o WHERE o.customer_id = c.id AND o.total > 100)"
    )
    assert got.restricted == "join"
    assert got.join is not None and got.join.conditions == ("o.total > 100",)


def test_a_condition_on_a_computed_column_is_known_only_after_the_transform():
    got = _classified(
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT * FROM c WHERE c.score > 5"
    )
    assert got.restricted == "filter"
    assert [c.known for c in got.conditions] == ["computed"]
    assert got.pushed == ()


def test_in_a_mixed_and_only_the_conjuncts_known_in_advance_are_pushed():
    got = _classified(
        "WITH c AS (SELECT id, region, enrich(note) FROM t) "
        "SELECT * FROM c JOIN orders AS o ON c.id = o.customer_id "
        "WHERE c.region = 'EU' AND c.score > 5 AND o.total > 100"
    )
    assert got.restricted == "both"
    assert _known(
        "WITH c AS (SELECT id, region, enrich(note) FROM t) "
        "SELECT * FROM c JOIN orders AS o ON c.id = o.customer_id "
        "WHERE c.region = 'EU' AND c.score > 5 AND o.total > 100"
    ) == {"c.id = o.customer_id": "join", "c.region = 'EU'": "advance", "c.score > 5": "computed"}
    assert got.pushed == ("c.region = 'EU'",)


def test_a_cte_nothing_restricts_is_neither():
    got = _classified("WITH c AS (SELECT id, enrich(note) FROM t) SELECT * FROM c")
    assert (got.restricted, got.conditions, got.join) == ("neither", (), None)


@pytest.mark.parametrize(
    "outer",
    [
        "SELECT * FROM orders AS o LEFT JOIN c ON c.id = o.customer_id WHERE c.id > 3",
        "SELECT region, count(*) FROM c WHERE c.id > 3 GROUP BY region",
        "SELECT * FROM c WHERE c.id > 3 LIMIT 5",
    ],
    ids=["outer join", "aggregate", "limit"],
)
def test_no_rewrite_is_made_across_an_outer_join_an_aggregate_or_a_limit(outer):
    got = _classified(f"WITH c AS (SELECT id, region, enrich(note) FROM t) {outer}")
    assert not got.rewritable and got.pushed == () and got.join is None


# -- staged ---------------------------------------------------------------------------------------


def test_a_statement_with_ctes_and_no_transform_is_left_untouched():
    sql = "WITH c AS (SELECT id FROM t) SELECT * FROM c WHERE c.id > 3"
    assert not holds(sql, TRANSFORMS)
    assert stage(sql, TRANSFORMS, lambda command, name: None) is None
    assert fold(sql, TRANSFORMS, lambda command, name: None).sql == sql


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT id FROM t WHERE enrich(note) > 1) SELECT * FROM c",
        "WITH c AS (SELECT id, enrich(note) + 1 AS s FROM t) SELECT * FROM c",
        "WITH c AS (SELECT id FROM t) SELECT * FROM c WHERE enrich(c.id) > 1",
    ],
    ids=["a cte's where", "inside an expression", "the statement's where"],
)
def test_a_call_anywhere_but_a_select_list_item_is_still_refused_by_name(sql):
    from provisa.codesource.columns import ColumnTransformRefused

    assert not holds(sql, TRANSFORMS)
    with pytest.raises(ColumnTransformRefused, match="enrich"):
        fold(sql, TRANSFORMS, lambda command, name: None)


def test_each_call_in_a_cte_is_admitted_as_the_command_it_is():
    admitted: list = []
    stage(
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT * FROM c",
        TRANSFORMS,
        lambda command, name: admitted.append(name),
    )
    assert admitted == ["enrich"]


def _db() -> duckdb.DuckDBPyConnection:
    db = duckdb.connect()
    db.execute(
        "CREATE TABLE t AS SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'EU' ELSE 'US' END AS region, "
        "repeat('x', i % 7) AS note FROM range(1, 41) r(i)"
    )
    db.execute(
        "CREATE TABLE orders AS SELECT i AS order_id, (i * 3) % 40 + 1 AS customer_id, "
        "i * 10 AS total FROM range(1, 16) r(i)"
    )
    return db


def _as_sql(sql: str) -> str:
    """The statement with the transform's stand-in for each call of it."""
    return re.sub(r"enrich\(note\)", STAND_IN, sql, flags=re.IGNORECASE)


def _staged_answer(db, sql: str) -> tuple[list, int]:
    """The statement's answer run staged, and how many rows the staged step enriched."""
    staged = stage(sql, TRANSFORMS, lambda command, name: None)
    assert staged is not None
    enriched = 0
    for s in staged.stages:
        body = s.body
        if s.key_read is not None:
            keys = [r[0] for r in db.execute(s.key_read).fetchall()]
            body = s.restricted(keys)
        rows = db.execute(_as_sql(body)).fetchall()
        enriched += len(rows)
        db.execute(f"CREATE TEMP TABLE {s.table} AS {_as_sql(body)}")
    return sorted(db.execute(staged.outer).fetchall()), enriched


STATEMENTS = {
    "filter known in advance": (
        "WITH c AS (SELECT id, region, enrich(note) FROM t) SELECT id, score FROM c "
        "WHERE c.region = 'EU' AND c.id > 30",
        5,
    ),
    "filter on a computed column": (
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT id FROM c WHERE c.score > 4",
        40,
    ),
    "mixed": (
        "WITH c AS (SELECT id, region, enrich(note) FROM t) SELECT id FROM c "
        "WHERE c.region = 'US' AND c.score > 2",
        20,
    ),
    "inner join": (
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT c.id, o.order_id, c.score "
        "FROM c JOIN orders AS o ON c.id = o.customer_id WHERE o.total > 100",
        5,  # the customers of orders 11 to 15
    ),
    "exists": (
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT id, score FROM c "
        "WHERE EXISTS (SELECT 1 FROM orders AS o WHERE o.customer_id = c.id AND o.total > 100)",
        5,
    ),
    "outer join": (
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT o.order_id, c.score "
        "FROM orders AS o LEFT JOIN c ON c.id = o.customer_id WHERE o.total > 100",
        40,
    ),
    "aggregate": (
        "WITH c AS (SELECT id, region, enrich(note) FROM t) "
        "SELECT region, sum(score) FROM c WHERE c.id > 30 GROUP BY region",
        40,
    ),
    "limit": (
        "WITH c AS (SELECT id, enrich(note) FROM t) SELECT id FROM c WHERE c.id > 30 "
        "ORDER BY id LIMIT 3",
        40,
    ),
    "a cte that limits its own rows": (
        "WITH c AS (SELECT id, enrich(note) FROM t ORDER BY id LIMIT 10) SELECT id FROM c "
        "WHERE c.id > 5",
        10,
    ),
    "an earlier cte it reads": (
        "WITH base AS (SELECT id, note FROM t WHERE id <= 20), "
        "c AS (SELECT id, enrich(note) FROM base) SELECT id, score FROM c WHERE c.id > 15",
        5,
    ),
}


@pytest.mark.parametrize(("sql", "needed"), STATEMENTS.values(), ids=STATEMENTS.keys())
def test_staged_the_statement_answers_as_written_enriching_only_the_rows_it_can_use(sql, needed):
    db = _db()
    as_written = sorted(db.execute(_as_sql(sql)).fetchall())
    answer, enriched = _staged_answer(db, sql)
    assert answer == as_written
    assert enriched == needed


# -- run by the pipeline --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_each_step_is_a_read_of_the_governed_statement_run_in_order(monkeypatch):
    """The join's other side, then the CTE restricted to its keys and folded, then the statement
    reading the landed table: each a read of the governed statement, under its role; the role's
    row filter (``o.tenant``) in the governed statement is in the key read; the tables end with
    the statement."""
    import dataclasses
    from types import SimpleNamespace

    from provisa.compiler import temp_tables
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline

    governed_sql = (
        "WITH c AS (SELECT id, enrich(note) FROM t WHERE t.tenant = 'a') "
        "SELECT c.id, c.score FROM c JOIN orders AS o ON c.id = o.customer_id "
        "WHERE o.total > 100 AND o.tenant = 'a'"
    )
    staged = stage(governed_sql, TRANSFORMS, lambda command, name: None)
    assert staged is not None
    (step,) = staged.stages
    governed = SimpleNamespace(
        role_id="analyst", governed_semantic=governed_sql, parsed=None, memo={}, table_ids=(1, 2)
    )
    monkeypatch.setattr(
        dataclasses, "replace", lambda g, **kw: SimpleNamespace(**{**vars(g), **kw})
    )
    reads: list = []

    async def route(g, *, params, as_of):
        reads.append(g)
        return SimpleNamespace(audit="kept", governed=g)

    async def execute(part, _state):
        sql = part.governed.governed_semantic
        if sql.startswith("SELECT DISTINCT"):
            return QueryResult(rows=[(7,), (9,)], column_names=["customer_id"])
        if step.table in sql:
            return QueryResult(rows=[(7, 3)], column_names=["id", "score"])
        return QueryResult(
            rows=[(7, "xxx"), (9, "x")], column_names=["id", "__provisa_columns_0_0"]
        )

    admitted, landed, dropped = [], [], []

    async def reland(_state, _session, name, columns, rows):
        landed.append((name, columns, rows))
        _session.tables[name] = None

    async def drop(_store, _schema, name):
        dropped.append(name)

    monkeypatch.setattr(_pipeline, "route_governed", route)
    monkeypatch.setattr(_pipeline, "_execute_plan_bound", execute)
    monkeypatch.setattr("provisa.pgwire.temp_exec.reland", reland)
    monkeypatch.setattr("provisa.federation.store_scope.drop_temp_table", drop)
    monkeypatch.setattr("provisa.federation.temp_address.schema_of", lambda session: "s")
    monkeypatch.setattr(
        "provisa.api.data.action_exec.admit_command",
        lambda command, state, role, name: admitted.append((role, name)),
    )
    state = SimpleNamespace(
        column_transforms=TRANSFORMS,
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(materialize_store=lambda: "store")
        ),
    )
    plan = _pipeline._staged_plan(governed, staged, params=None, as_of=None, audit=None)
    token = temp_tables.bind(temp_tables.TempSession())
    try:
        answer = await _pipeline._run_staged(plan, state)
    finally:
        temp_tables.unbind(token)
    key_read, body_read, outer_read = (r.governed_semantic for r in reads)
    assert (
        key_read
        == "SELECT DISTINCT o.customer_id FROM orders AS o WHERE o.total > 100 AND o.tenant = 'a'"
    )
    assert "t.tenant = 'a'" in body_read and "id IN (7, 9)" in body_read
    assert "__provisa_columns_0_0" in body_read and "columns" in reads[1].memo  # folded
    assert step.table in outer_read and "enrich" not in outer_read.lower()
    assert all(r.role_id == "analyst" for r in reads)
    assert admitted == [("analyst", "enrich")]
    assert [n for n, _, _ in landed] == dropped == [step.table]
    assert answer.rows == [(7, 3)]
