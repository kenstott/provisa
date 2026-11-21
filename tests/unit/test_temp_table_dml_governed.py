# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0d2c7e-91a4-4e3f-8c6d-2f7a1e9b4d08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An UPDATE or a DELETE of a session's temporary table reads as the caller's role (REQ-615,
REQ-1926): its WHERE and SET expressions are in the one statement the pipeline governs, so a
subquery in them reads a registered table through the role's row filter and masks, and a relation
nothing governs is refused. The platform's own statements over the table carry none of the
client's words."""

# Requirements: REQ-615, REQ-1926

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from provisa.compiler import temp_tables
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_validator import ValidationViolation, validate_sql
from provisa.compiler.stage2 import apply_governance
from provisa.compiler.temp_tables import TempSession, TempTable, action_of
from provisa.executor.result import QueryResult
from provisa.pgwire import temp_exec
from provisa.security.masking import MaskingRule, MaskType
from provisa.transpiler.transpile import transpile
from tests.helpers import governance_for_role, unscoped_role

_ROLE = "clerk"
_CUSTOMERS = TableMeta(
    table_id=2,
    field_name="customers",
    type_name="Customers",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="customers",
)
_COLUMNS = {2: [("id", "integer"), ("email", "varchar"), ("region", "varchar")]}
_ROW_RULE = {2: "region = 'east'"}
_MASK = {(2, _ROLE): {"email": (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")}}


class Refused(Exception):
    """The statement the pipeline governs was refused: its violations."""

    def __init__(self, violations: list[ValidationViolation]) -> None:
        self.codes = [v.code for v in violations]
        super().__init__("; ".join(f"[{v.code}] {v.message}" for v in violations))


class Store:
    """The engine's store as one DuckDB: a registered table the role reads through a row filter
    and a mask, a table nothing registers, and the session's temporary table ``t``."""

    def __init__(self, monkeypatch: Any) -> None:
        self.duck = duckdb.connect()
        self.duck.execute("CREATE SCHEMA public")
        self.duck.execute(
            "CREATE TABLE public.customers (id INTEGER, email VARCHAR, region VARCHAR)"
        )
        self.duck.execute(
            "INSERT INTO public.customers VALUES (1, 'a@x.io', 'east'), (2, 'b@x.io', 'west')"
        )
        self.duck.execute("CREATE TABLE public.payroll AS SELECT 2 AS id, 900 AS pay")
        self.duck.execute("CREATE TABLE t (id BIGINT, note VARCHAR)")
        self.duck.execute("INSERT INTO t VALUES (1, 'a@x.io'), (2, 'b@x.io'), (3, NULL)")
        self.session = TempSession()
        self.session.tables["t"] = TempTable("t", [("id", "bigint"), ("note", "text")])
        #: Each statement run with the platform's own authority, no role's rules applied.
        self.system: list[str] = []
        self.ctx = CompilationContext()
        self.ctx.tables = {"customers": _CUSTOMERS}
        self.ctx.joins = {}
        self.gov = governance_for_role(
            self.ctx, _ROLE, rls=_ROW_RULE, masking_rules=_MASK, column_types=_COLUMNS
        )

        async def address(state: Any, session: TempSession, name: str) -> str:
            return f'"{name}"'

        def system_rows(state: Any, pg_sql: str, label: str) -> tuple[list[str], list[tuple]]:
            self.system.append(pg_sql)
            cursor = self.duck.execute(transpile(pg_sql, "duckdb"))
            return [d[0] for d in cursor.description], cursor.fetchall()

        async def land(
            state: Any, session: TempSession, name: str, batch: Any, columns: list
        ) -> None:
            self.duck.execute(f'DELETE FROM "{name}"')
            for row in batch.to_pylist():
                self.duck.execute(f'INSERT INTO "{name}" VALUES (?, ?)', list(row.values()))

        monkeypatch.setattr(temp_exec, "_store_address", address)
        monkeypatch.setattr(temp_exec, "_system_rows", system_rows)
        monkeypatch.setattr(temp_exec, "_land", land)
        monkeypatch.setattr(temp_exec, "_within_rows", lambda name, rows: None)

    def governed(self, sql: str) -> QueryResult:
        """``sql`` as the one pipeline serves it to the role: judged, governed, run."""
        violations = validate_sql(sql, self.ctx, self.gov, unscoped_role(_ROLE), [])
        if violations:
            raise Refused(violations)
        governed = apply_governance(sql, self.gov).replace('"pg".', "")
        cursor = self.duck.execute(transpile(governed, "duckdb"))
        return QueryResult(rows=cursor.fetchall(), column_names=[d[0] for d in cursor.description])

    async def run(self, sql: str) -> QueryResult:
        """A statement on the session's temporary table as the pipeline does it: the action's
        SELECT governed and run as the role, then the action applied with its rows."""
        token = temp_tables.bind(self.session)
        try:
            action = action_of(sql)
            assert action is not None
            return await temp_exec.apply(action, self.governed(action.select), None, _ROLE)
        finally:
            temp_tables.unbind(token)

    def held(self) -> list[tuple]:
        return self.duck.execute("SELECT * FROM t ORDER BY id").fetchall()


@pytest.fixture
def store(monkeypatch) -> Store:
    return Store(monkeypatch)


async def test_an_update_subquery_reads_only_the_rows_the_roles_row_filter_gives(store):
    await store.run("UPDATE t SET id = (SELECT count(*) FROM public.customers) WHERE id = 3")
    assert store.held() == [(1, "a@x.io"), (1, None), (2, "b@x.io")]


async def test_a_delete_subquery_reads_only_the_rows_the_roles_row_filter_gives(store):
    result = await store.run("DELETE FROM t WHERE id IN (SELECT id FROM public.customers)")
    assert store.held() == [(2, "b@x.io"), (3, None)]
    assert result.rowcount == 1


async def test_an_update_subquery_reads_a_masked_column_masked(store):
    await store.run("UPDATE t SET note = (SELECT max(email) FROM public.customers)")
    assert store.held() == [(1, "***"), (2, "***"), (3, "***")]


async def test_a_delete_subquery_reads_a_masked_column_masked(store):
    result = await store.run("DELETE FROM t WHERE note IN (SELECT email FROM public.customers)")
    assert store.held() == [(1, "a@x.io"), (2, "b@x.io"), (3, None)]
    assert result.rowcount == 0


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE t SET id = (SELECT max(pay) FROM public.payroll)",
        "UPDATE t SET id = (SELECT count(*) FROM duckdb_secrets())",
        "UPDATE t SET note = (SELECT max(content) FROM read_text('/etc/hosts'))",
        "UPDATE t SET note = 'x' WHERE id IN (SELECT pay FROM public.payroll)",
        "DELETE FROM t WHERE id IN (SELECT id FROM public.payroll)",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM duckdb_secrets())",
        "DELETE FROM t WHERE note = (SELECT max(content) FROM read_text('/etc/hosts'))",
    ],
)
async def test_a_relation_nothing_governs_is_refused_and_the_table_left_as_it_was(store, sql):
    before = store.held()
    with pytest.raises(Refused) as refused:
        await store.run(sql)
    assert "V006" in refused.value.codes
    assert store.held() == before


async def test_plain_updates_and_deletes_of_the_table_still_work(store):
    await store.run("UPDATE t SET note = 'venus' WHERE id = 1")
    assert store.held() == [(1, "venus"), (2, "b@x.io"), (3, None)]
    await store.run("UPDATE t SET id = id + 10, note = upper(note) WHERE note IS NOT NULL")
    assert store.held() == [(3, None), (11, "VENUS"), (12, "B@X.IO")]
    one = await store.run("DELETE FROM t WHERE id = 11 OR note = 'nobody'")
    assert one.rowcount == 1
    assert store.held() == [(3, None), (12, "B@X.IO")]
    # A condition that is NULL for a row leaves the row, as PostgreSQL does.
    assert (await store.run("DELETE FROM t WHERE note <> 'x'")).rowcount == 1
    assert store.held() == [(3, None)]
    assert (await store.run("DELETE FROM t")).rowcount == 1
    assert store.held() == []


async def test_the_table_is_read_by_the_alias_the_statement_gives_it(store):
    await store.run("UPDATE t AS x SET note = 'none' WHERE x.note IS NULL")
    assert store.held() == [(1, "a@x.io"), (2, "b@x.io"), (3, "none")]
    await store.run("DELETE FROM t AS x WHERE x.id < 3")
    assert store.held() == [(3, "none")]


async def test_the_platforms_own_statements_carry_none_of_the_clients_words(store):
    await store.run("UPDATE t SET note = (SELECT max(email) FROM public.customers) WHERE id = 1")
    await store.run("DELETE FROM t WHERE id IN (SELECT id FROM public.customers)")
    assert set(store.system) == {'SELECT count(*) FROM "t"'}


async def test_a_read_that_did_not_give_every_row_of_the_table_changes_nothing(store):
    """The governed read is bounded like any read of the role's (its row limit): a table read
    short of what it holds is never landed as the rows it has left."""
    token = temp_tables.bind(store.session)
    try:
        action = action_of("DELETE FROM t WHERE id = 1")
        assert action is not None
        cut = store.governed(action.select)
        cut.rows = cut.rows[:2]
        with pytest.raises(ValueError, match="holds 3 row.*read 2 of them"):
            await temp_exec.apply(action, cut, None, _ROLE)
    finally:
        temp_tables.unbind(token)
    assert len(store.held()) == 3
