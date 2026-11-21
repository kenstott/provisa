# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e7d3c-2f4a-4c86-9e07-a3d6c18f4b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Snowflake source's Dynamic Tables and Tasks, as tables and commands (REQ-2230).

A Dynamic Table is offered as a table and what its definition reads joins the lineage graph. What
can be run -- refreshing, suspending and resuming a Dynamic Table, executing, suspending and
resuming a Task -- is offered as commands, each a mutation, run with the source's credential and
recorded as any command is. Offering registers nothing."""

# Requirements: REQ-2230

from __future__ import annotations

from types import SimpleNamespace

import pytest
from snowflake.connector.errors import ProgrammingError

from provisa.api.errors import ApiError
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.executor import source_operation as ops
from provisa.executor.result import QueryResult
from provisa.otel_compat import MemoryUdfTraceSink

SF = "sf"
DDL = (
    "create or replace dynamic table DAILY_SALES target_lag = '1 minute' warehouse = WH "
    "as select o.id as order_id, amount from RAW.ORDERS o"
)


class _Pool:
    """A connected Snowflake source: answers its SHOW statements, records every statement."""

    def __init__(self, *, dynamic=None, tasks=None, refusal: Exception | None = None) -> None:
        self.dynamic = (
            [{"name": "DAILY_SALES", "comment": "", "text": DDL}] if dynamic is None else dynamic
        )
        self.tasks = [{"name": "NIGHTLY", "comment": ""}] if tasks is None else tasks
        self.refusal = refusal
        self.statements: list[str] = []
        self.information_schema: list[tuple] = [("ORDERS",)]

    def has(self, source_id: str) -> bool:
        return source_id == SF

    @staticmethod
    def _rows(found: list[dict]) -> QueryResult:
        names = ["created_on", "name", "comment", "text"]
        return QueryResult(
            rows=[tuple(row.get(n) for n in names) for row in found], column_names=names
        )

    async def execute(self, source_id: str, sql: str, params: list | None = None) -> QueryResult:
        self.statements.append(sql)
        if sql.startswith("SHOW DYNAMIC TABLES"):
            return self._rows(self.dynamic)
        if sql.startswith("SHOW TASKS"):
            return self._rows(self.tasks)
        if "information_schema.tables" in sql:
            return QueryResult(rows=self.information_schema, column_names=["TABLE_NAME"])
        if self.refusal is not None:
            raise self.refusal
        return QueryResult(rows=[("done",)], column_names=["status"])


@pytest.fixture
def state(monkeypatch) -> SimpleNamespace:
    st = SimpleNamespace(
        multitenancy=False,
        source_types={SF: "snowflake", "pg": "postgresql"},
        source_pools=_Pool(),
    )
    monkeypatch.setattr("provisa.api.app.state", st)
    return st


async def _offered_tables(native_tables, state) -> list:
    """What the source offers as tables; a Snowflake source reads no control-plane connection."""
    offered = await native_tables(SF, "snowflake", "ANALYTICS", state.source_pools, None, state)
    assert offered is not None
    return offered


async def _offered_operations(state, schema_name: str = "ANALYTICS") -> list[ops.Operation]:
    offered = await ops.offered_operations(state, SF, schema_name)
    assert offered is not None
    return offered


# --- what the source offers ----------------------------------------------------------------------


async def test_a_dynamic_table_is_offered_as_a_table_and_named_as_one(state):
    from provisa.api.admin.introspect import native_tables

    offered = await _offered_tables(native_tables, state)
    assert [(t.name, t.comment) for t in offered] == [
        ("DAILY_SALES", "DYNAMIC TABLE"),
        ("ORDERS", None),
    ]


async def test_a_dynamic_table_the_catalog_also_lists_is_offered_once(state):
    from provisa.api.admin.introspect import native_tables

    state.source_pools.information_schema = [("DAILY_SALES",), ("ORDERS",)]
    offered = await _offered_tables(native_tables, state)
    assert [t.name for t in offered] == ["DAILY_SALES", "ORDERS"]


async def test_a_dynamic_tables_own_comment_is_offered_with_it(state):
    from provisa.api.admin.introspect import native_tables

    state.source_pools.dynamic = [{"name": "D", "comment": "Sales by day", "text": DDL}]
    offered = await _offered_tables(native_tables, state)
    assert offered[0].comment == "DYNAMIC TABLE — Sales by day"


async def test_the_refresh_suspend_resume_and_execute_operations_are_offered(state):
    offered = await _offered_operations(state)
    assert [(op.name, op.comment, op.arguments, op.reads) for op in offered] == [
        (
            "refresh_dynamic_table.DAILY_SALES",
            'ALTER DYNAMIC TABLE "ANALYTICS"."DAILY_SALES" REFRESH',
            (),
            False,
        ),
        (
            "suspend_dynamic_table.DAILY_SALES",
            'ALTER DYNAMIC TABLE "ANALYTICS"."DAILY_SALES" SUSPEND',
            (),
            False,
        ),
        (
            "resume_dynamic_table.DAILY_SALES",
            'ALTER DYNAMIC TABLE "ANALYTICS"."DAILY_SALES" RESUME',
            (),
            False,
        ),
        ("execute_task.NIGHTLY", 'EXECUTE TASK "ANALYTICS"."NIGHTLY"', (), False),
        ("suspend_task.NIGHTLY", 'ALTER TASK "ANALYTICS"."NIGHTLY" SUSPEND', (), False),
        ("resume_task.NIGHTLY", 'ALTER TASK "ANALYTICS"."NIGHTLY" RESUME', (), False),
    ]


async def test_offering_runs_nothing_but_the_reads_of_the_catalog(state):
    from provisa.api.admin.introspect import native_tables

    await _offered_tables(native_tables, state)
    await ops.offered_operations(state, SF, "ANALYTICS")
    assert not [s for s in state.source_pools.statements if s.startswith(("ALTER", "EXECUTE"))]


async def test_a_source_that_is_not_connected_offers_no_operations(state):
    state.source_pools.has = lambda _source_id: False
    assert await ops.offered_operations(state, SF, "ANALYTICS") is None


async def test_a_name_is_quoted_as_the_identifier_it_is(state):
    state.source_pools.dynamic = [{"name": 'we"ird', "comment": "", "text": DDL}]
    state.source_pools.tasks = []
    offered = await _offered_operations(state)
    assert offered[0].comment == 'ALTER DYNAMIC TABLE "ANALYTICS"."we""ird" REFRESH'


async def test_the_schema_read_is_quoted_too(state):
    await ops.offered_operations(state, SF, 'an"x')
    assert state.source_pools.statements[0] == 'SHOW DYNAMIC TABLES IN SCHEMA "an""x"'


# --- registering one -----------------------------------------------------------------------------


def _form(**over):
    from provisa.api.admin.actions_router import FunctionInput

    return FunctionInput(
        **{
            "name": "refresh_sales",
            "sourceId": SF,
            "schemaName": "ANALYTICS",
            "functionName": "refresh_dynamic_table.DAILY_SALES",
            "returns": "something",
            "kind": "query",
            "implKind": "source_procedure",
            "arguments": [{"name": "x", "type": "text"}],
            **over,
        }
    )


async def test_registering_an_operation_takes_what_it_is_from_the_source(state):
    from provisa.api.admin.actions_router import _as_source_operation

    form = _form()
    await _as_source_operation(form)
    assert (form.implKind, form.kind, form.returns, form.arguments) == (
        "source_operation",
        "mutation",
        "",
        [],
    )
    assert form.schemaName == "ANALYTICS"  # the schema the object is in, as the steward named it


async def test_an_operation_on_an_object_the_source_does_not_have_is_refused(state):
    from provisa.api.admin.actions_router import _as_source_operation

    with pytest.raises(ApiError) as refused:
        await _as_source_operation(_form(functionName="refresh_dynamic_table.GONE"))
    assert refused.value.code == "functions.operation_not_offered"


async def test_a_routine_of_a_snowflake_source_is_left_as_the_form_states_it(state):
    from provisa.api.admin.actions_router import _as_source_operation

    form = _form(functionName="MY_PROC")
    await _as_source_operation(form)
    assert (form.implKind, form.kind) == ("source_procedure", "query")


# --- running one ---------------------------------------------------------------------------------


def _command(function_name: str = "refresh_dynamic_table.DAILY_SALES", **over) -> dict:
    return {
        "name": "refresh_sales",
        "source_id": SF,
        "schema_name": "ANALYTICS",
        "function_name": function_name,
        "impl_kind": "source_operation",
        "kind": "mutation",
        "arguments": [],
        **over,
    }


@pytest.mark.parametrize(
    ("operation", "statement"),
    [
        ("refresh_dynamic_table.DAILY_SALES", 'ALTER DYNAMIC TABLE "A"."DAILY_SALES" REFRESH'),
        ("suspend_dynamic_table.DAILY_SALES", 'ALTER DYNAMIC TABLE "A"."DAILY_SALES" SUSPEND'),
        ("resume_dynamic_table.DAILY_SALES", 'ALTER DYNAMIC TABLE "A"."DAILY_SALES" RESUME'),
        ("execute_task.NIGHTLY", 'EXECUTE TASK "A"."NIGHTLY"'),
        ("suspend_task.NIGHTLY", 'ALTER TASK "A"."NIGHTLY" SUSPEND'),
        ("resume_task.NIGHTLY", 'ALTER TASK "A"."NIGHTLY" RESUME'),
    ],
)
async def test_each_operation_runs_its_statement_on_the_source(state, operation, statement):
    from provisa.executor.function_dispatch import dispatch_function

    rows = await dispatch_function(_command(operation, schema_name="A"), {}, state, "dev")
    assert state.source_pools.statements[-1] == statement
    assert rows == [{"status": "done"}]


async def test_the_call_is_recorded_as_any_command_is(state):
    from provisa.executor.function_dispatch import dispatch_function

    sink = MemoryUdfTraceSink()
    await dispatch_function(_command(), {}, state, "dev", trace_sink=sink)
    (trace,) = sink.records
    assert (trace.udf_name, trace.role_id, trace.output_cardinality) == ("refresh_sales", "dev", 1)


def _held_by(state, role: str) -> None:
    fn = _command(writable_by=["dev"], visible_to=["dev"])
    state.tracked_functions = {fn["name"]: fn}
    state.roles = {
        role: {"id": role, "capabilities": ["write", "command_execution"], "domain_access": ["*"]}
    }


async def test_a_role_that_holds_the_command_runs_the_refresh(state):
    from provisa.api.data.action_exec import invoke_tracked_function

    _held_by(state, "dev")
    await invoke_tracked_function("refresh_sales", {}, state, "dev")
    assert state.source_pools.statements[-1] == (
        'ALTER DYNAMIC TABLE "ANALYTICS"."DAILY_SALES" REFRESH'
    )


async def test_a_role_that_does_not_hold_the_command_cannot_run_it(state):
    from provisa.api.data.action_exec import invoke_tracked_function

    _held_by(state, "guest")
    with pytest.raises(ApiError) as refused:
        await invoke_tracked_function("refresh_sales", {}, state, "guest")
    assert (refused.value.status_code, refused.value.code) == (404, "functions.unknown_command")
    assert not [s for s in state.source_pools.statements if s.startswith("ALTER")]


async def test_snowflakes_refusal_comes_back_as_it_stated_it(state):
    state.source_pools.refusal = ProgrammingError(msg="Insufficient privileges", errno=3001)
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(state, SF, "refresh_dynamic_table.DAILY_SALES", {}, "ANALYTICS")
    assert refused.value.code == "functions.remote_refused"
    assert refused.value.status_code == 422
    assert "Insufficient privileges" in str(refused.value.detail)


async def test_an_object_gone_since_it_was_registered_is_refused_before_any_statement(state):
    state.source_pools.dynamic = []
    with pytest.raises(ApiError) as refused:
        await ops.call_operation(state, SF, "refresh_dynamic_table.DAILY_SALES", {}, "ANALYTICS")
    assert refused.value.code == "functions.operation_not_offered"
    assert not [s for s in state.source_pools.statements if s.startswith("ALTER")]


# --- the definition, kept with the table -----------------------------------------------------------


def _model(table_name: str = "DAILY_SALES", **over) -> SimpleNamespace:
    return SimpleNamespace(
        source_id=SF,
        schema_name="ANALYTICS",
        table_name=table_name,
        view_sql=None,
        source_definition=None,
        **over,
    )


async def test_registering_a_dynamic_table_keeps_its_definition(state):
    from provisa.api.admin.introspect import record_source_definition

    model = _model()
    await record_source_definition(model, "snowflake")
    assert model.source_definition == DDL


async def test_an_ordinary_table_has_no_definition(state):
    from provisa.api.admin.introspect import record_source_definition

    model = _model("ORDERS")
    await record_source_definition(model, "snowflake")
    assert model.source_definition == ""  # read, and the source reports none


async def test_a_source_that_is_not_connected_leaves_the_definition_unread(state):
    from provisa.api.admin.introspect import record_source_definition

    state.source_pools.has = lambda _source_id: False
    model = _model()
    await record_source_definition(model, "snowflake")
    assert model.source_definition is None


def _table(definition: str | None):
    from provisa.core.models import Table

    return Table(
        source_id=SF,
        domain_id="sales",
        schema_name="ANALYTICS",
        table_name="DAILY_SALES",
        columns=[],
        source_definition=definition,
    )


@pytest.mark.asyncio
async def test_a_save_that_did_not_read_the_definition_keeps_the_one_held(control_plane):
    from sqlalchemy import insert, select

    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import domains, registered_tables, sources

    stored = select(registered_tables.c.source_definition)
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales", description="sales"))
            await conn.execute_core(insert(sources).values(id=SF, type="snowflake"))
            await table_repo.upsert(conn, _table(DDL))
            await table_repo.upsert(conn, _table(None))  # a configuration applied again
            assert (await conn.execute_core(stored)).scalar_one() == DDL
            await table_repo.upsert(conn, _table(""))  # read: it is no Dynamic Table any more
            assert (await conn.execute_core(stored)).scalar_one() == ""
    finally:
        await db.close()


async def test_a_table_of_another_source_type_reads_nothing(state):
    from provisa.api.admin.introspect import record_source_definition

    model = _model()
    await record_source_definition(model, "postgresql")
    assert model.source_definition is None
    assert state.source_pools.statements == []


def test_the_definition_is_a_field_of_a_table_and_is_stored():
    from provisa.core.models import Table
    from provisa.core.schema_org import registered_tables

    assert "source_definition" in Table.model_fields
    assert "source_definition" in registered_tables.c


# --- what the definition contributes to lineage ----------------------------------------------------


def _row(table: str, column: str, **over) -> dict:
    return {
        "source_id": SF,
        "database": "DW",
        "domain_id": "sales",
        "schema_name": "RAW",
        "table_name": table,
        "source_definition": None,
        "column_name": column,
        "alias": None,
        **over,
    }


def _dynamic(*columns: str, definition: str = DDL) -> list[dict]:
    return [
        _row("DAILY_SALES", c, schema_name="ANALYTICS", source_definition=definition)
        for c in columns
    ]


def _edges(rows: list[dict]) -> set[tuple[str, str]]:
    """Each (source node, target node) the definitions make, over the graph the model build reads."""
    from provisa.lineage.merge import federation_subdags
    from provisa.lineage.source_definitions import definition_views

    views = [(f"{v['domain_id']}.{v['table_name']}", v["view_sql"]) for v in definition_views(rows)]
    relations = sorted({f"{r['domain_id']}.{r['table_name']}" for r in rows})
    found: set[tuple[str, str]] = set()
    for _relation, graph in federation_subdags(views, extra_relations=relations):
        found |= {(e.source, e.target) for e in graph.edges}
    return found


def _reaches(edges: set[tuple[str, str]], source: str, target: str) -> bool:
    frontier, seen = {source}, set()
    while frontier:
        at = frontier.pop()
        seen.add(at)
        frontier |= {t for s, t in edges if s == at and t not in seen}
    return target in seen


def test_each_column_of_a_dynamic_table_comes_from_the_registered_column_its_definition_reads():
    edges = _edges(
        [_row("ORDERS", "ID"), _row("ORDERS", "AMOUNT"), *_dynamic("ORDER_ID", "AMOUNT")]
    )
    assert _reaches(edges, "sales.ORDERS.id", "sales.DAILY_SALES.order_id")
    assert _reaches(edges, "sales.ORDERS.amount", "sales.DAILY_SALES.amount")


def test_a_table_read_by_three_part_name_in_the_sources_database_is_the_registered_one():
    definition = "create dynamic table D as select id from DW.RAW.ORDERS"
    edges = _edges([_row("ORDERS", "ID"), *_dynamic("ID", definition=definition)])
    assert _reaches(edges, "sales.ORDERS.id", "sales.DAILY_SALES.id")


def test_a_table_of_another_database_is_not_the_registered_one():
    definition = "create dynamic table D as select id from OTHER.RAW.ORDERS"
    edges = _edges([_row("ORDERS", "ID"), *_dynamic("ID", definition=definition)])
    assert not _reaches(edges, "sales.ORDERS.id", "sales.DAILY_SALES.id")


def test_a_table_of_another_source_is_not_the_registered_one():
    edges = _edges([_row("ORDERS", "ID", source_id="elsewhere"), *_dynamic("ORDER_ID", "AMOUNT")])
    assert not _reaches(edges, "sales.ORDERS.id", "sales.DAILY_SALES.order_id")


def test_an_unqualified_table_is_read_in_the_dynamic_tables_own_schema():
    definition = "create dynamic table D as select id from LOCAL_T"
    edges = _edges(
        [_row("LOCAL_T", "ID", schema_name="ANALYTICS"), *_dynamic("ID", definition=definition)]
    )
    assert _reaches(edges, "sales.LOCAL_T.id", "sales.DAILY_SALES.id")


def test_an_alias_on_a_column_is_the_name_its_lineage_is_read_by():
    edges = _edges(
        [
            _row("ORDERS", "ID", alias="order_key"),
            _row("ORDERS", "AMOUNT"),
            *_dynamic("ORDER_ID", "AMOUNT"),
        ]
    )
    assert _reaches(edges, "sales.ORDERS.order_key", "sales.DAILY_SALES.order_id")


def test_a_common_table_expression_is_walked_through():
    definition = (
        "create dynamic table D as with c as (select id from RAW.ORDERS) select c.id from c"
    )
    edges = _edges([_row("ORDERS", "ID"), *_dynamic("ID", definition=definition)])
    assert _reaches(edges, "sales.ORDERS.id", "sales.DAILY_SALES.id")


def test_an_expression_with_no_name_does_not_cost_the_columns_beside_it_their_lineage():
    definition = "create dynamic table D as select amount + 1, id from RAW.ORDERS"
    edges = _edges(
        [_row("ORDERS", "ID"), _row("ORDERS", "AMOUNT"), *_dynamic("ID", definition=definition)]
    )
    assert _reaches(edges, "sales.ORDERS.id", "sales.DAILY_SALES.id")


def test_a_definition_that_does_not_parse_contributes_nothing():
    from provisa.lineage.source_definitions import definition_views

    assert definition_views(_dynamic("ID", definition="create dynamic (((")) == []


def test_a_table_with_no_definition_contributes_nothing():
    from provisa.lineage.source_definitions import definition_views

    assert definition_views([_row("ORDERS", "ID")]) == []


@pytest.mark.asyncio
async def test_the_model_build_stores_the_edges_of_a_definition(control_plane):
    from sqlalchemy import insert, select

    from provisa.api.admin.lineage_router import write_lineage_edges
    from provisa.core.schema_org import (
        domains,
        lineage_edges,
        registered_tables,
        sources,
        table_columns,
    )

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales", description="sales"))
            await conn.execute_core(insert(sources).values(id=SF, type="snowflake", database="DW"))
            await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="postgres"))
            for table_id, schema, name, definition in (
                (1, "RAW", "ORDERS", None),
                (2, "ANALYTICS", "DAILY_SALES", DDL),
            ):
                await conn.execute_core(
                    insert(registered_tables).values(
                        id=table_id,
                        source_id=SF,
                        domain_id="sales",
                        schema_name=schema,
                        table_name=name,
                        source_definition=definition,
                    )
                )
            for table_id, column in ((1, "ID"), (1, "AMOUNT"), (2, "ORDER_ID"), (2, "AMOUNT")):
                await conn.execute_core(
                    insert(table_columns).values(
                        table_id=table_id,
                        domain_id="sales",
                        column_name=column,
                        data_type="numeric",
                    )
                )
            await write_lineage_edges(conn, SimpleNamespace(tracked_functions={}))
            e = lineage_edges
            rows = await conn.execute_core(
                select(e.c.source_table_id, e.c.source_column_name, e.c.target_column_name).where(
                    e.c.target_table_id == 2
                )
            )
            assert {tuple(r) for r in rows.fetchall()} == {
                (1, "ID", "ORDER_ID"),
                (1, "AMOUNT", "AMOUNT"),
            }
    finally:
        await db.close()
