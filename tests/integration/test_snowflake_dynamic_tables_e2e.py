# Copyright (c) 2026 Kenneth Stott
# Canary: c47a1e9b-3d82-4f06-a5b7-1e6d0c3f8a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a live Snowflake source's Dynamic Table and Task, as a table and commands (REQ-2230).

The warehouse lane's account. A database of the run's own holds a table, a Dynamic Table over it
and a Task. What the source offers is read through the real pool, the definition is recorded and
read for lineage, and each operation runs on the source through the command dispatcher.
"""

# Requirements: REQ-2230

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from tests.env_creds import unset

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

pytest.importorskip("snowflake.connector", reason="snowflake-connector-python not installed")

_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(pytest.mark.skipif(not _HAVE_CREDS, reason=f"not set: {unset(*_ENV)}"))

from provisa.executor.pool import SourcePool  # noqa: E402

from tests.integration.live_teardown import run_name  # noqa: E402

_SID = "sf_dynamic_e2e"
_DB = run_name("PROVISA_DYNAMIC_E2E").upper()
_SCHEMA = "PUBLIC"
_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")


@pytest.fixture(scope="module", autouse=True)
def _warehouse_left_as_found():
    """The statements here resume the warehouse if it is suspended; it is suspended again after
    the module however it ends, unless it was running before (REQ-254)."""
    from tests.integration.snowflake_warehouse import warehouse_in_use

    with warehouse_in_use(_WAREHOUSE):
        yield


@pytest.fixture
def seeded():
    import snowflake.connector as sf

    from tests.integration.live_teardown import run_every
    from tests.integration.snowflake_warehouse import drop_database

    conn = sf.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=_WAREHOUSE,
    )
    try:
        cur = conn.cursor()
        try:
            for statement in (
                f"CREATE DATABASE IF NOT EXISTS {_DB}",
                f"CREATE SCHEMA IF NOT EXISTS {_DB}.{_SCHEMA}",
                f"CREATE OR REPLACE TABLE {_DB}.{_SCHEMA}.ORDERS (ID NUMBER, AMOUNT NUMBER)",
                f"INSERT INTO {_DB}.{_SCHEMA}.ORDERS VALUES (1, 10), (2, 20)",
                f"CREATE OR REPLACE DYNAMIC TABLE {_DB}.{_SCHEMA}.DAILY_SALES "
                f"TARGET_LAG = '1 hour' WAREHOUSE = {_WAREHOUSE} "
                f"AS SELECT ID AS ORDER_ID, AMOUNT FROM {_DB}.{_SCHEMA}.ORDERS",
                f"CREATE OR REPLACE TASK {_DB}.{_SCHEMA}.NIGHTLY WAREHOUSE = {_WAREHOUSE} "
                "SCHEDULE = '60 MINUTE' AS SELECT 1",
            ):
                cur.execute(statement)
        finally:
            cur.close()
        yield conn
    finally:
        run_every(
            [
                (f"drop database {_DB}", lambda: drop_database(conn, _DB)),
                ("close the connection", conn.close),
            ]
        )


def _show(conn, what: str, name: str) -> dict:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW {what} IN SCHEMA {_DB}.{_SCHEMA}")
        columns = [d[0].lower() for d in cur.description]
        return next(
            row for row in (dict(zip(columns, r)) for r in cur.fetchall()) if row["name"] == name
        )
    finally:
        cur.close()


def _rows_in(conn, table: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {_DB}.{_SCHEMA}.{table}")
        return cur.fetchone()[0]
    finally:
        cur.close()


def _command(function_name: str) -> dict:
    return {
        "name": function_name.replace(".", "_").lower(),
        "source_id": _SID,
        "schema_name": _SCHEMA,
        "function_name": function_name,
        "impl_kind": "source_operation",
        "kind": "mutation",
        "arguments": [],
    }


@pytest.mark.asyncio
async def test_a_dynamic_table_and_a_task_are_offered_and_run(seeded, monkeypatch):
    from provisa.api.admin.introspect import native_tables, record_source_definition
    from provisa.executor.function_dispatch import dispatch_function
    from provisa.executor.source_operation import offered_operations
    from provisa.lineage.source_definitions import definition_views
    from provisa.otel_compat import MemoryUdfTraceSink

    pool = SourcePool()
    await pool.add(
        source_id=_SID,
        source_type="snowflake",
        host=os.environ["SNOWFLAKE_ACCOUNT"],
        port=443,
        database=_DB,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        extra={"account": os.environ["SNOWFLAKE_ACCOUNT"], "warehouse": _WAREHOUSE},
    )
    state = SimpleNamespace(multitenancy=False, source_types={_SID: "snowflake"}, source_pools=pool)
    monkeypatch.setattr("provisa.api.app.state", state)
    try:
        # What a steward views: the Dynamic Table as a table, the operations as commands.
        offered = await native_tables(_SID, "snowflake", _SCHEMA, pool, None, state)  # type: ignore[arg-type]
        assert {t.name: t.comment for t in offered or []} == {
            "DAILY_SALES": "DYNAMIC TABLE",
            "ORDERS": None,
        }
        operations = await offered_operations(state, _SID, _SCHEMA)
        assert sorted(op.name for op in operations or []) == [
            "execute_task.NIGHTLY",
            "refresh_dynamic_table.DAILY_SALES",
            "resume_dynamic_table.DAILY_SALES",
            "resume_task.NIGHTLY",
            "suspend_dynamic_table.DAILY_SALES",
            "suspend_task.NIGHTLY",
        ]

        # Its definition, as Snowflake reports it, contributes its lineage.
        model = SimpleNamespace(
            source_id=_SID, schema_name=_SCHEMA, table_name="DAILY_SALES", source_definition=None
        )
        await record_source_definition(model, "snowflake")
        common = {"source_id": _SID, "database": _DB, "domain_id": "sales", "alias": None}
        views = definition_views(
            [
                {
                    **common,
                    "schema_name": _SCHEMA,
                    "table_name": "ORDERS",
                    "source_definition": None,
                    "column_name": "ID",
                },
                {
                    **common,
                    "schema_name": _SCHEMA,
                    "table_name": "DAILY_SALES",
                    "source_definition": model.source_definition,
                    "column_name": "ORDER_ID",
                },
            ]
        )
        assert '"sales"."ORDERS"' in views[0]["view_sql"]

        # A refresh brings in a row the table did not hold, and is recorded.
        cur = seeded.cursor()
        try:
            cur.execute(f"INSERT INTO {_DB}.{_SCHEMA}.ORDERS VALUES (3, 30)")
        finally:
            cur.close()
        sink = MemoryUdfTraceSink()
        refresh = _command("refresh_dynamic_table.DAILY_SALES")
        await dispatch_function(refresh, {}, state, "dev", trace_sink=sink)
        assert _rows_in(seeded, "DAILY_SALES") == 3
        assert [(t.udf_name, t.role_id) for t in sink.records] == [(refresh["name"], "dev")]

        suspend = _command("suspend_dynamic_table.DAILY_SALES")
        await dispatch_function(suspend, {}, state, "dev")
        assert _show(seeded, "DYNAMIC TABLES", "DAILY_SALES")["scheduling_state"] == "SUSPENDED"
        resume = _command("resume_dynamic_table.DAILY_SALES")
        await dispatch_function(resume, {}, state, "dev")
        assert _show(seeded, "DYNAMIC TABLES", "DAILY_SALES")["scheduling_state"] == "ACTIVE"

        await dispatch_function(_command("resume_task.NIGHTLY"), {}, state, "dev")
        assert _show(seeded, "TASKS", "NIGHTLY")["state"] == "started"
        await dispatch_function(_command("suspend_task.NIGHTLY"), {}, state, "dev")
        assert _show(seeded, "TASKS", "NIGHTLY")["state"] == "suspended"
        answered = await dispatch_function(_command("execute_task.NIGHTLY"), {}, state, "dev")
        assert answered  # Snowflake answers an execution with the statement's status
    finally:
        await pool.close_all()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
