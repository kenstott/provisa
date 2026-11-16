# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0e2f7a-93c4-4d18-a5e1-7f2c8d4b1a36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A CATALOG FUNCTION'S ARGUMENT AT ITS EDGE (REQ-2236, REQ-2167): a function the catalog answers
reads its argument as PostgreSQL reads it. An oid literal past the type is oidin's error, SQLSTATE
22003, whichever function it is given to; a NULL argument of a strict function is NULL. Each
statement is the one the functions measure runs (Conformance (functions) run 43861326336), and
each answer PostgreSQL 17's: oidin (src/backend/utils/adt/oid.c, uint32in_subr) and pg_proc's
proisstrict."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.core.pg_error import PostgresError
from provisa.pgwire.catalog import classify
from provisa.pgwire.catalog_route import CatalogRequest, addressed_statement
from provisa.transpiler.metadata_facts import catalog_facts
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

TEXT = 25
PAST_OID = "4294967296"  # 2^32: one past the greatest oid

#: The calls of an oid past the type that the catalog answers, as the measure writes them.
_OID_CALLS = (
    "pg_get_partition_constraintdef({oid})",
    "pg_get_partkeydef({oid})",
    "pg_get_ruledef({oid})",
    "pg_get_ruledef({oid}, true::boolean)",
    "pg_get_statisticsobjdef({oid})",
    "pg_get_statisticsobjdef_columns({oid})",
    "pg_get_statisticsobjdef_expressions({oid})",
    "pg_get_triggerdef({oid})",
    "pg_get_triggerdef({oid}, true::boolean)",
    "pg_indexam_progress_phasename({oid}, 3::bigint)",
    "pg_is_other_temp_schema({oid})",
    "pg_stat_get_analyze_count({oid})",
    "pg_stat_get_autoanalyze_count({oid})",
    "pg_stat_get_autovacuum_count({oid})",
    "pg_stat_get_blocks_fetched({oid})",
    "pg_stat_get_blocks_hit({oid})",
    "pg_stat_get_dead_tuples({oid})",
    "pg_stat_get_function_calls({oid})",
    "pg_stat_get_function_self_time({oid})",
    "pg_stat_get_function_total_time({oid})",
    "pg_stat_get_ins_since_vacuum({oid})",
    "pg_stat_get_last_analyze_time({oid})",
    "pg_stat_get_last_autoanalyze_time({oid})",
    "pg_stat_get_last_autovacuum_time({oid})",
    "pg_stat_get_last_vacuum_time({oid})",
    "pg_stat_get_lastscan({oid})",
    "pg_stat_get_live_tuples({oid})",
    "pg_stat_get_mod_since_analyze({oid})",
    "pg_stat_get_subscription({oid})",
    "pg_stat_get_subscription_stats({oid})",
    "pg_stat_get_tuples_deleted({oid})",
    "pg_stat_get_tuples_fetched({oid})",
    "pg_stat_get_tuples_hot_updated({oid})",
    "pg_stat_get_tuples_inserted({oid})",
    "pg_stat_get_tuples_newpage_updated({oid})",
    "pg_stat_get_tuples_returned({oid})",
    "pg_stat_get_tuples_updated({oid})",
    "pg_stat_get_vacuum_count({oid})",
    "row_security_active({oid})",
)

#: The calls of a NULL boolean to a strict function the catalog answers, as the measure writes
#: them. Each has a value where the boolean is one, so NULL is the argument's doing.
_NULL_CALLS = (
    "\"current_setting\"('TimeZone'::text, {flag})",
    '"pg_get_constraintdef"((SELECT oid FROM pg_catalog.pg_constraint '
    "WHERE conname = 'pg_class_oid_index'), {flag})",
    "\"pg_get_indexdef\"('pg_catalog.pg_class_oid_index'::regclass::oid, 0::integer, {flag})",
)


def _measured(call: str, **values: str) -> str:
    """``call`` as the measure writes it: pg_catalog's, quoted, one column, bounded."""
    name, rest = call.split("(", 1)
    return f'SELECT pg_catalog."{name.strip(chr(34))}"({rest.format(**values)} AS v LIMIT 1001'


def _run(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


def _value(res: Any) -> str | None:
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.ntuples == 1
    return None if (v := res.get_value(0, 0)) is None else v.decode()


_REQUEST = CatalogRequest(
    role_id="reader",
    database="sales_db",
    facts=catalog_facts("reader", {1: {"SELECT"}}),
    settings={"server_version": "17.0", "timezone": "UTC"},
    pid=4242,
)


@pytest.mark.parametrize("call", _OID_CALLS)
def test_an_oid_past_the_type_is_oidins_error(conn: Any, call: str):
    sql = _measured(call, oid=f"'{PAST_OID}'::oid")
    assert classify(sql) == "INTERCEPT"  # the catalog answers it, on every engine
    res = _run(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert res.error_field(pq.DiagnosticField.SQLSTATE) == b"22003"
    assert res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) == (
        f'value "{PAST_OID}" is out of range for type oid'.encode()
    )


@pytest.mark.parametrize("call", _OID_CALLS)
def test_the_oids_error_is_raised_as_the_statement_is_addressed(call: str):
    """Before any engine has the statement: the same error whichever engine serves."""
    with pytest.raises(
        PostgresError, match=f'value "{PAST_OID}" is out of range for type oid'
    ) as e:
        addressed_statement(_measured(call, oid=f"'{PAST_OID}'::oid"), _REQUEST)
    assert e.value.sqlstate == "22003"


@pytest.mark.parametrize(
    ("written", "raises"),
    [
        ("4294967295", False),  # the greatest oid
        ("4294967296", True),
        ("0", False),
        ("-1", False),  # oidin reads a negative int4 as its unsigned value
        ("-2147483648", False),
        ("-2147483649", True),
        (" 4294967296 ", True),  # oidin skips the spaces and says the text as written
        ("99999999999999999999999", True),  # past strtoul itself
    ],
)
def test_oidin_bounds(written: str, raises: bool):
    sql = f"SELECT pg_catalog.\"pg_get_ruledef\"('{written}'::oid) AS v"
    if not raises:
        addressed_statement(sql, _REQUEST)
        return
    with pytest.raises(PostgresError) as e:
        addressed_statement(sql, _REQUEST)
    assert (e.value.sqlstate, str(e.value)) == (
        "22003",
        f'value "{written}" is out of range for type oid',
    )


@pytest.mark.parametrize("reg", ["regclass", "regtype", "regnamespace", "regrole"])
def test_an_object_identifier_types_input_reads_digits_as_oidin_does(reg: str):
    """regclassin and its kin read a string of digits with oidin: its error names oid."""
    with pytest.raises(
        PostgresError, match=f'value "{PAST_OID}" is out of range for type oid'
    ) as e:
        addressed_statement(f"SELECT '{PAST_OID}'::{reg} AS v", _REQUEST)
    assert e.value.sqlstate == "22003"


def test_an_oid_in_a_string_that_is_no_cast_is_text():
    assert PAST_OID in addressed_statement(f"SELECT '{PAST_OID}'::text AS v", _REQUEST)


@pytest.mark.parametrize("call", _NULL_CALLS)
def test_a_null_boolean_to_a_strict_function_is_null(conn: Any, call: str):
    given = _run(conn, _measured(call, flag="true::boolean"))
    assert _value(given) is not None  # the call has an answer, so NULL is the argument's
    sql = _measured(call, flag="NULL::boolean")
    assert classify(sql) == "INTERCEPT"
    res = _run(conn, sql)
    assert _value(res) is None
    assert res.ftype(0) == TEXT


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_get_constraintdef(NULL::oid) AS v",
        "SELECT pg_get_constraintdef(NULL, true) AS v",
        "SELECT pg_get_indexdef(NULL::oid) AS v",
        "SELECT pg_get_indexdef(1::oid, NULL::integer, true) AS v",
        "SELECT pg_get_expr(NULL::pg_node_tree, 1::oid) AS v",
        "SELECT pg_get_expr('x'::pg_node_tree, 1::oid, NULL::boolean) AS v",
        "SELECT pg_get_viewdef(1::oid, NULL::boolean) AS v",
        "SELECT pg_get_function_arg_default(1::oid, NULL::integer) AS v",
        "SELECT current_setting(NULL::text) AS v",
        "SELECT current_setting(NULL::text, true) AS v",
        "SELECT current_setting('TimeZone', NULL) AS v",
    ],
)
def test_a_constant_null_argument_of_a_strict_function_is_written_as_null(sql: str):
    """Strict in pg_proc, so NULL without the form: nothing is read for an answer that is NULL."""
    assert addressed_statement(sql, _REQUEST) == "SELECT CAST(NULL AS TEXT) AS v"


def test_format_type_is_not_strict_and_keeps_its_form():
    """format_type(oid, NULL) is the type's name: pg_proc says it is not strict."""
    assert "CAST(NULL AS TEXT)" != addressed_statement(
        "SELECT format_type(23, NULL) AS v", _REQUEST
    ).removeprefix("SELECT ").removesuffix(" AS v")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_create_restore_point(NULL::text) AS v",  # pg_lsn
        "SELECT to_regclass(NULL::text) AS v",  # regclass
        "SELECT makeaclitem(NULL::oid, 1::oid, 'SELECT', false) AS v",  # aclitem
    ],
)
def test_a_server_forms_null_is_of_the_type_its_engine_holds(sql: str):
    """A server form is strict in its own SQL, in a type every engine has: never PostgreSQL's
    name of a type an engine lacks."""
    addressed = addressed_statement(sql, _REQUEST).upper()
    assert not any(named in addressed for named in ("PG_LSN", "REGCLASS", "ACLITEM"))
