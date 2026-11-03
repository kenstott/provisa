# Copyright (c) 2026 Kenneth Stott
# Canary: 4d7b2e91-6c3a-4f08-b5e2-91a7c0d3f6e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pg_catalog function the meta domain answers gives PostgreSQL 17's answer about what
PostgreSQL itself ships (REQ-2190, META IS A SUPERSET OF THE CATALOG; REQ-2167, PGWIRE'S CATALOG
FUNCTIONS): its predefined roles and their memberships, a strict function's NULL, and a
statistic of a server where nothing has run. Each expected value is PostgreSQL 17.11's answer to
the statement, as the functions conformance run (function_support.py) asks it."""

# Requirements: REQ-2190, REQ-2167

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.pgwire.catalog import answer

#: The functions conformance run's engine: a statement whose forms read no table runs on PostgreSQL.
pytestmark = pytest.mark.postgres_engine

_ROLE = "testrole"


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {_ROLE: ctx}
    state.roles = {_ROLE: {"id": _ROLE, "capabilities": []}}
    state.schema_build_cache = {"column_types": {}, "tables": []}
    return state


def _rows(sql: str) -> list[tuple]:
    return [tuple(r) for r in answer(sql, _ROLE, _state()).rows]


def _value(sql: str):
    ((value,),) = _rows(sql)
    return bytes(value) if isinstance(value, memoryview) else value


_COUNTED = (
    "analyze_count autoanalyze_count autovacuum_count blocks_fetched blocks_hit dead_tuples "
    "function_calls function_self_time function_total_time ins_since_vacuum live_tuples "
    "mod_since_analyze numscans tuples_deleted tuples_fetched tuples_hot_updated tuples_inserted "
    "tuples_newpage_updated tuples_returned tuples_updated vacuum_count xact_blocks_fetched "
    "xact_blocks_hit xact_function_calls xact_function_self_time xact_function_total_time "
    "xact_numscans xact_tuples_deleted xact_tuples_fetched xact_tuples_hot_updated "
    "xact_tuples_inserted xact_tuples_newpage_updated xact_tuples_returned xact_tuples_updated "
    "db_active_time db_blk_read_time db_blk_write_time db_blocks_fetched db_blocks_hit "
    "db_checksum_failures db_conflict_all db_conflict_bufferpin db_conflict_lock "
    "db_conflict_logicalslot db_conflict_snapshot db_conflict_startup_deadlock "
    "db_conflict_tablespace db_deadlocks db_idle_in_transaction_time db_numbackends "
    "db_session_time db_sessions db_sessions_abandoned db_sessions_fatal db_sessions_killed "
    "db_temp_bytes db_temp_files db_tuples_deleted db_tuples_fetched db_tuples_inserted "
    "db_tuples_returned db_tuples_updated db_xact_commit db_xact_rollback"
).split()
_OF_A_BACKEND = ("backend_client_port", "backend_dbid", "backend_pid", "backend_userid")


@pytest.mark.parametrize(
    "sql",
    [f'SELECT pg_catalog."pg_stat_get_{n}"(NULL::oid) AS v' for n in _COUNTED]
    + [f'SELECT pg_catalog."pg_stat_get_{n}"(NULL::integer) AS v' for n in _OF_A_BACKEND],
)
def test_a_statistic_of_null_is_null(sql):
    assert _value(sql) is None


def test_a_counted_statistic_of_an_object_is_still_zero():
    assert _value("SELECT pg_catalog.pg_stat_get_numscans(1259::oid) AS v") == 0


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."pg_stat_get_progress_info"(NULL::text) AS v',
        "SELECT pg_catalog.pg_stat_get_progress_info('VACUUM') AS v",
        'SELECT pg_catalog."pg_stat_get_subscription"(NULL::oid) AS v',
        "SELECT pg_catalog.pg_stat_get_subscription(12345::oid) AS v",
    ],
)
def test_no_command_is_in_progress_and_no_subscription_worker_runs(sql):
    assert _rows(sql) == []


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT pg_catalog.\"pg_stat_get_replication_slot\"('abc'::text) AS v",
            "(abc,0,0,0,0,0,0,0,0,)",
        ),
        (
            'SELECT pg_catalog."pg_stat_get_subscription_stats"(12345::oid) AS v',
            "(12345,0,0,)",
        ),
    ],
)
def test_a_slots_or_a_subscriptions_counts_are_zero_with_no_reset(sql, expected):
    assert _value(sql) == expected


_STRICT = [
    'SELECT pg_catalog."pg_has_role"(NULL::name, NULL::name, NULL::text) AS v',
    'SELECT pg_catalog."pg_has_role"(NULL::name, NULL::oid, NULL::text) AS v',
    'SELECT pg_catalog."pg_has_role"(NULL::oid, NULL::text) AS v',
    'SELECT pg_catalog."pg_has_role"(NULL::name, NULL::text) AS v',
    *(
        f'SELECT pg_catalog."pg_{kind}_is_visible"(NULL::oid) AS v'
        for kind in (
            "collation conversion function opclass operator opfamily statistics_obj ts_config "
            "ts_dict ts_parser ts_template type"
        ).split()
    ),
    'SELECT pg_catalog."pg_is_other_temp_schema"(NULL::oid) AS v',
    'SELECT pg_catalog."pg_relation_is_publishable"(NULL::regclass) AS v',
]


@pytest.mark.parametrize("sql", _STRICT)
def test_a_strict_catalog_function_of_null_is_null(sql):
    assert _value(sql) is None


def test_a_relation_in_no_partition_tree_has_no_root():
    assert (
        _value("SELECT pg_catalog.\"pg_partition_root\"('pg_catalog.pg_class'::regclass) AS v")
        is None
    )


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT pg_catalog.\"pg_get_userbyid\"('pg_monitor'::regrole::oid) AS v", "pg_monitor"),
        ("SELECT pg_catalog.pg_get_userbyid(6181) AS v", "pg_read_all_data"),
        ("SELECT pg_catalog.pg_get_userbyid(10) AS v", "provisa"),
        ("SELECT pg_catalog.pg_get_userbyid(424242) AS v", "unknown (OID=424242)"),
        ("SELECT pg_catalog.\"to_regrole\"('pg_read_all_data'::text) AS v", "pg_read_all_data"),
        ("SELECT pg_catalog.\"to_regrole\"('pg_maintain'::text) AS v", "pg_maintain"),
        ("SELECT pg_catalog.\"regroleout\"('pg_monitor'::regrole) AS v", "pg_monitor"),
        ("SELECT pg_catalog.\"regrolesend\"('pg_monitor'::regrole) AS v", b"\x00\x00\x0d\x2d"),
        (
            "SELECT pg_catalog.\"makeaclitem\"('pg_read_all_data'::regrole::oid, "
            "'pg_read_all_data'::regrole::oid, 'INSERT'::text, false::boolean) AS v",
            "pg_read_all_data=a/pg_read_all_data",
        ),
        (
            "SELECT pg_catalog.\"pg_has_role\"('pg_monitor'::name, 'pg_read_all_stats'::name, "
            "'MEMBER'::text) AS v",
            True,
        ),
        (
            "SELECT pg_catalog.\"pg_has_role\"('pg_read_all_stats'::name, 'pg_monitor'::name, "
            "'MEMBER'::text) AS v",
            False,
        ),
        (
            "SELECT pg_catalog.\"pg_has_role\"('pg_monitor'::regrole::oid, "
            "'pg_stat_scan_tables'::regrole::oid, 'USAGE'::text) AS v",
            True,
        ),
    ],
)
def test_postgresqls_predefined_roles_are_listed(sql, expected):
    assert _value(sql) == expected


def test_every_predefined_role_of_postgresql_17_is_listed():
    assert _rows(
        "SELECT oid, rolname FROM pg_catalog.pg_roles WHERE rolname LIKE 'pg\\_%' ORDER BY oid"
    ) == [
        (3373, "pg_monitor"),
        (3374, "pg_read_all_settings"),
        (3375, "pg_read_all_stats"),
        (3377, "pg_stat_scan_tables"),
        (4200, "pg_signal_backend"),
        (4544, "pg_checkpoint"),
        (4550, "pg_use_reserved_connections"),
        (4569, "pg_read_server_files"),
        (4570, "pg_write_server_files"),
        (4571, "pg_execute_server_program"),
        (6171, "pg_database_owner"),
        (6181, "pg_read_all_data"),
        (6182, "pg_write_all_data"),
        (6304, "pg_create_subscription"),
        (6337, "pg_maintain"),
    ]


def test_pg_monitors_memberships_are_listed():
    assert _rows(
        "SELECT roleid, member, grantor, admin_option, inherit_option, set_option "
        "FROM pg_catalog.pg_auth_members WHERE member = 3373 ORDER BY roleid"
    ) == [
        (3374, 3373, 10, False, True, True),
        (3375, 3373, 10, False, True, True),
        (3377, 3373, 10, False, True, True),
    ]


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT pg_catalog.\"regnamespaceout\"('information_schema'::regnamespace) AS v",
            "information_schema",
        ),
        (
            "SELECT pg_catalog.\"regnamespacesend\"('information_schema'::regnamespace) AS v",
            b"\x00\x00\x33\xe3",
        ),
        (
            "SELECT pg_catalog.\"regclassout\"('information_schema.tables'::regclass) AS v",
            "information_schema.tables",
        ),
        ("SELECT pg_catalog.\"regclassout\"('pg_catalog.pg_class'::regclass) AS v", "pg_class"),
        ("SELECT pg_catalog.\"regtypeout\"('integer'::regtype) AS v", "integer"),
        ('SELECT pg_catalog."to_regcollation"(\'"C"\'::text) AS v', '"C"'),
        ("SELECT pg_catalog.\"to_regcollation\"('C'::text) AS v", None),
    ],
)
def test_an_object_identifier_is_written_by_its_name_in_meta(sql, expected):
    assert _value(sql) == expected


def test_information_schema_has_postgresql_17s_oid():
    assert _rows(
        "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'information_schema'"
    ) == [(13283,)]


def test_regclass_of_a_name_given_as_text_is_that_relation():
    # A regclass result is sent with its oid and its name together (catalog_route._result_regs).
    assert _value("SELECT pg_catalog.\"regclass\"('pg_catalog.pg_class'::text) AS v") == {
        "oid": 1259,
        "name": "pg_class",
    }
