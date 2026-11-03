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


#: The privilege functions asked of a predefined role, as the functions conformance run asks them,
#: with PostgreSQL 17's answers: its default grants (PUBLIC reads the system catalogs, executes
#: functions, uses types and languages, connects) and the predefined roles' own rights.
_PREDEFINED_ROLE_PRIVILEGES = [
    (
        "SELECT pg_catalog.\"has_any_column_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::regclass::oid, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_any_column_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_any_column_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::regclass::oid, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_any_column_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::regclass::oid, 'relname'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::regclass::oid, 2::smallint, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::text, 'relname'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::text, 2::smallint, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::regclass::oid, 'relname'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::regclass::oid, 2::smallint, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::text, 'relname'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_column_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::text, 2::smallint, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_database_privilege\"('pg_read_all_data'::name, (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()), 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_database_privilege\"('pg_read_all_data'::name, current_database()::text, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_database_privilege\"('pg_read_all_data'::regrole::oid, (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()), 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_database_privilege\"('pg_read_all_data'::regrole::oid, current_database()::text, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_function_privilege\"('pg_read_all_data'::regrole::oid, 'abs(integer)'::regprocedure::oid, 'EXECUTE'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_function_privilege\"('pg_read_all_data'::regrole::oid, 'pg_catalog.abs(integer)'::text, 'EXECUTE'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_language_privilege\"('pg_read_all_data'::regrole::oid, 'internal'::text, 'USAGE'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_language_privilege\"('pg_read_all_data'::regrole::oid, 12, 'USAGE'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_parameter_privilege\"('pg_monitor'::name, 'work_mem'::text, 'SET'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_parameter_privilege\"('pg_monitor'::regrole::oid, 'work_mem'::text, 'SET'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_schema_privilege\"('pg_read_all_data'::name, 'public'::regnamespace::oid, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_schema_privilege\"('pg_read_all_data'::name, 'public'::text, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_schema_privilege\"('pg_read_all_data'::regrole::oid, 'public'::regnamespace::oid, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_schema_privilege\"('pg_read_all_data'::regrole::oid, 'public'::text, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_table_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::regclass::oid, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_table_privilege\"('pg_monitor'::name, 'pg_catalog.pg_class'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_table_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::regclass::oid, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_table_privilege\"('pg_monitor'::regrole::oid, 'pg_catalog.pg_class'::text, 'SELECT'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_tablespace_privilege\"('pg_monitor'::name, 'pg_default'::text, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_tablespace_privilege\"('pg_monitor'::name, 1663, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_tablespace_privilege\"('pg_monitor'::regrole::oid, 'pg_default'::text, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_tablespace_privilege\"('pg_monitor'::regrole::oid, 1663, 'CREATE'::text) AS v",
        False,
    ),
    (
        "SELECT pg_catalog.\"has_type_privilege\"('pg_read_all_data'::regrole::oid, 'text'::regtype::oid, 'USAGE'::text) AS v",
        True,
    ),
    (
        "SELECT pg_catalog.\"has_type_privilege\"('pg_read_all_data'::regrole::oid, 'text'::text, 'USAGE'::text) AS v",
        True,
    ),
    ("SELECT has_table_privilege('pg_read_all_data', 'pg_catalog.pg_authid', 'SELECT')", True),
    ("SELECT has_table_privilege('pg_monitor', 'pg_catalog.pg_authid', 'SELECT')", False),
    ("SELECT has_table_privilege('pg_write_all_data', 'pg_catalog.pg_class', 'INSERT')", False),
    ("SELECT has_table_privilege('pg_monitor', 'pg_catalog.pg_class', 'INSERT, SELECT')", True),
    (
        "SELECT has_table_privilege('pg_monitor', 'pg_catalog.pg_class', "
        "'SELECT WITH GRANT OPTION')",
        False,
    ),
    ("SELECT has_database_privilege('pg_monitor', current_database(), 'CONNECT')", True),
    ("SELECT has_function_privilege('pg_monitor', 'pg_ls_logdir()', 'EXECUTE')", True),
    ("SELECT has_function_privilege('pg_read_all_data', 'pg_ls_logdir()', 'EXECUTE')", False),
    (
        "SELECT has_function_privilege('pg_monitor', "
        "'pg_catalog.pg_get_backend_memory_contexts()', 'EXECUTE')",
        True,
    ),
    ("SELECT has_schema_privilege('pg_monitor', 'pg_catalog', 'USAGE')", True),
    ("SELECT has_tablespace_privilege('pg_monitor', 'pg_global', 'CREATE')", False),
    ("SELECT has_parameter_privilege('pg_monitor', 'work_mem', 'ALTER SYSTEM')", False),
]


@pytest.mark.parametrize("sql,expected", _PREDEFINED_ROLE_PRIVILEGES)
def test_a_predefined_roles_privilege_is_what_postgresql_grants_it(sql, expected):
    assert _value(sql) is expected


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("c", []),
        ("r", ["pg_monitor=arwdDxtm/pg_monitor"]),
        ("s", ["pg_monitor=rwU/pg_monitor"]),
        ("d", ["=Tc/pg_monitor", "pg_monitor=CTc/pg_monitor"]),
        ("f", ["=X/pg_monitor", "pg_monitor=X/pg_monitor"]),
        ("l", ["=U/pg_monitor", "pg_monitor=U/pg_monitor"]),
        ("L", ["pg_monitor=rw/pg_monitor"]),
        ("n", ["pg_monitor=UC/pg_monitor"]),
        ("p", ["pg_monitor=sA/pg_monitor"]),
        ("t", ["pg_monitor=C/pg_monitor"]),
        ("F", ["pg_monitor=U/pg_monitor"]),
        ("S", ["pg_monitor=U/pg_monitor"]),
        ("T", ["=U/pg_monitor", "pg_monitor=U/pg_monitor"]),
    ],
)
def test_acldefault_is_postgresqls_default_acl(kind, expected):
    sql = f"SELECT pg_catalog.\"acldefault\"('{kind}'::\"char\", 'pg_monitor'::regrole::oid) AS v"
    assert _value(sql) == expected


def test_acldefault_of_an_oid_no_role_has_writes_the_number():
    assert _value("SELECT pg_catalog.acldefault('d', 99999) AS v") == [
        "=Tc/99999",
        "99999=CTc/99999",
    ]


@pytest.mark.parametrize(
    "sql,expected",
    [
        ('SELECT pg_catalog."pg_tablespace_location"(0::oid) AS v', ""),
        ("SELECT pg_catalog.pg_tablespace_location(1663) AS v", ""),
        ('SELECT pg_catalog."pg_identify_object"(NULL::oid, NULL::oid, NULL::integer) AS v', None),
        (
            "SELECT pg_catalog.\"pg_identify_object\"('pg_catalog.pg_class'::regclass::oid, "
            "'pg_catalog.pg_class'::regclass::oid, 0::integer) AS v",
            "(table,pg_catalog,pg_class,pg_catalog.pg_class)",
        ),
        (
            'SELECT pg_catalog."pg_identify_object_as_address"(NULL::oid, NULL::oid, '
            "NULL::integer) AS v",
            None,
        ),
        (
            "SELECT pg_catalog.\"pg_column_is_updatable\"('pg_catalog.pg_type'::regclass, "
            "(-7)::smallint, false::boolean) AS v",
            False,
        ),
        (
            "SELECT pg_catalog.\"pg_column_is_updatable\"('pg_catalog.pg_type'::regclass, "
            "1::smallint, false::boolean) AS v",
            True,
        ),
        ("SELECT pg_catalog.\"pg_input_is_valid\"('42'::text, 'integer'::text) AS v", True),
        (
            "SELECT pg_catalog.\"pg_input_error_info\"('x'::text, 'integer'::text) AS v",
            '("invalid input syntax for type integer: ""x""",,,22P02)',
        ),
        ("SELECT pg_catalog.pg_input_error_info('42', 'integer') AS v", "(,,,)"),
    ],
)
def test_a_catalog_function_answers_as_postgresql_17(sql, expected):
    assert _value(sql) == expected


def test_a_tablespace_no_catalog_lists_has_no_location():
    with pytest.raises(Exception, match='could not stat file "pg_tblspc/99999"'):
        _value("SELECT pg_catalog.pg_tablespace_location(99999) AS v")


_NOW = "'now()'::regprocedure::oid"
_MAKE_INTERVAL = (
    "'pg_catalog.make_interval(integer, integer, integer, integer, integer, integer, "
    "double precision)'::regprocedure::oid"
)


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT pg_catalog.\"to_regproc\"('now'::text) AS v", "now"),
        ("SELECT pg_catalog.to_regproc('abs') AS v", None),  # more than one: none
        ("SELECT pg_catalog.\"to_regprocedure\"('now()'::text) AS v", "now()"),
        ("SELECT pg_catalog.to_regprocedure('pg_catalog.abs(integer)') AS v", "abs(integer)"),
        (
            "SELECT pg_catalog.to_regprocedure('make_interval(integer, integer, integer, integer, "
            "integer, integer, double precision)') AS v",
            "make_interval(integer,integer,integer,integer,integer,integer,double precision)",
        ),
        (f'SELECT pg_catalog."pg_get_function_arguments"({_NOW}) AS v', ""),
        (f'SELECT pg_catalog."pg_get_function_identity_arguments"({_NOW}) AS v', ""),
        (f'SELECT pg_catalog."pg_get_function_result"({_NOW}) AS v', "timestamp with time zone"),
        (
            f'SELECT pg_catalog."pg_get_function_arg_default"({_MAKE_INTERVAL}, 1::integer) AS v',
            "0",
        ),
        (f"SELECT pg_catalog.pg_get_function_arg_default({_MAKE_INTERVAL}, 7) AS v", "0.0"),
        (
            "SELECT pg_catalog.pg_get_function_arguments('jsonb_path_query'::regproc) AS v",
            "target jsonb, path jsonpath, vars jsonb DEFAULT '{}'::jsonb, silent boolean "
            "DEFAULT false",
        ),
        (
            "SELECT pg_catalog.pg_get_function_identity_arguments('jsonb_path_query'::regproc) AS v",
            "target jsonb, path jsonpath, vars jsonb, silent boolean",
        ),
        (
            "SELECT pg_catalog.pg_get_function_result('jsonb_path_query'::regproc) AS v",
            "SETOF jsonb",
        ),
        (
            "SELECT pg_catalog.pg_get_function_arguments('pg_catalog.percentile_cont(double "
            "precision, interval)'::regprocedure) AS v",
            "double precision ORDER BY interval",
        ),
        (
            f'SELECT pg_catalog."pg_get_functiondef"({_NOW}) AS v',
            (
                "CREATE OR REPLACE FUNCTION pg_catalog.now()\n RETURNS timestamp with time zone\n"
                " LANGUAGE internal\n STABLE PARALLEL SAFE STRICT\nAS $function$now$function$\n"
            ),
        ),
        (f'SELECT pg_catalog."obj_description"({_NOW}) AS v', "current transaction time"),
        (
            f"SELECT pg_catalog.\"obj_description\"({_NOW}, 'pg_proc'::name) AS v",
            "current transaction time",
        ),
        (
            "SELECT pg_catalog.\"pg_describe_object\"('pg_catalog.pg_proc'::regclass::oid, "
            f"{_NOW}, 0::integer) AS v",
            "function now()",
        ),
        (f'SELECT pg_catalog."fmgr_internal_validator"({_NOW}) AS v', ""),
        (
            "SELECT pg_catalog.\"pg_get_object_address\"('function'::text, "
            "'{pg_catalog,now}'::text[], '{}'::text[]) AS v",
            "(1255,1299,0)",
        ),
    ],
)
def test_a_function_postgresql_ships_is_described_as_postgresql_17_describes_it(sql, expected):
    assert _value(sql) == expected


@pytest.mark.parametrize(
    "signature,md5",
    [
        ("pg_catalog.ts_debug(regconfig,text)", "881ff946ce37d59d764181100be8e66c"),
        ("pg_catalog.make_interval(integer,integer,integer,integer,integer,integer,"
         "double precision)", "34b9009000108688a1c8b6c3baf7634b"),
        ("pg_catalog.koi8r_to_mic(integer,integer,cstring,internal,integer,boolean)",
         "265526ad6d36536e0a508c6f278882e4"),
        ("pg_catalog.jsonb_path_query(jsonb,jsonpath,jsonb,boolean)",
         "6e06258e1b30b23d8c97366e4ba90d3c"),
    ],
)  # fmt: skip
def test_a_functions_definition_is_postgresql_17s(signature, md5):
    import hashlib

    written = _value(f"SELECT pg_catalog.pg_get_functiondef('{signature}'::regprocedure) AS v")
    assert hashlib.md5(written.encode()).hexdigest() == md5


def test_an_aggregates_definition_is_refused_as_postgresql_refuses_it():
    with pytest.raises(Exception, match='"max" is an aggregate function'):
        _value("SELECT pg_catalog.pg_get_functiondef('pg_catalog.max(integer)'::regprocedure) AS v")


def test_the_catalogs_foreign_keys_are_postgresql_17s():
    rows = _rows('SELECT pg_catalog."pg_get_catalog_foreign_keys"() AS v')
    assert len(rows) == 219
    assert rows[0] == ("(pg_proc,{pronamespace},pg_namespace,{oid},f,f)",)


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT pg_catalog.\"pg_settings_get_flags\"('work_mem'::text) AS v", ["EXPLAIN"]),
        ("SELECT pg_catalog.pg_settings_get_flags('no_such_setting') AS v", None),
    ],
)
def test_a_settings_flags_are_postgresql_17s(sql, expected):
    assert _value(sql) == expected


_PLAIN, _PRETTY = "2123afb3b5613547675b418be66ea27a", "30a9194c13d239c08c847be8ce92a298"


@pytest.mark.parametrize(
    "call,md5",
    [
        ("'pg_catalog.pg_tables'::regclass::oid", _PLAIN),
        ("'pg_catalog.pg_tables'::regclass::oid, true::boolean", _PRETTY),
        ("'pg_catalog.pg_tables'::regclass::oid, 0::integer", _PRETTY),
        ("'pg_catalog.pg_tables'::text", _PLAIN),
        ("'pg_catalog.pg_tables'::text, true::boolean", _PRETTY),
        ("'pg_catalog.pg_tables'::text, false", _PLAIN),
        ("'information_schema.tables'::text, true", "acdc6eb1ff8215f525200f6742477ae0"),
    ],
)
def test_a_system_views_definition_is_postgresql_17s(call, md5):
    import hashlib

    written = _value(f'SELECT pg_catalog."pg_get_viewdef"({call}) AS v')
    assert hashlib.md5(written.encode()).hexdigest() == md5


def test_a_relation_that_is_no_view_has_no_definition():
    assert (
        _value("SELECT pg_catalog.pg_get_viewdef('pg_catalog.pg_class'::regclass::oid) AS v")
        is None
    )
