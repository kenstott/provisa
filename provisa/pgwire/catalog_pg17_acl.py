# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2c9e41-3d58-4b06-a1f7-c84e2b5d9036
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL 17's predefined roles, and what it grants beyond its defaults on what it ships, from
its catalog (REQ-2190, META IS A SUPERSET OF THE CATALOG): the roles and their memberships, the
system relations PUBLIC may not read, and the functions whose EXECUTE is revoked from PUBLIC with
the predefined roles it is granted to. Every other system relation is PUBLIC's to read, and every
other function PUBLIC's to execute. Read from PostgreSQL 17.11 with:

    SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('pg_catalog', 'information_schema') AND c.relkind IN ('r', 'v', 'm', 'p')
      AND NOT has_table_privilege('pg_monitor', c.oid, 'SELECT');
    SELECT p.oid, p.oid::regprocedure, <EXECUTE grantees of p.proacl but the bootstrap role>
    FROM pg_proc p WHERE p.proacl IS NOT NULL AND p.pronamespace = 11;"""

# Requirements: REQ-2190

from __future__ import annotations

#: PostgreSQL 17's predefined roles, as its pg_authid lists them (REQ-2190, META IS A SUPERSET OF THE
#: CATALOG): none is a superuser or logs in, and each inherits.
PG17_ROLES: tuple[tuple[int, str, bool, bool, bool, bool, bool, bool, int, bool], ...] = (
    # OID, name, super, inherit, createrole, createdb, canlogin, replication, connlimit, bypassrls
    (3373, "pg_monitor", False, True, False, False, False, False, -1, False),
    (3374, "pg_read_all_settings", False, True, False, False, False, False, -1, False),
    (3375, "pg_read_all_stats", False, True, False, False, False, False, -1, False),
    (3377, "pg_stat_scan_tables", False, True, False, False, False, False, -1, False),
    (4200, "pg_signal_backend", False, True, False, False, False, False, -1, False),
    (4544, "pg_checkpoint", False, True, False, False, False, False, -1, False),
    (4550, "pg_use_reserved_connections", False, True, False, False, False, False, -1, False),
    (4569, "pg_read_server_files", False, True, False, False, False, False, -1, False),
    (4570, "pg_write_server_files", False, True, False, False, False, False, -1, False),
    (4571, "pg_execute_server_program", False, True, False, False, False, False, -1, False),
    (6171, "pg_database_owner", False, True, False, False, False, False, -1, False),
    (6181, "pg_read_all_data", False, True, False, False, False, False, -1, False),
    (6182, "pg_write_all_data", False, True, False, False, False, False, -1, False),
    (6304, "pg_create_subscription", False, True, False, False, False, False, -1, False),
    (6337, "pg_maintain", False, True, False, False, False, False, -1, False),
)

#: PostgreSQL 17's memberships among them: pg_monitor is a member of the three roles it gathers.
#: (oid, roleid, member, grantor, admin_option, inherit_option, set_option)
PG17_ROLE_MEMBERS: tuple[tuple[int, int, int, int, bool, bool, bool], ...] = (
    (10226, 3374, 3373, 10, False, True, True),
    (10227, 3375, 3373, 10, False, True, True),
    (10228, 3377, 3373, 10, False, True, True),
)

#: The system relations PUBLIC holds no SELECT on.
NO_PUBLIC_SELECT: frozenset[tuple[str, str]] = frozenset(
    {
        ("information_schema", "_pg_foreign_data_wrappers"),
        ("information_schema", "_pg_foreign_servers"),
        ("information_schema", "_pg_foreign_table_columns"),
        ("information_schema", "_pg_foreign_tables"),
        ("information_schema", "_pg_user_mappings"),
        ("information_schema", "sql_parts"),
        ("information_schema", "transforms"),
        ("pg_catalog", "pg_authid"),
        ("pg_catalog", "pg_config"),
        ("pg_catalog", "pg_file_settings"),
        ("pg_catalog", "pg_hba_file_rules"),
        ("pg_catalog", "pg_ident_file_mappings"),
        ("pg_catalog", "pg_largeobject"),
        ("pg_catalog", "pg_replication_origin_status"),
        ("pg_catalog", "pg_shadow"),
        ("pg_catalog", "pg_statistic"),
        ("pg_catalog", "pg_statistic_ext_data"),
        ("pg_catalog", "pg_subscription"),
        ("pg_catalog", "pg_user_mapping"),
    }
)

#: The functions whose EXECUTE PUBLIC does not hold: oid, regprocedure's text, the predefined roles
#: granted it (the bootstrap role, which holds every one, aside).
REVOKED_FUNCTIONS: tuple[tuple[int, str, tuple[str, ...]], ...] = (
    (764, "lo_import(text)", ()),
    (765, "lo_export(oid,text)", ()),
    (767, "lo_import(text,oid)", ()),
    (2172, "pg_backup_start(text,boolean)", ()),
    (2274, "pg_stat_reset()", ()),
    (2282, "pg_get_backend_memory_contexts()", ("pg_read_all_stats",)),
    (2307, "pg_stat_reset_slru(text)", ()),
    (2621, "pg_reload_conf()", ()),
    (2622, "pg_rotate_logfile()", ()),
    (2623, "pg_stat_file(text)", ()),
    (2624, "pg_read_file(text,bigint,bigint)", ()),
    (2625, "pg_ls_dir(text)", ()),
    (2739, "pg_backup_stop(boolean)", ()),
    (2848, "pg_switch_wal()", ()),
    (3071, "pg_wal_replay_pause()", ()),
    (3072, "pg_wal_replay_resume()", ()),
    (3098, "pg_create_restore_point(text)", ()),
    (3293, "pg_read_file(text,bigint,bigint,boolean)", ()),
    (3295, "pg_read_binary_file(text,bigint,bigint,boolean)", ()),
    (3297, "pg_ls_dir(text,boolean,boolean)", ()),
    (3307, "pg_stat_file(text,boolean)", ()),
    (3329, "pg_show_all_file_settings()", ()),
    (3353, "pg_ls_logdir()", ("pg_monitor",)),
    (3354, "pg_ls_waldir()", ("pg_monitor",)),
    (3400, "pg_config()", ()),
    (3401, "pg_hba_file_rules()", ()),
    (3436, "pg_promote(boolean,integer)", ()),
    (3775, "pg_stat_reset_shared(text)", ()),
    (3776, "pg_stat_reset_single_table_counters(oid)", ()),
    (3777, "pg_stat_reset_single_function_counters(oid)", ()),
    (3800, "pg_current_logfile()", ("pg_monitor",)),
    (3801, "pg_current_logfile(text)", ("pg_monitor",)),
    (3826, "pg_read_file(text)", ()),
    (3827, "pg_read_binary_file(text,bigint,bigint)", ()),
    (3828, "pg_read_binary_file(text)", ()),
    (4543, "pg_log_backend_memory_contexts(integer)", ()),
    (5029, "pg_ls_tmpdir()", ("pg_monitor",)),
    (5030, "pg_ls_tmpdir(oid)", ("pg_monitor",)),
    (5031, "pg_ls_archive_statusdir()", ("pg_monitor",)),
    (5052, "pg_get_shmem_allocations()", ("pg_read_all_stats",)),
    (6003, "pg_replication_origin_create(text)", ()),
    (6004, "pg_replication_origin_drop(text)", ()),
    (6005, "pg_replication_origin_oid(text)", ()),
    (6006, "pg_replication_origin_session_setup(text)", ()),
    (6007, "pg_replication_origin_session_reset()", ()),
    (6008, "pg_replication_origin_session_is_setup()", ()),
    (6009, "pg_replication_origin_session_progress(boolean)", ()),
    (6010, "pg_replication_origin_xact_setup(pg_lsn,timestamp with time zone)", ()),
    (6011, "pg_replication_origin_xact_reset()", ()),
    (6012, "pg_replication_origin_advance(text,pg_lsn)", ()),
    (6013, "pg_replication_origin_progress(text,boolean)", ()),
    (6014, "pg_show_replication_origin_status()", ()),
    (6170, "pg_stat_reset_replication_slot(text)", ()),
    (6208, "pg_read_file(text,boolean)", ()),
    (6209, "pg_read_binary_file(text,boolean)", ()),
    (6230, "pg_stat_have_stats(text,oid,oid)", ()),
    (6232, "pg_stat_reset_subscription_stats(oid)", ()),
    (6250, "pg_ident_file_mappings()", ()),
    (6270, "pg_ls_logicalsnapdir()", ("pg_monitor",)),
    (6271, "pg_ls_logicalmapdir()", ("pg_monitor",)),
    (6272, "pg_ls_replslotdir(text)", ("pg_monitor",)),
    (6305, "pg_log_standby_snapshot()", ()),
)
