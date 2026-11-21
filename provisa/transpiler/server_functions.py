# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c62-9d47-4b05-a7e3-5c2b8e916d04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions that read or act on a server outside its governed tables (REQ-001): the one list.

A statement's governance is of the tables it names. A function that runs SQL it is given, reads a
file, a large object, a setting or another session, acts on the server, or changes the session it
is called on, does what no table rule sees, as the login the statement runs under: a source's own,
or the engine's.

Two readers:

* the meta path (``metadata_functions``, ``pgwire.catalog``) writes a form for each function a
  catalog answers and refuses :data:`READS_THE_SERVER`, those with no form, by name;
* the validator (``compiler.sql_validator``) refuses :func:`refused_in_a_governed_statement` in
  a governed statement, on every surface, before it is routed.

This module imports nothing of the product, so both may read it.
"""

# Requirements: REQ-001, REQ-2190

from __future__ import annotations

#: A function that would read the server, with no form on the meta path: refused by name, never
#: run on the control plane. These run SQL they are given, or read what a server keeps, on the
#: connection that calls them: the role a client's SQL runs as (metadata_settings.CLIENT_ROLE)
#: would otherwise take a statement past governance. Beside a view of meta they would answer about
#: the control plane (REQ-2190, ONLY NAMED METADATA READS THE CONTROL PLANE): the list is
#: PostgreSQL's non-immutable functions that read or change a server's own state and have no form
#: there.
READS_THE_SERVER = frozenset(
    {
        "binary_upgrade_create_empty_extension",
        "currval",
        "current_query",
        "cursor_to_xml",
        "cursor_to_xmlschema",
        "database_to_xml",
        "database_to_xml_and_xmlschema",
        "database_to_xmlschema",
        "dblink",
        "inet_client_addr",
        "inet_client_port",
        "inet_server_addr",
        "inet_server_port",
        "lastval",
        "nextval",
        "pg_advisory_lock",
        "pg_advisory_lock_shared",
        "pg_advisory_unlock",
        "pg_advisory_unlock_all",
        "pg_advisory_unlock_shared",
        "pg_advisory_xact_lock",
        "pg_advisory_xact_lock_shared",
        "pg_cancel_backend",
        "pg_column_compression",
        "pg_event_trigger_ddl_commands",
        "pg_event_trigger_dropped_objects",
        "pg_event_trigger_table_rewrite_oid",
        "pg_event_trigger_table_rewrite_reason",
        "pg_get_publication_tables",
        "pg_get_wal_replay_pause_state",
        "pg_is_wal_replay_paused",
        "pg_last_committed_xact",
        "pg_logical_slot_get_binary_changes",
        "pg_logical_slot_get_changes",
        "pg_logical_slot_peek_binary_changes",
        "pg_logical_slot_peek_changes",
        "pg_ls_logdir",
        "pg_my_temp_schema",
        "pg_notify",
        "pg_read_file_old",
        "pg_replication_origin_session_reset",
        "pg_rotate_logfile_old",
        "pg_sleep",
        "pg_sleep_for",
        "pg_sleep_until",
        "pg_stop_making_pinned_objects",
        "pg_terminate_backend",
        "pg_try_advisory_lock",
        "pg_try_advisory_lock_shared",
        "pg_try_advisory_xact_lock",
        "pg_try_advisory_xact_lock_shared",
        "pg_wal_replay_pause",
        "pg_wal_replay_resume",
        "query_to_xml",
        "query_to_xml_and_xmlschema",
        "query_to_xmlschema",
        "schema_to_xml",
        "schema_to_xml_and_xmlschema",
        "schema_to_xmlschema",
        "setval",
        "table_to_xml",
        "table_to_xml_and_xmlschema",
        "table_to_xmlschema",
        "ts_rewrite",
        "ts_stat",
    }
)

#: The functions the meta path answers with a form, or never meets, that sent to a server as
#: written read or act on it. Each line says what of the server.
ACTS_ON_THE_SERVER = frozenset(
    {
        # Its files, directories and configuration.
        "pg_config",
        "pg_control_checkpoint",
        "pg_control_init",
        "pg_control_recovery",
        "pg_control_system",
        "pg_current_logfile",
        "pg_hba_file_rules",
        "pg_ident_file_mappings",
        "pg_ls_archive_statusdir",
        "pg_ls_dir",
        "pg_ls_logicalmapdir",
        "pg_ls_logicalsnapdir",
        "pg_ls_replslotdir",
        "pg_ls_summariesdir",
        "pg_ls_tmpdir",
        "pg_ls_waldir",
        "pg_read_binary_file",
        "pg_read_file",
        "pg_relation_filepath",
        "pg_show_all_file_settings",
        "pg_show_all_settings",
        # adminpack: files written, renamed and removed.
        "pg_file_rename",
        "pg_file_sync",
        "pg_file_unlink",
        "pg_file_write",
        "pg_logdir_ls",
        # Its large objects, read and written, and the files they are imported from and exported to.
        "lo_close",
        "lo_creat",
        "lo_create",
        "lo_export",
        "lo_from_bytea",
        "lo_get",
        "lo_import",
        "lo_lseek",
        "lo_lseek64",
        "lo_open",
        "lo_put",
        "lo_tell",
        "lo_tell64",
        "lo_truncate",
        "lo_truncate64",
        "lo_unlink",
        "loread",
        "lowrite",
        # The session it is called on, which a pooled connection keeps for its next caller.
        "pg_export_snapshot",
        "set_config",
        # Its other sessions.
        "pg_blocking_pids",
        "pg_cursor",
        "pg_get_backend_memory_contexts",
        "pg_get_shmem_allocations",
        "pg_listening_channels",
        "pg_lock_status",
        "pg_log_backend_memory_contexts",
        "pg_prepared_statement",
        "pg_prepared_xact",
        "pg_safe_snapshot_blocking_pids",
        # Its operation: configuration, logs, WAL, backups, recovery, replication, maintenance.
        "brin_desummarize_range",
        "brin_summarize_new_values",
        "brin_summarize_range",
        "gin_clean_pending_list",
        "pg_backup_start",
        "pg_backup_stop",
        "pg_copy_logical_replication_slot",
        "pg_copy_physical_replication_slot",
        "pg_create_logical_replication_slot",
        "pg_create_physical_replication_slot",
        "pg_create_restore_point",
        "pg_drop_replication_slot",
        "pg_extension_config_dump",
        "pg_get_replication_slots",
        "pg_import_system_collations",
        "pg_log_standby_snapshot",
        "pg_logical_emit_message",
        "pg_nextoid",
        "pg_promote",
        "pg_reload_conf",
        "pg_replication_slot_advance",
        "pg_rotate_logfile",
        "pg_show_replication_origin_status",
        "pg_switch_wal",
        "pg_sync_replication_slots",
        # Extensions that run SQL they are given as text, or reach another server.
        "connectby",
        "crosstab",
        "crosstab2",
        "crosstab3",
        "crosstab4",
        "postgres_fdw_disconnect",
        "postgres_fdw_disconnect_all",
        "postgres_fdw_get_connections",
        "xpath_table",
    }
)

#: Families refused by prefix, which covers each function a server adds under one: dblink's
#: (another server, and SQL as text), a binary upgrade's, replication origins (the session's and
#: the server's), and a server's statistics, which hold its other sessions' statements, are reset
#: by a call, and take a file's (pg_stat_file).
ACTING_PREFIXES = (
    "binary_upgrade_",
    "dblink_",
    "pg_replication_origin_",
    "pg_stat_",
    "pg_statio_",
)

#: Of :data:`READS_THE_SERVER`, the functions that run SQL they are given as text, or send it to
#: another server: what the text reads, no rule of the statement sees. Refused in every statement.
#: The rest of that list read or act on the server a statement's tables are on, and are refused
#: in a statement that names a table; with none the statement is the engine's, which answers each
#: as a server in the state every Provisa server is in (pg_forms_system.SERVER_STATE) or refuses
#: it by name (REQ-2190).
RUNS_SQL_AS_TEXT = frozenset(
    {
        "cursor_to_xml",
        "cursor_to_xmlschema",
        "database_to_xml",
        "database_to_xml_and_xmlschema",
        "database_to_xmlschema",
        "dblink",
        "query_to_xml",
        "query_to_xml_and_xmlschema",
        "query_to_xmlschema",
        "schema_to_xml",
        "schema_to_xml_and_xmlschema",
        "schema_to_xmlschema",
        "table_to_xml",
        "table_to_xml_and_xmlschema",
        "table_to_xmlschema",
        "ts_rewrite",
        "ts_stat",
    }
)

#: The product's own functions, which an engine holds for the statements the product writes (a
#: fake's method, a keyed digest, a polyfill's helper): a client's statement names none.
OWN_PREFIX = "provisa_"
#: Those the product writes into a statement before it is governed, so the validator meets them:
#: an XML value already answered (compiler.xml_mapping), and what a catalog function's form raises
#: with and counts transactions by (metadata_server_forms). Each answers from its arguments alone.
WRITTEN_BEFORE_GOVERNANCE = frozenset({"provisa_next_xid", "provisa_raise", "provisa_xml"})


def refused_in_a_governed_statement(name: str, names_a_table: bool) -> str | None:
    """Why a governed statement may not call the function ``name`` (as written, any case), or None
    for a function it may: ``"server"`` for one that reads or acts on its server, ``"own"`` for
    one of the product's own. ``names_a_table``: whether the statement reads a table, and so runs
    where that table is."""
    called = name.lower()
    if called.startswith(OWN_PREFIX):
        return None if called in WRITTEN_BEFORE_GOVERNANCE else "own"
    if called in READS_THE_SERVER:
        return "server" if names_a_table or called in RUNS_SQL_AS_TEXT else None
    if called in ACTS_ON_THE_SERVER or called.startswith(ACTING_PREFIXES):
        return "server"
    return None
