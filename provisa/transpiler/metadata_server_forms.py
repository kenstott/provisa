# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e5f17-b3a4-4e90-a1d6-47f09c6b2e31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions on a server, in the compiler (REQ-2190, with REQ-2167's CLIENT-CALLED SERVER
FUNCTIONS ANSWER LIKE POSTGRESQL): a form for each, in PostgreSQL's dialect, as a server with no
such file, sequence or other object answers it, so a call never reaches the control plane to answer
about itself. A form is strict where pg_proc says so (a NULL argument is a NULL result), answers what
needs no object, and otherwise raises PostgreSQL's own error through ``provisa_raise``. A parameter
is text unless it names a type (``x:bigint``): an argument may be of a type no form operates on
(xid8, pg_lsn), which its text reads as."""

# Requirements: REQ-2190, REQ-2167

from __future__ import annotations

import re

_OLD_RELATION = re.compile(r"\b_pg_([a-z_]+)\b")


def _catalog_relations(sql: str) -> str:
    """Every relation a form names is meta's, by its PostgreSQL name."""
    return _OLD_RELATION.sub(lambda m: f"meta.catalog_pg_{m[1]}", sql)


WAL_SEGMENT = 16777216
NEXT_XID = "provisa_next_xid()"
#: The privileges every role holds: PostgreSQL grants them to PUBLIC, and Provisa keeps no ACLs.
_ALWAYS_PRIVILEGED = (
    "has_database_privilege",
    "has_foreign_data_wrapper_privilege",
    "has_function_privilege",
    "has_language_privilege",
    "has_schema_privilege",
    "has_sequence_privilege",
    "has_server_privilege",
    "has_tablespace_privilege",
    "has_type_privilege",
)

#: Stands for the type a raised error's expression takes, which only the form knows: PostgreSQL
#: types each arm of a CASE, and a call that never returns still has one.
_ITS_TYPE = "@TYPE@"

SERVER_FORM_RAISE = "provisa_raise"


def raising(sqlstate: str, message_sql: str) -> str:
    """SQL raising PostgreSQL's error ``sqlstate`` with the text of ``message_sql``."""
    return f"CAST({SERVER_FORM_RAISE}('{sqlstate}', {message_sql}) AS {_ITS_TYPE})"


def strict(params: str, sql_type: str, answer: str) -> str:
    """``answer``, NULL of ``sql_type`` where any of ``params`` is NULL."""
    if not params:
        return answer
    nulls = " OR ".join(f"{_name(p)} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


def _name(param: str) -> str:
    return param.split(":", 1)[0]


def _typed(sql: str, sql_type: str) -> str:
    return sql.replace(_ITS_TYPE, sql_type)


_NO_FILE = "No such file or directory"
_FILE_REFUSALS = (
    # convert_and_check_filename, for a role without pg_read_server_files.
    ("STARTS_WITH({f}, '/')", raising("42501", "'absolute path not allowed'")),
    (
        "({f} = '..' OR STARTS_WITH({f}, '../') OR RIGHT({f}, 3) = '/..' "
        "OR POSITION('/../' IN {f}) > 0)",
        raising("42501", "'path must be in or below the data directory'"),
    ),
)


def _missing_file(path: str, missing_ok: str, verb: str, sql_type: str) -> str:
    """What a call on the file ``path`` answers: every file is missing."""
    refusals = " ".join(
        f"WHEN {test.format(f=path)} THEN {refusal}" for test, refusal in _FILE_REFUSALS
    )
    missing = raising("58P01", f"'could not {verb} file \"' || {path} || '\": {_NO_FILE}'")
    return f"CASE {refusals} WHEN {missing_ok} THEN CAST(NULL AS {sql_type}) ELSE {missing} END"


def _read_file(a: "_Answers", name: str, sql_type: str) -> None:
    """pg_read_file or pg_read_binary_file: (filename [, offset, length] [, missing_ok])."""

    def read(path: str, missing_ok: str, length: str | None = None) -> str:
        nulls = " OR ".join(
            f"{p} IS NULL" for p in (path, missing_ok, *(() if length is None else ("off", length)))
        )
        negative = (
            ""
            if length is None
            else f"WHEN {length} < 0 THEN "
            + raising("22023", "'requested length cannot be negative'")
        )
        opened = _missing_file(path, missing_ok, "open", sql_type).replace(
            "'\": No such", "'\" for reading: No such"
        )
        return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) {negative} ELSE {opened} END"

    a.body(name, "f", read("f", "FALSE"), sql_type)
    a.body(name, "f, missing_ok:boolean", read("f", "missing_ok"), sql_type)
    a.body(name, "f, off:bigint, len:bigint", read("f", "FALSE", "len"), sql_type)
    a.body(
        name,
        "f, off:bigint, len:bigint, missing_ok:boolean",
        read("f", "missing_ok", "len"),
        sql_type,
    )


_STAT_COLUMNS = (
    ("size", "BIGINT"),
    ("access", "TIMESTAMPTZ"),
    ("modification", "TIMESTAMPTZ"),
    ("change", "TIMESTAMPTZ"),
    ("creation", "TIMESTAMPTZ"),
    ("isdir", "BOOLEAN"),
)


def _setval(*params: str) -> str:
    """setval(sequence, value [, is_called]): no relation is a sequence."""
    nulls = " OR ".join(f"{_name(p)} IS NULL" for p in params)
    relation = "(SELECT _sc.relname FROM meta.catalog_pg_class _sc WHERE _sc.oid = seq)"
    return (
        f"CASE WHEN {nulls} THEN CAST(NULL AS BIGINT) "
        f"WHEN {relation} IS NOT NULL THEN "
        + raising("42809", f"'cannot open relation \"' || {relation} || '\"'")
        + " ELSE "
        + raising("XX000", "'could not open relation with OID ' || seq")
        + " END"
    )


_VOID = "''"
_BINARY_UPGRADE = raising(
    "55P02", "'function can only be called when server is in binary upgrade mode'"
)
_NO_SLOTS = raising(
    "55000", "'replication slots can only be used if \"max_replication_slots\" > 0'"
)
_NO_ORIGINS = raising(
    "55000",
    "'cannot query or manipulate replication origin when \"max_replication_slots\" is 0'",
)
_NOT_IN_RECOVERY = raising("55000", "'recovery is not in progress'")
_NO_LSN = "'0/0'"
_FIXED_STATS = "('archiver', 'bgwriter', 'checkpointer', 'io', 'slru', 'wal')"
_OBJECT_STATS = "('database', 'relation', 'function', 'replslot', 'subscription')"
_RESET_TARGETS = (
    "('archiver', 'bgwriter', 'checkpointer', 'io', 'recovery_prefetch', 'slru', 'wal')"
)
_LO_WRITES = (
    ("lo_creat", "mode", "BIGINT"),
    ("lo_create", "lobj", "BIGINT"),
    ("lo_from_bytea", "lobj, data", "BIGINT"),
)


class _Answers:
    """Each function's forms, one per arity: its form as a value, and, where it has rows, its form
    in FROM (a select)."""

    def __init__(self) -> None:
        self.scalar: dict[str, list[tuple[str, str]]] = {}
        self.table: dict[str, list[tuple[str, str]]] = {}

    def body(self, name: str, params: str, sql: str, sql_type: str = "TEXT") -> None:
        """``name(params)`` answers ``sql``, which is of ``sql_type``."""
        self.scalar.setdefault(name, []).append((params, _typed(sql, sql_type)))

    def strict(self, name: str, params: str, sql_type: str, answer: str) -> None:
        """A strict function: a NULL argument is a NULL result of ``sql_type``."""
        self.body(name, params, strict(params, sql_type, answer), sql_type)

    def strict_all(self, name: str, arities: tuple[str, ...], sql_type: str, answer: str) -> None:
        for params in arities:
            self.strict(name, params, sql_type, answer)

    def rows(
        self, name: str, params: str, columns: tuple[tuple[str, str], ...], where: str
    ) -> None:
        """A set-returning function of ``columns``: no rows, or (``where``) its refusal."""
        typed = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in columns)
        where = _typed(where, "BOOLEAN")
        self.table.setdefault(name, []).append((params, f"SELECT {typed} WHERE {where}"))
        self.body(
            name,
            params,
            f"UNNEST(CASE WHEN {where} THEN ARRAY[''] ELSE CAST(ARRAY[] AS TEXT[]) END)",
        )

    def selected(self, name: str, params: str, select: str, value: str = "") -> None:
        """A set-returning function of the rows of ``select``. A result column is each row's
        record, or ``value`` where the function returns one value."""
        self.table.setdefault(name, []).append((params, select))
        row = value or "CAST(_r AS TEXT)"
        self.body(name, params, f"UNNEST(ARRAY(SELECT {row} FROM ({select}) _r))")

    def values(self, name: str, sql_type: str) -> None:
        """A set-returning function of one column, with no rows."""
        self.body(name, "", f"UNNEST(CAST(ARRAY[] AS {sql_type}[]))", sql_type)
        self.table.setdefault(name, []).append(
            ("", f"SELECT CAST(NULL AS {sql_type}) AS {name} WHERE FALSE")
        )

    def record(self, name: str, params: str, columns: tuple[tuple[str, str, str], ...]) -> None:
        """A record-returning function: its one row, strict in ``params``."""
        fields = ", ".join(f"CAST({_typed(v, t)} AS {t})" for _, t, v in columns)
        self.body(name, params, strict(params, "TEXT", f"CAST(ROW({fields}) AS TEXT)"))
        out = ", ".join(
            f"CAST({strict(params, t, _typed(v, t))} AS {t}) AS {c}" for c, t, v in columns
        )
        self.table.setdefault(name, []).append((params, f"SELECT {out}"))

    def refused_record(
        self, name: str, params: str, columns: tuple[tuple[str, str], ...], refusal: str
    ) -> None:
        """A record-returning function that refuses every non-NULL call."""
        self.body(name, params, strict(params, "TEXT", refusal))
        first, *rest = columns
        out = [f"{strict(params, first[1], _typed(refusal, first[1]))} AS {first[0]}"]
        out += [f"CAST(NULL AS {t}) AS {c}" for c, t in rest]
        self.table.setdefault(name, []).append((params, f"SELECT {', '.join(out)}"))


def _server_objects(a: _Answers) -> None:  # noqa: PLR0915 -- one line per function
    """The functions on what a server has that Provisa has none of."""
    for suffix in (
        "set_next_array_pg_type_oid",
        "set_next_heap_pg_class_oid",
        "set_next_heap_relfilenode",
        "set_next_index_pg_class_oid",
        "set_next_index_relfilenode",
        "set_next_pg_authid_oid",
        "set_next_pg_enum_oid",
        "set_next_pg_tablespace_oid",
        "set_next_pg_type_oid",
        "set_next_toast_pg_class_oid",
        "set_next_toast_relfilenode",
    ):
        a.strict(f"binary_upgrade_{suffix}", "oid", "VARCHAR", _BINARY_UPGRADE)
    a.strict("binary_upgrade_set_record_init_privs", "flag", "VARCHAR", _BINARY_UPGRADE)
    a.strict("binary_upgrade_set_missing_value", "a, b, c", "VARCHAR", _BINARY_UPGRADE)
    a.strict("binary_upgrade_logical_slot_has_caught_up", "slot", "BOOLEAN", _BINARY_UPGRADE)
    # The statistics Provisa does not keep: nothing to reset.
    for name in ("pg_stat_force_next_flush", "pg_stat_clear_snapshot", "pg_stat_reset"):
        a.body(name, "", _VOID)
    a.strict("pg_stat_reset_single_function_counters", "oid", "VARCHAR", _VOID)
    a.body("pg_stat_reset_subscription_stats", "oid", _VOID)
    a.body("pg_stat_reset_slru", "target", _VOID)
    a.body(
        "pg_stat_reset_shared",
        "target",
        f"CASE WHEN target IS NULL OR target IN {_RESET_TARGETS} THEN {_VOID} ELSE "
        + raising("22023", "'unrecognized reset target: \"' || target || '\"'")
        + " END",
    )
    a.body(
        "pg_stat_reset_replication_slot",
        "slot",
        f"CASE WHEN slot IS NULL THEN {_VOID} ELSE "
        + raising("42704", "'replication slot \"' || slot || '\" does not exist'")
        + " END",
    )
    a.strict(
        "pg_stat_have_stats",
        "kind, db, obj",
        "BOOLEAN",
        f"CASE WHEN kind IN {_FIXED_STATS} THEN true WHEN kind IN {_OBJECT_STATS} THEN false ELSE "
        + raising("22023", "'invalid statistics kind: \"' || kind || '\"'")
        + " END",
    )
    # Replication: a server whose max_replication_slots is 0.
    a.strict("pg_replication_origin_oid", "name", "BIGINT", "CAST(NULL AS BIGINT)")
    a.body("pg_replication_origin_session_is_setup", "", "FALSE", "BOOLEAN")
    no_origin = raising("42704", "'replication origin \"' || name || '\" does not exist'")
    unconfigured = raising("55000", "'no replication origin is configured'")
    a.strict("pg_replication_origin_create", "name", "BIGINT", _NO_ORIGINS)
    a.strict("pg_replication_origin_drop", "name", "VARCHAR", no_origin)
    a.strict("pg_replication_origin_advance", "name, lsn", "VARCHAR", no_origin)
    a.strict("pg_replication_origin_progress", "name, flush", "VARCHAR", no_origin)
    a.strict("pg_replication_origin_session_setup", "name", "VARCHAR", no_origin)
    a.strict("pg_replication_origin_session_progress", "flush", "VARCHAR", unconfigured)
    a.strict("pg_replication_origin_xact_setup", "lsn, ts", "VARCHAR", unconfigured)
    a.body("pg_replication_origin_xact_reset", "", _VOID)  # no origin is set up to reset
    a.rows(
        "pg_show_replication_origin_status",
        "",
        (
            ("local_id", "BIGINT"),
            ("external_id", "VARCHAR"),
            ("remote_lsn", "VARCHAR"),
            ("local_lsn", "VARCHAR"),
        ),
        "false",
    )
    slot = (("slot_name", "VARCHAR"), ("lsn", "VARCHAR"))
    no_slot = raising("42704", "'replication slot \"' || name || '\" does not exist'")
    for name, arities in (
        ("pg_create_physical_replication_slot", ("name", "name, reserve", "name, reserve, temp")),
        (
            "pg_create_logical_replication_slot",
            (
                "name, plugin",
                "name, plugin, temp",
                "name, plugin, temp, two",
                "name, plugin, temp, two, failover",
            ),
        ),
    ):
        for params in arities:
            a.refused_record(name, params, slot, _NO_SLOTS)
    for name, arities in (
        ("pg_copy_physical_replication_slot", ("name, dst", "name, dst, temp")),
        (
            "pg_copy_logical_replication_slot",
            ("name, dst", "name, dst, temp", "name, dst, temp, plugin"),
        ),
    ):
        for params in arities:
            a.refused_record(name, params, slot, no_slot)
    a.strict("pg_drop_replication_slot", "name", "VARCHAR", no_slot)
    a.refused_record(
        "pg_replication_slot_advance",
        "name, upto",
        (("slot_name", "VARCHAR"), ("end_lsn", "VARCHAR")),
        f"CASE WHEN upto = '0/0' THEN {raising('XX000', "'invalid target WAL LSN'")} "
        f"ELSE {no_slot} END",
    )
    a.strict("pg_logical_emit_message", "tx, prefix, message", "VARCHAR", _NO_LSN)
    a.strict("pg_logical_emit_message", "tx, prefix, message, flush", "VARCHAR", _NO_LSN)
    # The WAL: a primary that has written none.
    for name in ("pg_current_wal_lsn", "pg_current_wal_insert_lsn", "pg_current_wal_flush_lsn"):
        a.body(name, "", _NO_LSN)
    a.body("pg_switch_wal", "", _NO_LSN)
    a.body("pg_log_standby_snapshot", "", _NO_LSN)
    a.body("pg_last_wal_receive_lsn", "", "CAST(NULL AS VARCHAR)")
    a.body("pg_last_wal_replay_lsn", "", "CAST(NULL AS VARCHAR)")
    a.body("pg_last_xact_replay_timestamp", "", "CAST(NULL AS TIMESTAMPTZ)", "TIMESTAMPTZ")
    a.strict_all("pg_promote", ("", "do_wait", "do_wait, seconds"), "BOOLEAN", _NOT_IN_RECOVERY)
    backup_level = raising("55000", "'WAL level not sufficient for making an online backup'")
    a.strict_all("pg_backup_start", ("label", "label, fast"), "VARCHAR", backup_level)
    stop = (("lsn", "VARCHAR"), ("labelfile", "VARCHAR"), ("spcmapfile", "VARCHAR"))
    for params in ("", "archive"):
        a.refused_record(
            "pg_backup_stop", params, stop, raising("55000", "'backup is not in progress'")
        )
    a.body("pg_rotate_logfile", "", "FALSE", "BOOLEAN")
    a.record(
        "pg_get_wal_summarizer_state",
        "",
        (
            ("summarized_tli", "BIGINT", "0"),
            ("summarized_lsn", "VARCHAR", _NO_LSN),
            ("pending_lsn", "VARCHAR", _NO_LSN),
            ("summarizer_pid", "INTEGER", "NULL"),
        ),
    )
    a.record(
        "pg_control_system",
        "",
        (
            ("pg_control_version", "INTEGER", "1700"),
            ("catalog_version_no", "INTEGER", "202406281"),
            ("system_identifier", "BIGINT", "0"),
            ("pg_control_last_modified", "TIMESTAMPTZ", "NULL"),
        ),
    )
    a.record(
        "pg_control_init",
        "",
        (
            ("max_data_alignment", "INTEGER", "8"),
            ("database_block_size", "INTEGER", "8192"),
            ("blocks_per_segment", "INTEGER", "131072"),
            ("wal_block_size", "INTEGER", "8192"),
            ("bytes_per_wal_segment", "INTEGER", str(WAL_SEGMENT)),
            ("max_identifier_length", "INTEGER", "64"),
            ("max_index_columns", "INTEGER", "32"),
            ("max_toast_chunk_size", "INTEGER", "1996"),
            ("large_object_chunk_size", "INTEGER", "2048"),
            ("float8_pass_by_value", "BOOLEAN", "true"),
            ("data_page_checksum_version", "INTEGER", "0"),
        ),
    )
    a.record(
        "pg_control_recovery",
        "",
        (
            ("min_recovery_end_lsn", "VARCHAR", _NO_LSN),
            ("min_recovery_end_timeline", "INTEGER", "0"),
            ("backup_start_lsn", "VARCHAR", _NO_LSN),
            ("backup_end_lsn", "VARCHAR", _NO_LSN),
            ("end_of_backup_record_required", "BOOLEAN", "false"),
        ),
    )
    a.record(
        "pg_control_checkpoint",
        "",
        (
            ("checkpoint_lsn", "VARCHAR", _NO_LSN),
            ("redo_lsn", "VARCHAR", _NO_LSN),
            ("redo_wal_file", "VARCHAR", "'000000010000000000000000'"),
            ("timeline_id", "INTEGER", "1"),
            ("prev_timeline_id", "INTEGER", "1"),
            ("full_page_writes", "BOOLEAN", "true"),
            ("next_xid", "VARCHAR", "'0:3'"),
            ("next_oid", "BIGINT", "16384"),
            ("next_multixact_id", "BIGINT", "1"),
            ("next_multi_offset", "BIGINT", "0"),
            ("oldest_xid", "BIGINT", "3"),
            ("oldest_xid_dbid", "BIGINT", "1"),
            ("oldest_active_xid", "BIGINT", "0"),
            ("oldest_multi_xid", "BIGINT", "1"),
            ("oldest_multi_dbid", "BIGINT", "1"),
            ("oldest_commit_ts_xid", "BIGINT", "0"),
            ("newest_commit_ts_xid", "BIGINT", "0"),
            ("checkpoint_time", "TIMESTAMPTZ", "NULL"),
        ),
    )
    # What the server lists: nothing of its own to list.
    named = (("name", "VARCHAR"), ("size", "BIGINT"), ("modification", "TIMESTAMPTZ"))
    for name in (
        "pg_ls_waldir",
        "pg_ls_archive_statusdir",
        "pg_ls_logicalmapdir",
        "pg_ls_logicalsnapdir",
    ):
        a.rows(name, "", named, "false")
    a.rows("pg_ls_tmpdir", "", named, "false")
    a.rows(
        "pg_ls_tmpdir",
        "tablespace:bigint",
        named,
        "tablespace IS NOT NULL AND tablespace NOT IN (1663, 1664) AND "
        + raising("42704", "'tablespace with OID ' || tablespace || ' does not exist'"),
    )
    a.rows(
        "pg_ls_replslotdir",
        "slot",
        named,
        "slot IS NOT NULL AND "
        + raising("42704", "'replication slot \"' || slot || '\" does not exist'"),
    )
    for name, columns in (
        (
            "pg_available_wal_summaries",
            (("tli", "BIGINT"), ("start_lsn", "VARCHAR"), ("end_lsn", "VARCHAR")),
        ),
        (
            "pg_lock_status",
            (
                ("locktype", "VARCHAR"),
                ("database", "BIGINT"),
                ("relation", "BIGINT"),
                ("page", "INTEGER"),
                ("tuple", "SMALLINT"),
                ("virtualxid", "VARCHAR"),
                ("transactionid", "BIGINT"),
                ("classid", "BIGINT"),
                ("objid", "BIGINT"),
                ("objsubid", "SMALLINT"),
                ("virtualtransaction", "VARCHAR"),
                ("pid", "INTEGER"),
                ("mode", "VARCHAR"),
                ("granted", "BOOLEAN"),
                ("fastpath", "BOOLEAN"),
                ("waitstart", "TIMESTAMPTZ"),
            ),
        ),
        (
            "pg_prepared_statement",
            (
                ("name", "VARCHAR"),
                ("statement", "VARCHAR"),
                ("prepare_time", "TIMESTAMPTZ"),
                ("parameter_types", "BIGINT[]"),
                ("result_types", "BIGINT[]"),
                ("from_sql", "BOOLEAN"),
                ("generic_plans", "BIGINT"),
                ("custom_plans", "BIGINT"),
            ),
        ),
        (
            "pg_prepared_xact",
            (
                ("transaction", "BIGINT"),
                ("gid", "VARCHAR"),
                ("prepared", "TIMESTAMPTZ"),
                ("ownerid", "BIGINT"),
                ("dbid", "BIGINT"),
            ),
        ),
        (
            "pg_get_replication_slots",
            (
                ("slot_name", "VARCHAR"),
                ("plugin", "VARCHAR"),
                ("slot_type", "VARCHAR"),
                ("datoid", "BIGINT"),
                ("temporary", "BOOLEAN"),
                ("active", "BOOLEAN"),
                ("active_pid", "INTEGER"),
                ("xmin", "BIGINT"),
                ("catalog_xmin", "BIGINT"),
                ("restart_lsn", "VARCHAR"),
                ("confirmed_flush_lsn", "VARCHAR"),
                ("wal_status", "VARCHAR"),
                ("safe_wal_size", "BIGINT"),
                ("two_phase", "BOOLEAN"),
                ("inactive_since", "TIMESTAMPTZ"),
                ("conflicting", "BOOLEAN"),
                ("invalidation_reason", "VARCHAR"),
                ("failover", "BOOLEAN"),
                ("synced", "BOOLEAN"),
            ),
        ),
        (
            "pg_get_backend_memory_contexts",
            (
                ("name", "VARCHAR"),
                ("ident", "VARCHAR"),
                ("parent", "VARCHAR"),
                ("level", "INTEGER"),
                ("total_bytes", "BIGINT"),
                ("total_nblocks", "BIGINT"),
                ("free_bytes", "BIGINT"),
                ("free_chunks", "BIGINT"),
                ("used_bytes", "BIGINT"),
            ),
        ),
        (
            "pg_get_shmem_allocations",
            (
                ("name", "VARCHAR"),
                ("off", "BIGINT"),
                ("size", "BIGINT"),
                ("allocated_size", "BIGINT"),
            ),
        ),
        (
            "pg_get_wait_events",
            (("type", "VARCHAR"), ("name", "VARCHAR"), ("description", "VARCHAR")),
        ),
        (
            "pg_get_wal_resource_managers",
            (("rm_id", "INTEGER"), ("rm_name", "VARCHAR"), ("rm_builtin", "BOOLEAN")),
        ),
        (
            "pg_hba_file_rules",
            (
                ("rule_number", "INTEGER"),
                ("file_name", "VARCHAR"),
                ("line_number", "INTEGER"),
                ("type", "VARCHAR"),
                ("database", "VARCHAR[]"),
                ("user_name", "VARCHAR[]"),
                ("address", "VARCHAR"),
                ("netmask", "VARCHAR"),
                ("auth_method", "VARCHAR"),
                ("options", "VARCHAR[]"),
                ("error", "VARCHAR"),
            ),
        ),
        (
            "pg_ident_file_mappings",
            (
                ("map_number", "INTEGER"),
                ("file_name", "VARCHAR"),
                ("line_number", "INTEGER"),
                ("map_name", "VARCHAR"),
                ("sys_name", "VARCHAR"),
                ("pg_username", "VARCHAR"),
                ("error", "VARCHAR"),
            ),
        ),
        (
            "pg_show_all_file_settings",
            (
                ("sourcefile", "VARCHAR"),
                ("sourceline", "INTEGER"),
                ("seqno", "INTEGER"),
                ("name", "VARCHAR"),
                ("setting", "VARCHAR"),
                ("applied", "BOOLEAN"),
                ("error", "VARCHAR"),
            ),
        ),
        (
            "pg_get_catalog_foreign_keys",
            (
                ("fktable", "BIGINT"),
                ("fkcols", "VARCHAR[]"),
                ("pktable", "BIGINT"),
                ("pkcols", "VARCHAR[]"),
                ("is_array", "BOOLEAN"),
                ("is_opt", "BOOLEAN"),
            ),
        ),
        ("pg_config", (("name", "VARCHAR"), ("setting", "VARCHAR"))),
    ):
        a.rows(name, "", columns, "false")  # fmt: skip
    a.values("pg_listening_channels", "VARCHAR")
    a.body(
        "pg_ls_dir",
        "f",
        _missing_directory("f", "FALSE"),
        "TEXT[]",
    )
    a.body(
        "pg_ls_dir",
        "f, missing_ok:boolean, include_dots",
        _missing_directory("f", "COALESCE(missing_ok, FALSE)"),
        "TEXT[]",
    )
    # A large object: Provisa holds none, and takes none: its catalog DB is read-only to a client.
    missing_object = raising(
        "42704", "'large object ' || CAST(lobj AS BIGINT) || ' does not exist'"
    )
    bad_descriptor = raising("42704", "'invalid large-object descriptor: ' || fd")
    for name, params, sql_type in (
        ("lo_close", "fd", "INTEGER"),
        ("lo_tell", "fd", "INTEGER"),
        ("lo_tell64", "fd", "BIGINT"),
        ("lo_lseek", "fd, off, whence", "INTEGER"),
        ("lo_lseek64", "fd, off, whence", "BIGINT"),
        ("lo_truncate", "fd, len", "INTEGER"),
        ("lo_truncate64", "fd, len", "INTEGER"),
        ("loread", "fd, len", "BYTEA"),
        ("lowrite", "fd, data", "INTEGER"),
    ):
        a.strict(name, params, sql_type, bad_descriptor)
    for name, params, sql_type in (
        ("lo_open", "lobj, mode", "INTEGER"),
        ("lo_get", "lobj", "BYTEA"),
        ("lo_get", "lobj, off, len", "BYTEA"),
        ("lo_put", "lobj, off, data", "VARCHAR"),
        ("lo_unlink", "lobj", "INTEGER"),
        ("lo_export", "lobj, f", "INTEGER"),
    ):
        a.strict(name, params, sql_type, missing_object)
    for name, params, sql_type in _LO_WRITES:
        a.strict(
            name,
            params,
            sql_type,
            raising("25006", f"'cannot execute {name}() in a read-only transaction'"),
        )
    unreadable = raising("58P01", f"'could not open server file \"' || f || '\": {_NO_FILE}'")
    a.strict("lo_import", "f", "BIGINT", unreadable)
    a.strict("lo_import", "f, lobj", "BIGINT", unreadable)


def _missing_directory(path: str, missing_ok: str) -> str:
    """pg_ls_dir's rows: strict, and every directory is missing."""
    refusals = " ".join(
        f"WHEN {test.format(f=path)} THEN {refusal}" for test, refusal in _FILE_REFUSALS
    )
    missing = raising("58P01", f"'could not open directory \"' || {path} || '\": {_NO_FILE}'")
    empty = "CAST(ARRAY[] AS TEXT[])"
    return (
        f"UNNEST(CASE WHEN {path} IS NULL THEN {empty} {refusals} WHEN {missing_ok} THEN {empty} "
        f"ELSE {missing} END)"
    )


def _xid(a: _Answers) -> None:
    """Transaction ids and snapshots: nothing runs but the statement asking, which has no xid.
    ``NEXT_XID`` is the session's next id, which the routing writes in."""
    a.body("pg_current_xact_id_if_assigned", "", "CAST(NULL AS BIGINT)", "BIGINT")
    a.body("txid_current_if_assigned", "", "CAST(NULL AS BIGINT)", "BIGINT")
    snapshot = f"{NEXT_XID} || ':' || {NEXT_XID} || ':'"
    a.body("pg_current_snapshot", "", snapshot)
    a.body("txid_current_snapshot", "", snapshot)
    status = (
        f"CASE WHEN x >= {NEXT_XID} THEN "
        + raising("22023", "'transaction ID ' || x || ' is in the future'")
        + " ELSE 'committed' END"
    )
    a.strict("pg_xact_status", "x:bigint", "TEXT", status)
    a.strict("txid_status", "x:bigint", "TEXT", status)
    no_commit_ts = raising("55000", "'could not get commit timestamp data'")
    a.strict("pg_xact_commit_timestamp", "x", "TIMESTAMPTZ", no_commit_ts)
    a.refused_record(
        "pg_xact_commit_timestamp_origin",
        "x",
        (("timestamp", "TIMESTAMPTZ"), ("roident", "BIGINT")),
        no_commit_ts,
    )
    a.rows("pg_get_multixact_members", "m", (("xid", "BIGINT"), ("mode", "TEXT")), "FALSE")
    a.body(
        "pg_export_snapshot",
        "",
        f"'00000003-' || LPAD(pg_catalog.to_hex({NEXT_XID}), 8, '0') || '-1'",
    )


_BTREE = 403
_BTREE_BUILD_PHASES = (
    (1, "initializing"),
    (2, "scanning table"),
    (3, "sorting live tuples"),
    (4, "sorting dead tuples"),
    (5, "loading tuples in tree"),
)
#: What each built-in index access method declares of itself (its IndexAmRoutine).
_INDEXAM_PROPERTIES = {
    "can_order": (_BTREE,),
    "can_unique": (_BTREE,),
    "can_multi_col": (_BTREE, 783, 2742, 3580),
    "can_exclude": (_BTREE, 405, 783, 4000),
    "can_include": (_BTREE, 783, 4000),
}
_BUILT_IN_ACCESS_METHODS = (_BTREE, 405, 783, 2742, 3580, 4000)
_BUILT_IN_COLLATIONS = "(100, 811, 950, 951, 962, 963)"


def _last_part(name: str) -> str:
    return f"(PARSE_IDENT({name}, FALSE))[ARRAY_LENGTH(PARSE_IDENT({name}, FALSE), 1)]"


def _qualifier(name: str) -> str:
    """The schema a possibly qualified name names; NULL for none."""
    parts = f"PARSE_IDENT({name}, FALSE)"
    return (
        f"CASE WHEN ARRAY_LENGTH({parts}, 1) > 1 THEN ({parts})[ARRAY_LENGTH({parts}, 1) - 1] END"
    )


def _pair(oid: str, name: str) -> str:
    """An object identifier value with the name its text form writes."""
    return f"JSONB_BUILD_OBJECT('oid', CAST({oid} AS BIGINT), 'name', {name})"


_CLASSES = "meta.catalog_pg_class _c JOIN meta.catalog_pg_namespace _n ON _n.oid = _c.relnamespace"
_IN_PATH = "('pg_catalog', 'public')"
#: The name of the relation a select's ``_r.relid`` holds.
_NAME_OF_RELID = (
    "(SELECT CASE WHEN _n.nspname IN ('pg_catalog', 'public') THEN '' "
    "ELSE QUOTE_IDENT(_n.nspname) || '.' END || QUOTE_IDENT(_c.relname) "
    "FROM meta.catalog_pg_class _c JOIN meta.catalog_pg_namespace _n "
    "ON _n.oid = _c.relnamespace WHERE _c.oid = _r.relid)"
)
#: aclitemout's order of the rights, each by the word a grant names it with.
_ACL_RIGHTS = (
    ("a", "INSERT"), ("r", "SELECT"), ("w", "UPDATE"), ("d", "DELETE"), ("D", "TRUNCATE"),
    ("x", "REFERENCES"), ("t", "TRIGGER"), ("X", "EXECUTE"), ("U", "USAGE"), ("C", "CREATE"),
    ("T", "TEMP"), ("c", "CONNECT"),
)  # fmt: skip


def _to_reg(a: _Answers) -> None:
    """to_regclass and its kin: an object identifier of the object a name names; NULL for none."""
    a.strict(
        "to_regclass",
        "name",
        "OID",
        f"(SELECT _c.oid FROM {_CLASSES} "
        f"WHERE _c.relname = {_last_part('name')} AND (_n.nspname = {_qualifier('name')} "
        f"OR ({_qualifier('name')} IS NULL AND _n.nspname IN {_IN_PATH})) LIMIT 1)",
    )
    a.strict(
        "to_regnamespace",
        "name",
        "OID",
        f"(SELECT _n.oid FROM meta.catalog_pg_namespace _n "
        f"WHERE _n.nspname = {_last_part('name')})",
    )
    a.strict(
        "to_regrole",
        "name",
        "TEXT",
        "(SELECT CAST(_r.rolname AS TEXT) FROM meta.catalog_pg_roles _r "
        f"WHERE _r.rolname = {_last_part('name')})",
    )
    a.strict(
        "to_regproc",
        "name",
        "OID",
        f"(SELECT _p.oid FROM meta.catalog_pg_proc _p WHERE _p.proname = {_last_part('name')} "
        "LIMIT 1)",
    )
    a.strict(
        "to_regprocedure",
        "name",
        "TEXT",
        "(SELECT SPLIT_PART(name, '(', 1) || '(' || SPLIT_PART(name, '(', 2) "
        "FROM meta.catalog_pg_proc _p WHERE _p.proname = SPLIT_PART(name, '(', 1) LIMIT 1)",
    )
    a.strict(
        "to_regoper",
        "name",
        "TEXT",
        "(SELECT CAST(_o.oprname AS TEXT) FROM meta.catalog_pg_operator _o "
        "WHERE _o.oprname = name LIMIT 1)",
    )
    a.strict(
        "to_regoperator",
        "name",
        "TEXT",
        "(SELECT name FROM meta.catalog_pg_operator _o "
        "WHERE _o.oprname = SPLIT_PART(name, '(', 1) LIMIT 1)",
    )
    a.strict(
        "to_regcollation",
        "name",
        "TEXT",
        f"(SELECT CAST(_c.collname AS TEXT) FROM meta.catalog_pg_collation _c "
        f"WHERE _c.collname = {_last_part('name')} LIMIT 1)",
    )
    a.strict("to_regtype", "name", "OID", _type_lookup("name"))
    a.strict(
        "to_regtypemod",
        "name",
        "INTEGER",
        f"CASE WHEN {_type_lookup("REGEXP_REPLACE(name, '\\s*\\(.*$', '')")} IS NOT NULL "
        "THEN COALESCE(CASE WHEN LOWER(name) ~ '^\\s*(time|timestamp|interval)' THEN "
        "CAST((REGEXP_MATCH(name, '\\((\\d+)\\)'))[1] AS INTEGER) "
        "WHEN LOWER(name) ~ '^\\s*text\\s*\\(' THEN "
        + raising("42601", "'type modifier is not allowed for type \"text\"'").replace(
            _ITS_TYPE, "INTEGER"
        )
        + " WHEN LOWER(name) ~ '^\\s*(numeric|decimal)\\s*\\(' THEN "
        "CAST(SPLIT_PART((REGEXP_MATCH(name, '\\(([^)]*)\\)'))[1], ',', 1) AS INTEGER) * 65536 "
        "+ COALESCE(CAST(NULLIF(TRIM(SPLIT_PART((REGEXP_MATCH(name, '\\(([^)]*)\\)'))[1], ',', 2)), "
        "'') AS INTEGER), 0) + 4 ELSE "
        "CAST((REGEXP_MATCH(name, '\\((\\d+)\\)'))[1] AS INTEGER) + 4 END, -1) END",
    )


def _type_lookup(name: str) -> str:
    spelled = " ".join(f"WHEN '{d}' THEN '{i}'" for i, d in _TYPE_SPELLINGS)
    return (
        "(SELECT _t.oid FROM meta.catalog_pg_type _t WHERE _t.typname = "
        f"CASE LOWER(TRIM({name})) {spelled} ELSE LOWER(TRIM({name})) END LIMIT 1)"
    )


#: A type's internal name, and the name format_type writes for it.
_TYPE_SPELLINGS = (
    ("bool", "boolean"),
    ("int2", "smallint"),
    ("int4", "integer"),
    ("int8", "bigint"),
    ("float4", "real"),
    ("float8", "double precision"),
    ("varchar", "character varying"),
    ("bpchar", "character"),
    ("timestamptz", "timestamp with time zone"),
    ("timetz", "time with time zone"),
)


def _relation_kind(relkind: str) -> str:
    return (
        f"CASE {relkind} WHEN 'r' THEN 'table' WHEN 'p' THEN 'table' WHEN 'v' THEN 'view' "
        "WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'I' THEN 'index' "
        "WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' WHEN 'c' THEN 'composite type' "
        "WHEN 't' THEN 'TOAST table' END"
    )


def _named_in_path(schema: str, name: str) -> str:
    return (
        f"CASE WHEN {schema} IN {_IN_PATH} THEN '' ELSE QUOTE_IDENT({schema}) || '.' END "
        f"|| QUOTE_IDENT({name})"
    )


def _type_names(vector: str, separator: str, qualified: bool) -> str:
    """The names of a vector of type oids, in order, joined by ``separator``."""
    name = (
        "format_type(_v._k, NULL)"
        if not qualified
        else "QUOTE_IDENT(_tn.nspname) || '.' || QUOTE_IDENT(_ty.typname)"
    )
    joined = (
        ""
        if not qualified
        else " JOIN meta.catalog_pg_type _ty ON _ty.oid = _v._k "
        "JOIN meta.catalog_pg_namespace _tn ON _tn.oid = _ty.typnamespace"
    )
    return (
        f"(SELECT COALESCE(STRING_AGG({name}, '{separator}' ORDER BY _v._i), '') FROM "
        f"UNNEST(CAST(REGEXP_SPLIT_TO_ARRAY(TRIM(BOTH '{{}}' FROM CAST({vector} AS TEXT)), '[ ,]+') "
        "AS _TEXT)) WITH ORDINALITY _v0(_t, _i) "
        "JOIN LATERAL (SELECT CAST(NULLIF(_v0._t, '') AS OID) AS _k, _v0._i AS _i) _v ON _v._k IS NOT NULL"
        f"{joined})"
    )


_COLUMN = (
    "(SELECT QUOTE_IDENT(_att.attname) FROM meta.catalog_pg_attribute _att "
    "WHERE _att.attrelid = _c.oid AND _att.attnum = objsubid)"
)
_RELATION_ROW = "QUOTE_IDENT(_n.nspname) || '.' || QUOTE_IDENT(_c.relname)"
#: pg_identify_object for the objects a catalog holds -- relations and their columns, functions,
#: types and schemas -- as (type, schema, name, identity, description), the identity
#: schema-qualified, the name only where it alone names the object, and the description as
#: pg_describe_object writes it.
_IDENTIFIED = (
    f"SELECT CASE WHEN objsubid = 0 THEN {_relation_kind('_c.relkind')} "
    f"ELSE {_relation_kind('_c.relkind')} || ' column' END AS type, "
    "CAST(_n.nspname AS TEXT) AS schema, "
    "CASE WHEN objsubid = 0 THEN QUOTE_IDENT(_c.relname) END AS name, "
    f"CASE WHEN objsubid = 0 THEN {_RELATION_ROW} ELSE {_RELATION_ROW} || '.' || {_COLUMN} END "
    "AS identity, "
    f"CASE WHEN objsubid = 0 THEN {_relation_kind('_c.relkind')} || ' ' "
    f"|| {_named_in_path('_n.nspname', '_c.relname')} "
    f"ELSE 'column ' || {_COLUMN} || ' of ' || {_relation_kind('_c.relkind')} || ' ' "
    f"|| {_named_in_path('_n.nspname', '_c.relname')} END AS description "
    f"FROM {_CLASSES} WHERE classid = 1259 AND _c.oid = objid "
    "UNION ALL "
    "SELECT CASE _p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END, "
    "CAST(_n.nspname AS TEXT), CAST(NULL AS TEXT), "
    "QUOTE_IDENT(_n.nspname) || '.' || QUOTE_IDENT(_p.proname) || '(' || "
    f"{_type_names('_p.proargtypes', ',', True)} || ')', "
    "CASE _p.prokind WHEN 'p' THEN 'procedure ' ELSE 'function ' END "
    f"|| {_named_in_path('_n.nspname', '_p.proname')} || '(' "
    f"|| {_type_names('_p.proargtypes', ',', False)} || ')' "
    "FROM meta.catalog_pg_proc _p JOIN meta.catalog_pg_namespace _n "
    "ON _n.oid = _p.pronamespace WHERE classid = 1255 AND _p.oid = objid "
    "UNION ALL "
    "SELECT 'type', CAST(_n.nspname AS TEXT), QUOTE_IDENT(_t.typname), "
    "CASE WHEN _t.typname IN ("
    + ", ".join(f"'{internal}'" for internal, _ in _TYPE_SPELLINGS)
    + ") THEN format_type(_t.oid, NULL) "
    "ELSE QUOTE_IDENT(_n.nspname) || '.' || QUOTE_IDENT(_t.typname) END, "
    "'type ' || format_type(_t.oid, NULL) "
    "FROM meta.catalog_pg_type _t JOIN meta.catalog_pg_namespace _n "
    "ON _n.oid = _t.typnamespace WHERE classid = 1247 AND _t.oid = objid "
    "UNION ALL "
    "SELECT 'schema', CAST(NULL AS TEXT), QUOTE_IDENT(_n.nspname), QUOTE_IDENT(_n.nspname), "
    "'schema ' || _n.nspname FROM meta.catalog_pg_namespace _n "
    "WHERE classid = 2615 AND _n.oid = objid"
)
_OBJECT_PARAMS = "classid:bigint, objid:bigint, objsubid:bigint"


def _addresses(a: _Answers) -> None:
    """pg_describe_object, pg_identify_object, pg_identify_object_as_address and
    pg_get_object_address, for what a catalog holds. Any other object is none it holds, which
    PostgreSQL answers NULL."""
    a.body(
        "pg_describe_object",
        _OBJECT_PARAMS,
        f"(SELECT _i.description FROM ({_IDENTIFIED}) _i)",
    )
    identified = f"SELECT _i.type, _i.schema, _i.name, _i.identity FROM ({_IDENTIFIED}) _i"
    a.selected("pg_identify_object", _OBJECT_PARAMS, identified)
    a.selected(
        "pg_identify_object_as_address",
        _OBJECT_PARAMS,
        "SELECT _i.type, CASE _i.type WHEN 'type' THEN ARRAY[SUBSTR(_i.description, 6)] "
        "ELSE STRING_TO_ARRAY(_i.identity, '.') END AS object_names, "
        f"CAST(ARRAY[] AS TEXT[]) AS object_args FROM ({_IDENTIFIED}) _i",
    )
    names = "names:text[]"
    last = "names[ARRAY_LENGTH(names, 1)]"
    before = "names[ARRAY_LENGTH(names, 1) - 1]"
    relation = (
        f"(SELECT _c.oid FROM {_CLASSES} WHERE _c.relname = {last} "
        f"AND (ARRAY_LENGTH(names, 1) = 1 OR _n.nspname = {before}) LIMIT 1)"
    )
    namespace = f"(SELECT _n.oid FROM meta.catalog_pg_namespace _n WHERE _n.nspname = {last})"
    named = "ARRAY_TO_STRING(names, '.')"
    missing = {
        "table": raising("42P01", f"'relation \"' || {named} || '\" does not exist'"),
        "schema": raising("3F000", f"'schema \"' || {named} || '\" does not exist'"),
        "type": raising("42704", f"'type \"' || {named} || '\" does not exist'"),
    }
    one_name = raising("22023", "'name list length must be exactly 1'")
    found = (
        f"CASE WHEN kind IN ('schema', 'type') AND ARRAY_LENGTH(names, 1) <> 1 THEN {one_name} "
        f"ELSE CASE kind WHEN 'table' THEN COALESCE({relation}, {missing['table']}) "
        f"WHEN 'schema' THEN COALESCE({namespace}, {missing['schema']}) "
        f"WHEN 'type' THEN COALESCE({_type_lookup(last)}, {missing['type']}) ELSE "
        + raising("22023", "'unrecognized object type \"' || kind || '\"'")
        + " END END"
    )
    a.record(
        "pg_get_object_address",
        f"kind, {names}, args",
        (
            (
                "classid",
                "OID",
                "CASE kind WHEN 'table' THEN 1259 WHEN 'schema' THEN 2615 WHEN 'type' THEN 1247 END",
            ),
            ("objid", "OID", found),
            ("objsubid", "INTEGER", "0"),
        ),
    )


def _walfile(a: _Answers) -> None:
    """pg_walfile_name, which PostgreSQL 17 reads as the file holding the position itself, and
    earlier servers as the file before it: PostgreSQL 17's on every server."""
    part = "CAST(CAST('x' || LPAD(SPLIT_PART(lsn, '/', {n}), 16, '0') AS BIT(64)) AS BIGINT)"
    position = f"({part.format(n=1)} * 4294967296 + {part.format(n=2)})"
    segment = f"({position} / {WAL_SEGMENT})"
    written = (
        "UPPER('00000001' "
        f"|| LPAD(pg_catalog.to_hex({segment} / 256), 8, '0') "
        f"|| LPAD(pg_catalog.to_hex({segment} % 256), 8, '0'))"
    )
    a.strict("pg_walfile_name", "lsn", "TEXT", written)


def _catalog_objects(a: _Answers) -> None:
    """The functions on what the catalog holds, answered from it."""
    _walfile(a)
    _to_reg(a)
    _addresses(a)
    a.selected(
        "pg_available_extension_versions",
        "",
        "SELECT _e.extname AS name, _e.extversion AS version, "
        "CAST(NULL AS BOOLEAN) AS superuser, CAST(NULL AS BOOLEAN) AS trusted, "
        "CAST(NULL AS BOOLEAN) AS relocatable, CAST(NULL AS TEXT) AS schema, "
        "CAST(NULL AS TEXT[]) AS requires, CAST(NULL AS TEXT) AS comment "
        "FROM meta.catalog_pg_extension _e",
    )
    a.selected(
        "pg_available_extensions",
        "",
        "SELECT _e.name, _e.default_version, _e.comment FROM meta.catalog_pg_available_extensions _e",
    )
    a.rows(
        "pg_cursor",
        "",
        (
            ("name", "TEXT"), ("statement", "TEXT"), ("is_holdable", "BOOLEAN"),
            ("is_binary", "BOOLEAN"), ("is_scrollable", "BOOLEAN"),
            ("creation_time", "TIMESTAMPTZ"),
        ),
        "FALSE",
    )  # fmt: skip
    a.selected("pg_show_all_settings", "", "SELECT * FROM meta.catalog_pg_settings")
    a.body("getdatabaseencoding", "", "'UTF8'")
    a.body("pg_client_encoding", "", "'UTF8'")
    a.body("pg_statistics_obj_is_visible", "s", "TRUE", "BOOLEAN")
    a.strict("pg_sequence_last_value", "seq", "BIGINT", "CAST(NULL AS BIGINT)")
    partitionable = f"FROM {_CLASSES} WHERE _c.oid = rel AND _c.relkind IN ('r', 'p', 'i', 'I')"
    a.selected(
        "pg_partition_tree",
        "rel:bigint",
        "SELECT _c.oid AS relid, CAST(NULL AS OID) AS parentrelid, "
        f"_c.relkind NOT IN ('p', 'I') AS isleaf, 0 AS level {partitionable}",
    )
    a.selected(
        "pg_partition_ancestors",
        "rel:bigint",
        f"SELECT _c.oid AS relid {partitionable}",
        "_r.relid",
    )
    # As a result column: a regclass, sent with its oid and its name together.
    a.selected(
        "pg_partition_ancestors_named",
        "rel:bigint",
        f"SELECT _c.oid AS relid {partitionable}",
        _pair("_r.relid", _NAME_OF_RELID),
    )
    a.strict(
        "pg_partition_root",
        "rel:bigint",
        "OID",
        f"(SELECT _c.oid {partitionable})",
    )
    a.strict("pg_get_replica_identity_index", "rel", "OID", "CAST(NULL AS OID)")
    a.strict("pg_get_statisticsobjdef_expressions", "s", "TEXT[]", "CAST(NULL AS TEXT[])")
    for name in _ALWAYS_PRIVILEGED:
        a.strict(name, "thing, priv", "BOOLEAN", "TRUE")
        a.strict(name, "who, thing, priv", "BOOLEAN", "TRUE")
    a.strict("pg_blocking_pids", "pid", "INTEGER[]", "CAST(ARRAY[] AS INTEGER[])")
    a.body("pg_current_logfile", "", "CAST(NULL AS TEXT)")
    a.body("pg_current_logfile", "format", "CAST(NULL AS TEXT)")
    a.strict("pg_stat_reset_single_table_counters", "tbl", "TEXT", _VOID)
    a.strict("oidvectortypes", "types", "TEXT", _type_names("types", ", ", False))
    a.rows(
        "pg_wal_summary_contents",
        "tli, start_lsn, end_lsn",
        (
            ("relfilenode", "BIGINT"), ("reltablespace", "BIGINT"), ("reldatabase", "BIGINT"),
            ("relforknumber", "SMALLINT"), ("relblocknumber", "BIGINT"),
            ("is_limit_block", "BOOLEAN"),
        ),
        "false",
    )  # fmt: skip
    a.strict(
        "currtid2",
        "rel, tid",
        "TEXT",
        raising("42P01", "'relation \"' || rel || '\" does not exist'"),
    )
    a.strict("pg_basetype", "t:oid", "OID", "t")
    rights = " || ".join(
        f"CASE WHEN POSITION('{word}' IN UPPER(priv)) > 0 THEN '{letter}' "
        "|| CASE WHEN grantable THEN '*' ELSE '' END ELSE '' END"
        for letter, word in _ACL_RIGHTS
    )
    role_name = (
        "COALESCE((SELECT CAST(_r.rolname AS TEXT) FROM meta.catalog_pg_roles _r "
        "WHERE _r.oid = {oid}), {other})"
    )
    a.strict(
        "makeaclitem",
        "grantee:bigint, grantor:bigint, priv, grantable:boolean",
        "TEXT",
        f"CASE WHEN ({rights}) = '' THEN "
        + raising("22023", "'unrecognized privilege type: \"' || priv || '\"'")
        + " ELSE "
        + role_name.format(oid="grantee", other="''")
        + f" || '=' || {rights} || '/' || "
        + role_name.format(oid="grantor", other="CAST(grantor AS TEXT)")
        + " END",
    )
    a.strict("acldefault", "kind, owner", "TEXT[]", "CAST(NULL AS TEXT[])")
    a.strict(
        "pg_collation_actual_version",
        "c:bigint",
        "VARCHAR",
        f"CASE WHEN c IN (SELECT _c.oid FROM _pg_collation _c) OR c IN {_BUILT_IN_COLLATIONS} "
        "THEN CAST(NULL AS VARCHAR) ELSE "
        + raising("42704", "'collation with OID ' || c || ' does not exist'")
        + " END",
    )
    a.strict(
        "pg_database_collation_actual_version",
        "d:bigint",
        "VARCHAR",
        "CASE WHEN d IN (SELECT _d.oid FROM _pg_database _d) THEN CAST(NULL AS VARCHAR) ELSE "
        + raising("42704", "'database with OID ' || d || ' does not exist'")
        + " END",
    )
    is_table = "(SELECT _c.relkind IN ('r', 'p') FROM _pg_class _c WHERE _c.oid = rel)"
    a.strict(
        "pg_relation_is_updatable",
        "rel:bigint, cascaded",
        "INTEGER",
        f"CASE WHEN coalesce({is_table}, false) THEN 28 ELSE 0 END",
    )
    a.strict(
        "pg_column_is_updatable",
        "rel:bigint, att, cascaded",
        "BOOLEAN",
        f"coalesce({is_table}, false)",
    )
    a.strict("pg_filenode_relation", "tablespace, filenode", "OID", "CAST(NULL AS OID)")
    a.strict("pg_relation_filenode", "rel", "BIGINT", "CAST(NULL AS BIGINT)")
    a.strict("pg_relation_filepath", "rel", "VARCHAR", "CAST(NULL AS VARCHAR)")
    a.strict("pg_get_function_arg_default", "f, n", "VARCHAR", "CAST(NULL AS VARCHAR)")
    a.strict("pg_get_statisticsobjdef", "s", "VARCHAR", "CAST(NULL AS VARCHAR)")
    phases = " ".join(f"WHEN {n} THEN '{name}'" for n, name in _BTREE_BUILD_PHASES)
    a.strict(
        "pg_indexam_progress_phasename",
        "am:bigint, n:bigint",
        "VARCHAR",
        f"CASE WHEN am = {_BTREE} THEN CASE n {phases} END END",
    )
    a.strict("pg_isolation_test_session_is_blocked", "pid, pids", "BOOLEAN", "FALSE")
    a.strict("pg_safe_snapshot_blocking_pids", "pid", "INTEGER[]", "CAST(ARRAY[] AS INTEGER[])")
    a.strict("pg_log_backend_memory_contexts", "pid", "BOOLEAN", "false")
    a.body("pg_jit_available", "", "FALSE", "BOOLEAN")
    a.body("pg_notification_queue_usage", "", "CAST(0 AS DOUBLE PRECISION)", "DOUBLE PRECISION")
    a.body("pg_trigger_depth", "", "0", "INTEGER")
    a.strict("row_security_active", "rel", "BOOLEAN", "false")
    a.strict(
        "pg_settings_get_flags",
        "name",
        "VARCHAR[]",
        "(SELECT CAST(ARRAY[] AS TEXT[]) FROM _pg_settings _s WHERE _s.name = LOWER(name))",
    )
    a.strict(
        "pg_extension_config_dump",
        "rel, cond",
        "VARCHAR",
        raising(
            "55000",
            "'pg_extension_config_dump() can only be called from an SQL script executed by "
            "CREATE EXTENSION'",
        ),
    )
    a.strict(
        "pg_nextoid",
        "rel, att, idx",
        "BIGINT",
        raising("42501", "'must be superuser to call pg_nextoid()'"),
    )
    a.strict(
        "pg_import_system_collations",
        "ns",
        "INTEGER",
        raising("42501", "'must be superuser to import system collations'"),
    )
    a.strict(
        "pg_sequence_parameters",
        "seq",
        "TEXT",
        raising("42P01", "'relation with OID ' || seq || ' does not exist'"),
    )
    a.rows(
        "pg_extension_update_paths",
        "name",
        (("source", "VARCHAR"), ("target", "VARCHAR"), ("path", "VARCHAR")),
        "name IS NOT NULL AND "
        + raising("0A000", "'extension \"' || name || '\" is not available'"),
    )
    a.selected(
        "pg_tablespace_databases",
        "t:bigint",
        "SELECT _d.oid AS pg_tablespace_databases FROM meta.catalog_pg_database _d WHERE t = 1663",
        "_r.pg_tablespace_databases",
    )
    for name in ("pg_mcv_list", "pg_ndistinct", "pg_dependencies", "pg_node_tree"):
        # No value of these types exists here: only a NULL reaches them.
        a.body(f"{name}_send", "v", "CAST(NULL AS BYTEA)", "BYTEA")
    a.rows(
        "pg_mcv_list_items",
        "v",
        (
            ("index", "INTEGER"), ("values", "VARCHAR[]"), ("nulls", "BOOLEAN[]"),
            ("frequency", "DOUBLE"), ("base_frequency", "DOUBLE"),
        ),
        "false",
    )  # fmt: skip
    for name in (
        "fmgr_c_validator",
        "fmgr_internal_validator",
        "fmgr_sql_validator",
        "plpgsql_validator",
    ):
        a.strict(name, "f", "VARCHAR", raising("XX000", "'cache lookup failed for function ' || f"))
    a.strict(
        "amvalidate",
        "c",
        "BOOLEAN",
        raising("XX000", "'cache lookup failed for operator class ' || c"),
    )
    a.strict("postgresql_fdw_validator", "options, catalog", "BOOLEAN", "true")
    a.strict(
        "aclinsert",
        "acl, item",
        "VARCHAR[]",
        raising("0A000", "'aclinsert is no longer supported'"),
    )
    a.strict(
        "aclremove",
        "acl, item",
        "VARCHAR[]",
        raising("0A000", "'aclremove is no longer supported'"),
    )
    # Any role may set what the catalog holds: Provisa keeps no ACLs.
    privilege = (
        "CASE WHEN UPPER(priv) NOT IN ('SET', 'ALTER SYSTEM', 'SET WITH GRANT OPTION', "
        "'ALTER SYSTEM WITH GRANT OPTION') THEN "
        + raising("22023", "'unrecognized privilege type: \"' || priv || '\"'")
        + " ELSE TRUE END"
    )
    a.strict("has_parameter_privilege", "param, priv", "BOOLEAN", privilege)
    a.strict("has_parameter_privilege", "who, param, priv", "BOOLEAN", privilege)


def _answers() -> _Answers:
    answers = _Answers()
    _server_objects(answers)
    _xid(answers)
    _catalog_objects(answers)
    _files(answers)
    return answers


def _files(a: _Answers) -> None:
    # A reload of a configuration Provisa does not read from a file: nothing to do, and
    # PostgreSQL's signal to its postmaster answers true.
    a.body("pg_reload_conf", "", "TRUE", "BOOLEAN")
    # As a server whose wal_level is minimal refuses it: Provisa writes no WAL.
    a.strict(
        "pg_create_restore_point",
        "name",
        "TEXT",
        raising("55000", "'WAL level not sufficient for creating a restore point'"),
    )
    _read_file(a, "pg_read_file", "TEXT")
    _read_file(a, "pg_read_binary_file", "BYTEA")
    a.body("setval", "seq:bigint, value", _setval("seq", "value"), "BIGINT")
    a.body("setval", "seq:bigint, value, called", _setval("seq", "value", "called"), "BIGINT")
    for params, missing_ok in (("f", "FALSE"), ("f, missing_ok:boolean", "missing_ok")):
        nulls = "f IS NULL OR " + (missing_ok + " IS NULL")
        check = (
            f"CASE WHEN {nulls} THEN NULL ELSE "
            + _missing_file("f", missing_ok, "stat", "TEXT")
            + " END"
        )
        a.body(
            "pg_stat_file",
            params,
            f"CASE WHEN ({check}) IS NULL THEN CAST(NULL AS TEXT) END",
        )
        columns = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in _STAT_COLUMNS)
        a.table.setdefault("pg_stat_file", []).append(
            (params, _typed(f"SELECT {columns} FROM (SELECT {check} AS _s) _f", "TEXT"))
        )


#: A parameter's name and the type its argument reads as.
def parameters(params: str) -> tuple[tuple[str, str], ...]:
    return tuple(
        (name, sql_type or "text")
        for name, _, sql_type in (p.partition(":") for p in params.split(", ") if p)
    )


#: Each function's value forms and FROM forms, by name and arity.
SCALAR_FORMS: dict[tuple[str, int], tuple[tuple[tuple[str, str], ...], str]] = {}
TABLE_FORMS: dict[tuple[str, int], tuple[tuple[tuple[str, str], ...], str]] = {}
_built = _answers()
for _kind, _forms in ((_built.scalar, SCALAR_FORMS), (_built.table, TABLE_FORMS)):
    for _name_of, _overloads in _kind.items():
        for _params, _sql in _overloads:
            _typed_params = parameters(_params)
            _forms[(_name_of, len(_typed_params))] = (
                _typed_params,
                _catalog_relations(_typed(_sql, "TEXT")),
            )
