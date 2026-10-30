# Copyright (c) 2026 Kenneth Stott
# Canary: b0a2c47e-19f6-4d38-a75e-6c3e81d2f904
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server functions the catalog DB answers as macros (REQ-2167, CLIENT-CALLED SERVER FUNCTIONS
ANSWER LIKE POSTGRESQL): each as a PostgreSQL server with no such file, sequence or other object
answers it. A function is strict where pg_proc says so (a NULL argument is a NULL result), answers
what needs no object, and otherwise raises PostgreSQL's own error. None of them reads a server
file or changes anything: Provisa has no server files, sequences or WAL a client may reach."""

# Requirements: REQ-2167

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.core.pg_error import PostgresError, raising

#: PostgreSQL's types the catalog DB has no type for, each what its values are held as there.
_HELD_AS = {
    "VARCHAR": (
        "pg_lsn", "pg_snapshot", "txid_snapshot", "aclitem", "macaddr", "macaddr8", "cidr",
        "tsvector", "pg_mcv_list", "pg_ndistinct", "pg_dependencies", "pg_node_tree",
        "box", "circle", "line", "lseg", "path", "polygon", "point", "xml", "varbit",
    ),
    "UBIGINT": ("xid8",),
    "BIGINT": (
        "regcollation", "regconfig", "regdictionary", "regoper", "regoperator", "regprocedure",
        "regrole",
    ),
    "DECIMAL(18,2)": ("money",),
}  # fmt: skip
#: The aliases that let a statement name them in a cast.
PG_ONLY_TYPES = tuple(
    f"CREATE TYPE {name} AS {held}" for held, names in _HELD_AS.items() for name in names
)
#: Their names, as a cast to one is written.
PG_ONLY_TYPE_NAMES = frozenset(name for names in _HELD_AS.values() for name in names)

_NO_FILE = "No such file or directory"
_FILE_REFUSALS = (
    # convert_and_check_filename, for a role without pg_read_server_files.
    ("starts_with({f}, '/')", raising("42501", "'absolute path not allowed'")),
    (
        "({f} = '..' OR starts_with({f}, '../') OR ends_with({f}, '/..') OR contains({f}, '/../'))",
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


def _read_file(name: str, sql_type: str) -> str:
    """pg_read_file or pg_read_binary_file: (filename [, offset, length] [, missing_ok])."""

    def read(path: str, missing_ok: str, length: str | None = None) -> str:
        nulls = " OR ".join(
            f"{a} IS NULL" for a in (path, missing_ok, *(() if length is None else ("off", length)))
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

    return (
        f"CREATE MACRO {name}(f) AS {read('f', 'false')}, "
        f"(f, missing_ok) AS {read('f', 'missing_ok')}, "
        f"(f, off, len) AS {read('f', 'false', 'len')}, "
        f"(f, off, len, missing_ok) AS {read('f', 'missing_ok', 'len')}"
    )


_STAT_COLUMNS = (
    ("size", "BIGINT"),
    ("access", "TIMESTAMPTZ"),
    ("modification", "TIMESTAMPTZ"),
    ("change", "TIMESTAMPTZ"),
    ("creation", "TIMESTAMPTZ"),
    ("isdir", "BOOLEAN"),
)
_STAT_STRUCT = "STRUCT(" + ", ".join(f"{c} {t}" for c, t in _STAT_COLUMNS) + ")"


def _stat(path: str, missing_ok: str) -> str:
    return (
        f"CASE WHEN {path} IS NULL OR {missing_ok} IS NULL THEN CAST(NULL AS {_STAT_STRUCT}) "
        f"ELSE {_missing_file(path, missing_ok, 'stat', _STAT_STRUCT)} END"
    )


def _stat_table(path: str, missing_ok: str) -> str:
    """pg_stat_file in FROM: its one row, of NULLs where it has no file."""
    columns = ", ".join(f"_s.{c} AS {c}" for c, _ in _STAT_COLUMNS)
    return f"SELECT {columns} FROM (SELECT {_stat(path, missing_ok)} AS _s)"


def _setval(*args: str) -> str:
    """setval(sequence, value [, is_called]): no relation is a sequence."""
    nulls = " OR ".join(f"{a} IS NULL" for a in args)
    relation = "(SELECT _sc.relname FROM _pg_class _sc WHERE _sc.oid = seq)"
    named = "CAST(seq AS BIGINT)"
    catalog = _catalog_relation_names()
    return (
        f"CASE WHEN {nulls} THEN CAST(NULL AS BIGINT) "
        f"WHEN coalesce({catalog}, {relation}) IS NOT NULL THEN "
        + raising("42809", f"'cannot open relation \"' || coalesce({catalog}, {relation}) || '\"'")
        + " ELSE "
        + raising("XX000", f"'could not open relation with OID ' || {named}")
        + " END"
    )


def _catalog_relation_names() -> str:
    from provisa.pgwire.catalog_rewrite import _CATALOG_CLASS_OIDS

    known = " ".join(f"WHEN {oid} THEN '{name}'" for name, oid in _CATALOG_CLASS_OIDS.items())
    return f"CASE seq {known} END"


#: What a void function answers: PostgreSQL's void value, its empty text.
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
    """The macros of the functions below, one macro per name with one overload per arity."""

    def __init__(self) -> None:
        self.scalar: dict[str, list[str]] = {}
        self.table: dict[str, list[str]] = {}

    def body(self, name: str, params: str, sql: str) -> None:
        """``name(params)`` answers ``sql``."""
        self.scalar.setdefault(name, []).append(f"({params}) AS {sql}")

    def strict(self, name: str, params: str, sql_type: str, answer: str) -> None:
        """A strict function: a NULL argument is a NULL result of ``sql_type``."""
        self.body(name, params, _strictly(params, sql_type, answer))

    def strict_all(self, name: str, arities: tuple[str, ...], sql_type: str, answer: str) -> None:
        for params in arities:
            self.strict(name, params, sql_type, answer)

    def rows(
        self, name: str, params: str, columns: tuple[tuple[str, str], ...], where: str
    ) -> None:
        """A set-returning function of ``columns``: no rows, or (``where``) its refusal."""
        typed = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in columns)
        self.table.setdefault(name, []).append(
            f"({params}) AS TABLE (SELECT {typed} WHERE {where})"
        )
        struct = "STRUCT(" + ", ".join(f"{c} {t}" for c, t in columns) + ")"
        self.body(
            name, params, f"unnest(CASE WHEN {where} THEN [] ELSE CAST([] AS {struct}[]) END)"
        )

    def values(self, name: str, sql_type: str) -> None:
        """A set-returning function of one column, with no rows."""
        self.body(name, "", f"unnest(CAST([] AS {sql_type}[]))")
        self.table.setdefault(name, []).append(
            f"() AS TABLE (SELECT CAST(NULL AS {sql_type}) AS {name} WHERE false)"
        )

    def record(self, name: str, params: str, columns: tuple[tuple[str, str, str], ...]) -> None:
        """A record-returning function: its one row, strict in ``params``."""
        struct = "STRUCT(" + ", ".join(f"{c} {t}" for c, t, _ in columns) + ")"
        fields = ", ".join(f"{c} := CAST({v} AS {t})" for c, t, v in columns)
        value = _strictly(params, struct, f"struct_pack({fields})")
        self.body(name, params, value)
        out = ", ".join(f"_s.{c} AS {c}" for c, _, _ in columns)
        self.table.setdefault(name, []).append(
            f"({params}) AS TABLE (SELECT {out} FROM (SELECT {value} AS _s))"
        )

    def refused_record(
        self, name: str, params: str, columns: tuple[tuple[str, str], ...], refusal: str
    ) -> None:
        """A record-returning function that refuses every non-NULL call."""
        struct = "STRUCT(" + ", ".join(f"{c} {t}" for c, t in columns) + ")"
        value = _strictly(params, struct, refusal)
        self.body(name, params, value)
        out = ", ".join(f"_s.{c} AS {c}" for c, _ in columns)
        self.table.setdefault(name, []).append(
            f"({params}) AS TABLE (SELECT {out} FROM (SELECT {value} AS _s))"
        )

    def statements(self) -> tuple[str, ...]:
        scalar = tuple(f"CREATE MACRO {n}{', '.join(o)}" for n, o in self.scalar.items())
        table = tuple(f"CREATE MACRO {n}{', '.join(o)}" for n, o in self.table.items())
        return scalar + table


def _strictly(params: str, sql_type: str, answer: str) -> str:
    if not params:
        return answer
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


_LSN_NUMBER = (
    "((CAST('0x' || split_part({l}, '/', 1) AS BIGINT) << 32) + "
    "CAST('0x' || split_part({l}, '/', 2) AS BIGINT))"
)
_WAL_SEGMENT = 16777216
_SEGMENTS_PER_ID = 256


def _lsn(value: str) -> str:
    return _LSN_NUMBER.format(l=value)


def _wal_file(value: str) -> str:
    segment = f"({_lsn(value)} // {_WAL_SEGMENT})"
    return (
        f"upper(printf('%08x%08x%08x', 1, {segment} // {_SEGMENTS_PER_ID}, "
        f"{segment} % {_SEGMENTS_PER_ID}))"
    )


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
    a.body("pg_replication_origin_session_is_setup", "", "false")
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
    a.body("pg_last_xact_replay_timestamp", "", "CAST(NULL AS TIMESTAMPTZ)")
    a.strict_all("pg_promote", ("", "do_wait", "do_wait, seconds"), "BOOLEAN", _NOT_IN_RECOVERY)
    backup_level = raising("55000", "'WAL level not sufficient for making an online backup'")
    a.strict_all("pg_backup_start", ("label", "label, fast"), "VARCHAR", backup_level)
    stop = (("lsn", "VARCHAR"), ("labelfile", "VARCHAR"), ("spcmapfile", "VARCHAR"))
    for params in ("", "archive"):
        a.refused_record(
            "pg_backup_stop", params, stop, raising("55000", "'backup is not in progress'")
        )
    a.body("pg_rotate_logfile", "", "false")
    a.strict("pg_walfile_name", "lsn", "VARCHAR", _wal_file("lsn"))
    a.record(
        "pg_walfile_name_offset",
        "lsn",
        (
            ("file_name", "VARCHAR", _wal_file("lsn")),
            ("file_offset", "INTEGER", f"{_lsn('lsn')} % {_WAL_SEGMENT}"),
        ),
    )
    a.strict(
        "pg_wal_lsn_diff",
        "a, b",
        "DECIMAL(38,0)",
        f"CAST({_lsn('a')} AS DECIMAL(38,0)) - CAST({_lsn('b')} AS DECIMAL(38,0))",
    )
    a.strict(
        "pg_lsn",
        "value",
        "VARCHAR",
        "CASE WHEN value < 0 OR value >= 18446744073709551616 THEN "
        + raising("22023", "'pg_lsn out of range'")
        + " ELSE upper(printf('%x/%x', CAST(value AS UBIGINT) >> 32, "
        "CAST(value AS UBIGINT) & 4294967295)) END",
    )
    a.record(
        "pg_split_walfile_name",
        "f",
        (
            (
                "segment_number",
                "DECIMAL(38,0)",
                "CASE WHEN regexp_full_match(f, '[0-9A-F]{24}') THEN "
                "(CAST('0x' || substr(f, 9, 8) AS BIGINT) * 256) + "
                "CAST('0x' || substr(f, 17, 8) AS BIGINT) ELSE "
                + raising("22023", "'invalid WAL file name \"' || f || '\"'")
                + " END",
            ),
            ("timeline_id", "BIGINT", "CAST('0x' || substr(f, 1, 8) AS BIGINT)"),
        ),
    )
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
            ("bytes_per_wal_segment", "INTEGER", str(_WAL_SEGMENT)),
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
        "tablespace",
        named,
        "(tablespace IS NULL OR tablespace NOT IN (1663, 1664)) AND "
        "(tablespace IS NOT NULL AND "
        + raising("42704", "'tablespace with OID ' || tablespace || ' does not exist'")
        + ")",
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
        "CASE WHEN f IS NULL THEN CAST(NULL AS VARCHAR) ELSE "
        + _missing_directory("f", "false")
        + " END",
    )
    a.body(
        "pg_ls_dir",
        "f, missing_ok, include_dots",
        "CASE WHEN f IS NULL THEN CAST(NULL AS VARCHAR) ELSE "
        + _missing_directory("f", "coalesce(missing_ok, false)")
        + " END",
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
        ("loread", "fd, len", "BLOB"),
        ("lowrite", "fd, data", "INTEGER"),
    ):
        a.strict(name, params, sql_type, bad_descriptor)
    for name, params, sql_type in (
        ("lo_open", "lobj, mode", "INTEGER"),
        ("lo_get", "lobj", "BLOB"),
        ("lo_get", "lobj, off, len", "BLOB"),
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
    refusals = " ".join(
        f"WHEN {test.format(f=path)} THEN {refusal}" for test, refusal in _FILE_REFUSALS
    )
    missing = raising("58P01", f"'could not open directory \"' || {path} || '\": {_NO_FILE}'")
    return (
        f"unnest(CASE {refusals} WHEN {missing_ok} THEN CAST([] AS VARCHAR[]) ELSE {missing} END)"
    )


def _xid(a: _Answers) -> None:
    """Transaction ids and snapshots: nothing runs but the statement asking, which has no xid."""
    a.body("pg_current_xact_id", "", "provisa_next_xid()")
    a.body("pg_current_xact_id_if_assigned", "", "CAST(NULL AS BIGINT)")
    a.body("txid_current_if_assigned", "", "CAST(NULL AS BIGINT)")
    snapshot = "(SELECT _n || ':' || _n || ':' FROM (SELECT provisa_next_xid() AS _n))"
    a.body("pg_current_snapshot", "", snapshot)
    a.body("txid_current_snapshot", "", snapshot)
    xip = (
        "list_transform(string_split(nullif(split_part(s, ':', 3), ''), ','), "
        "_x -> CAST(_x AS BIGINT))"
    )
    xmin, xmax = "CAST(split_part(s, ':', 1) AS BIGINT)", "CAST(split_part(s, ':', 2) AS BIGINT)"
    for family, name in (
        ("pg_snapshot", "pg_visible_in_snapshot"),
        ("txid_snapshot", "txid_visible_in_snapshot"),
    ):
        a.strict(f"{family}_xmin", "s", "BIGINT", xmin)
        a.strict(f"{family}_xmax", "s", "BIGINT", xmax)
        a.body(f"{family}_xip", "s", f"unnest({xip})")
        a.strict(
            f"{family}_send",
            "s",
            "BLOB",
            f"unhex(printf('%08x%016x%016x', coalesce(len({xip}), 0), {xmin}, {xmax}) || "
            f"coalesce(array_to_string(list_transform({xip}, _x -> printf('%016x', _x)), ''), ''))",
        )
        a.strict(
            name,
            "x, s",
            "BOOLEAN",
            f"CASE WHEN x < {xmin} THEN true WHEN x >= {xmax} THEN false "
            f"ELSE NOT coalesce(list_contains({xip}, x), false) END",
        )
    status = (
        "CASE WHEN x >= provisa_next_xid() THEN "
        + raising("22023", "'transaction ID ' || x || ' is in the future'")
        + " ELSE 'committed' END"
    )
    a.strict("pg_xact_status", "x", "VARCHAR", status)
    a.strict("txid_status", "x", "VARCHAR", status)
    no_commit_ts = raising("55000", "'could not get commit timestamp data'")
    a.strict("pg_xact_commit_timestamp", "x", "TIMESTAMPTZ", no_commit_ts)
    a.refused_record(
        "pg_xact_commit_timestamp_origin",
        "x",
        (("timestamp", "TIMESTAMPTZ"), ("roident", "BIGINT")),
        no_commit_ts,
    )
    a.strict(
        "provisa_xid_age",
        "x",
        "INTEGER",
        "CASE WHEN x < 3 THEN 2147483647 ELSE CAST(provisa_next_xid() - x AS INTEGER) END",
    )
    a.strict(
        "mxid_age",
        "x",
        "INTEGER",
        "CASE WHEN x = 0 THEN 2147483647 ELSE CAST(1 - x AS INTEGER) END",
    )
    a.rows("pg_get_multixact_members", "m", (("xid", "BIGINT"), ("mode", "VARCHAR")), "false")
    a.body(
        "pg_export_snapshot",
        "",
        "(SELECT printf('%08X-%08X-1', 3, _n) FROM (SELECT provisa_next_xid() AS _n))",
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

#: Each encoding's longest character in bytes, by its number (common/wchar.c's pg_wchar_table).
_ENCODING_MAX_LENGTHS = (1, 3, 3, 3, 4, 3, 4, 4, *(1,) * 27, 2, 2, 2, 2, 4, 3, 2)
_NAMED_STRUCT = "STRUCT(oid BIGINT, name VARCHAR)"


def _last_part(name: str) -> str:
    return f"provisa_parse_ident({name}, true)[-1]"


def _qualifier(name: str) -> str:
    """The schema a possibly qualified name names; NULL for none."""
    parts = f"provisa_parse_ident({name}, true)"
    return f"CASE WHEN len({parts}) > 1 THEN {parts}[-2] END"


def _to_reg(a: _Answers) -> None:
    """to_regclass and its kin: an object identifier of the object a name names; NULL for none."""
    in_path = "('pg_catalog', 'public')"
    a.strict(
        "to_regclass",
        "name",
        _NAMED_STRUCT,
        "(SELECT struct_pack(oid := provisa_relation_oid(_c.relname, _c.oid), name := _c.relname) "
        "FROM _pg_class _c JOIN _pg_namespace _n ON _n.oid = _c.relnamespace "
        f"WHERE _c.relname = {_last_part('name')} AND (_n.nspname = {_qualifier('name')} "
        f"OR ({_qualifier('name')} IS NULL AND _n.nspname IN {in_path})) LIMIT 1)",
    )
    a.strict(
        "to_regnamespace",
        "name",
        _NAMED_STRUCT,
        "(SELECT struct_pack(oid := _n.oid, name := _n.nspname) FROM _pg_namespace _n "
        f"WHERE _n.nspname = {_last_part('name')})",
    )
    a.strict(
        "to_regrole",
        "name",
        "VARCHAR",
        f"(SELECT _r.rolname FROM _pg_roles _r WHERE _r.rolname = {_last_part('name')})",
    )
    a.strict(
        "to_regproc",
        "name",
        "BIGINT",
        f"(SELECT _p.oid FROM _pg_proc _p WHERE _p.proname = {_last_part('name')} LIMIT 1)",
    )
    a.strict(
        "to_regprocedure",
        "name",
        "VARCHAR",
        "(SELECT split_part(name, '(', 1) || '(' || split_part(name, '(', 2) "
        "FROM _pg_proc _p WHERE _p.proname = split_part(name, '(', 1) LIMIT 1)",
    )
    a.strict(
        "to_regoper",
        "name",
        "VARCHAR",
        "(SELECT _o.oprname FROM _pg_operator _o WHERE _o.oprname = name LIMIT 1)",
    )
    a.strict(
        "to_regoperator",
        "name",
        "VARCHAR",
        "(SELECT name FROM _pg_operator _o WHERE _o.oprname = split_part(name, '(', 1) LIMIT 1)",
    )
    a.strict(
        "to_regcollation",
        "name",
        "VARCHAR",
        f"(SELECT _c.collname FROM _pg_collation _c WHERE _c.collname = {_last_part('name')} "
        "LIMIT 1)",
    )
    a.strict("to_regtype", "name", "BIGINT", _type_lookup("name"))
    a.strict(
        "to_regtypemod",
        "name",
        "INTEGER",
        f"CASE WHEN {_type_lookup("regexp_replace(name, '\\s*\\(.*$', '')")} IS NOT NULL "
        "THEN provisa_typemod(name) END",
    )


def _type_lookup(name: str) -> str:
    return (
        "(SELECT _t.oid FROM _pg_type _t WHERE _t.typname = "
        f"provisa_type_name(lower(trim({name}))) LIMIT 1)"
    )


def _addresses(a: _Answers) -> None:
    """pg_identify_object_as_address and pg_get_object_address, for what pg_identify_object
    identifies: relations, types and schemas."""
    identified = "pg_identify_object(classid, objid, objsubid)"
    address = (
        ("type", "VARCHAR", f"{identified}.type"),
        (
            "object_names",
            "VARCHAR[]",
            f"CASE WHEN {identified}.identity IS NOT NULL "
            f"THEN string_split({identified}.identity, '.') END",
        ),
        (
            "object_args",
            "VARCHAR[]",
            f"CASE WHEN {identified}.type IS NOT NULL THEN CAST([] AS VARCHAR[]) END",
        ),
    )
    a.record("pg_identify_object_as_address", "classid, objid, objsubid", address)
    relation = (
        "(SELECT _c.oid FROM _pg_class _c JOIN _pg_namespace _n ON _n.oid = _c.relnamespace "
        "WHERE _c.relname = names[-1] AND (len(names) = 1 OR _n.nspname = names[-2]) LIMIT 1)"
    )
    namespace = "(SELECT _n.oid FROM _pg_namespace _n WHERE _n.nspname = names[-1])"
    named = "array_to_string(names, '.')"
    missing = {
        "table": raising("42P01", f"'relation \"' || {named} || '\" does not exist'"),
        "schema": raising("3F000", f"'schema \"' || {named} || '\" does not exist'"),
        "type": raising("42704", f"'type \"' || {named} || '\" does not exist'"),
    }
    one_name = raising("22023", "'name list length must be exactly 1'")
    found = (
        f"CASE WHEN kind IN ('schema', 'type') AND len(names) <> 1 THEN {one_name} ELSE "
        f"CASE kind WHEN 'table' THEN coalesce({relation}, {missing['table']}) "
        f"WHEN 'schema' THEN coalesce({namespace}, {missing['schema']}) "
        f"WHEN 'type' THEN coalesce({_type_lookup('names[-1]')}, {missing['type']}) ELSE "
        + raising("22023", "'unrecognized object type \"' || kind || '\"'")
        + " END END"
    )
    a.record(
        "pg_get_object_address",
        "kind, names, args",
        (
            (
                "classid",
                "BIGINT",
                "CASE kind WHEN 'table' THEN 1259 WHEN 'schema' THEN 2615 WHEN 'type' THEN 1247 END",
            ),
            ("objid", "BIGINT", found),
            ("objsubid", "INTEGER", "0"),
        ),
    )


def _catalog_objects(a: _Answers) -> None:
    """The functions on what the catalog holds, answered from it."""
    _to_reg(a)
    _addresses(a)
    lengths = ", ".join(str(n) for n in _ENCODING_MAX_LENGTHS)
    a.strict("pg_encoding_max_length", "e", "INTEGER", f"[{lengths}][e + 1]")
    a.body(
        "pg_available_extension_versions",
        "",
        "unnest((SELECT list(struct_pack(name := _e.extname, version := _e.extversion, "
        "superuser := CAST(NULL AS BOOLEAN), trusted := CAST(NULL AS BOOLEAN), "
        "relocatable := CAST(NULL AS BOOLEAN), schema := CAST(NULL AS VARCHAR), "
        "requires := CAST(NULL AS VARCHAR[]), comment := CAST(NULL AS VARCHAR))) "
        "FROM _pg_extension _e))",
    )
    a.body(
        "pg_cursor",
        "",
        "unnest(CAST([] AS STRUCT(name VARCHAR, statement VARCHAR, is_holdable BOOLEAN, "
        "is_binary BOOLEAN, is_scrollable BOOLEAN, creation_time TIMESTAMPTZ)[]))",
    )
    a.body(
        "pg_timezone_names",
        "",
        "unnest((SELECT list(struct_pack(name := _z.name, abbrev := _z.abbrev, "
        "utc_offset := _z.utc_offset, is_dst := _z.is_dst)) FROM pg_timezone_names() _z))",
    )
    a.body(
        "pg_timezone_abbrevs",
        "",
        "unnest((SELECT list(struct_pack(abbrev := _z.abbrev, utc_offset := _z.utc_offset, "
        "is_dst := _z.is_dst)) FROM (SELECT DISTINCT abbrev, utc_offset, is_dst "
        "FROM pg_timezone_names()) _z))",
    )
    a.table.setdefault("pg_timezone_abbrevs", []).append(
        "() AS TABLE (SELECT DISTINCT abbrev, utc_offset, is_dst FROM pg_timezone_names())"
    )
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
        "BIGINT",
        raising("42P01", "'relation \"' || rel || '\" does not exist'"),
    )
    a.strict(
        "makeaclitem",
        "grantee, grantor, priv, grantable",
        "VARCHAR",
        "provisa_aclitem(coalesce((SELECT _r.rolname FROM _pg_roles _r WHERE _r.oid = grantee), "
        "''), coalesce((SELECT _r.rolname FROM _pg_roles _r WHERE _r.oid = grantor), "
        "CAST(grantor AS VARCHAR)), priv, grantable)",
    )
    a.strict("pg_basetype", "t", "BIGINT", "t")
    a.body("pg_client_encoding", "", "'UTF8'")
    a.strict(
        "pg_collation_actual_version",
        "c",
        "VARCHAR",
        f"CASE WHEN c IN (SELECT _c.oid FROM _pg_collation _c) OR c IN {_BUILT_IN_COLLATIONS} "
        "THEN CAST(NULL AS VARCHAR) ELSE "
        + raising("42704", "'collation with OID ' || c || ' does not exist'")
        + " END",
    )
    a.strict(
        "pg_database_collation_actual_version",
        "d",
        "VARCHAR",
        "CASE WHEN d IN (SELECT _d.oid FROM _pg_database _d) THEN CAST(NULL AS VARCHAR) ELSE "
        + raising("42704", "'database with OID ' || d || ' does not exist'")
        + " END",
    )
    is_table = (
        "coalesce((SELECT _c.relkind IN ('r', 'p') FROM _pg_class _c WHERE _c.oid = rel), "
        "provisa_is_catalog_relation(rel))"
    )
    a.strict(
        "pg_relation_is_updatable",
        "rel, cascaded",
        "INTEGER",
        f"CASE WHEN coalesce({is_table}, false) THEN 28 ELSE 0 END",
    )
    a.strict(
        "pg_column_is_updatable", "rel, att, cascaded", "BOOLEAN", f"coalesce({is_table}, false)"
    )
    a.strict(
        "pg_filenode_relation",
        "tablespace, filenode",
        "STRUCT(oid BIGINT, name VARCHAR)",
        "CAST(NULL AS STRUCT(oid BIGINT, name VARCHAR))",
    )
    a.strict("pg_relation_filenode", "rel", "BIGINT", "CAST(NULL AS BIGINT)")
    a.strict("pg_relation_filepath", "rel", "VARCHAR", "CAST(NULL AS VARCHAR)")
    a.strict("pg_get_function_arg_default", "f, n", "VARCHAR", "CAST(NULL AS VARCHAR)")
    a.strict("pg_get_statisticsobjdef", "s", "VARCHAR", "CAST(NULL AS VARCHAR)")
    phases = " ".join(f"WHEN {n} THEN '{name}'" for n, name in _BTREE_BUILD_PHASES)
    a.strict(
        "pg_indexam_progress_phasename",
        "am, n",
        "VARCHAR",
        f"CASE WHEN am = {_BTREE} THEN CASE n {phases} END END",
    )
    a.strict("pg_isolation_test_session_is_blocked", "pid, pids", "BOOLEAN", "false")
    a.strict("pg_safe_snapshot_blocking_pids", "pid", "INTEGER[]", "CAST([] AS INTEGER[])")
    a.strict("pg_log_backend_memory_contexts", "pid", "BOOLEAN", "false")
    a.body("pg_jit_available", "", "false")
    a.body("pg_notification_queue_usage", "", "CAST(0 AS DOUBLE)")
    a.body("pg_trigger_depth", "", "0")
    a.strict("row_security_active", "rel", "BOOLEAN", "false")
    a.strict(
        "pg_settings_get_flags",
        "name",
        "VARCHAR[]",
        "(SELECT CAST([] AS VARCHAR[]) FROM _pg_settings _s WHERE _s.name = lower(name))",
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
        "VARCHAR",
        raising("42P01", "'relation with OID ' || seq || ' does not exist'"),
    )
    a.rows(
        "pg_extension_update_paths",
        "name",
        (("source", "VARCHAR"), ("target", "VARCHAR"), ("path", "VARCHAR")),
        "name IS NOT NULL AND "
        + raising("0A000", "'extension \"' || name || '\" is not available'"),
    )
    a.body(
        "pg_tablespace_databases",
        "t",
        "unnest((SELECT list(_d.oid) FROM _pg_database _d WHERE t = 1663))",
    )
    for name in ("pg_mcv_list", "pg_ndistinct", "pg_dependencies", "pg_node_tree"):
        # No value of these types exists here: only a NULL reaches them.
        a.body(f"{name}_send", "v", "CAST(NULL AS BLOB)")
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
    a.strict("aclitemeq", "a, b", "BOOLEAN", "a = b")
    a.strict("aclcontains", "acl, item", "BOOLEAN", "list_contains(acl, item)")
    # Any role may set what the catalog holds: Provisa keeps no ACLs.
    privilege = (
        "CASE WHEN upper(priv) NOT IN ('SET', 'ALTER SYSTEM', 'SET WITH GRANT OPTION', "
        "'ALTER SYSTEM WITH GRANT OPTION') THEN "
        + raising("22023", "'unrecognized privilege type: \"' || priv || '\"'")
        + " ELSE true END"
    )
    a.strict("has_parameter_privilege", "param, priv", "BOOLEAN", privilege)
    a.strict("has_parameter_privilege", "who, param, priv", "BOOLEAN", privilege)


_STAT_ARITIES = (("f", ("f", "false")), ("f, missing_ok", ("f", "missing_ok")))


def _answers() -> _Answers:
    answers = _Answers()
    _server_objects(answers)
    _xid(answers)
    _catalog_objects(answers)
    return answers


def macros() -> tuple[str, ...]:
    """The macros, over the catalog relations they read."""
    stat = ", ".join(f"({p}) AS {_stat(*a)}" for p, a in _STAT_ARITIES)
    stat_table = ", ".join(f"({p}) AS TABLE ({_stat_table(*a)})" for p, a in _STAT_ARITIES)
    return (
        # A reload of a configuration Provisa does not read from a file: nothing to do, and
        # PostgreSQL's signal to its postmaster answers true.
        "CREATE MACRO pg_reload_conf() AS true",
        # As a server whose wal_level is minimal refuses it: Provisa writes no WAL.
        "CREATE MACRO pg_create_restore_point(name) AS CASE WHEN name IS NULL "
        "THEN CAST(NULL AS VARCHAR) ELSE "
        + raising("55000", "'WAL level not sufficient for creating a restore point'")
        + " END",
        _read_file("pg_read_file", "VARCHAR"),
        _read_file("pg_read_binary_file", "BLOB"),
        f"CREATE MACRO pg_stat_file{stat}",
        f"CREATE MACRO pg_stat_file{stat_table}",
        "CREATE MACRO setval(seq, value) AS "
        + _setval("seq", "value")
        + ", (seq, value, called) AS "
        + _setval("seq", "value", "called"),
        *_answers().statements(),
    )


#: The functions these macros answer, which a statement reading no table is answered with.
ANSWERED = frozenset(
    {
        "pg_create_restore_point",
        "pg_read_binary_file",
        "pg_read_file",
        "pg_reload_conf",
        "pg_stat_file",
        "setval",
        "money",
        "regclass",
        *_answers().scalar,
        *_answers().table,
    }
)
#: Names the catalog's rewrite answers, a statement reading no table is answered with too.
REWRITTEN = frozenset(
    {
        "convert",
        "getpgusername",
        "json_each",
        "json_object",
        "normalize",
        "pg_indexam_has_property",
        "pg_input_error_info",
        "pg_input_is_valid",
        "system_user",
    }
)


def indexam_property(am: exp.Expr, name: exp.Expr) -> exp.Expr:
    """pg_indexam_has_property: strict; NULL for an access method or a property it has not."""
    method, prop = am.sql(dialect="duckdb"), name.sql(dialect="duckdb")
    cases = " ".join(
        f"WHEN {prop} = '{p}' THEN ({method}) IN ({', '.join(map(str, ams))})"
        for p, ams in _INDEXAM_PROPERTIES.items()
    )
    methods = ", ".join(map(str, _BUILT_IN_ACCESS_METHODS))
    return sqlglot.parse_one(
        f"CASE WHEN ({method}) IN ({methods}) THEN CASE {cases} END END", read="duckdb"
    )


def known_type(name: str) -> bool:
    """Whether PostgreSQL has a type of this name."""
    from provisa.pgwire.catalog_pg17 import TYPES

    return any(name.lower().removesuffix("[]") in (e[1], e[-1]) for e in TYPES.values())


def _type_display(name: str) -> str:
    from provisa.pgwire.catalog_pg17 import TYPES

    lowered = name.lower()
    return next((e[-1] for e in TYPES.values() if lowered in (e[1], e[-1])), lowered)


#: The integer types: their input refuses a number out of range with an error of its own.
_INTEGER_RANGES = {"smallint": 15, "integer": 31, "bigint": 63}


def input_failure(value: exp.Expr, type_name: exp.Expr) -> tuple[str, str]:
    """What the type's input function says of a literal it refuses, and its SQLSTATE."""
    display = _type_display(type_name.this)
    text = value.name
    bits = _INTEGER_RANGES.get(display)
    if bits is not None and re.fullmatch(r"\s*[-+]?\d+\s*", text):
        return f'value "{text}" is out of range for type {display}', "22003"
    return f'invalid input syntax for type {display}: "{text}"', "22P02"


def input_check(value: exp.Expr, type_name: exp.Expr) -> exp.Expr:
    """For pg_input_is_valid and pg_input_error_info: whether the value reads as the type. The
    type is named in the statement: the check is a cast to it."""
    if not (isinstance(type_name, exp.Literal) and type_name.is_string):
        raise NotImplementedError(
            "pg_input_is_valid: a type that is not a literal is not supported; the cast is "
            "written where the statement is read"
        )
    missing = PostgresError("42704", f'type "{type_name.this}" does not exist')
    try:
        to = exp.DataType.build(type_name.this, dialect="postgres", udt=True)
    except (ValueError, SqlglotError) as exc:
        raise missing from exc
    if to.this == exp.DataType.Type.USERDEFINED and not known_type(type_name.this):
        raise missing
    if to.is_type(exp.DataType.Type.ARRAY) and isinstance(value, exp.Literal):
        value = exp.Literal.string(value.this.replace("{", "[").replace("}", "]"))
    return exp.Not(this=exp.Is(this=exp.TryCast(this=value.copy(), to=to), expression=exp.null()))
