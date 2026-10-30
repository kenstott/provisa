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

from provisa.pgwire.pg_error import raising

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
        + raising("42809", f"'\"' || coalesce({catalog}, {relation}) || '\" is not a sequence'")
        + " ELSE "
        + raising("XX000", f"'could not open relation with OID ' || {named}")
        + " END"
    )


def _catalog_relation_names() -> str:
    from provisa.pgwire.catalog_rewrite import _CATALOG_CLASS_OIDS

    known = " ".join(f"WHEN {oid} THEN '{name}'" for name, oid in _CATALOG_CLASS_OIDS.items())
    return f"CASE seq {known} END"


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
    )


_STAT_ARITIES = (("f", ("f", "false")), ("f, missing_ok", ("f", "missing_ok")))

#: The functions these macros answer, which a statement reading no table is answered with.
ANSWERED = frozenset(
    {
        "pg_create_restore_point",
        "pg_read_binary_file",
        "pg_read_file",
        "pg_reload_conf",
        "pg_stat_file",
        "setval",
    }
)
