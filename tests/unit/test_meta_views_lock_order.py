# Copyright (c) 2026 Kenneth Stott
# Canary: 5e7a1c93-2b84-4d60-9f3a-c18e6d02b457
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A meta view that reads registered_tables locks it before table_columns, as schema.sql does.

schema.sql's ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` blocks take ACCESS EXCLUSIVE on
``registered_tables`` and then on ``table_columns`` in one transaction. A reader that parse-locks
``table_columns`` first and then waits for ``registered_tables`` deadlocks with it. PostgreSQL locks
a view's relations in the order of the view's stored range table, so the order is read off the
server, not the SQL: a transaction holds ACCESS EXCLUSIVE on ``registered_tables``, a reader selects
from each view, and the reader, once it waits on that lock, must hold no lock on ``table_columns``.
"""

from __future__ import annotations

import threading
import time

import psycopg
from psycopg import sql

from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_REPORT_VIEWS

_SCHEMA = "org_default"
_STEP_TIMEOUT = 30.0
_SEARCH_PATH = "SET search_path TO org_default, public"
_VIEWS = {**_META_TABLE_VIEWS, **_OPS_REPORT_VIEWS}


def _made(conninfo: str) -> list[str]:
    """Every built-in view that can be made on the template's layout, made; returns the ones that
    name table_columns."""
    made: list[str] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(_SEARCH_PATH)
        for ddl in _VIEWS.values():
            name = ddl.split("VIEW", 1)[1].split()[0]
            try:
                conn.execute(ddl.replace("%", "%%").encode())
            except psycopg.errors.UndefinedTable:
                continue  # reads a table of another domain's layout; not this lock order's
            made.append(name)
    return made


def _reads_both(conninfo: str, view: str) -> bool:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(_SEARCH_PATH)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT c.relname FROM pg_depend d "
                "JOIN pg_rewrite r ON r.oid = d.objid "
                "JOIN pg_class c ON c.oid = d.refobjid "
                "WHERE r.ev_class = %s::regclass AND c.relkind = 'r'",
                (view,),
            )
        }
    return {"registered_tables", "table_columns"} <= tables


def _holds_table_columns_while_waiting(conninfo: str, view: str) -> bool:
    """Whether a reader of ``view``, blocked on a lock on registered_tables, holds table_columns."""
    result: dict[str, object] = {}
    with (
        psycopg.connect(conninfo) as ddl,
        psycopg.connect(conninfo, autocommit=True) as monitor,
    ):
        ddl.execute(_SEARCH_PATH)
        ddl.execute("LOCK TABLE registered_tables IN ACCESS EXCLUSIVE MODE")

        def read() -> None:
            try:
                with psycopg.connect(conninfo, autocommit=True) as reader:
                    reader.execute(_SEARCH_PATH)
                    result["pid"] = reader.execute("SELECT pg_backend_pid()").fetchone()[0]  # type: ignore[index]
                    reader.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(sql.Identifier(view)))
            except BaseException as exc:  # handed to the test thread
                result["error"] = exc

        thread = threading.Thread(target=read, name=f"reader-{view}")
        thread.start()
        deadline = time.monotonic() + _STEP_TIMEOUT
        held: bool | None = None
        while time.monotonic() < deadline:
            pid = result.get("pid")
            if pid is None:
                continue
            row = monitor.execute(
                "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,)
            ).fetchone()
            if row is not None and row[0] == "Lock":
                held = bool(
                    monitor.execute(
                        "SELECT 1 FROM pg_locks WHERE pid = %s AND granted "
                        "AND relation = %s::regclass",
                        (pid, f"{_SCHEMA}.table_columns"),
                    ).fetchone()
                )
                break
        ddl.rollback()
        thread.join(_STEP_TIMEOUT)
        assert not thread.is_alive(), f"the read of {view} never finished"
        if "error" in result:
            raise result["error"]  # type: ignore[misc]
        assert held is not None, f"the read of {view} never waited on registered_tables"
        return held


def test_no_meta_view_locks_table_columns_before_registered_tables(control_plane):
    conninfo = control_plane.url.replace("+psycopg", "")
    made = _made(conninfo)
    both = [name for name in made if _reads_both(conninfo, name)]
    assert "table_columns_meta" in both
    crossed = [name for name in both if _holds_table_columns_while_waiting(conninfo, name)]
    assert crossed == [], f"views that lock table_columns before registered_tables: {crossed}"
