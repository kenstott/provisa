# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e5a31-9d42-4b86-a1f3-6e2d8b5c7a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The config stamp: one stored counter per kind of configuration, per control plane (REQ-1914).

``config_stamp(kind, stamp)`` holds one row per kind. The DATABASE advances a row, by trigger, in
the same transaction as any write to a table of that kind — so no write path can forget it, and
the value is one the control plane assigned, never a clock reading. Every process remembers the
stamp it loaded and reloads when the stored one differs (``provisa/core/config_watch.py``).

The stamp is used strictly for reloading. It is never what an update is checked against.

Kinds, and where their row lives:

* ``model`` — the tenant plane (one per org and environment): the governed model and its rules.
* ``settings`` — the tenant plane (``org_settings``) and the platform plane
  (``deployment_settings``), each with its own row.
* ``replica`` — the tenant plane: which tables are served from a replica because they are busy
  (REQ-826). It has no trigger. ``replica_state`` is state, written on every build and refresh;
  only two of its changes move a table between live and its replica, and the code that makes
  each advances the row itself (:func:`advance`, called by ``federation/replica_state.py``).
"""

# Requirements: REQ-1914, REQ-826, REQ-1920

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text, update

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

MODEL = "model"
SETTINGS = "settings"
REPLICA = "replica"

# Tenant-plane kinds no trigger advances: the code that makes the change calls :func:`advance`
# in the same transaction. Seeded beside the trigger-advanced kinds of TENANT_TABLES.
TENANT_ADVANCED: tuple[str, ...] = (REPLICA,)

# Tenant-plane tables and the kind a write to each advances. ``model`` is every table the governed
# model is built from (what ``_rebuild_schemas`` reads). Tables a reload itself writes, and runtime
# state (events, freshness, refresh logs, audit), are deliberately absent: stamping them would make
# every worker reload on its own bookkeeping. So is ``provisa_sources``: the grpc_remote router
# creates it on first use (an org that registered none has no such table), and every row it holds
# is written beside the ``sources`` and ``registered_tables`` rows of the same registration.
#
# THE ONE PLACE a table is added: name it here with its kind. A table here holds configuration only
# (a view's build state is mv_build_state, state, REQ-1922), so every write to it advances the
# stamp. The table must exist in the plane's schema definition (schema.sql and schema_org.py, or
# schema_admin.py).
TENANT_TABLES: dict[str, str] = {
    "sources": MODEL,
    "domains": MODEL,
    "data_products": MODEL,
    "naming_rules": MODEL,
    "registered_tables": MODEL,
    "table_columns": MODEL,
    "relationships": MODEL,
    "metrics": MODEL,
    "roles": MODEL,
    "rls_rules": MODEL,
    "tags": MODEL,
    "tag_param_values": MODEL,
    "tag_assignments": MODEL,
    "tracked_functions": MODEL,
    "tracked_webhooks": MODEL,
    "scheduled_triggers": MODEL,
    "api_sources": MODEL,
    "api_endpoints": MODEL,
    "kafka_sources": MODEL,
    "kafka_topics": MODEL,
    "kafka_sinks": MODEL,
    "table_meta_links": MODEL,
    "calendars": MODEL,
    "materialized_views": MODEL,
    "org_settings": SETTINGS,
    # REQ-1939: a dataset's tables decide what this environment's tables read.
    "synthetic_datasets": MODEL,
    "synthetic_dataset_tables": MODEL,
}

PLATFORM_TABLES: dict[str, str] = {"deployment_settings": SETTINGS}

# Serializes trigger creation across the worker processes of a launch (PostgreSQL). "PROVISA4".
_INSTALL_LOCK_KEY = 0x50524F5649534134

_PG_FUNCTION = "advance_config_stamp"


def _seed(conn: Any, qualified: str, kinds: set[str]) -> None:
    for kind in sorted(kinds):
        conn.execute(
            text(
                f"INSERT INTO {qualified} (kind, stamp) SELECT :kind, 0 "
                f"WHERE NOT EXISTS (SELECT 1 FROM {qualified} WHERE kind = :kind)"
            ),
            {"kind": kind},
        )


def _install_postgresql(conn: Any, tables: dict[str, str], schema: str | None) -> None:
    conn.execute(text(f"SELECT pg_advisory_xact_lock({_INSTALL_LOCK_KEY})"))
    if schema is None:
        schema = conn.execute(text("SELECT current_schema()")).scalar_one()
    stamp = f'"{schema}".config_stamp'
    _seed(conn, stamp, set(tables.values()))
    function = f'"{schema}".{_PG_FUNCTION}'
    # The table's own schema names the stamp it advances, so the trigger is right whatever
    # search_path the writing connection carries.
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $fn$ "
            "BEGIN "
            "EXECUTE format('UPDATE %I.config_stamp SET stamp = stamp + 1 WHERE kind = $1', "
            "TG_TABLE_SCHEMA) USING TG_ARGV[0]; "
            "RETURN NULL; "
            "END $fn$"
        )
    )
    existing = {
        (row[0], row[1])
        for row in conn.execute(
            text(
                "SELECT c.relname, t.tgname FROM pg_trigger t "
                "JOIN pg_class c ON c.oid = t.tgrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND NOT t.tgisinternal"
            ),
            {"schema": schema},
        )
    }
    # A table holding a ``json`` column (one created from the portable metadata rather than
    # schema.sql, which declares jsonb) cannot be compared row to row: json has no equality
    # operator. Its every UPDATE advances the stamp.
    uncomparable = {
        row[0]
        for row in conn.execute(
            text(
                "SELECT DISTINCT table_name FROM information_schema.columns "
                "WHERE table_schema = :schema AND data_type = 'json'"
            ),
            {"schema": schema},
        )
    }
    for table, kind in tables.items():
        target = f'"{schema}"."{table}"'
        call = f"EXECUTE FUNCTION {function}('{kind}')"
        # A row rewritten with the values it already holds is not a change.
        changed = "" if table in uncomparable else "WHEN (OLD.* IS DISTINCT FROM NEW.*) "
        wanted = {
            "config_stamp_rows": f"AFTER INSERT OR DELETE ON {target} FOR EACH ROW {call}",
            "config_stamp_update": (f"AFTER UPDATE ON {target} FOR EACH ROW {changed}{call}"),
            "config_stamp_truncate": f"AFTER TRUNCATE ON {target} FOR EACH STATEMENT {call}",
        }
        for name, definition in wanted.items():
            if (table, name) not in existing:
                conn.execute(text(f"CREATE TRIGGER {name} {definition}"))


def _install_sqlite(conn: Any, tables: dict[str, str]) -> None:
    _seed(conn, "config_stamp", set(tables.values()))
    for table, kind in tables.items():
        advance = f"UPDATE config_stamp SET stamp = stamp + 1 WHERE kind = '{kind}'"
        for name, event in (
            ("insert", "INSERT"),
            ("delete", "DELETE"),
            ("update", "UPDATE"),
        ):
            conn.exec_driver_sql(
                f'CREATE TRIGGER IF NOT EXISTS "config_stamp_{table}_{name}" '
                f'AFTER {event} ON "{table}" BEGIN {advance}; END'
            )


def install(
    conn: Any, tables: dict[str, str], schema: str | None = None, *, advanced: tuple[str, ...] = ()
) -> None:
    """Seed the stamp rows and create the triggers that advance them, for ``tables`` (table name →
    kind) in ``schema``. Idempotent. ``conn`` is a SQLAlchemy connection inside a transaction, on
    which ``config_stamp`` and every one of ``tables`` already exist. ``advanced``: the kinds of
    this plane that have a row and no trigger (``TENANT_ADVANCED``).

    An embedded DuckDB control plane (REQ-828) gets the rows and no triggers: DuckDB has none,
    and a DuckDB file admits one process, so there is no second process for a change to reach —
    the process that made the change rebuilds itself. A control plane on any other dialect is
    refused: without the triggers a change would reach only the process that made it."""
    dialect = conn.dialect.name
    if dialect == "postgresql":
        _install_postgresql(conn, tables, schema)
        if schema is None:
            schema = conn.execute(text("SELECT current_schema()")).scalar_one()
        _seed(conn, f'"{schema}".config_stamp', set(advanced))
    elif dialect == "sqlite":
        _install_sqlite(conn, tables)
        _seed(conn, "config_stamp", set(advanced))
    elif dialect == "duckdb":
        _seed(conn, "config_stamp", set(tables.values()) | set(advanced))
    else:
        raise NotImplementedError(
            f"the config stamp (REQ-1914) is not implemented for a {dialect} control plane; "
            "PostgreSQL, SQLite and (single-process) DuckDB are supported"
        )


async def advance(conn: "Connection", kind: str) -> None:
    """Advance the stamp of ``kind`` on ``conn``'s plane, for a kind no trigger advances
    (``TENANT_ADVANCED``). Called inside the transaction of the change it announces, so the
    change and its stamp are stored together or not at all."""
    from provisa.core.schema_org import config_stamp as stamps

    result = await conn.execute_core(
        update(stamps).where(stamps.c.kind == kind).values(stamp=stamps.c.stamp + 1)
    )
    if result.rowcount != 1:
        raise RuntimeError(
            f"config stamp {kind!r} has no row on this control plane (REQ-1914): the plane was "
            "set up without it, so no process would learn of this change"
        )


async def read(db: "Database") -> dict[str, int]:
    """The stored stamps of ``db``'s plane, by kind. One short statement."""
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT kind, stamp FROM config_stamp")
    return {row["kind"]: int(row["stamp"]) for row in rows}


def read_sync(db: "Database") -> dict[str, int]:
    """:func:`read` for a caller that cannot await (the settings snapshot)."""
    with db.engine.connect() as conn:
        enter = db.capabilities.enter_org_sql(db.search_path) if db.search_path else None
        if enter:
            conn.execute(text(enter))
        try:
            rows = conn.execute(text("SELECT kind, stamp FROM config_stamp")).fetchall()
        finally:
            if enter:
                # The pooled connection must not carry this org's search_path to its next user
                # (see Database.acquire).
                conn.execute(text("RESET search_path"))
            conn.commit()
    return {row[0]: int(row[1]) for row in rows}
