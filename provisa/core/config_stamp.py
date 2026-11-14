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
  What is keyed by the model a process loaded follows it: the response cache, the hot tables, the
  audit record of the model commit.
* ``build`` — the tenant plane: the part of the model the schema build reads. Every write that
  advances ``model`` advances it too, except an update that changes only columns of
  :data:`NOT_READ_BY_THE_BUILD`. A process whose ``build`` stamp is current takes the new ``model``
  stamp without building anything (``provisa/api/model_reload.py``).
* ``settings`` — the tenant plane (``org_settings``) and the platform plane
  (``deployment_settings``), each with its own row.
* ``replica`` — the tenant plane: which tables are served from a replica because they are busy
  (REQ-826). It has no trigger. ``replica_state`` is state, written on every build and refresh;
  only two of its changes move a table between live and its replica, and the code that makes
  each advances the row itself (:func:`advance`, called by ``federation/replica_state.py``).
"""

# Requirements: REQ-1914, REQ-826, REQ-1920

from __future__ import annotations

import zlib
from typing import TYPE_CHECKING, Any

from sqlalchemy import text, update

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

MODEL = "model"
BUILD = "build"
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
    "data_product_tables": MODEL,  # REQ-1634
    "data_product_commands": MODEL,  # REQ-1634
    "naming_rules": MODEL,
    "registered_tables": MODEL,
    "table_columns": MODEL,
    "relationships": MODEL,
    "metrics": MODEL,
    "roles": MODEL,
    # REQ-1382: read into the runtime build (state.user_attributes), so every worker reloads.
    "user_attributes": MODEL,
    # REQ-2219: read into the runtime build (state.held_entitlements), so every worker reloads.
    "entitlements": MODEL,
    "entitlement_expiries": MODEL,
    "rls_rules": MODEL,
    "table_write_grants": MODEL,  # REQ-2074
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
    "connectors": MODEL,  # REQ-2221
    "table_meta_links": MODEL,
    "calendars": MODEL,
    "materialized_views": MODEL,
    "org_settings": SETTINGS,
    # REQ-1939: a dataset's tables decide what this environment's tables read.
    "synthetic_datasets": MODEL,
    "synthetic_dataset_tables": MODEL,
}

PLATFORM_TABLES: dict[str, str] = {"deployment_settings": SETTINGS}

# The columns of a ``model`` table that nothing a schema build makes depends on: an update of these
# alone advances ``model`` and not ``build``. A column is named here only once every reader of it
# reads the store when asked, so one not named always rebuilds. A data product's lifecycle: the
# build reads a product's id and domain (the entitlement refs) and its owner role (REQ-2188); its
# status is read from the store by the admin surfaces, the catalog exports and the meta view.
NOT_READ_BY_THE_BUILD: dict[str, tuple[str, ...]] = {
    "data_products": (
        "status",
        "status_reason",
        "retire_on",
        "status_by",
        "status_at",
        "successor_id",
    ),
}

# Serializes trigger creation across the worker processes of a launch (PostgreSQL). "PROVISA4".
_INSTALL_LOCK_KEY = 0x50524F5649534134

_PG_FUNCTION = "advance_config_stamp"


def _install_lock_key(schema: str) -> int:
    """REQ-2006: the lock of one schema's install. What it serializes is two installs of the same
    schema's function and triggers (the worker processes of a launch); one key for every schema
    queued each environment a sandbox's open lays out side by side behind the other's."""
    return ((_INSTALL_LOCK_KEY & 0x7FFFFFFF) << 32) | zlib.crc32(schema.encode())


def _seed(conn: Any, qualified: str, kinds: set[str]) -> None:
    """A row of stamp 0 for each of ``kinds`` that has none: one statement, whatever their number."""
    conn.execute(
        text(
            f"INSERT INTO {qualified} (kind, stamp) "
            "SELECT k, 0 FROM unnest(CAST(:kinds AS text[])) AS k "
            f"WHERE NOT EXISTS (SELECT 1 FROM {qualified} s WHERE s.kind = k)"
        ),
        {"kinds": sorted(kinds)},
    )


def trigger_function_body() -> str:
    """The trigger function's PL/pgSQL. Its rule lives here and not in the triggers' definitions:
    an existing schema keeps its triggers, and laying it out again replaces this function, so the
    rule reaches every org schema (``db.layout_digest`` carries this text).

    The table's own schema names the stamp it advances, so the trigger is right whatever
    search_path the writing connection carries."""
    cases = " ".join(
        f"WHEN '{table}' THEN ARRAY[{', '.join(repr(column) for column in columns)}]"
        for table, columns in sorted(NOT_READ_BY_THE_BUILD.items())
    )
    return (
        "DECLARE kinds text[] := ARRAY[TG_ARGV[0]]; ignored text[]; "
        "BEGIN "
        f"IF TG_ARGV[0] = '{MODEL}' THEN "
        f"ignored := CASE TG_TABLE_NAME {cases} END; "
        # OLD and NEW are read only for an UPDATE: a statement-level TRUNCATE has neither.
        "IF TG_OP <> 'UPDATE' OR ignored IS NULL THEN "
        f"kinds := array_append(kinds, '{BUILD}'); "
        "ELSIF (to_jsonb(OLD) - ignored) IS DISTINCT FROM (to_jsonb(NEW) - ignored) THEN "
        f"kinds := array_append(kinds, '{BUILD}'); "
        "END IF; "
        "END IF; "
        "EXECUTE format('UPDATE %I.config_stamp SET stamp = stamp + 1 WHERE kind = ANY($1)', "
        "TG_TABLE_SCHEMA) USING kinds; "
        "RETURN NULL; "
        "END"
    )


def _install_postgresql(conn: Any, tables: dict[str, str], schema: str | None) -> None:
    if schema is None:
        schema = conn.execute(text("SELECT current_schema()")).scalar_one()
    assert schema is not None
    conn.execute(text(f"SELECT pg_advisory_xact_lock({_install_lock_key(schema)})"))
    stamp = f'"{schema}".config_stamp'
    kinds = set(tables.values())
    _seed(conn, stamp, kinds | {BUILD} if MODEL in kinds else kinds)
    function = f'"{schema}".{_PG_FUNCTION}'
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $fn$ "
            f"{trigger_function_body()} $fn$"
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
    # Read from pg_catalog: information_schema.columns is a view over every schema's columns that
    # PostgreSQL filters by privilege row by row, and a control plane holding many org schemas
    # made it the slowest statement of a layout.
    uncomparable = {
        row[0]
        for row in conn.execute(
            text(
                "SELECT DISTINCT c.relname FROM pg_catalog.pg_attribute a "
                "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p', 'v', 'm', 'f') "
                "AND a.attnum > 0 AND NOT a.attisdropped "
                "AND a.atttypid = 'pg_catalog.json'::pg_catalog.regtype"
            ),
            {"schema": schema},
        )
    }
    missing: list[str] = []
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
        missing.extend(
            f"CREATE TRIGGER {name} {definition};"
            for name, definition in wanted.items()
            if (table, name) not in existing
        )
    if missing:
        # One statement for them all: a new schema has ~100 of them, and each was a round trip.
        conn.execute(text(f"DO $install$ BEGIN {' '.join(missing)} END $install$"))


def install(
    conn: Any, tables: dict[str, str], schema: str | None = None, *, advanced: tuple[str, ...] = ()
) -> None:
    """Seed the stamp rows and create the triggers that advance them, for ``tables`` (table name →
    kind) in ``schema``. Idempotent. ``conn`` is a SQLAlchemy connection inside a transaction, on
    which ``config_stamp`` and every one of ``tables`` already exist. ``advanced``: the kinds of
    this plane that have a row and no trigger (``TENANT_ADVANCED``).

    The control plane is PostgreSQL (REQ-2014)."""
    _install_postgresql(conn, tables, schema)
    if schema is None:
        schema = conn.execute(text("SELECT current_schema()")).scalar_one()
    _seed(conn, f'"{schema}".config_stamp', set(advanced))


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
            f"config stamp {kind!r} has no row on this control plane: the plane was "
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
