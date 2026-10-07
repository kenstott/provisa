# Copyright (c) 2026 Kenneth Stott
# Canary: f532a010-50ae-4239-9141-43ecd54538b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Install LISTEN/NOTIFY triggers on registered PostgreSQL subscription tables."""

# Requirements: REQ-258

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from provisa.subscriptions.pg_provider import CHANNEL_PREFIX

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)

# Postgres refuses a NOTIFY payload of 8000 bytes or more; stay clear of the boundary.
MAX_NOTIFY_BYTES = 7900


def _trigger_sql(schema: str, table: str) -> str:
    """Return idempotent SQL to install a notify trigger on schema.table."""
    fn = f"provisa_notify_{schema}_{table}"
    trig = f"provisa_sub_{schema}_{table}"
    channel = f"{CHANNEL_PREFIX}{table}"
    return f"""
CREATE OR REPLACE FUNCTION {fn}()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  rowjson text;
  payload text;
  budget int;
BEGIN
  rowjson := (CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE row_to_json(NEW) END)::text;
  payload := json_build_object('op', lower(TG_OP), 'row', rowjson::json)::text;
  -- REQ-1515: NOTIFY caps a payload at 8000 bytes and raises otherwise, which would abort the
  -- writer's transaction. A row too large to fit is truncated rather than dropped: the
  -- subscriber still sees the change and the leading columns, and a write never fails for
  -- want of a notification. The loop halves the character budget until the encoded envelope
  -- fits, since a character is up to four bytes.
  IF octet_length(payload) > {MAX_NOTIFY_BYTES} THEN
    budget := {MAX_NOTIFY_BYTES};
    LOOP
      payload := json_build_object(
        'op', lower(TG_OP), 'truncated', true, 'row_text', left(rowjson, budget)
      )::text;
      EXIT WHEN octet_length(payload) <= {MAX_NOTIFY_BYTES} OR budget < 1;
      budget := budget / 2;
    END LOOP;
  END IF;
  PERFORM pg_notify('{channel}', payload);
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS {trig} ON {schema}.{table};
CREATE TRIGGER {trig}
AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table}
FOR EACH ROW EXECUTE FUNCTION {fn}();
"""


# The advisory-lock key serializing the notify-trigger walk across the servers of a launch.
_TRIGGER_INSTALL_LOCK_KEY = 0x50524F5649534135


async def _base_tables(conn: "Connection", pairs: list[tuple[str, str]]) -> set[tuple[str, str]]:
    """The subset of ``(schema, table)`` pairs that are ordinary or partitioned base tables
    (``pg_class.relkind`` in ``r``/``p``) — the only relations a row-level AFTER trigger can be
    installed on. A view, materialized view, or foreign table is excluded, as is a name with no
    relation yet. One catalog query, so the decision is made up front, not by a failed CREATE.

    The pairs go over as ONE jsonb array of records: the control-plane Connection binds a list
    argument as jsonb (``provisa.core.database._translate``) and strips a ``::text[]`` cast on a
    bind, so ``unnest($1::text[], ...)`` reached the server as ``unnest(jsonb)`` and failed."""
    if not pairs:
        return set()
    rows = await conn.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS name
        FROM jsonb_to_recordset($1) AS want(schema text, name text)
        JOIN pg_namespace n ON n.nspname = want.schema
        JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = want.name
        WHERE c.relkind IN ('r', 'p')
        """,
        [{"schema": schema, "name": name} for schema, name in pairs],
    )
    return {(r["schema"], r["name"]) for r in rows}


async def ensure_pg_notify_triggers(  # REQ-258
    conn: "Connection",
    tables: list[dict],
    source_types: dict[str, str],
) -> set[str]:
    """Idempotently install notify triggers on registered PostgreSQL BASE tables.

    Returns the set of table_names where triggers were installed. A relation that is a view or
    materialized view cannot carry a row trigger, so its subscription is served by watermark
    polling (REQ-258) -- that is decided up front from the catalog, never discovered by a failed
    CREATE TRIGGER. A base table whose install fails for another reason (e.g. insufficient
    privilege) is omitted too, and its caller likewise falls back to polling.
    """
    # LISTEN/NOTIFY and the pg_class base-table probe are PostgreSQL-only: on any other control
    # plane (e.g. a SQLite demo/dev plane) there are no notify triggers at all, so every table's
    # subscription is served by polling. Returning early also keeps the PG-only catalog query in
    # _base_tables from running against a non-PG plane (it would otherwise fail the whole walk).
    # The control-plane Connection names its dialect on its capabilities; it has no ``dialect``
    # attribute, so reading one there silently skipped every install on a PostgreSQL plane.
    if conn.capabilities.dialect != "postgresql":
        return set()
    pg_tables = [
        (tbl.get("schema_name", "public"), tbl["table_name"])
        for tbl in tables
        if source_types.get(tbl["source_id"], "") == "postgresql"
    ]
    installed: set[str] = set()
    # Every server of a launch walks the same tables at boot; CREATE OR REPLACE FUNCTION and
    # DROP/CREATE TRIGGER on one relation from two sessions at once fails "tuple concurrently
    # updated". One walk at a time, on the plane's advisory lock.
    async with conn.advisory_lock(_TRIGGER_INSTALL_LOCK_KEY):
        base = await _base_tables(conn, pg_tables)
        await _install(conn, pg_tables, base, installed)
    return installed


async def _install(
    conn: "Connection",
    pg_tables: list[tuple[str, str]],
    base: set[tuple[str, str]],
    installed: set[str],
) -> None:
    for schema, table in pg_tables:
        if (schema, table) not in base:
            # A view/matview (or a not-yet-created relation): polling serves it, by design.
            log.debug("Subscription on %s.%s uses polling (not a base table)", schema, table)
            continue
        try:
            await conn.execute(_trigger_sql(schema, table))
            installed.add(table)
            log.debug("Installed notify trigger on %s.%s", schema, table)
        except Exception as exc:
            log.warning(
                "Failed to install notify trigger on %s.%s: %s — "
                "subscription will fall back to polling",
                schema,
                table,
                exc,
            )
