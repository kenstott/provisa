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
from typing import Any

from provisa.subscriptions.pg_provider import CHANNEL_PREFIX

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


async def _base_tables(conn: Any, pairs: list[tuple[str, str]]) -> set[tuple[str, str]]:
    """The subset of ``(schema, table)`` pairs that are ordinary or partitioned base tables
    (``pg_class.relkind`` in ``r``/``p``) — the only relations a row-level AFTER trigger can be
    installed on. A view, materialized view, or foreign table is excluded, as is a name with no
    relation yet. One catalog query, so the decision is made up front, not by a failed CREATE.
    The control-plane connection binds a list as JSONB (provisa.core.database._translate), so the
    names arrive as two JSON arrays, paired by position."""
    if not pairs:
        return set()
    schemas = [s for s, _ in pairs]
    names = [t for _, t in pairs]
    rows = await conn.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS name
        FROM jsonb_array_elements_text($1) WITH ORDINALITY AS s(schema, i)
        JOIN jsonb_array_elements_text($2) WITH ORDINALITY AS t(name, j) ON t.j = s.i
        JOIN pg_namespace n ON n.nspname = s.schema
        JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = t.name
        WHERE c.relkind IN ('r', 'p')
        """,
        schemas,
        names,
    )
    return {(r["schema"], r["name"]) for r in rows}


async def ensure_pg_notify_triggers(  # REQ-258
    conn: Any,
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
    # ``conn`` is a control-plane connection (provisa.core.database); its capabilities say whether
    # the plane carries LISTEN/NOTIFY. (It once read a ``dialect`` attribute the connection does
    # not have, defaulting to none, so no plane ever installed a trigger.)
    if not conn.capabilities.listen_notify:
        return set()
    pg_tables = [
        (tbl.get("schema_name", "public"), tbl["table_name"])
        for tbl in tables
        if source_types.get(tbl["source_id"], "") == "postgresql"
    ]
    base = await _base_tables(conn, pg_tables)
    installed: set[str] = set()
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
    return installed
