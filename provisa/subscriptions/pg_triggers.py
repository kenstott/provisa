# Copyright (c) 2026 Kenneth Stott
# Canary: 3724ecd5-ebb4-45a7-9f79-9bb0fedfc777
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Install LISTEN/NOTIFY triggers on pre-approved PostgreSQL subscription tables."""

from __future__ import annotations

import logging
from typing import Any

from provisa.subscriptions.pg_provider import CHANNEL_PREFIX

log = logging.getLogger(__name__)


def _trigger_sql(schema: str, table: str) -> str:
    """Return idempotent SQL to install a notify trigger on schema.table."""
    fn = f"provisa_notify_{schema}_{table}"
    trig = f"provisa_sub_{schema}_{table}"
    channel = f"{CHANNEL_PREFIX}{table}"
    return f"""
CREATE OR REPLACE FUNCTION {fn}()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify(
    '{channel}',
    json_build_object(
      'op', lower(TG_OP),
      'row', CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE row_to_json(NEW) END
    )::text
  );
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS {trig} ON {schema}.{table};
CREATE TRIGGER {trig}
AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table}
FOR EACH ROW EXECUTE FUNCTION {fn}();
"""


async def ensure_pg_notify_triggers(
    conn: Any,
    tables: list[dict],
    source_types: dict[str, str],
) -> None:
    """Idempotently install notify triggers on all pre-approved PostgreSQL tables."""
    for tbl in tables:
        if tbl.get("governance") != "pre-approved":
            continue
        source_type = source_types.get(tbl["source_id"], "")
        if source_type != "postgresql":
            continue
        schema = tbl.get("schema_name", "public")
        table = tbl["table_name"]
        try:
            await conn.execute(_trigger_sql(schema, table))
            log.debug("Installed notify trigger on %s.%s", schema, table)
        except Exception as exc:
            log.warning("Failed to install notify trigger on %s.%s: %s", schema, table, exc)
