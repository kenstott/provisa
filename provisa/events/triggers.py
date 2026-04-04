# Copyright (c) 2025 Kenneth Stott
# Canary: 867eaee4-03ad-4112-8611-69d3ed503f1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""EventTriggerManager: PG LISTEN/NOTIFY → webhook dispatch with retry."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import asyncpg
import httpx

from provisa.core.models import EventTrigger

logger = logging.getLogger(__name__)

# SQL templates for PG trigger + notify function
_CREATE_NOTIFY_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION provisa_notify_{safe_name}()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    '{channel}',
    json_build_object(
      'operation', TG_OP,
      'table', TG_TABLE_NAME,
      'schema', TG_TABLE_SCHEMA,
      'row', CASE
        WHEN TG_OP = 'DELETE' THEN row_to_json(OLD)
        ELSE row_to_json(NEW)
      END
    )::text
  );
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
"""

_CREATE_TRIGGER_SQL = """
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'provisa_trigger_{safe_name}'
  ) THEN
    CREATE TRIGGER provisa_trigger_{safe_name}
      AFTER {operations} ON {schema_table}
      FOR EACH ROW EXECUTE FUNCTION provisa_notify_{safe_name}();
  END IF;
END $$;
"""

_DROP_TRIGGER_SQL = """
DROP TRIGGER IF EXISTS provisa_trigger_{safe_name} ON {schema_table};
DROP FUNCTION IF EXISTS provisa_notify_{safe_name}();
"""


def _safe_name(table_id: str) -> str:
    """Sanitize table_id for use in PG identifiers."""
    return table_id.replace("-", "_").replace(".", "_").lower()


def _channel_name(table_id: str) -> str:
    return f"provisa_evt_{_safe_name(table_id)}"


def _operations_clause(operations: list[str]) -> str:
    """Build 'INSERT OR UPDATE OR DELETE' clause from operation list."""
    return " OR ".join(op.upper() for op in operations)


class EventTriggerManager:
    """Manages PG event triggers and webhook dispatch.

    Lifecycle:
        1. setup(pool) — creates PG trigger functions and starts LISTEN
        2. Incoming NOTIFY payloads are dispatched to webhook URLs
        3. teardown() — stops listeners and drops triggers
    """

    def __init__(self, triggers: list[EventTrigger]) -> None:
        self._triggers = {t.table_id: t for t in triggers}
        self._listen_conn: asyncpg.Connection | None = None
        self._listen_task: asyncio.Task | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._running = False

    async def setup(self, pool: asyncpg.Pool) -> None:
        """Install PG triggers and start listening."""
        if not self._triggers:
            return

        self._http_client = httpx.AsyncClient(timeout=30.0)

        # Install trigger functions and triggers
        async with pool.acquire() as conn:
            for trigger in self._triggers.values():
                if not trigger.enabled:
                    continue
                await self._install_trigger(conn, trigger)

        # Acquire a dedicated connection for LISTEN
        self._listen_conn = await pool.acquire()
        self._running = True

        for trigger in self._triggers.values():
            if not trigger.enabled:
                continue
            channel = _channel_name(trigger.table_id)
            await self._listen_conn.add_listener(channel, self._on_notify)
            logger.info("Listening on channel %s for table %s", channel, trigger.table_id)

        logger.info("EventTriggerManager started with %d triggers", len(self._triggers))

    async def teardown(self, pool: asyncpg.Pool) -> None:
        """Remove listeners and drop PG triggers."""
        self._running = False

        if self._listen_conn is not None:
            for trigger in self._triggers.values():
                channel = _channel_name(trigger.table_id)
                try:
                    await self._listen_conn.remove_listener(channel, self._on_notify)
                except Exception:
                    pass
            await pool.release(self._listen_conn)
            self._listen_conn = None

        # Drop triggers
        async with pool.acquire() as conn:
            for trigger in self._triggers.values():
                await self._drop_trigger(conn, trigger)

        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

        logger.info("EventTriggerManager stopped")

    async def _install_trigger(
        self, conn: asyncpg.Connection, trigger: EventTrigger,
    ) -> None:
        safe = _safe_name(trigger.table_id)
        channel = _channel_name(trigger.table_id)
        ops_clause = _operations_clause(trigger.operations)

        # Schema-qualified table name
        schema_table = trigger.table_id
        if "." not in schema_table:
            schema_table = f"public.{schema_table}"

        func_sql = _CREATE_NOTIFY_FUNCTION_SQL.format(
            safe_name=safe, channel=channel,
        )
        trig_sql = _CREATE_TRIGGER_SQL.format(
            safe_name=safe,
            operations=ops_clause,
            schema_table=schema_table,
        )
        await conn.execute(func_sql)
        await conn.execute(trig_sql)
        logger.info("Installed PG trigger for %s (%s)", trigger.table_id, ops_clause)

    async def _drop_trigger(
        self, conn: asyncpg.Connection, trigger: EventTrigger,
    ) -> None:
        safe = _safe_name(trigger.table_id)
        schema_table = trigger.table_id
        if "." not in schema_table:
            schema_table = f"public.{schema_table}"
        drop_sql = _DROP_TRIGGER_SQL.format(
            safe_name=safe, schema_table=schema_table,
        )
        await conn.execute(drop_sql)

    def _on_notify(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """asyncpg listener callback — schedule webhook dispatch."""
        if not self._running:
            return
        asyncio.ensure_future(self._dispatch(channel, payload))

    async def _dispatch(self, channel: str, payload: str) -> None:
        """Parse notification and POST to webhook URL with retry."""
        # Find matching trigger by channel
        trigger = None
        for t in self._triggers.values():
            if _channel_name(t.table_id) == channel:
                trigger = t
                break

        if trigger is None:
            logger.warning("No trigger found for channel %s", channel)
            return

        try:
            data: dict[str, Any] = json.loads(payload)
        except json.JSONDecodeError:
            logger.error("Invalid JSON payload on channel %s: %s", channel, payload)
            return

        # Filter by operation
        operation = data.get("operation", "").lower()
        if operation not in [op.lower() for op in trigger.operations]:
            return

        await self._post_webhook(trigger, data)

    async def _post_webhook(
        self, trigger: EventTrigger, data: dict[str, Any],
    ) -> None:
        """POST data to webhook URL with exponential backoff retry."""
        if self._http_client is None:
            return

        max_retries = trigger.retry_max
        base_delay = trigger.retry_delay

        for attempt in range(max_retries + 1):
            try:
                resp = await self._http_client.post(
                    trigger.webhook_url,
                    json=data,
                    headers={"Content-Type": "application/json"},
                )
                if resp.status_code < 400:
                    logger.debug(
                        "Webhook delivered for %s (attempt %d): %d",
                        trigger.table_id, attempt + 1, resp.status_code,
                    )
                    return
                logger.warning(
                    "Webhook %s returned %d (attempt %d/%d)",
                    trigger.webhook_url, resp.status_code,
                    attempt + 1, max_retries + 1,
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    "Webhook %s failed (attempt %d/%d): %s",
                    trigger.webhook_url, attempt + 1, max_retries + 1, exc,
                )

            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        logger.error(
            "Webhook delivery failed after %d attempts for %s → %s",
            max_retries + 1, trigger.table_id, trigger.webhook_url,
        )
