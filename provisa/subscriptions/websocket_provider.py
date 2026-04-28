# Copyright (c) 2026 Kenneth Stott
# Canary: 0fb7892a-7846-47c3-bc87-31d4a4834dbc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WebSocket subscription provider — connects to an external WS feed and yields ChangeEvents."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)


def _extract_path(data: object, path: str) -> object:
    """Walk a dot-notation path into nested dicts/lists. Returns None on miss."""
    for segment in path.split("."):
        if isinstance(data, dict):
            data = data.get(segment)
        elif isinstance(data, list):
            try:
                data = data[int(segment)]
            except (IndexError, ValueError):
                return None
        else:
            return None
    return data


class WebSocketNotificationProvider(NotificationProvider):
    """Connects to an external WebSocket URL and yields ChangeEvents.

    Args:
        url: Full WebSocket URL (ws:// or wss://).
        subscribe_payload: Optional dict sent as JSON after connecting
                           (e.g. channel subscription handshake).
        event_path: Optional dot-notation path to extract the event dict
                    from the received JSON message (e.g. ``"data"``).
        reconnect_interval: Seconds to wait before reconnecting on disconnect.
    """

    def __init__(
        self,
        url: str,
        subscribe_payload: dict | None = None,
        event_path: str | None = None,
        reconnect_interval: float = 5.0,
    ) -> None:
        self._url = url
        self._subscribe_payload = subscribe_payload
        self._event_path = event_path
        self._reconnect_interval = reconnect_interval
        self._running = True
        self._ws: object | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        import websockets

        log.info("WebSocketProvider: connecting to %s", self._url)
        while self._running:
            try:
                async with websockets.connect(self._url) as ws:
                    self._ws = ws
                    if self._subscribe_payload:
                        await ws.send(json.dumps(self._subscribe_payload))
                        log.info("WebSocketProvider: sent subscribe payload")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            data = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            log.warning("WebSocketProvider: non-JSON message skipped")
                            continue

                        if self._event_path:
                            data = _extract_path(data, self._event_path) or {}

                        if not isinstance(data, dict):
                            data = {"value": data}

                        op = data.pop("op", "insert").lower() if "op" in data else "insert"

                        ts_raw = data.get("_ts") or data.get("timestamp")
                        if isinstance(ts_raw, str):
                            try:
                                ts = datetime.fromisoformat(ts_raw)
                            except ValueError:
                                ts = datetime.now(timezone.utc)
                        elif isinstance(ts_raw, (int, float)):
                            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
                        else:
                            ts = datetime.now(timezone.utc)

                        yield ChangeEvent(operation=op, table=table, row=data, timestamp=ts)

            except Exception as exc:
                if not self._running:
                    break
                log.warning(
                    "WebSocketProvider: disconnected (%s), reconnecting in %.1fs",
                    exc,
                    self._reconnect_interval,
                )
                await asyncio.sleep(self._reconnect_interval)

        self._ws = None

    async def close(self) -> None:
        self._running = False
        if self._ws and hasattr(self._ws, "close"):
            await self._ws.close()
