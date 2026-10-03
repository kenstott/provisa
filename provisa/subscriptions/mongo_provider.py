# Copyright (c) 2026 Kenneth Stott
# Canary: 58995606-2344-4dc9-a247-4663c860ec42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MongoDB Change Streams subscription provider."""

# Requirements: REQ-258

from __future__ import annotations

import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, AsyncIterator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorChangeStream, AsyncIOMotorDatabase

log = logging.getLogger(__name__)

# Map MongoDB change stream operation types to our canonical names
_OP_MAP = {
    "insert": "insert",
    "update": "update",
    "replace": "update",
    "delete": "delete",
}


class MongoNotificationProvider(NotificationProvider):  # REQ-258
    """Uses motor ``collection.watch()`` for MongoDB Change Streams."""

    def __init__(self, database: AsyncIOMotorDatabase) -> None:
        self._db = database
        self._cursor: AsyncIOMotorChangeStream | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        collection = self._db[table]
        pipeline: list[dict] = []
        if filter_expr:
            pipeline.append({"$match": {"operationType": filter_expr}})

        self._cursor = collection.watch(pipeline)
        assert self._cursor is not None
        log.info("MongoProvider: watching collection %s", table)

        try:
            async for change in self._cursor:
                op_type = str(change.get("operationType", "unknown"))
                op = _OP_MAP.get(op_type, op_type)

                if op == "delete":
                    row = {"_id": str(change.get("documentKey", {}).get("_id", ""))}
                else:
                    full_doc = change.get("fullDocument", {})
                    row = {
                        k: str(v) if not isinstance(v, (str, int, float, bool)) else v
                        for k, v in full_doc.items()
                    }

                yield ChangeEvent(
                    operation=op,
                    table=table,
                    row=row,
                    timestamp=datetime.now(timezone.utc),
                )
        finally:
            if self._cursor:
                await self._cursor.close()
                self._cursor = None

    async def close(self) -> None:
        if self._cursor:
            await self._cursor.close()
            self._cursor = None


@asynccontextmanager
async def open_change_stream(
    *,
    host: str,
    port: int,
    username: str | None,
    password: str | None,
    database: str,
    collection: str,
    wait_ms: int,
) -> AsyncIterator[AsyncIOMotorChangeStream]:
    """An open change stream on ``database.collection`` (REQ-1861), closed with its client when
    the block ends. Entering opens the stream at the server, so a change made after the block is
    entered is not missed. ``wait_ms`` bounds each ``try_next``. The server must be a replica
    set member: a standalone mongod serves no change streams and fails here with its own reason.

    ``directConnection``: a single-node replica set reports a hostname only its own network
    resolves (see ``provisa/mongodb/fetch.py``), so the address given is the one connected to."""
    from motor.motor_asyncio import AsyncIOMotorClient

    client: AsyncIOMotorClient = AsyncIOMotorClient(
        host=host,
        port=port,
        username=username,
        password=password,
        serverSelectionTimeoutMS=30000,
        directConnection=True,
    )
    try:
        async with client[database][collection].watch(max_await_time_ms=wait_ms) as stream:
            yield stream
    finally:
        client.close()
