# Copyright (c) 2025 Kenneth Stott
# Canary: 58995606-2344-4dc9-a247-4663c860ec42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MongoDB Change Streams subscription provider."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

# Map MongoDB change stream operation types to our canonical names
_OP_MAP = {
    "insert": "insert",
    "update": "update",
    "replace": "update",
    "delete": "delete",
}


class MongoNotificationProvider(NotificationProvider):
    """Uses motor ``collection.watch()`` for MongoDB Change Streams."""

    def __init__(self, database: Any) -> None:
        self._db = database
        self._cursor: Any | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        collection = self._db[table]
        pipeline: list[dict] = []
        if filter_expr:
            pipeline.append({"$match": {"operationType": filter_expr}})

        self._cursor = collection.watch(pipeline)
        log.info("MongoProvider: watching collection %s", table)

        try:
            async for change in self._cursor:
                op_type = change.get("operationType", "unknown")
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
