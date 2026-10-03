# Copyright (c) 2026 Kenneth Stott
# Canary: 4261bc19-6c88-4157-8d62-e65c7c20394c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Whether a MongoDB table may follow its source's change feed (REQ-1861).

A table opts in with the ``native`` change signal (its own, else its source's). Change streams
are served only by a replica set or a sharded cluster, so a table that opts in on a standalone
server is refused by name, at save and at load, instead of being built once and never refreshed.
"""

# Requirements: REQ-1861

from __future__ import annotations

import asyncio
from typing import Any

from provisa.mongodb.fetch import ChangeStreamsUnavailable, MongoConnection, require_change_streams

__all__ = ["ChangeStreamsUnavailable", "follows_change_feed", "require_change_feed"]

#: The change signal that opts a table into its source's change feed (REQ-929).
CHANGE_FEED_SIGNAL = "native"


def follows_change_feed(
    source_type: str, table_signal: str | None, source_signal: str | None
) -> bool:
    """Whether a table of a ``source_type`` source follows the source's change feed: a MongoDB
    table whose effective change signal (its own, else its source's) is ``native``."""
    signal = table_signal if table_signal is not None else source_signal
    return source_type == "mongodb" and signal == CHANGE_FEED_SIGNAL


async def require_change_feed(source: Any) -> None:
    """Refuse a source whose server serves no change streams (:class:`ChangeStreamsUnavailable`).
    The source's connection values are resolved here, at use; a server that cannot be reached
    raises the driver's own error."""
    from provisa.core.secrets import resolve_secrets

    conn = MongoConnection.build(
        resolve_secrets(source.host or "localhost"),
        int(source.port or 0),
        username=source.username or None,
        password=resolve_secrets(source.password or "") or None,
    )
    await asyncio.to_thread(require_change_streams, conn, source.id)
