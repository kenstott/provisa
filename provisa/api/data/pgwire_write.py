# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6b2f48-7a13-4c95-b8e7-3f1a9c5d2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The write route of a source written through its own bundled pgwire server (REQ-1946).

A SharePoint or Salesforce source is written by the DIRECT terminal, as any other writable source
is: the statement runs on the source's driver in ``state.source_pools``. For these two types that
driver is the PostgreSQL driver connected to the source's own pgwire server
(``federation/pgwire_replica.py``), whatever engine serves the source's reads — where the engine
reads through a connector of its own (Trino), the server runs for writes alone and no read is sent
to it.

The server is started when the source is registered or loaded (:func:`start_write_server`), since
a Salesforce server describes every sObject before it listens. The pool is opened by the first
write that finds the server listening (:func:`ensure_write_pool`); a write that arrives earlier is
answered with the connector-starting error (REQ-1824), not held.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.writable import PGWIRE_SERVER_WRITTEN

# What the bundled server accepts: it checks no credentials and serves one database.
_SERVER_DATABASE = "provisa"
_SERVER_USER = "provisa"


def start_write_server(source: Any) -> None:
    """Start ``source``'s pgwire server in the background if its type is written through one.
    A failure to start is logged under the task's name; the write that needs the server reports
    it again."""
    stype = source.type.value if hasattr(source.type, "value") else str(source.type)
    if stype not in PGWIRE_SERVER_WRITTEN:
        return
    from provisa.core.connection_loop import spawn_background
    from provisa.federation.pgwire_replica import start_endpoint

    spawn_background(
        asyncio.to_thread(start_endpoint, source), name=f"pgwire-write-server:{source.id}"
    )


async def ensure_write_pool(state: Any, source_id: str) -> None:
    """Open the DIRECT pool a write to ``source_id`` runs on, if its type is written through its
    pgwire server and the pool is not open yet. Raises ``SourceStillStartingError`` while the
    server has not begun listening."""
    if state.source_types.get(source_id) not in PGWIRE_SERVER_WRITTEN:
        return
    if state.source_pools.has(source_id):
        return
    from provisa.api.admin.schema_query import _source_for_introspection
    from provisa.federation.pgwire_replica import ensure_endpoint_for_discovery

    source = await _source_for_introspection(source_id)
    if source is None:
        raise LookupError(f"source {source_id!r} is not registered")
    from provisa.executor.drivers.pgwire_server import PgwireServerDriver

    ports = await asyncio.to_thread(ensure_endpoint_for_discovery, source)
    driver = PgwireServerDriver()
    await driver.connect(
        ports.calcite_child_host, ports.pgwire_port, _SERVER_DATABASE, _SERVER_USER, ""
    )
    await state.source_pools.add_driver(source_id, driver, "postgres")
