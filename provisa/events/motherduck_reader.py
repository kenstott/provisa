# Copyright (c) 2026 Kenneth Stott
# Canary: 9b47e1c2-3f6d-4a85-b0d9-71e2c5a8f364
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The reader that lands a MotherDuck source's replicas through MotherDuck's own Postgres-wire
endpoint, where the engine cannot attach MotherDuck (REQ-1998, MOTHERDUCK LANDS THROUGH ITS
POSTGRES ENDPOINT): no second DuckDB version is run.

The endpoint is ``<host>:5432`` -- ``pg.<region>-aws.motherduck.com``, the host MotherDuck's
Postgres settings show for the account's region, entered as the source's host -- signed in as
``postgres`` with the source's own MotherDuck token (its password field) resolved with the
standard resolution, over TLS verified against the system's roots, as MotherDuck asks. The
connection opens the source's database alone (``attach_mode=single``), and a table's registered
columns are read from ``<schema>.<table>`` there, in DuckDB SQL, which the endpoint speaks.

Rows stream off the connection a bounded batch at a time (libpq's chunked rows; the endpoint
offers no server-side cursor), and become Arrow record batches of the registered column types,
which the landing path writes as the replica.
"""

# Requirements: REQ-1998
from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.postgresql import quoted_select, stream_rows

#: The port of MotherDuck's Postgres endpoint, in every region.
PG_PORT = 5432
#: The user every MotherDuck token signs in to the endpoint as.
PG_USER = "postgres"
#: The rows one read of the stream holds, at most, for a whole load.
LOAD_BATCH_ROWS = 10_000


def connection_settings(source: Any) -> dict[str, Any]:
    """The psycopg connection settings of ``source``'s endpoint, refusing by name a source with
    no endpoint host, token or database."""
    from provisa.core.secrets import resolve_secrets

    if not source.host:
        raise ValueError(
            f"Source {source.id!r}: a MotherDuck replica lands through MotherDuck's Postgres "
            "endpoint; enter its host (pg.<region>-aws.motherduck.com, as MotherDuck's Postgres "
            "settings show it) as the source's host"
        )
    if not source.password:
        raise ValueError(
            f"Source {source.id!r}: a MotherDuck source signs in with its token (the password "
            "field); none is set"
        )
    if not source.database:
        raise ValueError(f"Source {source.id!r}: name the MotherDuck database to read")
    return {
        "host": resolve_secrets(source.host),
        "port": source.port or PG_PORT,
        "dbname": resolve_secrets(source.database),
        "user": PG_USER,
        "password": resolve_secrets(source.password),
        "sslmode": "verify-full",
        "sslrootcert": "system",
        "options": "--attach_mode=single",
        "autocommit": True,
    }


def select_sql(table: Any, names: list[str]) -> Any:
    """The read of ``names`` from ``table`` in the source's database, every name quoted."""
    return quoted_select(table.schema_name, table.table_name, names)


def _names(table: Any) -> list[str]:
    return [c.name for c in table.columns if getattr(c, "native_filter_type", None) is None]


def make_motherduck_loader() -> Any:
    """The MotherDuck row-fetch (REQ-1998): whole for :meth:`SourceRowLoader.load`, and a
    bounded batch at a time for a replica build (``replica_source``)."""

    async def _load(source: Any, table: Any) -> list[dict]:
        names = _names(table)
        if not names:
            return []
        settings = connection_settings(source)
        sql = select_sql(table, names)

        def _all() -> list[dict]:
            return [row for rows in stream_rows(settings, sql, LOAD_BATCH_ROWS) for row in rows]

        return await asyncio.to_thread(_all)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource

        settings = connection_settings(source)
        sql = select_sql(table, [name for name, _ in columns])
        return BlockingCursorSource(
            lambda batch_rows: stream_rows(settings, sql, batch_rows), columns
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load
