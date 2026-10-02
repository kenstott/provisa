# Copyright (c) 2026 Kenneth Stott
# Canary: 8fa5fc33-774e-4e4a-86c7-507c3589806f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The store and the engine as parties to a replica build (REQ-1915).

``data_replicator`` is given a source, a target and an engine, each declaring what it can do.
The source parties are in ``replica_source``; the store's write faces in ``replica_target``.
This module holds the engine parties and the one place the target for a store is chosen.

- :class:`StoreReadingEngine` — an engine that only reads the finished replica from its store.
  It copies nothing itself; its part is what it must do after a swap.
- :class:`PgStatementCopy` — the PostgreSQL engine copying a table it reaches through
  postgres_fdw as its own statement: no row passes through Provisa.
"""

# Requirements: REQ-1915

from __future__ import annotations

from typing import Any

from provisa.core import request_deadline
from provisa.federation.data_replicator import (
    BuildOutcome,
    EngineCaps,
    EngineRun,
    Method,
    NoReplicationMethod,
    TargetCaps,
    TargetWrite,
)
from provisa.federation.replica_address import ReplicaAddress
from provisa.federation.replica_target import PostgresStoreTarget

_NO_COPY = EngineCaps(reaches_source=False, runs=frozenset())


class StoreReadingEngine:
    """An engine that takes no part in the copy: it reads the replica once it stands in the
    store. ``after_swap`` is the engine's own step for seeing a newly swapped table."""

    caps = _NO_COPY

    def __init__(self, backend: Any, state: Any) -> None:
        self._backend = backend
        self._state = state

    async def copy(self, prior_hash: str | None) -> BuildOutcome:
        del prior_hash
        raise NoReplicationMethod(["the engine runs no statement-level copy"])

    async def after_swap(self) -> None:
        await self._backend.after_replica_swap(self._state)


class PgStatementCopy:
    """The PostgreSQL engine builds the replica from the source's postgres_fdw foreign table as
    its own statements, in one transaction: a build table filled by ``INSERT ... SELECT``, its
    content hashed by the server, then swapped in (``replica_target.pg_statement_copy``). The
    copy runs on a connection of its own, taken and returned inside the deadline shield."""

    caps = EngineCaps(reaches_source=True, runs=frozenset({EngineRun.STATEMENT}))

    def __init__(
        self,
        backend: Any,
        state: Any,
        merged_source: Any,
        *,
        address: ReplicaAddress,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        self._backend = backend
        self._state = state
        self._source = merged_source
        self._address = address
        self._columns = columns
        self._pk = pk_columns

    async def copy(self, prior_hash: str | None) -> BuildOutcome:
        import asyncio

        copied, content_hash, changed = await asyncio.to_thread(self._copy, prior_hash)
        return BuildOutcome(
            rows_copied=copied,
            method=Method.ENGINE_STATEMENT.value,
            content_hash=content_hash,
            changed=changed,
        )

    def _copy(self, prior_hash: str | None) -> tuple[int, str, bool]:
        import psycopg2

        from provisa.federation.replica_target import pg_statement_copy

        runtime = self._backend._runtime_for(self._state)
        details = runtime._engine.resolve(self._source).details
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            con = psycopg2.connect(runtime._engine_dsn)
            con.autocommit = True  # the copy issues its own BEGIN / COMMIT
        try:
            cur = con.cursor()
            runtime._ensure_foreign_table(cur, details, self._source.table_name)
            local, name = details["local_schema"], self._source.table_name
            return pg_statement_copy(
                cur,
                source_relation='"{}"."{}"'.format(
                    local.replace('"', '""'), name.replace('"', '""')
                ),
                schema=self._address.schema,
                table=self._address.table,
                columns=self._columns,
                pk_columns=self._pk,
                prior_hash=prior_hash,
            )
        finally:
            with shield.lock:
                shield.settle()
                con.close()

    async def after_swap(self) -> None:
        await self._backend.after_replica_swap(self._state)


def store_target(
    store_backend: str,
    store_dsn: str,
    *,
    address: ReplicaAddress,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    engine_writes_store: bool,
) -> Any:
    """The write face of a replica at ``address`` in the store ``store_backend``
    (``FederationEngine.replica_store_backend``). ``engine_writes_store`` says the engine can
    write this store with a statement of its own, which is what a statement-level copy needs."""
    if store_backend == "postgresql":
        target = PostgresStoreTarget(
            store_dsn,
            schema=address.schema,
            table=address.table,
            columns=columns,
            pk_columns=pk_columns,
        )
        if engine_writes_store:
            target.caps = TargetCaps(
                frozenset({TargetWrite.COPY_STREAM, TargetWrite.STATEMENT_COPY}),
                atomic_swap=True,
                load=target.caps.load,
            )
        return target
    raise NoReplicationMethod([f"no replica write face for a {store_backend!r} store"])
