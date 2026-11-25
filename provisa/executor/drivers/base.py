# Copyright (c) 2026 Kenneth Stott
# Canary: 9395989b-3971-4ae2-b073-444f3d6120fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Abstract driver interface for direct database execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, NamedTuple

from provisa.executor.result import QueryResult

if TYPE_CHECKING:
    import pyarrow as pa

# Requirements: REQ-027, REQ-052


class DirectResultStream(ABC):  # REQ-1190
    """A lazily-drained DIRECT read: a server-side cursor whose fetch state OUTLIVES the open call.

    The single reachable source's own driver streams the result in bounded batches so a large
    passthrough scan never fully materializes in Provisa — the DIRECT analogue of the ENGINE
    streaming terminal (REQ-1190, streaming-uniformity-gap Defect 1). Column names/types are known
    at open (before the first row); ``fetch`` pulls the next batch (``[]`` = exhausted); ``close``
    releases the cursor/connection/transaction exactly once."""

    column_names: list[str]
    column_types: list[str] | None

    @abstractmethod
    async def fetch(self, size: int) -> list[tuple]:
        """Pull up to ``size`` rows from the server-side cursor; an empty list means exhausted."""

    @abstractmethod
    async def close(self) -> None:
        """Release the cursor, transaction, and pooled connection. Idempotent."""


class DirectArrowStream(DirectResultStream):  # REQ-2194
    """A DIRECT read whose driver fetches its result as Arrow: the Arrow face of
    :class:`DirectResultStream`, as :class:`~provisa.executor.result.ArrowStreamResult` is of the
    ENGINE stream.

    ``schema`` (the result's Arrow schema) is known at open, beside ``column_names``;
    ``fetch_arrow`` pulls the driver's next record batch (``None`` = exhausted), bounded by the
    driver's own fetch; ``to_rows`` builds a batch's Python rows, the values the driver's own rows
    hold. ``fetch`` is the row face for a consumer that reads rows: each batch is turned into rows
    as that consumer pulls it, never at the driver's fetch. Either face drains the one cursor."""

    schema: pa.Schema

    @abstractmethod
    async def fetch_arrow(self) -> pa.RecordBatch | None:
        """The driver's next record batch; ``None`` when the result is exhausted."""

    @abstractmethod
    def to_rows(self, batch: pa.RecordBatch) -> list[tuple]:
        """``batch`` as the Python rows the driver's own rows hold."""

    def __init__(self) -> None:
        # The rows of a batch the row face built past the size its last ``fetch`` asked for.
        self._pending_rows: list[tuple] = []

    async def fetch(self, size: int) -> list[tuple]:
        pending = self._pending_rows
        while len(pending) < size:
            batch = await self.fetch_arrow()
            if batch is None:
                break
            pending.extend(self.to_rows(batch))
        rows, self._pending_rows = pending[:size], pending[size:]
        return rows


class GeneratedColumn(NamedTuple):  # REQ-2167
    """A column its source fills from a sequence or counter when a row is given no value for it:
    its name, and its place among the table's columns (1-based), which a row given without a
    column list is matched to."""

    name: str
    position: int


class DirectDriver(ABC):  # REQ-027, REQ-052
    """Abstract async driver for direct database execution."""

    #: REQ-2167: how a write reports the value it made the source generate -- ``"returning"``,
    #: the statement returns it; ``"last_insert_id"``, the connection reports it
    #: (:meth:`execute_reporting_generated`) -- or None: the source has no way to.
    generated_keys: str | None = None

    async def generated_columns(
        self, schema: str | None, table: str
    ) -> tuple[GeneratedColumn, ...]:  # REQ-2167
        """The columns of ``table`` the source generates a value for, read from its catalog, in
        their order in the table. Only for a driver whose ``generated_keys`` is set."""
        raise NotImplementedError(f"{type(self).__name__} reports no generated key")

    async def execute_reporting_generated(
        self, sql: str, params: list | None = None
    ) -> QueryResult:  # REQ-2167
        """Run a write, the result carrying the value it made the source generate
        (``QueryResult.generated_key``), None when it generated none. Only for a driver whose
        ``generated_keys`` is ``"last_insert_id"``."""
        raise NotImplementedError(f"{type(self).__name__} reports no generated key")

    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        """Whether ``open_stream`` yields a genuine server-side cursor. Default ``False`` — a driver
        without a bounded streaming read still materializes via ``execute`` (the remaining DIRECT
        conformance gap for that source, streaming-uniformity-gap Defect 1)."""
        return False

    async def open_stream(
        self, sql: str, params: list | None = None
    ) -> DirectResultStream:  # REQ-1190
        """Open a bounded server-side cursor over ``sql``. Only valid when ``supports_streaming``."""
        raise NotImplementedError(f"{type(self).__name__} does not support streaming DIRECT reads")

    def bind_source(self, source_id: str) -> None:  # REQ-2000
        """Told which registered source the driver serves, before ``configure``. A driver that
        reads its source's registered tables to lower a statement (Qdrant) keeps it; the SQL
        drivers do not need it."""

    def configure(self, extra: dict[str, str]) -> None:  # REQ-986/987/988
        """Accept source-specific connection extras from ``Source.federation_hints`` before
        ``connect`` — e.g. Databricks ``http_path``, Snowflake ``account``/``warehouse``, ClickHouse
        native-vs-http ``scheme``. The standard ``connect(host, port, database, user, password)`` args
        can't carry these, so warehouse drivers override this to stash them. No-op by default, so the
        RDBMS drivers are untouched."""

    #: REQ-2191: the driver connects at an address its settings name (a JDBC source's
    #: URL), checked and pinned where it connects, never at the ``host`` it is handed.
    names_own_destination: bool = False

    @property
    def dialect(self) -> str | None:  # REQ-2191
        """The SQL dialect this source's statements are written in when the source chose it
        (a JDBC source's ``sql_dialect``), read after ``configure``; None when its type
        names it (``SOURCE_TO_DIALECT``)."""
        return None

    @abstractmethod
    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        """Establish connection/pool."""

    @abstractmethod
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL and return rows + column names."""

    @abstractmethod
    async def close(self) -> None:
        """Close connection/pool."""

    async def execute_ddl(self, sql: str) -> None:
        """Execute a DDL statement (CREATE/DROP/ALTER). No return value."""
        # Default: fall back to execute() and discard result
        await self.execute(sql)

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Whether the driver has an active connection."""
