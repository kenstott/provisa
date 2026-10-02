# Copyright (c) 2026 Kenneth Stott
# Canary: 1851671b-792c-4901-909c-fc6cdfc99c96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Spooling a single-document source to a temporary file while its replica is built (REQ-1915).

Some sources cannot be read by row or through a cursor: they answer a request with one document
(Druid, Pinot, Prometheus, a remote GraphQL endpoint, an RSS feed). Held in memory, that
document and the rows parsed from it cost several times its size for the length of the build.
Here the response body is written to a file as it arrives and parsed from the file a bounded
batch at a time, so the build holds one batch whatever the document's size.

This is NOT optimal and is declared as such (``SourceRead.SINGLE_DOCUMENT_SPOOLED``): the
source still produces its whole answer for every build, the parse is a second pass, and for the
length of a build the source's data sits on this node's disk, not encrypted by Provisa. It is
used only for a source with no cursor and no stream; a store is never loaded from a file.

The files live in one directory this node owns, under its data directory
(``<data dir>/replica-spool``, mode 0700, files 0600), never the system temporary directory.
Each file is held under an OS file lock by the process using it and removed when its build
ends, however it ends. A file found unlocked is one a crashed build left: it is removed at node
start and before every new spool. The directory's total size is bounded by one operator setting
(``replication.spool_max_bytes``); a build that would pass it fails by name
(:class:`ReplicaSpoolFull`) — the document is never truncated and never read into memory
instead.
"""

# Requirements: REQ-1915

from __future__ import annotations

import fcntl
import logging
import os
import secrets
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractContextManager, aclosing, contextmanager
from pathlib import Path
from typing import IO, Any

import pyarrow as pa

from provisa.core import request_deadline
from provisa.core.ir_arrow import arrow_schema, rows_to_batch
from provisa.federation.data_replicator import SourceCaps, SourceRead
from provisa.federation.replica_source import _stepped

log = logging.getLogger(__name__)

SPOOL_SUFFIX = ".spool"
_CHUNK_BYTES = 1 << 20
#: The directory's total is re-read after this many bytes written, to see other builds' files.
_RECOUNT_BYTES = 16 << 20

#: What a kind's reader is handed: given the call that opens its streamed HTTP response, a
#: context manager yielding the response body as a readable, seekable file.
Spooler = Callable[[Callable[[], AbstractContextManager[Any]]], AbstractContextManager[IO[bytes]]]


class ReplicaSpoolFull(Exception):
    """Spooling a source's answer would take the node's spool directory over its limit."""

    def __init__(self, table: str, needed: int, limit: int) -> None:
        self.table = table
        self.needed = needed
        self.limit = limit
        super().__init__(
            f"the replica build of {table} was refused: spooling its source's answer needs "
            f"{needed:,} bytes of this node's spool directory, over the limit of {limit:,} "
            "(replication.spool_max_bytes)"
        )


def spool_directory() -> Path:
    """The one directory this node spools into: ``replica-spool`` under its data directory."""
    from provisa.core import settings_registry

    data_dir = settings_registry.value("bootstrap.data_dir")
    # The data directory's documented default when the operator names none (REQ-1913).
    base = Path(data_dir) if data_dir else Path.home() / ".provisa"
    return base / "replica-spool"


def spool_limit() -> int:
    """The most bytes this node's spool directory may hold (``replication.spool_max_bytes``)."""
    from provisa.core import settings_registry

    return int(settings_registry.value("replication.spool_max_bytes"))


def sweep(directory: Path) -> list[Path]:
    """Remove every spool file in ``directory`` that no process holds: what a crashed build
    left. A file whose lock is held belongs to a running build and is left alone. Returns the
    paths removed."""
    removed: list[Path] = []
    if not directory.is_dir():
        return removed
    for path in sorted(directory.glob(f"*{SPOOL_SUFFIX}")):
        try:
            fd = os.open(path, os.O_RDWR)
        except FileNotFoundError:
            continue  # its build ended between the listing and this open
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue  # held: a running build's
            path.unlink(missing_ok=True)
            removed.append(path)
        finally:
            os.close(fd)
    if removed:
        log.warning("replica spool: removed %d file(s) left by a build that died", len(removed))
    return removed


def used_bytes(directory: Path) -> int:
    """The bytes the spool files in ``directory`` hold now."""
    total = 0
    for path in directory.glob(f"*{SPOOL_SUFFIX}"):
        try:
            total += path.stat().st_size
        except FileNotFoundError:
            continue  # its build ended between the listing and the stat
    return total


class SpoolFile:
    """One build's spool file: created locked, bounded as it is written, removed on exit."""

    def __init__(self, directory: Path, *, table: str, limit: int) -> None:
        self._directory = directory
        self._table = table
        self._limit = limit
        self._file: IO[bytes] | None = None
        self._written = 0
        self._others = 0
        self._since_recount = 0
        self.path: Path | None = None

    def __enter__(self) -> "SpoolFile":
        self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        sweep(self._directory)
        shield = request_deadline.shielded()
        while self._file is None:
            path = self._directory / f"{secrets.token_hex(16)}{SPOOL_SUFFIX}"
            with shield.lock:
                shield.settle()
                fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX)
            # A sweep in another process may have taken the file between its creation and its
            # lock and removed it: the name must still be this file.
            try:
                same = os.stat(path).st_ino == os.fstat(fd).st_ino
            except FileNotFoundError:
                same = False
            if not same:
                os.close(fd)
                continue
            self._file = os.fdopen(fd, "w+b")
            self.path = path
        self._others = used_bytes(self._directory)
        return self

    def reserve(self, declared: int) -> None:
        """Refuse a body of ``declared`` bytes before any of it is written, if it cannot fit."""
        if self._others + declared > self._limit:
            raise ReplicaSpoolFull(self._table, self._others + declared, self._limit)

    def write(self, chunk: bytes) -> None:
        assert self._file is not None
        self._written += len(chunk)
        self._since_recount += len(chunk)
        if self._since_recount >= _RECOUNT_BYTES:
            self._file.flush()
            self._since_recount = 0
            self._others = used_bytes(self._directory) - self._written
        if self._others + self._written > self._limit:
            raise ReplicaSpoolFull(self._table, self._others + self._written, self._limit)
        self._file.write(chunk)

    def receive(self, response: Any) -> None:
        """Write a streamed HTTP response's body to the file as it arrives."""
        response.raise_for_status()
        length = response.headers.get("content-length")
        # A compressed body's declared length is not the size it is written at.
        if length is not None and "content-encoding" not in response.headers:
            self.reserve(int(length))
        for chunk in response.iter_bytes(_CHUNK_BYTES):
            self.write(chunk)

    def reader(self) -> IO[bytes]:
        """The file, rewound, for parsing. It may be rewound again for another pass."""
        assert self._file is not None
        self._file.flush()
        self._file.seek(0)
        return self._file

    def __exit__(self, *exc: Any) -> None:
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            file, self._file = self._file, None
            path, self.path = self.path, None
            if path is not None:
                path.unlink(missing_ok=True)
            if file is not None:
                file.close()  # releases the lock


def _json_parser() -> Any:
    """ijson's C backend, by name: where it is not installed this raises, and no slower parser
    runs in its place."""
    import ijson

    return ijson.get_backend("yajl2_c")


#: How much of the document the parser reads at a time; a parse failure is located to within it.
_PARSE_BYTES = 64 * 1024


class AnswerNotJson(ValueError):
    """A spooled answer the JSON parser cannot read: what is wrong with it and where.

    The message carries the parser's reason and the stretch of the answer it was in, and never
    the text around it: that is the source's data, and this message is recorded on the replica
    for an operator to read."""

    def __init__(self, cause: str, before_byte: int, table: str | None = None) -> None:
        self.cause = cause
        self.before_byte = before_byte
        of = f" read for the replica of {table}" if table else ""
        super().__init__(
            f"the answer{of} is not valid JSON: {cause} "
            f"(in the {_PARSE_BYTES // 1024} KiB before byte {before_byte} of the answer)"
        )


def _parsed(body: IO[bytes], events: Iterator[Any]) -> Iterator[Any]:
    """``events`` of the parser reading ``body``, a failure of the parse raised as
    :class:`AnswerNotJson`."""
    import ijson

    try:
        yield from events
    except ijson.JSONError as exc:
        # The parser's reason is its first line; the lines after it quote the document.
        cause = str(exc).splitlines()[0].strip().rstrip(".")
        raise AnswerNotJson(cause, body.tell()) from None


def json_items(body: IO[bytes], prefix: str) -> Iterator[Any]:
    """The JSON values at ``prefix`` of the document in ``body``, read incrementally from its
    start. Numbers with a fraction are floats, as ``json`` reads them. A document the parser
    cannot read raises :class:`AnswerNotJson` where the read reaches the fault."""
    parser = _json_parser()

    body.seek(0)
    return _parsed(body, parser.items(body, prefix, use_float=True, buf_size=_PARSE_BYTES))


def json_starts(body: IO[bytes], prefix: str) -> str | None:
    """The first parse event at ``prefix`` of the document in ``body`` (``start_array``,
    ``start_map``, ``string``, ``null`` …), or None when the document has nothing there."""
    parser = _json_parser()

    body.seek(0)
    events = _parsed(body, parser.parse(body, use_float=True, buf_size=_PARSE_BYTES))
    for at, event, _value in events:
        if at == prefix:
            return event
    return None


class SpooledDocumentSource:
    """A single-document source read through a spool file. ``rows(spooled)`` is the kind's own
    reader: a blocking generator of row dicts that opens its HTTP response through ``spooled``
    and parses the file it is given. Each response it opens is spooled, bounded and removed
    when the reader leaves it; batches are cut from the rows as they are parsed."""

    caps = SourceCaps(frozenset({SourceRead.SINGLE_DOCUMENT_SPOOLED}))

    def __init__(
        self,
        rows: Callable[[Spooler], Iterator[dict]],
        columns: list[tuple[str, str]],
        *,
        table: str,
        directory: Path | None = None,
        limit: int | None = None,
    ) -> None:
        self._rows = rows
        self._columns = columns
        self._table = table
        self._directory = directory
        self._limit = limit

    def _row_batches(self, batch_rows: int) -> Iterator[list[dict]]:
        directory = self._directory if self._directory is not None else spool_directory()
        limit = self._limit if self._limit is not None else spool_limit()

        @contextmanager
        def spooled(send: Callable[[], AbstractContextManager[Any]]) -> Iterator[IO[bytes]]:
            with SpoolFile(directory, table=self._table, limit=limit) as spool:
                with send() as response:
                    spool.receive(response)
                yield spool.reader()

        batch: list[dict] = []
        try:
            for row in self._rows(spooled):
                batch.append(row)
                if len(batch) >= batch_rows:
                    yield batch
                    batch = []
        except AnswerNotJson as exc:
            # Named for the table, so the build's recorded error says what and where.
            raise AnswerNotJson(exc.cause, exc.before_byte, self._table) from None
        if batch:
            yield batch

    async def batches(self, batch_rows: int) -> AsyncIterator[pa.RecordBatch]:
        schema = arrow_schema(self._columns)
        # aclosing: a reader that stops early closes the generator now, which removes the file.
        async with aclosing(_stepped(lambda: self._row_batches(batch_rows))) as stepped:
            async for rows in stepped:
                yield rows_to_batch(rows, self._columns, schema)
