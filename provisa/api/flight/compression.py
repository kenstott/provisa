# Copyright (c) 2026 Kenneth Stott
# Canary: e6a2c518-9d47-4b03-8f71-0c5b3d9e2a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compression of the record batches Arrow Flight sends, as the operator sets it.

``server.flight_compression`` (``none`` | ``lz4`` | ``zstd``, shipped ``none``) is read when a
stream is created, so a change applies to the next stream on every worker. The codec is applied
to each batch's buffers in the Arrow IPC framing; a Flight client reads which codec a message
uses from the message itself, so nothing is negotiated and a client needs no setting.

Every Flight response is built through :func:`record_batch_stream` or :func:`generator_stream`
so the setting reaches all of them."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

SETTING = "server.flight_compression"
OFF = "none"
CODECS = (OFF, "lz4", "zstd")


def ipc_write_options() -> pa.ipc.IpcWriteOptions | None:
    """The IPC write options for a stream created now: None when compression is off."""
    from provisa.core import settings_registry

    codec = settings_registry.value(SETTING)
    if codec == OFF:
        return None
    return pa.ipc.IpcWriteOptions(compression=codec)


def warnings_metadata(warnings: Any) -> pa.Buffer:
    """What a statement's answer says about itself (REQ-1350), as the ``app_metadata`` of a
    batch: ``{"provisa_warnings": [{code, params, message}, ...]}``, ASCII-escaped JSON."""
    import json

    payload = {"provisa_warnings": [w.as_dict() for w in warnings]}
    return pa.py_buffer(json.dumps(payload, ensure_ascii=True).encode("ascii"))


def _with_warnings(schema: pa.Schema, batches: Iterable[Any], warnings: Any) -> Iterable[Any]:
    """``batches`` behind one zero-row batch carrying the warnings: a Flight header is sent
    before do_get runs, so the warnings ride the stream, and a zero-row batch carries them for
    an empty result too.

    REQ-1949: a warning known only once the rows are read -- the answer was cut at a row limit
    -- is added to ``warnings`` when the drain ends, and rides a second zero-row batch, the last
    of the stream."""
    said = list(warnings)
    if said:
        yield (pa.RecordBatch.from_pylist([], schema=schema), warnings_metadata(said))
    yield from batches
    late = [w for w in warnings if w not in said]
    if late:
        yield (pa.RecordBatch.from_pylist([], schema=schema), warnings_metadata(late))


def record_batch_stream(
    data: Any, warnings: Any = ()
) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A stream over ``data`` (a Table), with IPC compression where configured. ``warnings``:
    what the statement's answer says about itself, sent ahead of the rows."""
    if warnings:
        return generator_stream(data.schema, data.to_batches(), warnings)
    return flight.RecordBatchStream(data, options=ipc_write_options())  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


def generator_stream(
    schema: pa.Schema, batches: Iterable[Any], warnings: Any = ()
) -> flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A lazy stream over ``batches``, with IPC compression where configured. ``warnings``: what
    the statement's answer says about itself, sent ahead of the rows -- and, for what is known
    only once they are read, after them. A plan's own list is read again when the rows end."""
    if warnings or isinstance(warnings, list):
        batches = _with_warnings(schema, batches, warnings)
    return flight.GeneratorStream(schema, batches, options=ipc_write_options())  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
