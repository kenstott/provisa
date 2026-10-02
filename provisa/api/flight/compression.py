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


def record_batch_stream(data: Any) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A Flight stream over a table or reader, compressed as the operator set."""
    return flight.RecordBatchStream(data, options=ipc_write_options())  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


def generator_stream(schema: pa.Schema, batches: Iterable[Any]) -> flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A Flight stream over lazily produced batches, compressed as the operator set."""
    return flight.GeneratorStream(schema, batches, options=ipc_write_options())  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
