# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8a2e17-9b63-4f50-a7d1-2e6f3b9c0d84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ClickHouse String is bytes (REQ-2167, REQ-2171).

chdb and the server write a String to Arrow as utf8 (output_format_arrow_string_as_string), whatever
its bytes: a send function's answer, int4send(255), is the bytes 00 00 00 ff, which no text is. Run
43382046085 found 35 such calls failing XX000 "'utf-8' codec can't decode": the rows were read as
text. A column whose bytes are not UTF-8 is read as the bytes it is; every other column as it came.
(String and Nothing, a bare NULL, are both binary when the setting is off, so it stays on.)"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime, bytes_where_not_text


def _text_of(values: list[bytes | None]) -> pa.Array:
    """A utf8 column holding these bytes, as ClickHouse writes a String."""
    binary = pa.array(values, type=pa.binary())
    return pa.Array.from_buffers(pa.string(), len(values), binary.buffers(), binary.null_count)


def test_a_string_of_bytes_no_text_holds_is_read_as_its_bytes():
    batch = pa.record_batch(
        [_text_of([b"\x00\x00\x00\xff", None]), _text_of([b"abc", b"\xc3\xa9"]), pa.array([1, 2])],
        names=["sent", "text", "n"],
    )
    read = bytes_where_not_text(batch)
    assert read.column(0).to_pylist() == [b"\x00\x00\x00\xff", None]
    assert read.column(1).to_pylist() == ["abc", "é"]
    assert read.column(2).to_pylist() == [1, 2]
    assert read.schema.names == ["sent", "text", "n"]


def test_a_batch_of_text_is_the_batch_it_was():
    batch = pa.record_batch([_text_of([b"abc"])], names=["text"])
    assert bytes_where_not_text(batch) is batch


class _Backend:
    """A backend answering one batch, as chdb's ArrowStream does."""

    def __init__(self, batch: pa.RecordBatch) -> None:
        self._batch = batch

    def command(self, sql: str, params: dict | None = None) -> None:
        del sql, params

    def query_arrow_stream(
        self, sql: str, params: dict | None = None
    ) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        del sql, params
        return self._batch.schema, iter([self._batch])


def test_the_rows_of_a_send_are_its_bytes():
    batch = pa.record_batch([_text_of([b"\x00\x00\x00\xff"])], names=["v"])
    runtime: Any = ClickHouseFederationRuntime(_Backend(batch))  # pyright: ignore[reportArgumentType]
    result = runtime.run_sync("SELECT int4send(255) AS v")
    assert list(result.iter_rows()) == [(b"\x00\x00\x00\xff",)]
    assert result.column_types == ["VARCHAR"]
