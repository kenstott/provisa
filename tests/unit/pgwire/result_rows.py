# Copyright (c) 2026 Kenneth Stott
# Canary: e33d54a1-5c06-4702-bd48-131af196cdc8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a pgwire result hands the encoder, read back as Python rows for a test to look at
(REQ-2194: a result has no Python rows of its own; its batches go to the Rust encoder)."""

from __future__ import annotations

from typing import Any

from provisa.core.ir_arrow import column_values


def rows_of(result: Any, limit: int = 0) -> list[list[Any]]:
    """The next ``limit`` rows (0: all that are left) of ``result``'s batches, as an Execute of
    that limit takes them, each value brought to its described type by the result's coercers; a
    passthrough's rows are its DataRow messages."""
    data = result.data()
    coerce = getattr(result, "_coerce", ())
    out: list[list[Any]] = []
    for batch in data.take(limit):
        if data.raw:
            out.extend(batch)
            continue
        if data.rows:  # REQ-2195: a row result's rows, as they are
            for row in batch:
                cells = list(row)
                for i, coercer in coerce:
                    cells[i] = None if cells[i] is None else coercer(cells[i])
                out.append(cells)
            continue
        columns = [column_values(c) for c in batch.columns]
        for i, coercer in coerce:
            columns[i] = [None if v is None else coercer(v) for v in columns[i]]
        out.extend(list(row) for row in zip(*columns, strict=True))
    return out
