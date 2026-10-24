# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's statement over ten million rows in a bounded DuckDB (REQ-2100): its function's
rows stream in through the credit window, an ORDER BY sorts them past the memory limit by spilling
to disk, and the answer streams out a batch at a time. The process grows by no more than the
DuckDB's memory limit, the frames the window and the pull's queue hold, one answer batch and a
pinned baseline."""

# Requirements: REQ-2100

from __future__ import annotations

import os
import sys
import textwrap
import threading
import time
from pathlib import Path

import psutil
import pyarrow as pa
import pytest

from provisa.codehost import residual
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import schema_text
from provisa.python.source import HOST

pytestmark = [pytest.mark.unit, pytest.mark.slow]

ROWS = 10_000_000
MEMORY_MB = 256
#: What the process grows by beyond the DuckDB's memory, the window's frames and one answer
#: batch: the allocator's retained arenas and DuckDB's own buffers outside its memory limit.
#: PROVISIONAL: the largest "overhead" line of five runs of this test on the macOS arm64 dev host
#: (-1.8, 9.9, 31.8, 60.9 and 79.4 MiB), rounded up. To be pinned to the largest of five runs on
#: the CI runner.
BASELINE_MB = 80

_CODE = textwrap.dedent(
    '''
    from dataclasses import dataclass


    @dataclass
    class ManyInputs:
        n: int


    def many(inputs: ManyInputs):
        """@table"""
        for i in range(inputs.n):
            yield {"n": i, "s": f"{(i * 7919) % 1000003:07d}-row"}
    '''
)
SCHEMA = pa.schema([("n", pa.int64()), ("s", pa.utf8())])


@pytest.fixture
def worker(tmp_path):
    code = tmp_path / "code"
    code.mkdir()
    (code / "big.py").write_text(_CODE)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), "big"],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
    )
    started = w.Worker("big-src", "h1", command)
    started.start()
    yield started
    started.stop()


def _spilled(directory: Path) -> int:
    return sum(p.stat().st_size for p in directory.rglob("*") if p.is_file())


def test_ten_million_rows_sort_in_a_bounded_duckdb_and_stream_out(worker):
    spill = Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill"
    spill.mkdir(parents=True, exist_ok=True)
    residual.configure(memory_mb=MEMORY_MB, threads=2, spill_cap_mb=8192, spill_dir=str(spill))
    process = psutil.Process()
    before = process.memory_info().rss
    peak, spilled, frame = [before], [0], [0]
    done = threading.Event()

    def watch() -> None:
        while not done.is_set():
            peak[0] = max(peak[0], process.memory_info().rss)
            spilled[0] = max(spilled[0], _spilled(spill))
            time.sleep(0.05)

    def ipcs():
        for ipc in worker.batches(
            "many",
            "table",
            [ROWS],
            ["n"],
            schema=schema_text(SCHEMA),
            takes=("inputs",),
            context={"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None},
        ):
            frame[0] = max(frame[0], len(ipc))
            yield ipc

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    rows = answered = 0
    widest = 0
    try:
        # As a direct read takes them: pulled on one thread of their own (worker.pulled).
        reader = pa.RecordBatchReader.from_batches(SCHEMA, residual.batches(w.pulled(ipcs())))
        for batch in residual.answer(
            "SELECT n, s FROM big ORDER BY s DESC", "duckdb", "big", reader
        ):
            rows += batch.num_rows
            answered += 1
            widest = max(widest, batch.nbytes)
    finally:
        done.set()
        watcher.join()
    assert rows == ROWS and answered > 1  # every row, a batch at a time
    assert spilled[0] > 0  # the sort went past the memory limit and spilled
    grew = peak[0] - before
    bound = MEMORY_MB * 2**20 + (2 * w._QUEUED + 1) * frame[0] + widest + BASELINE_MB * 2**20  # noqa: SLF001
    overhead = (grew - MEMORY_MB * 2**20 - (2 * w._QUEUED + 1) * frame[0] - widest) / 2**20  # noqa: SLF001
    print(f"overhead {overhead:.1f} MiB (grew {grew / 2**20:.1f} MiB)")
    assert grew <= bound, f"the process grew {grew / 2**20:.1f} MiB, past {bound / 2**20:.1f} MiB"
