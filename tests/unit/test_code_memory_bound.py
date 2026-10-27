# Copyright (c) 2026 Kenneth Stott
# Canary: 713aa1ee-52e4-4c83-93cc-10d4ccf73af8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's statement over millions of rows in a bounded DuckDB (REQ-2100): its function's
rows stream in through the credit window, an ORDER BY sorts them past the memory limit by spilling
to disk, and the answer streams out a batch at a time. Its memory does not grow with the rows: a
sort of ten million rows grows the process no more than one of two million does."""

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
MEMORY_MB = 64
#: The smaller read's rows: past the memory limit too, a fifth of the larger.
SMALL_ROWS = 2_000_000
#: What the larger read may grow the process by beyond the smaller's: the allocator's noise
#: between two reads of one size.
SLACK_MB = 64

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


def _read(worker, rows: int, frame: list[int]) -> tuple[int, int, int]:
    """``rows`` rows sorted through the direct path, as a direct read takes them: pulled on one
    thread of their own (worker.pulled). The rows answered, the batches, the widest batch."""

    def ipcs():
        for ipc in worker.batches(
            "many",
            "table",
            [rows],
            ["n"],
            schema=schema_text(SCHEMA),
            takes=("inputs",),
            context={"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None},
        ):
            frame[0] = max(frame[0], len(ipc))
            yield ipc

    answered = batches = widest = 0
    reader = pa.RecordBatchReader.from_batches(SCHEMA, residual.batches(w.pulled(ipcs())))
    for batch in residual.answer("SELECT n, s FROM big ORDER BY s DESC", "duckdb", "big", reader):
        answered += batch.num_rows
        batches += 1
        widest = max(widest, batch.nbytes)
    return answered, batches, widest


def _growth(worker, rows: int, spill: Path) -> tuple[int, int]:
    """The process's growth while ``rows`` rows are sorted, from its size as the read began, and
    the most the read spilled."""
    process = psutil.Process()
    before = process.memory_info().rss
    peak, spilled = [before], [0]
    done = threading.Event()

    def watch() -> None:
        while not done.is_set():
            peak[0] = max(peak[0], process.memory_info().rss)
            spilled[0] = max(spilled[0], _spilled(spill))
            time.sleep(0.05)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        answered, batches, _widest = _read(worker, rows, [0])
    finally:
        done.set()
        watcher.join()
    assert answered == rows and batches > 1  # every row, a batch at a time
    return peak[0] - before, spilled[0]


def test_a_bounded_duckdbs_memory_does_not_grow_with_the_rows_it_sorts(worker):
    """Sorting five times the rows, both past the memory limit, the process grows no more than it
    did for the smaller read, give or take :data:`SLACK_MB`: what the rows take is on disk."""
    spill = Path(os.environ["PROVISA_DATA_DIR"]) / "code-spill"
    spill.mkdir(parents=True, exist_ok=True)
    residual.configure(memory_mb=MEMORY_MB, threads=2, spill_cap_mb=8192, spill_dir=str(spill))
    _read(worker, SMALL_ROWS, [0])  # a warm-up: the allocator's arenas and the pool taken once
    small, small_spilled = _growth(worker, SMALL_ROWS, spill)
    large, large_spilled = _growth(worker, ROWS, spill)
    print(f"grew {small / 2**20:.1f} MiB for {SMALL_ROWS}, {large / 2**20:.1f} MiB for {ROWS}")
    assert small_spilled > 0 and large_spilled > 0  # both sorts went past the memory limit
    assert large_spilled > small_spilled  # the rows went to disk
    assert large <= small + SLACK_MB * 2**20, (
        f"sorting {ROWS} rows grew the process {large / 2**20:.1f} MiB, past the "
        f"{small / 2**20:.1f} MiB of {SMALL_ROWS} plus {SLACK_MB} MiB"
    )
