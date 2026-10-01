# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3b7c21-4f60-4a8d-b5e2-6c1d0f9a7e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Several worker processes evaluate licensing at the same moment (REQ-1900, REQ-1136).

Every worker of a `--workers N` launch reaches the licensing check together. Each persisted the
high-water mark through the SAME temporary file name, so one worker's rename took the file out
from under another's: ``FileNotFoundError: highwater.tmp -> highwater.json`` and a worker that
started with no licensing state."""

# Requirements: REQ-1900, REQ-1136

from __future__ import annotations

import threading

from provisa.licensing import monotonic


def test_concurrent_writers_all_persist_the_high_water_mark(tmp_path):
    path = tmp_path / "highwater.json"
    writers = 8
    start = threading.Barrier(writers)
    failures: list[BaseException] = []

    def _write(n: int) -> None:
        try:
            start.wait(timeout=10)
            for i in range(200):
                monotonic.update_highwater(path, float(n * 1000 + i))
        except BaseException as exc:  # collected and asserted on below
            failures.append(exc)

    threads = [threading.Thread(target=_write, args=(n,)) for n in range(writers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not failures, f"{len(failures)} writers failed: {failures[0]!r}"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["highwater.json"]
    assert monotonic.read_highwater(path) > 0
