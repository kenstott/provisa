# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A segment key is read at the width the system keeps it.

PostgreSQL writes its segment's key to ``postmaster.pid`` as an unsigned long, so a key with its
top bit set (key_t is 32 bits) reads back as 18446744071574667811; ``ipcs`` lists it as
0x80c04223. Read whole, the key matched no listed segment and the segment was never released."""

from __future__ import annotations

from pathlib import Path

from provisa.core.pg_shared_memory import recorded_segment


def _pid_file(datadir: Path, shmem_line: str) -> None:
    lines = ["4242", str(datadir), "1792940000", "5432", "/tmp", "localhost", shmem_line, "ready"]
    (datadir / "postmaster.pid").write_text("\n".join(lines) + "\n")


def test_a_key_with_its_top_bit_set_reads_as_the_key_the_system_lists(tmp_path):
    _pid_file(tmp_path, "18446744071574667811   7340051")
    assert recorded_segment(tmp_path) == (0x80C04223, 7340051)


def test_a_key_without_it_reads_as_written(tmp_path):
    _pid_file(tmp_path, "  1042447707   65536")
    assert recorded_segment(tmp_path) == (1042447707, 65536)
