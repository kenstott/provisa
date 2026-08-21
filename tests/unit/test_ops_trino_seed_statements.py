# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements ops seeding sends must be ones Trino accepts (REQ-016).

Every one of these is best-effort and swallowed, so a statement Trino can never parse fails
silently forever: partition evolution went out as Spark's ``ADD PARTITION FIELD`` and raised
SYNTAX_ERROR on every boot, and snapshot reclamation went out with a TIMESTAMP where the
procedures take a duration and raised INVALID_PROCEDURE_ARGUMENT.
"""

from __future__ import annotations

from provisa.observability.ops_trino import seed_ops_trino


class _Cursor:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def execute(self, sql: str) -> None:
        self._sink.append(sql)

    def fetchall(self):
        return []


class _Conn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.statements)


def _seed() -> list[str]:
    conn = _Conn()
    seed_ops_trino(conn, [])
    return conn.statements


def test_partition_evolution_uses_trino_syntax():
    evolutions = [s for s in _seed() if "partitioning" in s and s.startswith("ALTER TABLE")]
    assert evolutions, "no partition evolution statement was sent"
    assert all("SET PROPERTIES partitioning = ARRAY[" in s for s in evolutions)
    assert not any("ADD PARTITION FIELD" in s for s in _seed())


def test_seeding_does_not_run_snapshot_reclamation():
    """Reclamation belongs to the scheduler (REQ-302/303), which passes a duration threshold."""
    assert not [s for s in _seed() if "EXECUTE expire_snapshots" in s or "remove_orphan_files" in s]
