# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8d3f21-7c6a-4e19-9d02-1a4f8e2b6c53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Transaction coordinator for the airport Flight service (REQ-1120).

The DuckDB airport extension issues ``create_transaction`` before a DML statement
and can query ``get_transaction_status``; the identifier is carried into the DML
call and the server commits/rolls back around the operation. Provisa's governed
pipeline routes each mutation through ``_compile_govern_execute`` → the native
driver, which auto-commits per statement (read-committed on the engine that can't
do better — the airport transport has no multi-statement session channel).

So this coordinator is best-effort read-committed, matching airport-go's own model
(``docs`` note: "each operation auto-commits on success; multi-statement
transactions are out of scope"). It mints a stable identifier, tracks status
(active → committed / aborted) so ``get_transaction_status`` answers correctly, and
lets the DML handler scope a mutation to a created transaction. Statuses use the
airport-go vocabulary: ``active`` / ``committed`` / ``aborted``.
"""

from __future__ import annotations

import threading
import uuid


class AirportTransactionManager:
    """Thread-safe in-memory transaction registry (airport worker threads call in)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: dict[str, str] = {}

    def begin(self) -> str:
        tx_id = str(uuid.uuid4())
        with self._lock:
            self._states[tx_id] = "active"
        return tx_id

    def commit(self, tx_id: str) -> None:
        with self._lock:
            if tx_id in self._states:
                self._states[tx_id] = "committed"

    def rollback(self, tx_id: str) -> None:
        with self._lock:
            if tx_id in self._states:
                self._states[tx_id] = "aborted"

    def status(self, tx_id: str) -> tuple[str, bool]:
        """Return ``(status, exists)`` for ``tx_id`` (airport get_transaction_status shape)."""
        with self._lock:
            if tx_id in self._states:
                return self._states[tx_id], True
        return "", False
