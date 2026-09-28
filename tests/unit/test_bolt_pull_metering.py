# Copyright (c) 2026 Kenneth Stott
# Canary: 1a7fd90e-12a7-4013-bb70-902205522296
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1885: Bolt row streaming no longer logs/reports unconditionally per row.

``_send``'s per-write WARNING is gated behind DEBUG (and downgraded), and PULL's per-row
metering call is batched into one ``report()`` per PULL instead of one per row.
"""

from __future__ import annotations

import logging


from provisa.bolt.session import BoltSession, State


class _Writer:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.chunks.append(data)

    async def drain(self) -> None:  # pragma: no cover - unused by these tests
        pass


def _session_with_rows(rows: list[list[object]]) -> BoltSession:
    session = BoltSession(_Writer(), (5, 4))
    session.org_id = "org-1"
    session.state = State.STREAMING
    session._result_columns = ["a"]
    session._result_rows = rows
    session._pull_offset = 0
    return session


class TestSendLogGating:
    def test_send_does_not_warn(self, caplog):
        session = _session_with_rows([[1]])
        with caplog.at_level(logging.WARNING, logger="provisa.bolt.session"):
            session.send_success({})
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_send_debug_message_only_emitted_at_debug_level(self, caplog):
        session = _session_with_rows([[1]])

        with caplog.at_level(logging.INFO, logger="provisa.bolt.session"):
            session.send_success({})
        assert not [r for r in caplog.records if "[BOLT] send" in r.getMessage()]

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="provisa.bolt.session"):
            session.send_success({})
        debug_records = [r for r in caplog.records if "[BOLT] send" in r.getMessage()]
        assert debug_records
        assert debug_records[0].levelno == logging.DEBUG


class TestPullBatchedMetering:
    def test_pull_reports_metering_once_per_batch(self, monkeypatch):
        rows = [[1], [2], [3]]
        session = _session_with_rows(rows)

        calls: list[tuple[str | None, int]] = []
        monkeypatch.setattr(
            "provisa.core.egress.report", lambda org_id, n: calls.append((org_id, n))
        )

        session.handle_pull([{"n": -1}])

        # One report() call for the 3-row batch (not one per row), plus one for the trailing
        # PULL SUCCESS message that handle_pull sends afterward.
        assert len(calls) == 2
        batch_org_id, batch_bytes = calls[0]
        assert batch_org_id == "org-1"
        assert batch_bytes > 0

    def test_pull_does_not_warn_per_row(self, caplog):
        rows = [[1], [2], [3]]
        session = _session_with_rows(rows)

        with caplog.at_level(logging.WARNING, logger="uvicorn.error"):
            session.handle_pull([{"n": -1}])

        per_row_warnings = [
            r for r in caplog.records if "[BOLT] PULL sending record" in r.getMessage()
        ]
        assert not per_row_warnings
