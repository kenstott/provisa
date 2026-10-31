# Copyright (c) 2026 Kenneth Stott
# Canary: c31d9469-7a2a-4be0-9357-7e3692fb82d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt server's log carries no statement and no credential at WARNING, and no credential at
any level: the per-message lines are DEBUG, and HELLO and LOGON are logged without their fields."""

# Requirements: REQ-1885

from __future__ import annotations

import asyncio
import logging

import pytest

from provisa.bolt import messages as msg
from provisa.bolt.server import BoltMessage, _dispatch, _log_received

SECRET = "a-session-token"  # noqa: S105


@pytest.mark.parametrize("tag", [msg.HELLO, msg.LOGON])
def test_a_credential_is_not_logged(tag: int, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        _log_received(BoltMessage(tag=tag, fields=[{"scheme": "bearer", "credentials": SECRET}]))
    assert SECRET not in caplog.text
    assert f"0x{tag:02X}" in caplog.text


def test_a_statement_is_logged_at_debug(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG):
        _log_received(BoltMessage(tag=msg.RUN, fields=["RETURN 1", {}, {}]))
    assert "RETURN 1" in caplog.text


def test_dispatching_a_message_says_nothing_at_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        asyncio.run(_dispatch(None, BoltMessage(tag=msg.GOODBYE, fields=[])))  # type: ignore[arg-type]
    assert caplog.records == []
