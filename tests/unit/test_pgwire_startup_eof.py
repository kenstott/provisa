# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2f41-0c7d-4a93-b6e1-9d2a47c3f081
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pgwire connection the client closes before sending a startup packet ends quietly.

A TCP health probe (or a client that declines the server's SSL answer) opens a connection and
closes it without sending anything. PostgreSQL ends such a connection without a complaint; the
pgwire server does the same instead of failing to unpack the missing length word. A packet cut
short after it started is still refused."""

from __future__ import annotations

import io
from typing import Any, cast

import pytest
from buenavista.postgres import BVBuffer

from provisa.pgwire.server import ProvisaHandler


def _handler(wire: bytes) -> Any:
    handler = cast(Any, object.__new__(ProvisaHandler))
    handler.r = BVBuffer(io.BytesIO(wire))
    return handler


def test_a_connection_closed_before_any_startup_byte_ends_with_no_session():
    assert _handler(b"").handle_startup(conn=None) is None


def test_a_startup_packet_cut_short_is_refused_by_name():
    with pytest.raises(ConnectionError, match="incomplete startup packet"):
        _handler(b"\x00\x00").handle_startup(conn=None)
