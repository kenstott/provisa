# Copyright (c) 2026 Kenneth Stott
# Canary: 091e2729-bb5f-405c-b280-088cee2b42db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A CancelRequest ends the named connection's running statement (57014) and leaves the
connection open (pgspec TestLifecycle/Cancel)."""

from __future__ import annotations

from types import SimpleNamespace

from provisa.core import request_deadline
from provisa.pgwire.server import ProvisaHandler, ProvisaSession


def _handler(ctxts):
    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.server = SimpleNamespace(ctxts=ctxts)
    return handler


def _ctx(secret, deadline):
    session = ProvisaSession.__new__(ProvisaSession)
    session.statement_deadline = deadline
    return SimpleNamespace(secret_key=secret, session=session, process_id=7)


def test_cancel_expires_the_running_statement_and_keeps_the_connection():
    deadline = request_deadline.open_request("pgwire")
    try:
        ctxts = {7: _ctx(42, deadline)}
        _handler(ctxts)._cancel(7, 42)
        assert deadline.fired and deadline.ended_early
        assert "canceling statement due to user request" in str(deadline.expired_error())
        assert 7 in ctxts
    finally:
        deadline.stop()


def test_a_wrong_secret_is_ignored():
    deadline = request_deadline.open_request("pgwire")
    try:
        _handler({7: _ctx(42, deadline)})._cancel(7, 41)
        assert not deadline.fired
    finally:
        deadline.stop()


def test_a_connection_running_nothing_is_left_alone():
    _handler({7: _ctx(42, None)})._cancel(7, 42)
