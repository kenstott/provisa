# Copyright (c) 2026 Kenneth Stott
# Canary: 5a9e1c64-7f2b-4d38-b6e0-3c8d2f7a9b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shutdown ends in-flight requests (REQ-1882, REQ-1905).

Every request runs on its own thread and its driver calls block that thread. A worker told to
stop (SIGTERM, SIGINT) waited for those requests, and a request inside a long run of statements
never looked up — the worker ignored the signal until the request finished on its own. On
shutdown every live request deadline is expired: the in-flight statement is cancelled through its
driver and the next one is refused, so the request fails with a clear error and the worker exits."""

# Requirements: REQ-1882, REQ-1905

from __future__ import annotations

import signal
import threading

import pytest

from provisa.core import request_deadline


def test_expiring_live_deadlines_cancels_the_statement_in_flight_and_refuses_the_next():
    cancelled: list[str] = []
    entered, release = threading.Event(), threading.Event()
    failure: list[BaseException] = []

    def _request() -> None:
        try:
            with request_deadline.within(300.0):
                with request_deadline.cancel_on_deadline(lambda: cancelled.append("a")):
                    entered.set()
                    release.wait(10)
                # the next statement of the same request
                with request_deadline.cancel_on_deadline(lambda: cancelled.append("b")):
                    raise AssertionError("a statement started after shutdown")
        except BaseException as exc:
            failure.append(exc)

    thread = threading.Thread(target=_request)
    thread.start()
    assert entered.wait(5)
    assert request_deadline.expire_all("the server is shutting down") == 1
    assert cancelled == ["a"], "the in-flight statement was not cancelled"
    release.set()
    thread.join(5)
    assert len(failure) == 1 and isinstance(failure[0], TimeoutError), failure
    assert "shutting down" in str(failure[0])
    assert cancelled == ["a"]


def test_a_finished_request_is_not_touched():
    with request_deadline.within(300.0):
        pass
    assert request_deadline.expire_all("the server is shutting down") == 0


def test_the_stop_signal_expires_live_deadlines_then_runs_the_servers_own_handler(monkeypatch):
    """The handler is chained in front of the one already installed (uvicorn's), which still runs."""
    calls: list[object] = []
    installed: dict[int, object] = {}
    previous = {
        signal.SIGTERM: lambda s, f: calls.append(("server", s)),
        signal.SIGINT: signal.SIG_DFL,
    }
    monkeypatch.setattr(signal, "getsignal", lambda sig: previous[sig])
    monkeypatch.setattr(signal, "signal", lambda sig, handler: installed.__setitem__(sig, handler))
    monkeypatch.setattr(
        request_deadline, "expire_all", lambda reason: calls.append(("expired", reason)) or 0
    )
    with request_deadline.expire_on_stop_signals():
        assert set(installed) == {signal.SIGTERM, signal.SIGINT}
        installed[signal.SIGTERM](signal.SIGTERM, None)
        assert calls[0][0] == "expired" and "shutting down" in calls[0][1]
        assert calls[1] == ("server", signal.SIGTERM)
    # restored on the way out
    assert installed[signal.SIGTERM] is previous[signal.SIGTERM]
    assert installed[signal.SIGINT] is signal.SIG_DFL


def test_signal_chaining_is_skipped_off_the_main_thread():
    """Signal handlers can only be installed from the main thread; a lifespan run elsewhere (an
    in-process test client) installs nothing and raises nothing."""
    outcome: list[object] = []

    def _run() -> None:
        try:
            with request_deadline.expire_on_stop_signals():
                outcome.append(signal.getsignal(signal.SIGTERM))
        except BaseException as exc:
            outcome.append(exc)

    before = signal.getsignal(signal.SIGTERM)
    thread = threading.Thread(target=_run)
    thread.start()
    thread.join(5)
    assert outcome == [before]


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
