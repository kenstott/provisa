# Copyright (c) 2026 Kenneth Stott
# Canary: b52d6b40-fde4-4efd-bb06-65895726fa0c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A pytest session killed before its own teardown still tears its Docker stack down.

pytest_sessionfinish runs on a clean exit and on KeyboardInterrupt, but not when the process is
SIGTERMed — which is how ``timeout`` and most supervisors end a run. install_abnormal_exit_teardown
registers the stack teardown on SIGTERM and atexit, so a killed run cannot leave a stack behind
for a later run's memory to starve against."""

from __future__ import annotations

import signal

import pytest

import tests.itest_stack as itest_stack
from tests.itest_stack import install_abnormal_exit_teardown


@pytest.fixture(autouse=True)
def _reset_install_guard(monkeypatch):
    # Each test installs fresh handlers without touching the real process signal disposition.
    monkeypatch.setattr(itest_stack, "_abnormal_teardown_installed", False, raising=False)
    installed: dict = {}
    monkeypatch.setattr(signal, "signal", lambda s, h: installed.__setitem__(s, h))
    yield installed


def test_sigterm_runs_the_teardown_then_chains_the_previous_handler(
    monkeypatch, _reset_install_guard
):
    calls: list[str] = []
    seen: list[tuple] = []
    monkeypatch.setattr(
        signal, "getsignal", lambda s: lambda signum, frame: seen.append((signum, frame))
    )

    install_abnormal_exit_teardown(lambda: calls.append("down"))
    _reset_install_guard[signal.SIGTERM](signal.SIGTERM, None)

    assert calls == ["down"]  # the stack was torn down
    assert seen == [(signal.SIGTERM, None)]  # the previous handler still ran


def test_teardown_runs_at_most_once_across_signal_and_atexit(monkeypatch, _reset_install_guard):
    calls: list[str] = []
    monkeypatch.setattr(signal, "getsignal", lambda s: signal.SIG_DFL)
    monkeypatch.setattr(itest_stack.os, "kill", lambda pid, sig: calls.append("reraise"))

    run = install_abnormal_exit_teardown(lambda: calls.append("down"))
    run()  # the atexit runner
    _reset_install_guard[signal.SIGTERM](signal.SIGTERM, None)

    assert calls.count("down") == 1


def test_only_the_first_install_registers_handlers(monkeypatch, _reset_install_guard):
    monkeypatch.setattr(signal, "getsignal", lambda s: signal.SIG_DFL)
    install_abnormal_exit_teardown(lambda: None)
    _reset_install_guard.clear()
    install_abnormal_exit_teardown(lambda: None)  # a second stack (e2e) in the same session
    assert signal.SIGTERM not in _reset_install_guard  # no second handler installed
