# Copyright (c) 2026 Kenneth Stott
# Canary: 7bb220c4-8c9b-4eae-99d2-49e5d6a6370c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The request-deadline module's clock, moved only by the test.

A test that asserts on a short request budget (well under a second) on the real clock fails on a
loaded machine for a reason that has nothing to do with what it tests: the budget runs out while
the test is still setting up, or the watchdog's expiry raise lands before the code under test
reaches its own bounded wait. ``deadline_clock`` replaces the clock ``provisa.core.request_deadline``
reads (it reads only ``time.monotonic``):

- a test that needs the budget to pass moves it with :meth:`DeadlineClock.advance`;
- a test whose code waits for the budget in real time (a queue put, a pool acquire, a permit poll
  bounded by ``remaining()``) leaves it where it is: the code's own wait still ends on the real
  clock, and the watchdog, which reads this clock, never sees the budget pass and never races it.
"""

from __future__ import annotations

import time
import types

import pytest


class DeadlineClock:
    def __init__(self) -> None:
        self.now = time.monotonic()

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def deadline_clock(monkeypatch) -> DeadlineClock:
    from provisa.core import request_deadline

    clock = DeadlineClock()
    monkeypatch.setattr(request_deadline, "time", types.SimpleNamespace(monotonic=clock.monotonic))
    return clock
