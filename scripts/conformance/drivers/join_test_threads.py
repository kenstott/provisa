# Copyright (c) 2026 Kenneth Stott
# Canary: ed9ff317-b8e8-4db4-b1f9-8301741931e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pytest plugin for a driver's own suite: a test's threads end inside that test's report (REQ-1989).

pytest's threadexception plugin charges a thread's exception to whichever test is running when it
is collected, so a worker that raises just after its test ended fails the next test, a different
one each run. This joins the threads a test started, for at most ``BOUND_S``, before that collection,
and the exception lands on the test that started the worker. Nothing is caught or filtered: the
thread's exception is reported as before, on its own test.

It is a function-scoped autouse fixture, not a hook. The collection is a ``trylast`` impl of
``pytest_runtest_teardown``, and the fixture's finalizer runs inside the runner's plain impl, so
after every fixture of the test has released its threads and before the collection. A plugin
named with ``-p`` registers after pytest's own, which makes its ``trylast`` impl run after the
collection (too late), and a ``tryfirst`` impl runs before the test's fixtures are finalized. The
fixture's set-up comes after the higher-scoped fixtures', whose threads are not the test's.

Loaded with ``-p join_test_threads``, its directory on PYTHONPATH (conformance-drivers.yml).
"""

# Requirements: REQ-1989

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import pytest

#: How long one test's threads are waited for, all together.
BOUND_S = 2.0


@pytest.fixture(autouse=True)
def _join_test_threads() -> Iterator[None]:
    """Wait, within the bound, for the threads started since this test's set-up."""
    before = set(threading.enumerate())
    yield
    deadline = time.monotonic() + BOUND_S
    for thread in set(threading.enumerate()) - before:
        thread.join(max(0.0, deadline - time.monotonic()))
