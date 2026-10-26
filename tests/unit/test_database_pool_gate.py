# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7c3a58-9e24-4b06-8d91-6a2e0c4b7f35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1900, REQ-1905: a control-plane checkout that finds no free connection says so.

The gate in front of the shared engine makes an extra borrower wait for a slot. When none frees
in time it raises ``PoolBusy`` -- distinct from a database that cannot be reached -- and a caller
that must answer fast (the health probe) bounds the wait with ``checkout_wait``.
"""

# Requirements: REQ-1900, REQ-1905

from __future__ import annotations

import time

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from provisa.core import database


def _engine():
    return create_engine("sqlite://", poolclass=QueuePool, pool_size=1, max_overflow=0)


def test_a_full_gate_refuses_as_busy_within_the_checkout_wait() -> None:
    engine = _engine()
    with database.bounded_connection(engine):
        started = time.monotonic()
        with database.checkout_wait(0.2):
            with pytest.raises(database.PoolBusy):
                with database.bounded_connection(engine):
                    pass
        assert time.monotonic() - started < 2
    assert issubclass(database.PoolBusy, TimeoutError)


def test_a_free_slot_is_taken_whatever_the_wait() -> None:
    engine = _engine()
    with database.checkout_wait(0.2):
        with database.bounded_connection(engine) as conn:
            assert conn is not None
