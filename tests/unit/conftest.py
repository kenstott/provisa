# Copyright (c) 2026 Kenneth Stott
# Canary: 5f5ea307-3022-45ba-ba17-362aaf04e7e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fixtures for tests/unit only (tests/conftest.py holds the suite-wide ones)."""

# The request-deadline clock a test moves itself (tests/deadline_clock.py).
from tests.deadline_clock import deadline_clock  # noqa: F401


import pytest  # noqa: E402


@pytest.fixture(scope="session")
def embedded_postgres():
    """A Postgres of this session's own (tests/embedded_pg.py): the pgserver wheel's server on a
    leased 127.0.0.1 port. A unit test that needs Postgres uses this, never whatever answers on the
    machine's 5432 — other jobs start and stop that one, and a parallel worker shares it."""
    from tests.embedded_pg import start_local_postgres

    pg = start_local_postgres()
    yield pg
    pg.stop()
