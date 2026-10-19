# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replica build's failure names its cause (REQ-2031).

A connect to a source's connector server that times out raised asyncio's bare TimeoutError, whose
message is empty, and the build's log line read "failed: ;". The connect now names the server it
waited on, and the log names a failure's type whatever its message."""

# Requirements: REQ-2031

from __future__ import annotations

import asyncio

import pytest

from provisa.federation import pgwire_replica, replica_runner

pytestmark = pytest.mark.unit


def test_a_connect_that_times_out_names_the_connector_server(monkeypatch):
    async def _times_out(**_kwargs):
        raise TimeoutError

    monkeypatch.setattr("asyncpg.connect", _times_out)
    with pytest.raises(TimeoutError, match="connector server at 127.0.0.1:41000"):
        asyncio.run(pgwire_replica._pg_connect("127.0.0.1", 41000, "pw"))


@pytest.mark.parametrize(
    ("exc", "named"),
    [
        (TimeoutError(), "TimeoutError"),
        (ValueError("bad row"), "ValueError: bad row"),
    ],
)
def test_the_log_names_a_failure_s_type_and_its_message(exc, named):
    assert replica_runner._named(exc) == named
