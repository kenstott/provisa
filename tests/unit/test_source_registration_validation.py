# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-012: source registration validates the direct connection (no silent swallow)."""

from __future__ import annotations

import types
from typing import Any

import pytest

from provisa.api.admin.schema_common import _add_source_pool

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


def _input(type_: str = "postgresql") -> Any:
    return types.SimpleNamespace(
        type=type_, id="s1", host="h", port=5432, database="d", username="u", password="p"
    )


class _RaisingPools:
    async def add(self, **kwargs):
        raise ConnectionError("could not connect")


class _RecordingPools:
    def __init__(self):
        self.added = False

    async def add(self, **kwargs):
        self.added = True


async def test_connection_failure_propagates():
    # A failed direct connection must surface, not be swallowed with a warning.
    state = types.SimpleNamespace(source_pools=_RaisingPools())
    with pytest.raises(ConnectionError):
        await _add_source_pool(state, _input("postgresql"))


async def test_no_driver_type_skips_pool_without_error():
    # Trino-routed sources (no direct driver) register without a direct pool.
    pools = _RecordingPools()
    state = types.SimpleNamespace(source_pools=pools)
    await _add_source_pool(state, _input("no-such-driver-type"))
    assert pools.added is False
