# Copyright (c) 2026 Kenneth Stott
# Canary: a1a34510-fc12-4e2f-9d24-2b96e79a8e71
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


def _input(type_: str = "postgresql", **overrides: Any) -> Any:
    base = dict(type=type_, id="s1", host="h", port=5432, database="d", username="u", password="p")
    base.update(overrides)
    return types.SimpleNamespace(**base)


class _RaisingPools:
    async def add(self, **kwargs):
        raise ConnectionError("could not connect")


class _RecordingPools:
    def __init__(self):
        self.added = False
        self.kwargs: dict = {}

    async def add(self, **kwargs):
        self.added = True
        self.kwargs = kwargs


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


# -- REQ-1726: a file-embedded source's path is the pool's "database" -------------


async def test_sqlite_file_path_becomes_the_pool_database_with_no_host():
    """The Sources form writes a file-embedded source's location into ``path``, never
    ``database``/``host`` — SQLAlchemy's sqlite dialect takes the file path as the URL's
    "database" segment and admits NO host component at all. A caller that dropped ``path`` on the
    floor here connected as ``sqlite://localhost`` (a bare host, no path); one that forwarded
    ``path`` as ``database`` but kept the "localhost" host default connected as
    ``sqlite://localhost/<path>`` (URL.create renders the host as an extra path segment before the
    real one) — both equally invalid, and every sqlite source failed connection validation until
    this branched on ``path`` for both fields, never having been driven through the form before."""
    pools = _RecordingPools()
    state = types.SimpleNamespace(source_pools=pools)
    inp = _input("sqlite", host="", database="", path="./demo/files/inquiries.sqlite")
    await _add_source_pool(state, inp)
    assert pools.kwargs["database"] == "./demo/files/inquiries.sqlite"
    assert pools.kwargs["host"] == ""


async def test_a_host_database_source_is_unaffected_by_the_path_field():
    """A source with no ``path`` (the ordinary host/port/database shape) must not regress —
    ``path`` is only ever set by a file-embedded source's form."""
    pools = _RecordingPools()
    state = types.SimpleNamespace(source_pools=pools)
    inp = _input("postgresql", path=None)
    await _add_source_pool(state, inp)
    assert pools.kwargs["database"] == "d"
    assert pools.kwargs["host"] == "h"


async def test_a_caller_that_never_sets_path_is_unaffected():
    """Some callers (this file's own original fixture, a config-built input) never set ``path`` at
    all — the field must be optional, not required, on ``_add_source_pool``'s input."""
    pools = _RecordingPools()
    state = types.SimpleNamespace(source_pools=pools)
    await _add_source_pool(state, _input("postgresql"))
    assert pools.kwargs["database"] == "d"
