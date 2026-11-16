# Copyright (c) 2026 Kenneth Stott
# Canary: 351000ea-29aa-496b-b7b7-9de86532bdbf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The embedded ClickHouse the polyfill tests run their statements on.

chdb's in-memory engine is one per process: every session on it sees the same databases and
tables, and they last until the last open session closes. A module's runtime is therefore closed
when its module ends. Left to the garbage collector it stays open for as long as a
``pytest.raises`` traceback holds the test's frame, and a later test's tables then outlive that
test's own session."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime


@pytest.fixture(scope="module")
def runtime() -> Iterator[ClickHouseFederationRuntime]:
    embedded = ClickHouseFederationRuntime.embedded()
    yield embedded
    embedded.close()
