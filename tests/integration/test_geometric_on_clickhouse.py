# Copyright (c) 2026 Kenneth Stott
# Canary: e35a71c9-08b4-4d6e-8f23-b16d4c09a7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms over columns on ClickHouse (REQ-2171). conformance-functions measures every literal statement there."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import pytest

from tests.integration.every_case import failures_of_every_case
from tests.integration.geometric_engines import clickhouse_client, on_columns
from tests.unit.pgwire.geometric_replay import BY_FUNCTION

pytestmark = [pytest.mark.integration]


@pytest.mark.requires_clickhouse
def test_geometric_columns_on_clickhouse():
    client = clickhouse_client()
    rows = lambda sql: [r[0] for r in client.query(sql).result_rows]  # noqa: E731
    assert (
        failures_of_every_case(sorted(BY_FUNCTION), lambda f: on_columns(f, "clickhouse", rows))
        == {}
    )
