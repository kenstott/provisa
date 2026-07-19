# Copyright (c) 2026 Kenneth Stott
# Canary: 62e8804b-f1ea-4e9e-9c3a-f3170916facd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1143: a __provisa__ virtual view has no external-source refresh policy and its persisted
source `type` is the federation engine name (e.g. "trino", not a SourceType). summarize_table_policy
must return None for it — never raise — so the admin `tables` query does not error on a view row
(which, under Apollo's default errorPolicy, would discard the whole result and blank the UI lists)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from provisa.api.admin.types import RegisteredTableType


def _view_table() -> SimpleNamespace:
    return SimpleNamespace(
        source_id="__provisa__",
        domain_id="analytics",
        schema_name="views",
        table_name="top_users",
        cache_ttl=None,
        prefer_materialized=None,
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        change_signal=None,
    )


@pytest.mark.asyncio
async def test_summarize_returns_none_for_provisa_view_without_raising():
    from provisa.api.admin._refresh_summary import summarize_table_policy

    # No engine/db is touched: the __provisa__ guard short-circuits before _resolve_engine/_load_source.
    result = await summarize_table_policy(cast("RegisteredTableType", _view_table()))
    assert result is None
