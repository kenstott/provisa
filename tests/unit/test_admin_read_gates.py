# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8a1f63-9c05-4e27-b3a8-61e0f7d2c954
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin read queries that reach a source, an LLM or governance rules carry a capability gate."""

# Requirements: REQ-1337, REQ-1349, REQ-434

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api.admin.schema_query import Query
from tests.unit.gate_identity import grant


class _Conn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return None


_CALLS = [
    ("table_registration", "generate_table_description", {"table_id": "1"}),
    ("table_registration", "generate_column_description", {"table_id": "1", "column_name": "c"}),
    ("table_registration", "available_schemas", {"source_id": "s"}),
    ("table_registration", "available_tables", {"source_id": "s", "schema_name": "x"}),
    ("table_registration", "available_functions", {"source_id": "s", "schema_name": "x"}),
    ("table_registration", "calendars", {}),
    ("table_registration", "dq_check_catalog", {"checker": "soda", "dataset": "d"}),
    ("source_registration", "kaggle_token_valid", {"token": "t"}),
    ("source_registration", "crawl_source", {"path": "/x"}),
    ("access_config", "rls_rules", {}),
]


@pytest.mark.parametrize("capability,name,kwargs", _CALLS)
async def test_a_caller_without_the_capability_is_refused(monkeypatch, capability, name, kwargs):
    info, _ = grant(monkeypatch, "observability")
    with pytest.raises(PermissionError, match=capability):
        await getattr(Query(), name)(info, **kwargs)


async def test_creation_requests_lists_only_those_the_caller_may_act_on(monkeypatch):
    rows = [
        {
            "id": 1,
            "request_type": "webhook",
            "capability": "table_registration",
            "requested_by": "a",
            "status": "pending",
            "payload": {},
        },
        {
            "id": 2,
            "request_type": "source",
            "capability": "source_registration",
            "requested_by": "a",
            "status": "pending",
            "payload": {},
        },
    ]
    info, _ = grant(monkeypatch, "table_registration")
    with (
        patch("provisa.api.admin.schema_query._get_pool") as pool,
        patch(
            "provisa.core.repositories.creation_request.list_pending",
            new=AsyncMock(return_value=rows),
        ),
    ):
        pool.return_value = AsyncMock(acquire=MagicMock(return_value=_Conn()))
        seen = await Query().creation_requests(info)
    assert [r.id for r in seen] == [1]
