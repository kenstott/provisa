# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846: GET /admin/schema-discovery/ir-types exposes the canonical IR type vocabulary."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from provisa.api.admin.discovery_schema import router
from provisa.core.ir_types import IR_TYPES


@pytest.mark.asyncio
async def test_ir_types_endpoint_returns_sorted_vocabulary():
    app = FastAPI()
    app.include_router(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        resp = await client.get("/admin/schema-discovery/ir-types")
    assert resp.status_code == 200
    body = resp.json()
    assert body == sorted(IR_TYPES)  # exactly the IR vocabulary, sorted for a stable dropdown
    # canonical IR names (not engine-physical spellings) — e.g. text, not VARCHAR
    assert "text" in body and "VARCHAR" not in body
    assert "integer" in body and "bigint" in body
