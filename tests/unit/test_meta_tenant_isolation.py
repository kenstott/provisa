# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for set_tenant_context DB helper."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from provisa.core.db import set_tenant_context


@pytest.mark.asyncio
async def test_set_tenant_context_with_id_executes_set_local():
    conn = AsyncMock()
    await set_tenant_context(conn, "a1b2c3d4-0000-0000-0000-000000000001")
    conn.execute.assert_awaited_once_with(
        "SET LOCAL app.tenant_id = 'a1b2c3d4-0000-0000-0000-000000000001'"
    )


@pytest.mark.asyncio
async def test_set_tenant_context_with_none_executes_nothing():
    conn = AsyncMock()
    await set_tenant_context(conn, None)
    conn.execute.assert_not_awaited()
