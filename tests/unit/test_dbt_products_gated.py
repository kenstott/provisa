# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9e5d17-6c2b-4f84-b0e1-7d58c2a94f06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a dbt source's public models as data products, and previewing that, are refused to a
caller without the right (REQ-2228): the preview asks source_registration, as the dbt source's other
endpoints do, and registering asks table_registration in the domain the tables land in, as
registerTable does. A refused caller reaches neither the control plane nor the mutations."""

# Requirements: REQ-2228

from __future__ import annotations

import types
from typing import Any

import pytest

import provisa.api.admin.capabilities as capmod
import provisa.api.admin.dbt_router as router
from provisa.api.admin.dbt_router import (
    DbtRegisterProducts,
    dbt_products_preview,
    register_dbt_products,
)
from provisa.api.errors import ApiError

SOURCE = "dbt-jaffle"


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="sam", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


@pytest.fixture(name="holding")
def _holding(monkeypatch: pytest.MonkeyPatch):
    """Set the rights the caller holds; anything past the gate fails the test."""

    def _hold(*rights: str) -> None:
        held = frozenset(rights)
        monkeypatch.setattr(capmod, "_resolved_capabilities", lambda _identity, _state: held)

    async def _reached(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a refused caller reached past the gate")

    monkeypatch.setattr(router, "products_preview", _reached)
    monkeypatch.setattr(router, "register_products", _reached)
    return _hold


@pytest.mark.asyncio
async def test_the_preview_is_refused_without_source_registration(holding):
    holding("table_registration")
    with pytest.raises(ApiError) as refused:
        await dbt_products_preview(_request(), SOURCE)
    assert refused.value.status_code == 403
    assert refused.value.code == "auth.missing_capability"


@pytest.mark.asyncio
async def test_registering_products_is_refused_without_table_registration(holding):
    # source_registration adds the source; it does not register its models as tables.
    holding("source_registration")
    with pytest.raises(ApiError) as refused:
        await register_dbt_products(_request(), SOURCE, DbtRegisterProducts(domain_id="sales"))
    assert refused.value.status_code == 403
    assert refused.value.code == "auth.missing_capability"
    assert "table_registration" in refused.value.detail
