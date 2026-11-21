# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d4e83-2a7c-4f96-8e05-b3c9a1d7f240
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2216: the org's request delivery setting -- email (the default), events only, or both --
read and kept under the org administrators' right, a mode outside the three refused by name."""

from __future__ import annotations

import types

import pytest
from fastapi import HTTPException

from provisa.api.errors import ApiError


def _request():
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id="acme"
        )
    )


@pytest.fixture
def surface(monkeypatch):
    import provisa.api.admin.capabilities as capmod
    import provisa.core.org_settings as org_settings_mod

    held: set[str] = {"org_settings"}
    settings: dict = {}
    written: dict = {}

    async def _resolve(_db):
        return dict(settings)

    async def _write(_db, updates, *, updated_by):
        written.update(updates)
        written["_updated_by"] = updated_by
        settings.update(updates)
        return list(updates)

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: set(held))
    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(model_db=object()), raising=False
    )
    return types.SimpleNamespace(held=held, written=written)


async def test_an_org_that_set_nothing_is_told_by_email(surface):
    from provisa.api.admin.request_delivery_router import get_request_delivery

    got = await get_request_delivery(_request())  # type: ignore[arg-type]
    assert got == {"delivery": "email", "modes": ["email", "events", "both"]}


async def test_events_only_is_kept_and_read_back(surface):
    from provisa.api.admin.request_delivery_router import (
        DeliveryBody,
        get_request_delivery,
        set_request_delivery,
    )

    assert await set_request_delivery(_request(), DeliveryBody(delivery="events")) == {  # type: ignore[arg-type]
        "delivery": "events"
    }
    assert surface.written == {"requests": {"delivery": "events"}, "_updated_by": "alice"}
    assert (await get_request_delivery(_request()))["delivery"] == "events"  # type: ignore[arg-type]


async def test_a_mode_outside_the_three_is_refused_by_name(surface):
    from provisa.api.admin.request_delivery_router import DeliveryBody, set_request_delivery

    with pytest.raises(ApiError) as caught:
        await set_request_delivery(_request(), DeliveryBody(delivery="pager"))  # type: ignore[arg-type]
    assert caught.value.status_code == 400
    assert caught.value.code == "requests.delivery_unknown"
    assert surface.written == {}


async def test_the_setting_is_the_org_administrators(surface):
    from provisa.api.admin.request_delivery_router import DeliveryBody, set_request_delivery

    surface.held.clear()
    with pytest.raises((ApiError, HTTPException)) as caught:
        await set_request_delivery(_request(), DeliveryBody(delivery="both"))  # type: ignore[arg-type]
    assert caught.value.status_code == 403
    assert surface.written == {}
