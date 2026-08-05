# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2f7b81-4c56-4a03-b7e9-1f5a08c36d24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1074: the admin surface that configures and operates metadata export.

The four things worth pinning here are the ones a wrong implementation gets wrong quietly:
credentials must never travel back out on the read, an edit that omits a credential must not
erase it, an org below the REQ-1073 tier must not reach the writer, and a target that refuses
the connection must be reported with the text it refused with rather than as a bare failure.
"""

# Requirements: REQ-1068, REQ-1072, REQ-1073, REQ-1074

from __future__ import annotations

import types

import pytest

from provisa.api.errors import ApiError
from provisa.control_plane.models import Org
from provisa.control_plane.store import control_plane_store

ORG_ID = "acme"


def _request(org_id: str = ORG_ID):
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=org_id
        )
    )


def _json_body(body: dict):
    async def _json():
        return body

    return _json


@pytest.fixture
def surface(monkeypatch):
    """Drive the handlers against recorded org settings and a registered premium org.

    Returns a namespace with ``stored`` (what the org has saved — mutate it to set up a case),
    ``written`` (what a handler persisted), and ``org`` (call it to re-register at a tier).
    """
    import provisa.api.admin.capabilities as capmod
    import provisa.core.org_settings as org_settings_mod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: {"org_settings"})

    stored: dict = {}
    written: dict = {}

    async def _resolve(_db):
        return {"metadata_export": dict(stored)}

    async def _write(_db, updates, *, updated_by):
        written.clear()
        written.update(updates)
        written["_updated_by"] = updated_by
        return list(updates)

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(tenant_db=object(), config=object()),
        raising=False,
    )

    def _org(tier: str = "premium", org_id: str = ORG_ID):
        control_plane_store().register_org(
            Org(id=org_id, name=org_id, data_plane_id="dp", created_at="2026-01-01", tier=tier)
        )

    _org()
    return types.SimpleNamespace(stored=stored, written=written, org=_org)


async def _put(body: dict):
    from provisa.api.admin.metadata_export_router import set_metadata_export

    request = _request()
    request.json = _json_body(body)
    return await set_metadata_export(request)


# --- reading ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_read_reports_credentials_as_set_and_never_returns_them(surface):
    """A token readable through the tab is a token any org admin can exfiltrate."""
    from provisa.api.admin.metadata_export_router import get_metadata_export

    surface.stored.update(
        {"enabled": True, "provider": "openlineage", "endpoint": "http://mz", "api_key": "s3cr3t"}
    )
    body = await get_metadata_export(_request())

    config = body["config"]
    assert config["api_key_set"] is True
    assert config["token_set"] is False
    assert "api_key" not in config
    assert "s3cr3t" not in str(body)
    assert config["endpoint"] == "http://mz"


@pytest.mark.asyncio
async def test_the_read_carries_the_entitlement_flag_the_tab_gates_on(surface):
    from provisa.api.admin.metadata_export_router import get_metadata_export

    assert (await get_metadata_export(_request()))["entitled"] is True
    surface.org(tier="standard")
    body = await get_metadata_export(_request())
    assert body["entitled"] is False
    # The tab tells the admin what tier would open it, so the gate is explicable, not just shut.
    assert body["required_tier"] == "premium"


@pytest.mark.asyncio
async def test_the_read_lists_the_providers_the_registry_actually_has(surface):
    from provisa.api.admin.metadata_export_router import get_metadata_export

    providers = (await get_metadata_export(_request()))["providers"]
    assert {"openlineage", "openmetadata"} <= set(providers)


# --- writing ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_editing_the_endpoint_does_not_erase_the_stored_credential(surface):
    """The tab never receives the secret, so it cannot send it back.

    If an omitted credential meant "clear", every endpoint edit would silently break the
    connection — and the next publish, not the edit, would be what reported it.
    """
    surface.stored.update(
        {"enabled": True, "provider": "openlineage", "endpoint": "http://old", "api_key": "keep-me"}
    )
    await _put({"endpoint": "http://new"})

    assert surface.written["metadata_export"]["endpoint"] == "http://new"
    assert surface.written["metadata_export"]["api_key"] == "keep-me"


@pytest.mark.asyncio
async def test_an_empty_credential_clears_it(surface):
    surface.stored.update(
        {"enabled": True, "provider": "openlineage", "endpoint": "http://mz", "api_key": "old"}
    )
    await _put({"api_key": ""})

    assert surface.written["metadata_export"]["api_key"] == ""


@pytest.mark.asyncio
async def test_a_key_outside_the_export_settings_is_not_written(surface):
    # The body is an untrusted dict; only the enumerated settings may reach storage, or an admin
    # writes arbitrary config through the export door.
    await _put({"endpoint": "http://mz", "provider": "openlineage", "enabled": True, "tier": "x"})

    assert "tier" not in surface.written["metadata_export"]


@pytest.mark.asyncio
async def test_an_enabled_target_with_no_endpoint_is_refused_at_the_write(surface):
    with pytest.raises(ApiError) as exc:
        await _put({"enabled": True, "provider": "openlineage"})

    assert exc.value.status_code == 400
    assert surface.written == {}


@pytest.mark.asyncio
async def test_the_write_is_attributed_to_the_caller(surface):
    await _put({"enabled": False})

    assert surface.written["_updated_by"] == "alice"


# --- the tier gate ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", ["set_metadata_export", "check_metadata_export",
                                          "publish_metadata_export"])
async def test_every_operating_handler_refuses_an_unentitled_org(surface, handler_name):
    """REQ-1073: hiding the tab is not the gate — the endpoints are."""
    import provisa.api.admin.metadata_export_router as mod

    surface.org(tier="standard")
    surface.stored.update({"enabled": True, "provider": "openlineage", "endpoint": "http://mz"})
    request = _request()
    request.json = _json_body({"enabled": True})

    with pytest.raises(ApiError) as exc:
        await getattr(mod, handler_name)(request)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_an_org_the_control_plane_never_registered_is_refused(surface):
    request = _request(org_id="ghost")
    request.json = _json_body({"enabled": False})

    with pytest.raises(ApiError) as exc:
        await _unregistered(request)
    assert exc.value.status_code == 403


async def _unregistered(request):
    from provisa.api.admin.metadata_export_router import set_metadata_export

    return await set_metadata_export(request)


# --- health and publish ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_reports_the_refusal_text_rather_than_a_bare_failure(surface, monkeypatch):
    """A wrong URL and a rejected credential look identical without the message."""
    import provisa.api.admin.metadata_export_router as mod
    from provisa.api.admin.metadata_export_router import check_metadata_export

    surface.stored.update({"enabled": True, "provider": "openlineage", "endpoint": "http://mz"})

    class _Failing:
        async def health(self):
            raise ConnectionError("Name or service not known")

    monkeypatch.setattr(mod, "metadata_export", lambda config: _Failing())
    body = await check_metadata_export(_request())

    assert body["ok"] is False
    assert "Name or service not known" in body["error"]
    assert "ConnectionError" in body["error"]


@pytest.mark.asyncio
async def test_publish_returns_the_assets_the_target_rejected(surface, monkeypatch):
    """A partial publish is the case the tab exists to diagnose."""
    from provisa.api.admin.metadata_export_router import (
        get_metadata_export,
        publish_metadata_export,
    )
    from provisa.api.metadata_export.model import AssetRef
    from provisa.api.metadata_export.provider import AssetError, PublishResult

    surface.stored.update({"enabled": True, "provider": "openlineage", "endpoint": "http://mz"})
    result = PublishResult(
        provider_name="openlineage",
        published={"dataset": 2},
        errors=[
            AssetError(
                asset=AssetRef(kind="table", parts=("wh", "public.orders")),
                message="422 unknown field type",
            )
        ],
    )

    class _Partial:
        async def publish(self, snapshot):
            return result

    # The endpoint publishes through the REQ-1072 sync path, so the adapter and the builder are
    # stubbed where that path resolves them.
    import provisa.api.metadata_export.sync as sync_mod

    monkeypatch.setattr(sync_mod, "metadata_export", lambda config: _Partial())
    monkeypatch.setattr(sync_mod, "build_snapshot", lambda config, *, org_id, dialect: object())
    body = await publish_metadata_export(_request())

    assert body["ok"] is False
    assert body["total_published"] == 2
    assert body["errors"] == [
        {"asset": "wh.public.orders", "message": "422 unknown field type"}
    ]
    # The tab shows the last outcome without re-publishing to find out what it was.
    assert (await get_metadata_export(_request()))["last_publish"]["errors"] == body["errors"]


@pytest.mark.asyncio
async def test_publishing_a_disabled_target_is_refused_before_a_snapshot_is_built(surface):
    from provisa.api.admin.metadata_export_router import publish_metadata_export

    surface.stored.update({"enabled": False, "provider": "openlineage", "endpoint": "http://mz"})

    with pytest.raises(ApiError) as exc:
        await publish_metadata_export(_request())
    assert exc.value.status_code == 400
