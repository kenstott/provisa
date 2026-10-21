# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7a9c51-6b28-4d04-a1f3-9c5d0e72b846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2122: the Catalogs panel's surface.

Each catalog is connected once, with the domains that work with it. What is asserted is what a
wrong surface gets wrong silently: returning a stored credential, erasing one the admin did not
retype, keeping a catalog for a domain the org does not have, and dropping a catalog the export
still publishes to.
"""

from __future__ import annotations

import types

import pytest

from provisa.api.errors import ApiError

ORG_ID = "acme"


def _request(body: dict | None = None):
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=ORG_ID
        )
    )

    async def _json():
        return body or {}

    request.json = _json
    return request


@pytest.fixture
def surface(monkeypatch):
    """The handlers against recorded org settings and the org's domains."""
    import provisa.api.admin.capabilities as capmod
    import provisa.api.admin.catalogs_router as mod
    import provisa.core.org_settings as org_settings_mod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: {"org_settings"})
    settings: dict = {"catalogs": [], "metadata_export": {}}
    written: dict = {}

    async def _resolve(_db):
        return {key: value for key, value in settings.items()}

    async def _write(_db, updates, *, updated_by):
        written.clear()
        written.update(updates)
        written["_updated_by"] = updated_by
        return list(updates)

    async def _domains():
        return {"sales", "finance"}

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(org_settings_mod, "write_org_overrides", _write)
    monkeypatch.setattr(mod, "_domain_ids", _domains)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(model_db=object()), raising=False
    )
    return types.SimpleNamespace(settings=settings, written=written)


async def _put(catalogs: list[dict]):
    from provisa.api.admin.catalogs_router import set_catalogs

    return await set_catalogs(_request({"catalogs": catalogs}))


@pytest.mark.asyncio
async def test_the_read_reports_credentials_as_set_and_never_returns_them(surface):
    from provisa.api.admin.catalogs_router import get_catalogs

    surface.settings["catalogs"] = [
        {
            "id": "mz",
            "provider": "openlineage",
            "endpoint": "http://mz",
            "api_key": "s3cr3t",
            "domains": ["sales"],
        }
    ]
    body = await get_catalogs(_request())

    (catalog,) = body["catalogs"]
    assert (catalog["api_key_set"], catalog["token_set"]) == (True, False)
    assert "api_key" not in catalog
    assert "s3cr3t" not in str(body)
    assert (catalog["endpoint"], catalog["domains"]) == ("http://mz", ["sales"])
    assert {"openlineage", "openmetadata"} <= set(body["providers"])


@pytest.mark.asyncio
async def test_editing_the_endpoint_does_not_erase_the_stored_credential(surface):
    surface.settings["catalogs"] = [
        {
            "id": "mz",
            "provider": "openlineage",
            "endpoint": "http://old",
            "api_key": "keep-me",
            "domains": ["sales"],
        }
    ]
    await _put(
        [{"id": "mz", "provider": "openlineage", "endpoint": "http://new", "domains": ["sales"]}]
    )

    (kept,) = surface.written["catalogs"]
    assert (kept["endpoint"], kept["api_key"]) == ("http://new", "keep-me")
    assert surface.written["_updated_by"] == "alice"


@pytest.mark.asyncio
async def test_two_catalogs_of_one_provider_keep_their_credentials_apart(surface):
    surface.settings["catalogs"] = [
        {
            "id": "om-a",
            "provider": "openmetadata",
            "endpoint": "http://a",
            "token": "ta",
            "domains": ["sales"],
        },
        {
            "id": "om-b",
            "provider": "openmetadata",
            "endpoint": "http://b",
            "token": "tb",
            "domains": ["finance"],
        },
    ]
    await _put(
        [
            {
                "id": "om-b",
                "provider": "openmetadata",
                "endpoint": "http://b2",
                "domains": ["finance"],
            },
            {
                "id": "om-a",
                "provider": "openmetadata",
                "endpoint": "http://a",
                "domains": ["sales"],
            },
        ]
    )

    kept = {c["id"]: c for c in surface.written["catalogs"]}
    assert (kept["om-a"]["token"], kept["om-b"]["token"]) == ("ta", "tb")
    assert kept["om-b"]["endpoint"] == "http://b2"


@pytest.mark.asyncio
async def test_an_empty_credential_clears_it(surface):
    surface.settings["catalogs"] = [
        {
            "id": "mz",
            "provider": "openlineage",
            "endpoint": "http://mz",
            "api_key": "old",
            "domains": ["sales"],
        }
    ]
    await _put(
        [
            {
                "id": "mz",
                "provider": "openlineage",
                "endpoint": "http://mz",
                "api_key": "",
                "domains": ["sales"],
            }
        ]
    )

    assert surface.written["catalogs"][0]["api_key"] == ""


@pytest.mark.asyncio
async def test_a_key_outside_the_catalog_settings_is_not_written(surface):
    await _put(
        [
            {
                "id": "mz",
                "provider": "openlineage",
                "endpoint": "http://mz",
                "domains": ["sales"],
                "tier": "x",
            }
        ]
    )

    assert "tier" not in surface.written["catalogs"][0]


@pytest.mark.asyncio
async def test_a_catalog_with_no_domain_is_refused(surface):
    with pytest.raises(ApiError) as exc:
        await _put(
            [{"id": "mz", "provider": "openlineage", "endpoint": "http://mz", "domains": []}]
        )

    assert exc.value.status_code == 400
    assert "assigned to no domain" in str(exc.value.detail)
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_catalog_for_a_domain_the_org_does_not_have_is_refused(surface):
    with pytest.raises(ApiError) as exc:
        await _put(
            [
                {
                    "id": "mz",
                    "provider": "openlineage",
                    "endpoint": "http://mz",
                    "domains": ["sales", "hr"],
                }
            ]
        )

    assert (exc.value.status_code, exc.value.code) == (400, "catalogs.unknown_domain")
    assert exc.value.params["domains"] == "hr"
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_catalog_the_export_publishes_to_cannot_be_removed(surface):
    surface.settings["catalogs"] = [
        {"id": "mz", "provider": "openlineage", "endpoint": "http://mz", "domains": ["sales"]}
    ]
    surface.settings["metadata_export"] = {"enabled": True, "targets": [{"catalog": "mz"}]}

    with pytest.raises(ApiError) as exc:
        await _put([])

    assert (exc.value.status_code, exc.value.code) == (400, "catalogs.exported")
    assert surface.written == {}


@pytest.mark.asyncio
async def test_a_catalog_with_no_endpoint_is_refused_naming_the_setting(surface):
    with pytest.raises(ApiError) as exc:
        await _put([{"id": "om", "provider": "openmetadata", "domains": ["sales"]}])

    assert "catalog 'om' has no endpoint" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_the_connection_test_reports_the_providers_refusal(surface, monkeypatch):
    """The test is the provider's own health check, the one the export runs."""
    import provisa.api.admin.catalogs_router as mod

    surface.settings["catalogs"] = [
        {"id": "mz", "provider": "openlineage", "endpoint": "http://mz", "domains": ["sales"]}
    ]
    built = []

    class _Failing:
        async def health(self):
            raise ConnectionError("Name or service not known")

    def _export(target):
        built.append(target)
        return _Failing()

    monkeypatch.setattr(mod, "metadata_export", _export)
    body = await mod.check_catalog(_request(), "mz")

    assert body["ok"] is False
    assert "ConnectionError: Name or service not known" in body["error"]
    assert (built[0].id, built[0].endpoint) == ("mz", "http://mz")


@pytest.mark.asyncio
async def test_the_connection_test_carries_the_exports_settings_for_the_catalog(
    surface, monkeypatch
):
    """A DataZone catalog the export publishes to is tested with the export's owning project."""
    import provisa.api.admin.catalogs_router as mod

    surface.settings["catalogs"] = [
        {
            "id": "dz",
            "provider": "datazone",
            "aws_region": "us-east-1",
            "datazone_domain_id": "dzd_1",
            "domains": ["sales"],
        }
    ]
    surface.settings["metadata_export"] = {
        "targets": [{"catalog": "dz", "datazone_project_id": "prj_1"}]
    }
    built = []

    class _Healthy:
        async def health(self):
            return None

    monkeypatch.setattr(mod, "metadata_export", lambda target: built.append(target) or _Healthy())
    body = await mod.check_catalog(_request(), "dz")

    assert body["ok"] is True
    assert built[0].datazone_project_id == "prj_1"


@pytest.mark.asyncio
async def test_testing_a_catalog_the_org_does_not_have_is_a_404(surface):
    from provisa.api.admin.catalogs_router import check_catalog

    with pytest.raises(ApiError) as exc:
        await check_catalog(_request(), "nope")
    assert exc.value.status_code == 404
