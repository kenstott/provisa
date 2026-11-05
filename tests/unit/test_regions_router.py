# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0e3b57-1c8a-4f29-9e64-b2a7c5d19f30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regions edited in the UI (REQ-1922, amended 2026-11-05): the platform's regions, written to
the config file by a platform administrator and applied on restart; the org's stores and regions,
saved whole by its administrator, validated as a configuration is and refused naming the field."""

# Requirements: REQ-1922

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from provisa.api.admin import regions_router as rr
from provisa.api.errors import ApiError
from provisa.core.regions import PlatformConfig, PlatformRegion

_PLATFORM = PlatformConfig(
    regions=[
        PlatformRegion(id="eu", address="https://eu.example.com"),
        PlatformRegion(id="us", address="https://us.example.com"),
    ]
)


def _stores(rid: str) -> list[dict]:
    return [
        {"id": f"{rid}-pg", "url": f"postgresql://app:s3cret@{rid}.db:5432/provisa"},
        {"id": f"{rid}-redis", "url": "${env:REDIS_URL}"},
        {"id": f"{rid}-trino", "url": f"trino://{rid}:8080", "kind": "trino-byo"},
    ]


def _region(rid: str, **over) -> dict:
    return {
        "id": rid,
        "engine": f"{rid}-trino",
        "replicas": f"{rid}-pg",
        "views": f"{rid}-pg",
        "cache": f"{rid}-redis",
        "state": f"{rid}-pg",
        "record": f"{rid}-pg",
        **over,
    }


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organization (REQ-2006), as the API layer says it at import."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture
async def model(control_plane, monkeypatch):
    """An org's model store on a node of a platform declaring eu and us, the caller holding
    org_settings."""
    from sqlalchemy import insert

    from provisa.core import process_region
    from provisa.core.schema_org import domains

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
    process_region.bind_launch(_PLATFORM.model_dump(), requested="eu")
    monkeypatch.setattr(rr, "_model_db", lambda: db)
    monkeypatch.setattr(rr, "_platform", lambda: _PLATFORM)
    monkeypatch.setattr(rr, "require_org_settings", lambda request: None)
    yield db
    process_region.bind_launch({}, requested=None)


async def _save(stores: list[dict], regions: list[dict]) -> dict:
    return await rr.set_org_regions(MagicMock(), rr.OrgRegionsBody(stores=stores, regions=regions))


async def _refused(stores: list[dict], regions: list[dict]) -> ApiError:
    with pytest.raises(ApiError) as refused:
        await _save(stores, regions)
    assert refused.value.status_code == 422
    return refused.value


async def test_an_org_admin_lists_and_adds_stores_and_regions(model):
    empty = await rr.get_org_regions(MagicMock())
    assert empty["stores"] == [] and empty["regions"] == []
    assert empty["platform_regions"] == [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
    assert "trino-byo" in empty["engine_kinds"]

    await _save(_stores("eu"), [_region("eu")])
    listed = await rr.get_org_regions(MagicMock())
    assert listed["regions"] == [_region("eu")]
    assert {s["id"] for s in listed["stores"]} == {"eu-pg", "eu-redis", "eu-trino"}


async def test_a_store_url_is_kept_as_entered_and_its_password_never_comes_back(model):
    await _save(_stores("eu"), [_region("eu")])
    shown = {s["id"]: s for s in (await rr.get_org_regions(MagicMock()))["stores"]}
    # A reference stays the reference; nothing resolves it.
    assert shown["eu-redis"]["url"] == "${env:REDIS_URL}"
    assert "s3cret" not in shown["eu-pg"]["url"]
    assert shown["eu-pg"]["url"] == "postgresql://app@eu.db:5432/provisa"

    # Sending the page back unchanged keeps the password the form never saw.
    await _save(list(shown.values()), [_region("eu")])
    from provisa.core.repositories import region as region_repo

    async with model.acquire() as conn:
        held = {s.id: s.url for s in await region_repo.list_stores(conn)}
    assert held["eu-pg"] == "postgresql://app:s3cret@eu.db:5432/provisa"
    assert held["eu-redis"] == "${env:REDIS_URL}"


async def test_an_org_admin_edits_and_removes_regions_and_stores(model):
    await _save(_stores("eu") + _stores("us"), [_region("eu"), _region("us")])

    # Edit: eu's cache moves to us's Redis store.
    await _save(_stores("eu") + _stores("us"), [_region("eu", cache="us-redis"), _region("us")])
    listed = await rr.get_org_regions(MagicMock())
    assert listed["regions"][0]["cache"] == "us-redis"

    # Remove: the org stops selecting us, and its stores go with it.
    saved = await _save(_stores("eu"), [_region("eu")])
    assert [r["id"] for r in saved["regions"]] == ["eu"]
    assert {s["id"] for s in saved["stores"]} == {"eu-pg", "eu-redis", "eu-trino"}


async def test_a_region_naming_an_unknown_store_is_refused_naming_the_field(model):
    refused = await _refused(_stores("eu"), [_region("eu", engine="nowhere")])
    assert refused.detail == "region 'eu' engine store 'nowhere' is not declared in stores"
    assert refused.params["field"] == "regions.eu.engine"
    assert (await rr.get_org_regions(MagicMock()))["regions"] == []  # nothing was written


async def test_a_region_the_platform_does_not_declare_is_refused_naming_the_field(model):
    refused = await _refused(_stores("eu"), [_region("eu"), {**_region("eu"), "id": "mars"}])
    assert refused.detail == "region 'mars' is not one of the platform's (eu, us)"
    assert refused.params["field"] == "regions.mars.id"


async def test_a_store_a_region_names_cannot_be_removed(model):
    stores = [s for s in _stores("eu") if s["id"] != "eu-redis"]
    refused = await _refused(stores, [_region("eu")])
    assert refused.params["field"] == "regions.eu.cache"
    assert "eu-redis" in refused.detail


async def test_an_engine_store_without_an_engine_kind_is_refused_naming_the_store(model):
    stores = [{**s, "kind": None} if s["id"] == "eu-trino" else s for s in _stores("eu")]
    refused = await _refused(stores, [_region("eu")])
    assert refused.params["field"] == "stores.eu-trino.kind"


async def test_a_malformed_region_id_is_refused_naming_the_field(model):
    refused = await _refused(_stores("eu"), [{**_region("eu"), "id": "EU"}])
    assert refused.params["field"] == "regions.EU.id"


async def test_a_region_a_source_names_is_not_removed(model):
    from sqlalchemy import update

    from provisa.core.models import Source, SourceType
    from provisa.core.repositories import source as source_repo
    from provisa.core.schema_org import sources

    await _save(_stores("eu") + _stores("us"), [_region("eu"), _region("us")])
    async with model.acquire() as conn:
        await source_repo.upsert(
            conn,
            Source(
                id="crm",
                type=SourceType("postgresql"),
                host="h",
                database="d",
                username="u",
                password="",
            ),
        )
        await conn.execute_core(update(sources).values(region="us"))
    refused = await _refused(_stores("eu"), [_region("eu")])
    assert refused.params["field"] == "regions.us.id"
    assert "us" in refused.detail
    assert [r["id"] for r in (await rr.get_org_regions(MagicMock()))["regions"]] == ["eu", "us"]


async def test_the_org_routes_need_org_settings(model, monkeypatch):
    def _refuse(request):
        raise ApiError(403, "platform.right_required", "org_settings capability required")

    monkeypatch.setattr(rr, "require_org_settings", _refuse)
    with pytest.raises(ApiError) as get:
        await rr.get_org_regions(MagicMock())
    with pytest.raises(ApiError) as put:
        await _save(_stores("eu"), [_region("eu")])
    assert get.value.status_code == put.value.status_code == 403


# -- the platform's regions ----------------------------------------------------------------------


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    path = tmp_path / "provisa.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "multitenancy": True,
                "platform": {
                    "regions": [
                        {
                            "id": "eu",
                            "address": "${env:EU_ADDRESS}",
                            "data_addresses": ["https://eu-data.example.com"],
                        }
                    ]
                },
                "sources": [],
            }
        )
    )
    monkeypatch.setenv("PROVISA_CONFIG", str(path))
    monkeypatch.setattr(rr, "require_deployment_settings", lambda request: None)
    return path


def _platform_body(*regions: tuple[str, str]) -> rr.PlatformRegionsBody:
    return rr.PlatformRegionsBody(
        regions=[rr.PlatformRegionBody(id=i, address=a) for i, a in regions]
    )


async def test_a_platform_admin_reads_the_platform_regions_as_written(config_file):
    shown = await rr.get_platform_regions(MagicMock())
    assert shown == {
        "regions": [{"id": "eu", "address": "${env:EU_ADDRESS}"}],
        "restart_required": True,
    }


async def test_a_platform_admin_edits_the_platform_regions_into_the_config_file(config_file):
    saved = await rr.set_platform_regions(
        MagicMock(), _platform_body(("eu", "${env:EU_ADDRESS}"), ("us", "https://us.example.com"))
    )
    assert saved["restart_required"] is True
    written = yaml.safe_load(config_file.read_text())
    assert written["platform"]["regions"] == [
        # What the page does not edit stays as it was written.
        {
            "id": "eu",
            "address": "${env:EU_ADDRESS}",
            "data_addresses": ["https://eu-data.example.com"],
        },
        {"id": "us", "address": "https://us.example.com"},
    ]
    assert written["sources"] == [] and written["multitenancy"] is True


async def test_a_platform_region_twice_or_malformed_or_without_address_is_refused(config_file):
    before = config_file.read_text()
    for body, field in (
        (_platform_body(("eu", "a"), ("eu", "b")), "regions"),
        (_platform_body(("Europe", "a")), "regions.0.id"),
        (_platform_body(("eu", "a"), ("us", " ")), "regions.1.address"),
    ):
        with pytest.raises(ApiError) as refused:
            await rr.set_platform_regions(MagicMock(), body)
        assert refused.value.status_code == 422
        assert refused.value.params["field"] == field
    assert config_file.read_text() == before


async def test_the_platform_regions_are_a_platform_administrators(config_file, monkeypatch):
    def _refuse(request):
        raise ApiError(403, "platform.control_plane_role_required", "platform administrators")

    monkeypatch.setattr(rr, "require_deployment_settings", _refuse)
    with pytest.raises(ApiError) as get:
        await rr.get_platform_regions(MagicMock())
    with pytest.raises(ApiError) as put:
        await rr.set_platform_regions(MagicMock(), _platform_body(("eu", "a")))
    assert get.value.status_code == put.value.status_code == 403
