# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0e3b57-1c8a-4f29-9e64-b2a7c5d19f30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regions edited in the UI (REQ-1922, amended 2026-11-05): the platform's regions -- an id, a
description, its purposes and its data addresses -- and the default grant, written to the config
file by a platform administrator and applied on restart; each org's grant (its regions and the
stores it keeps in each), saved whole by the platform administrator, validated as a configuration
is and refused naming the field; the org administrator reads its grant and changes nothing; a demo
sandbox reads the platform's regions and writes nothing."""

# Requirements: REQ-1922

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from provisa.api.admin import regions_router as rr
from provisa.api.errors import ApiError
from provisa.core.regions import PlatformConfig, PlatformRegion

_PLATFORM = PlatformConfig(
    regions=[
        PlatformRegion(id="eu", description="Data that stays in the EU", purposes=["residency"]),
        PlatformRegion(id="us", description="Analytics", purposes=["workload", "latency"]),
    ]
)
_ORG = "default"
# The sandbox check as the module defines it, before a fixture stands in for it.
_IN_DEMO_SANDBOX = rr._in_demo_sandbox


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


@pytest.fixture(autouse=True)
def _not_a_sandbox(monkeypatch):
    monkeypatch.setattr(rr, "_in_demo_sandbox", lambda: False)


@pytest.fixture
async def model(control_plane, monkeypatch):
    """An org's model store on a node of a platform declaring eu and us, the caller a platform
    administrator."""
    from sqlalchemy import insert

    from provisa.core import process_region
    from provisa.core.schema_org import domains

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
    process_region.bind_launch(_PLATFORM.model_dump(), requested="eu")

    @asynccontextmanager
    async def _store(org_id):
        assert org_id == _ORG
        yield db

    async def _orgs():
        return [_ORG]

    monkeypatch.setattr(rr, "_org_model_store", _store)
    monkeypatch.setattr(rr, "_org_ids", _orgs)
    monkeypatch.setattr(rr, "_acting_model_db", lambda: db)
    monkeypatch.setattr(rr, "_platform", lambda: _PLATFORM)
    monkeypatch.setattr(rr, "require_deployment_settings", lambda request: None)
    monkeypatch.setattr(rr, "require_org_settings", lambda request: None)
    yield db
    process_region.bind_launch({}, requested=None)


async def _grant(stores: list[dict], regions: list[dict], *, confirm: bool = False) -> dict:
    return await rr.set_org_grant(
        _ORG,
        MagicMock(),
        rr.GrantBody(stores=stores, regions=regions, confirm_removal=confirm),
    )


async def _refused(stores: list[dict], regions: list[dict], status: int = 422) -> ApiError:
    with pytest.raises(ApiError) as refused:
        await _grant(stores, regions)
    assert refused.value.status_code == status
    return refused.value


# -- the platform administrator grants an org regions and assigns its stores -----------------------


async def test_a_platform_admin_grants_an_org_regions_with_its_stores(model):
    empty = await rr.get_org_grant(_ORG, MagicMock())
    assert empty["stores"] == [] and empty["regions"] == []
    assert empty["platform_regions"] == [
        {"id": "eu", "description": "Data that stays in the EU", "purposes": ["residency"]},
        {"id": "us", "description": "Analytics", "purposes": ["workload", "latency"]},
    ]
    assert "trino-byo" in empty["engine_kinds"]
    assert empty["purposes"] == ["residency", "workload", "latency", "resilience", "dedicated"]

    await _grant(_stores("eu"), [_region("eu")])
    listed = await rr.get_org_grant(_ORG, MagicMock())
    assert listed["regions"] == [_region("eu")]
    assert {s["id"] for s in listed["stores"]} == {"eu-pg", "eu-redis", "eu-trino"}
    assert await rr.get_org_grants(MagicMock()) == {"orgs": [{"id": _ORG, "regions": ["eu"]}]}


async def test_a_store_url_is_kept_as_entered_and_its_password_never_comes_back(model):
    await _grant(_stores("eu"), [_region("eu")])
    shown = {s["id"]: s for s in (await rr.get_org_grant(_ORG, MagicMock()))["stores"]}
    # A reference stays the reference; nothing resolves it.
    assert shown["eu-redis"]["url"] == "${env:REDIS_URL}"
    assert "s3cret" not in shown["eu-pg"]["url"]
    assert shown["eu-pg"]["url"] == "postgresql://app@eu.db:5432/provisa"

    # Sending the page back unchanged keeps the password the form never saw.
    await _grant(list(shown.values()), [_region("eu")])
    from provisa.core.repositories import region as region_repo

    async with model.acquire() as conn:
        held = {s.id: s.url for s in await region_repo.list_stores(conn)}
    assert held["eu-pg"] == "postgresql://app:s3cret@eu.db:5432/provisa"
    assert held["eu-redis"] == "${env:REDIS_URL}"


async def test_a_platform_admin_edits_and_removes_an_orgs_grant(model):
    await _grant(_stores("eu") + _stores("us"), [_region("eu"), _region("us")])

    # Edit: eu's cache moves to us's Redis store.
    await _grant(_stores("eu") + _stores("us"), [_region("eu", cache="us-redis"), _region("us")])
    listed = await rr.get_org_grant(_ORG, MagicMock())
    assert listed["regions"][0]["cache"] == "us-redis"

    # Remove: the org is no longer granted us, and its stores go with it -- once confirmed.
    saved = await _grant(_stores("eu"), [_region("eu")], confirm=True)
    assert [r["id"] for r in saved["regions"]] == ["eu"]
    assert {s["id"] for s in saved["stores"]} == {"eu-pg", "eu-redis", "eu-trino"}


async def test_removing_a_grant_the_org_holds_is_refused_naming_it_unless_confirmed(model):
    await _grant(_stores("eu") + _stores("us"), [_region("eu"), _region("us")])
    refused = await _refused(_stores("eu"), [_region("eu")], status=409)
    assert refused.code == "regions.affects_orgs"
    assert refused.params["orgs"] == [_ORG] and refused.params["regions"] == ["us"]
    held = await rr.get_org_grant(_ORG, MagicMock())
    assert [r["id"] for r in held["regions"]] == ["eu", "us"]  # nothing was written


async def test_a_region_naming_an_unknown_store_is_refused_naming_the_field(model):
    refused = await _refused(_stores("eu"), [_region("eu", engine="nowhere")])
    assert refused.detail == "region 'eu' engine store 'nowhere' is not declared in stores"
    assert refused.params["field"] == "regions.eu.engine"
    assert (await rr.get_org_grant(_ORG, MagicMock()))["regions"] == []  # nothing was written


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

    await _grant(_stores("eu") + _stores("us"), [_region("eu"), _region("us")])
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
    with pytest.raises(ApiError) as refused:
        await _grant(_stores("eu"), [_region("eu")], confirm=True)
    assert refused.value.status_code == 422
    assert refused.value.params["field"] == "regions.us.id"
    assert "us" in refused.value.detail
    held = await rr.get_org_grant(_ORG, MagicMock())
    assert [r["id"] for r in held["regions"]] == ["eu", "us"]


async def test_an_org_the_platform_does_not_hold_is_refused(model):
    with pytest.raises(ApiError) as refused:
        await rr.get_org_grant("nosuch", MagicMock())
    assert refused.value.status_code == 404


async def test_granting_is_a_platform_administrators(model, monkeypatch):
    def _refuse(request):
        raise ApiError(403, "platform.control_plane_role_required", "platform administrators")

    monkeypatch.setattr(rr, "require_deployment_settings", _refuse)
    for call in (
        lambda: rr.get_org_grants(MagicMock()),
        lambda: rr.get_org_grant(_ORG, MagicMock()),
        lambda: _grant(_stores("eu"), [_region("eu")]),
    ):
        with pytest.raises(ApiError) as refused:
            await call()
        assert refused.value.status_code == 403


# -- the org administrator reads its grant, and changes nothing ------------------------------------


async def test_an_org_admin_reads_its_regions_and_stores_with_their_purposes(model):
    await _grant(_stores("eu"), [_region("eu")])
    shown = await rr.get_org_regions(MagicMock())
    assert shown["regions"] == [_region("eu")]
    assert shown["platform_regions"][0] == {
        "id": "eu",
        "description": "Data that stays in the EU",
        "purposes": ["residency"],
    }
    assert "s3cret" not in str(shown["stores"])


def test_the_org_admin_has_no_write_to_its_regions():
    """REQ-1922 (amended 2026-11-05): which regions an org uses is the platform's decision. The
    org's own route reads; there is nothing there to write."""
    methods = {
        method
        for route in rr.router.routes
        if getattr(route, "path", None) == "/admin/regions"
        for method in route.methods  # type: ignore[attr-defined]
    }
    assert methods == {"GET"}


async def test_reading_the_orgs_regions_needs_org_settings(model, monkeypatch):
    def _refuse(request):
        raise ApiError(403, "platform.right_required", "org_settings capability required")

    monkeypatch.setattr(rr, "require_org_settings", _refuse)
    with pytest.raises(ApiError) as get:
        await rr.get_org_regions(MagicMock())
    assert get.value.status_code == 403


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
                            "description": "Data that stays in the EU",
                            "purposes": ["residency"],
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

    async def _no_orgs():
        return []

    monkeypatch.setattr(rr, "_org_ids", _no_orgs)
    return path


def _eu(**over) -> dict:
    return {
        "id": "eu",
        "description": "Data that stays in the EU",
        "purposes": ["residency"],
        "data_addresses": ["https://eu-data.example.com"],
        **over,
    }


def _platform_body(*regions: dict, **over) -> rr.PlatformRegionsBody:
    return rr.PlatformRegionsBody.model_validate({"regions": list(regions), **over})


async def test_a_platform_admin_reads_the_platform_regions_as_written(config_file):
    shown = await rr.get_platform_regions(MagicMock())
    assert shown["regions"] == [_eu()]
    assert shown["default_grant"] == {"stores": [], "regions": []}
    assert shown["purposes"] == ["residency", "workload", "latency", "resilience", "dedicated"]
    assert shown["restart_required"] is True and shown["read_only"] is False


async def test_descriptions_purposes_and_data_addresses_round_trip(config_file):
    us = {"id": "us", "description": "Analytics", "purposes": ["workload", "latency"]}
    saved = await rr.set_platform_regions(MagicMock(), _platform_body(_eu(), us))
    assert saved["restart_required"] is True
    written = yaml.safe_load(config_file.read_text())
    assert written["platform"]["regions"] == [_eu(), us]
    assert written["sources"] == [] and written["multitenancy"] is True
    shown = await rr.get_platform_regions(MagicMock())
    assert shown["regions"] == [_eu(), {**us, "data_addresses": []}]


def test_a_platform_region_sent_with_an_address_is_refused():
    with pytest.raises(ValidationError):
        rr.PlatformRegionBody.model_validate({**_eu(), "address": "https://eu.example.com"})


@pytest.mark.parametrize(
    ("regions", "field"),
    [
        ((_eu(), _eu()), "regions"),
        ((_eu(id="Europe"),), "regions.0.id"),
        ((_eu(purposes=[]),), "regions.0.purposes"),
        ((_eu(purposes=["speed"]),), "regions.0.purposes"),
        ((_eu(description=""),), "regions.0.description"),
        ((_eu(data_addresses=["ftp://x"]),), "regions.0.data_addresses"),
    ],
)
async def test_a_platform_region_that_does_not_hold_together_is_refused(
    config_file, regions, field
):
    before = config_file.read_text()
    with pytest.raises(ApiError) as refused:
        await rr.set_platform_regions(MagicMock(), _platform_body(*regions))
    assert refused.value.status_code == 422
    assert refused.value.params["field"] == field
    assert config_file.read_text() == before


async def test_the_default_grant_round_trips_with_its_password_kept(config_file):
    grant = {"stores": _stores("eu"), "regions": [_region("eu")]}
    saved = await rr.set_platform_regions(MagicMock(), _platform_body(_eu(), default_grant=grant))
    assert saved["default_grant"]["regions"] == [_region("eu")]
    shown = {s["id"]: s for s in saved["default_grant"]["stores"]}
    assert shown["eu-pg"]["url"] == "postgresql://app@eu.db:5432/provisa"

    # Sent back as shown, the password the form never saw is kept.
    sent = {"stores": list(shown.values()), "regions": [_region("eu")]}
    await rr.set_platform_regions(MagicMock(), _platform_body(_eu(), default_grant=sent))
    written = yaml.safe_load(config_file.read_text())["platform"]["default_grant"]
    held = {s["id"]: s["url"] for s in written["stores"]}
    assert held["eu-pg"] == "postgresql://app:s3cret@eu.db:5432/provisa"
    assert held["eu-redis"] == "${env:REDIS_URL}"


async def test_a_default_grant_that_does_not_hold_together_is_refused_naming_the_field(
    config_file,
):
    before = config_file.read_text()
    grant = {"stores": _stores("eu"), "regions": [_region("eu", engine="nowhere")]}
    with pytest.raises(ApiError) as refused:
        await rr.set_platform_regions(MagicMock(), _platform_body(_eu(), default_grant=grant))
    assert refused.value.status_code == 422
    assert refused.value.params["field"] == "default_grant.regions.eu.engine"
    assert config_file.read_text() == before


async def test_removing_a_region_orgs_are_granted_lists_them_unless_confirmed(
    config_file, monkeypatch
):
    granted = {"acme": ["eu"], "globex": ["eu", "us"], "initech": ["us"]}

    async def _orgs():
        return sorted(granted)

    async def _granted(org_id):
        return granted[org_id]

    monkeypatch.setattr(rr, "_org_ids", _orgs)
    monkeypatch.setattr(rr, "_granted", _granted)
    us = {"id": "us", "description": "Analytics", "purposes": ["workload"]}
    before = config_file.read_text()
    with pytest.raises(ApiError) as refused:
        await rr.set_platform_regions(MagicMock(), _platform_body(us))
    assert refused.value.status_code == 409
    assert refused.value.code == "regions.affects_orgs"
    assert refused.value.params["orgs"] == ["acme", "globex"]
    assert refused.value.params["regions"] == ["eu"]
    assert config_file.read_text() == before

    await rr.set_platform_regions(MagicMock(), _platform_body(us, confirm_removal=True))
    written = yaml.safe_load(config_file.read_text())
    assert [r["id"] for r in written["platform"]["regions"]] == ["us"]


async def test_the_platform_regions_are_a_platform_administrators(config_file, monkeypatch):
    def _refuse(request):
        raise ApiError(403, "platform.control_plane_role_required", "platform administrators")

    monkeypatch.setattr(rr, "require_deployment_settings", _refuse)
    with pytest.raises(ApiError) as get:
        await rr.get_platform_regions(MagicMock())
    with pytest.raises(ApiError) as put:
        await rr.set_platform_regions(MagicMock(), _platform_body(_eu()))
    assert get.value.status_code == put.value.status_code == 403


# -- a demo sandbox: the platform's regions are shown, and nothing is written ---------------------


async def test_a_sandbox_reads_the_platform_regions_read_only(config_file, monkeypatch):
    def _refuse(request):
        raise ApiError(403, "platform.control_plane_role_required", "platform administrators")

    monkeypatch.setattr(rr, "require_deployment_settings", _refuse)
    monkeypatch.setattr(rr, "_in_demo_sandbox", lambda: True)
    shown = await rr.get_platform_regions(MagicMock())
    assert shown["read_only"] is True
    assert shown["regions"] == [_eu()]


async def test_a_sandbox_writes_nothing(config_file, model, monkeypatch):
    monkeypatch.setattr(rr, "_in_demo_sandbox", lambda: True)
    before = config_file.read_text()
    with pytest.raises(ApiError) as platform:
        await rr.set_platform_regions(MagicMock(), _platform_body(_eu()))
    with pytest.raises(ApiError) as grant:
        await _grant(_stores("eu"), [_region("eu")])
    for refused in (platform, grant):
        assert refused.value.status_code == 403
        assert refused.value.code == "regions.sandbox_read_only"
    assert config_file.read_text() == before
    assert (await rr.get_org_grant(_ORG, MagicMock()))["regions"] == []


def test_a_demo_sandbox_is_recognised_as_the_rest_of_the_product_recognises_it():
    from provisa.core.request_context import current_org

    for org, sandbox in (("sbx0123456789ab", True), ("sbxtplretail", False), ("acme", False)):
        token = current_org.set(org)
        try:
            assert _IN_DEMO_SANDBOX() is sandbox
        finally:
            current_org.reset(token)


# -- a new org receives the platform's default grant ------------------------------------------------


async def test_a_new_org_receives_the_default_grant_and_an_org_with_regions_keeps_its_own(model):
    from provisa.core.regions import RegionGrant
    from provisa.core.repositories import region as region_repo

    grant = RegionGrant.model_validate({"stores": _stores("eu"), "regions": [_region("eu")]})
    async with model.acquire() as conn:
        assert await region_repo.apply_default_grant(conn, grant) is True
        assert [r.id for r in await region_repo.list_regions(conn)] == ["eu"]
        held = {s.id: s.url for s in await region_repo.list_stores(conn)}
    assert held["eu-redis"] == "${env:REDIS_URL}"  # kept as entered

    other = RegionGrant.model_validate({"stores": _stores("us"), "regions": [_region("us")]})
    async with model.acquire() as conn:
        assert await region_repo.apply_default_grant(conn, other) is False
        assert [r.id for r in await region_repo.list_regions(conn)] == ["eu"]
