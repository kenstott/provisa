# Copyright (c) 2026 Kenneth Stott
# Canary: b10fdb0e-d057-4b59-90af-f0b0a17a14a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1486: the two token-free branding reads, and who may write them.

The read side is unusual for this codebase in that it answers without a bearer — it dresses the
sign-in page, which by definition renders before a token exists. That makes two properties worth
pinning: which org a request addresses is decided by REQ-1276's rule and nothing else, and an
address naming an org that does not exist answers exactly like an address naming none, so the
endpoint cannot be walked to learn which org ids are real.

The write side is the ordinary org_admin gate; what is checked here is that it is applied at all.
"""

# Requirements: REQ-1276, REQ-1486

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

from provisa.api.branding_router import router as branding_router
from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.org_branding import serialize_branding, validate_branding
from provisa.core.schema_admin import metadata, orgs

_PNG = b"\x89PNG\r\n\x1a\n" + b"0" * 32


@pytest.fixture
async def admin_db():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    db = Database(engine, "test")
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(orgs).values(
                id="acme",
                name="Acme Analytics",
                branding=serialize_branding(
                    validate_branding(
                        {
                            "display_name": "Acme Data Platform",
                            "primary_color": "#4F46E5",
                            "welcome_message": "Sign in with your Acme account.",
                        }
                    )
                ),
            )
        )
        await conn.execute_core(insert(orgs).values(id="plain", name="Plain Co"))
    try:
        yield db
    finally:
        await engine.dispose()


@pytest.fixture
def client(admin_db, monkeypatch):
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(admin_db=admin_db), raising=False)
    app = FastAPI()
    app.include_router(branding_router)
    with TestClient(app) as test_client:
        yield test_client


async def _set_logo(db: Database, org_id: str, data: bytes, media_type: str) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            update(orgs)
            .where(orgs.c.id == org_id)
            .values(branding_logo=data, branding_logo_media_type=media_type)
        )


def test_an_org_subdomain_reads_its_own_branding_without_a_token(client):
    read = client.get("/orgs/branding", headers={"host": "acme.provisa.dev"})

    assert read.status_code == 200
    assert read.json() == {
        "org_id": "acme",
        "name": "Acme Analytics",
        "branding": {
            "display_name": "Acme Data Platform",
            "primary_color": "#4f46e5",
            "welcome_message": "Sign in with your Acme account.",
        },
        "logo": False,
    }


def test_the_control_plane_host_names_no_org_of_its_own(client):
    """REQ-1276: cloud.<base> is the control plane. Its own branding read is the product's."""
    read = client.get("/orgs/branding", headers={"host": "cloud.provisa.dev"})

    assert read.json() == {"org_id": None, "name": None, "branding": {}, "logo": False}


def test_the_control_plane_reads_the_org_the_sign_in_is_for_from_the_header(client):
    """REQ-1348: an org subdomain redirects to the control plane to sign in, so on that page the
    Host names no org and the page names it itself."""
    read = client.get(
        "/orgs/branding",
        headers={"host": "cloud.provisa.dev", "x-org-provisa": "acme"},
    )

    assert read.json()["org_id"] == "acme"


def test_an_org_named_by_header_on_an_org_subdomain_is_ignored(client):
    """The Host is the address. A header naming a different org must not read across it."""
    read = client.get(
        "/orgs/branding",
        headers={"host": "plain.provisa.dev", "x-org-provisa": "acme"},
    )

    assert read.json()["org_id"] == "plain"
    assert read.json()["branding"] == {}


def test_an_address_naming_no_existing_org_answers_like_an_address_naming_none(client):
    """Otherwise the endpoint enumerates which org ids exist, to anyone, without a token."""
    unknown = client.get("/orgs/branding", headers={"host": "nosuchorg.provisa.dev"})
    none_named = client.get("/orgs/branding", headers={"host": "cloud.provisa.dev"})

    assert unknown.status_code == none_named.status_code
    assert unknown.json() == none_named.json()


@pytest.mark.parametrize("host", ["provisa.dev", "localhost", "127.0.0.1"])
def test_a_host_with_no_org_label_reads_as_no_org(client, host):
    assert client.get("/orgs/branding", headers={"host": host}).json()["org_id"] is None


def test_a_malformed_org_name_in_the_header_is_refused_rather_than_queried(client):
    read = client.get(
        "/orgs/branding", headers={"host": "cloud.provisa.dev", "x-org-provisa": "bad;org"}
    )

    assert read.json()["org_id"] is None


async def test_the_logo_is_served_as_its_own_bytes_with_the_stored_type(client, admin_db):
    await _set_logo(admin_db, "acme", _PNG, "image/png")

    read = client.get("/orgs/branding/logo", headers={"host": "acme.provisa.dev"})

    assert read.status_code == 200
    assert read.content == _PNG
    assert read.headers["content-type"] == "image/png"
    # The type is what was stored, never sniffed from the bytes.
    assert read.headers["x-content-type-options"] == "nosniff"


async def test_a_tenant_supplied_svg_is_served_under_a_policy_that_cannot_execute(client, admin_db):
    await _set_logo(admin_db, "acme", b"<svg xmlns='http://www.w3.org/2000/svg'/>", "image/svg+xml")

    read = client.get("/orgs/branding/logo", headers={"host": "acme.provisa.dev"})

    policy = read.headers["content-security-policy"]
    assert "default-src 'none'" in policy
    assert "sandbox" in policy


async def test_the_branding_read_reports_that_a_logo_exists(client, admin_db):
    await _set_logo(admin_db, "acme", _PNG, "image/png")

    assert client.get("/orgs/branding", headers={"host": "acme.provisa.dev"}).json()["logo"] is True


def test_an_org_with_no_logo_404s_so_the_img_shows_nothing(client):
    read = client.get("/orgs/branding/logo", headers={"host": "plain.provisa.dev"})

    assert read.status_code == 404


async def test_an_image_names_its_org_by_query_since_it_cannot_send_a_header(client, admin_db):
    """The sign-in page runs on the control plane, where the Host names no org; an <img> there has
    no way to set x-org-provisa."""
    await _set_logo(admin_db, "acme", _PNG, "image/png")

    read = client.get("/orgs/branding/logo?org=acme", headers={"host": "cloud.provisa.dev"})

    assert read.status_code == 200
    assert read.content == _PNG


# The write side.


@pytest.fixture
def admin_client(admin_db, monkeypatch):
    """The org admin gate is the surface under test, so it is left real and its inputs are faked
    per call by the tests below."""
    from provisa.api.admin import orgs_router as orgs_router_mod

    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(admin_db=admin_db, org_registry=None),
        raising=False,
    )
    monkeypatch.setattr(orgs_router_mod, "_admin_pool", lambda: admin_db)
    app = FastAPI()
    app.include_router(orgs_router_mod.router)
    with TestClient(app) as test_client:
        yield test_client


def _allow_org_admin(monkeypatch, allowed: str | None):
    from provisa.api.admin import invites_router as invites_mod

    async def _gate(_request, org_id, *, allow_cross_org=True):
        if org_id != allowed:
            raise ApiError(403, "orgs.forbidden", "Not an admin of this org")

    monkeypatch.setattr(invites_mod, "_require_org_admin", _gate)


def test_only_an_admin_of_the_org_may_write_its_branding(admin_client, monkeypatch):
    _allow_org_admin(monkeypatch, "plain")

    denied = admin_client.patch("/admin/orgs/acme/branding", json={"display_name": "Not Mine"})

    assert denied.status_code == 403


def test_an_admin_writes_and_reads_back_its_branding(admin_client, monkeypatch):
    _allow_org_admin(monkeypatch, "acme")

    written = admin_client.patch(
        "/admin/orgs/acme/branding",
        json={"display_name": "Acme Data", "primary_color": "#B91C1C"},
    )

    assert written.status_code == 200
    assert written.json()["branding"] == {
        "display_name": "Acme Data",
        "primary_color": "#b91c1c",
    }
    read = admin_client.get("/admin/orgs/acme/branding")
    assert read.json()["branding"] == written.json()["branding"]


def test_an_invalid_value_is_refused_with_the_field_that_caused_it(admin_client, monkeypatch):
    _allow_org_admin(monkeypatch, "acme")

    refused = admin_client.patch("/admin/orgs/acme/branding", json={"primary_color": "red"})

    assert refused.status_code == 422
    assert "primary_color" in refused.json()["detail"]


def test_a_patch_replaces_the_document_rather_than_merging_into_it(admin_client, monkeypatch):
    """The editor edits the whole document, so an omitted field is an intentional clear."""
    _allow_org_admin(monkeypatch, "acme")

    admin_client.patch("/admin/orgs/acme/branding", json={"display_name": "Acme Data"})

    assert admin_client.get("/admin/orgs/acme/branding").json()["branding"] == {
        "display_name": "Acme Data"
    }


def test_a_logo_upload_stores_the_bytes_and_a_delete_removes_them(admin_client, monkeypatch):
    _allow_org_admin(monkeypatch, "acme")

    uploaded = admin_client.put(
        "/admin/orgs/acme/branding/logo",
        content=_PNG,
        headers={"content-type": "image/png"},
    )

    assert uploaded.status_code == 200
    assert uploaded.json() == {"org_id": "acme", "logo_media_type": "image/png", "bytes": len(_PNG)}
    assert admin_client.get("/admin/orgs/acme/branding").json()["logo_media_type"] == "image/png"

    admin_client.delete("/admin/orgs/acme/branding/logo")
    assert admin_client.get("/admin/orgs/acme/branding").json()["logo_media_type"] is None


def test_a_logo_of_a_type_the_page_cannot_render_is_refused(admin_client, monkeypatch):
    _allow_org_admin(monkeypatch, "acme")

    refused = admin_client.put(
        "/admin/orgs/acme/branding/logo",
        content=b"<html></html>",
        headers={"content-type": "text/html"},
    )

    assert refused.status_code == 422
    assert admin_client.get("/admin/orgs/acme/branding").json()["logo_media_type"] is None
