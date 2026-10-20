# Copyright (c) 2026 Kenneth Stott
# Canary: 68b0cc67-e027-434a-b702-7373e7f2e1f9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OpenAPI source kept beside a ServiceNow source (REQ-1954): written from the source as
stored, keeping what its steward added, its credential a reference to the source's."""

from __future__ import annotations

import json

import pytest

from provisa.api.admin import _servicenow_companion as companion
from provisa.core.models import Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.openapi.servicenow import MAPPING_KEY

pytestmark = pytest.mark.asyncio


@pytest.fixture
def model_db(control_plane, monkeypatch):
    from provisa.core import secrets_providers

    from provisa.api.app import state

    monkeypatch.setattr(companion, "_forget_spec", lambda cid: None)
    monkeypatch.setattr(state, "source_types", {})
    # A deployment of one organization: what the API layer says at import (provisa.api.app),
    # which this module does not import. Saving a source asks it (sandbox_egress.pinned_source).
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    # The stored credential is a reference: encrypted at rest without a key service here.
    from provisa.encryption import encryption_service

    service = encryption_service()
    monkeypatch.setattr(service, "encrypt", lambda raw: b"sealed:" + raw)
    return control_plane.org(holds="model")


async def _servicenow(db, **over) -> None:
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn,
            Source(
                **{
                    "id": "helpdesk",
                    "type": SourceType.servicenow,
                    "host": "https://acme.service-now.com",
                    "username": "admin",
                    "password": "${secret:helpdesk_password}",
                    **over,
                }
            ),
        )


async def test_the_companion_is_written_from_the_stored_source(model_db):
    from provisa.core.schema_org import api_sources
    from sqlalchemy import select

    await _servicenow(model_db)
    async with model_db.acquire() as conn:
        await companion.sync_companion(conn, "helpdesk")
        row = await source_repo.get(conn, "helpdesk-api")
        api = (await conn.execute_core(select(api_sources))).fetchone()
    assert (row["type"], row["path"]) == ("openapi", "servicenow:helpdesk")
    from provisa.api.app import state

    assert state.source_types["helpdesk-api"] == "openapi"
    assert row["mapping"][MAPPING_KEY] == {
        "source": "helpdesk",
        "instance_url": "https://acme.service-now.com",
        "username": "admin",
        "password": "${secret:helpdesk_password}",
        "apis": [],
        "import_sets": [],
    }
    assert api.base_url == "https://acme.service-now.com"
    sealed = json.loads(api.auth.removeprefix(b"sealed:"))
    assert sealed == {
        "type": "basic",
        "username": "admin",
        "password": "${secret:helpdesk_password}",
    }


async def test_saving_the_source_again_keeps_what_the_steward_added(model_db):
    await _servicenow(model_db)
    api = {"namespace": "sn_sc", "name": "Service Catalog API", "version": "latest"}
    async with model_db.acquire() as conn:
        await companion.sync_companion(conn, "helpdesk")
        await companion.change_companion(conn, "helpdesk", apis=[api])
    await _servicenow(model_db, host="https://acme2.service-now.com")
    async with model_db.acquire() as conn:
        await companion.sync_companion(conn, "helpdesk")
        held = (await source_repo.get(conn, "helpdesk-api"))["mapping"][MAPPING_KEY]
    assert held["apis"] == [api]
    assert held["instance_url"] == "https://acme2.service-now.com"


async def test_only_a_servicenow_source_has_a_companion(model_db):
    async with model_db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id="crm", type=SourceType.postgresql, host="db", port=5432)
        )
        with pytest.raises(LookupError, match="not a ServiceNow source"):
            await companion.sync_companion(conn, "crm")


@pytest.fixture
def section(model_db, monkeypatch):
    """The APIs section's helpers against the store above, with an instance that publishes two
    APIs and one active transform map into sys_user."""
    import contextlib

    from provisa.api.admin import schema
    from provisa.federation import pgwire_replica as pr
    from provisa.openapi.servicenow import InstanceApi

    async def pool():
        return model_db

    async def stored(source_id):
        async with model_db.acquire() as conn:
            return source_repo.source_from_row(await source_repo.get(conn, source_id))

    published = [
        InstanceApi("now", "Import Set API", "now/import", "", ("latest",), False),
        InstanceApi("sn_sc", "Service Catalog API", "sn_sc/servicecatalog", "", ("latest",), True),
    ]

    async def maps(source, schema_, target):
        return [("User Import", "imp_user")] if target == "sys_user" else []

    async def columns(source, schema_, table):
        return [("sys_id", "character varying"), ("u_user_name", "character varying")]

    monkeypatch.setattr(schema, "_get_pool", pool)
    monkeypatch.setattr(companion, "_servicenow_source", stored)
    monkeypatch.setattr(
        "provisa.openapi.servicenow.list_instance_apis", lambda url, user, pw: published
    )
    monkeypatch.setattr(pr, "servicenow_transform_maps", maps)
    monkeypatch.setattr(pr, "servicenow_columns", columns)
    monkeypatch.setattr("provisa.core.secrets_store.bound_to_request_org", contextlib.nullcontext)
    return model_db


async def _held_now(db) -> dict:
    async with db.acquire() as conn:
        return (await source_repo.get(conn, "helpdesk-api"))["mapping"][MAPPING_KEY]


async def test_an_api_the_instance_publishes_is_added_and_removed(section):
    await _servicenow(section)
    async with section.acquire() as conn:
        await companion.sync_companion(conn, "helpdesk")
    await companion.set_api("helpdesk", "sn_sc", "Service Catalog API", True)
    assert (await _held_now(section))["apis"] == [
        {"namespace": "sn_sc", "name": "Service Catalog API", "version": "latest"}
    ]
    await companion.set_api("helpdesk", "sn_sc", "Service Catalog API", False)
    assert (await _held_now(section))["apis"] == []


async def test_an_api_the_instance_does_not_publish_is_refused(section):
    await _servicenow(section)
    async with section.acquire() as conn:
        await companion.sync_companion(conn, "helpdesk")
    with pytest.raises(LookupError, match="publishes no API now/nothing"):
        await companion.set_api("helpdesk", "now", "nothing", True)


async def test_an_import_set_is_added_with_its_staging_columns(section):
    await _servicenow(section)
    async with section.acquire() as conn:
        await companion.sync_companion(conn, "helpdesk")
    await companion.set_import_set("helpdesk", "sys_user", "imp_user", True)
    assert (await _held_now(section))["import_sets"] == [
        {
            "staging": "imp_user",
            "table": "sys_user",
            "columns": [["sys_id", "character varying"], ["u_user_name", "character varying"]],
        }
    ]
    with pytest.raises(LookupError, match="'imp_other' is not the import set"):
        await companion.set_import_set("helpdesk", "sys_user", "imp_other", True)
    await companion.set_import_set("helpdesk", "sys_user", "imp_user", False)
    assert (await _held_now(section))["import_sets"] == []
