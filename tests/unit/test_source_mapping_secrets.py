# Copyright (c) 2026 Kenneth Stott
# Canary: 2e9d5a74-b360-4c18-8f27-d1a6c0e9b453
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A credential typed into a source's mapping is kept as its password is (REQ-1695): a literal
goes into the org vault and the row's mapping holds the reference that names it. A SharePoint
certificate password or service password sat in the row as typed before this."""

from __future__ import annotations

import pytest

from provisa.api.admin import schema_common
from provisa.api.admin.schema_common import (
    SOURCE_MAPPING_SECRET_KEYS,
    forget_source_mapping_secrets,
    source_mapping_secret_name,
    store_source_mapping_secrets,
)
from provisa.core.secrets_store import NAME

_aio = pytest.mark.asyncio(loop_scope="session")


@pytest.fixture
def vault(monkeypatch):
    """The org vault, as the calls made on it."""
    from provisa.api import app
    from provisa.core import request_context, secrets_store

    held: dict[str, str] = {}

    async def _put(db, org, name, value, **kw):
        held[name] = value

    async def _remove(db, org, name, **kw):
        held.pop(name)

    async def _planes(org):
        return []

    from provisa.api.admin import secrets_router

    monkeypatch.setattr(secrets_store, "put", _put)
    monkeypatch.setattr(secrets_store, "remove", _remove)
    monkeypatch.setattr(secrets_router, "_environment_planes", _planes)
    monkeypatch.setattr(app.state, "admin_db", object())
    monkeypatch.setattr(request_context, "require_current_org", lambda: "org-1")
    monkeypatch.setattr(request_context, "active_env", lambda: "prod")
    return held


def test_the_types_whose_mapping_carries_credentials():
    assert set(SOURCE_MAPPING_SECRET_KEYS["sharepoint"]) == {"certificate_password", "sp_password"}


@pytest.mark.parametrize("source_id", ["hr-sp", "hr.sharepoint", "9lives"])
def test_every_derived_name_is_storable_and_names_its_key(source_id):
    name = source_mapping_secret_name(source_id, "sp_password", "prod")
    assert NAME.match(name)
    assert name.endswith("__sp_password")
    assert name != source_mapping_secret_name(source_id, "certificate_password", "prod")
    assert name != schema_common.source_password_secret_name(source_id, "prod")
    assert name != source_mapping_secret_name(source_id, "sp_password", "staging")


@_aio
async def test_a_literal_is_vaulted_and_the_mapping_keeps_the_reference(vault):
    mapping = {
        "auth_type": "USERNAME_PASSWORD",
        "sp_username": "ops@contoso.com",
        "sp_password": "hunter2",
        "certificate_password": "pfx-123",
    }
    stored = await store_source_mapping_secrets(
        "user-1", "hr-sp", "sharepoint", mapping, env="prod"
    )
    pw = source_mapping_secret_name("hr-sp", "sp_password", "prod")
    pfx = source_mapping_secret_name("hr-sp", "certificate_password", "prod")
    assert stored == {
        "auth_type": "USERNAME_PASSWORD",
        "sp_username": "ops@contoso.com",
        "sp_password": f"${{secret:{pw}}}",
        "certificate_password": f"${{secret:{pfx}}}",
    }
    assert vault == {pw: "hunter2", pfx: "pfx-123"}
    # Nothing the row will hold, or the API will return, carries the literal.
    assert "hunter2" not in str(stored) and "pfx-123" not in str(stored)
    assert mapping["sp_password"] == "hunter2"  # the caller's mapping is not edited in place


@_aio
async def test_a_reference_is_kept_verbatim_and_nothing_is_vaulted(vault):
    mapping = {"certificate_path": "/c.pfx", "certificate_password": "${secret:my_pfx}"}
    stored = await store_source_mapping_secrets(None, "hr-sp", "sharepoint", mapping, env="prod")
    assert stored == mapping
    assert vault == {}


@_aio
async def test_an_empty_value_stays_empty(vault):
    # A password-less PFX names its password as the empty string.
    mapping = {"certificate_path": "/c.pfx", "certificate_password": ""}
    stored = await store_source_mapping_secrets(None, "hr-sp", "sharepoint", mapping, env="prod")
    assert stored == mapping
    assert vault == {}


@_aio
async def test_a_retyped_literal_replaces_the_entry_under_the_same_name(vault):
    for value in ("first", "second"):
        await store_source_mapping_secrets(
            None, "hr-sp", "sharepoint", {"sp_password": value}, env="prod"
        )
    assert vault == {source_mapping_secret_name("hr-sp", "sp_password", "prod"): "second"}


@_aio
async def test_a_type_with_no_credential_in_its_mapping_is_left_alone(vault):
    mapping = {"use_token": True, "sp_password": "not a sharepoint source"}
    assert await store_source_mapping_secrets(None, "s", "splunk", mapping, env="prod") == mapping
    assert vault == {}


@_aio
async def test_deleting_the_source_forgets_only_the_entries_it_minted(vault):
    stored = await store_source_mapping_secrets(
        None,
        "hr-sp",
        "sharepoint",
        {"sp_password": "hunter2", "certificate_password": "${secret:operators_own}"},
        env="prod",
    )
    vault["operators_own"] = "kept"
    await forget_source_mapping_secrets("hr-sp", "sharepoint", stored)
    assert vault == {"operators_own": "kept"}
