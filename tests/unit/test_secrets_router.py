# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the Secrets endpoints hand back, and what they will not (REQ-1558).

The one claim worth a test here is a negative one: no response carries a stored value. It is not
enough that no route is named "get" -- the list, the write and the audit record each pass through
a shape that could carry the value along with the name, so each of them is checked for it.
"""

# Requirements: REQ-1361, REQ-1557, REQ-1558

from __future__ import annotations

import pytest

from provisa.api.admin import secrets_router as sr
from provisa.api.errors import ApiError
from provisa.core.secrets_store import SecretInfo
from provisa.core.secrets_registry import SecretsProviderSpec

pytestmark = pytest.mark.asyncio

ORG = "acme"
VALUE = "ghp_thisisthesecretvalue"


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


def _info(name="GIT_TOKEN"):
    return SecretInfo(
        name=name, description="push", created_at=None, updated_at=None, updated_by="uid-admin"
    )


@pytest.fixture
def store(monkeypatch):
    """The store as a record of calls, and the built-in (writable) provider selected."""
    calls: dict[str, list] = {"guard": [], "put": [], "remove": [], "audit": []}
    held = {"GIT_TOKEN": _info()}

    async def _guard(request, org_id):
        calls["guard"].append(org_id)
        return "uid-admin"

    async def _listing(admin_db, org_id):
        return list(held.values())

    async def _describe(admin_db, org_id, name):
        return held.get(name)

    async def _put(admin_db, org_id, name, value, *, description=None, actor=None):
        calls["put"].append((org_id, name, value, actor))
        held[name] = _info(name)
        return held[name]

    async def _remove(admin_db, org_id, name):
        calls["remove"].append((org_id, name))
        return held.pop(name, None) is not None

    async def _audit(org_id, actor, action, name):
        calls["audit"].append((org_id, actor, action, name))

    monkeypatch.setattr(sr, "_guard", _guard)
    monkeypatch.setattr(sr, "_audit", _audit)
    monkeypatch.setattr(sr, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(sr.secrets_store, "listing", _listing)
    monkeypatch.setattr(sr.secrets_store, "describe", _describe)
    monkeypatch.setattr(sr.secrets_store, "put", _put)
    monkeypatch.setattr(sr.secrets_store, "remove", _remove)
    return calls


@pytest.fixture(autouse=True)
def _builtin():
    from provisa.core.secrets_runtime import configure_secrets, reset_secrets

    configure_secrets("provisa")
    yield
    reset_secrets()


def _central(monkeypatch):
    """Select a read-only central backend, as a deployment wired to Vault would have."""
    spec = SecretsProviderSpec(
        key="hashicorp_vault", label="Vault", description="", build=lambda cfg: None
    )
    monkeypatch.setattr(
        "provisa.core.secrets_runtime.secrets_backend_spec", lambda: spec, raising=True
    )


class TestNoResponseCarriesAValue:
    async def test_the_listing_is_names_and_metadata(self, store):
        answer = await sr.list_secrets(_Request(), ORG)
        assert answer["secrets"][0]["name"] == "GIT_TOKEN"
        assert answer["secrets"][0]["reference"] == "${secret:GIT_TOKEN}"
        assert "value" not in answer["secrets"][0]

    async def test_a_write_echoes_the_name_not_what_was_written(self, store):
        answer = await sr.put_secret(_Request(), ORG, "NEW", sr.SecretBody(value=VALUE))
        assert VALUE not in str(answer)
        assert answer["name"] == "NEW"

    async def test_the_audit_record_holds_no_value(self, store):
        await sr.put_secret(_Request(), ORG, "NEW", sr.SecretBody(value=VALUE))
        assert store["audit"] == [(ORG, "uid-admin", "secret.created", "NEW")]

    async def test_replacing_is_recorded_as_a_replacement(self, store):
        await sr.put_secret(_Request(), ORG, "GIT_TOKEN", sr.SecretBody(value=VALUE))
        assert store["audit"][0][2] == "secret.replaced"


class TestWhoMayAct:
    async def test_even_reading_the_names_is_an_org_admin_act(self, store):
        await sr.list_secrets(_Request(), ORG)
        assert store["guard"] == [ORG]

    async def test_every_write_and_delete_asks_the_same_guard(self, store):
        await sr.put_secret(_Request(), ORG, "GIT_TOKEN", sr.SecretBody(value=VALUE))
        await sr.delete_secret(_Request(), ORG, "GIT_TOKEN")
        assert store["guard"] == [ORG, ORG]


class TestWhenACentralServiceOwnsThem:
    async def test_provisa_does_not_enumerate_somebody_elses_store(self, store, monkeypatch):
        _central(monkeypatch)
        answer = await sr.list_secrets(_Request(), ORG)
        assert answer["provider"]["writable"] is False
        assert answer["secrets"] == []

    async def test_a_write_is_refused_rather_than_kept_in_a_second_place(self, store, monkeypatch):
        _central(monkeypatch)
        with pytest.raises(ApiError) as raised:
            await sr.put_secret(_Request(), ORG, "GIT_TOKEN", sr.SecretBody(value=VALUE))
        assert raised.value.status_code == 400
        assert raised.value.code == "secrets.provider_read_only"
        assert store["put"] == []

    async def test_a_delete_is_refused_too(self, store, monkeypatch):
        _central(monkeypatch)
        with pytest.raises(ApiError) as raised:
            await sr.delete_secret(_Request(), ORG, "GIT_TOKEN")
        assert raised.value.code == "secrets.provider_read_only"
        assert store["remove"] == []


class TestWhatIsRefused:
    async def test_deleting_a_name_that_is_not_there_is_a_404(self, store):
        with pytest.raises(ApiError) as raised:
            await sr.delete_secret(_Request(), ORG, "ABSENT")
        assert raised.value.status_code == 404
        assert raised.value.code == "secrets.not_found"

    async def test_a_rejected_name_is_the_callers_fault(self, store, monkeypatch):
        async def _put(*args, **kwargs):
            raise ValueError("Secret name 'a-b' must start with a letter")

        monkeypatch.setattr(sr.secrets_store, "put", _put)
        with pytest.raises(ApiError) as raised:
            await sr.put_secret(_Request(), ORG, "a-b", sr.SecretBody(value=VALUE))
        assert raised.value.status_code == 400
        assert raised.value.code == "secrets.invalid"

    async def test_a_deployment_that_cannot_encrypt_says_so(self, store, monkeypatch):
        """REQ-1557: no master key is an error, never a plaintext write."""

        async def _put(*args, **kwargs):
            raise RuntimeError("No encryption master key")

        monkeypatch.setattr(sr.secrets_store, "put", _put)
        with pytest.raises(ApiError) as raised:
            await sr.put_secret(_Request(), ORG, "GIT_TOKEN", sr.SecretBody(value=VALUE))
        assert raised.value.status_code == 500
        assert raised.value.code == "secrets.unencryptable"
