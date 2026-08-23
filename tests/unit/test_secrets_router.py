# Copyright (c) 2026 Kenneth Stott
# Canary: 74514c6c-29a5-4b17-97c3-1b2160cf317c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the Secrets endpoints hand back, and what they will not (REQ-1558, REQ-1560).

The one claim worth a test here is a negative one: no response carries a stored value. It is not
enough that no route is named "get" -- the list, the write and the audit record each pass through
a shape that could carry the value along with the name, so each of them is checked for it.

The second claim is about ADDRESSING: the org vault and a person's vault are the same table under
two owners, and the personal endpoints take their owner from the authenticated identity, so there
is no request shape that names another person's secret at all.
"""

# Requirements: REQ-1361, REQ-1557, REQ-1558, REQ-1560

from __future__ import annotations

import pytest

from provisa.api.admin import secrets_router as sr
from provisa.api.errors import ApiError
from provisa.core.secrets_store import ORG_OWNER, SecretInfo
from provisa.core.secrets_registry import SecretsProviderSpec

pytestmark = pytest.mark.asyncio

ORG = "acme"
VALUE = "ghp_thisisthesecretvalue"


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


def _info(name="GIT_TOKEN", owner_id=ORG_OWNER):
    return SecretInfo(
        name=name,
        description="push",
        created_at=None,
        updated_at=None,
        updated_by="uid-admin",
        owner_id=owner_id,
    )


@pytest.fixture
def store(monkeypatch):
    """The store as a record of calls, and the built-in (writable) provider selected.

    Held is keyed by (owner_id, name) -- the same shape the table's primary key has, which is what
    makes "another developer cannot reach your secret" a fact about addressing rather than a check.
    """
    calls: dict[str, list] = {"guard": [], "owner": [], "put": [], "remove": [], "audit": []}
    held: dict[tuple[str, str], SecretInfo] = {(ORG_OWNER, "GIT_TOKEN"): _info()}

    async def _org_guard(request, org_id):
        calls["guard"].append(org_id)
        return "uid-admin"

    def _personal_owner(request, org_id):
        calls["owner"].append(org_id)
        return "uid-dev"

    async def _listing(admin_db, org_id, *, owner_id):
        return [i for (o, _), i in held.items() if o == owner_id]

    async def _describe(admin_db, org_id, name, *, owner_id):
        return held.get((owner_id, name))

    async def _put(admin_db, org_id, name, value, *, owner_id, description=None, actor=None):
        calls["put"].append((org_id, owner_id, name, value, actor))
        held[(owner_id, name)] = _info(name, owner_id)
        return held[(owner_id, name)]

    async def _remove(admin_db, org_id, name, *, owner_id):
        calls["remove"].append((org_id, owner_id, name))
        return held.pop((owner_id, name), None) is not None

    async def _audit(org_id, actor, action, name):
        calls["audit"].append((org_id, actor, action, name))

    monkeypatch.setattr(sr, "_org_guard", _org_guard)
    monkeypatch.setattr(sr, "_personal_owner", _personal_owner)
    monkeypatch.setattr(sr, "_audit", _audit)
    monkeypatch.setattr(sr, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(sr.secrets_store, "listing", _listing)
    monkeypatch.setattr(sr.secrets_store, "describe", _describe)
    monkeypatch.setattr(sr.secrets_store, "put", _put)
    monkeypatch.setattr(sr.secrets_store, "remove", _remove)
    calls["held"] = held
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


class TestThePersonalVault:
    """REQ-1560: whose secret it is, is part of where it is."""

    async def test_a_personal_write_lands_under_the_caller_not_the_org(self, store):
        await sr.put_my_secret(_Request(), ORG, "GIT_TOKEN", sr.SecretBody(value=VALUE))
        assert store["put"] == [(ORG, "uid-dev", "GIT_TOKEN", VALUE, "uid-dev")]
        # The org's GIT_TOKEN is untouched: two people may each hold one of that name.
        assert (ORG_OWNER, "GIT_TOKEN") in store["held"]
        assert ("uid-dev", "GIT_TOKEN") in store["held"]

    async def test_the_reference_names_the_vault_it_came_from(self, store):
        answer = await sr.put_my_secret(_Request(), ORG, "GIT_TOKEN", sr.SecretBody(value=VALUE))
        assert answer["reference"] == "${user:GIT_TOKEN}"
        assert answer["scope"] == "user"
        assert VALUE not in str(answer)

    async def test_a_personal_listing_shows_only_the_callers_own(self, store):
        await sr.put_my_secret(_Request(), ORG, "MINE", sr.SecretBody(value=VALUE))
        names = [s["name"] for s in (await sr.list_my_secrets(_Request(), ORG))["secrets"]]
        assert names == ["MINE"]
        org_names = [s["name"] for s in (await sr.list_secrets(_Request(), ORG))["secrets"]]
        assert org_names == ["GIT_TOKEN"]

    async def test_holding_a_personal_secret_asks_no_capability(self, store):
        """No org_settings, no admin -- the owner comes off the identity and that is the whole
        authorization. What it must NOT do is fall through to the org guard."""
        await sr.list_my_secrets(_Request(), ORG)
        await sr.put_my_secret(_Request(), ORG, "MINE", sr.SecretBody(value=VALUE))
        await sr.delete_my_secret(_Request(), ORG, "MINE")
        assert store["guard"] == []
        assert store["owner"] == [ORG, ORG, ORG]

    async def test_a_personal_delete_cannot_reach_the_org_vault(self, store):
        """GIT_TOKEN exists -- in the ORG vault. Asking for it as a personal secret is a 404,
        because the name alone does not address it."""
        with pytest.raises(ApiError) as raised:
            await sr.delete_my_secret(_Request(), ORG, "GIT_TOKEN")
        assert raised.value.status_code == 404
        assert (ORG_OWNER, "GIT_TOKEN") in store["held"]

    async def test_the_audit_record_says_which_vault(self, store):
        await sr.put_my_secret(_Request(), ORG, "MINE", sr.SecretBody(value=VALUE))
        await sr.delete_my_secret(_Request(), ORG, "MINE")
        assert store["audit"] == [
            (ORG, "uid-dev", "user_secret.created", "MINE"),
            (ORG, "uid-dev", "user_secret.deleted", "MINE"),
        ]


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
