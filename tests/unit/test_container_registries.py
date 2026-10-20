# Copyright (c) 2026 Kenneth Stott
# Canary: 9d67aa2e-662b-443e-971d-0f78109b0fda
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The container registries an organisation's built code-source images are pushed to (REQ-2098):
entered once by its administrator, each field kept as entered and resolved at use in that
organisation's vault, one of them the default, opened by ``org_settings`` in that organisation
alone. The control plane is a real PostgreSQL database; every credential is made up."""

# Requirements: REQ-2098
from __future__ import annotations

import pytest
import sqlalchemy as sa

from provisa.api.admin import container_registries_router as router
from provisa.api.errors import ApiError
from provisa.core import container_registries as registries
from provisa.core import schema_admin, secrets_store
from provisa.core.container_registries import ContainerRegistryRefused

pytestmark = pytest.mark.asyncio

TOKEN = "made-up-registry-token"


class _Sealer:
    def encrypt(self, value: bytes) -> bytes:
        return b"sealed:" + value[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        return blob[len(b"sealed:") :][::-1]


@pytest.fixture
def plane(control_plane, monkeypatch):
    with control_plane.engine().begin() as conn:
        for org_id in ("acme", "globex"):  # org_container_registries' foreign key
            conn.execute(sa.insert(schema_admin.orgs).values(id=org_id, name=org_id))
    monkeypatch.setattr(secrets_store, "_cipher", lambda *, mint: _Sealer())

    async def held(conn, *, mint):
        return None

    monkeypatch.setattr(secrets_store, "_hold_the_deployments_key", held)
    return control_plane.platform


async def _registry(plane, org="acme", **changed):
    given = {
        "registry_id": None,
        "address": "ghcr.io/acme",
        "username": "acme-bot",
        "credential": TOKEN,
        "default": False,
        "actor": "uid-ada",
    }
    given.update(changed)
    return await registries.put(plane, org, **given)


def _stored(plane) -> list[dict]:
    with plane.engine.connect() as conn:
        return [
            dict(r._mapping) for r in conn.execute(sa.select(schema_admin.org_container_registries))
        ]


class TestTheStore:
    async def test_an_entry_is_kept_exactly_as_entered(self, plane):
        kept = await _registry(plane, credential="${secret:ghcr_acme}", default=True)
        assert (kept.address, kept.username, kept.credential, kept.default) == (
            "ghcr.io/acme",
            "acme-bot",
            "${secret:ghcr_acme}",
            True,
        )
        (row,) = _stored(plane)
        assert row["credential"] == "${secret:ghcr_acme}" and row["updated_by"] == "uid-ada"

    async def test_an_address_typed_as_a_url_is_kept_as_the_registry_and_namespace(self, plane):
        kept = await _registry(plane, address="https://registry.acme.test:5000/data/")
        assert kept.address == "registry.acme.test:5000/data"

    async def test_one_organisations_entries_are_not_anothers(self, plane):
        await _registry(plane)
        assert await registries.entries(plane, "globex") == []

    async def test_a_replace_keeps_the_credential_left_out(self, plane):
        kept = await _registry(plane)
        replaced = await _registry(plane, registry_id=kept.id, username="other", credential=None)
        assert (replaced.username, replaced.credential) == ("other", TOKEN)

    @pytest.mark.parametrize(
        ("changed", "code"),
        [
            ({"address": "ghcr.io"}, "container_registry.address"),
            ({"address": "not a registry/x"}, "container_registry.address"),
            ({"username": " "}, "container_registry.incomplete"),
            ({"credential": None}, "container_registry.incomplete"),
            ({"ca_bundle": "not a certificate"}, "container_registry.ca_bundle_not_pem"),
        ],
    )
    async def test_an_entry_that_cannot_be_kept_is_refused_by_name(self, plane, changed, code):
        with pytest.raises(ContainerRegistryRefused) as refused:
            await _registry(plane, **changed)
        assert refused.value.code == code

    async def test_an_organisation_has_one_default_and_an_address_once(self, plane):
        await _registry(plane, default=True)
        with pytest.raises(ContainerRegistryRefused) as refused:
            await _registry(plane, address="quay.io/acme", default=True)
        assert refused.value.code == "container_registry.second_default"
        with pytest.raises(ContainerRegistryRefused) as refused:
            await _registry(plane)
        assert refused.value.code == "container_registry.address_taken"

    async def test_forgetting_removes_the_row(self, plane):
        kept = await _registry(plane)
        assert await registries.forget(plane, "acme", kept.id) is True
        assert _stored(plane) == []


class TestResolution:
    async def test_every_field_is_resolved_at_use_in_the_organisations_vault(self, plane):
        await secrets_store.put(plane, "acme", "ghcr_acme", TOKEN, owner_id=secrets_store.ORG_OWNER)
        await _registry(plane, credential="${secret:ghcr_acme}", default=True)
        entry = await registries.resolved(plane, "acme", None)
        assert (entry.host, entry.namespace, entry.username, entry.credential) == (
            "ghcr.io",
            "acme",
            "acme-bot",
            TOKEN,
        )
        assert _stored(plane)[0]["credential"] == "${secret:ghcr_acme}"

    async def test_a_build_names_a_registry_or_the_default_is_used(self, plane):
        named = await _registry(plane, address="quay.io/acme")
        assert (await registries.resolved(plane, "acme", named.id)).address == "quay.io/acme"
        with pytest.raises(ContainerRegistryRefused) as refused:
            await registries.resolved(plane, "acme", None)
        assert refused.value.code == "container_registry.none"

    async def test_a_reference_is_resolved_in_its_own_organisations_vault_only(self, plane):
        await secrets_store.put(
            plane, "globex", "ghcr_acme", "globex-token", owner_id=secrets_store.ORG_OWNER
        )
        await _registry(plane, credential="${secret:ghcr_acme}", default=True)
        with pytest.raises(ContainerRegistryRefused) as refused:
            await registries.resolved(plane, "acme", None)
        assert refused.value.code == "container_registry.unresolved"
        assert refused.value.params["field"] == "credential"

    async def test_the_vault_keeps_a_secret_a_registry_names(self, plane):
        from provisa.core import secret_references

        await secrets_store.put(plane, "acme", "ghcr_acme", "x", owner_id=secrets_store.ORG_OWNER)
        kept = await _registry(plane, credential="${secret:ghcr_acme}")
        found = await secret_references.references(plane, "acme", "ghcr_acme", environments={})
        assert [(r.table, r.column, r.id) for r in found] == [
            ("org_container_registries", "credential", ["acme", kept.id])
        ]


class TestTheAdminSurface:
    @pytest.fixture
    def wired(self, plane, monkeypatch):
        seen: dict = {"guarded": []}

        async def guard(request, org_id):
            seen["guarded"].append(org_id)
            if seen.get("refuse"):
                raise ApiError(403, "secrets.org_settings_in_org_required", "no")
            return "uid-ada"

        monkeypatch.setattr(router, "_org_guard", guard)
        monkeypatch.setattr(router, "_admin_pool", lambda: plane)
        return seen

    def _body(self, **changed):
        given = {"address": "ghcr.io/acme", "username": "acme-bot", "credential": TOKEN}
        given.update(changed)
        return router.RegistryBody(**given)

    async def test_it_is_opened_by_the_organisations_own_right_in_that_organisation(self, wired):
        added = await router.add_registry(None, "acme", self._body())
        await router.replace_registry(None, "acme", added["id"], self._body(credential=None))
        listed = await router.list_registries(None, "acme")
        assert [r["address"] for r in listed["registries"]] == ["ghcr.io/acme"]
        await router.delete_registry(None, "acme", added["id"])
        assert wired["guarded"] == ["acme"] * 4

    async def test_without_it_nothing_is_read_kept_or_removed(self, wired, plane):
        kept = await _registry(plane)
        wired["refuse"] = True
        for call in (
            lambda: router.list_registries(None, "acme"),
            lambda: router.add_registry(None, "acme", self._body(address="quay.io/x")),
            lambda: router.replace_registry(None, "acme", kept.id, self._body()),
            lambda: router.delete_registry(None, "acme", kept.id),
        ):
            with pytest.raises(ApiError) as raised:
                await call()
            assert raised.value.status_code == 403

    async def test_a_refusal_is_answered_by_name(self, wired):
        with pytest.raises(ApiError) as raised:
            await router.add_registry(None, "acme", self._body(address="ghcr.io"))
        assert raised.value.status_code == 400
