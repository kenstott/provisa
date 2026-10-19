# Copyright (c) 2026 Kenneth Stott
# Canary: 8ee02e4c-1334-4c86-a42c-67d5b414ad27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The package registries an organisation's Node installs go through (REQ-1984): npm registries
and Git hosts, entered once by the organisation's administrator, each field kept as entered and
resolved at use in that organisation's vault, and opened by ``org_settings`` in that
organisation alone. The control plane is a real PostgreSQL database; every credential is made up."""

# Requirements: REQ-1984
from __future__ import annotations

import pytest
import sqlalchemy as sa

from provisa.api.admin import package_registries_router as router
from provisa.api.errors import ApiError
from provisa.core import package_registries as registries
from provisa.core import schema_admin, secrets_store
from provisa.core.package_registries import PackageRegistryRefused

pytestmark = pytest.mark.asyncio

TOKEN = "made-up-npm-token"


class _Sealer:
    def encrypt(self, value: bytes) -> bytes:
        return b"sealed:" + value[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        return blob[len(b"sealed:") :][::-1]


@pytest.fixture
def plane(control_plane, monkeypatch):
    with control_plane.engine().begin() as conn:
        for org_id in ("acme", "globex"):  # org_package_registries' foreign key
            conn.execute(sa.insert(schema_admin.orgs).values(id=org_id, name=org_id))
    monkeypatch.setattr(secrets_store, "_cipher", lambda *, mint: _Sealer())

    async def held(conn, *, mint):
        return None

    monkeypatch.setattr(secrets_store, "_hold_the_deployments_key", held)
    return control_plane.platform


async def _npm(plane, org="acme", **changed):
    given = {
        "registry_id": None,
        "kind": "npm",
        "address": "https://npm.acme.test/",
        "scope": "@acme",
        "credential": TOKEN,
        "mirror": False,
        "actor": "uid-ada",
    }
    given.update(changed)
    return await registries.put(plane, org, **given)


async def _git(plane, org="acme", **changed):
    given = {
        "registry_id": None,
        "kind": "git",
        "address": "git.acme.test",
        "scope": None,
        "credential": "made-up-git-token",
        "mirror": False,
        "actor": "uid-ada",
    }
    given.update(changed)
    return await registries.put(plane, org, **given)


async def _pypi(plane, org="acme", **changed):
    given = {
        "registry_id": None,
        "kind": "pypi",
        "address": "https://pypi.acme.test/simple/",
        "scope": None,
        "credential": "made-up-index-token",
        "mirror": False,
        "actor": "uid-ada",
    }
    given.update(changed)
    return await registries.put(plane, org, **given)


def _stored(plane) -> list[dict]:
    with plane.engine.connect() as conn:
        return [
            dict(r._mapping) for r in conn.execute(sa.select(schema_admin.org_package_registries))
        ]


async def _vault(plane, org: str) -> dict[str, str]:
    return await secrets_store._decrypted(plane, org, secrets_store.ORG_OWNER)


class TestTheStore:
    async def test_an_organisation_that_entered_nothing_has_nothing(self, plane):
        assert await registries.entries(plane, "acme") == []
        assert await registries.resolved(plane, "acme") == []

    async def test_an_entry_is_kept_exactly_as_entered(self, plane):
        """What the administrator typed is what the entry holds: nothing is written to the
        organisation's vault for it, and no name is made up."""
        kept = await _npm(plane, credential="${secret:npm_acme}")
        assert (kept.kind, kept.address, kept.scope, kept.credential, kept.mirror) == (
            "npm",
            "https://npm.acme.test/",
            "@acme",
            "${secret:npm_acme}",
            False,
        )
        (row,) = _stored(plane)
        assert row["credential"] == "${secret:npm_acme}" and row["updated_by"] == "uid-ada"
        assert await _vault(plane, "acme") == {}

    async def test_entries_are_listed_npm_first_then_git(self, plane):
        await _git(plane)
        await _npm(plane)
        assert [e.kind for e in await registries.entries(plane, "acme")] == ["npm", "git"]

    async def test_one_organisations_entries_are_not_anothers(self, plane):
        await _npm(plane, org="acme")
        assert await registries.entries(plane, "globex") == []

    async def test_a_replace_keeps_the_credential_left_out(self, plane):
        kept = await _npm(plane)
        again = await _npm(
            plane, registry_id=kept.id, address="https://npm2.acme.test/", credential=None
        )
        assert (again.id, again.address, again.credential) == (
            kept.id,
            "https://npm2.acme.test/",
            TOKEN,
        )
        assert len(_stored(plane)) == 1

    @pytest.mark.parametrize(
        ("changed", "code"),
        [
            ({"kind": "cargo"}, "package_registry.unknown_kind"),
            ({"address": "  "}, "package_registry.incomplete"),
            ({"credential": None}, "package_registry.incomplete"),
        ],
    )
    async def test_an_entry_that_cannot_be_kept_is_refused_by_name(self, plane, changed, code):
        with pytest.raises(PackageRegistryRefused) as refused:
            await _npm(plane, **changed)
        assert refused.value.code == code
        assert _stored(plane) == []

    async def test_a_git_host_has_no_scope_and_is_never_the_mirror(self, plane):
        with pytest.raises(PackageRegistryRefused) as scoped:
            await _git(plane, scope="@acme")
        with pytest.raises(PackageRegistryRefused) as mirrored:
            await _git(plane, mirror=True)
        assert (scoped.value.code, mirrored.value.code) == (
            "package_registry.git_scope",
            "package_registry.git_mirror",
        )

    async def test_an_organisation_has_one_mirror(self, plane):
        await _npm(plane, scope=None, mirror=True, address="https://mirror.acme.test/")
        with pytest.raises(PackageRegistryRefused) as refused:
            await _npm(plane, scope=None, mirror=True, address="https://other.acme.test/")
        assert refused.value.code == "package_registry.second_mirror"
        assert refused.value.params == {"mirror": "https://mirror.acme.test/"}

    async def test_a_pypi_index_has_no_scope(self, plane):
        with pytest.raises(PackageRegistryRefused) as scoped:
            await _pypi(plane, scope="@acme")
        assert scoped.value.code == "package_registry.pypi_scope"

    async def test_npm_and_python_each_have_their_own_mirror(self, plane):
        await _npm(plane, scope=None, mirror=True, address="https://mirror.acme.test/")
        index = await _pypi(plane, mirror=True)
        assert index.mirror is True
        with pytest.raises(PackageRegistryRefused) as refused:
            await _pypi(plane, mirror=True, address="https://other.acme.test/simple/")
        assert refused.value.code == "package_registry.second_mirror"
        assert refused.value.params == {"mirror": "https://pypi.acme.test/simple/"}

    async def test_the_mirror_itself_is_replaced_in_place(self, plane):
        kept = await _npm(plane, scope=None, mirror=True)
        again = await _npm(plane, registry_id=kept.id, scope=None, mirror=True, credential=None)
        assert again.mirror is True

    async def test_a_scope_and_a_git_host_are_entered_once(self, plane):
        await _npm(plane)
        await _git(plane)
        with pytest.raises(PackageRegistryRefused) as scope:
            await _npm(plane, address="https://elsewhere.test/")
        with pytest.raises(PackageRegistryRefused) as host:
            await _git(plane)
        assert (scope.value.code, host.value.code) == (
            "package_registry.scope_taken",
            "package_registry.host_taken",
        )

    async def test_an_entry_keeps_its_kind(self, plane):
        kept = await _npm(plane)
        with pytest.raises(PackageRegistryRefused) as refused:
            await _git(plane, registry_id=kept.id)
        assert refused.value.code == "package_registry.kind_changed"

    async def test_replacing_an_entry_that_is_not_there_is_not_found(self, plane):
        with pytest.raises(PackageRegistryRefused) as refused:
            await _npm(plane, registry_id="nope")
        assert (refused.value.code, refused.value.status) == ("package_registry.not_found", 404)

    async def test_forgetting_removes_the_row_and_nothing_from_the_vault(self, plane):
        await secrets_store.put(plane, "acme", "npm_acme", "x", owner_id=secrets_store.ORG_OWNER)
        kept = await _npm(plane, credential="${secret:npm_acme}")
        assert await registries.forget(plane, "acme", kept.id) is True
        assert await registries.forget(plane, "acme", kept.id) is False
        assert await _vault(plane, "acme") == {"npm_acme": "x"}


class TestResolution:
    async def test_every_field_is_resolved_at_use_in_the_organisations_vault(
        self, plane, monkeypatch
    ):
        """The URL, the scope and the credential are each resolved by the one resolver, so any of
        them can be a secret reference; the stored row keeps what was typed."""
        monkeypatch.setenv("NPM_ACME_URL", "https://npm.acme.test/")
        await secrets_store.put(plane, "acme", "npm_acme", TOKEN, owner_id=secrets_store.ORG_OWNER)
        await secrets_store.put(
            plane, "acme", "acme_scope", "@acme", owner_id=secrets_store.ORG_OWNER
        )
        await _npm(
            plane,
            address="${env:NPM_ACME_URL}",
            scope="${secret:acme_scope}",
            credential="${secret:npm_acme}",
        )
        (entry,) = await registries.resolved(plane, "acme")
        assert (entry.address, entry.scope, entry.credential) == (
            "https://npm.acme.test/",
            "@acme",
            TOKEN,
        )
        assert _stored(plane)[0]["credential"] == "${secret:npm_acme}"

    async def test_a_value_that_is_no_reference_is_used_as_it_is(self, plane):
        await _git(plane)
        (entry,) = await registries.resolved(plane, "acme")
        assert entry.credential == "made-up-git-token"

    async def test_a_reference_is_resolved_in_its_own_organisations_vault_only(self, plane):
        await secrets_store.put(
            plane, "globex", "npm_acme", "globex-token", owner_id=secrets_store.ORG_OWNER
        )
        await _npm(plane, credential="${secret:npm_acme}")
        with pytest.raises(PackageRegistryRefused) as refused:
            await registries.resolved(plane, "acme")
        assert refused.value.code == "package_registry.unresolved"
        assert refused.value.params["field"] == "credential"

    async def test_the_vault_keeps_a_secret_a_registry_names(self, plane):
        """The Secrets screen's delete asks the vault's reference search, which finds the
        organisation's registry entry, with nothing registered for it."""
        from provisa.core import secret_references

        await secrets_store.put(plane, "acme", "npm_acme", "x", owner_id=secrets_store.ORG_OWNER)
        kept = await _npm(plane, credential="${secret:npm_acme}")
        found = await secret_references.references(plane, "acme", "npm_acme", environments={})
        assert [(r.table, r.column, r.id) for r in found] == [
            ("org_package_registries", "credential", ["acme", kept.id])
        ]
        with pytest.raises(secrets_store.SecretDeleteRefused):
            await secrets_store.remove(
                plane, "acme", "npm_acme", owner_id=secrets_store.ORG_OWNER, environments={}
            )


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
        given = {
            "kind": "npm",
            "address": "https://npm.acme.test/",
            "scope": "@acme",
            "credential": TOKEN,
        }
        given.update(changed)
        return router.RegistryBody(**given)

    async def test_it_is_opened_by_the_organisations_own_right_in_that_organisation(self, wired):
        added = await router.add_registry(None, "acme", self._body())
        await router.replace_registry(None, "acme", added["id"], self._body(credential=None))
        await router.list_registries(None, "acme")
        await router.delete_registry(None, "acme", added["id"])
        assert wired["guarded"] == ["acme"] * 4

    async def test_without_it_nothing_is_read_kept_or_removed(self, wired, plane):
        kept = await _npm(plane)
        wired["refuse"] = True
        for call in (
            lambda: router.list_registries(None, "acme"),
            lambda: router.add_registry(None, "acme", self._body(scope="@other")),
            lambda: router.replace_registry(None, "acme", kept.id, self._body(address="x")),
            lambda: router.delete_registry(None, "acme", kept.id),
        ):
            with pytest.raises(ApiError) as raised:
                await call()
            assert raised.value.status_code == 403
        assert [e.address for e in await registries.entries(plane, "acme")] == [
            "https://npm.acme.test/"
        ]

    async def test_each_field_is_answered_as_it_was_entered(self, wired):
        added = await router.add_registry(None, "acme", self._body(credential="${secret:npm_acme}"))
        (listed,) = (await router.list_registries(None, "acme"))["registries"]
        assert added == listed
        assert listed["credential"] == "${secret:npm_acme}"

    async def test_a_refusal_is_answered_with_its_code(self, wired):
        with pytest.raises(ApiError) as raised:
            await router.add_registry(None, "acme", self._body(kind="git"))
        assert (raised.value.status_code, raised.value.code) == (400, "package_registry.git_scope")

    async def test_removing_what_was_never_entered_removes_nothing(self, wired):
        assert await router.delete_registry(None, "acme", "nope") == {"removed": False}

    async def test_the_certificate_authority_is_answered_as_it_was_entered(self, wired):
        added = await router.add_registry(
            None, "acme", self._body(ca_bundle="${secret:proxy_ca}", credential="ada:made-up")
        )
        (listed,) = (await router.list_registries(None, "acme"))["registries"]
        assert added == listed
        assert (listed["ca_bundle"], listed["credential"]) == ("${secret:proxy_ca}", "ada:made-up")


# REQ-2088: an entry's certificate authority, for a proxy or Git server behind a private CA.
PEM = "-----BEGIN CERTIFICATE-----\nMIIBmade-up\n-----END CERTIFICATE-----\n"


class TestTheCertificateAuthority:
    async def test_it_is_kept_as_entered_and_resolved_at_use(self, plane):
        await secrets_store.put(plane, "acme", "proxy_ca", PEM, owner_id=secrets_store.ORG_OWNER)
        await _npm(plane, ca_bundle="${secret:proxy_ca}")
        await _pypi(plane, ca_bundle=PEM)
        await _git(plane, ca_bundle=PEM)
        assert sorted(r["ca_bundle"] for r in _stored(plane)) == [
            "${secret:proxy_ca}",
            PEM.strip(),
            PEM.strip(),
        ]
        assert {e.ca_bundle for e in await registries.resolved(plane, "acme")} == {PEM.strip(), PEM}

    async def test_an_entry_without_one_has_none(self, plane):
        kept = await _npm(plane, ca_bundle="  ")
        assert kept.ca_bundle is None
        (entry,) = await registries.resolved(plane, "acme")
        assert entry.ca_bundle is None

    async def test_a_value_that_holds_no_certificate_is_refused_by_name(self, plane):
        with pytest.raises(PackageRegistryRefused) as refused:
            await _npm(plane, ca_bundle="not a certificate")
        assert refused.value.code == "package_registry.ca_bundle_not_pem"
        assert refused.value.params == {"address": "https://npm.acme.test/"}

    async def test_a_reference_to_nothing_fails_by_name_at_use(self, plane):
        await _pypi(plane, ca_bundle="${secret:no_such_ca}")
        with pytest.raises(PackageRegistryRefused) as refused:
            await registries.resolved(plane, "acme")
        assert refused.value.code == "package_registry.unresolved"
        assert refused.value.params["field"] == "ca_bundle"
        assert "no_such_ca" in str(refused.value)

    async def test_a_reference_that_resolves_to_no_certificate_is_refused_at_use(
        self, plane, monkeypatch
    ):
        monkeypatch.setenv("PROXY_CA", "plain text")
        await _npm(plane, ca_bundle="${env:PROXY_CA}")
        with pytest.raises(PackageRegistryRefused) as refused:
            await registries.resolved(plane, "acme")
        assert refused.value.code == "package_registry.ca_bundle_not_pem"

    async def test_a_replace_without_one_removes_it(self, plane):
        kept = await _npm(plane, ca_bundle=PEM)
        replaced = await _npm(plane, registry_id=kept.id, ca_bundle=None)
        assert replaced.ca_bundle is None
