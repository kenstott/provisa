# Copyright (c) 2026 Kenneth Stott
# Canary: 0c503e04-26ac-475a-9fc0-75d34f7bff77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The mail platforms an organisation's sources sign in to (REQ-1923): entered once by the
organisation's administrator, kept per organisation with the secret in its vault, and opened by
``org_settings`` in that organisation alone. The control plane is a real SQLite file; every
credential is made up."""

# Requirements: REQ-1923
from __future__ import annotations

import pytest
import sqlalchemy as sa

from provisa.api.admin import mail_platforms_router as router
from provisa.api.errors import ApiError
from provisa.core import mail_platforms, schema_admin, secrets_store
from provisa.core.database import Database, create_engine_from_url
from provisa.core.mail_platforms import Configured, MailPlatformRefused, Platform

pytestmark = pytest.mark.asyncio

GOOGLE = "google_workspace"
TENANTED = "tenanted_platform"
SECRET = "made-up-client-secret"


class _Sealer:
    def encrypt(self, value: bytes) -> bytes:
        return b"sealed:" + value[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        return blob[len(b"sealed:") :][::-1]


@pytest.fixture
def plane(tmp_path, monkeypatch):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'plane.db'}")
    with engine.begin() as conn:
        schema_admin.metadata.create_all(
            conn,
            tables=[
                schema_admin.orgs,
                schema_admin.org_mail_platforms,
                schema_admin.secrets_store,
                schema_admin.deployment_encryption_key,
            ],
        )
    monkeypatch.setattr(secrets_store, "_cipher", lambda *, mint: _Sealer())

    async def held(conn, *, mint):
        return None

    monkeypatch.setattr(secrets_store, "_hold_the_deployments_key", held)
    monkeypatch.setitem(mail_platforms._PLATFORMS, TENANTED, Platform(TENANTED, ("tenant",)))
    return Database(engine, name="admin")


def _stored(plane) -> list[dict]:
    with plane.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sa.select(schema_admin.org_mail_platforms))]


async def _vault(plane, org: str) -> dict[str, str]:
    return await secrets_store._decrypted(plane, org, secrets_store.ORG_OWNER)


async def _put(plane, org="acme", platform=GOOGLE, **changed):
    given = {
        "client_id": "client-1",
        "client_secret": SECRET,
        "settings": {},
        "actor": "uid-ada",
        "organisation_mailboxes": False,
    }
    given.update(changed)
    return await mail_platforms.put(plane, org, platform, **given)


class TestTheStore:
    async def test_google_workspace_is_a_platform_with_nothing_beside_its_client(self):
        import provisa.google_workspace  # noqa: F401

        assert mail_platforms.platform(GOOGLE) == Platform(GOOGLE)
        assert GOOGLE in [p.id for p in mail_platforms.platforms()]

    async def test_an_organisation_that_entered_nothing_has_nothing(self, plane):
        assert await mail_platforms.read(plane, "acme", GOOGLE) is None
        with pytest.raises(MailPlatformRefused) as raised:
            await mail_platforms.require(plane, "acme", GOOGLE)
        assert raised.value.code == "mail_platform.not_configured"
        assert raised.value.params == {"platform": GOOGLE}

    async def test_the_client_is_kept_and_its_secret_goes_to_the_vault(self, plane):
        kept = await _put(plane)
        assert kept == Configured(GOOGLE, "client-1", {})
        assert kept.client_secret == "${secret:mail_platform_google_workspace_client_secret}"
        assert await mail_platforms.read(plane, "acme", GOOGLE) == kept
        (row,) = _stored(plane)
        assert SECRET not in map(str, row.values())
        assert await _vault(plane, "acme") == {
            "mail_platform_google_workspace_client_secret": SECRET
        }

    async def test_one_organisations_client_is_not_anothers(self, plane):
        await _put(plane, org="acme")
        assert await mail_platforms.read(plane, "globex", GOOGLE) is None
        assert await _vault(plane, "globex") == {}
        await _put(plane, org="globex", client_id="client-g", client_secret="made-up-other")
        assert (await mail_platforms.read(plane, "acme", GOOGLE)).client_id == "client-1"
        assert (await _vault(plane, "acme"))[
            "mail_platform_google_workspace_client_secret"
        ] == SECRET

    async def test_a_client_is_replaced_keeping_its_secret_when_none_is_given(self, plane):
        await _put(plane)
        again = await _put(plane, client_id="client-2", client_secret=None)
        assert again.client_id == "client-2"
        assert len(_stored(plane)) == 1
        assert (await _vault(plane, "acme"))[
            "mail_platform_google_workspace_client_secret"
        ] == SECRET

    async def test_a_client_is_never_kept_without_a_secret(self, plane):
        with pytest.raises(MailPlatformRefused) as raised:
            await _put(plane, client_secret=None)
        assert raised.value.code == "mail_platform.incomplete"
        assert raised.value.params == {"missing": "client_secret"}
        assert _stored(plane) == []

    async def test_a_client_needs_its_id(self, plane):
        with pytest.raises(MailPlatformRefused) as raised:
            await _put(plane, client_id="  ")
        assert raised.value.params == {"missing": "client_id"}
        assert await _vault(plane, "acme") == {}

    async def test_a_platform_takes_the_settings_it_declares_and_no_other(self, plane):
        kept = await _put(plane, platform=TENANTED, settings={"tenant": " contoso "})
        assert kept.settings == {"tenant": "contoso"}
        with pytest.raises(MailPlatformRefused) as missing:
            await _put(plane, platform=TENANTED, settings={})
        assert missing.value.params == {"missing": "tenant"}
        with pytest.raises(MailPlatformRefused) as unknown:
            await _put(plane, settings={"tenant": "contoso"})
        assert unknown.value.code == "mail_platform.unknown_setting"

    async def test_a_platform_no_source_kind_declared_is_refused(self, plane):
        with pytest.raises(MailPlatformRefused) as raised:
            await _put(plane, platform="carrier_pigeon")
        assert (raised.value.code, raised.value.status) == ("mail_platform.unknown", 404)

    async def test_forgetting_removes_only_that_organisations_client(self, plane):
        await _put(plane, org="acme")
        await _put(plane, org="globex")
        assert await mail_platforms.forget(plane, "acme", GOOGLE) is True
        assert await mail_platforms.forget(plane, "acme", GOOGLE) is False
        assert await mail_platforms.read(plane, "globex", GOOGLE) is not None


class TestTheAdminSurface:
    @pytest.fixture
    def wired(self, plane, monkeypatch):
        seen: dict = {"guarded": [], "dropped": []}

        async def guard(request, org_id):
            seen["guarded"].append(org_id)
            if seen.get("refuse"):
                raise ApiError(403, "secrets.org_settings_in_org_required", "no")
            return "uid-ada"

        async def drop(org_id, owner_id, name, actor):
            seen["dropped"].append((org_id, owner_id, name, actor))
            return {}

        from provisa.api.admin import source_sign_in_router

        monkeypatch.setattr(router, "_org_guard", guard)
        monkeypatch.setattr(router, "_admin_pool", lambda: plane)
        monkeypatch.setattr(router, "_drop", drop)
        monkeypatch.setattr(
            source_sign_in_router, "_public_address", lambda: "http://localhost:3000"
        )
        return seen

    async def test_it_is_opened_by_the_organisations_own_right_in_that_organisation(self, wired):
        await router.list_platforms(None, "acme")
        await router.put_platform(
            None,
            "acme",
            GOOGLE,
            router.PlatformBody(client_id="c", client_secret=SECRET, organisation_mailboxes=False),
        )
        await router.delete_platform(None, "acme", GOOGLE)
        assert wired["guarded"] == ["acme"] * 3

    async def test_without_it_nothing_is_read_kept_or_removed(self, wired, plane):
        await _put(plane)
        wired["refuse"] = True
        for call in (
            lambda: router.list_platforms(None, "acme"),
            lambda: router.put_platform(
                None,
                "acme",
                GOOGLE,
                router.PlatformBody(client_id="x", client_secret="y", organisation_mailboxes=False),
            ),
            lambda: router.delete_platform(None, "acme", GOOGLE),
        ):
            with pytest.raises(ApiError) as raised:
                await call()
            assert raised.value.status_code == 403
        assert (await mail_platforms.read(plane, "acme", GOOGLE)).client_id == "client-1"
        assert wired["dropped"] == []

    async def test_the_list_says_what_is_entered_and_gives_the_address_to_register(
        self, wired, plane
    ):
        before = await router.list_platforms(None, "acme")
        google = next(p for p in before["platforms"] if p["platform"] == GOOGLE)
        assert google == {
            "platform": GOOGLE,
            "settings_fields": [],
            "configured": False,
            "client_id": None,
            "settings": {},
            "organisation_mailboxes": False,
        }
        assert before["redirect_address"] == "http://localhost:3000/source-sign-in.html"
        await _put(plane)
        after = await router.list_platforms(None, "acme")
        google = next(p for p in after["platforms"] if p["platform"] == GOOGLE)
        assert (google["configured"], google["client_id"]) == (True, "client-1")

    async def test_no_answer_carries_the_secret(self, wired):
        kept = await router.put_platform(
            None,
            "acme",
            GOOGLE,
            router.PlatformBody(
                client_id="client-1", client_secret=SECRET, organisation_mailboxes=False
            ),
        )
        listed = await router.list_platforms(None, "acme")
        assert SECRET not in repr(kept) and SECRET not in repr(listed)

    async def test_a_platform_with_settings_lists_them_for_the_form(self, wired):
        listed = await router.list_platforms(None, "acme")
        tenanted = next(p for p in listed["platforms"] if p["platform"] == TENANTED)
        assert tenanted["settings_fields"] == ["tenant"]

    async def test_a_refusal_keeps_its_name(self, wired):
        with pytest.raises(ApiError) as raised:
            await router.put_platform(
                None,
                "acme",
                GOOGLE,
                router.PlatformBody(client_id="c", organisation_mailboxes=False),
            )
        assert (raised.value.code, raised.value.status_code) == ("mail_platform.incomplete", 400)

    async def test_an_unset_public_address_is_said_and_does_not_hide_the_list(
        self, wired, monkeypatch
    ):
        from provisa.api.admin import source_sign_in_router

        monkeypatch.setattr(source_sign_in_router, "_public_address", lambda: None)
        listed = await router.list_platforms(None, "acme")
        assert listed["redirect_address"] is None
        assert listed["redirect_problem"] == "source_sign_in.public_address_not_set"
        assert listed["platforms"]

    async def test_removing_takes_the_client_and_then_its_secret(self, wired, plane):
        await _put(plane)
        assert await router.delete_platform(None, "acme", GOOGLE) == {"removed": True}
        assert wired["dropped"] == [
            (
                "acme",
                secrets_store.ORG_OWNER,
                "mail_platform_google_workspace_client_secret",
                "uid-ada",
            )
        ]
        assert await mail_platforms.read(plane, "acme", GOOGLE) is None

    async def test_a_vault_that_will_not_let_the_secret_go_says_so(self, wired, plane, monkeypatch):
        await _put(plane)

        async def refused(org_id, owner_id, name, actor):
            raise ApiError(409, "secrets.still_referenced", "in use")

        monkeypatch.setattr(router, "_drop", refused)
        with pytest.raises(ApiError) as raised:
            await router.delete_platform(None, "acme", GOOGLE)
        assert raised.value.code == "secrets.still_referenced"

    async def test_the_vault_keeps_a_secret_the_client_names(self, plane):
        """The Secrets screen's delete asks the vault's reference search, which finds the
        organisation's platform entry and no other organisation's."""
        from provisa.core import secret_references

        with plane.engine.begin() as conn:  # the search reads every table of the plane
            schema_admin.metadata.create_all(conn)
        await _put(plane, org="acme")
        name = mail_platforms.secret_name(GOOGLE)
        found = await secret_references.references(plane, "acme", name, environments={})
        assert [(r.table, r.column, r.id) for r in found] == [
            ("org_mail_platforms", "client_secret", ["acme", GOOGLE])
        ]
        assert await secret_references.references(plane, "globex", name, environments={}) == []
        with pytest.raises(secrets_store.SecretDeleteRefused):
            await secrets_store.remove(
                plane, "acme", name, owner_id=secrets_store.ORG_OWNER, environments={}
            )
        await mail_platforms.forget(plane, "acme", GOOGLE)
        assert await secrets_store.remove(
            plane, "acme", name, owner_id=secrets_store.ORG_OWNER, environments={}
        )

    async def test_removing_what_was_never_entered_removes_nothing(self, wired):
        assert await router.delete_platform(None, "acme", GOOGLE) == {"removed": False}
        assert wired["dropped"] == []
