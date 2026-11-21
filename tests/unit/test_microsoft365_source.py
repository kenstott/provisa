# Copyright (c) 2026 Kenneth Stott
# Canary: 25f3747b-e88c-49b5-9726-6342563e7a4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A Microsoft 365 source as it is set up and signed in: its settings, the organisation's
client it signs in with, the addresses that carry the directory, the token taken through the
store that keeps a replaced refresh token, and what the table picker lists for it."""

# Requirements: REQ-1923

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from provisa.core import mail_platforms, source_sign_in
from provisa.core.source_sign_in import SignInRefused
from provisa.microsoft365 import SOURCE_TYPE, loader, settings
from provisa.microsoft365.settings import InvalidMicrosoft365Source, parse

TENANT = "0a1b2c3d-1111-2222-3333-444455556666"
GOOD = {
    "accounts": ["megan@contoso.com"],
    "resources": ["mail"],
}
#: A source connected before approvals were kept in the state store: it still names its
#: owner's approval by a reference of that time.
EARLIER = {**GOOD, "refresh_token": "${secret:source_m365_refresh_token}"}


# -------------------------------------------------------------------------------- settings


def test_the_settings_a_source_keeps():
    read = parse(GOOD)
    assert read.account == "megan@contoso.com" and read.resources == ("mail",)
    # The owner's approval is sign-in state and no setting of the source. Only a source
    # connected before that still names one, and it is read as it stands.
    assert read.refresh_token is None
    assert parse(EARLIER).refresh_token == EARLIER["refresh_token"]
    assert parse({**GOOD, "refresh_token": " "}).refresh_token is None
    assert read.scopes() == [
        "https://graph.microsoft.com/Mail.Read",
        "offline_access",
        "https://graph.microsoft.com/User.Read",
    ]
    assert settings.SECRET_KEYS == ("refresh_token",)


@pytest.mark.parametrize(
    ("change", "says"),
    [
        ({"accounts": []}, "one mailbox"),
        ({"accounts": ["a@b.co", "c@d.co"]}, "one mailbox"),
        ({"accounts": ["not an address"]}, "not a mailbox address"),
        ({"resources": []}, "what is read"),
        ({"resources": ["drive"]}, "cannot read drive"),
        # REQ-1994: a source that reads calendars says how many days a check looks at.
        ({"resources": ["calendar"]}, "calendar_check_days must be a whole number"),
        ({"resources": ["mail"], "calendar_check_days": 30}, "is a setting of calendars"),
        ({"refresh_token": 7}, "is text"),
        ({"mail_since": "10/01/2026"}, "mail_since must be a date as YYYY-MM-DD"),
        ({"mail_since": 20261001}, "mail_since must be a date as YYYY-MM-DD"),
        ({"client_secret": "x"}, "unknown setting"),
    ],
)
def test_settings_that_cannot_be_a_source_are_refused_by_name(change, says):
    with pytest.raises(InvalidMicrosoft365Source, match=says) as refused:
        parse({**GOOD, **change})
    assert "a-literal-token" not in str(refused.value)


def test_a_save_is_refused_in_the_setups_terms():
    from provisa.api.admin.schema_common import microsoft_365_refusal

    def source(mapping) -> SimpleNamespace:
        return SimpleNamespace(id="m365", type=SOURCE_TYPE, mapping_json=json.dumps(mapping))

    refused = microsoft_365_refusal(source({**GOOD, "accounts": []}))
    assert refused is not None and refused.code == "schema.microsoft_365_invalid"
    assert "one mailbox" in refused.message and refused.params["source"] == "m365"
    # A source is saved before or after its owner signs in: the approval is no part of it.
    assert microsoft_365_refusal(source(GOOD)) is None
    assert microsoft_365_refusal(source(EARLIER)) is None
    other = SimpleNamespace(id="pg", type="postgresql", mapping_json="{}")
    assert microsoft_365_refusal(other) is None


# --------------------------------------------------------------------------------- sign-in


def test_the_platform_and_the_sign_in_kind_carry_the_source_types_name():
    import provisa.microsoft365.sign_in  # noqa: F401  (registers the kind)

    assert mail_platforms.platform(SOURCE_TYPE).settings == ("tenant",)
    assert source_sign_in.KINDS[SOURCE_TYPE].id == SOURCE_TYPE == "microsoft_365"


@pytest.mark.parametrize("tenant", [TENANT, "contoso.onmicrosoft.com", "contoso.com"])
def test_the_addresses_carry_the_organisations_directory(tenant):
    from provisa.microsoft365.sign_in import MICROSOFT

    base = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0"
    assert MICROSOFT.authorization_endpoint({"tenant": tenant}) == f"{base}/authorize"
    assert MICROSOFT.token_endpoint({"tenant": tenant}) == f"{base}/token"


@pytest.mark.parametrize("tenant", ["", "common/../x", "a b", "evil.example/path?x=", "x", None])
def test_only_a_directory_id_or_domain_goes_into_an_address(tenant):
    from provisa.microsoft365.sign_in import MICROSOFT

    stated = {} if tenant is None else {"tenant": tenant}
    with pytest.raises(SignInRefused) as refused:
        MICROSOFT.token_endpoint(stated)
    assert refused.value.code == "source_sign_in.tenant_invalid"


def test_the_person_is_offered_the_account_the_source_names():
    from provisa.microsoft365.sign_in import MICROSOFT

    assert MICROSOFT.authorization_params("megan@contoso.com") == {
        "login_hint": "megan@contoso.com",
        "prompt": "select_account",
    }


# ------------------------------------------------------------------------- the connection


@pytest.fixture
def signed_in(monkeypatch, control_plane):
    """What `connect` reaches: the organisation's client, its vault, its state store (where
    the owner's approval is kept) and the exchange."""
    from contextlib import asynccontextmanager

    from provisa.api_source import oauth_grants, oauth_store
    from provisa.core import config_loader, config_location, request_context, secrets_store

    calls = SimpleNamespace(required=[], exchanged=[], resolved=[], bound=[])
    calls.state = SimpleNamespace(admin_db=object(), tenant_db=control_plane.org())
    oauth_store._held.clear()
    monkeypatch.setattr(config_location, "config_path_str", lambda: "provisa.yaml")
    monkeypatch.setattr(
        config_loader,
        "load_control_plane",
        lambda path: SimpleNamespace(resolved_platform_url=lambda: control_plane.url),
    )

    async def require(admin_db, org_id, platform_id):
        calls.required.append((org_id, platform_id))
        return mail_platforms.Configured(
            platform_id, "client-id", {"tenant": TENANT}, client_secret="as-entered"
        )

    def exchange_refresh_token(auth):
        calls.exchanged.append(auth)
        # Microsoft answers every exchange with a newer approval.
        return oauth_grants.RefreshGrant(
            f"access-{len(calls.exchanged)}", calls.__dict__.get("lifetime", 3600.0), "rt-newer"
        )

    async def resolved(admin_db, org_id, platform_id, field, value):
        # The entry's secret, read in the vault of the organisation it is asked for.
        calls.resolved.append((org_id, platform_id, field, value))
        return "the-client-secret"

    @asynccontextmanager
    async def bound(admin_db, org_id, **kw):
        calls.bound.append(org_id)
        yield

    monkeypatch.setattr(mail_platforms, "require", require)
    monkeypatch.setattr(oauth_grants, "exchange_refresh_token", exchange_refresh_token)
    monkeypatch.setattr(mail_platforms, "credential", resolved)
    monkeypatch.setattr(secrets_store, "bound", bound)
    monkeypatch.setattr(request_context, "require_current_org", lambda: "acme")
    yield calls
    oauth_store._held.clear()


async def _kept(state) -> str | None:
    from provisa.federation import mail_sync_state

    async with state.tenant_db.acquire() as conn:
        return await mail_sync_state.grant(conn, "m365", "megan@contoso.com")


async def _keep(state, token: str) -> None:
    from provisa.federation import mail_sync_state

    async with state.tenant_db.acquire() as conn:
        await mail_sync_state.keep_grant(conn, "m365", "megan@contoso.com", token)


def test_a_source_is_signed_in_with_the_organisations_client_and_its_owners_approval(signed_in):
    state = signed_in.state
    source = SimpleNamespace(id="m365", mapping=GOOD)

    async def run():
        await _keep(state, "rt-from-the-sign-in")
        reading = await loader.make_connect(state)(source)
        assert reading.organisation is False and reading.at_once == 1
        (account,) = reading.accounts
        first = await asyncio.to_thread(reading.token)  # Graph is read off the loop
        again = await asyncio.to_thread(reading.token)
        return account, first, again, await _kept(state)

    account, token, again, kept = asyncio.run(run())
    assert (account, token, again) == ("megan@contoso.com", "access-1", "access-1")
    assert signed_in.required == [("acme", SOURCE_TYPE)]
    # The client's secret as its administrator entered it, resolved in that organisation's vault.
    assert signed_in.resolved == [("acme", SOURCE_TYPE, "client_secret", "as-entered")]
    assert signed_in.bound == []  # the approval is state: no vault is read for it
    (auth,) = signed_in.exchanged  # exchanged once; the access token is held to its end
    assert auth.refresh_token == "rt-from-the-sign-in"  # what the state store kept
    assert auth.client_id == "client-id" and auth.client_secret == "the-client-secret"
    assert auth.token_url == f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"
    assert auth.scope == (
        "https://graph.microsoft.com/Mail.Read offline_access https://graph.microsoft.com/User.Read"
    )
    # The newer approval Microsoft answered with is what the next exchange sends.
    assert kept == "rt-newer"


def test_each_renewal_sends_the_approval_the_one_before_it_was_answered_with(signed_in):
    """Microsoft replaces the approval at every use; a source goes on reading past the first
    one's lifetime because each replacement is kept."""
    state, signed_in.lifetime = signed_in.state, 0.0  # no access token is held: every read renews

    async def run():
        await _keep(state, "rt-from-the-sign-in")
        reading = await loader.make_connect(state)(SimpleNamespace(id="m365", mapping=GOOD))
        return [await asyncio.to_thread(reading.token) for _ in range(2)]

    assert asyncio.run(run()) == ["access-1", "access-2"]
    assert [auth.refresh_token for auth in signed_in.exchanged] == [
        "rt-from-the-sign-in",
        "rt-newer",
    ]


def test_a_source_connected_the_earlier_way_still_reads_with_nothing_done_to_it(
    signed_in, monkeypatch
):
    """Its settings name the approval by a reference of that time, still in the organisation's
    vault. The first read resolves it there, in the source's own organisation, and keeps it in
    the state store; the reference is not read again."""
    from provisa.core import secrets as core_secrets

    state = signed_in.state
    seen: list[str] = []

    def resolve(value: str, **_marked: object) -> str:
        seen.append(value)
        return "rt-the-vault-held"

    monkeypatch.setattr(core_secrets, "resolve_secrets", resolve)

    async def run():
        reading = await loader.make_connect(state)(SimpleNamespace(id="m365", mapping=EARLIER))
        first = await asyncio.to_thread(reading.token)
        return first, await _kept(state)

    assert asyncio.run(run()) == ("access-1", "rt-newer")
    assert seen == [EARLIER["refresh_token"]] and signed_in.bound == ["acme"]
    assert signed_in.exchanged[0].refresh_token == "rt-the-vault-held"


def test_an_organisation_with_no_client_is_refused_by_name(signed_in, monkeypatch):
    async def require(admin_db, org_id, platform_id):
        raise mail_platforms.MailPlatformRefused("not_configured", "no client entered")

    monkeypatch.setattr(mail_platforms, "require", require)
    connect = loader.make_connect(SimpleNamespace(admin_db=object()))
    with pytest.raises(mail_platforms.MailPlatformRefused):
        asyncio.run(connect(SimpleNamespace(id="m365", mapping=GOOD)))
    assert signed_in.exchanged == []  # nothing was asked of Microsoft


def test_a_source_that_is_not_connected_is_refused_by_name_and_asks_nothing_of_microsoft(signed_in):
    from provisa.api_source.oauth_store import NotConnected

    async def run():
        reading = await loader.make_connect(signed_in.state)(
            SimpleNamespace(id="m365", mapping=GOOD)
        )
        await asyncio.to_thread(reading.token)

    with pytest.raises(NotConnected, match="megan@contoso.com is not connected"):
        asyncio.run(run())
    assert signed_in.exchanged == []


# ------------------------------------------------------------------------ the table picker


class _Stored:
    """The control plane as the picker reads a source's settings from it."""

    def __init__(self, mapping: dict) -> None:
        self._mapping = mapping

    async def execute_core(self, _statement):
        return SimpleNamespace(scalar_one=lambda: self._mapping)


@pytest.mark.parametrize(
    ("resources", "offered"),
    [
        (["mail"], loader.MAIL_TABLES),
        # REQ-1994: the calendar and task tables of a source that reads them.
        (["calendar"], ("calendars", "events", "event_attendees")),
        (["mail", "calendar", "tasks"], loader.TABLES),
    ],
)
def test_the_table_picker_lists_one_schema_and_the_tables_of_what_is_read(resources, offered):
    from provisa.api.admin.introspect import native_schemas, native_tables
    from provisa.core import canonical_mail as cm

    stored = _Stored({"resources": resources})

    async def run():
        schemas = await native_schemas("m365", SOURCE_TYPE, None, None)  # type: ignore[arg-type]
        tables = await native_tables("m365", SOURCE_TYPE, "default", None, stored, None)  # type: ignore[arg-type]
        elsewhere = await native_tables("m365", SOURCE_TYPE, "other", None, stored, None)  # type: ignore[arg-type]
        return schemas, tables, elsewhere

    schemas, tables, elsewhere = asyncio.run(run())
    assert schemas == ["default"]
    assert [t.name for t in tables] == list(offered)
    assert [t.comment for t in tables] == [cm.TABLES[name].note for name in offered]
    assert elsewhere == []


def test_the_loader_is_given_to_the_product():
    import inspect

    from provisa.events import app_wiring

    assert 'loaders["microsoft_365"] = make_microsoft365_loader(make_connect(state))' in (
        inspect.getsource(app_wiring)
    )


def test_the_column_picker_marks_each_tables_key_and_offers_when_a_row_landed(monkeypatch):
    """A mail table is kept by key (rows are added, changed and removed in place), so the
    picker marks its key columns; and it ends with the landing's own stamp, the table's
    watermark for live delivery."""
    from provisa.api.admin import schema_query
    from provisa.core import canonical_mail as cm

    src = inspect_source(schema_query)
    # Both mail kinds; a calendar or task table marks what a write names its rows by (REQ-1994).
    assert src.count("is_primary_key=name in identity_columns(table_name)") == 2
    assert src.count("for name, ir_type in landing_columns(table_name)") == 2
    assert cm.landing_columns("messages")[-1] == (cm.LANDED_AT, "timestamp")


def inspect_source(module) -> str:
    import inspect

    return inspect.getsource(module)


def test_a_whole_build_stamps_every_row_with_when_it_landed(signed_in, monkeypatch):
    """One whole build writes its copy at once: every row of every table carries one stamp."""
    from provisa.core import canonical_mail as cm

    async def read_mailboxes(reading, per_mailbox, *rest):
        yield "messages", [{"id": "m1"}, {"id": "m2"}]
        yield "message_recipients", [{"message_id": "m1"}]

    async def connect(source):
        return SimpleNamespace(keep=None)

    monkeypatch.setattr(loader, "_read_mailboxes", read_mailboxes)
    columns = {
        "messages": [("id", "text"), (cm.LANDED_AT, "timestamp")],
        "message_recipients": [("message_id", "text"), (cm.LANDED_AT, "timestamp")],
    }
    read = loader._MessagesRead(connect, SimpleNamespace(id="m365"), tuple(columns), columns)

    async def run():
        return [(table, batch.to_pylist()) async for table, batch in read.batches(100)]

    landed = asyncio.run(run())
    stamps = {row[cm.LANDED_AT] for _table, rows in landed for row in rows}
    assert len(stamps) == 1 and None not in stamps
    assert [table for table, _ in landed] == ["messages", "message_recipients"]


def test_a_source_may_state_a_date_before_which_nothing_received_is_held():
    """``mail_since``, as a Google Workspace source states it: a date as YYYY-MM-DD, for one
    mailbox or the organisation's. Left out, or empty, the source holds mail of any age."""
    import datetime as dt

    from provisa.google_workspace import settings as google

    assert parse(GOOD).since is None
    assert parse({**GOOD, "mail_since": ""}).since is None
    assert parse({**GOOD, "mail_since": "2026-10-01"}).since == dt.date(2026, 10, 1)
    organisation = {"resources": ["mail"], "mailboxes": {"everyone": True}}
    assert parse({**organisation, "mail_since": " 2026-10-01 "}).since == dt.date(2026, 10, 1)
    # The same name the Google source's setting has.
    assert "mail_since" in google._MAIL_KEYS
