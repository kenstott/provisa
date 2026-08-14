# Copyright (c) 2026 Kenneth Stott
# Canary: baa8bdb7-73f2-42e7-b07b-b5034600007f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1310/REQ-1330: an invitation addressed to an email address reaches that address.

The assertions run against a real SMTP server this module starts on a loopback port — a mocked send
would prove only that a call was made, not that a message a person could act on came out the other
end. The strongest test here takes the link out of the delivered body, feeds it back through
redemption, and shows the invitee lands in the org: the message is the whole onboarding path, not a
notification about one.
"""

from __future__ import annotations

import os
import re
from email import policy as email_policy
from email import message_from_bytes
from types import SimpleNamespace

import pytest
from aiosmtpd.controller import Controller
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

from provisa.api.admin.invites_router import router as invites_router
from provisa.api.auth_router import router as auth_router
from provisa.api.org_runtime import ActiveOrgPool
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import MailConfig
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import orgs, user_org_memberships
from provisa.core.schema_org import admin_audit_log, query_audit_log
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_invmail_admin"
_ORG_SCHEMAS = {"root": "test_invmail_root", "acme": "test_invmail_acme"}
_TENANT_TABLES = [roles, user_role_assignments, admin_audit_log, query_audit_log]

# REQ-1297 seeds these as system template roles in every org schema. REQ-1337: the invite gates read
# the RIGHTS these rows carry — an empty capability list authorizes nothing however the row is named
# — so the capabilities schema.sql seeds are mirrored here.
_SEEDED_ROLE_CAPS: dict[str, list[str]] = {
    "platform_admin": ["admin", "superadmin", "platform_settings", "cross_org"],
    "org_admin": ["user_management", "source_registration", "access_config", "query_development"],
    "analyst": ["usage", "query_development"],
}

# alice administers acme. carol is the invitee — she has no account, no membership, and no way to
# learn about the invitation except the message.
_TOKENS = {"tok-alice": "alice", "tok-carol": "carol"}


class _Sink:
    """Captures what the server received. ``handle_DATA`` is aiosmtpd's delivery hook."""

    def __init__(self) -> None:
        self.messages: list[SimpleNamespace] = []

    async def handle_DATA(self, server, session, envelope):  # noqa: N802 — aiosmtpd's hook name
        # Parsed and decoded, which is what the invitee's mail client does: the transport encodes
        # the body quoted-printable, so the raw octets contain neither the URL nor the long lines
        # as written.
        parsed = message_from_bytes(envelope.content, policy=email_policy.default)
        self.messages.append(
            SimpleNamespace(
                mail_from=envelope.mail_from,
                rcpt_tos=list(envelope.rcpt_tos),
                subject=parsed["Subject"],
                content=parsed.get_content(),
            )
        )
        return "250 Message accepted for delivery"


@pytest.fixture
def smtp():
    """A real SMTP server on a free loopback port.

    The port is claimed and released here rather than passed as 0, because the controller's
    readiness probe dials the port it was given and cannot dial an unbound 0.
    """
    import socket as _socket

    with _socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    sink = _Sink()
    controller = Controller(sink, hostname="127.0.0.1", port=port)
    controller.start()
    yield SimpleNamespace(sink=sink, host=controller.hostname, port=controller.port)
    controller.stop()


def _prepare_sync():
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, *_ORG_SCHEMAS.values()):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA {schema}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="root", name="Root", created_by="alice"))
        conn.execute(insert(orgs).values(id="acme", name="Acme Analytics", created_by="alice"))
        conn.execute(insert(user_org_memberships).values(user_id="alice", org_id="acme"))

        for schema in _ORG_SCHEMAS.values():
            conn.execute(text(f"SET search_path TO {schema}"))
            org_metadata.create_all(conn, tables=_TENANT_TABLES)
            for role_id, caps in _SEEDED_ROLE_CAPS.items():
                conn.execute(insert(roles).values(id=role_id, capabilities=caps))

        conn.execute(text(f"SET search_path TO {_ORG_SCHEMAS['acme']}"))
        conn.execute(
            insert(user_role_assignments).values(user_id="alice", role_id="org_admin", domain_id="*")
        )
    return engine


@pytest.fixture
def planes(monkeypatch, smtp):
    try:
        sync_engine = _prepare_sync()
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    org_dbs = {
        org_id: Database(create_engine_from_url(_ASYNC_URL), name=org_id, search_path=schema)
        for org_id, schema in _ORG_SCHEMAS.items()
    }

    from provisa.api.app import state as app_state
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "org_id", "root", raising=False)

    # REQ-1337: the runtime's roles registry is where a role id becomes the rights it carries, and
    # every invite gate reads those rights. In a real process it comes from the schema.sql seed;
    # here it mirrors the capability lists written into the role rows above.
    loaded_roles = {rid: {"id": rid, "capabilities": caps} for rid, caps in _SEEDED_ROLE_CAPS.items()}
    registry = OrgRegistry()
    for org_id, db in org_dbs.items():
        registry.set(org_id, OrgRuntime(org_id=org_id, tenant_db=db, roles=dict(loaded_roles)))
    monkeypatch.setattr(app_state, "org_registry", registry, raising=False)

    async def _org_runtime(org_id: str):
        return registry.get(org_id)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)

    mail = MailConfig(
        host=smtp.host,
        port=smtp.port,
        from_address="provisa@example.test",
        base_url="https://provisa.example.test",
    )
    monkeypatch.setattr(
        app_state, "config", SimpleNamespace(mail=mail, multitenancy=True), raising=False
    )

    yield SimpleNamespace(admin_db=admin_db, org_dbs=org_dbs, sync=sync_engine, mail=mail)

    with sync_engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, *_ORG_SCHEMAS.values()):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    sync_engine.dispose()


def _make_app(planes) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider(_TOKENS),
        admin_pool=planes.admin_db,
        db_pool=ActiveOrgPool(),
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id="root",
    )
    app.include_router(invites_router)
    app.include_router(auth_router)
    return app


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _create_invite(client, **body) -> dict:
    resp = client.post(
        "/admin/invites/", json={"org_id": "acme", **body}, headers=_auth("tok-alice")
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_an_addressed_invitation_is_delivered(planes, smtp):
    """The message reaches the address, from the configured sender."""
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="Carol@Example.Test", role_id="analyst")

    assert invite["delivery"] == "sent"
    assert len(smtp.sink.messages) == 1
    msg = smtp.sink.messages[0]
    assert msg.rcpt_tos == ["carol@example.test"]
    assert msg.mail_from == "provisa@example.test"


def test_the_message_names_org_inviter_role_and_expiry(planes, smtp):
    """Everything the invitee needs to decide is in the message — including the expiry, which runs
    against a clock they otherwise cannot see."""
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test", role_id="analyst")

    body = smtp.sink.messages[0].content
    assert "Acme Analytics" in body  # the org, by display name
    assert "alice" in body  # who invited them
    assert "analyst" in body  # the role they will hold
    assert invite["expires_at"][:10] in body  # the expiry date
    assert f"https://provisa.example.test/?invite={invite['token']}" in body


def test_the_delivered_link_redeems_the_invitation(planes, smtp):
    """The link is the onboarding path, not a notice about one: the invitee follows it and is a
    member of the org afterward, in both planes."""
    with TestClient(_make_app(planes)) as client:
        _create_invite(client, email="carol@example.test", role_id="analyst")
        body = smtp.sink.messages[0].content
        match = re.search(r"\?invite=([0-9a-f-]{36})", body)
        assert match is not None, body
        token = match.group(1)

        redeemed = client.post(
            "/auth/redeem-invite", json={"token": token}, headers=_auth("tok-carol")
        )
        assert redeemed.status_code == 200, redeemed.text

    with planes.sync.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        memberships = conn.execute(
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == "carol"
            )
        ).fetchall()
        assert [r[0] for r in memberships] == ["acme"]

        conn.execute(text(f"SET search_path TO {_ORG_SCHEMAS['acme']}"))
        assigned = conn.execute(
            select(user_role_assignments.c.role_id).where(
                user_role_assignments.c.user_id == "carol"
            )
        ).fetchall()
        assert [r[0] for r in assigned] == ["analyst"]


def test_a_link_invitation_sends_nothing(planes, smtp):
    """No address, nothing to send — the org_admin distributes this one themselves."""
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client)

    assert invite["email"] is None
    assert invite["delivery"] == "not_addressed"
    assert smtp.sink.messages == []


def test_a_delivery_failure_is_reported_and_the_invitation_survives(planes, smtp, monkeypatch):
    """A mail-server problem must not destroy a usable invitation — the link still works, and the
    org_admin is told delivery failed so they can send it by hand."""
    monkeypatch.setattr(planes.mail, "port", 1, raising=False)  # nothing listens on port 1
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test")
        assert invite["delivery"].startswith("failed: "), invite["delivery"]

        redeemed = client.post(
            "/auth/redeem-invite", json={"token": invite["token"]}, headers=_auth("tok-carol")
        )
        assert redeemed.status_code == 200, redeemed.text


def test_no_mail_host_configured_is_reported_not_silent(planes, smtp, monkeypatch):
    """An invitation that reaches nobody is the same as no invitation, so the refusal is surfaced in
    the response rather than logged and forgotten."""
    monkeypatch.setattr(planes.mail, "host", "", raising=False)
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test")

    assert "No SMTP host is configured" in invite["delivery"]
    assert smtp.sink.messages == []


# --- REQ-1330: EmailSender port — SaaS-only gating and the provider adapter ---


def test_self_hosted_deployments_send_no_email(planes, smtp, monkeypatch):
    """REQ-1330: outbound mail exists only in SaaS mode. A self-hosted deployment creates the
    invitation and reports saas_only — the row and its link stay valid for out-of-band delivery —
    and no transport is touched even though SMTP is fully configured."""
    from provisa.api.app import state as app_state

    monkeypatch.setattr(
        app_state,
        "config",
        SimpleNamespace(mail=planes.mail, multitenancy=False),
        raising=False,
    )
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test", role_id="analyst")
        assert invite["delivery"] == "saas_only"

        redeemed = client.post(
            "/auth/redeem-invite", json={"token": invite["token"]}, headers=_auth("tok-carol")
        )
        assert redeemed.status_code == 200, redeemed.text

    assert smtp.sink.messages == []


@pytest.fixture
def resend_api():
    """A real HTTP server standing in for api.resend.com — the adapter's full request (auth
    header, JSON shape) is what a provider would receive, not what a mock recorded."""
    import json as _json
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    received: list[SimpleNamespace] = []

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 — http.server's hook name
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append(
                SimpleNamespace(
                    path=self.path,
                    authorization=self.headers.get("Authorization"),
                    payload=_json.loads(body),
                )
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"id": "test"}')

        def log_message(self, format, *args):  # noqa: A002 — http.server's signature
            pass

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield SimpleNamespace(
        url=f"http://127.0.0.1:{server.server_address[1]}/emails", received=received
    )
    server.shutdown()
    thread.join()


def test_resend_adapter_delivers_through_the_port(planes, smtp, resend_api, monkeypatch):
    """REQ-1330: switching mail.provider to resend re-routes delivery through the Resend adapter —
    no call-site change — and the request carries the key, the sender and the redemption link."""
    monkeypatch.setattr(planes.mail, "provider", "resend", raising=False)
    monkeypatch.setattr(planes.mail, "api_key", "re_test_key", raising=False)
    monkeypatch.setattr(planes.mail, "api_url", resend_api.url, raising=False)
    monkeypatch.setattr(planes.mail, "from_address", "invites@provisa.dev", raising=False)

    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="Carol@Example.Test", role_id="analyst")

    assert invite["delivery"] == "sent"
    assert smtp.sink.messages == []  # the SMTP adapter was never involved
    assert len(resend_api.received) == 1
    req = resend_api.received[0]
    assert req.authorization == "Bearer re_test_key"
    assert req.payload["from"] == "invites@provisa.dev"
    assert req.payload["to"] == ["carol@example.test"]
    assert f"https://provisa.example.test/?invite={invite['token']}" in req.payload["text"]


def test_resend_without_api_key_is_reported_not_silent(planes, smtp, monkeypatch):
    """Same refusal contract as the SMTP switch: the unset key surfaces in the response."""
    monkeypatch.setattr(planes.mail, "provider", "resend", raising=False)
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test")

    assert "No Resend API key is configured" in invite["delivery"]
    assert smtp.sink.messages == []


def test_an_unknown_provider_is_a_named_config_fault(planes, smtp, monkeypatch):
    """A typo in mail.provider must name the setting, not vanish into a generic failure."""
    monkeypatch.setattr(planes.mail, "provider", "sendgrid", raising=False)
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test")

    assert "Unknown mail provider 'sendgrid'" in invite["delivery"]
    assert smtp.sink.messages == []


def test_an_empty_provider_is_the_unconfigured_refusal(planes, smtp, monkeypatch):
    """Compose overlays interpolate PROVISA_MAIL_PROVIDER as "" on nodes that set none; that is
    the unconfigured state and gets the same named refusal as an unset SMTP host."""
    monkeypatch.setattr(planes.mail, "provider", "", raising=False)
    with TestClient(_make_app(planes)) as client:
        invite = _create_invite(client, email="carol@example.test")

    assert "No mail provider is configured" in invite["delivery"]
    assert smtp.sink.messages == []
