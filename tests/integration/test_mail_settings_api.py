# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8a6c14-9b27-4f0d-a5e3-6d81f2b4c709
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1576: the platform mail surface -- configure the transport, see whether mail is going out.

The endpoints are exercised against the real app so the guard, the registry and the REQ-1575
redaction are the ones that ship. Writes go to a temporary config file: the point under test is
what the endpoint persists and rebinds, not the maintainer's provisa.yaml.
"""

import os
from pathlib import Path

import pytest
import pytest_asyncio
import yaml
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    """Point the endpoint's config read/write at a throwaway file."""
    from provisa.api.admin import mail_router

    path = Path(tmp_path) / "provisa.yaml"
    path.write_text(yaml.dump({"mail": {"provider": "smtp", "smtp": {"host": "relay.internal"}}}))
    monkeypatch.setattr(mail_router, "config_path", lambda: path)
    monkeypatch.setattr(mail_router, "read_config", lambda: yaml.safe_load(path.read_text()) or {})
    return path


class TestWhatTheSurfaceOffers:
    async def test_every_transport_is_listed_with_its_form(self, client):
        body = (await client.get("/admin/mail")).json()
        keys = [p["key"] for p in body["providers"]]
        assert {"smtp", "resend", "sendgrid", "mailgun", "postmark", "ses", "microsoft365"} <= set(
            keys
        )
        for provider in body["providers"]:
            assert provider["label"] and provider["config_fields"]

    async def test_an_unavailable_transport_is_listed_saying_why(self, client):
        """Hiding it would answer "can we send through SES?" with silence; the row plus
        ``requires`` answers it with "yes, once boto3 is installed"."""
        body = (await client.get("/admin/mail")).json()
        for provider in body["providers"]:
            if not provider["available"]:
                assert provider["requires"]

    async def test_the_shared_delivery_settings_come_back(self, client):
        body = (await client.get("/admin/mail")).json()
        for key in ("provider", "from_address", "base_url", "timeout_seconds"):
            assert key in body


class TestCredentialsNeverComeBackOut:
    async def test_no_stored_secret_is_returned(self, client):  # REQ-1575
        body = (await client.get("/admin/mail")).json()
        for provider in body["providers"]:
            secret = {f["config_key"] for f in provider["config_fields"] if f.get("secret")}
            assert not (secret & set(body["config"].get(provider["key"], {})))
            assert set(body["secret_set"].get(provider["key"], {})) == secret

    async def test_secret_set_reports_only_the_bit(self, client, config_file):
        config_file.write_text(
            yaml.dump({"mail": {"provider": "resend", "resend": {"api_key": "re_live_secret"}}})
        )
        body = (await client.get("/admin/mail")).json()
        assert body["secret_set"]["resend"]["api_key"] is True
        assert "api_key" not in body["config"]["resend"]
        assert "re_live_secret" not in (await client.get("/admin/mail")).text


class TestSelectingATransport:
    async def test_the_selection_and_its_fields_are_persisted(self, client, config_file):
        resp = await client.put(
            "/admin/mail",
            json={
                "provider": "postmark",
                "from_address": "invites@example.test",
                "config": {"server_token": "pm-token", "message_stream": "outbound"},
            },
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"success": True, "provider": "postmark"}
        saved = yaml.safe_load(config_file.read_text())["mail"]
        assert saved["provider"] == "postmark"
        assert saved["from_address"] == "invites@example.test"
        assert saved["postmark"]["server_token"] == "pm-token"

    async def test_an_absent_secret_leaves_the_stored_one_alone(self, client, config_file):
        """The form cannot echo back a value it was never given (REQ-1575), so "not submitted"
        has to mean "unchanged" -- otherwise saving an unrelated field wipes the credential."""
        config_file.write_text(
            yaml.dump(
                {"mail": {"provider": "postmark", "postmark": {"server_token": "pm-existing"}}}
            )
        )
        await client.put(
            "/admin/mail",
            json={"provider": "postmark", "config": {"message_stream": "broadcast"}},
        )
        saved = yaml.safe_load(config_file.read_text())["mail"]["postmark"]
        assert saved["server_token"] == "pm-existing"
        assert saved["message_stream"] == "broadcast"

    async def test_an_empty_secret_clears_it(self, client, config_file):
        config_file.write_text(
            yaml.dump(
                {"mail": {"provider": "postmark", "postmark": {"server_token": "pm-existing"}}}
            )
        )
        await client.put(
            "/admin/mail", json={"provider": "postmark", "config": {"server_token": ""}}
        )
        assert yaml.safe_load(config_file.read_text())["mail"]["postmark"]["server_token"] == ""

    async def test_an_unknown_transport_is_refused(self, client, config_file):
        resp = await client.put("/admin/mail", json={"provider": "sendgird", "config": {}})
        assert resp.status_code == 400
        assert yaml.safe_load(config_file.read_text())["mail"]["provider"] == "smtp"

    async def test_a_field_the_transport_does_not_declare_is_not_written(self, client, config_file):
        await client.put(
            "/admin/mail",
            json={"provider": "resend", "config": {"api_key": "re_k", "nonsense": "x"}},
        )
        assert "nonsense" not in yaml.safe_load(config_file.read_text())["mail"]["resend"]

    async def test_the_running_process_sends_through_the_new_transport(self, client, config_file):
        """Persisting alone would leave the deployment sending through the old transport until a
        restart -- which is the manual step this surface exists to remove."""
        from provisa.api.app import state
        from provisa.core.mail import PostmarkEmailSender, email_sender

        await client.put(
            "/admin/mail", json={"provider": "postmark", "config": {"server_token": "pm-token"}}
        )
        assert state.config.mail.provider == "postmark"
        assert isinstance(email_sender(state.config.mail), PostmarkEmailSender)


class TestIsMailGoingOut:
    async def test_the_stats_report_the_record_not_the_config(self, client):
        body = (await client.get("/admin/mail/stats")).json()
        assert set(body) == {"total", "windows", "last_success", "last_failure", "recent"}
        assert set(body["windows"]) == {"day", "week"}
        assert set(body["total"]) == {"attempted", "delivered", "failed"}

    async def test_a_test_send_needs_a_recipient(self, client):
        resp = await client.post("/admin/mail/test", json={})
        assert resp.status_code == 400

    async def test_a_failing_test_send_answers_with_the_transport_words(self, client, config_file):
        """A test whose failure came back as a 500 would tell the operator that the SURFACE is
        broken. The request succeeded; the transport refused, and its refusal is the answer."""
        await client.put(
            "/admin/mail",
            json={"provider": "smtp", "config": {"host": "127.0.0.1", "port": 1}},
        )
        resp = await client.post("/admin/mail/test", json={"to": "ops@example.test"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is False
        assert body["error"]

    async def test_a_failed_test_send_is_on_the_record(self, client, config_file):
        await client.put(
            "/admin/mail",
            json={"provider": "smtp", "config": {"host": "127.0.0.1", "port": 1}},
        )
        await client.post("/admin/mail/test", json={"to": "ops@example.test"})
        stats = (await client.get("/admin/mail/stats")).json()
        assert stats["last_failure"] is not None
        assert stats["last_failure"]["kind"] == "test"
        assert stats["last_failure"]["recipient"] == "ops@example.test"
        assert stats["last_failure"]["error"]
