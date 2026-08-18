# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2c46f1-90a7-4e53-b6d8-27fa04c9e315
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1330: outbound mail goes through the port, and an unconfigured transport refuses loudly.

Two failure modes are worth pinning. A deployment with no mail configured must REFUSE to send —
an invitation that is silently dropped looks identical to one the recipient ignored, and the
admin has no way to tell. And the provider must stay a deployment detail: the moment a call site
imports Resend or smtplib directly, swapping transports stops being a config change.

The structural half of that is checked by reading the source of the call sites, because the
guarantee is "nobody imports it", which no amount of exercising one call site can show.
"""

# Requirements: REQ-1310, REQ-1330, REQ-1485, REQ-1486

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.core.mail import (
    MailMessage,
    MailNotConfiguredError,
    ResendEmailSender,
    SmtpEmailSender,
    email_sender,
)
from provisa.core.models import MailConfig

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_an_unconfigured_provider_is_refused_at_construction():
    """Named at the setting, before a caller holds a sender that cannot send."""
    with pytest.raises(MailNotConfiguredError, match="mail.provider"):
        email_sender(MailConfig(provider=""))


def test_an_unknown_provider_is_refused_rather_than_defaulted():
    with pytest.raises(MailNotConfiguredError):
        email_sender(MailConfig(provider="carrier-pigeon"))


@pytest.mark.parametrize(
    ("provider", "expected"),
    [("smtp", SmtpEmailSender), ("resend", ResendEmailSender)],
)
def test_each_configured_provider_resolves_to_its_adapter(provider, expected):
    assert isinstance(email_sender(MailConfig(provider=provider)), expected)


def test_resend_without_a_key_refuses_at_send_rather_than_dropping_the_message():
    """The API key is the SaaS transport's whole credential. Sending without it has to fail in a
    way the admin sees, not return quietly."""
    sender = ResendEmailSender(MailConfig(provider="resend", api_key=""))

    with pytest.raises(MailNotConfiguredError, match="mail.api_key"):
        sender.send(MailMessage(to="a@example.com", subject="hi", body="<p>hi</p>"))


def test_smtp_without_a_host_refuses_at_send():
    sender = SmtpEmailSender(MailConfig(provider="smtp", host=""))

    with pytest.raises(MailNotConfiguredError):
        sender.send(MailMessage(to="a@example.com", subject="hi", body="<p>hi</p>"))


def test_no_application_code_reaches_a_mail_transport_directly():
    """The port exists so a provider swap is a config change. A call site importing smtplib or
    the Resend client makes that false, and nothing else in the suite would notice."""
    offenders = []
    for path in (_REPO_ROOT / "provisa").rglob("*.py"):
        if path.name == "mail.py":
            continue  # the adapters themselves
        source = path.read_text(errors="ignore")
        for marker in ("import smtplib", "import resend", "from resend"):
            if marker in source:
                offenders.append(f"{path.relative_to(_REPO_ROOT)}: {marker}")
    # models.py carries the Resend endpoint as a CONFIG default, which is the port working as
    # intended — the URL is a setting, not an import.
    assert offenders == []


# REQ-1485: the branded alternative part.


def _invite() -> MailMessage:
    from datetime import datetime, timezone

    from provisa.core.mail import compose_invite_message

    return compose_invite_message(
        to="carol@example.test",
        org_name="Acme Analytics",
        org_id="acme",
        inviter="alice",
        role_id="analyst",
        expires_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        base_url="https://provisa.example.test",
        token="tok-1",
    )


def test_the_invitation_carries_both_a_text_and_a_branded_part():
    """Text stays canonical: a client that renders neither HTML nor remote content still receives
    every fact and the link."""
    message = _invite()
    for part in (message.body, message.html):
        assert part is not None
        assert "Acme Analytics" in part
        assert "alice" in part
        assert "analyst" in part
        assert "2026-09-01" in part
        assert "https://provisa.example.test/?invite=tok-1" in part


def test_the_branded_part_uses_the_product_palette_and_no_remote_assets():
    """A blocked image would leave the message looking broken, so the wordmark is text."""
    html = _invite().html
    assert html is not None
    assert ">Provisa<" in html
    assert "<img" not in html
    assert "http://" not in html  # no external stylesheet or tracking pixel
    for color in ("#1F2933", "#10B981", "#4f46e5"):
        assert color in html


def test_the_branded_part_escapes_user_supplied_values():
    from provisa.core.mail import _invite_html

    html = _invite_html(
        org_name="<script>alert(1)</script>",
        org_id="acme",
        inviter="a&b",
        role_id="analyst",
        expiry="2026-09-01 12:00 UTC",
        url="https://provisa.example.test/?invite=t&x=1",
    )
    assert "<script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "a&amp;b" in html
    assert "invite=t&amp;x=1" in html


# REQ-1486: the inviting org's own branding on the message.


def test_an_org_with_no_branding_sends_the_products_own_invitation():
    """Branding is additive: an org that set none must be indistinguishable from before REQ-1486."""
    assert _invite() == _invite_with({})


def _invite_with(branding: dict[str, str]) -> MailMessage:
    from datetime import datetime, timezone

    from provisa.core.mail import compose_invite_message

    return compose_invite_message(
        branding=branding,
        to="carol@example.test",
        org_name="Acme Analytics",
        org_id="acme",
        inviter="alice",
        role_id="analyst",
        expires_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        base_url="https://provisa.example.test",
        token="tok-1",
    )


def test_the_display_name_replaces_the_org_name_in_the_subject_and_both_parts():
    message = _invite_with({"display_name": "Acme Data Platform"})

    assert "Acme Data Platform" in message.subject
    assert "Acme Analytics" not in message.subject
    assert message.html is not None
    for part in (message.body, message.html):
        assert "Acme Data Platform" in part
    # The org id still identifies the org unambiguously, whatever it calls itself.
    assert "acme" in message.body


def test_the_orgs_own_sentence_is_carried_above_the_products_copy():
    note = "Ping #data-platform if you have questions."
    message = _invite_with({"invite_message": note})

    assert note in message.body
    assert message.html is not None
    assert note in message.html
    # Provisa's own facts survive alongside it.
    assert "analyst" in message.body


def test_the_orgs_sentence_is_escaped_in_the_branded_part():
    message = _invite_with({"invite_message": "<script>alert(1)</script>"})

    assert message.html is not None
    assert "<script>" not in message.html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in message.html


def test_the_primary_color_becomes_the_button_color():
    message = _invite_with({"primary_color": "#b91c1c"})

    assert message.html is not None
    assert "#b91c1c" in message.html
