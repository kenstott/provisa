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

# Requirements: REQ-1310, REQ-1330

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
