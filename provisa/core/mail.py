# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Outbound mail (REQ-1310, REQ-1330).

The only message Provisa sends today is the org invitation. It is composed here rather than in the
invites router so the wording, the link and the transport are one testable unit: the delivery
test starts a real SMTP server, redeems the link it captures, and asserts on the message a person
would actually receive.

Transport sits behind the ``EmailSender`` port (REQ-1330): callers obtain a sender from
``email_sender()`` and depend only on ``send()``. Which concrete transport backs it is a
deployment detail selected by ``mail.provider`` — ``smtp`` (REQ-1310) or ``resend`` (the SaaS
transactional provider). Swapping providers is a config change plus, at most, a new adapter in
this module; no call site changes.

Configuration follows the same shape as every other backing service — a section on ProvisaConfig
(``mail:``) with the provider's own switch (SMTP host, Resend API key). An unset switch means no
mail transport is available, which is a refusal at the point of sending, not a silent drop.
"""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Protocol

log = logging.getLogger(__name__)


class MailNotConfiguredError(RuntimeError):
    """Raised when a message must be sent and the selected provider is not configured.

    Deliberately not a silent no-op: an invitation the invitee never receives is indistinguishable
    from no invitation, and the org_admin who created it must be told so they can send the link
    themselves instead.
    """


@dataclass(frozen=True)
class MailMessage:
    to: str
    subject: str
    body: str


class EmailSender(Protocol):  # REQ-1330
    """The port. Callers hold one of these and call ``send``; the provider behind it is a
    deployment detail. Implementations are synchronous by design — they run inside a
    ``run_in_threadpool`` at the one call site. A dedicated queue is the right shape once there
    is more than one kind of message; there is not yet."""

    def send(self, message: MailMessage) -> None: ...


class SmtpEmailSender:  # REQ-1310
    """SMTP transport, for self-managed mail infrastructure and the loopback delivery test."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        cfg = self._config
        if not cfg.host:
            raise MailNotConfiguredError(
                "No SMTP host is configured (mail.host). Set it to deliver invitations by email, "
                "or distribute the invitation link yourself."
            )
        msg = EmailMessage()
        msg["From"] = cfg.from_address
        msg["To"] = message.to
        msg["Subject"] = message.subject
        msg.set_content(message.body)

        smtp_class = smtplib.SMTP_SSL if cfg.use_ssl else smtplib.SMTP
        with smtp_class(cfg.host, cfg.port, timeout=cfg.timeout_seconds) as smtp:
            if cfg.use_starttls:
                smtp.starttls()
            if cfg.username:
                smtp.login(cfg.username, cfg.password)
            smtp.send_message(msg)
        log.info("invitation mail delivered to %s", message.to)


class ResendEmailSender:  # REQ-1330
    """Resend HTTPS transport — the SaaS deployment's provider adapter."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        import httpx

        cfg = self._config
        if not cfg.api_key:
            raise MailNotConfiguredError(
                "No Resend API key is configured (mail.api_key). Set it to deliver invitations "
                "by email, or distribute the invitation link yourself."
            )
        response = httpx.post(
            cfg.api_url,
            headers={"Authorization": f"Bearer {cfg.api_key}"},
            json={
                "from": cfg.from_address,
                "to": [message.to],
                "subject": message.subject,
                "text": message.body,
            },
            timeout=cfg.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Resend refused the message ({response.status_code}): {response.text}"
            )
        log.info("invitation mail delivered to %s via resend", message.to)


_PROVIDERS = {"smtp": SmtpEmailSender, "resend": ResendEmailSender}


def email_sender(mail_config) -> EmailSender:  # REQ-1330
    """The configured transport behind the port. An unknown provider is a config fault and is
    refused here, at construction, where the message names the setting."""
    # Compose overlays interpolate PROVISA_MAIL_PROVIDER as "" when the node sets none, and the
    # env resolver keeps a set-but-empty variable rather than the yaml default — so empty is the
    # documented unconfigured state, not a typo.
    if not mail_config.provider:
        raise MailNotConfiguredError(
            "No mail provider is configured (mail.provider / PROVISA_MAIL_PROVIDER). Set it to "
            "deliver invitations by email, or distribute the invitation link yourself."
        )
    try:
        provider = _PROVIDERS[mail_config.provider]
    except KeyError:
        raise MailNotConfiguredError(
            f"Unknown mail provider '{mail_config.provider}' (mail.provider); "
            f"expected one of: {', '.join(sorted(_PROVIDERS))}"
        ) from None
    return provider(mail_config)


def invite_redemption_url(base_url: str, token: str) -> str:
    """The link the invitee follows. The UI reads ``?invite=<token>`` and drives redemption after
    sign-in, so the token never has to be copied by hand."""
    return f"{base_url.rstrip('/')}/?invite={token}"


def compose_invite_message(
    *,
    to: str,
    org_name: str,
    org_id: str,
    inviter: str,
    role_id: str,
    expires_at,
    base_url: str,
    token: str,
) -> MailMessage:
    """The invitation as the invitee reads it.

    Names the org, who invited them, the role they will hold and when the invitation stops working —
    the expiry runs against a clock they otherwise cannot see — plus the link that carries them into
    redemption.
    """
    expiry = expires_at.strftime("%Y-%m-%d %H:%M UTC")
    url = invite_redemption_url(base_url, token)
    body = (
        f"{inviter} invited you to join {org_name} on Provisa.\n"
        f"\n"
        f"Organization: {org_name} ({org_id})\n"
        f"Your role: {role_id}\n"
        f"Invitation expires: {expiry}\n"
        f"\n"
        f"Accept the invitation:\n"
        f"{url}\n"
        f"\n"
        f"If you do not already have a Provisa account you will be asked to sign in first; the "
        f"invitation is applied once you do.\n"
    )
    return MailMessage(to=to, subject=f"{inviter} invited you to {org_name} on Provisa", body=body)
