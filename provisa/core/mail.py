# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Outbound mail (REQ-1310).

The only message Provisa sends today is the org invitation. It is composed here rather than in the
invites router so the wording, the link and the SMTP transport are one testable unit: the delivery
test starts a real SMTP server, redeems the link it captures, and asserts on the message a person
would actually receive.

Configuration follows the same shape as every other backing service — a section on ProvisaConfig
(``mail:``) with the host as the switch. No host configured means no mail transport is available,
which is a refusal at the point of sending, not a silent drop.
"""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage

log = logging.getLogger(__name__)


class MailNotConfiguredError(RuntimeError):
    """Raised when a message must be sent and no SMTP host is configured.

    Deliberately not a silent no-op: an invitation the invitee never receives is indistinguishable
    from no invitation, and the org_admin who created it must be told so they can send the link
    themselves instead.
    """


@dataclass(frozen=True)
class MailMessage:
    to: str
    subject: str
    body: str


def send_message(mail_config, message: MailMessage) -> None:
    """Deliver ``message`` over SMTP using ``mail_config`` (a ``MailConfig``).

    Synchronous by design — it runs inside a ``run_in_threadpool`` at the one call site. A dedicated
    queue is the right shape once there is more than one kind of message; there is not yet.
    """
    if not mail_config.host:
        raise MailNotConfiguredError(
            "No SMTP host is configured (mail.host). Set it to deliver invitations by email, or "
            "distribute the invitation link yourself."
        )
    msg = EmailMessage()
    msg["From"] = mail_config.from_address
    msg["To"] = message.to
    msg["Subject"] = message.subject
    msg.set_content(message.body)

    smtp_class = smtplib.SMTP_SSL if mail_config.use_ssl else smtplib.SMTP
    with smtp_class(mail_config.host, mail_config.port, timeout=mail_config.timeout_seconds) as smtp:
        if mail_config.use_starttls:
            smtp.starttls()
        if mail_config.username:
            smtp.login(mail_config.username, mail_config.password)
        smtp.send_message(msg)
    log.info("invitation mail delivered to %s", message.to)


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
