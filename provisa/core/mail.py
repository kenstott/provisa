# Copyright (c) 2026 Kenneth Stott
# Canary: b3901456-4fc7-488d-83e5-4eaa548779b6
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
from html import escape
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
    # REQ-1485: branded alternative part. `body` stays the canonical content — every message is
    # readable without it, and text-only clients get the same link and facts.
    html: str | None = None


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
        if message.html is not None:  # REQ-1485: multipart/alternative, text part first
            msg.add_alternative(message.html, subtype="html")

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
        payload = {
            "from": cfg.from_address,
            "to": [message.to],
            "subject": message.subject,
            "text": message.body,
        }
        if message.html is not None:  # REQ-1485
            payload["html"] = message.html
        response = httpx.post(
            cfg.api_url,
            headers={"Authorization": f"Bearer {cfg.api_key}"},
            json=payload,
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
    branding: dict[str, str] | None = None,
) -> MailMessage:
    """The invitation as the invitee reads it.

    Names the org, who invited them, the role they will hold and when the invitation stops working —
    the expiry runs against a clock they otherwise cannot see — plus the link that carries them into
    redemption.

    REQ-1486: an org that has set branding is named by its display_name, colors the button with its
    primary_color, and may add one sentence of its own (invite_message) above Provisa's copy. The
    org's own words are additive: everything the invitee needs — role, expiry, link — is stated by
    this function regardless of what the org wrote.
    """
    brand = branding or {}
    display_name = brand.get("display_name", org_name)
    org_note = brand.get("invite_message")
    expiry = expires_at.strftime("%Y-%m-%d %H:%M UTC")
    url = invite_redemption_url(base_url, token)
    body = (
        f"{inviter} invited you to join {display_name} on Provisa.\n"
        f"\n"
        + (f"{org_note}\n\n" if org_note else "")
        + f"Organization: {display_name} ({org_id})\n"
        f"Your role: {role_id}\n"
        f"Invitation expires: {expiry}\n"
        f"\n"
        f"Accept the invitation:\n"
        f"{url}\n"
        f"\n"
        f"If you do not already have a Provisa account you will be asked to create an account first; the "
        f"invitation is applied once you do.\n"
    )
    return MailMessage(
        to=to,
        subject=f"{inviter} invited you to {display_name} on Provisa",
        body=body,
        html=_invite_html(
            org_name=display_name,
            org_id=org_id,
            inviter=inviter,
            role_id=role_id,
            expiry=expiry,
            url=url,
            org_note=org_note,
            button_color=brand.get("primary_color", _BRAND_PRIMARY),
        ),
    )


# Brand palette, from provisa-ui/src/theme/tokens.css and public/icon.svg. Duplicated as literals
# because email needs inline styles — no stylesheet, no CSS variables, no external assets.
_BRAND_INK = "#1F2933"  # icon background
_BRAND_ACCENT = "#10B981"  # icon dot
_BRAND_PRIMARY = "#4f46e5"  # --primary
_BRAND_TEXT = "#1a1d27"  # --text
_BRAND_MUTED = "#55596b"  # --text-muted
_BRAND_BORDER = "#d5d8e2"  # --border
_BRAND_SURFACE_ALT = "#f1f3f9"  # --surface-alt


def _invite_html(  # REQ-1485
    *,
    org_name: str,
    org_id: str,
    inviter: str,
    role_id: str,
    expiry: str,
    url: str,
    org_note: str | None = None,
    button_color: str = _BRAND_PRIMARY,
) -> str:
    """The branded alternative part.

    Table layout with inline styles and no images: every mail client renders it the same way, and
    nothing depends on the recipient allowing remote content — a blocked logo would leave the
    message looking broken. The wordmark is text, so it survives image blocking.
    """
    org = escape(org_name)
    who = escape(inviter)
    role = escape(role_id)
    org_key = escape(org_id)
    href = escape(url, quote=True)
    row = (
        f'<tr><td style="padding:4px 0;color:{_BRAND_MUTED};font-size:14px;">%s</td>'
        f'<td style="padding:4px 0;color:{_BRAND_TEXT};font-size:14px;font-weight:600;'
        f'text-align:right;">%s</td></tr>'
    )
    return (
        '<!doctype html><html><body style="margin:0;padding:0;background:#ffffff;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="background:#ffffff;">'
        '<tr><td align="center" style="padding:32px 16px;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        "style=\"max-width:540px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',"
        'Roboto,Helvetica,Arial,sans-serif;">'
        f'<tr><td style="background:{_BRAND_INK};border-radius:12px;padding:20px 24px;">'
        '<span style="color:#ffffff;font-size:20px;font-weight:700;letter-spacing:-0.3px;">'
        f'Provisa</span><span style="color:{_BRAND_ACCENT};font-size:20px;font-weight:700;">'
        "&#9679;</span></td></tr>"
        f'<tr><td style="padding:28px 4px 8px;color:{_BRAND_TEXT};font-size:20px;'
        f'font-weight:600;">{who} invited you to {org}</td></tr>'
        f'<tr><td style="padding:0 4px 20px;color:{_BRAND_MUTED};font-size:15px;'
        'line-height:22px;">Accept the invitation to join the organization on Provisa.'
        # REQ-1486: the org's own sentence, below Provisa's line rather than in place of it.
        + (
            f'<br><br><span style="color:{_BRAND_TEXT};">{escape(org_note)}</span>'
            if org_note
            else ""
        )
        + "</td></tr>"
        f'<tr><td style="padding:0 4px 24px;"><table role="presentation" width="100%" '
        f'cellpadding="0" cellspacing="0" style="background:{_BRAND_SURFACE_ALT};'
        f'border:1px solid {_BRAND_BORDER};border-radius:10px;padding:14px 16px;">'
        + row % ("Organization", f"{org} ({org_key})")
        + row % ("Your role", role)
        + row % ("Invitation expires", escape(expiry))
        + "</table></td></tr>"
        f'<tr><td style="padding:0 4px 24px;"><a href="{href}" '
        f'style="display:inline-block;background:{escape(button_color)};color:#ffffff;'
        "text-decoration:none;font-size:15px;font-weight:600;padding:12px 24px;"
        'border-radius:8px;">Accept invitation</a></td></tr>'
        f'<tr><td style="padding:0 4px 8px;color:{_BRAND_MUTED};font-size:13px;'
        'line-height:20px;">If the button does not work, paste this link into your browser:<br>'
        f'<a href="{href}" style="color:{_BRAND_PRIMARY};word-break:break-all;">{href}</a>'
        "</td></tr>"
        f'<tr><td style="padding:16px 4px 0;border-top:1px solid {_BRAND_BORDER};'
        f'color:{_BRAND_MUTED};font-size:13px;line-height:20px;">If you do not already have a '
        "Provisa account you will be asked to create an account first; the invitation is applied once you "
        "do.</td></tr>"
        "</table></td></tr></table></body></html>"
    )
