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
deployment detail selected by ``mail.provider`` and looked up in ``mail_registry`` (REQ-1576) —
SMTP for relays and self-managed servers, and the HTTPS transactional APIs (Resend, SendGrid,
Mailgun, Postmark, Amazon SES, Microsoft 365 via Graph). Adding a transport is a spec in the
registry and an adapter here; no call site changes.

Configuration follows the same shape as every other backing service — a section on ProvisaConfig
(``mail:``) with a nested block per transport, so a deployment holds an SMTP host and an API key
at once and switches between them without retyping either. An unset switch means no mail transport
is available, which is a refusal at the point of sending, not a silent drop.
"""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr
from html import escape
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

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
    # REQ-1577: who the message appears to come from, and where a reply goes. The address stays the
    # deployment's one verified sender; `from_name` names the org in the inbox list and `reply_to`
    # carries the answer to a person. Both unset is a message delivered exactly as before.
    from_name: str | None = None
    reply_to: str | None = None


def sender_identity(from_address: str, message: MailMessage) -> str:  # REQ-1577
    """The sender as a transport writes it: the deployment address, display name in front of it."""
    return (
        formataddr((message.from_name, from_address))
        if message.from_name is not None
        else from_address
    )


class EmailSender(Protocol):  # REQ-1330
    """The port. Callers hold one of these and call ``send``; the provider behind it is a
    deployment detail. Implementations are synchronous by design — they run inside a
    ``run_in_threadpool`` at the one call site. A dedicated queue is the right shape once there
    is more than one kind of message; there is not yet."""

    def send(self, message: MailMessage) -> None: ...


def _secret(value) -> str:
    """The plaintext of a config field held as ``SecretStr``."""
    return value.get_secret_value() if hasattr(value, "get_secret_value") else str(value)


def _unconfigured(setting: str) -> MailNotConfiguredError:
    return MailNotConfiguredError(
        f"No {setting} is configured. Set it under Admin -> Email to deliver invitations by "
        f"email, or distribute the invitation link yourself."
    )


def _refused(provider: str, status: int, text: str) -> RuntimeError:
    """The transport's own answer, verbatim. An operator fixes a rejected sender identity or an
    expired key from the words the provider used, not from a rephrasing of them."""
    return RuntimeError(f"{provider} refused the message ({status}): {text}")


class SmtpEmailSender:  # REQ-1310
    """SMTP transport, for self-managed mail infrastructure and the loopback delivery test."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        cfg = self._config
        smtp_cfg = cfg.smtp
        if not smtp_cfg.host:
            raise _unconfigured("SMTP host (mail.smtp.host)")
        msg = EmailMessage()
        msg["From"] = sender_identity(cfg.from_address, message)
        msg["To"] = message.to
        msg["Subject"] = message.subject
        if message.reply_to is not None:  # REQ-1577
            msg["Reply-To"] = message.reply_to
        msg.set_content(message.body)
        if message.html is not None:  # REQ-1485: multipart/alternative, text part first
            msg.add_alternative(message.html, subtype="html")

        smtp_class = smtplib.SMTP_SSL if smtp_cfg.use_ssl else smtplib.SMTP
        with smtp_class(smtp_cfg.host, smtp_cfg.port, timeout=cfg.timeout_seconds) as smtp:
            if smtp_cfg.use_starttls:
                smtp.starttls()
            if smtp_cfg.username:
                smtp.login(smtp_cfg.username, _secret(smtp_cfg.password))
            smtp.send_message(msg)
        log.info("mail delivered to %s via smtp", message.to)


class ResendEmailSender:  # REQ-1330
    """Resend HTTPS transport — the SaaS deployment's provider adapter."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        import httpx

        cfg = self._config
        api_key = _secret(cfg.resend.api_key)
        if not api_key:
            raise _unconfigured("Resend API key (mail.resend.api_key)")
        payload = {
            "from": sender_identity(cfg.from_address, message),
            "to": [message.to],
            "subject": message.subject,
            "text": message.body,
        }
        if message.html is not None:  # REQ-1485
            payload["html"] = message.html
        if message.reply_to is not None:  # REQ-1577
            payload["reply_to"] = [message.reply_to]
        response = httpx.post(
            cfg.resend.api_url,
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
            timeout=cfg.timeout_seconds,
        )
        if response.status_code >= 400:
            raise _refused("Resend", response.status_code, response.text)
        log.info("mail delivered to %s via resend", message.to)


class SendgridEmailSender:  # REQ-1576
    """Twilio SendGrid's v3 API."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        import httpx

        cfg = self._config
        api_key = _secret(cfg.sendgrid.api_key)
        if not api_key:
            raise _unconfigured("SendGrid API key (mail.sendgrid.api_key)")
        content = [{"type": "text/plain", "value": message.body}]
        if message.html is not None:  # REQ-1485: SendGrid picks the LAST part, so html goes last
            content.append({"type": "text/html", "value": message.html})
        payload = {
            "personalizations": [{"to": [{"email": message.to}]}],
            "from": {"email": cfg.from_address},
            "subject": message.subject,
            "content": content,
        }
        if message.from_name is not None:  # REQ-1577: SendGrid names the sender in its own field
            payload["from"]["name"] = message.from_name
        if message.reply_to is not None:
            payload["reply_to"] = {"email": message.reply_to}
        response = httpx.post(
            cfg.sendgrid.api_url,
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
            timeout=cfg.timeout_seconds,
        )
        if response.status_code >= 400:
            raise _refused("SendGrid", response.status_code, response.text)
        log.info("mail delivered to %s via sendgrid", message.to)


class MailgunEmailSender:  # REQ-1576
    """Mailgun's messages API. Form-encoded rather than JSON — that is the API Mailgun offers."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        import httpx

        cfg = self._config
        api_key = _secret(cfg.mailgun.api_key)
        if not api_key:
            raise _unconfigured("Mailgun API key (mail.mailgun.api_key)")
        if not cfg.mailgun.domain:
            raise _unconfigured("Mailgun sending domain (mail.mailgun.domain)")
        data = {
            "from": sender_identity(cfg.from_address, message),
            "to": message.to,
            "subject": message.subject,
            "text": message.body,
        }
        if message.html is not None:  # REQ-1485
            data["html"] = message.html
        if message.reply_to is not None:  # REQ-1577: Mailgun sets a header via the h: prefix
            data["h:Reply-To"] = message.reply_to
        response = httpx.post(
            f"{cfg.mailgun.api_url.rstrip('/')}/{cfg.mailgun.domain}/messages",
            auth=("api", api_key),
            data=data,
            timeout=cfg.timeout_seconds,
        )
        if response.status_code >= 400:
            raise _refused("Mailgun", response.status_code, response.text)
        log.info("mail delivered to %s via mailgun", message.to)


class PostmarkEmailSender:  # REQ-1576
    """Postmark's email API."""

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        import httpx

        cfg = self._config
        token = _secret(cfg.postmark.server_token)
        if not token:
            raise _unconfigured("Postmark server token (mail.postmark.server_token)")
        payload = {
            "From": sender_identity(cfg.from_address, message),
            "To": message.to,
            "Subject": message.subject,
            "TextBody": message.body,
            "MessageStream": cfg.postmark.message_stream,
        }
        if message.html is not None:  # REQ-1485
            payload["HtmlBody"] = message.html
        if message.reply_to is not None:  # REQ-1577
            payload["ReplyTo"] = message.reply_to
        response = httpx.post(
            cfg.postmark.api_url,
            headers={"X-Postmark-Server-Token": token, "Accept": "application/json"},
            json=payload,
            timeout=cfg.timeout_seconds,
        )
        if response.status_code >= 400:
            raise _refused("Postmark", response.status_code, response.text)
        log.info("mail delivered to %s via postmark", message.to)


class SesEmailSender:  # REQ-1576
    """Amazon SES v2 through boto3.

    Empty credentials are not an unconfigured transport: SES is normally reached from inside AWS,
    where the instance role IS the credential, so boto3's own chain is what resolves them.
    """

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def send(self, message: MailMessage) -> None:
        import boto3

        cfg = self._config
        if not cfg.ses.region:
            raise _unconfigured("SES region (mail.ses.region)")
        key_id = cfg.ses.access_key_id
        secret = _secret(cfg.ses.secret_access_key)
        client = boto3.client(
            "sesv2",
            region_name=cfg.ses.region,
            **(
                {"aws_access_key_id": key_id, "aws_secret_access_key": secret}
                if key_id
                else {}  # the ambient chain; see the class docstring
            ),
        )
        body: dict = {"Text": {"Data": message.body, "Charset": "UTF-8"}}
        if message.html is not None:  # REQ-1485
            body["Html"] = {"Data": message.html, "Charset": "UTF-8"}
        client.send_email(
            FromEmailAddress=sender_identity(cfg.from_address, message),
            Destination={"ToAddresses": [message.to]},
            Content={
                "Simple": {
                    "Subject": {"Data": message.subject, "Charset": "UTF-8"},
                    "Body": body,
                }
            },
            # REQ-1577: SES takes reply-to as its own list rather than a header.
            **({"ReplyToAddresses": [message.reply_to]} if message.reply_to is not None else {}),
        )
        log.info("mail delivered to %s via ses", message.to)


class Microsoft365EmailSender:  # REQ-1576
    """Microsoft Graph ``sendMail``, with the app-only client-credentials flow.

    The token is fetched per send rather than cached: sending is rare enough that a cache would
    save nothing measurable, and a cached token is one more piece of state to be wrong about.
    """

    def __init__(self, mail_config) -> None:
        self._config = mail_config

    def _token(self, httpx) -> str:
        cfg = self._config.microsoft365
        response = httpx.post(
            f"{cfg.login_url.rstrip('/')}/{cfg.tenant_id}/oauth2/v2.0/token",
            data={
                "client_id": cfg.client_id,
                "client_secret": _secret(cfg.client_secret),
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
            timeout=self._config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise _refused("Microsoft 365", response.status_code, response.text)
        return response.json()["access_token"]

    def send(self, message: MailMessage) -> None:
        import httpx

        cfg = self._config
        m365 = cfg.microsoft365
        for value, setting in (
            (m365.tenant_id, "Microsoft 365 tenant id (mail.microsoft365.tenant_id)"),
            (m365.client_id, "Microsoft 365 client id (mail.microsoft365.client_id)"),
            (_secret(m365.client_secret), "Microsoft 365 client secret"),
            (m365.sender, "Microsoft 365 sender mailbox (mail.microsoft365.sender)"),
        ):
            if not value:
                raise _unconfigured(setting)
        # Graph takes ONE body with a content type, so the branded part replaces the text part
        # rather than accompanying it -- there is no multipart/alternative to build here.
        content = (
            {"contentType": "HTML", "content": message.html}
            if message.html is not None
            else {"contentType": "Text", "content": message.body}
        )
        graph_message: dict = {
            "subject": message.subject,
            "body": content,
            "toRecipients": [{"emailAddress": {"address": message.to}}],
        }
        # REQ-1577: Graph takes a display name here, but Exchange Online rewrites the sender of an
        # app-only send from the mailbox object, so what arrives is the mailbox's own name. The
        # field is set because it is the correct thing to send; a deployment that needs the per-org
        # name in the inbox list uses a transport that renders the From header verbatim.
        if message.from_name is not None:
            graph_message["from"] = {
                "emailAddress": {"address": cfg.from_address, "name": message.from_name}
            }
        if message.reply_to is not None:
            graph_message["replyTo"] = [{"emailAddress": {"address": message.reply_to}}]
        response = httpx.post(
            f"{m365.api_url.rstrip('/')}/users/{m365.sender}/sendMail",
            headers={"Authorization": f"Bearer {self._token(httpx)}"},
            json={"message": graph_message, "saveToSentItems": True},
            timeout=cfg.timeout_seconds,
        )
        if response.status_code >= 400:
            raise _refused("Microsoft 365", response.status_code, response.text)
        log.info("mail delivered to %s via microsoft365", message.to)


def email_sender(mail_config) -> EmailSender:  # REQ-1330, REQ-1576
    """The configured transport behind the port.

    The choice is a registry lookup (REQ-1576), so an unknown key and an uninstalled SDK are both
    refused here, at construction, where the message can name the setting and what to install.
    """
    from provisa.core.mail_registry import get_mail_provider_spec, mail_provider_registry

    # Compose overlays interpolate PROVISA_MAIL_PROVIDER as "" when the node sets none, and the
    # env resolver keeps a set-but-empty variable rather than the yaml default — so empty is the
    # documented unconfigured state, not a typo.
    if not mail_config.provider:
        raise MailNotConfiguredError(
            "No mail provider is configured (mail.provider / PROVISA_MAIL_PROVIDER). Set it to "
            "deliver invitations by email, or distribute the invitation link yourself."
        )
    spec = get_mail_provider_spec(mail_config.provider)
    if spec is None:
        raise MailNotConfiguredError(
            f"Unknown mail provider '{mail_config.provider}' (mail.provider); expected one of: "
            f"{', '.join(sorted(s.key for s in mail_provider_registry()))}"
        )
    if not spec.available():
        raise MailNotConfiguredError(
            f"Mail provider '{spec.key}' is not available (install {spec.requires!r} to use it)"
        )
    return spec.build(mail_config)


def invite_redemption_url(base_url: str, token: str, org_id: str) -> str:
    """The link the invitee follows. The UI reads ``?invite=<token>`` and drives redemption after
    sign-in, so the token never has to be copied by hand.

    REQ-1276: an org is reached at its own host, so the invitation addresses the org rather than the
    control plane -- ``mail.base_url``'s leftmost label is replaced by the org id, the same rule the
    UI's ``orgOrigin`` applies. A base_url whose host has no label to strip (``localhost``, a bare
    hostname) addresses no org by name; there the deployment has exactly one address and it is the
    one configured. REQ-1348: the org host cannot sign the invitee in, and redirects them to the
    control-plane login carrying the token, which is where redemption runs.

    An empty ``base_url`` is refused rather than defaulted. ``${env:...:-default}`` resolves a
    variable that is SET BUT EMPTY to the empty string -- the default applies only when the
    variable is absent -- so a deployment that exports ``PROVISA_MAIL_BASE_URL=`` reaches here
    with nothing, and the link it would build, ``/?invite=<token>``, is a relative path no mail
    client can open. The deployment is misconfigured and the invitation must say so.
    """
    if not base_url.strip():
        raise MailNotConfiguredError(
            "mail.base_url is empty, so the invitation link would be a relative path no mail "
            "client can open. Set PROVISA_MAIL_BASE_URL to the public origin of the UI."
        )
    parsed = urlsplit(base_url.strip().rstrip("/"))
    labels = parsed.hostname.split(".") if parsed.hostname else []
    if len(labels) >= 2:
        host = f"{org_id}.{'.'.join(labels[1:])}"
        netloc = f"{host}:{parsed.port}" if parsed.port else host
        origin = urlunsplit((parsed.scheme, netloc, "", "", ""))
    else:
        origin = base_url.strip().rstrip("/")
    return f"{origin}/?invite={token}"


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
    inviter_email: str | None,
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

    REQ-1577: the org is named in the From display name and a reply goes to the inviter. When the
    inviter's identity carries no email address (``inviter_email`` is None) the message carries no
    Reply-To at all — a reply to an unattended address is worse than no reply address.
    """
    brand = branding or {}
    display_name = brand.get("display_name", org_name)
    org_note = brand.get("invite_message")
    expiry = expires_at.strftime("%Y-%m-%d %H:%M UTC")
    url = invite_redemption_url(base_url, token, org_id)
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
        # REQ-1577: "(via Provisa)" keeps the platform visible, so the display name is a
        # description of the sender rather than an impersonation of the org.
        from_name=f"{display_name} (via Provisa)",
        reply_to=inviter_email,
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
