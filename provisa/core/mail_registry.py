# Copyright (c) 2026 Kenneth Stott
# Canary: 5b03dcc9-e3d8-4899-9a1a-3f086157446e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WHICH MAIL TRANSPORT THE PLATFORM SENDS THROUGH (REQ-1576).

REQ-1330 gave sending a port -- callers hold an ``EmailSender`` and call ``send()`` -- and picked
the adapter behind it out of a dict of two. This is that choice as a REGISTRY, in the shape the
secrets (``secrets_registry``) and encryption registries already use: a named spec carrying the
label and description a settings page renders, the ``config_fields`` its form is built from, an
availability probe over whatever the adapter imports, and a builder over that transport's own
config block.

The registry is what makes the transport a SETTING rather than a code branch. A platform_admin
picks a key, fills the fields the spec declares, and the deployment sends through it -- no restart,
no edit on the node. Selection is FAIL-CLOSED: an unknown key, or one whose SDK is not installed,
raises where the message can name the setting and the distribution to install.

Mail has no one popular protocol, so the shipped set is the several that deployments actually use:
SMTP for relays and self-managed servers, and the HTTPS transactional APIs -- Resend, SendGrid,
Mailgun, Postmark, Amazon SES, Microsoft 365 via Graph.

A field marked ``secret`` is write-only everywhere it is rendered (REQ-1575): the settings GET
drops it and returns only whether a value is on file.
"""

# Requirements: REQ-1310, REQ-1330, REQ-1576

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from provisa.core.mail import EmailSender


@dataclass(frozen=True)
class MailProviderSpec:
    key: str
    label: str
    description: str
    #: (the whole MailConfig) -> EmailSender. The adapter reads its own block off the config plus
    #: the shared from_address/timeout, which is why the builder is handed the config and not the
    #: block: a transport is nothing without the sender identity it sends as.
    build: Callable[[Any], "EmailSender"]
    #: UI field descriptors (config_key/label/type/required/secret/placeholder), named within this
    #: transport's own config block.
    config_fields: list[dict] = field(default_factory=list)
    available: Callable[[], bool] = lambda: True
    #: The distribution a deployment installs to make this transport available, named so the page
    #: can say WHY an unavailable transport is unavailable. ``None`` when nothing is needed.
    requires: str | None = None


_REGISTRY: dict[str, MailProviderSpec] = {}


def register_mail_provider(spec: MailProviderSpec) -> None:
    """Register (or replace) a transport spec. Public extension API."""
    _REGISTRY[spec.key] = spec


def get_mail_provider_spec(key: str | None) -> MailProviderSpec | None:
    _load_builtin()
    return _REGISTRY.get(key or "")


def mail_provider_registry() -> list[MailProviderSpec]:
    """Every transport, in the order the settings page lists them."""
    _load_builtin()
    return list(_REGISTRY.values())


def _load_builtin() -> None:
    if _REGISTRY:
        return
    from provisa.core import mail

    for spec in _BUILTIN(mail):
        _REGISTRY[spec.key] = spec


def _f(
    key: str,
    label: str,
    *,
    type: str = "text",
    required: bool = False,
    secret: bool = False,
    placeholder: str = "",
) -> dict:
    return {
        "config_key": key,
        "label": label,
        "type": type,
        "required": required,
        "secret": secret,
        "placeholder": placeholder,
    }


def _BUILTIN(mail) -> list[MailProviderSpec]:
    return [
        MailProviderSpec(
            key="smtp",
            label="SMTP",
            description="A mail server spoken to over SMTP — a corporate relay or a self-managed "
            "server. Port 587 with STARTTLS and port 465 with implicit TLS are the usual pairs.",
            build=mail.SmtpEmailSender,
            config_fields=[
                _f("host", "Host", required=True, placeholder="smtp.example.com"),
                _f("port", "Port", type="number", required=True, placeholder="587"),
                _f("username", "Username"),
                _f("password", "Password", type="password", secret=True),
                _f("use_starttls", "Use STARTTLS", type="boolean"),
                _f("use_ssl", "Use implicit TLS (SMTPS)", type="boolean"),
            ],
        ),
        MailProviderSpec(
            key="resend",
            label="Resend",
            description="Resend's HTTPS API. The sender domain must be verified in the Resend "
            "account before it will accept a message.",
            build=mail.ResendEmailSender,
            config_fields=[
                _f("api_key", "API key", type="password", required=True, secret=True),
                _f("api_url", "API endpoint", placeholder="https://api.resend.com/emails"),
            ],
        ),
        MailProviderSpec(
            key="sendgrid",
            label="SendGrid",
            description="Twilio SendGrid's v3 API. The sender address must pass SendGrid's sender "
            "identity verification.",
            build=mail.SendgridEmailSender,
            config_fields=[
                _f("api_key", "API key", type="password", required=True, secret=True),
                _f("api_url", "API endpoint", placeholder="https://api.sendgrid.com/v3/mail/send"),
            ],
        ),
        MailProviderSpec(
            key="mailgun",
            label="Mailgun",
            description="Mailgun's messages API. The domain is the sending domain the account is "
            "provisioned for; the EU region is a different endpoint host.",
            build=mail.MailgunEmailSender,
            config_fields=[
                _f("domain", "Sending domain", required=True, placeholder="mg.example.com"),
                _f("api_key", "API key", type="password", required=True, secret=True),
                _f("api_url", "API base", placeholder="https://api.mailgun.net/v3"),
            ],
        ),
        MailProviderSpec(
            key="postmark",
            label="Postmark",
            description="Postmark's email API. The token belongs to one Postmark server, so it "
            "selects the stream mail is sent on as well as authenticating.",
            build=mail.PostmarkEmailSender,
            config_fields=[
                _f("server_token", "Server token", type="password", required=True, secret=True),
                _f("message_stream", "Message stream", placeholder="outbound"),
                _f("api_url", "API endpoint", placeholder="https://api.postmarkapp.com/email"),
            ],
        ),
        MailProviderSpec(
            key="ses",
            label="Amazon SES",
            description="Amazon SES v2 through boto3. Leave the credentials empty to use the "
            "ambient AWS credential chain — an instance role or a configured profile.",
            build=mail.SesEmailSender,
            available=lambda: find_spec("boto3") is not None,
            requires="boto3",
            config_fields=[
                _f("region", "Region", required=True, placeholder="us-east-1"),
                _f("access_key_id", "Access key id"),
                _f("secret_access_key", "Secret access key", type="password", secret=True),
            ],
        ),
        MailProviderSpec(
            key="microsoft365",
            label="Microsoft 365",
            description="Microsoft Graph sendMail, for a tenant whose mail already leaves through "
            "Exchange Online. The app registration needs the Mail.Send application permission, and "
            "the sender is the mailbox the message is sent as. Exchange rewrites the sender from "
            "that mailbox, so every message shows the mailbox's own name: invitations reach the "
            "inviter on reply but do not name the inviting organization in the sender column "
            "(REQ-1577).",
            build=mail.Microsoft365EmailSender,
            config_fields=[
                _f("tenant_id", "Directory (tenant) id", required=True),
                _f("client_id", "Application (client) id", required=True),
                _f("client_secret", "Client secret", type="password", required=True, secret=True),
                _f("sender", "Sender mailbox", required=True, placeholder="invites@example.com"),
            ],
        ),
    ]
