# Copyright (c) 2026 Kenneth Stott
# Canary: 4a19f0c2-7d51-4d3e-9a86-2c0be5b1f7d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""THE MAIL TRANSPORT, SET FROM THE PLATFORM SETTINGS PAGE (REQ-1576).

Until now the deployment's mail transport was a block in provisa.yaml resolved from environment
variables, which means the only way to point a running install at a mail server was a shell on the
node and a restart. An invitation is a platform communication -- the platform_admin owns it -- so
the transport belongs on the same settings surface as the secrets service and the encryption
provider, and is written and applied the same way: persisted to provisa.yaml, then rebound on the
running process, because the sender is built per send.

What the surface answers, beyond "what is it wired to", is "is mail actually going out" -- read out
of ``mail_events``, the record of real attempts, not out of the configuration. See
``provisa.core.mail_stats``.

Credentials obey REQ-1575 without exception: no GET here returns a stored secret, only the bit
saying one is on file. A submitted field that is absent leaves the stored value alone; a submitted
field that is present and empty clears it.
"""

# Requirements: REQ-1310, REQ-1330, REQ-1337, REQ-1575, REQ-1576

from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.admin._config_io import config_path, read_config, write_config
from provisa.api.admin._platform_guard import require_platform_settings
from provisa.api.admin.secret_redaction import redact_per_provider  # REQ-1575
from provisa.api.errors import ApiError
from provisa.core.database import Database

router = APIRouter(tags=["admin"])

#: Settings that belong to the deployment rather than to any one transport.
_SHARED_FIELDS = ("from_address", "base_url", "timeout_seconds")


def _admin_db() -> Database:
    """The control plane, where the mail record lives."""
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


def _providers() -> list[dict]:
    """UI view of the mail-provider registry (REQ-1576).

    Every registered transport is listed, available or not, for the same reason the secrets page
    lists every backend: an operator asking "can Provisa send through SES?" is answered by the row
    being there, with ``requires`` naming the one missing package, rather than by an absence that
    reads as "no".
    """
    from provisa.core.mail_registry import mail_provider_registry

    return [
        {
            "key": s.key,
            "label": s.label,
            "description": s.description,
            "available": s.available(),
            "requires": s.requires,
            "config_fields": s.config_fields,
        }
        for s in mail_provider_registry()
    ]


@router.get("/admin/mail")
async def get_mail(request: Request):  # REQ-1576
    """The configured transport, every transport it could be, and which credentials are on file."""
    require_platform_settings(request)  # REQ-1337
    cfg = read_config()
    mail = cfg.get("mail", {}) or {}
    providers = _providers()
    safe, is_set = redact_per_provider(
        {p["key"]: mail.get(p["key"]) or {} for p in providers}, providers
    )
    from provisa.core.models import MailConfig

    defaults = MailConfig()
    return {
        "provider": mail.get("provider", defaults.provider),
        "from_address": mail.get("from_address", defaults.from_address),
        "base_url": mail.get("base_url", defaults.base_url),
        "timeout_seconds": mail.get("timeout_seconds", defaults.timeout_seconds),
        "providers": providers,
        "config": safe,
        "secret_set": is_set,
    }


@router.put("/admin/mail")
async def set_mail(request: Request):  # REQ-1576
    """Select and configure the transport. Persisted to provisa.yaml AND applied to this process."""
    require_platform_settings(request)  # REQ-1337
    from provisa.core.mail_registry import get_mail_provider_spec

    body = await request.json()
    provider = body.get("provider")
    spec = get_mail_provider_spec(provider)
    if spec is None:
        raise ApiError(
            400,
            "mail.unknown_provider",
            f"unknown mail provider {provider!r}",
            provider=str(provider),
        )
    if not spec.available():
        # Fail closed, as the secrets service does: selecting a transport that cannot be built must
        # not quietly leave the deployment sending through the previous one.
        raise ApiError(
            400,
            "mail.provider_unavailable",
            f"mail provider {provider!r} is not available (install {spec.requires!r} to use it)",
            provider=str(provider),
        )
    cfg = read_config()
    mail = dict(cfg.get("mail", {}) or {})
    mail["provider"] = spec.key
    for field in _SHARED_FIELDS:
        if field in body:
            mail[field] = body[field]
    allowed = {f["config_key"] for f in spec.config_fields}
    pcfg = dict(mail.get(spec.key, {}) or {})
    for k, v in (body.get("config") or {}).items():
        if k in allowed:
            # Absent leaves the stored value alone (the key never reaches this loop); present and
            # empty clears it. REQ-1575: there is no third state, because no value came out.
            pcfg[k] = v
    if pcfg:
        mail[spec.key] = pcfg
    cfg["mail"] = mail
    write_config(config_path(), cfg)
    _apply(mail)
    return {"success": True, "provider": spec.key}


def _apply(mail: dict) -> None:
    """Rebind the running process to the saved mail block.

    The sender is constructed at each send from ``state.config.mail``, so replacing that object is
    the whole of applying the change -- no restart, unlike the encryption provider whose service is
    held by objects built at startup. Secret references are resolved here because a block that
    still carries a ``${env:...}`` template would be sent to the transport verbatim.
    """
    from provisa.api.app import state
    from provisa.core.models import MailConfig
    from provisa.core.secrets import resolve_secrets_in_dict

    config = getattr(state, "config", None)
    if config is None:
        raise ApiError(503, "mail.config_not_loaded", "Server configuration is not loaded")
    config.mail = MailConfig.model_validate(resolve_secrets_in_dict(mail))


@router.get("/admin/mail/stats")
async def get_mail_stats(request: Request):  # REQ-1576
    """What the transport has actually done: counts, the last success, the last failure."""
    require_platform_settings(request)  # REQ-1337
    from provisa.core.mail_stats import stats

    return await stats(_admin_db())


@router.post("/admin/mail/test")
async def send_test_mail(request: Request):  # REQ-1576
    """Send a test message through the configured transport and record the attempt.

    The point of the test is that it exercises the real path -- the same selection, the same
    credentials, the same network -- and lands in the same record as an invitation, so a failure
    here reads on the stats panel exactly as the failure an invitation would have hit. The
    transport's own words are returned verbatim: "550 sender domain not verified" is the answer,
    and a paraphrase of it is not.
    """
    require_platform_settings(request)  # REQ-1337
    from starlette.concurrency import run_in_threadpool

    from provisa.api.app import state
    from provisa.core.mail import MailMessage, email_sender
    from provisa.core.mail_stats import MailAttempt, record

    body = await request.json()
    to = (body.get("to") or "").strip()
    if not to:
        raise ApiError(400, "mail.test_recipient_required", "A recipient address is required")
    config = getattr(state, "config", None)
    if config is None:
        raise ApiError(503, "mail.config_not_loaded", "Server configuration is not loaded")
    identity = getattr(request.state, "identity", None)
    requested_by = getattr(identity, "user_id", None)
    message = MailMessage(
        to=to,
        subject="Provisa test message",
        body=(
            "This is a test message from Provisa.\n\n"
            "It was sent from the platform Email settings page to verify that this deployment "
            "can deliver mail. If you received it, invitations will reach their recipients the "
            "same way."
        ),
    )
    try:
        sender = email_sender(config.mail)
        await run_in_threadpool(sender.send, message)
    except Exception as exc:
        await record(
            _admin_db(),
            MailAttempt(
                provider=config.mail.provider,
                kind="test",
                recipient=to,
                succeeded=False,
                error=str(exc),
                requested_by=requested_by,
            ),
        )
        # 200 with the transport's message, not a 500: the request itself succeeded -- it asked a
        # question and got an answer, and the answer is what the page renders.
        return {"success": False, "error": str(exc)}
    await record(
        _admin_db(),
        MailAttempt(
            provider=config.mail.provider,
            kind="test",
            recipient=to,
            succeeded=True,
            requested_by=requested_by,
        ),
    )
    return {"success": True}
