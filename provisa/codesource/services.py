# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0b6c1e-2a47-4d8e-9b1f-7c5e0a9d2b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's services to a code source's worker (REQ-2104): its declared secrets, read at use.

A code source declares the names its code may read (``secrets`` in its mapping): each the org
secret of that name, or a reference the steward typed into the org's vault or the deployment's
environment (``${secret:NAME}``, ``${env:NAME}``). What was entered is kept as written
through config load and storage; nothing resolves it until the code asks.

When a worker starts it is given one environment variable, ``PROVISA_SERVICES``: an address on a
host only the egress proxy answers (``http://provisa-services/<token>``), the token scoped to the
source, its org and its declared names, and revoked when the worker stops. The code reads a secret
with a plain GET of ``$PROVISA_SERVICES/secrets/<name>``; the proxy answers it here, resolving the
reference in the org's vault at that moment, so a rotated secret is read on the next fetch. Every
other service sits beside ``secrets/`` under the same address. Nothing else of Provisa is
reachable: the proxy still never connects to loopback.

Every read is audited in the org's administrative trail (the source, the name, the time, and
whether it was answered). A value is never logged, never put in a refusal, and never part of the
code's content hash. An undeclared name, or one whose reference resolves to nothing, is refused,
naming the name.
"""

# Requirements: REQ-2104, REQ-2105, REQ-2106
from __future__ import annotations

import logging
import secrets
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from provisa.otel_compat import get_tracer, stage

log = logging.getLogger(__name__)

_tracer = get_tracer(__name__)

#: The host the egress proxy answers itself. It resolves nowhere, so only the proxy reaches it.
SERVICES_HOST = "provisa-services"

#: The one variable a worker is given to reach Provisa's services.
SERVICES_ENV = "PROVISA_SERVICES"

#: The audit trail's action for one read of a declared secret.
SECRET_READ = "code_source.secret.read"


@dataclass(frozen=True)
class ServiceGrant:
    """What one source's worker may ask for: its declared names, each the reference typed for it
    (None: the org secret of that name), in the org it is registered in."""

    org_id: str
    source_id: str
    secrets: Mapping[str, str | None] = field(default_factory=dict)
    admin_db: Any = None
    #: REQ-2105: the source's monthly (UTC) cap on what its LLM calls cost, USD; None: no cap.
    llm_cap_usd: float | None = None
    #: REQ-2106: the source's declared non-secret settings, as its steward edits them.
    settings: Mapping[str, Any] = field(default_factory=dict)
    #: REQ-2106: the egress allow-list a fetch Provisa makes for the source is held to -- the one
    #: the worker's own egress is.
    egress: tuple[str, ...] = ()
    #: REQ-2133: the role the dq service answers as for every call under this grant -- a view's
    #: preflight's, the role its build runs as. None: each call's own request role (a code source).
    role: str | None = None
    #: REQ-2133: the product of the view whose preflight holds this grant, which dq relates each
    #: checker to (promise, expectation or partner DQ). None for a code source, or a view of none.
    product_id: str | None = None


class ServiceRefused(Exception):
    """A request the services answer with a refusal: its HTTP status, and a reason that names
    what was asked for, never a value."""

    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


_grants: dict[str, ServiceGrant] = {}
_lock = threading.Lock()


def declared(raw: Any) -> dict[str, str | None]:
    """A mapping's ``secrets``, as name -> the reference typed for it, or None for the org secret
    of that name: a list of names, or a mapping whose values are references or null. Refused by
    name when it is neither."""
    from provisa.codesource.bundle import BundleRefused

    if raw is None:
        return {}
    if isinstance(raw, list):
        if not all(isinstance(n, str) and n for n in raw):
            raise BundleRefused("secrets_invalid", "a source's secrets are names")
        return dict.fromkeys(raw)
    if isinstance(raw, dict):
        for name, reference in raw.items():
            if reference is not None and not (
                isinstance(reference, str) and _is_reference(reference)
            ):
                raise BundleRefused(
                    "secrets_invalid",
                    f"the secret {name!r} is a reference to the vault or the environment, as "
                    "entered, or null for the organization secret of that name",
                    name=str(name),
                )
        return dict(raw)
    raise BundleRefused("secrets_invalid", "a source's secrets are a list of names or a mapping")


def declared_settings(raw: Any) -> dict[str, Any]:
    """A mapping's ``settings`` (REQ-2106): the source's non-secret settings, name -> a text,
    number or true/false. Refused by name when it is anything else."""
    from provisa.codesource.bundle import BundleRefused

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise BundleRefused(
            "settings_invalid", "a source's settings are a mapping of name to value"
        )
    for name, value in raw.items():
        if not isinstance(name, str) or not name or not isinstance(value, (str, int, float, bool)):
            raise BundleRefused(
                "settings_invalid",
                f"the setting {name!r} is a text, a number or true/false",
                name=str(name),
            )
    return dict(raw)


def llm_cap(raw: Any) -> float | None:
    """A mapping's ``llm.cap_usd`` (REQ-2105): the source's monthly cap on what its LLM calls
    cost, USD. Refused by name when it is not a number of zero or more."""
    from provisa.codesource.bundle import BundleRefused

    if raw is None:
        return None
    cap = raw.get("cap_usd") if isinstance(raw, dict) else raw
    if cap is None:
        return None
    if isinstance(cap, bool) or not isinstance(cap, (int, float)) or cap < 0:
        raise BundleRefused("llm_cap_invalid", "a source's llm.cap_usd is a number of 0 or more")
    return float(cap)


def _is_reference(value: str) -> bool:
    return (value.startswith("${secret:") or value.startswith("${env:")) and value.endswith("}")


def issue(grant: ServiceGrant) -> str:
    """A new token for ``grant``, good until :func:`revoke`."""
    token = secrets.token_urlsafe(32)
    with _lock:
        _grants[token] = grant
    return token


def revoke(token: str) -> None:
    with _lock:
        _grants.pop(token, None)


def address(token: str) -> str:
    """The value of ``PROVISA_SERVICES`` for a worker holding ``token``."""
    return f"http://{SERVICES_HOST}/{token}"


#: The answer's content type: a secret is its value, as text; the LLM service speaks JSON.
TEXT = "text/plain; charset=utf-8"
JSON = "application/json"


#: Every service under ``$PROVISA_SERVICES`` (provisa/codesource/service_ops.py for REQ-2106's).
SERVICES = ("secrets", "llm", "settings", "cache", "vectors", "http", "log", "metric", "dq")

#: The header a worker's services request names its call by (the call's id), so the services'
#: stages report to the request that call serves (REQ-2106).
CALL_HEADER = "x-provisa-call"


def answer(
    method: str, path: str, body: bytes = b"", headers: Mapping[str, str] | None = None
) -> tuple[int, str, bytes]:
    """The answer to one request the proxy took for :data:`SERVICES_HOST`: its status, content
    type and body. A secret is its value, as text; every other service answers JSON, a refusal
    included, as an OpenAI-compatible SDK reads one."""
    from provisa.codesource.llm_service import refusal_body

    token, _, rest = path.lstrip("/").partition("/")
    service, _, sub = rest.partition("/")
    sub = sub.split("?", 1)[0]
    kind = TEXT if service == "secrets" else JSON
    try:
        grant = _grant(token)
        if service not in SERVICES:
            raise ServiceRefused(404, f"Provisa offers no service {service!r} to code")
        with (
            _call_scope(token, headers or {}),
            stage(_tracer, f"codesource.service.{service}") as span,
        ):
            span.set_attribute("codesource.source", grant.source_id)
            role = grant.role or _call_role(token, headers or {})
            return 200, kind, _dispatch(grant, service, method, sub, body, span, role)
    except ServiceRefused as refused:
        if kind == TEXT:
            return refused.status, TEXT, refused.reason.encode()
        return refused.status, JSON, refusal_body(refused.reason, refused.status)


def _call_scope(token: str, headers: Mapping[str, str]) -> Any:
    """The tracing scope of the call the request names, or none when it names none running."""
    from contextlib import nullcontext

    from provisa.codesource.worker import call_scope
    from provisa.otel_compat import in_carried_scope

    named = headers.get(CALL_HEADER, "")
    scope = call_scope(token, int(named)) if named.isdigit() else None
    return nullcontext() if scope is None else in_carried_scope(scope)


def _call_role(token: str, headers: Mapping[str, str]) -> str | None:
    """The role the request a call serves acts as, or None when the request names no call."""
    from provisa.codesource.worker import call_role

    named = headers.get(CALL_HEADER, "")
    return call_role(token, int(named)) if named.isdigit() else None


def _dispatch(
    grant: ServiceGrant,
    service: str,
    method: str,
    sub: str,
    body: bytes,
    span: Any,
    role: str | None = None,
) -> bytes:
    from provisa.codesource import llm_service, service_ops
    from provisa.core.connection_loop import connection_loop

    if service == "secrets":
        if method != "GET":
            raise ServiceRefused(405, f"a secret is read with GET, not {method}")
        name = unquote(sub)
        span.set_attribute("codesource.secret", name)  # the name, never the value
        return read_secret(grant, name).encode()
    if service == "settings":
        return service_ops.settings(grant, method, sub, span)
    if service == "log":
        return service_ops.log_line(grant, method, body, span)
    if service == "metric":
        return service_ops.metric(grant, method, body, span)
    with connection_loop() as cl:
        if service == "llm":
            return cl.run(llm_service.answer(grant, method, sub, body, span))
        if service == "cache":
            return cl.run(service_ops.cache(grant, method, sub, body, span))
        if service == "vectors":
            return cl.run(service_ops.vectors(grant, method, sub, body, span))
        if service == "dq":
            from provisa.dq import service as dq_service

            return cl.run(dq_service.answer(grant, role, method, sub, span))
        return cl.run(service_ops.http(grant, method, body, span))


def _grant(token: str) -> ServiceGrant:
    with _lock:
        grant = _grants.get(token)
    if grant is None:
        raise ServiceRefused(401, "the services token is not valid; its worker has stopped")
    return grant


def read_secret(grant: ServiceGrant, name: str) -> str:
    """The value of ``name`` for ``grant``'s source, resolved now in its org's vault, and the read
    audited. Called on the proxy's connection thread."""
    from provisa.core.connection_loop import connection_loop

    with connection_loop() as cl:
        return cl.run(_read(grant, name))


async def _read(grant: ServiceGrant, name: str) -> str:
    if name not in grant.secrets:
        await _audit(grant, name, "undeclared")
        raise ServiceRefused(
            403, f"the source {grant.source_id!r} does not declare the secret {name!r}"
        )
    reference = grant.secrets[name]
    # What a refusal names it by: the reference typed for it, or that it is the org secret.
    named = reference or "the organization secret of that name"
    try:
        value = await _resolve(grant, name, reference)
    except Exception as exc:  # allow-ble: any failure to resolve is the one refusal of the read, by name; the vault's message could carry what it read, so only its type is kept
        await _audit(grant, name, "unresolved")
        log.warning(
            "code source %s: secret %r (%s) could not be resolved: %s",
            grant.source_id,
            name,
            named,
            type(exc).__name__,
        )
        raise ServiceRefused(
            404, f"the secret {name!r} ({named}) could not be resolved in the org's vault"
        ) from None
    if not value:
        await _audit(grant, name, "unresolved")
        raise ServiceRefused(404, f"the secret {name!r} ({named}) resolves to nothing")
    await _audit(grant, name, "read")
    return value


async def _resolve(grant: ServiceGrant, name: str, reference: str | None) -> str:
    from provisa.core import secrets_store
    from provisa.core.secrets import resolve_secret_name, resolve_secrets

    async with secrets_store.bound(grant.admin_db, grant.org_id):
        if reference is None:
            return resolve_secret_name(name)
        return resolve_secrets(reference, providers=("env", "secret"))


async def _audit(grant: ServiceGrant, name: str, outcome: str) -> None:
    """One read in the org's trail: the source, the name and whether it was answered; the time
    is the row's own."""
    from provisa.api.admin.orgs_router import _org_model_db
    from provisa.core.org_membership import record_admin_action

    await record_admin_action(
        await _org_model_db(grant.org_id),
        action=SECRET_READ,
        actor_id=f"source:{grant.source_id}",
        subject_id=name,
        detail={"source": grant.source_id, "outcome": outcome},
    )
