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

A code source declares the names its code may read (``secrets`` in its mapping), each a reference
into the org's vault: ``${secret:NAME}`` or ``${env:NAME}``. The reference is kept as written
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

# Requirements: REQ-2104
from __future__ import annotations

import logging
import secrets
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

log = logging.getLogger(__name__)

#: The host the egress proxy answers itself. It resolves nowhere, so only the proxy reaches it.
SERVICES_HOST = "provisa-services"

#: The one variable a worker is given to reach Provisa's services.
SERVICES_ENV = "PROVISA_SERVICES"

#: The audit trail's action for one read of a declared secret.
SECRET_READ = "code_source.secret.read"


@dataclass(frozen=True)
class ServiceGrant:
    """What one source's worker may ask for: its declared names, each the vault reference it
    reads, in the org it is registered in."""

    org_id: str
    source_id: str
    secrets: Mapping[str, str] = field(default_factory=dict)
    admin_db: Any = None


class ServiceRefused(Exception):
    """A request the services answer with a refusal: its HTTP status, and a reason that names
    what was asked for, never a value."""

    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


_grants: dict[str, ServiceGrant] = {}
_lock = threading.Lock()


def declared(raw: Any) -> dict[str, str]:
    """A mapping's ``secrets``, as name -> reference. A bare name is the org secret of that name;
    a mapping names each reference itself. Refused by name when it is neither."""
    from provisa.codesource.bundle import BundleRefused

    if raw is None:
        return {}
    if isinstance(raw, list):
        if not all(isinstance(n, str) and n for n in raw):
            raise BundleRefused("secrets_invalid", "a source's secrets are names")
        return {n: f"${{secret:{n}}}" for n in raw}
    if isinstance(raw, dict):
        for name, reference in raw.items():
            if not (isinstance(reference, str) and _is_reference(reference)):
                raise BundleRefused(
                    "secrets_invalid",
                    f"the secret {name!r} is a reference, ${{secret:NAME}} or ${{env:NAME}}",
                    name=str(name),
                )
        return dict(raw)
    raise BundleRefused("secrets_invalid", "a source's secrets are a list of names or a mapping")


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


def answer(method: str, path: str) -> tuple[int, bytes]:
    """The answer to one request the proxy took for :data:`SERVICES_HOST`: its status and body."""
    try:
        return 200, _answer(method, path).encode()
    except ServiceRefused as refused:
        return refused.status, refused.reason.encode()


def _answer(method: str, path: str) -> str:
    token, _, rest = path.lstrip("/").partition("/")
    with _lock:
        grant = _grants.get(token)
    if grant is None:
        raise ServiceRefused(401, "the services token is not valid; its worker has stopped")
    service, _, name = rest.partition("/")
    if service != "secrets":
        raise ServiceRefused(404, f"Provisa offers no service {service!r} to code")
    if method != "GET":
        raise ServiceRefused(405, f"a secret is read with GET, not {method}")
    name = unquote(name.split("?", 1)[0])
    return read_secret(grant, name)


def read_secret(grant: ServiceGrant, name: str) -> str:
    """The value of ``name`` for ``grant``'s source, resolved now in its org's vault, and the read
    audited. Called on the proxy's connection thread."""
    from provisa.core.connection_loop import connection_loop

    with connection_loop() as cl:
        return cl.run(_read(grant, name))


async def _read(grant: ServiceGrant, name: str) -> str:
    reference = grant.secrets.get(name)
    if reference is None:
        await _audit(grant, name, "undeclared")
        raise ServiceRefused(
            403, f"the source {grant.source_id!r} does not declare the secret {name!r}"
        )
    try:
        value = await _resolve(grant, reference)
    except Exception as exc:  # allow-ble: any failure to resolve is the one refusal of the read, by name; the vault's message could carry what it read, so only its type is kept
        await _audit(grant, name, "unresolved")
        log.warning(
            "code source %s: secret %r (%s) could not be resolved: %s",
            grant.source_id,
            name,
            reference,
            type(exc).__name__,
        )
        raise ServiceRefused(
            404, f"the secret {name!r} ({reference}) could not be resolved in the org's vault"
        ) from None
    if not value:
        await _audit(grant, name, "unresolved")
        raise ServiceRefused(404, f"the secret {name!r} ({reference}) resolves to nothing")
    await _audit(grant, name, "read")
    return value


async def _resolve(grant: ServiceGrant, reference: str) -> str:
    from provisa.core import secrets_store
    from provisa.core.secrets import resolve_secrets

    async with secrets_store.bound(grant.admin_db, grant.org_id):
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
