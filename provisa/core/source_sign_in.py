# Copyright (c) 2026 Kenneth Stott
# Canary: 1e259f6c-a602-4d77-977e-5d3c6cdf1980
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Signing a source in to its issuer by a person's approval (REQ-1923).

Some sources are read with a credential only a person can grant: they approve the operator's
client at the issuer, and the issuer hands back a refresh token (RFC 6749 section 4.1). This is
that exchange, for any issuer a credential kind describes (:class:`SignInKind`):

- **start** makes the address the operator's browser is sent to, and records what the answer
  will be checked against: a ``state`` value, random, used once, good for
  :data:`STATE_LIFETIME` seconds, bound to the person who started, their organisation and
  environment, and the source being set up; and a PKCE verifier (RFC 7636, S256).
- **complete** takes the issuer's answer (``state`` and a code), consumes the state in one
  statement, exchanges the code, checks the account approved is the account named, and writes
  the refresh token to the organisation's vault under the source's own reference, through the
  writer that takes the lock the source's refreshes take.

What is pending is a row of the control plane (``source_sign_ins``), so the answer may arrive
at any process or host of the deployment. Only the state's digest is stored; the verifier is
sealed by the vault's cipher. The code, the tokens, the verifier and the client secret are
never part of a refusal, a log record or an answer.

The browser comes back to :data:`REDIRECT_PATH` on the deployment's configured public address,
never to an address read from a request.
"""

# Requirements: REQ-1923
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import re
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
from sqlalchemy import delete, insert, select, update

from provisa.core import secrets_store
from provisa.core.schema_admin import source_sign_ins
from provisa.core.secrets import resolve_secrets

if TYPE_CHECKING:
    from provisa.core.database import Database

#: How long a started sign-in may be completed, in seconds.
STATE_LIFETIME = 600
#: How long a completed sign-in's source may stay unsaved before its vault entries are removed.
UNSAVED_LIFETIME = 24 * 3600
#: Where the issuer sends the browser back to, on the host that runs sign-ins.
REDIRECT_PATH = "/source-sign-in.html"

_TIMEOUT = 10.0
#: The host label that runs every sign-in of a deployment whose hosts name organisations
#: (provisa-ui/src/lib/authHost.ts, REQ-1348): an issuer takes exact addresses only.
_SIGN_IN_LABEL = "cloud"
_ISSUER_ERROR = re.compile(r"[a-z_]{1,64}")
_IP = re.compile(r"\d+(\.\d+)*")


class SignInRefused(Exception):
    """A sign-in that was not started or not completed, with a stable code for the caller."""

    def __init__(self, code: str, message: str, *, status: int = 400, **params: str) -> None:
        super().__init__(message)
        self.code = f"source_sign_in.{code}"
        self.status = status
        self.params = params


#: An issuer's address, or how to make it from a sign-in's settings.
Endpoint = str | Callable[[Mapping[str, str]], str]


def _address(endpoint: Endpoint, settings: Mapping[str, str]) -> str:
    return endpoint if isinstance(endpoint, str) else endpoint(settings)


@dataclass(frozen=True)
class SignInKind:
    """What is particular to one issuer."""

    id: str
    #: Where the operator approves and where a code is exchanged: one address, or, for an
    #: issuer whose addresses depend on the source (a tenant in their path), how to make it
    #: from the settings the sign-in was started with.
    authorization_endpoint: Endpoint
    token_endpoint: Endpoint
    #: What the issuer is asked beside the standard parameters, for the account named.
    authorization_params: Callable[[str], dict[str, str]]
    #: The account an access token was approved by, as the issuer names it.
    approved_account: Callable[[str], Awaitable[str]]


KINDS: dict[str, SignInKind] = {}


def register_kind(kind: SignInKind) -> None:
    KINDS[kind.id] = kind


@dataclass(frozen=True)
class Started:
    authorization_url: str
    expires_in: int
    #: The origin of the address the issuer returns the browser to.
    return_origin: str


@dataclass(frozen=True)
class Completed:
    source_id: str
    account: str
    #: What the source's settings hold for its credential: a reference into the vault.
    refresh_token: str


#: Stores a source's refresh token (source id, vault name, token) where its refreshes read it,
#: under the lock they take, and returns the reference that names it.
StoreRefreshToken = Callable[[str, str, str], Awaitable[str]]
#: Removes a vault entry this sign-in wrote, unless a stored value still names it.
ForgetSecret = Callable[[str], Awaitable[None]]


def redirect_address(public_address: str | None) -> str:
    """Where the issuer sends the browser back to: :data:`REDIRECT_PATH` on the deployment's
    configured public address, moved to the sign-in host where hosts name organisations."""
    configured = (public_address or "").strip().rstrip("/")
    parsed = urlsplit(configured)
    if not configured or not parsed.scheme or not parsed.hostname:
        raise SignInRefused(
            "public_address_not_set",
            "This deployment's public address is not set (mail.base_url), so the address the "
            "issuer returns to cannot be stated.",
        )
    labels = parsed.hostname.split(".")
    host = parsed.hostname
    if len(labels) >= 3 and not _IP.fullmatch(host):
        host = ".".join([_SIGN_IN_LABEL, *labels[1:]])
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urlunsplit((parsed.scheme, netloc, REDIRECT_PATH, "", ""))


def _digest(state: str) -> str:
    return hashlib.sha256(state.encode()).hexdigest()


def _challenge(verifier: str) -> str:
    return (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _aware(moment: dt.datetime) -> dt.datetime:
    # SQLite answers a stored moment without its zone; every moment here is stored in UTC.
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=dt.timezone.utc)


def _kind(kind_id: str) -> SignInKind:
    if kind_id not in KINDS:
        raise SignInRefused("unknown_kind", f"No sign-in of kind {kind_id!r} exists", kind=kind_id)
    return KINDS[kind_id]


async def start(
    admin_db: "Database",
    *,
    org_id: str,
    env: str,
    user_id: str,
    source_id: str,
    kind_id: str,
    account: str,
    scopes: list[str],
    client_id: str,
    client_secret_name: str,
    refresh_token_name: str,
    public_address: str | None,
    settings: Mapping[str, str] | None = None,
) -> Started:
    """Record a sign-in and make the address the operator's browser is sent to.

    The client is the organisation's (``core.mail_platforms``): ``client_id``, and the vault
    name its secret is kept under, which is all that is recorded of it. ``refresh_token_name``
    is the vault name the source's refresh token will be kept under."""
    kind = _kind(kind_id)
    settings = dict(settings or {})
    if not (source_id and account and client_id and client_secret_name and scopes):
        raise SignInRefused("incomplete", "A sign-in needs the source, the account and a client")
    address = redirect_address(public_address)
    authorization_endpoint = _address(kind.authorization_endpoint, settings)
    secret_name, token_name = client_secret_name, refresh_token_name
    state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            insert(source_sign_ins).values(
                state_digest=_digest(state),
                org_id=org_id,
                env=env,
                user_id=user_id,
                source_id=source_id,
                kind=kind.id,
                account=account,
                scopes=" ".join(scopes),
                client_id=client_id,
                client_secret_name=secret_name,
                refresh_token_name=token_name,
                verifier=secrets_store.seal(verifier),
                redirect_address=address,
                settings=json.dumps(settings, sort_keys=True),
                expires_at=_now() + dt.timedelta(seconds=STATE_LIFETIME),
            )
        )
    query = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": address,
        "scope": " ".join(scopes),
        "state": state,
        "code_challenge": _challenge(verifier),
        "code_challenge_method": "S256",
        **kind.authorization_params(account),
    }
    returned_to = urlsplit(address)
    return Started(
        f"{authorization_endpoint}?{urlencode(query)}",
        STATE_LIFETIME,
        f"{returned_to.scheme}://{returned_to.netloc}",
    )


async def _consume(admin_db: "Database", state: str, *, org_id: str, user_id: str) -> dict:
    """The pending sign-in ``state`` names, taken so that it cannot be taken again."""
    digest = _digest(state)
    async with admin_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(source_sign_ins).where(source_sign_ins.c.state_digest == digest)
            )
        ).fetchone()
        if row is None:
            raise SignInRefused("unknown_state", "This sign-in was not started here")
        pending = dict(row._mapping)
        if pending["org_id"] != org_id or pending["user_id"] != user_id:
            raise SignInRefused(
                "state_not_yours",
                "This sign-in was started by someone else or in another organisation",
                status=403,
            )
        if pending["used_at"] is not None:
            raise SignInRefused("state_used", "This sign-in has already been completed")
        if _aware(pending["expires_at"]) <= _now():
            raise SignInRefused(
                "state_expired", "This sign-in was not completed in time; start it again"
            )
        taken = await conn.execute_core(
            update(source_sign_ins)
            .where(source_sign_ins.c.state_digest == digest, source_sign_ins.c.used_at.is_(None))
            .values(used_at=_now())
        )
        if taken.rowcount != 1:  # another process completed it between the read and the write
            raise SignInRefused("state_used", "This sign-in has already been completed")
    return pending


def _issuer_error(answer: httpx.Response) -> str:
    """The issuer's name for its refusal (RFC 6749 section 5.2), and nothing else it said."""
    try:
        body = answer.json()
    except ValueError:
        return f"HTTP {answer.status_code}"
    named = body.get("error") if isinstance(body, dict) else None
    if isinstance(named, str) and _ISSUER_ERROR.fullmatch(named):
        return named
    return f"HTTP {answer.status_code}"


async def _exchange(admin_db: "Database", kind: SignInKind, pending: dict, code: str) -> dict:
    async with secrets_store.bound(admin_db, pending["org_id"]):
        client_secret = resolve_secrets(_reference(pending["client_secret_name"]))
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": pending["client_id"],
        "client_secret": client_secret,
        "redirect_uri": pending["redirect_address"],
        "code_verifier": secrets_store.unseal(pending["verifier"]),
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        answer = await client.post(
            _address(kind.token_endpoint, json.loads(pending["settings"])), data=form
        )
    if answer.status_code != 200:
        raise SignInRefused(
            "refused_by_issuer",
            f"The issuer refused the sign-in: {_issuer_error(answer)}",
            error=_issuer_error(answer),
        )
    return answer.json()


def _reference(name: str) -> str:
    return f"${{secret:{name}}}"


async def complete(
    admin_db: "Database",
    *,
    org_id: str,
    user_id: str,
    state: str,
    code: str | None,
    error: str | None,
    store_refresh_token: StoreRefreshToken,
) -> Completed:
    """Finish the sign-in ``state`` names with the issuer's answer: a code, or its refusal."""
    pending = await _consume(admin_db, state, org_id=org_id, user_id=user_id)
    if error or not code:
        named = error if error and _ISSUER_ERROR.fullmatch(error) else "no_code"
        raise SignInRefused(
            "refused_by_issuer", f"The issuer refused the sign-in: {named}", error=named
        )
    kind = _kind(pending["kind"])
    answered = await _exchange(admin_db, kind, pending, code)
    refresh_token = answered.get("refresh_token")
    if not refresh_token:
        raise SignInRefused(
            "no_refresh_token",
            "The issuer approved the sign-in without a standing approval, so the source could "
            "not be read later. Approve again, allowing access while you are away.",
        )
    approved = (await kind.approved_account(answered["access_token"])).strip().lower()
    if approved != pending["account"].strip().lower():
        raise SignInRefused(
            "wrong_account",
            f"{approved} approved the sign-in, and the source reads {pending['account']}",
            approved=approved,
            account=pending["account"],
        )
    reference = await store_refresh_token(
        pending["source_id"], pending["refresh_token_name"], refresh_token
    )
    return Completed(
        source_id=pending["source_id"],
        account=approved,
        refresh_token=reference,
    )


async def sweep(
    admin_db: "Database",
    *,
    org_id: str,
    env: str,
    source_exists: Callable[[str], Awaitable[bool]],
    forget_secret: ForgetSecret,
) -> None:
    """Forget the organisation's sign-ins that came to nothing: never completed in time, or
    completed for a source that was not saved within :data:`UNSAVED_LIFETIME`.

    Only the refresh token a sign-in recorded is removed, and only while no source of its id
    exists; a sign-in whose source was saved just loses its row. The client's secret is the
    organisation's and is never removed here."""
    now = _now()
    async with admin_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(source_sign_ins).where(
                    source_sign_ins.c.org_id == org_id, source_sign_ins.c.env == env
                )
            )
        ).fetchall()
    sign_ins = [dict(row._mapping) for row in rows]

    def over(pending: dict) -> bool:
        used = pending["used_at"]
        if used is None:
            return _aware(pending["expires_at"]) <= now
        return _aware(used) + dt.timedelta(seconds=UNSAVED_LIFETIME) <= now

    # A source being signed in again shares its vault names with the attempt before it.
    live = {p["source_id"] for p in sign_ins if not over(p)}
    for pending in sign_ins:
        if not over(pending):
            continue
        if pending["source_id"] not in live and not await source_exists(pending["source_id"]):
            # The client's secret is the organisation's and is not this sign-in's to remove.
            await forget_secret(pending["refresh_token_name"])
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                delete(source_sign_ins).where(
                    source_sign_ins.c.state_digest == pending["state_digest"]
                )
            )
