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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
from sqlalchemy import delete, insert, select, update

from provisa.core import secrets_store
from provisa.core.schema_admin import source_sign_ins

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
#: Where a pending sign-in's settings hold a source's own client secret, sealed.
_SEALED_SECRET = "client_secret_sealed"
#: An account an issuer names in its return address: an identifier, never markup or a path.
_RETURNED_ACCOUNT = re.compile(r"[A-Za-z0-9_.@-]{1,128}")
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
    #: REQ-1994: what the issuer is asked to grant, from the source's settings as the form
    #: states them -- the scopes of what the source reads, and nothing the form chose.
    scopes: Callable[[Mapping[str, Any]], list[str]]
    #: The account an access token was approved by, as the issuer names it. None for an issuer
    #: that names it in the address it returns the browser to (:attr:`returned_account`).
    approved_account: Callable[[str], Awaitable[str]] | None = None
    #: REQ-2197: the parameter of the return address that names the account the operator chose
    #: at the issuer (Intuit's ``realmId``, the company approved). That account is the one the
    #: approval is kept for, whatever the source named before.
    returned_account: str | None = None
    #: REQ-2197: the client is the source's own (its settings), not the organisation's
    #: mail-platform entry: its secret, as the form holds it, is sealed in the pending sign-in.
    client_of_source: bool = False
    #: The client authenticates to the token endpoint with HTTP Basic (RFC 6749 section
    #: 2.3.1), not with its credentials in the form.
    basic_auth: bool = False
    #: Whether the issuer's answer says what it grants. One that does not (Intuit) grants what
    #: it was asked or refuses the sign-in.
    states_scope: bool = True


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


#: Keeps the approval a sign-in produced (source id, account, refresh token, the scopes the
#: issuer says it grants) where the source's reads take it from: the organisation's state
#: store, under the lock a renewal takes.
KeepGrant = Callable[[str, str, str, list[str]], Awaitable[None]]
#: Forgets the approval kept for (source id, account).
ForgetGrant = Callable[[str, str], Awaitable[None]]


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


def _addressing(settings: Mapping[str, str]) -> dict[str, str]:
    """What of a sign-in's settings its issuer's addresses are made from: never the client."""
    return {k: v for k, v in settings.items() if k != _SEALED_SECRET}


async def _source_client_secret(entered: str) -> str:
    """A source's own client secret as its settings hold it, resolved in the vault of the
    organisation completing the sign-in."""
    from provisa.core.secrets import resolve_secrets

    async with secrets_store.bound_to_request_org():
        return resolve_secrets(entered)


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
    mapping: Mapping[str, Any],
    client_id: str,
    public_address: str | None,
    settings: Mapping[str, str] | None = None,
    client_secret: str | None = None,
) -> Started:
    """Record a sign-in and make the address the operator's browser is sent to.

    The client is the organisation's (``core.mail_platforms``): only its ``client_id`` is
    recorded. Its secret is read from the organisation's entry when the sign-in is completed.
    A kind whose client is the source's own (:attr:`SignInKind.client_of_source`) is given its
    ``client_secret`` as the source's settings hold it (a value or a ``${secret:...}``
    reference); it is sealed by the vault's cipher until the code is exchanged."""
    kind = _kind(kind_id)
    settings = dict(settings or {})
    scopes = kind.scopes(mapping)
    named = bool(account) or kind.returned_account is not None
    if not (source_id and named and client_id and scopes):
        raise SignInRefused("incomplete", "A sign-in needs the source, the account and a client")
    if client_secret and not kind.client_of_source:
        raise SignInRefused(
            "client_not_taken", "This kind of source signs in with the organisation's client"
        )
    if kind.client_of_source and not client_secret:
        raise SignInRefused("incomplete", "A sign-in needs the source, the account and a client")
    if client_secret:
        settings[_SEALED_SECRET] = base64.b64encode(secrets_store.seal(client_secret)).decode()
    address = redirect_address(public_address)
    authorization_endpoint = _address(kind.authorization_endpoint, _addressing(settings))
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
                # Two columns of an earlier design, which named vault entries. Nothing reads
                # them; they are written empty because an existing control plane holds them
                # NOT NULL and its schema is only ever added to (no migration in V1).
                client_secret_name="",
                refresh_token_name="",
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


async def _client_secret(admin_db: "Database", kind: SignInKind, pending: dict) -> str:
    settings = json.loads(pending["settings"])
    if kind.client_of_source:
        # The source's own, as the form held it when the sign-in started.
        entered = secrets_store.unseal(base64.b64decode(settings[_SEALED_SECRET]))
        return await _source_client_secret(entered)
    from provisa.core import mail_platforms

    # The organisation's entry as it stands now: its client secret as its administrator entered
    # it, resolved in that organisation's vault.
    entry = await mail_platforms.require(admin_db, pending["org_id"], kind.id)
    return await mail_platforms.credential(
        admin_db, pending["org_id"], kind.id, mail_platforms.CLIENT_SECRET, entry.client_secret
    )


async def _exchange(admin_db: "Database", kind: SignInKind, pending: dict, code: str) -> dict:
    client_secret = await _client_secret(admin_db, kind, pending)
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": pending["redirect_address"],
        "code_verifier": secrets_store.unseal(pending["verifier"]),
    }
    client_auth: dict[str, Any] = {}
    if kind.basic_auth:
        client_auth["auth"] = (pending["client_id"], client_secret)
    else:
        form.update(client_id=pending["client_id"], client_secret=client_secret)
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        answer = await client.post(
            _address(kind.token_endpoint, _addressing(json.loads(pending["settings"]))),
            data=form,
            headers={"Accept": "application/json"},
            **client_auth,
        )
    if answer.status_code != 200:
        raise SignInRefused(
            "refused_by_issuer",
            f"The issuer refused the sign-in: {_issuer_error(answer)}",
            error=_issuer_error(answer),
        )
    return answer.json()


async def complete(
    admin_db: "Database",
    *,
    org_id: str,
    user_id: str,
    state: str,
    code: str | None,
    error: str | None,
    keep_grant: KeepGrant,
    returned: Mapping[str, str | None] | None = None,
) -> Completed:
    """Finish the sign-in ``state`` names with the issuer's answer: a code, or its refusal,
    and what else the issuer put in the return address (``returned``: Intuit's ``realmId``).
    The approval the issuer gives is sign-in state: it is handed to ``keep_grant`` and is in
    no answer."""
    pending = await _consume(admin_db, state, org_id=org_id, user_id=user_id)
    if error or not code:
        named = error if error and _ISSUER_ERROR.fullmatch(error) else "no_code"
        raise SignInRefused(
            "refused_by_issuer", f"The issuer refused the sign-in: {named}", error=named
        )
    kind = _kind(pending["kind"])
    chosen = None
    if kind.returned_account is not None:
        chosen = ((returned or {}).get(kind.returned_account) or "").strip()
        if not _RETURNED_ACCOUNT.fullmatch(chosen):
            raise SignInRefused(
                "no_account_returned",
                f"The issuer approved the sign-in without naming the account approved "
                f"({kind.returned_account}). Approve again.",
                parameter=kind.returned_account,
            )
    answered = await _exchange(admin_db, kind, pending, code)
    refresh_token = answered.get("refresh_token")
    if not refresh_token:
        raise SignInRefused(
            "no_refresh_token",
            "The issuer approved the sign-in without a standing approval, so the source could "
            "not be read later. Approve again, allowing access while you are away.",
        )
    if chosen is not None:
        # The account the operator chose at the issuer is the one the source reads from now on.
        approved = chosen
    else:
        assert kind.approved_account is not None, f"kind {kind.id} names no approved account"
        approved = (await kind.approved_account(answered["access_token"])).strip().lower()
        if approved != pending["account"].strip().lower():
            raise SignInRefused(
                "wrong_account",
                f"{approved} approved the sign-in, and the source reads {pending['account']}",
                approved=approved,
                account=pending["account"],
            )
    # REQ-1994: what the approval grants, as the issuer says, so a source that asks for more
    # than it grants is told to sign in again rather than refused call by call. An issuer that
    # does not say grants what it was asked.
    scope = answered.get("scope") if kind.states_scope else pending["scopes"]
    if not isinstance(scope, str):
        raise SignInRefused(
            "no_scope",
            "The issuer approved the sign-in without saying what it grants. Approve again.",
        )
    await keep_grant(pending["source_id"], approved, refresh_token, scope.split())
    if approved != pending["account"]:
        # What a sweep forgets, should the source not be saved, is what was kept.
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                update(source_sign_ins)
                .where(source_sign_ins.c.state_digest == pending["state_digest"])
                .values(account=approved)
            )
    return Completed(source_id=pending["source_id"], account=approved)


async def sweep(
    admin_db: "Database",
    *,
    org_id: str,
    env: str,
    source_exists: Callable[[str], Awaitable[bool]],
    forget_grant: ForgetGrant,
) -> None:
    """Forget the organisation's sign-ins that came to nothing: never completed in time, or
    completed for a source that was not saved within :data:`UNSAVED_LIFETIME`.

    The approval a sign-in kept goes with it only while no source of its id exists and the
    source is not being signed in again; a sign-in whose source was saved just loses its row."""
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

    # A source being signed in again shares its approval with the attempt before it.
    live = {p["source_id"] for p in sign_ins if not over(p)}
    for pending in sign_ins:
        if not over(pending):
            continue
        if pending["source_id"] not in live and not await source_exists(pending["source_id"]):
            await forget_grant(pending["source_id"], pending["account"])
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                delete(source_sign_ins).where(
                    source_sign_ins.c.state_digest == pending["state_digest"]
                )
            )
