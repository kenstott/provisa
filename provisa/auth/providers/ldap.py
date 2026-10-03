# Copyright (c) 2026 Kenneth Stott
# Canary: d6321f2b-6b16-406a-ab30-d86cd1425e6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LDAP directory auth provider (REQ-1265).

A user signs in with the directory's own username and password. The sequence is the standard
search-then-bind:

1. Bind as the configured service account.
2. Search ``user_base_dn`` with ``user_filter`` for the one entry the username names.
3. Bind as that entry with the presented password. The directory, not Provisa, checks it.
4. Search ``group_base_dn`` with ``group_filter`` for the groups the entry belongs to. Their
   names are the identity's roles, and ``groups`` in its claims for the role-mapping rules.

Two credential presentations, as for the local-accounts provider:

* ``Authorization: Basic`` -- API clients, pgwire and Bolt, which carry a username and password.
* ``Authorization: Bearer <session token>`` -- the browser, after ``POST /auth/login``. The
  account and its groups are re-read from the directory when the token comes back, so a user
  removed from the directory, or from a group, loses that access before the token expires.

A refused password, an unknown user and an empty password are all ``ValueError`` (a 401). A
directory that cannot be reached, a service account that cannot bind and a filter that matches
more than one entry are operator faults and propagate.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import ssl
from dataclasses import dataclass

from ldap3 import SUBTREE, Connection, Server, Tls
from ldap3.utils.conv import escape_filter_chars

from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.auth.session_token import SessionTokens

# Requirements: REQ-1265

# Seconds allowed for the TCP connect and for each directory response.
_CONNECT_TIMEOUT = 10
_RECEIVE_TIMEOUT = 10


@dataclass(frozen=True)
class LdapSettings:  # REQ-1265
    """The ``auth.ldap`` block.

    server_url: ``ldap://host:389`` or ``ldaps://host:636``.
    bind_dn / bind_password: the service account that searches for users and groups.
    user_base_dn: the subtree users are searched under.
    user_filter: the search filter for one user; ``{username}`` is the escaped login name,
        e.g. ``(uid={username})`` or ``(sAMAccountName={username})``.
    user_id_attribute: the attribute whose value is the stable user id, e.g. ``uid`` or
        ``entryUUID``.
    email_attribute / display_name_attribute: read when set; the identity carries no email or
        display name otherwise.
    group_base_dn / group_filter / group_name_attribute: set together or not at all. The filter
        takes ``{user_dn}`` and ``{username}``, e.g. ``(member={user_dn})``. Without them the
        identity carries no groups.
    start_tls: upgrade an ``ldap://`` connection with StartTLS before any bind.
    ca_cert_file: a CA bundle for a directory whose certificate the system store does not trust.
    """

    server_url: str
    bind_dn: str
    bind_password: str
    user_base_dn: str
    user_filter: str
    user_id_attribute: str
    email_attribute: str | None = None
    display_name_attribute: str | None = None
    group_base_dn: str | None = None
    group_filter: str | None = None
    group_name_attribute: str | None = None
    start_tls: bool = False
    ca_cert_file: str | None = None

    def __post_init__(self) -> None:
        if "{username}" not in self.user_filter:
            raise ValueError("auth.ldap.user_filter must contain {username}")
        group_fields = (self.group_base_dn, self.group_filter, self.group_name_attribute)
        if any(group_fields) and not all(group_fields):
            raise ValueError(
                "auth.ldap.group_base_dn, group_filter and group_name_attribute are set "
                "together or not at all"
            )
        if self.start_tls and self.server_url.lower().startswith("ldaps://"):
            raise ValueError("auth.ldap.start_tls applies to an ldap:// server_url, not ldaps://")


@dataclass(frozen=True)
class _Entry:
    dn: str
    attributes: dict


def _first(value) -> str | None:
    """An attribute's single value. ldap3 hands back a list for a multi-valued attribute."""
    if isinstance(value, (list, tuple)):
        return str(value[0]) if value else None
    return None if value is None else str(value)


class LdapAuthProvider(AuthProvider):  # REQ-1265
    """Authenticates a username and password against an LDAP directory."""

    provider_name: str = "ldap"

    def __init__(self, settings: LdapSettings, session_secret: str | None = None) -> None:
        self._settings = settings
        self._sessions = SessionTokens(session_secret, audience=self.provider_name)
        tls = None
        if settings.start_tls or settings.server_url.lower().startswith("ldaps://"):
            tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=settings.ca_cert_file)
        self._server = Server(settings.server_url, tls=tls, connect_timeout=_CONNECT_TIMEOUT)

    @property
    def auth_scheme(self) -> str:
        return "basic"

    @property
    def token_validators(self):
        """Credential presentation -> the validator that accepts it. Read by AuthMiddleware."""
        return {"basic": self.validate_token, "bearer": self.validate_bearer}

    async def validate_bearer(self, token: str) -> AuthIdentity:  # REQ-1263
        """A bearer credential: a personal access token, else this provider's session token."""
        return await self._with_pat(token, self.validate_session_token)

    # -- directory access (blocking; called through asyncio.to_thread) ------------------------

    def _connect(self, dn: str, password: str) -> Connection:
        """A connection that has attempted its bind; the caller reads ``bound``."""
        conn = Connection(
            self._server,
            user=dn,
            password=password,
            receive_timeout=_RECEIVE_TIMEOUT,
            raise_exceptions=False,
        )
        if self._settings.start_tls:
            conn.open()
            if not conn.start_tls():
                raise RuntimeError(f"LDAP StartTLS failed: {conn.result}")
        conn.bind()
        return conn

    def _service_connection(self) -> Connection:
        conn = self._connect(self._settings.bind_dn, self._settings.bind_password)
        if not conn.bound:
            raise RuntimeError(
                f"LDAP service account {self._settings.bind_dn!r} could not bind: "
                f"{conn.result.get('description')}"
            )
        return conn

    def _user_attributes(self) -> list[str]:
        s = self._settings
        return [a for a in (s.user_id_attribute, s.email_attribute, s.display_name_attribute) if a]

    def _find_user(self, service: Connection, username: str) -> _Entry:
        s = self._settings
        service.search(
            search_base=s.user_base_dn,
            search_filter=s.user_filter.replace("{username}", escape_filter_chars(username)),
            search_scope=SUBTREE,
            attributes=self._user_attributes(),
        )
        entries = [e for e in service.response or [] if e.get("type") == "searchResEntry"]
        if not entries:
            raise ValueError("Invalid credentials")
        if len(entries) > 1:
            raise RuntimeError(
                f"auth.ldap.user_filter matched {len(entries)} entries for one username; "
                "it must identify exactly one"
            )
        return _Entry(dn=entries[0]["dn"], attributes=dict(entries[0]["attributes"]))

    def _groups(self, service: Connection, user: _Entry, username: str) -> list[str]:
        s = self._settings
        if not s.group_base_dn:
            return []
        assert s.group_filter is not None and s.group_name_attribute is not None
        service.search(
            search_base=s.group_base_dn,
            search_filter=s.group_filter.replace("{user_dn}", escape_filter_chars(user.dn)).replace(
                "{username}", escape_filter_chars(username)
            ),
            search_scope=SUBTREE,
            attributes=[s.group_name_attribute],
        )
        names = []
        for entry in service.response or []:
            if entry.get("type") != "searchResEntry":
                continue
            name = _first(entry["attributes"].get(s.group_name_attribute))
            if name:
                names.append(name)
        return sorted(names)

    def _identity(self, user: _Entry, username: str, groups: list[str]) -> AuthIdentity:
        s = self._settings
        user_id = _first(user.attributes.get(s.user_id_attribute))
        if not user_id:
            raise RuntimeError(
                f"LDAP entry {user.dn!r} has no {s.user_id_attribute!r} attribute "
                "(auth.ldap.user_id_attribute)"
            )
        email = _first(user.attributes.get(s.email_attribute)) if s.email_attribute else None
        display_name = (
            _first(user.attributes.get(s.display_name_attribute))
            if s.display_name_attribute
            else None
        )
        return AuthIdentity(
            user_id=user_id,
            email=email,
            display_name=display_name,
            roles=groups,
            raw_claims={"username": username, "dn": user.dn, "email": email, "groups": groups},
        )

    def _authenticate_blocking(self, username: str, password: str) -> AuthIdentity:
        # RFC 4513 5.1.2: a bind with a DN and an empty password is an "unauthenticated bind",
        # which a directory may accept. It proves nothing about the user, so it never reaches one.
        if not username or not password:
            raise ValueError("Invalid credentials")
        service = self._service_connection()
        try:
            user = self._find_user(service, username)
            as_user = self._connect(user.dn, password)
            try:
                if not as_user.bound:
                    raise ValueError("Invalid credentials")
            finally:
                as_user.unbind()
            return self._identity(user, username, self._groups(service, user, username))
        finally:
            service.unbind()

    def _lookup_blocking(self, username: str) -> AuthIdentity:
        service = self._service_connection()
        try:
            user = self._find_user(service, username)
            return self._identity(user, username, self._groups(service, user, username))
        finally:
            service.unbind()

    # -- the provider interface ---------------------------------------------------------------

    async def authenticate(self, username: str, password: str) -> AuthIdentity:
        return await asyncio.to_thread(self._authenticate_blocking, username, password)

    async def issue_session_token(self, username: str, password: str) -> str:
        """Verify a password against the directory and mint the browser's session token."""
        identity = await self.authenticate(username, password)
        return self._sessions.issue({"sub": identity.user_id, "username": username})

    async def password_login(self, username: str, password: str) -> str:
        """The ``POST /auth/login`` exchange (provisa/auth/login_router.py)."""
        return await self.issue_session_token(username, password)

    async def validate_session_token(self, token: str) -> AuthIdentity:
        """Validate a session token minted by ``issue_session_token``.

        The entry and its groups are re-read rather than trusted from the token: a user removed
        from the directory or from a group loses that access at once.
        """
        claims = self._sessions.verify(token)
        identity = await asyncio.to_thread(self._lookup_blocking, claims["username"])
        if identity.user_id != claims["sub"]:
            # The login name now resolves to a different account than the one that signed in.
            raise ValueError("Invalid credentials")
        return identity

    async def validate_token(self, token: str) -> AuthIdentity:
        """Validate an ``Authorization: Basic`` credential -- b64(username:password)."""
        try:
            decoded = base64.b64decode(token).decode("utf-8")
            username, password = decoded.split(":", 1)
        except (ValueError, binascii.Error, UnicodeDecodeError):
            raise ValueError("Invalid credentials")
        return await self.authenticate(username, password)
