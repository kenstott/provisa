# Copyright (c) 2026 Kenneth Stott
# Canary: 5a943725-8f59-4395-bb37-2954c1fffd4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PROVISA'S OWN SECRETS SERVICE, for the deployment with no central one (REQ-1557, REQ-1558).

``${secret:NAME}`` names the configured secrets backend. When a deployment is wired to Vault or a
cloud secrets manager that backend answers; when it is not -- the open-source install, the demo,
and every hosted org that does not own the server's process environment -- THIS answers, and it is
the default rather than the degraded case.

WHAT AUTHORIZES A READ. Not a key of this table's own: the stored value is an envelope blob
(REQ-685), so the authority to read it is the encryption master key the process holds. A control
plane copied without that key holds ciphertext and nothing else. The key is provisioned on first
write when the host has a keychain to hold it, and demanded explicitly when it does not -- a key
this module minted but could not persist would encrypt one secret and lose it, which is worse than
refusing.

NAMES GO IN, VALUES NEVER COME BACK OUT (REQ-1558). Nothing here returns a stored value to a
caller who asks for it by name. The only path out is ``resolve``, reached through the reference
grammar at the moment a secret is used, inside a request already bound to the one org that owns it.

TWO VAULTS, ONE SERVICE (REQ-1560). Every row names its OWNER: ``ORG_OWNER`` for the org vault,
whose one value every member resolves alike, or a user id for that person's own vault. The scope
is part of the reference rather than a permission around it -- ``${secret:NAME}`` reads the org
vault and ``${user:NAME}`` reads the acting person's -- so there is no syntax for "somebody else's
secret" to write down, and neither scope ever answers for the other.
"""

# Requirements: REQ-125, REQ-557, REQ-684, REQ-685, REQ-1557, REQ-1558, REQ-1560

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import delete as sql_delete
from sqlalchemy import func, select

from provisa.core.schema_admin import secrets_store
from provisa.core.secrets import SecretsProvider

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.encryption.service import EncryptionService

#: A name is a reference a person types into a config field, so it is held to what the reference
#: grammar can carry back out unambiguously: ``${secret:NAME}`` ends at the first ``}``, and a name
#: with a brace, a space or a colon in it would produce a reference that reads as something else.
NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: REQ-1560: the owner id standing for the ORG's own vault -- the one every member of the org
#: resolves ``${secret:NAME}`` against. A real user id never collides with it: ``*`` is the same
#: wildcard the domain grants use and is not a shape any identity provider issues.
ORG_OWNER = "*"


@dataclass(frozen=True)
class _Binding:
    """What a bound operation may resolve: the org's vault, and the acting person's (REQ-1560).

    ``user_id`` is None when the operation has no acting person -- a scheduled refresh, a CLI run
    against a service identity. Personal resolution then RAISES naming the scope, because a
    ``${user:...}`` with nobody acting has no correct answer and the org value is not it.
    """

    org_id: str
    org_values: dict[str, str]
    user_id: str | None
    user_values: dict[str, str]


#: The org whose secrets ``${secret:...}`` resolves against, and the maps it resolves from. Bound
#: for the duration of one operation by ``bound``; unbound outside it, so a resolution that
#: happens where no org was established RAISES rather than reaching into some other org's names.
_bound: ContextVar[_Binding | None] = ContextVar("provisa_secrets_store", default=None)


@dataclass(frozen=True)
class SecretInfo:
    """Everything about a secret except the one thing it holds."""

    name: str
    description: str | None
    created_at: datetime | None
    updated_at: datetime | None
    updated_by: str | None
    #: REQ-1560: ORG_OWNER, or the id of the person whose vault holds it.
    owner_id: str = ORG_OWNER

    @property
    def personal(self) -> bool:
        return self.owner_id != ORG_OWNER

    def as_dict(self) -> dict:
        # REQ-1560: the reference carries the SCOPE, so what a person copies off the screen reads
        # the vault they are looking at and never the other one.
        scope = "user" if self.personal else "secret"
        return {
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "updated_by": self.updated_by,
            "scope": "user" if self.personal else "org",
            "reference": f"${{{scope}:{self.name}}}",
        }


def validate_name(name: str) -> str:
    if not NAME.match(name):
        raise ValueError(
            f"Secret name {name!r} must start with a letter or underscore and contain only "
            "letters, digits and underscores."
        )
    return name


def _cipher() -> "EncryptionService":
    """The service that encrypts a stored secret. Never a passthrough.

    The process-wide service (REQ-684) is used when one is configured. When it is NOT -- which is
    the ordinary state of an install that never asked for column encryption -- this store still
    encrypts, because a secrets service whose store depends on unrelated config to not be
    plaintext is not a secrets service. It falls to the local-keychain provider, whose master key
    is minted on first use if the host can keep it.
    """
    from provisa.encryption.runtime import encryption_service
    from provisa.encryption.service import NullEncryption

    service = encryption_service()
    if not isinstance(service, NullEncryption):
        return service

    from provisa.encryption.envelope import EnvelopeEncryption
    from provisa.encryption.providers import (
        LocalKeychain,
        generate_master_key_b64,
        master_key_present,
        store_master_key,
    )

    if not master_key_present() and not store_master_key(generate_master_key_b64()):
        raise RuntimeError(
            "No encryption master key, and this host has no keychain to hold one. Set "
            "PROVISA_ENCRYPTION_KEY to a 32-byte base64 key before storing secrets."
        )
    return EnvelopeEncryption(LocalKeychain.from_config())


async def put(
    admin_db: "Database",
    org_id: str,
    name: str,
    value: str,
    *,
    owner_id: str,
    description: str | None = None,
    actor: str | None = None,
) -> SecretInfo:
    """Store ``value`` under ``name`` in ``owner_id``'s vault within ``org_id``.

    A rotation is the same call as a creation: the name is the identity, and an org replacing an
    expired token is not creating a second secret (REQ-1558). ``owner_id`` is ``ORG_OWNER`` for
    the shared vault or a user id for a personal one, and it is REQUIRED rather than defaulted:
    which vault a secret lands in is the whole of REQ-1560 and must never be decided by omission.
    """
    validate_name(name)
    if value == "":
        raise ValueError("A secret's value cannot be empty.")
    blob = _cipher().encrypt(value.encode())
    async with admin_db.acquire() as conn:
        await conn.upsert(
            secrets_store,
            {
                "org_id": org_id,
                "owner_id": owner_id,
                "name": name,
                "value": blob,
                "description": description,
                "updated_by": actor,
            },
            index_elements=["org_id", "owner_id", "name"],
            update_columns=["value", "description", "updated_by"],
            set_extra={"updated_at": func.now()},
        )
    found = await describe(admin_db, org_id, name, owner_id=owner_id)
    if found is None:
        raise RuntimeError(f"secret {name!r} was written but cannot be read back for {org_id}")
    return found


async def describe(
    admin_db: "Database", org_id: str, name: str, *, owner_id: str
) -> SecretInfo | None:
    """What is known ABOUT one secret in one vault. Never what it holds."""
    for info in await listing(admin_db, org_id, owner_id=owner_id):
        if info.name == name:
            return info
    return None


async def listing(admin_db: "Database", org_id: str, *, owner_id: str) -> list[SecretInfo]:
    """Every secret in ONE vault, by name. The value column is not selected at all.

    REQ-1560: one vault, never a union of them. A person reading their own names is not shown the
    org's, and no caller may ask for another person's -- ``owner_id`` comes from the authenticated
    identity at every call site, never from a request.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                secrets_store.c.name,
                secrets_store.c.description,
                secrets_store.c.created_at,
                secrets_store.c.updated_at,
                secrets_store.c.updated_by,
                secrets_store.c.owner_id,
            )
            .where(secrets_store.c.org_id == org_id, secrets_store.c.owner_id == owner_id)
            .order_by(secrets_store.c.name)
        )
        rows = result.fetchall()
    return [SecretInfo(*row) for row in rows]


async def remove(admin_db: "Database", org_id: str, name: str, *, owner_id: str) -> bool:
    """Delete one secret. True when there was one to delete.

    A reference somewhere still naming it does NOT block this: the reference is text, and what it
    resolves to is decided when it is used (REQ-1558). Provisa does not hold a credential hostage
    to a config that mentions it.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            sql_delete(secrets_store).where(
                secrets_store.c.org_id == org_id,
                secrets_store.c.owner_id == owner_id,
                secrets_store.c.name == name,
            )
        )
    return result.rowcount > 0


async def _decrypted(admin_db: "Database", org_id: str, owner_id: str) -> dict[str, str]:
    """One vault's secrets as a name -> value map. Internal to the binding below."""
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(secrets_store.c.name, secrets_store.c.value).where(
                secrets_store.c.org_id == org_id, secrets_store.c.owner_id == owner_id
            )
        )
        rows = result.fetchall()
    cipher = _cipher()
    return {name: cipher.decrypt(blob).decode() for name, blob in rows}


@asynccontextmanager
async def bound(admin_db: "Database", org_id: str, *, user_id: str | None = None):
    """Make ``org_id``'s secrets -- and ``user_id``'s own -- resolvable for the duration of the block.

    Resolution itself is synchronous -- it happens deep inside config loading and inside the git
    calls that build a remote URL -- so the read and the decrypt happen HERE, where there is a
    connection and an org, rather than at the point of substitution where there is neither. The
    maps live on a ContextVar and die with the block, so nothing outside it can resolve a
    ``${secret:...}`` at all.

    REQ-1560: the personal vault read is the ACTING person's and is decided here, from the caller's
    own identity. An operation that names no person binds no personal vault, so ``${user:...}``
    inside it raises rather than resolving to somebody's credential by accident.
    """
    personal = await _decrypted(admin_db, org_id, user_id) if user_id is not None else {}
    token = _bound.set(
        _Binding(org_id, await _decrypted(admin_db, org_id, ORG_OWNER), user_id, personal)
    )
    try:
        yield
    finally:
        _bound.reset(token)


#: REQ-1580: how a core-layer read learns WHICH org's vault a stored reference names. ``core``
#: cannot import the API layer's ``current_org`` ContextVar or its AppState, so the API layer
#: installs this resolver at import -- the same seam ``domain_policy`` already uses. It returns
#: ``(admin_db, org_id)`` for the org this request is running as.
_request_org: "Callable[[], tuple[Database, str]] | None" = None


def set_request_org_resolver(resolver: "Callable[[], tuple[Database, str]]") -> None:
    """Install the API layer's answer to 'which org is running'. Called once, at import."""
    global _request_org
    _request_org = resolver


@asynccontextmanager
async def bound_to_request_org():
    """Bind the vault of the org running this request, for the duration of the block (REQ-1580).

    The one binding a core-layer read can establish on its own. It carries no acting person, so a
    ``${user:...}`` inside raises naming the scope -- a credential the ORGANIZATION owns is what
    ``${secret:...}`` is for, and no personal vault stands in for it.
    """
    if _request_org is None:
        raise RuntimeError(
            "No request-org resolver is installed, so a ${secret:...} cannot be resolved here. "
            "The API layer installs it at import (provisa.api.app)."
        )
    admin_db, org_id = _request_org()
    async with bound(admin_db, org_id):
        yield


class StoredSecretsProvider(SecretsProvider):
    """Resolve ``${secret:NAME}`` out of the ORG vault bound to this context."""

    def resolve(self, reference: str) -> str:
        held = _bound.get()
        if held is None:
            raise KeyError(
                f"Cannot resolve ${{secret:{reference}}}: no organization is bound to this "
                "context. Secrets belong to an org and are resolved inside its own operations."
            )
        if reference not in held.org_values:
            raise KeyError(f"Organization {held.org_id!r} has no secret named {reference!r}")
        return held.org_values[reference]

    def resolve_user(self, reference: str) -> str:
        """Resolve ``${user:NAME}`` out of the ACTING person's vault (REQ-1560).

        Whose vault is never named in the reference: it is whoever this operation is running as.
        The same text therefore yields each person their own credential, and nothing at all for a
        person who has not stored one -- which is exactly why one member cannot use another's.
        """
        held = _bound.get()
        if held is None:
            raise KeyError(
                f"Cannot resolve ${{user:{reference}}}: no organization is bound to this context."
            )
        if held.user_id is None:
            raise KeyError(
                f"Cannot resolve ${{user:{reference}}}: this operation has no acting user, so "
                "there is no personal vault to read. Use ${secret:...} for a credential the "
                "organization owns."
            )
        if reference not in held.user_values:
            raise KeyError(
                f"{held.user_id!r} has no personal secret named {reference!r} in "
                f"{held.org_id!r}. A personal secret is stored by its owner and by nobody else."
            )
        return held.user_values[reference]
