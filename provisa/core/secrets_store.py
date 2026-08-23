# Copyright (c) 2026 Kenneth Stott
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
"""

# Requirements: REQ-125, REQ-557, REQ-684, REQ-685, REQ-1557, REQ-1558

from __future__ import annotations

import re
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

#: The org whose secrets ``${secret:...}`` resolves against, and the map it resolves from. Bound
#: for the duration of one operation by ``bound``; unbound outside it, so a resolution that
#: happens where no org was established RAISES rather than reaching into some other org's names.
_bound: ContextVar[tuple[str, dict[str, str]] | None] = ContextVar(
    "provisa_secrets_store", default=None
)


@dataclass(frozen=True)
class SecretInfo:
    """Everything about a secret except the one thing it holds."""

    name: str
    description: str | None
    created_at: datetime | None
    updated_at: datetime | None
    updated_by: str | None

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "updated_by": self.updated_by,
            "reference": f"${{secret:{self.name}}}",
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
    description: str | None = None,
    actor: str | None = None,
) -> SecretInfo:
    """Store ``value`` under ``name`` for ``org_id``, replacing whatever was there.

    A rotation is the same call as a creation: the name is the identity, and an org replacing an
    expired token is not creating a second secret (REQ-1558).
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
                "name": name,
                "value": blob,
                "description": description,
                "updated_by": actor,
            },
            index_elements=["org_id", "name"],
            update_columns=["value", "description", "updated_by"],
            set_extra={"updated_at": func.now()},
        )
    found = await describe(admin_db, org_id, name)
    if found is None:
        raise RuntimeError(f"secret {name!r} was written but cannot be read back for {org_id}")
    return found


async def describe(admin_db: "Database", org_id: str, name: str) -> SecretInfo | None:
    """What is known ABOUT one secret. Never what it holds."""
    for info in await listing(admin_db, org_id):
        if info.name == name:
            return info
    return None


async def listing(admin_db: "Database", org_id: str) -> list[SecretInfo]:
    """Every secret the org holds, by name. The value column is not selected at all."""
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                secrets_store.c.name,
                secrets_store.c.description,
                secrets_store.c.created_at,
                secrets_store.c.updated_at,
                secrets_store.c.updated_by,
            )
            .where(secrets_store.c.org_id == org_id)
            .order_by(secrets_store.c.name)
        )
        rows = result.fetchall()
    return [SecretInfo(*row) for row in rows]


async def remove(admin_db: "Database", org_id: str, name: str) -> bool:
    """Delete one secret. True when there was one to delete.

    A reference somewhere still naming it does NOT block this: the reference is text, and what it
    resolves to is decided when it is used (REQ-1558). Provisa does not hold a credential hostage
    to a config that mentions it.
    """
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            sql_delete(secrets_store).where(
                secrets_store.c.org_id == org_id, secrets_store.c.name == name
            )
        )
    return result.rowcount > 0


async def _decrypted(admin_db: "Database", org_id: str) -> dict[str, str]:
    """The org's secrets as a name -> value map. Internal to the binding below."""
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(secrets_store.c.name, secrets_store.c.value).where(
                secrets_store.c.org_id == org_id
            )
        )
        rows = result.fetchall()
    cipher = _cipher()
    return {name: cipher.decrypt(blob).decode() for name, blob in rows}


@asynccontextmanager
async def bound(admin_db: "Database", org_id: str):
    """Make ``org_id``'s secrets resolvable for the duration of the block.

    Resolution itself is synchronous -- it happens deep inside config loading and inside the git
    calls that build a remote URL -- so the read and the decrypt happen HERE, where there is a
    connection and an org, rather than at the point of substitution where there is neither. The
    map lives on a ContextVar and dies with the block, so nothing outside it can resolve a
    ``${secret:...}`` at all.
    """
    token = _bound.set((org_id, await _decrypted(admin_db, org_id)))
    try:
        yield
    finally:
        _bound.reset(token)


class StoredSecretsProvider(SecretsProvider):
    """Resolve ``${secret:NAME}`` out of the org bound to this context."""

    def resolve(self, reference: str) -> str:
        held = _bound.get()
        if held is None:
            raise KeyError(
                f"Cannot resolve ${{secret:{reference}}}: no organization is bound to this "
                "context. Secrets belong to an org and are resolved inside its own operations."
            )
        org_id, values = held
        if reference not in values:
            raise KeyError(f"Organization {org_id!r} has no secret named {reference!r}")
        return values[reference]
