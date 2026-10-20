# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3e9c15-4b2d-4f80-9e61-c8d05b3f2a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The container registries an organisation's built code-source images are pushed to (REQ-2098).

An organisation setting beside its package registries (provisa/core/package_registries.py),
entered by its administrator: each registry an address -- a registry host, with its port where it
has one, and a repository namespace (``ghcr.io/acme``, ``registry.acme.com:5000/data``) -- a user
name and a credential (a password or token), and optionally the PEM certificate authority its TLS
chains to. One may be the default a build pushes to.

Every field holds what was entered -- the value, or a ``${secret:NAME}`` / ``${env:NAME}``
reference -- and is resolved at use (:func:`resolved`), in the organisation's own vault. Nothing
here writes to a vault or makes up a secret's name. An organisation reads and writes only its own.
"""

# Requirements: REQ-2098
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from provisa.core import secrets_store
from provisa.core.schema_admin import org_container_registries as _t

if TYPE_CHECKING:
    from provisa.core.database import Database

_PEM_CERTIFICATE = "-----BEGIN CERTIFICATE-----"
#: A registry host with an optional port, then a repository namespace of one or more segments.
_ADDRESS = re.compile(r"^[a-z0-9.-]+(:\d+)?(/[a-z0-9._-]+)+$")


class ContainerRegistryRefused(ValueError):
    """A registry entry that cannot be kept, or that cannot be used as entered."""

    def __init__(self, code: str, message: str, *, status: int = 400, **params: str) -> None:
        super().__init__(message)
        self.code = f"container_registry.{code}"
        self.status = status
        self.params = params


@dataclass(frozen=True)
class Registry:
    """One entry, each field as its administrator entered it."""

    id: str
    address: str
    username: str
    credential: str
    default: bool
    ca_bundle: str | None = None


def _row(row) -> Registry:
    return Registry(
        row.id, row.address, row.username, row.credential, bool(row.is_default), row.ca_bundle
    )


def _is_reference(value: str) -> bool:
    return value.startswith("${") and value.endswith("}")


async def entries(admin_db: "Database", org_id: str) -> list[Registry]:
    """Every entry of ``org_id``, by address."""
    async with admin_db.acquire() as conn:
        rows = (
            await conn.execute_core(select(_t).where(_t.c.org_id == org_id).order_by(_t.c.address))
        ).fetchall()
    return [_row(r) for r in rows]


async def read(admin_db: "Database", org_id: str, registry_id: str) -> Registry | None:
    async with admin_db.acquire() as conn:
        row = (
            await conn.execute_core(select(_t).where(_t.c.org_id == org_id, _t.c.id == registry_id))
        ).fetchone()
    return None if row is None else _row(row)


async def put(
    admin_db: "Database",
    org_id: str,
    *,
    registry_id: str | None,
    address: str,
    username: str,
    credential: str | None,
    default: bool,
    actor: str | None,
    ca_bundle: str | None = None,
) -> Registry:
    """Keep an entry of ``org_id``: a new one when ``registry_id`` is None, else the one it names,
    replaced. Each field is kept exactly as entered; ``credential`` None keeps the one the entry
    holds, which there must then be.

    Refused by name: an address that is not a registry host and namespace (a reference is kept as
    entered); an empty user name or credential; a certificate authority entered as a value that
    holds no PEM certificate; a second default; an address another entry has."""
    address = address.strip().removeprefix("https://").removeprefix("http://").rstrip("/")
    username = username.strip()
    credential = (credential or "").strip() or None
    ca_bundle = (ca_bundle or "").strip() or None
    if not _is_reference(address) and not _ADDRESS.match(address):
        raise ContainerRegistryRefused(
            "address",
            f"{address!r} is not a registry and repository namespace, such as ghcr.io/acme",
            address=address,
        )
    if ca_bundle is not None and not _is_reference(ca_bundle) and _PEM_CERTIFICATE not in ca_bundle:
        raise ContainerRegistryRefused(
            "ca_bundle_not_pem",
            f"The certificate authority of {address} holds no PEM certificate",
            address=address,
        )
    if registry_id is not None:
        held = await read(admin_db, org_id, registry_id)
        if held is None:
            raise ContainerRegistryRefused(
                "not_found",
                f"There is no container registry {registry_id!r}",
                status=404,
                registry=registry_id,
            )
        credential = credential or held.credential
    missing = [n for n, v in (("username", username), ("credential", credential)) if not v]
    if missing:
        raise ContainerRegistryRefused(
            "incomplete",
            f"A container registry needs {', '.join(missing)}",
            missing=", ".join(missing),
        )
    others = [e for e in await entries(admin_db, org_id) if e.id != registry_id]
    if default and any(e.default for e in others):
        (current,) = [e for e in others if e.default]
        raise ContainerRegistryRefused(
            "second_default",
            f"{current.address} is already the default container registry; an organisation has one",
            default=current.address,
        )
    if any(e.address == address for e in others):
        raise ContainerRegistryRefused(
            "address_taken", f"The container registry {address} is already entered", address=address
        )
    values = {
        "org_id": org_id,
        "id": registry_id or uuid.uuid4().hex,
        "address": address,
        "username": username,
        "credential": credential,
        "is_default": default,
        "ca_bundle": ca_bundle,
        "updated_by": actor,
    }
    async with admin_db.acquire() as conn:
        await conn.upsert(
            _t,
            values,
            index_elements=["org_id", "id"],
            update_columns=[
                "address",
                "username",
                "credential",
                "is_default",
                "ca_bundle",
                "updated_by",
            ],
            set_extra={"updated_at": func.now()},
        )
    kept = await read(admin_db, org_id, values["id"])
    if kept is None:
        raise RuntimeError(
            f"container registry {values['id']} was kept for {org_id} but cannot be read back"
        )
    return kept


async def forget(admin_db: "Database", org_id: str, registry_id: str) -> bool:
    """Remove an entry of ``org_id``: True when there was one. A secret it named is left as is."""
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            delete(_t).where(_t.c.org_id == org_id, _t.c.id == registry_id)
        )
    return result.rowcount > 0


@dataclass(frozen=True)
class Resolved:
    """An entry ready for a push: every field resolved."""

    id: str
    address: str
    username: str
    credential: str
    default: bool
    ca_bundle: str | None = None

    @property
    def host(self) -> str:
        """The registry's host (and port): what its API is addressed at."""
        return self.address.split("/", 1)[0]

    @property
    def namespace(self) -> str:
        """The repository namespace an image is pushed under."""
        return self.address.split("/", 1)[1]


async def resolved(admin_db: "Database", org_id: str, registry_id: str | None) -> Resolved:
    """The entry ``registry_id`` of ``org_id`` -- the default when it is None -- with each field
    resolved in THAT organisation's vault, bound here for this read alone. Refused by name when
    there is no such entry (or no default) or a field cannot be resolved."""
    from provisa.core.secrets import resolve_secrets

    kept = await entries(admin_db, org_id)
    chosen = [e for e in kept if (e.id == registry_id if registry_id else e.default)]
    if not chosen:
        raise ContainerRegistryRefused(
            "none",
            f"There is no container registry {registry_id!r}"
            if registry_id
            else "The organisation has no default container registry: name one, or download the image",
            status=404,
        )
    (entry,) = chosen
    fields: dict[str, str | None] = {}
    async with secrets_store.bound(admin_db, org_id):
        for name in ("address", "username", "credential", "ca_bundle"):
            value = getattr(entry, name)
            if value is None:
                fields[name] = None
                continue
            try:
                fields[name] = resolve_secrets(value)
            except (KeyError, ValueError) as unresolved:
                reason = unresolved.args[0] if unresolved.args else type(unresolved).__name__
                raise ContainerRegistryRefused(
                    "unresolved",
                    f"The {name} of container registry {entry.address} could not be resolved: "
                    f"{reason}",
                    registry=entry.id,
                    field=name,
                ) from None
    address, username, credential = fields["address"], fields["username"], fields["credential"]
    assert address is not None and username is not None and credential is not None  # NOT NULL
    if not _ADDRESS.match(address):
        raise ContainerRegistryRefused(
            "address",
            f"The container registry {entry.address} resolves to {address!r}, which is not a "
            "registry and repository namespace",
            registry=entry.id,
        )
    return Resolved(entry.id, address, username, credential, entry.default, fields["ca_bundle"])
