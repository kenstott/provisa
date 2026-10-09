# Copyright (c) 2026 Kenneth Stott
# Canary: ce34215e-bd2d-4773-a1b0-d7a8cbdee25b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The mail platforms an organisation's sources sign in to (REQ-1923).

A source that reads a person's mail is signed in by that person's approval of a client the
ORGANISATION registered with the platform (Google Workspace, Microsoft 365). The client is the
organisation's: its administrator enters it once, and every source of the organisation uses it.
Nobody adding a source sees or types it.

What is kept, per organisation and platform: the client's id and what else the platform's
addresses depend on (Microsoft's tenant) in ``org_mail_platforms``; the client's secret in the
organisation's vault under :func:`secret_name`. An organisation reads and writes only its own:
every call here names the organisation, and nothing looks one up across them.

A platform is one a source kind has declared (:func:`declare`), with the settings it takes
beside the client. A setting it does not take is refused by name.
"""

# Requirements: REQ-1923
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from provisa.core import secrets_store
from provisa.core.schema_admin import org_mail_platforms

if TYPE_CHECKING:
    from provisa.core.database import Database


class MailPlatformRefused(ValueError):
    """A mail platform's settings that cannot be kept, or that are not there to use."""

    def __init__(self, code: str, message: str, *, status: int = 400, **params: str) -> None:
        super().__init__(message)
        self.code = f"mail_platform.{code}"
        self.status = status
        self.params = params


@dataclass(frozen=True)
class Platform:
    """A mail platform sources sign in to: its id (the source type that reads it) and the
    settings its client is entered with beside the id and secret."""

    id: str
    settings: tuple[str, ...] = ()


@dataclass(frozen=True)
class Configured:
    """An organisation's client for one platform. The secret is named, never held."""

    platform: str
    client_id: str
    settings: dict[str, str] = field(default_factory=dict)

    @property
    def client_secret(self) -> str:
        """The reference that names the client's secret in the organisation's vault."""
        return f"${{secret:{secret_name(self.platform)}}}"


_PLATFORMS: dict[str, Platform] = {}


def declare(platform: Platform) -> None:
    _PLATFORMS[platform.id] = platform


def platforms() -> list[Platform]:
    """Every platform a source kind has declared, in the order declared."""
    return list(_PLATFORMS.values())


def platform(platform_id: str) -> Platform:
    if platform_id not in _PLATFORMS:
        raise MailPlatformRefused(
            "unknown", f"No mail platform {platform_id!r} exists", status=404, platform=platform_id
        )
    return _PLATFORMS[platform_id]


def secret_name(platform_id: str) -> str:
    """The name of the platform's client secret in an organisation's vault."""
    return secrets_store.validate_name(f"mail_platform_{platform_id}_client_secret")


async def read(admin_db: "Database", org_id: str, platform_id: str) -> Configured | None:
    """``org_id``'s client for the platform, or None when it has entered none."""
    platform(platform_id)
    async with admin_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(org_mail_platforms.c.client_id, org_mail_platforms.c.settings).where(
                    org_mail_platforms.c.org_id == org_id,
                    org_mail_platforms.c.platform == platform_id,
                )
            )
        ).fetchone()
    if row is None:
        return None
    return Configured(platform_id, row[0], json.loads(row[1]))


async def require(admin_db: "Database", org_id: str, platform_id: str) -> Configured:
    """``org_id``'s client for the platform, refused by name when it has entered none."""
    configured = await read(admin_db, org_id, platform_id)
    if configured is None:
        raise MailPlatformRefused(
            "not_configured",
            f"This organisation has not connected {platform_id} yet; an administrator enters "
            "its client once, under Admin > Email.",
            platform=platform_id,
        )
    return configured


async def put(
    admin_db: "Database",
    org_id: str,
    platform_id: str,
    *,
    client_id: str,
    client_secret: str | None,
    settings: dict[str, str],
    actor: str | None,
) -> Configured:
    """Keep ``org_id``'s client for the platform. ``client_secret`` None keeps the secret already
    in the vault, which there must then be: a client is never kept without its secret."""
    declared = platform(platform_id)
    client_id = client_id.strip()
    settings = {key: value.strip() for key, value in settings.items()}
    unknown = sorted(set(settings) - set(declared.settings))
    if unknown:
        raise MailPlatformRefused(
            "unknown_setting",
            f"{', '.join(unknown)} is not a setting of {platform_id}",
            setting=", ".join(unknown),
        )
    missing = [key for key in declared.settings if not settings.get(key)]
    if not client_id:
        missing.insert(0, "client_id")
    name = secret_name(platform_id)
    owner = secrets_store.ORG_OWNER
    if (
        not client_secret
        and await secrets_store.describe(admin_db, org_id, name, owner_id=owner) is None
    ):
        missing.append("client_secret")
    if missing:
        raise MailPlatformRefused(
            "incomplete", f"{platform_id} needs {', '.join(missing)}", missing=", ".join(missing)
        )
    if client_secret:
        await secrets_store.put(
            admin_db,
            org_id,
            name,
            client_secret,
            owner_id=owner,
            description=f"client secret of the organisation's {platform_id} client",
            actor=actor,
        )
    async with admin_db.acquire() as conn:
        await conn.upsert(
            org_mail_platforms,
            {
                "org_id": org_id,
                "platform": platform_id,
                "client_id": client_id,
                "client_secret": Configured(platform_id, client_id).client_secret,
                "settings": json.dumps(settings, sort_keys=True),
                "updated_by": actor,
            },
            index_elements=["org_id", "platform"],
            update_columns=["client_id", "client_secret", "settings", "updated_by"],
            set_extra={"updated_at": func.now()},
        )
    return Configured(platform_id, client_id, settings)


async def forget(admin_db: "Database", org_id: str, platform_id: str) -> bool:
    """Remove ``org_id``'s client for the platform: True when there was one. Its secret is the
    caller's to remove from the vault afterwards, by the vault's own rules: while the client
    stands it names the secret, and the vault lets no named secret go."""
    platform(platform_id)
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            delete(org_mail_platforms).where(
                org_mail_platforms.c.org_id == org_id,
                org_mail_platforms.c.platform == platform_id,
            )
        )
    return result.rowcount > 0
