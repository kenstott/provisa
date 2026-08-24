# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's own encryption key: set it, rotate it, never read it (REQ-1574).

An org may hold the key its data is wrapped under. It can be generated here or supplied by the
org's administrator, and it can be rotated at any time -- but there is no path by which its value
comes back out. Nothing in this module returns key material: :func:`org_key_status` returns a
FINGERPRINT (the first 16 hex of SHA-256 over the raw key), which answers the only questions an
operator has to be able to answer about a key they cannot see -- is this the key I meant to set,
and is this org still on the key I gave it -- and discloses nothing else. The same trade ssh makes.

The key is a RING (``org_encryption_keys``), not a slot. Setting a key retires the previous one
rather than overwriting it, so a payload wrapped under a retired key still decrypts: rotation
changes which key NEW writes use and is not a re-encryption of what is already stored. Envelope
blobs stamp the key id they were wrapped under (envelope format v2), which is what lets the ring
pick the right entry per blob instead of guessing.

Key material is never stored in the clear: each row holds the org key wrapped by the DEPLOYMENT's
encryption service, the same argument ``secrets_store`` makes for its values -- a copy of the
control plane without the deployment master key is not the org's key.
"""

# Requirements: REQ-1574

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.db import Database
    from provisa.encryption.envelope import RingEnvelopeEncryption

KEY_BYTES = 32  # AES-256, the same size the deployment master key is
_FINGERPRINT_HEX = 16  # 8 bytes of SHA-256, rendered hex


class OrgKeyError(ValueError):
    """The supplied key is not a key: wrong encoding, or not 32 bytes."""


@dataclass(frozen=True)
class OrgKeyStatus:
    """What may be told about an org's key. Deliberately no field carries key material."""

    key_id: str
    fingerprint: str
    supplied: bool
    created_at: datetime | None
    created_by: str | None
    retired_count: int

    def as_dict(self) -> dict:
        return {
            "key_id": self.key_id,
            "fingerprint": self.fingerprint,
            "supplied": self.supplied,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "created_by": self.created_by,
            "retired_count": self.retired_count,
        }


def fingerprint(raw_key: bytes) -> str:
    """The public name of a key: first 16 hex of SHA-256. Never invertible to the key."""
    return hashlib.sha256(raw_key).hexdigest()[:_FINGERPRINT_HEX]


def decode_key(key_b64: str) -> bytes:
    """Decode a supplied base64 key, or raise :class:`OrgKeyError`.

    Refuses anything that is not exactly ``KEY_BYTES``: a short key is a weak key, and a long one
    means the caller pasted something that is not a key at all.
    """
    try:
        raw = base64.b64decode(key_b64, validate=True)
    except Exception as exc:  # noqa: BLE001 - any decode failure is the same answer to the caller
        raise OrgKeyError("key must be base64") from exc
    if len(raw) != KEY_BYTES:
        raise OrgKeyError(f"key must decode to {KEY_BYTES} bytes (AES-256), got {len(raw)}")
    return raw


def generate_key() -> bytes:
    return os.urandom(KEY_BYTES)


def _next_key_id(existing: list[str]) -> str:
    """``k1``, ``k2``, … -- short because every blob this key wraps carries the id."""
    used = {int(k[1:]) for k in existing if k.startswith("k") and k[1:].isdigit()}
    return f"k{max(used, default=0) + 1}"


async def set_org_key(
    admin_db: "Database",
    org_id: str,
    *,
    key_b64: str | None,
    actor: str | None,
    adopt_unkeyed: bool | None = None,
) -> OrgKeyStatus:
    """Set (or rotate) the org's key and return its status. NEVER returns key material.

    ``key_b64`` supplied means the org brought its own key; ``None`` means generate one here, in
    which case the key exists nowhere but this ring -- the org cannot be given a copy, because
    giving one out is exactly what this requirement forbids.

    ``adopt_unkeyed`` decides whether this entry opens blobs written before the org held a ring
    (envelope v1, which names no key). It defaults to True for an org's FIRST key -- whose data was
    written under the deployment key and must stay readable -- and False for a rotation, since a
    rotation's predecessor already adopts them.
    """
    from sqlalchemy import select, update

    from provisa.core.schema_admin import org_encryption_keys as t
    from provisa.encryption.runtime import deployment_encryption_service

    raw = decode_key(key_b64) if key_b64 is not None else generate_key()
    now = datetime.now(timezone.utc)
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.key_id, t.c.retired_at).where(t.c.org_id == org_id)
        )
        rows = result.fetchall()
        first = not rows
        adopts = first if adopt_unkeyed is None else adopt_unkeyed
        key_id = _next_key_id([r[0] for r in rows])
        await conn.execute_core(
            update(t).where(t.c.org_id == org_id, t.c.retired_at.is_(None)).values(retired_at=now)
        )
        await conn.execute_core(
            t.insert().values(
                org_id=org_id,
                key_id=key_id,
                # Wrapped by the DEPLOYMENT service on purpose: this row is the org's key, and the
                # org's own service is the thing being defined here -- it cannot wrap itself.
                wrapped_key=deployment_encryption_service().encrypt(raw),
                fingerprint=fingerprint(raw),
                supplied=key_b64 is not None,
                adopts_unkeyed=adopts,
                created_at=now,
                created_by=actor,
            )
        )
    return OrgKeyStatus(
        key_id=key_id,
        fingerprint=fingerprint(raw),
        supplied=key_b64 is not None,
        created_at=now,
        created_by=actor,
        retired_count=len(rows),
    )


async def org_key_status(admin_db: "Database", org_id: str) -> OrgKeyStatus | None:
    """The active key's metadata, or ``None`` when the org has set no key of its own."""
    from sqlalchemy import select

    from provisa.core.schema_admin import org_encryption_keys as t

    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                t.c.key_id,
                t.c.fingerprint,
                t.c.supplied,
                t.c.created_at,
                t.c.created_by,
                t.c.retired_at,
            ).where(t.c.org_id == org_id)
        )
        rows = result.fetchall()
    active = [r for r in rows if r[5] is None]
    if not active:
        return None
    row = active[0]
    return OrgKeyStatus(
        key_id=row[0],
        fingerprint=row[1],
        supplied=bool(row[2]),
        created_at=row[3],
        created_by=row[4],
        retired_count=len(rows) - 1,
    )


async def load_org_ring(admin_db: "Database", org_id: str) -> "RingEnvelopeEncryption | None":
    """Build the org's encryption service from its ring, or ``None`` if it holds no key.

    Every entry is unwrapped -- retired ones included -- because a retired key is exactly what
    reads the data written while it was active.
    """
    from sqlalchemy import select

    from provisa.core.schema_admin import org_encryption_keys as t
    from provisa.encryption.envelope import RingEnvelopeEncryption
    from provisa.encryption.providers import LocalKeychain, MasterKeyProvider
    from provisa.encryption.runtime import deployment_encryption_service

    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(t.c.key_id, t.c.wrapped_key, t.c.adopts_unkeyed, t.c.retired_at).where(
                t.c.org_id == org_id
            )
        )
        rows = result.fetchall()
    if not rows:
        return None
    service = deployment_encryption_service()
    providers: dict[str, MasterKeyProvider] = {
        str(r[0]): LocalKeychain(service.decrypt(bytes(r[1]))) for r in rows
    }
    active = [r[0] for r in rows if r[3] is None]
    if len(active) != 1:
        # The ring's one invariant. Serving an org on a guessed key would write its next payload
        # under a key it did not choose, so this is an error and not something to pick a side of.
        raise OrgKeyError(
            f"org {org_id!r} has {len(active)} active encryption keys; exactly one is required"
        )
    adopting = [r[0] for r in rows if r[2]]
    return RingEnvelopeEncryption(active[0], providers, v1_key_id=adopting[0] if adopting else None)


async def ring_owner_org_ids(admin_db: "Database") -> list[str]:
    """Every org that holds a ring at all (REQ-1574).

    Startup reads this once and hands it to ``note_org_rings``: it is what makes selection fail
    closed. Knowing an org has a key is not knowing the key, so this is cheap and safe to hold.
    """
    from sqlalchemy import select

    from provisa.core.schema_admin import org_encryption_keys as t

    async with admin_db.acquire() as conn:
        result = await conn.execute_core(select(t.c.org_id).distinct())
        return [str(r[0]) for r in result.fetchall()]
