# Copyright (c) 2026 Kenneth Stott
# Canary: 8e788d30-2239-4eb5-8c8b-89c6973b7a99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Personal access tokens — the one long-lived credential every protocol accepts (REQ-1263).

A PAT is a bearer secret carrying its own org and role. Every surface resolves it through
``AuthProvider.validate_credential``, so pgwire, Bolt, Flight, gRPC, MCP and HTTP all reach the
same store without any of them knowing what a PAT is.

The wire form is ``provisa_pat_<43 url-safe base64 chars>``. The prefix is what lets a surface
route a presented secret to this store instead of the configured identity provider, and it is
what makes a leaked token greppable in logs and repositories.

Only the SHA-256 of the secret is stored. SHA-256 rather than bcrypt on purpose: a PAT is 256
bits of machine-generated entropy, so it is not guessable and needs no work factor, and lookup
is by hash — a per-row bcrypt cost would mean scanning every token on every request.
"""

from __future__ import annotations

import datetime
import hashlib
import secrets

from sqlalchemy import delete, select, update

from provisa.auth.models import AuthIdentity
from provisa.core.schema_admin import personal_access_tokens, user_profiles

# Requirements: REQ-1263

TOKEN_PREFIX = "provisa_pat_"

# Characters of the token kept in cleartext so the owner can identify a row in the UI. Short
# enough that it is not a usable fraction of the secret.
_DISPLAY_PREFIX_LEN = 8

# 32 bytes of entropy, url-safe base64 encoded.
_TOKEN_BYTES = 32


def is_personal_access_token(token: str) -> bool:
    """True when the presented secret is shaped like a PAT and belongs to this store."""
    return token.startswith(TOKEN_PREFIX)


def hash_token(token: str) -> str:
    """The stored form of a token. Never reversible; the wire secret is shown once."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def mint_token() -> tuple[str, str, str]:
    """Generate a new token. Returns ``(secret, token_hash, display_prefix)``."""
    secret = TOKEN_PREFIX + secrets.token_urlsafe(_TOKEN_BYTES)
    return secret, hash_token(secret), secret[: len(TOKEN_PREFIX) + _DISPLAY_PREFIX_LEN]


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _as_aware(value: datetime.datetime) -> datetime.datetime:
    """Treat a naive timestamp as UTC.

    SQLite has no timezone-aware storage, so a control plane on SQLite reads back naive
    datetimes for the same column PostgreSQL returns aware. Comparing the two raises, so the
    tz is restored here rather than every call site guessing.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.timezone.utc)
    return value


class PersonalAccessTokenStore:  # REQ-1263
    """Issues, validates and revokes PATs against the platform control plane."""

    def __init__(self, admin_pool) -> None:
        self._pool = admin_pool

    async def issue(
        self,
        *,
        user_id: str,
        org_id: str,
        name: str,
        role_id: str | None = None,
        scopes: list[str] | None = None,
        expires_at: datetime.datetime | None = None,
    ) -> tuple[str, dict]:
        """Mint a token. Returns ``(secret, row)``; the secret is never recoverable afterwards."""
        if not name.strip():
            raise ValueError("A token name is required")
        if expires_at is not None and _as_aware(expires_at) <= _utcnow():
            raise ValueError("Token expiry must be in the future")

        secret, token_hash, prefix = mint_token()
        row = {
            "token_hash": token_hash,
            "prefix": prefix,
            "name": name.strip(),
            "user_id": user_id,
            "org_id": org_id,
            "role_id": role_id,
            "scopes": scopes or [],
            "expires_at": expires_at,
        }
        async with self._pool.acquire() as conn:
            await conn.execute_core(personal_access_tokens.insert().values(**row))
        return secret, {k: v for k, v in row.items() if k != "token_hash"}

    async def validate(self, token: str) -> AuthIdentity:  # REQ-1263
        """Resolve a presented PAT to an identity, or raise ValueError.

        Revoked and expired tokens raise the same error as an unknown one: a caller must not
        learn from the response whether a token ever existed.

        The identity carries the owner's account — user id, email and display name off
        ``user_profiles`` — not the token's own label, so an audit row or usage report written
        under a PAT names the person who acts, the same as any interactive session. The token's
        label rides in ``raw_claims["token_name"]``, which is what distinguishes *which*
        credential of theirs was used.
        """
        token_hash = hash_token(token)
        async with self._pool.acquire() as conn:
            result = await conn.execute_core(
                select(
                    personal_access_tokens.c.user_id,
                    personal_access_tokens.c.org_id,
                    personal_access_tokens.c.role_id,
                    personal_access_tokens.c.scopes,
                    personal_access_tokens.c.expires_at,
                    personal_access_tokens.c.revoked_at,
                    personal_access_tokens.c.name,
                    user_profiles.c.email,
                    user_profiles.c.display_name,
                )
                .select_from(
                    personal_access_tokens.outerjoin(
                        user_profiles,
                        user_profiles.c.user_id == personal_access_tokens.c.user_id,
                    )
                )
                .where(personal_access_tokens.c.token_hash == token_hash)
            )
            fetched = result.fetchone()
            if fetched is None:
                raise ValueError("Invalid token")
            row = dict(fetched._mapping)
            if row["revoked_at"] is not None:
                raise ValueError("Invalid token")
            if row["expires_at"] is not None and _as_aware(row["expires_at"]) <= _utcnow():
                raise ValueError("Invalid token")
            await conn.execute_core(
                update(personal_access_tokens)
                .where(personal_access_tokens.c.token_hash == token_hash)
                .values(last_used_at=_utcnow())
            )

        return AuthIdentity(
            user_id=row["user_id"],
            email=row["email"],
            display_name=row["display_name"],
            # A token with no role_id grants whatever its owner resolves to; one with a role_id
            # is narrowed to it. The role claim carries no domain, so it applies org-wide.
            roles=[row["role_id"]] if row["role_id"] else [],
            raw_claims={
                "pat": True,
                "scopes": list(row["scopes"] or []),
                "token_name": row["name"],
            },
            active_org_id=row["org_id"],
        )

    async def list_for_user(self, user_id: str, org_id: str) -> list[dict]:
        """Every token the user holds in this org, newest first. Never returns a secret."""
        async with self._pool.acquire() as conn:
            result = await conn.execute_core(
                select(
                    personal_access_tokens.c.token_hash,
                    personal_access_tokens.c.prefix,
                    personal_access_tokens.c.name,
                    personal_access_tokens.c.role_id,
                    personal_access_tokens.c.scopes,
                    personal_access_tokens.c.created_at,
                    personal_access_tokens.c.expires_at,
                    personal_access_tokens.c.last_used_at,
                    personal_access_tokens.c.revoked_at,
                ).where(
                    personal_access_tokens.c.user_id == user_id,
                    personal_access_tokens.c.org_id == org_id,
                )
            )
            rows = [dict(r._mapping) for r in result.fetchall()]
        rows.sort(key=lambda r: _as_aware(r["created_at"]), reverse=True)
        return rows

    async def revoke(self, *, token_hash: str, user_id: str) -> bool:
        """Revoke one token. Scoped to its owner so a hash alone cannot revoke another's token.

        Returns False when the token does not exist, is not the caller's, or is already revoked.
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute_core(
                update(personal_access_tokens)
                .where(
                    personal_access_tokens.c.token_hash == token_hash,
                    personal_access_tokens.c.user_id == user_id,
                    personal_access_tokens.c.revoked_at.is_(None),
                )
                .values(revoked_at=_utcnow())
            )
        return result.rowcount > 0

    async def revoke_all_for_user_in_org(self, *, user_id: str, org_id: str) -> int:
        """Revoke every token a user holds in one org. Called when their membership ends —
        a departed member's long-lived credential must stop working with the membership, not
        at its own expiry."""
        async with self._pool.acquire() as conn:
            result = await conn.execute_core(
                update(personal_access_tokens)
                .where(
                    personal_access_tokens.c.user_id == user_id,
                    personal_access_tokens.c.org_id == org_id,
                    personal_access_tokens.c.revoked_at.is_(None),
                )
                .values(revoked_at=_utcnow())
            )
        return result.rowcount

    async def purge_revoked_before(self, cutoff: datetime.datetime) -> int:
        """Delete revoked tombstones older than *cutoff*, so the audit window is bounded."""
        async with self._pool.acquire() as conn:
            result = await conn.execute_core(
                delete(personal_access_tokens).where(
                    personal_access_tokens.c.revoked_at.is_not(None),
                    personal_access_tokens.c.revoked_at < cutoff,
                )
            )
        return result.rowcount
