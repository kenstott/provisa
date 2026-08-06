# Copyright (c) 2026 Kenneth Stott
# Canary: 8e788d30-2239-4eb5-8c8b-89c6973b7a99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where SCRAM-SHA-256 verifiers live (REQ-1394).

Kept apart from :mod:`provisa.auth.scram`, which is the mechanism and knows nothing about storage.
A verifier is written at the one moment a plaintext password exists — user creation and password
change — and read by pgwire when a client negotiates SASL.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select

from provisa.auth.scram import ScramVerifier, make_verifier, parse_verifier
from provisa.core.schema_admin import scram_credentials

# Requirements: REQ-1394


async def write_verifier(pool, user_id: str, username: str, password: str) -> None:
    """Derive and store this user's verifier, replacing any earlier one.

    Called wherever a password is set. The plaintext never leaves this call — what is stored
    cannot be used to authenticate, only to check someone else's proof.
    """
    async with pool.acquire() as conn:
        await conn.upsert(
            scram_credentials,
            {
                "user_id": user_id,
                "username": username,
                "verifier": make_verifier(password).serialize(),
                "updated_at": func.now(),
            },
            index_elements=["user_id"],
            update_columns=["username", "verifier", "updated_at"],
        )


async def read_verifier(pool, username: str) -> ScramVerifier | None:
    """This user's verifier, or None when they have never set a password under SCRAM.

    None is a real answer, not a failure: SCRAM is opt-in and a bcrypt hash cannot be converted, so
    a deployment that has just turned it on has users with no verifier yet. The caller answers with
    a mock exchange rather than a different message, so the absence is not visible on the wire.
    """
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(scram_credentials.c.verifier).where(scram_credentials.c.username == username)
        )
        row = result.fetchone()
    return parse_verifier(row[0]) if row is not None else None


async def delete_verifier(pool, user_id: str) -> None:
    """Drop a user's verifier — used when the user itself is deleted."""
    async with pool.acquire() as conn:
        await conn.execute_core(
            delete(scram_credentials).where(scram_credentials.c.user_id == user_id)
        )
