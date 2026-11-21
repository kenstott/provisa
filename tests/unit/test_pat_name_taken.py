# Copyright (c) 2026 Kenneth Stott
# Canary: 4f7d1a93-2e68-4c05-b9a1-6d3e8f2c0b75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A personal access token is told apart by its name (REQ-1263): a name one of the owner's live
tokens in the org already has is refused, and nothing is issued. A revoked token's name is free
again, and another user, or the same user in another org, may use it."""

# Requirements: REQ-1263

from __future__ import annotations

import pytest
from sqlalchemy import func, insert, select

from provisa.auth.pat import PersonalAccessTokenStore, TokenNameTaken
from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import orgs, personal_access_tokens

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def store() -> tuple[PersonalAccessTokenStore, Database]:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="pat-names")
    with db.engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
    async with db.acquire() as conn:
        for org_id in ("acme", "globex"):
            await conn.execute_core(insert(orgs).values(id=org_id, name=org_id))
    return PersonalAccessTokenStore(db), db


async def _count(db: Database) -> int:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(select(func.count()).select_from(personal_access_tokens))
        ).scalar_one()


async def test_a_name_a_live_token_of_the_owner_has_is_refused(store):
    pats, db = store
    await pats.issue(user_id="ada", org_id="acme", env_name="prod", name="ci", scopes=["analyst"])
    with pytest.raises(TokenNameTaken, match="'ci' already exists"):
        await pats.issue(
            user_id="ada", org_id="acme", env_name="prod", name=" ci ", scopes=["analyst"]
        )
    assert await _count(db) == 1
    # Another owner, and the same owner in another org, may use it.
    await pats.issue(user_id="ben", org_id="acme", env_name="prod", name="ci", scopes=["analyst"])
    await pats.issue(user_id="ada", org_id="globex", env_name="prod", name="ci", scopes=["analyst"])
    # Revoked, its name is free again.
    async with db.acquire() as conn:
        first = (
            await conn.execute_core(
                select(personal_access_tokens.c.token_hash).where(
                    personal_access_tokens.c.user_id == "ada",
                    personal_access_tokens.c.org_id == "acme",
                )
            )
        ).scalar_one()
    assert await pats.revoke(token_hash=first, user_id="ada")
    await pats.issue(user_id="ada", org_id="acme", env_name="prod", name="ci", scopes=["analyst"])
    assert await _count(db) == 4
