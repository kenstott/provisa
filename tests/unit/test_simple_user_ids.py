# Copyright (c) 2026 Kenneth Stott
# Canary: 780c750a-4cd6-4577-9766-66117b180778
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A simple-provider user's id is a GUID assigned at first sign-in and stored, never the username.

Records keep the user id after a soft delete, so the id must be opaque. It is read from the
platform plane's table on every sign-in, which is what makes it the same on every node; two
nodes signing the same new user in at once agree on one id through the table's unique key.
"""

from __future__ import annotations

import asyncio
import base64
import uuid

import bcrypt
import jwt
import pytest

from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url

_SECRET = "simple-provider-signing-secret-of-48-bytes-or-more"
_PASSWORD = "the-right-password"


@pytest.fixture()
def admin_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    schema_admin.metadata.create_all(engine, tables=[schema_admin.simple_user_ids])
    return Database(engine, "test")


def _provider(admin_db):
    from provisa.auth.providers.simple import SimpleAuthProvider
    from provisa.auth.simple_user_ids import SimpleUserIds

    users = [
        {
            "username": "ana",
            "password_hash": bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode(),
            "roles": ["analyst"],
        }
    ]
    return SimpleAuthProvider(users=users, jwt_secret=_SECRET, user_ids=SimpleUserIds(admin_db))


def _basic(password: str = _PASSWORD) -> str:
    return base64.b64encode(f"ana:{password}".encode()).decode()


async def test_the_id_is_an_opaque_guid_not_the_username(admin_db):
    identity = await _provider(admin_db).validate_basic(_basic())
    assert identity.user_id != "ana"
    assert uuid.UUID(identity.user_id)  # a GUID, not a name or a hash of one
    assert identity.display_name == "ana"


async def test_every_sign_in_and_every_node_gets_the_same_id(admin_db):
    first = await _provider(admin_db).validate_basic(_basic())
    other_node = _provider(admin_db)  # a second process reading the same platform plane
    again = await other_node.validate_basic(_basic())
    token = await other_node.password_login("ana", _PASSWORD)
    via_token = await other_node.validate_token(token)
    assert first.user_id == again.user_id == via_token.user_id
    assert jwt.decode(token, _SECRET, algorithms=["HS256"])["sub"] == first.user_id


async def test_two_first_sign_ins_at_once_agree_on_one_id(admin_db):
    from provisa.auth.simple_user_ids import SimpleUserIds

    ids = await asyncio.gather(*(SimpleUserIds(admin_db).id_for("newcomer") for _ in range(8)))
    assert len(set(ids)) == 1


async def test_a_wrong_password_assigns_no_id(admin_db):
    from sqlalchemy import select

    with pytest.raises(ValueError):
        await _provider(admin_db).validate_basic(_basic("wrong"))
    async with admin_db.acquire() as conn:
        rows = (await conn.execute_core(select(schema_admin.simple_user_ids))).fetchall()
    assert rows == []


def test_the_provider_is_not_built_without_the_platform_plane():
    from provisa.auth.wiring import build_auth_provider

    with pytest.raises(ValueError, match="platform"):
        build_auth_provider(
            {"provider": "simple", "allow_simple_auth": True, "jwt_secret": _SECRET}
        )
