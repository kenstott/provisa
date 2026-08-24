# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's key ring against a real registry (REQ-1574).

A real schema because the claim being made is about what SURVIVES a rotation: the retired row, and
the payload that names it. A double holding a dict would agree with any implementation, including
one that overwrote the key in place and quietly stranded everything already written under it.
"""

from __future__ import annotations

import base64
import os
import uuid

import pytest
from sqlalchemy import select

from provisa.core.org_encryption import (
    KEY_BYTES,
    OrgKeyError,
    load_org_ring,
    org_key_status,
    ring_owner_org_ids,
    set_org_key,
)
from provisa.core.schema_admin import init_registry_schema, org_encryption_keys
from provisa.encryption import envelope_key_id, reset_encryption

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ACTOR = "uid-admin"


@pytest.fixture
async def registry(docker_postgres):
    """A real registry, plus a maker for orgs that exist in it (``org_encryption_keys`` has an FK)."""
    from provisa.core.database import Database, create_engine_from_url

    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = create_engine_from_url(url, pool_size=2)
    admin_db = Database(engine, name="admin")

    async def new_org() -> str:
        org_id = f"orgkey{uuid.uuid4().hex[:8]}"
        await init_registry_schema(admin_db, org_id)
        return org_id

    yield type("Registry", (), {"db": admin_db, "new_org": staticmethod(new_org)})
    reset_encryption()
    await engine.dispose()


async def _rows(db, org_id):
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(
                org_encryption_keys.c.key_id,
                org_encryption_keys.c.adopts_unkeyed,
                org_encryption_keys.c.retired_at,
            ).where(org_encryption_keys.c.org_id == org_id)
        )
        return result.fetchall()


async def test_a_key_is_reported_by_fingerprint_and_never_by_value(registry):
    org = await registry.new_org()
    raw = os.urandom(KEY_BYTES)
    supplied = base64.b64encode(raw).decode()

    status = await set_org_key(registry.db, org, key_b64=supplied, actor=ACTOR)

    assert (status.key_id, status.supplied, status.retired_count) == ("k1", True, 0)
    assert supplied not in str(status.as_dict())
    assert set(status.as_dict()) == {
        "key_id",
        "fingerprint",
        "supplied",
        "created_at",
        "created_by",
        "retired_count",
    }
    stored = await org_key_status(registry.db, org)
    assert (stored.fingerprint, stored.created_by) == (status.fingerprint, ACTOR)


async def test_an_org_that_set_no_key_has_no_status_and_no_ring(registry):
    org = await registry.new_org()
    assert await org_key_status(registry.db, org) is None
    assert await load_org_ring(registry.db, org) is None
    assert org not in await ring_owner_org_ids(registry.db)


async def test_rotation_retires_rather_than_overwrites(registry):
    org = await registry.new_org()
    await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    written_under_k1 = (await load_org_ring(registry.db, org)).encrypt(b"before")
    assert envelope_key_id(written_under_k1) == "k1"

    rotated = await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    assert (rotated.key_id, rotated.supplied, rotated.retired_count) == ("k2", False, 1)

    ring = await load_org_ring(registry.db, org)
    # The retired key stays in the ring and goes on reading what it wrote: a rotation moves which
    # key NEW writes use, and is neither a re-encryption of what is stored nor a revocation of it.
    assert ring.decrypt(written_under_k1) == b"before"
    written_under_k2 = ring.encrypt(b"after")
    assert envelope_key_id(written_under_k2) == "k2"
    assert ring.decrypt(written_under_k2) == b"after"


async def test_only_the_first_key_adopts_what_the_deployment_key_wrote(registry):
    org = await registry.new_org()
    await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    assert [(r[0], bool(r[1])) for r in await _rows(registry.db, org)] == [("k1", True)]

    await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    # A rotation's predecessor already adopts them, so the successor must not: two adopting entries
    # would make which key opens a v1 blob a matter of row order.
    assert {r[0]: bool(r[1]) for r in await _rows(registry.db, org)} == {"k1": True, "k2": False}


async def test_exactly_one_key_is_active_after_a_rotation(registry):
    org = await registry.new_org()
    await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    assert [r[0] for r in await _rows(registry.db, org) if r[2] is None] == ["k2"]


async def test_two_active_keys_is_an_error_and_not_a_choice(registry):
    org = await registry.new_org()
    await set_org_key(registry.db, org, key_b64=None, actor=ACTOR)
    async with registry.db.acquire() as conn:
        await conn.execute_core(
            org_encryption_keys.insert().values(
                org_id=org,
                key_id="k99",
                wrapped_key=os.urandom(KEY_BYTES),
                fingerprint="deadbeefdeadbeef",
                supplied=False,
                adopts_unkeyed=False,
                created_by=ACTOR,
            )
        )
    with pytest.raises(OrgKeyError, match="exactly one is required"):
        await load_org_ring(registry.db, org)


async def test_a_key_that_is_not_a_key_is_refused_before_anything_is_written(registry):
    org = await registry.new_org()
    with pytest.raises(OrgKeyError):
        await set_org_key(
            registry.db, org, key_b64=base64.b64encode(os.urandom(16)).decode(), actor=ACTOR
        )
    assert await org_key_status(registry.db, org) is None
    assert await _rows(registry.db, org) == []


async def test_the_roster_names_each_org_holding_a_ring_once(registry):
    kept = await registry.new_org()
    rotated = await registry.new_org()
    keyless = await registry.new_org()
    await set_org_key(registry.db, kept, key_b64=None, actor=ACTOR)
    await set_org_key(registry.db, rotated, key_b64=None, actor=ACTOR)
    await set_org_key(registry.db, rotated, key_b64=None, actor=ACTOR)

    owners = await ring_owner_org_ids(registry.db)

    assert kept in owners
    assert keyless not in owners
    # One org, not one per ring entry: startup hands this straight to ``note_org_rings``.
    assert owners.count(rotated) == 1
