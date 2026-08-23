# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's own secrets store, against a real control plane (REQ-1557, REQ-1558).

The claim under test is about what is ON DISK and who can read it back, so a double would prove
nothing: the row has to be read with SQL to see that the stored bytes are not the value, and two
orgs have to exist in one registry to see that a binding to one resolves nothing of the other's.
"""

# Requirements: REQ-684, REQ-685, REQ-1557, REQ-1558

from __future__ import annotations

import base64
import os
import uuid

import pytest
from sqlalchemy import select

from provisa.core import secrets_store
from provisa.core.schema_admin import init_registry_schema, secrets_store as table
from provisa.core.secrets import resolve_secrets
from provisa.core.secrets_runtime import configure_secrets, reset_secrets

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

VALUE = "ghp_averyrealtokenshapedstring"


@pytest.fixture
async def plane(docker_postgres, monkeypatch):
    """A registry holding two orgs, with a master key the process actually has."""
    from provisa.core.database import Database, create_engine_from_url

    # The store refuses to write without one (REQ-1557); an explicit key keeps the test off the
    # host keychain entirely.
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(os.urandom(32)).decode())
    configure_secrets("provisa")

    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    admin_db = Database(create_engine_from_url(url, pool_size=2), name="admin")
    orgs = (f"sec{uuid.uuid4().hex[:8]}", f"sec{uuid.uuid4().hex[:8]}")
    for org_id in orgs:
        await init_registry_schema(admin_db, org_id)
    yield admin_db, orgs
    reset_secrets()


class TestWhatGoesInAndWhatComesBack:
    async def test_a_name_is_listed_but_its_value_is_not(self, plane):
        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE, actor="uid-admin")
        listed = await secrets_store.listing(admin_db, org)
        assert [s.name for s in listed] == ["GIT_TOKEN"]
        assert VALUE not in str([s.as_dict() for s in listed])
        assert listed[0].updated_by == "uid-admin"

    async def test_writing_the_same_name_replaces_rather_than_duplicates(self, plane):
        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE)
        await secrets_store.put(admin_db, org, "GIT_TOKEN", "ghp_rotated", description="rotated")
        listed = await secrets_store.listing(admin_db, org)
        assert len(listed) == 1
        assert listed[0].description == "rotated"
        async with secrets_store.bound(admin_db, org):
            assert resolve_secrets("${secret:GIT_TOKEN}") == "ghp_rotated"

    async def test_an_empty_value_is_refused(self, plane):
        admin_db, (org, _) = plane
        with pytest.raises(ValueError, match="cannot be empty"):
            await secrets_store.put(admin_db, org, "GIT_TOKEN", "")

    async def test_a_deleted_secret_stops_resolving(self, plane):
        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE)
        assert await secrets_store.remove(admin_db, org, "GIT_TOKEN") is True
        assert await secrets_store.remove(admin_db, org, "GIT_TOKEN") is False
        async with secrets_store.bound(admin_db, org):
            with pytest.raises(KeyError):
                resolve_secrets("${secret:GIT_TOKEN}")


class TestWhatIsOnDisk:
    async def test_the_stored_bytes_are_not_the_value(self, plane):
        """REQ-685: the row is an envelope blob, so a registry copied without the key is inert."""
        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE)
        async with admin_db.acquire() as conn:
            result = await conn.execute_core(
                select(table.c.value).where(table.c.org_id == org, table.c.name == "GIT_TOKEN")
            )
            blob = result.fetchone()[0]
        assert isinstance(blob, bytes)
        assert VALUE.encode() not in blob

    async def test_a_reader_without_the_key_gets_nothing_out(self, plane, monkeypatch):
        """The authority to read is the master key, not access to the table (REQ-1557)."""
        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE)
        monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(os.urandom(32)).decode())
        with pytest.raises(Exception):  # noqa: B017 - any failure to decrypt is the point
            async with secrets_store.bound(admin_db, org):
                pass


class TestOneOrgAtATime:
    async def test_a_binding_resolves_only_the_bound_orgs_names(self, plane):
        admin_db, (first, second) = plane
        await secrets_store.put(admin_db, first, "GIT_TOKEN", VALUE)
        await secrets_store.put(admin_db, second, "OTHER_TOKEN", "ghp_theirs")
        async with secrets_store.bound(admin_db, first):
            assert resolve_secrets("${secret:GIT_TOKEN}") == VALUE
            with pytest.raises(KeyError, match="no secret named"):
                resolve_secrets("${secret:OTHER_TOKEN}")

    async def test_nothing_resolves_once_the_binding_ends(self, plane):
        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE)
        async with secrets_store.bound(admin_db, org):
            pass
        with pytest.raises(KeyError, match="no organization is bound"):
            resolve_secrets("${secret:GIT_TOKEN}")

    async def test_a_binding_survives_the_hop_into_a_worker_thread(self, plane):
        """Every git call resolves its remote in a thread (REQ-1525), so the org must travel."""
        import asyncio

        admin_db, (org, _) = plane
        await secrets_store.put(admin_db, org, "GIT_TOKEN", VALUE)
        async with secrets_store.bound(admin_db, org):
            resolved = await asyncio.to_thread(
                resolve_secrets, "https://${secret:GIT_TOKEN}@git.example/acme.git"
            )
        assert resolved == f"https://{VALUE}@git.example/acme.git"
