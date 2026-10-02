# Copyright (c) 2026 Kenneth Stott
# Canary: 3f2b8d16-9c47-4a5e-b0d1-7e6a2c4f9b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-686: RLS filter_expr column encryption, proven end-to-end against live PG.

The RLS predicate is injected as SQL at every governance read, so it is sensitive
metadata. This proves the repository boundary: ``rls_repo.upsert`` stores ciphertext
(the predicate never appears in the BYTEA column) and ``rls_repo.list_all`` /
``list_for_role`` decrypt it back to SQL on read. A wrong master key cannot recover
it. Uses a throwaway schema so it never touches real config.
"""

from __future__ import annotations

import base64
import os

import pytest
from provisa.core.database import create_engine_from_url

from provisa.core.database import Database
from provisa.core.models import RLSRule
from provisa.core.repositories import rls as rls_repo
from provisa.encryption import configure_encryption, reset_encryption

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_PG_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "test_req686_rls"

_PREDICATE = "region = 'us-east' AND owner = current_setting('provisa.role')"


@pytest.fixture(autouse=True)
def _enc(monkeypatch, tmp_path):
    import keyring
    from keyring.backends.null import Keyring as NullKeyring

    # LocalKeychain reads the OS keyring, then $PROVISA_DATA_DIR/encryption, BEFORE
    # PROVISA_ENCRYPTION_KEY (REQ-684, REQ-1802): isolate both so the env key is the one in force
    # and a key rotation in the test actually changes the key (and the host's store is untouched).
    previous_keyring = keyring.get_keyring()
    keyring.set_keyring(NullKeyring())
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    reset_encryption()
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(bytes(range(1, 33))).decode())
    configure_encryption("local")
    yield
    reset_encryption()
    keyring.set_keyring(previous_keyring)


@pytest.fixture
async def db():
    engine = create_engine_from_url(_PG_URL)
    database = Database(engine, name="req686rls", search_path=_SCHEMA)
    # The canonical org metadata, not hand-written DDL: a hand copy of rls_rules drifted (it lacked
    # REQ-1679's action_name) and every read then failed on the missing column. The integration
    # stack is self-provisioned (conftest _require_stack): a setup failure is a real failure, never
    # a skip.
    from sqlalchemy import text

    from provisa.core.schema_org import metadata as org_metadata

    with engine.begin() as sc:
        sc.execute(text(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE"))
        sc.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
        sc.execute(text(f"SET search_path TO {_SCHEMA}"))
        org_metadata.create_all(sc)
        # rls_rules references domains/roles; seed the rows the tests' rules point at.
        sc.execute(text("INSERT INTO domains (id, origin) VALUES ('sales', 'admin')"))
        sc.execute(text("INSERT INTO roles (id, origin) VALUES ('analyst', 'admin')"))
    yield database
    async with database.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    engine.dispose()


async def test_filter_stored_ciphertext_and_decrypts_on_read(db):
    rule = RLSRule(domain_id="sales", role_id="analyst", filter=_PREDICATE)
    async with db.acquire() as conn:
        await rls_repo.upsert(conn, rule, origin="admin")

        raw = await conn.fetchrow(
            f"SELECT filter_expr FROM {_SCHEMA}.rls_rules WHERE role_id='analyst'"
        )
        stored = bytes(raw["filter_expr"])
        assert b"region = 'us-east'" not in stored  # ciphertext at rest

        loaded = await rls_repo.list_all(conn)
        assert [r["filter_expr"] for r in loaded] == [_PREDICATE]

        for_role = await rls_repo.list_for_role(conn, "analyst")
        assert for_role[0]["filter_expr"] == _PREDICATE


async def test_wrong_master_key_cannot_read(db, monkeypatch):
    async with db.acquire() as conn:
        await rls_repo.upsert(
            conn, RLSRule(domain_id="sales", role_id="analyst", filter=_PREDICATE), origin="admin"
        )

    # Rotate to a different master key — the stored ciphertext must no longer decrypt.
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(bytes(range(33, 65))).decode())
    configure_encryption("local")
    async with db.acquire() as conn:
        with pytest.raises(Exception):
            await rls_repo.list_all(conn)
