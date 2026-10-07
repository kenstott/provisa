# Copyright (c) 2026 Kenneth Stott
# Canary: 3bd72a75-a556-408d-bd06-87b60f40e29e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org secret is deleted only when no stored value of the org names it (REQ-1918).

A secret is referred to by text, ``${secret:NAME}``, in a source's password reference, a hint, a
header, a setting. The delete is refused while any stored row of the org names it — in whichever
environment of the org the row sits, or among the org's own rows in the platform plane — listing
each one, and nothing is removed. The search reads stored text only: it resolves nothing and
reads no secret's value.

The scenarios run here on SQLite control planes, and unchanged on PostgreSQL in
``tests/integration/test_secret_delete_guarded_pg.py``.
"""

# Requirements: REQ-1918, REQ-1558

from __future__ import annotations

import base64
import os
from dataclasses import dataclass

import pytest
from sqlalchemy import insert, select, update

from provisa.core import schema_admin, schema_org, secret_references, secrets_store
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.secrets import resolve_secrets
from provisa.encryption.runtime import configure_encryption, reset_encryption
from provisa.core.secrets_runtime import configure_secrets, reset_secrets
from provisa.core.secrets_store import ORG_OWNER, SecretDeleteRefused

ORG = "acme"
OTHER = "globex"


@dataclass
class Planes:
    """The platform plane and the control plane of each environment ``ORG`` holds."""

    admin: Database
    environments: dict[str, Database]


@pytest.fixture(autouse=True)
def a_master_key(tmp_path, monkeypatch):
    """The vault refuses to write without a master key; this test's own, kept off the host."""
    import keyring
    from keyring.backends.null import Keyring as NullKeyring

    previous = keyring.get_keyring()
    keyring.set_keyring(NullKeyring())
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(os.urandom(32)).decode())
    configure_secrets("provisa")
    # A real envelope service, so the encrypted columns hold ciphertext the check has to open
    # (the passthrough service would store plaintext and prove nothing).
    configure_encryption("local")
    yield
    reset_encryption()
    reset_secrets()
    keyring.set_keyring(previous)


@pytest.fixture
async def planes(tmp_path) -> Planes:
    admin_engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with admin_engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
    admin = Database(admin_engine, name="admin")
    environments: dict[str, Database] = {}
    for env in ("prod", "dev"):
        # Each environment's plane is its model handle, guarded as the server's is (REQ-1922:
        # secrets_router._environment_planes passes ``runtime.model_db``).
        plane = Database(
            create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / f'{env}.db'}"),
            name=env,
            holds="model",
        )
        await _init_schema_portable(plane)
        environments[env] = plane
    await seed_orgs(admin)
    return Planes(admin, environments)


async def seed_orgs(admin: Database) -> None:
    async with admin.acquire() as conn:
        for org_id in (ORG, OTHER):
            await conn.execute_core(insert(schema_admin.orgs).values(id=org_id, name=org_id))


async def _secret(planes: Planes, name: str = "DB_PW", *, org: str = ORG) -> None:
    await secrets_store.put(planes.admin, org, name, "s3cret-value", owner_id=ORG_OWNER)


async def _source(planes: Planes, env: str, source_id: str, **values) -> None:
    async with planes.environments[env].acquire() as conn:
        await conn.execute_core(
            insert(schema_org.sources).values(id=source_id, type="postgresql", **values)
        )


async def _delete(planes: Planes, name: str = "DB_PW", *, org: str = ORG) -> bool:
    return await secrets_store.remove(
        planes.admin, org, name, owner_id=ORG_OWNER, environments=planes.environments
    )


def _named(refused: SecretDeleteRefused) -> list[tuple]:
    return sorted((r.environment or "", r.table, r.id, r.column) for r in refused.references)


async def _still_there(planes: Planes, name: str = "DB_PW") -> bool:
    listed = await secrets_store.listing(planes.admin, ORG, owner_id=ORG_OWNER)
    return name in [s.name for s in listed]


# --- refused, naming each value ---------------------------------------------------------------


async def test_a_source_that_names_the_secret_blocks_its_delete(planes):
    await _secret(planes)
    await _source(planes, "prod", "warehouse", password_ref="${secret:DB_PW}")

    with pytest.raises(SecretDeleteRefused) as err:
        await _delete(planes)

    assert _named(err.value) == [("prod", "sources", "warehouse", "password_ref")]
    assert err.value.references[0].as_dict() == {
        "kind": "sources",
        "id": "warehouse",
        "name": "warehouse",
        "column": "password_ref",
        "environment": "prod",
        "unreadable": False,
    }
    assert "Secret 'DB_PW' is still named by: sources 'warehouse' in prod" in str(err.value)
    assert await _still_there(planes)


async def test_a_reference_in_a_second_environment_blocks_too_and_says_where(planes):
    await _secret(planes)
    await _source(planes, "prod", "warehouse", password_ref="${secret:DB_PW}")
    # In the other environment the reference sits inside a JSON value, not a text column.
    await _source(
        planes, "dev", "lake", federation_hints={"token": "Bearer ${secret:DB_PW}", "role": "x"}
    )

    with pytest.raises(SecretDeleteRefused) as err:
        await _delete(planes)

    assert _named(err.value) == [
        ("dev", "sources", "lake", "federation_hints"),
        ("prod", "sources", "warehouse", "password_ref"),
    ]


async def test_one_of_the_orgs_own_platform_rows_blocks_and_another_orgs_does_not(planes):
    await _secret(planes)
    async with planes.admin.acquire() as conn:
        await conn.execute_core(
            update(schema_admin.orgs)
            .where(schema_admin.orgs.c.id == OTHER)
            .values(repo_remote="https://x:${secret:DB_PW}@git.example/other.git")
        )
    # Another org naming a secret of the same name names ITS OWN: nothing of ours is referred to.
    assert await _delete(planes) is True

    await _secret(planes)
    async with planes.admin.acquire() as conn:
        await conn.execute_core(
            update(schema_admin.orgs)
            .where(schema_admin.orgs.c.id == ORG)
            .values(repo_remote="https://x:${secret:DB_PW}@git.example/acme.git")
        )
    with pytest.raises(SecretDeleteRefused) as err:
        await _delete(planes)
    assert _named(err.value) == [("", "orgs", ORG, "repo_remote")]


# --- what does not block ----------------------------------------------------------------------


async def test_only_the_exact_reference_counts(planes):
    """A longer name, a name the underscore would match as a wildcard, and the personal vault's
    reference of the same name are other things."""
    await _secret(planes)
    await _source(planes, "prod", "a", password_ref="${secret:DB_PW2}")
    await _source(planes, "prod", "b", password_ref="${secret:DBXPW}")
    await _source(planes, "prod", "c", password_ref="${user:DB_PW}")
    await _source(planes, "dev", "d", description="rotate DB_PW quarterly")

    assert await _delete(planes) is True
    assert not await _still_there(planes)


async def test_once_nothing_names_it_the_secret_goes_and_stops_resolving(planes):
    await _secret(planes)
    await _source(planes, "dev", "lake", password_ref="${secret:DB_PW}")
    with pytest.raises(SecretDeleteRefused):
        await _delete(planes)

    async with planes.environments["dev"].acquire() as conn:
        await conn.execute_core(
            update(schema_org.sources)
            .where(schema_org.sources.c.id == "lake")
            .values(password_ref="${secret:OTHER_PW}")
        )
    assert await _delete(planes) is True
    assert await _delete(planes) is False
    async with secrets_store.bound(planes.admin, ORG):
        with pytest.raises(KeyError):
            resolve_secrets("${secret:DB_PW}")


async def test_a_personal_secret_is_its_owners_to_delete(planes):
    await secrets_store.put(planes.admin, ORG, "DB_PW", "mine", owner_id="uid-dev")
    await _source(planes, "prod", "warehouse", password_ref="${user:DB_PW}")
    assert await secrets_store.remove(planes.admin, ORG, "DB_PW", owner_id="uid-dev") is True


async def test_deleting_an_org_secret_needs_the_orgs_environments(planes):
    await _secret(planes)
    with pytest.raises(ValueError, match="environment control planes"):
        await secrets_store.remove(planes.admin, ORG, "DB_PW", owner_id=ORG_OWNER)
    assert await _still_there(planes)


# --- what the search reads --------------------------------------------------------------------


def test_every_binary_column_is_accounted_for():
    """Binary columns hold ciphertext or bytes. Each is either decrypted in memory and searched
    (the two whose plaintext is resolved for references when used) or listed with why it is not,
    so a new one cannot be added without deciding."""
    binary = secret_references.binary_columns(
        schema_org.metadata
    ) | secret_references.binary_columns(schema_admin.metadata)
    decrypted = set(secret_references.SEARCHED_DECRYPTED)
    not_searched = set(secret_references.NOT_SEARCHED)
    assert decrypted == {("api_sources", "auth"), ("org_secrets", "value_enc")}
    assert len(not_searched) == 10 and not (decrypted & not_searched)
    assert decrypted | not_searched == binary
    for metadata in (schema_org.metadata, schema_admin.metadata):
        for table in metadata.tables.values():
            for column in secret_references.searched_columns(table):
                assert (table.name, column.name) not in binary


# --- the two encrypted columns a reference can live in ----------------------------------------


def _encrypted(text: str) -> bytes:
    from provisa.encryption.runtime import encryption_service

    return encryption_service().encrypt(text.encode("utf-8"))


async def _api_source(planes: Planes, env: str, source_id: str, auth: bytes) -> None:
    async with planes.environments[env].acquire() as conn:
        await conn.execute_core(
            insert(schema_org.api_sources).values(
                id=source_id, type="openapi", base_url="https://api.example", auth=auth
            )
        )


async def test_a_reference_inside_an_api_sources_encrypted_auth_blocks(planes):
    await _secret(planes)
    await _api_source(
        planes, "dev", "billing", _encrypted('{"type": "bearer", "bearer": "${secret:DB_PW}"}')
    )
    await _api_source(planes, "dev", "crm", _encrypted('{"type": "bearer", "bearer": "literal"}'))

    with pytest.raises(SecretDeleteRefused) as err:
        await _delete(planes)

    assert _named(err.value) == [("dev", "api_sources", "billing", "auth")]
    assert err.value.references[0].unreadable is False
    # What is stored really is ciphertext: the reference's text is not in the stored bytes.
    async with planes.environments["dev"].acquire() as conn:
        stored = (
            await conn.execute_core(
                select(schema_org.api_sources.c.auth).where(
                    schema_org.api_sources.c.id == "billing"
                )
            )
        ).scalar_one()
    assert b"${secret:DB_PW}" not in bytes(stored)
    # Nothing decrypted leaves the check: the refusal names the row and no value.
    assert "bearer" not in str(err.value) + str([r.as_dict() for r in err.value.references])


async def test_a_reference_inside_an_orgs_encrypted_service_key_blocks(planes):
    await _secret(planes)
    async with planes.environments["prod"].acquire() as conn:
        await conn.execute_core(
            insert(schema_org.org_secrets).values(
                key="anthropic_api_key", value_enc=_encrypted("${secret:DB_PW}")
            )
        )
        await conn.execute_core(
            insert(schema_org.org_secrets).values(
                key="openai_api_key", value_enc=_encrypted("sk-a-literal-key")
            )
        )

    with pytest.raises(SecretDeleteRefused) as err:
        await _delete(planes)

    assert _named(err.value) == [("prod", "org_secrets", "anthropic_api_key", "value_enc")]
    assert "sk-a-literal-key" not in str(err.value)


async def test_an_encrypted_value_that_cannot_be_read_blocks_naming_its_row(planes):
    """An unreadable value is never taken for "no reference": the delete is refused, the row is
    named, and the refusal says it could not be read."""
    await _secret(planes)
    await _api_source(planes, "prod", "broken", b"not an envelope this worker can open")

    with pytest.raises(SecretDeleteRefused) as err:
        await _delete(planes)

    assert _named(err.value) == [("prod", "api_sources", "broken", "auth")]
    assert err.value.references[0].as_dict()["unreadable"] is True
    assert "api_sources 'broken' in prod (could not be read to check)" in str(err.value)
    assert await _still_there(planes)


async def test_encrypted_values_that_name_no_secret_do_not_block(planes):
    await _secret(planes)
    await _api_source(planes, "prod", "crm", _encrypted('{"type": "bearer", "bearer": "x"}'))
    await _api_source(planes, "dev", "other", _encrypted('{"bearer": "${secret:OTHER}"}'))
    assert await _delete(planes) is True


async def test_the_search_never_touches_the_vault(planes, monkeypatch):
    """It compares stored text with the reference's text: no secret's value is read."""
    await _secret(planes)
    await _source(planes, "prod", "warehouse", password_ref="${secret:DB_PW}")

    async def _refuse(*_a, **_k):
        raise AssertionError("the search decrypted the vault")

    monkeypatch.setattr(secrets_store, "_decrypted", _refuse)
    found = await secret_references.references(
        planes.admin, ORG, "DB_PW", environments=planes.environments
    )
    assert [(r.table, r.id) for r in found] == [("sources", "warehouse")]
