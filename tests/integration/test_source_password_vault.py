# Copyright (c) 2026 Kenneth Stott
# Canary: 6a3c81df-2b40-4d97-8e15-c0f7429b6d31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1695: a source registered through the admin API keeps its password, and keeps it properly.

The credential goes into the ORG VAULT (provisa.core.secrets_store -- encrypted at rest, values
unreadable by name) and the ``sources`` row keeps only the ``${secret:NAME}`` that names it. A
value the operator already wrote as a reference is stored verbatim instead, because putting a
reference into the vault would store the reference TEXT as a credential.

What this fixes: before the column existed, a source created through the Sources form reached the
REQ-1673 introspection seam with an empty password, so Register Table listed no schema for any
connector that authenticates.
"""

import base64
import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    # The vault encrypts what it stores (REQ-685), and this host has no keychain to mint a master
    # key in -- the same explicit key every other secrets-store suite supplies.
    os.environ["PROVISA_ENCRYPTION_KEY"] = base64.b64encode(bytes(range(1, 33))).decode()

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _gql(client, query):
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200
    body = resp.json()
    assert "errors" not in body, body["errors"]
    return body


def _create(source_id: str, password: str) -> str:
    # A `files` source needs no reachable server, so the mutation's REQ-012 connection validation
    # passes and what is under test -- where the password went -- is what the test observes.
    return f"""mutation {{ createSource(input: {{
        id: "{source_id}", type: "files", host: "", port: 0, database: "",
        username: "", password: "{password}", path: "/tmp"
    }}) {{ success message }} }}"""


async def _row(source_id: str) -> dict:
    from provisa.api.app import state
    from provisa.core.repositories import source as source_repo

    async with state.tenant_db.acquire() as conn:
        row = await source_repo.get(conn, source_id)
    assert row is not None, f"source {source_id!r} was not persisted"
    return row


async def _vault_names() -> set[str]:
    """Every name in the acting org's vault. Names only -- nothing returns a stored value."""
    from provisa.api.app import state
    from provisa.core import secrets_store

    listing = await secrets_store.listing(
        state.admin_db, state.org_id, owner_id=secrets_store.ORG_OWNER
    )
    return {info.name for info in listing}


class TestLiteralPassword:
    async def test_the_literal_goes_to_the_vault_and_the_row_keeps_the_reference(self, client):
        source_id = "req1695_literal"
        secret = "s3cr3t-not-in-the-row"
        result = (await _gql(client, _create(source_id, secret)))["data"]["createSource"]
        assert result["success"] is True, result["message"]

        row = await _row(source_id)
        assert row["password_ref"] == "${secret:source_req1695_literal_password}"
        # The credential itself is nowhere in the control-plane row.
        assert secret not in str(row)
        assert "source_req1695_literal_password" in await _vault_names()

    async def test_the_row_reads_back_as_a_source_carrying_the_reference(self, client):
        from provisa.core.repositories.source import source_from_row

        assert (
            source_from_row(await _row("req1695_literal")).password
            == "${secret:source_req1695_literal_password}"
        )

    async def test_retyping_the_password_rotates_the_one_vault_entry(self, client):
        source_id = "req1695_literal"
        update = _create(source_id, "rotated-value").replace("createSource", "updateSource")
        result = (await _gql(client, update))["data"]["updateSource"]
        assert result["success"] is True, result["message"]

        assert (await _row(source_id))["password_ref"] == (
            "${secret:source_req1695_literal_password}"
        )
        # A rotation is the same name, not a second secret.
        assert len([n for n in await _vault_names() if n == "source_req1695_literal_password"]) == 1

    async def test_deleting_the_source_removes_the_secret_it_minted(self, client):
        result = (
            await _gql(client, 'mutation { deleteSource(id: "req1695_literal") { success } }')
        )["data"]["deleteSource"]
        assert result["success"] is True
        assert "source_req1695_literal_password" not in await _vault_names()


class TestReferencePassword:
    async def test_a_reference_is_stored_verbatim_and_mints_nothing(self, client):
        source_id = "req1695_ref"
        reference = "${env:PROVISA_DEMO_SPLUNK_PASSWORD}"
        result = (await _gql(client, _create(source_id, reference)))["data"]["createSource"]
        assert result["success"] is True, result["message"]

        assert (await _row(source_id))["password_ref"] == reference
        assert "source_req1695_ref_password" not in await _vault_names()

    async def test_deleting_it_leaves_the_operators_own_secrets_alone(self, client):
        """The source names a secret it did not mint, so deleting the source does not delete it."""
        from provisa.api.app import state
        from provisa.core import secrets_store

        await secrets_store.put(
            state.admin_db,
            state.org_id,
            "an_operators_own_secret",
            "kept",
            owner_id=secrets_store.ORG_OWNER,
        )
        result = (await _gql(client, 'mutation { deleteSource(id: "req1695_ref") { success } }'))[
            "data"
        ]["deleteSource"]
        assert result["success"] is True
        assert "an_operators_own_secret" in await _vault_names()


class TestNoPassword:
    async def test_a_source_with_no_password_stores_the_empty_string(self, client):
        result = (await _gql(client, _create("req1695_none", "")))["data"]["createSource"]
        assert result["success"] is True, result["message"]
        assert (await _row("req1695_none"))["password_ref"] == ""
        assert "source_req1695_none_password" not in await _vault_names()
