# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1f7a0e-3c64-4e29-9b57-2a8e6c41f0d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa apply --dry-run`` is a real server dry run (REQ-2222, amended 2026-11-09): the whole
model build runs in a transaction that is rolled back, and nothing is saved.

Against real Postgres, through the endpoint the CLI calls: the answer says what would change, the
store and its config stamps are as they were, no model commit is made, the engine and the source
pools are not touched, and a model a real apply refuses is refused by name. The last test applies
the same file for real: it proves the assertions above can fail."""

# Requirements: REQ-2222

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
import yaml
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

APPLY = "/admin/import/hasura/apply"
ROLE = "org_admin"  # holds every right the file's sections ask for (SECTION_RIGHTS)
ADMIN = {"x-provisa-role": ROLE}
BASE_URL = "http://test"
TABLE = "j1146_dry_run_orders"
SOURCE = "j1146pg"
DOMAIN = "j1146"


def _config(**table) -> str:
    return yaml.safe_dump(
        {
            "roles": [],  # required by the config model; org_admin is a standard role of the org
            "domains": [{"id": "j1146"}],
            "sources": [
                {
                    "id": "j1146pg",
                    "type": "postgresql",
                    "host": "localhost",
                    "port": 1,
                    "database": "d",
                    "username": "u",
                    "password": "p",
                }
            ],
            "tables": [
                {
                    "source_id": "j1146pg",
                    "domain_id": "j1146",
                    "schema": "public",
                    "table": TABLE,
                    "columns": [{"name": "id", "data_type": "integer", "visible_to": [ROLE]}],
                    **table,
                }
            ],
        }
    )


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client(docker_postgres):
    os.environ.setdefault("PG_PASSWORD", "provisa")
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=BASE_URL, timeout=120
        ) as c:
            yield c


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def plane(client, tenant_db):
    """Reads of the org's own schema, on a connection of the test's."""
    from provisa.api.app import state

    schema = f'"org_{state.org_id}"'

    class Plane:
        async def tables(self) -> int:
            async with tenant_db.acquire() as conn:
                return await conn.fetchval(
                    f"SELECT count(*) FROM {schema}.registered_tables WHERE table_name = $1", TABLE
                )

        async def stamps(self) -> dict[str, int]:
            async with tenant_db.acquire() as conn:
                rows = await conn.fetch(f"SELECT kind, stamp FROM {schema}.config_stamp")
            return {r["kind"]: int(r["stamp"]) for r in rows}

    yield Plane()
    # The control test applies for real: its source (port 1) must not reach later modules' boots.
    # Deleting these two cascades to the table and its columns.
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DELETE FROM {schema}.sources WHERE id = $1", SOURCE)
        await conn.execute(f"DELETE FROM {schema}.domains WHERE id = $1", DOMAIN)


@pytest.fixture
def effects(monkeypatch):
    """Spies on what an apply does outside its transaction."""
    from provisa.api.app import state
    from provisa.core import model_change

    runtime = state._default_runtime()
    engine = MagicMock(wraps=runtime.federation_engine)
    pools = MagicMock(wraps=runtime.source_pools)
    pools.add = AsyncMock()
    commit = AsyncMock(return_value=None)
    monkeypatch.setattr(runtime, "federation_engine", engine)
    monkeypatch.setattr(runtime, "source_pools", pools)
    monkeypatch.setattr(model_change, "_commit", commit)
    return MagicMock(engine=engine, pools=pools, commit=commit)


class TestDryRun:
    async def test_reports_what_would_change_and_saves_nothing(self, client, plane, effects):
        stamps = await plane.stamps()
        resp = await client.post(
            APPLY, json={"config_yaml": _config(), "dry_run": True}, headers=ADMIN
        )
        assert resp.status_code == 200, resp.text
        report = resp.json()["dry_run"]
        assert report["changes"] is True
        assert f"j1146pg/public/{TABLE}" in report["sections"]["tables"]["add"]
        assert "j1146pg" in report["sections"]["sources"]["add"]
        assert report["not_run"], "the answer names what a dry run does not exercise"

        assert await plane.tables() == 0
        assert await plane.stamps() == stamps
        effects.commit.assert_not_awaited()
        effects.engine.register_source.assert_not_called()
        effects.pools.add.assert_not_awaited()

    async def test_leaves_the_domain_policy_as_it_was(self, client, plane):
        from provisa.api.app import state
        from provisa.core import domain_policy
        from provisa.core.request_context import current_org

        token = current_org.set(state.org_id)
        try:
            held = domain_policy.snapshot()
        finally:
            current_org.reset(token)
        model = yaml.safe_load(_config())
        model["naming"] = {"use_domains": True, "default_domain": "j1146"}
        resp = await client.post(
            APPLY, json={"config_yaml": yaml.safe_dump(model), "dry_run": True}, headers=ADMIN
        )
        assert resp.status_code == 200, resp.text
        token = current_org.set(state.org_id)
        try:
            assert domain_policy.snapshot() == held
        finally:
            current_org.reset(token)

    async def test_a_bad_model_is_refused_by_name_and_saves_nothing(self, client, plane):
        stamps = await plane.stamps()
        # A declared watermark that is no column of the table: a real apply refuses it in the build.
        resp = await client.post(
            APPLY,
            json={"config_yaml": _config(watermark_column="no_such"), "dry_run": True},
            headers=ADMIN,
        )
        assert resp.status_code == 400, resp.text
        body = resp.json()
        assert body["code"] == "import.refused", body
        assert "no_such" in body["detail"] and TABLE in body["detail"], body
        assert await plane.tables() == 0
        assert await plane.stamps() == stamps

    async def test_a_names_clash_only_the_build_finds_is_refused_by_name(self, client, plane):
        # Two columns differing only by case: the store keys them apart, and SQL's surface matches
        # names ignoring case (REQ-2214), so only the schema build's names table refuses them
        # (REQ-194, refuse being the default policy).
        model = yaml.safe_load(_config())
        for name in ("Code", "code"):
            model["tables"][0]["columns"].append(
                {"name": name, "data_type": "integer", "visible_to": [ROLE]}
            )
        stamps = await plane.stamps()
        resp = await client.post(
            APPLY, json={"config_yaml": yaml.safe_dump(model), "dry_run": True}, headers=ADMIN
        )
        assert resp.status_code == 400, resp.text
        body = resp.json()
        assert body["code"] == "import.refused", body
        assert "would both be named" in body["detail"], body
        assert await plane.tables() == 0
        assert await plane.stamps() == stamps

    async def test_the_same_file_applied_for_real_changes_the_store(self, client, plane, effects):
        # The control: every "nothing changed" above could otherwise pass on a dead endpoint.
        stamps = await plane.stamps()
        resp = await client.post(APPLY, json={"config_yaml": _config()}, headers=ADMIN)
        assert resp.status_code == 200, resp.text
        assert resp.json()["dry_run"] is None
        assert await plane.tables() == 1
        assert await plane.stamps() != stamps
        effects.commit.assert_awaited()
        effects.engine.register_source.assert_called()
