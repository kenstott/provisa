# Copyright (c) 2026 Kenneth Stott
# Canary: e40c9b73-1a86-4d52-8f37-2b95ce104a6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The repository integration as the control plane actually stores it (REQ-1527).

Against a real registry, because the claim under test is that the columns exist on a registry that
predates them (V1 adds no migrations -- ``add_missing_columns`` is what makes that true) and that a
remote carrying a secret reference is stored verbatim rather than resolved on the way in.
"""

from __future__ import annotations

import os
import uuid
import pytest
from dulwich.repo import Repo

from provisa.core.env_ci import announce, read_integration, write_integration
from provisa.core.env_repo import commit_files, ensure_repo

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ORG_REMOTE_REFERENCE = "https://${env:TEST_GIT_TOKEN}@github.example/acme/model.git"


@pytest.fixture
async def registry(docker_postgres):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import init_registry_schema

    org_id = f"envci{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    admin_db = Database(create_engine_from_url(url, pool_size=2), name="admin")
    await init_registry_schema(admin_db, org_id)
    yield admin_db, org_id
    await admin_db.engine.dispose()


@pytest.fixture(autouse=True)
def repo_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))


class TestWhatTheControlPlaneHolds:
    async def test_an_org_that_configured_nothing_is_not_configured(self, registry):
        admin_db, org_id = registry
        integration = await read_integration(admin_db, org_id)
        assert integration == type(integration)()
        assert integration.configured is False

    async def test_both_halves_survive_the_round_trip(self, registry):
        admin_db, org_id = registry
        await write_integration(
            admin_db, org_id, remote="https://git.example/x.git", status_webhook="https://ci/x"
        )
        integration = await read_integration(admin_db, org_id)
        assert integration.remote == "https://git.example/x.git"
        assert integration.status_webhook == "https://ci/x"
        assert integration.configured is True

    async def test_a_secret_reference_is_stored_and_not_resolved(self, registry, monkeypatch):
        admin_db, org_id = registry
        monkeypatch.setenv("TEST_GIT_TOKEN", "s3cret")
        await write_integration(admin_db, org_id, remote=ORG_REMOTE_REFERENCE, status_webhook=None)
        stored = (await read_integration(admin_db, org_id)).remote
        assert stored == ORG_REMOTE_REFERENCE
        assert "s3cret" not in stored

    async def test_clearing_the_remote_stops_the_mirror(self, registry):
        admin_db, org_id = registry
        await write_integration(admin_db, org_id, remote="https://git/x", status_webhook=None)
        await write_integration(admin_db, org_id, remote=None, status_webhook=None)
        assert (await read_integration(admin_db, org_id)).configured is False

    async def test_an_unknown_org_is_an_error_and_not_an_empty_integration(self, registry):
        admin_db, _org_id = registry
        with pytest.raises(KeyError):
            await read_integration(admin_db, "no-such-org")


class TestTheProjectionReachesTheConfiguredRemote:
    async def test_the_remote_holds_the_sha_the_status_names(self, registry, tmp_path, monkeypatch):
        admin_db, org_id = registry
        remote = tmp_path / "remote.git"
        Repo.init_bare(str(remote), mkdir=True)
        monkeypatch.setenv("TEST_ORG_REMOTE", str(remote))
        await write_integration(
            admin_db, org_id, remote="${env:TEST_ORG_REMOTE}", status_webhook="https://ci/x"
        )
        posted: list[dict] = []

        async def _post(_url, payload):
            posted.append(payload)

        monkeypatch.setattr("provisa.core.env_ci.post_status", _post)
        sha = commit_files(
            ensure_repo(org_id), "prod", {"sales/domain.yaml": "id: sales\n"}, "seed", None
        )
        await announce(admin_db, org_id, "prod", sha, False)
        assert posted == [
            {
                "org": org_id,
                "environment": "prod",
                "sha": sha,
                "drifted": False,
                "pushed": True,
            }
        ]
        assert Repo(str(remote)).refs[b"refs/heads/prod"].decode() == sha
