# Copyright (c) 2026 Kenneth Stott
# Canary: 3a5d8f61-27c4-4b9e-b0d3-6c14e2f79a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin schema commits the model after every mutation that landed (REQ-1524)."""

# Requirements: REQ-1487, REQ-1489, REQ-1524

from __future__ import annotations

import types
from contextlib import asynccontextmanager

import pytest
import strawberry

from provisa.api.admin import model_commit
from provisa.api.org_runtime import reset_current_org, set_current_org


class _FakeDb:
    @asynccontextmanager
    async def acquire(self):
        yield "connection"


@strawberry.type
class Query:
    @strawberry.field
    def ping(self) -> str:
        return "pong"


@strawberry.type
class Mutation:
    @strawberry.mutation
    def touch(self) -> str:
        return "touched"

    @strawberry.mutation
    def explode(self) -> str:
        raise ValueError("the resolver failed")


schema = strawberry.Schema(
    query=Query, mutation=Mutation, extensions=[model_commit.ModelCommitExtension]
)


@pytest.fixture
def commits(monkeypatch):
    """Every write_through the extension asked for, and a bound org/admin plane to ask on."""
    calls: list[dict] = []

    async def _write_through(conn, admin_db, org_id, env, schema_name, message, actor):
        calls.append(
            {
                "conn": conn,
                "org_id": org_id,
                "env": env,
                "schema": schema_name,
                "message": message,
                "actor": actor,
            }
        )
        return "0" * 40

    monkeypatch.setattr(model_commit, "write_through", _write_through)
    state = types.SimpleNamespace(org_id="root", admin_db=_FakeDb(), tenant_db=_FakeDb())
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    return calls


def _context(user_id: str | None):
    identity = types.SimpleNamespace(user_id=user_id) if user_id is not None else None
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))
    return {"request": request}


class TestAMutationCommits:
    @pytest.mark.asyncio
    async def test_a_landed_mutation_projects_the_model(self, commits):
        result = await schema.execute("mutation { touch }", context_value=_context("ada"))
        assert result.errors is None
        assert len(commits) == 1
        assert commits[0]["conn"] == "connection"

    @pytest.mark.asyncio
    async def test_the_acting_user_authors_the_commit(self, commits):
        await schema.execute("mutation { touch }", context_value=_context("ada"))
        assert commits[0]["actor"] == "ada"

    @pytest.mark.asyncio
    async def test_an_unauthenticated_act_names_no_author(self, commits):
        await schema.execute("mutation { touch }", context_value=_context("anonymous"))
        assert commits[0]["actor"] is None

    @pytest.mark.asyncio
    async def test_the_operation_name_becomes_the_message(self, commits):
        await schema.execute("mutation Rename { touch }", context_value=_context("ada"))
        assert commits[0]["message"] == "Rename"


class TestWhatIsNotCommitted:
    @pytest.mark.asyncio
    async def test_a_query_commits_nothing(self, commits):
        result = await schema.execute("{ ping }", context_value=_context("ada"))
        assert result.errors is None
        assert commits == []

    @pytest.mark.asyncio
    async def test_a_failed_mutation_commits_nothing(self, commits):
        result = await schema.execute("mutation { explode }", context_value=_context("ada"))
        assert result.errors
        assert commits == []


class TestWhichEnvironmentTheChangeBelongsTo:
    @pytest.mark.asyncio
    async def test_an_unbound_request_is_prod_of_the_default_org(self, commits):
        await schema.execute("mutation { touch }", context_value=_context("ada"))
        assert commits[0]["org_id"] == "root"
        assert commits[0]["env"] == "prod"
        assert commits[0]["schema"] == "org_root"

    @pytest.mark.asyncio
    async def test_a_bound_org_commits_to_its_own_repository(self, commits):
        token = set_current_org("acme")
        try:
            await schema.execute("mutation { touch }", context_value=_context("ada"))
        finally:
            reset_current_org(token)
        assert commits[0]["org_id"] == "acme"
        assert commits[0]["schema"] == "org_acme"

    @pytest.mark.asyncio
    async def test_a_branch_request_commits_to_its_branch(self, commits, monkeypatch):
        monkeypatch.setattr(model_commit, "active_env", lambda: "dev")
        await schema.execute("mutation { touch }", context_value=_context("ada"))
        assert commits[0]["env"] == "dev"
        assert commits[0]["schema"] == "org_root_env_dev"


class TestAProjectionNeverFailsTheChange:
    @pytest.mark.asyncio
    async def test_a_pool_that_cannot_be_acquired_leaves_the_mutation_standing(self, monkeypatch):
        class _BrokenDb:
            @asynccontextmanager
            async def acquire(self):
                raise RuntimeError("no connection")
                yield  # pragma: no cover — unreachable, present so this is a generator

        state = types.SimpleNamespace(org_id="root", admin_db=_FakeDb(), tenant_db=_BrokenDb())
        monkeypatch.setattr("provisa.api.app.state", state, raising=False)
        result = await schema.execute("mutation { touch }", context_value=_context("ada"))
        assert result.errors is None
        assert result.data == {"touch": "touched"}

    @pytest.mark.asyncio
    async def test_a_server_with_no_control_plane_commits_nothing(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            model_commit,
            "write_through",
            lambda *a, **k: called.append(a),  # pyright: ignore[reportUnknownLambdaType]
        )
        state = types.SimpleNamespace(org_id="root", admin_db=None, tenant_db=None)
        monkeypatch.setattr("provisa.api.app.state", state, raising=False)
        result = await schema.execute("mutation { touch }", context_value=_context("ada"))
        assert result.errors is None
        assert called == []
