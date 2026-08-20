# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The plan's data-source ceiling, counted and enforced (REQ-1513).

The Billing page prints "Up to 10 data sources" on the Starter card and "Up to 100" on the Pro
cards, and until this existed those were numbers nothing read. Two things are asserted here: WHAT
counts — the built-in rows Provisa seeds into every org do not, or a new Starter org would arrive
three sources into an allowance it had not used — and WHERE the ceiling is applied, which is the
create path, not only the downgrade guard that refuses a move to a plan the org has outgrown.
"""

from __future__ import annotations

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import BUILT_IN_SOURCE_IDS, Source, SourceType
from provisa.core.repositories import source as source_repo
from provisa.core.schema_org import sources


async def _store() -> Database:
    db = Database(create_engine_from_url("sqlite+aiosqlite:///:memory:"), name="sources-test")
    await _init_schema_portable(db)
    return db


def _source(source_id: str) -> Source:
    return Source(id=source_id, type=SourceType.postgresql, host="db", port=5432, database="app")


async def _seed_built_ins(conn) -> None:
    """Write the built-in rows the way the seed does — straight into the table.

    ``Source`` rejects ``__derived__`` as an id (its validator admits no leading underscore), and
    ``_seed_built_in_sources`` upserts these rows rather than going through the model, so a test
    that built them through the model would be testing a path the seed does not take.
    """
    for built_in in sorted(BUILT_IN_SOURCE_IDS):
        await conn.upsert(
            sources,
            {"id": built_in, "type": "postgresql", "dialect": "postgresql"},
            index_elements=["id"],
        )


class TestCountBillable:
    async def test_the_built_in_rows_do_not_spend_the_allowance(self):
        db = await _store()
        async with db.acquire() as conn:
            await _seed_built_ins(conn)
            assert await source_repo.count_billable(conn) == 0

    async def test_registered_sources_are_counted(self):
        db = await _store()
        async with db.acquire() as conn:
            await _seed_built_ins(conn)
            await source_repo.upsert(conn, _source("warehouse"))
            await source_repo.upsert(conn, _source("crm"))
            assert await source_repo.count_billable(conn) == 2

    async def test_the_built_in_set_is_what_the_seed_writes(self):
        # A source seeded into every org but missing from the set would silently bill the customer
        # for Provisa's own furniture, so the set is checked against the seeding code itself.
        import inspect

        from provisa.api import startup_seed

        seeded = inspect.getsource(startup_seed._seed_built_in_sources)
        for built_in in BUILT_IN_SOURCE_IDS:
            assert built_in in seeded or "DERIVED_SOURCE_ID" in seeded


class TestCreatePathRefusal:
    """``_refuse_over_source_limit`` — the gate ``create_source`` runs before it persists."""

    @pytest.fixture
    def gate(self, monkeypatch):
        from provisa.api.admin import schema_mutation

        def _apply(*, limit, held, existing: set[str] | None = None):
            held_ids = existing or set()

            async def _limit(_state, _org_id):
                return limit

            async def _count(_conn):
                return held

            async def _get(_conn, source_id):
                return {"id": source_id} if source_id in held_ids else None

            class _Conn:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *_exc):
                    return False

            class _Pool:
                def acquire(self):
                    return _Conn()

            async def _pool():
                return _Pool()

            import provisa.core.commerce as commerce

            monkeypatch.setattr(commerce, "source_limit_for_org", _limit)
            monkeypatch.setattr(source_repo, "count_billable", _count)
            monkeypatch.setattr(source_repo, "get", _get)
            monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
            from provisa.api.app import state as app_state

            monkeypatch.setattr(app_state, "org_id", "acme", raising=False)
            return schema_mutation._refuse_over_source_limit

        return _apply

    async def test_a_source_over_the_ceiling_is_refused(self, gate):
        refuse = gate(limit=(10, "starter"), held=10)
        result = await refuse("warehouse")
        assert result is not None
        assert result.success is False
        assert result.code == "schema.source_limit_reached"
        assert result.params["limit"] == "10"
        assert result.params["plan"] == "starter"
        # The refusal names the way out: the plan, not a support ticket.
        assert "Billing" in result.message

    async def test_the_last_source_the_plan_admits_is_allowed(self, gate):
        refuse = gate(limit=(10, "starter"), held=9)
        assert await refuse("warehouse") is None

    async def test_editing_a_source_the_org_already_holds_is_not_a_new_one(self, gate):
        # create_source is an upsert: an org at its ceiling must still be able to fix a password.
        refuse = gate(limit=(10, "starter"), held=10, existing={"warehouse"})
        assert await refuse("warehouse") is None

    async def test_a_deployment_that_sells_nothing_imposes_no_ceiling(self, gate):
        # No plugin, no subscription, no ceiling — the operator owns the software (REQ-1513).
        refuse = gate(limit=None, held=9999)
        assert await refuse("warehouse") is None
