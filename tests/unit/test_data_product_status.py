# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1634 STATUS and REQ-2146: a data product's lifecycle -- draft, live, deprecated, retired,
each asserted by an action -- and the findings a curator should look at in the products."""

from __future__ import annotations

import datetime
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import insert, select

from provisa.core import product_findings
from provisa.core.models import DataProduct
from provisa.core.repositories import data_product as repo
from provisa.core.schema_org import (
    data_products,
    domains,
    registered_tables,
    sources,
    tag_assignments,
    tracked_functions,
)

TODAY = datetime.date(2026, 10, 23)
CTX = product_findings.Context(today=TODAY, support_contact_required=False)


@asynccontextmanager
async def _store(control_plane):
    """sales (with a steward) holding c360 (live, two member tables and a command) and c360_v2
    (a draft, no members); finance (no steward) holding ops_kpis."""
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales", steward="dana"))
            await conn.execute_core(insert(domains).values(id="finance"))
            await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
            for pid, domain, status in (
                ("c360", "sales", "live"),
                ("c360_v2", "sales", "draft"),
                ("ops_kpis", "finance", "live"),
            ):
                await conn.execute_core(
                    insert(data_products).values(
                        id=pid,
                        domain_id=domain,
                        name=pid.upper(),
                        owner_role="sales_lead",
                        purpose="One view",
                        status=status,
                    )
                )
            for tid, name in ((1, "customers"), (2, "orders")):
                await conn.execute_core(
                    insert(registered_tables).values(
                        id=tid,
                        source_id="crm",
                        domain_id="sales",
                        schema_name="public",
                        table_name=name,
                        product_id="c360",
                    )
                )
            await conn.execute_core(
                insert(tracked_functions).values(
                    name="refresh_customer", domain_id="sales", product_id="c360"
                )
            )
        yield db
    finally:
        await db.close()


async def _members(conn, product_id: str) -> int:
    return await repo.member_count(conn, product_id)


async def _row(conn, product_id: str) -> dict:
    row = await repo.get(conn, product_id)
    assert row is not None
    return row


async def _refused(code: str, action) -> None:
    with pytest.raises(repo.StatusRefused) as caught:
        await action
    assert caught.value.code == code


def test_a_product_is_a_draft_until_published_and_states_why_it_left_service():
    assert DataProduct(id="p", domain_id="d", name="P").status == "draft"
    with pytest.raises(ValueError, match="status_reason"):
        DataProduct(id="p", domain_id="d", name="P", status="deprecated")
    with pytest.raises(ValueError, match="retire_on"):
        DataProduct(id="p", domain_id="d", name="P", status="live", retire_on=TODAY)
    with pytest.raises(ValueError, match="successor"):
        DataProduct(id="p", domain_id="d", name="P", status="live", successor="q")


@pytest.mark.asyncio
async def test_publish_takes_a_draft_live_and_only_a_draft(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.publish(conn, "c360_v2", by="dana")
        row = await _row(conn, "c360_v2")
        assert (row["status"], row["status_by"]) == ("live", "dana")
        assert row["status_at"] is not None
        await _refused("not_draft", repo.publish(conn, "c360", by=None))


@pytest.mark.asyncio
async def test_deprecate_keeps_a_live_product_in_service_until_its_date(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await _refused(
            "reason_required",
            repo.deprecate(conn, "c360", reason=" ", retire_on=None, by=None, today=TODAY),
        )
        await _refused(
            "retire_on_past",
            repo.deprecate(
                conn,
                "c360",
                reason="Replaced.",
                retire_on=TODAY - datetime.timedelta(days=1),
                by=None,
                today=TODAY,
            ),
        )
        await _refused(
            "not_live",
            repo.deprecate(conn, "c360_v2", reason="x", retire_on=None, by=None, today=TODAY),
        )
        await repo.deprecate(
            conn, "c360", reason="Replaced.", retire_on=TODAY, by=None, today=TODAY
        )
        row = await _row(conn, "c360")
        assert (row["status"], row["status_reason"], row["retire_on"]) == (
            "deprecated",
            "Replaced.",
            TODAY,
        )
        assert await _members(conn, "c360") == 3  # still in service: nothing moves


@pytest.mark.asyncio
async def test_retiring_moves_every_member_to_the_successor(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await _refused(
            "successor_required", repo.retire(conn, "c360", reason="Gone.", successor=None, by=None)
        )
        await _refused(
            "successor_is_self",
            repo.retire(conn, "c360", reason="Gone.", successor="c360", by=None),
        )
        await _refused(
            "successor_other_domain",
            repo.retire(conn, "c360", reason="Gone.", successor="ops_kpis", by=None),
        )
        await repo.retire(conn, "c360", reason="Gone.", successor="c360_v2", by="dana")
        row = await _row(conn, "c360")
        assert (row["status"], row["successor_id"], row["status_reason"]) == (
            "retired",
            "c360_v2",
            "Gone.",
        )
        assert (await _members(conn, "c360"), await _members(conn, "c360_v2")) == (0, 3)
        moved = (
            await conn.execute_core(
                select(tracked_functions.c.product_id).where(
                    tracked_functions.c.name == "refresh_customer"
                )
            )
        ).scalar_one()
        assert moved == "c360_v2"
        # a retired product succeeds nothing
        await repo.publish(conn, "c360_v2", by=None)
        await _refused(
            "successor_retired",
            repo.retire(conn, "c360_v2", reason="x", successor="c360", by=None),
        )


@pytest.mark.asyncio
async def test_a_product_with_no_members_retires_without_a_successor(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.retire(conn, "ops_kpis", reason="Unused.", successor=None, by=None)
        assert (await _row(conn, "ops_kpis"))["status"] == "retired"


@pytest.mark.asyncio
async def test_restore_undoes_a_deprecation_and_brings_a_retired_product_back_a_draft(
    control_plane,
):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.deprecate(conn, "c360", reason="x", retire_on=TODAY, by=None, today=TODAY)
        assert await repo.restore(conn, "c360", by=None) == "live"
        row = await _row(conn, "c360")
        assert (row["status_reason"], row["retire_on"]) == (None, None)
        await repo.retire(conn, "c360", reason="Gone.", successor="c360_v2", by=None)
        assert await repo.restore(conn, "c360", by=None) == "draft"
        row = await _row(conn, "c360")
        assert (row["status"], row["successor_id"]) == ("draft", None)
        assert await _members(conn, "c360") == 0  # the members stay with the successor
        await _refused("not_restorable", repo.restore(conn, "c360", by=None))


@pytest.mark.asyncio
async def test_the_scheduler_retires_what_is_due_and_leaves_a_product_with_members(control_plane):
    from provisa.scheduler.planned_retirement import retire_due_products

    async with _store(control_plane) as db:
        async with db.acquire() as conn:
            await repo.deprecate(conn, "c360", reason="x", retire_on=TODAY, by=None, today=TODAY)
            await repo.deprecate(
                conn, "ops_kpis", reason="y", retire_on=TODAY, by=None, today=TODAY
            )
        assert await retire_due_products(db, TODAY) == ["ops_kpis"]
        async with db.acquire() as conn:
            assert (await _row(conn, "c360"))["status"] == "deprecated"
            row = await _row(conn, "ops_kpis")
            assert (row["status"], row["status_reason"]) == ("retired", "y")
            found = await product_findings.retirement_blocked(conn, CTX)
            assert found["c360"][0]["params"] == {"retire_on": TODAY.isoformat()}


@pytest.mark.asyncio
async def test_findings_name_what_a_curator_should_look_at(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="deprecated",
                base_tag_id="deprecated",
                object_type="table",
                table_id=2,
                object_key="table:2",
                reason="old",
                expires_on="2026-12-01",
            )
        )
        found = await product_findings.findings(conn, CTX)
        kinds = {pid: [f["kind"] for f in items] for pid, items in found.items()}
        assert kinds["c360_v2"] == ["no_members"]
        assert kinds["c360"] == ["member_deprecated"]
        assert kinds["ops_kpis"] == ["no_members", "domain_steward_unset"]
        [member] = found["c360"]
        assert member["code"] == "data_product.finding.member_deprecated"
        assert member["params"] == {
            "table_id": 2,
            "table": "crm.public.orders",
            "expires_on": "2026-12-01",
        }
        # the ones a product's own fields raise, and the one a configured target does
        required = product_findings.Context(today=TODAY, support_contact_required=True)
        await conn.execute_core(
            data_products.update()
            .where(data_products.c.id == "c360_v2")
            .values(owner_role=None, purpose="")
        )
        kinds = [f["kind"] for f in (await product_findings.findings(conn, required))["c360_v2"]]
        assert kinds == ["no_members", "no_owner", "no_support_contact", "no_description"]


@pytest.mark.asyncio
async def test_a_retired_product_is_looked_at_only_for_its_succession(control_plane):
    async with _store(control_plane) as db, db.acquire() as conn:
        await repo.retire(conn, "c360", reason="Gone.", successor="c360_v2", by=None)
        await repo.publish(conn, "c360_v2", by=None)
        # c360_v2 now holds the members, so it needs a successor of its own: a third product
        await conn.execute_core(
            insert(data_products).values(
                id="c360_v3", domain_id="sales", name="V3", purpose="x", status="live"
            )
        )
        await repo.retire(conn, "c360_v2", reason="Gone too.", successor="c360_v3", by=None)
        found = await product_findings.findings(conn, CTX)
        assert found["c360"] == [
            {
                "kind": "successor_retired",
                "code": "data_product.finding.successor_retired",
                "message": "the product that took its members has been retired too",
                "params": {"successor": "c360_v2"},
            }
        ]
        assert "c360_v2" not in found  # its successor, c360_v3, is in service
