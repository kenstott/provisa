# Copyright (c) 2026 Kenneth Stott
# Canary: 5a93ce17-2d06-48b4-b1c9-70e3f4a26d88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-org storage metering and the tier ceiling it enforces (REQ-1046/1047/1049).

The storage analog of the REQ-1044 query-cost caps, and it exists for the same reason: the bytes
an org materializes sit on the operator's disk indefinitely, and nothing about federation bounds
them. A query cap bounds what one statement costs; only this bounds what accumulates.

WHAT IS METERED — the relational materialization store, which is where every durable per-org byte
the platform owns lands: the org's registry schema ``org_<id>``, its MV outputs
``org_<id>_mv_cache``, and the API/GraphQL result caches ``org_<id>_api_cache`` /
``org_<id>_gql_cache``. Redirect result objects are deliberately NOT metered: they are written
under a per-query prefix and deleted on a TTL by ``schedule_s3_cleanup``, so they are a rate, not
an accumulation, and the egress meter (REQ-1452) already prices them.

WHAT IS NOT METERED — an org on BYO storage (REQ-1048). Its bytes are on its own bucket and its
own bill, so it has no platform footprint to measure and no ceiling to breach. See
:mod:`provisa.storage.byo`.

ENFORCEMENT is a rejection (REQ-1047), never a truncation or an eviction. The same reasoning as
REQ-1044: a silently capped MV makes the boundary invisible to the customer who just hit it, and
an eviction destroys data the org believes it has. The error names both exits — a larger SKU or
BYO storage.

The ceiling itself comes from the commercial seam, so a self-hosted deployment resolves none and
this module is inert there: the operator owns the disk and there is nobody to bill.
"""

# Requirements: REQ-1044, REQ-1046, REQ-1047, REQ-1048, REQ-1049

from __future__ import annotations

import logging
from typing import Any

from provisa.api.errors import ApiError

log = logging.getLogger(__name__)

# The store schemas one org's platform-owned bytes live in. Enumerated rather than matched with a
# LIKE 'org_<id>%' pattern: an org id is user-chosen text, so a prefix match would bill "acme" for
# every byte belonging to "acme_eu".
_ORG_SCHEMA_SUFFIXES = ("", "_mv_cache", "_api_cache", "_gql_cache")


def org_store_schemas(org_id: str) -> list[str]:
    """The materialization-store schemas whose bytes are attributable to ``org_id``."""
    return [f"org_{org_id}{suffix}" for suffix in _ORG_SCHEMA_SUFFIXES]


# Postgres is the only relational store flagged materialized_store today (EngineDefinition.
# materialize_stores), so the size probe is written against it. A store engine added to that set
# later needs its own probe here rather than a guess: reporting zero for a store this cannot
# measure would silently disable the ceiling on it.
_SIZE_SQL = """
SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0) AS bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(:schemas)
  AND c.relkind IN ('r', 'm', 'p')
"""


async def org_storage_bytes(store_dsn: str, org_id: str) -> int:
    """Bytes ``org_id`` occupies in the platform's materialization store (REQ-1049).

    ``pg_total_relation_size`` is the on-disk figure — heap, indexes, TOAST — which is what the
    operator is billed for, not the logical row size. Measured on demand against the live store
    rather than accumulated in a counter, because a counter drifts from the disk the moment a
    table is dropped, vacuumed or rewritten, and the number this returns has to be the number the
    cloud bill reflects.
    """
    from sqlalchemy import text

    from provisa.federation.store_writer import store_connection

    async with store_connection(store_dsn) as conn:
        result = await conn.execute_core(
            text(_SIZE_SQL).bindparams(schemas=org_store_schemas(org_id))
        )
        row = result.fetchone()
    # A store with no schema for this org yet returns a single 0 row, never no row: the aggregate
    # is unconditional. A missing row would mean the probe did not run, so it is an error.
    if row is None:
        raise RuntimeError(
            f"storage probe returned no row for org {org_id!r} — the store did not run the query"
        )
    return int(row[0])


async def storage_ceiling(org_id: str) -> tuple[int, str] | None:
    """``(max_bytes, plan)`` in force for ``org_id``, or None when no ceiling applies (REQ-1046).

    None in three distinct cases, all of them correct: a self-hosted deployment (no commercial
    plugin — the operator owns the disk), an org whose plan sets no storage ceiling, and an org on
    BYO storage, whose bytes are not the platform's to cap (REQ-1048).
    """
    from provisa.api.app import state
    from provisa.core.commerce import storage_cap_for_org
    from provisa.storage.byo import org_has_byo_store

    if org_has_byo_store(org_id):
        return None
    return await storage_cap_for_org(state, org_id)


async def require_storage_headroom(org_id: str, *, operation: str) -> None:
    """Reject ``operation`` for ``org_id`` when its store footprint is already at the ceiling.

    Checked BEFORE the write, on the current footprint, rather than after it or against a
    prediction of the write's size: the size of a materialization is not knowable before it runs
    (that is what makes it the exposure), and a check that admits the write and measures afterwards
    has already put the bytes on the disk. The consequence is that one operation may cross the
    ceiling by its own size before the next is refused — the ceiling bounds accumulation, which is
    the unbounded quantity, not the size of any single write.

    ``operation`` names the rejected action in the error ("MV refresh", "source landing") so the
    customer knows what to shed or where to move it.
    """
    resolved = await storage_ceiling(org_id)
    if resolved is None:
        return
    ceiling, plan = resolved

    from provisa.api.app import state
    from provisa.core.commerce import meter_storage

    used = await org_storage_bytes(state.federation_engine.materialize_store_dsn(), org_id)
    # REQ-1049: the footprint was just measured off the live store, so the meter is written here
    # rather than by a sweep of its own — every write seam that checks the ceiling also records the
    # level, which is what makes the quota sizable and priceable before anything bills on it.
    await meter_storage(state.admin_db, org_id, used)
    if used < ceiling:
        return

    raise ApiError(
        507,
        "storage.quota_exceeded",
        f"{operation} refused: org {org_id!r} is using {_gb(used)} of the {_gb(ceiling)} "
        f"storage included with the {plan} plan. Free space by dropping materialized views or "
        f"landed tables, move to a plan with a larger storage allowance, or point this org at "
        f"its own S3-compatible bucket (bring-your-own storage), whose bytes are not metered here.",
        org=org_id,
        plan=plan,
        used_bytes=used,
        ceiling_bytes=ceiling,
        operation=operation,
    )


def _gb(n_bytes: int) -> str:
    return f"{n_bytes / 1024**3:.2f} GB"


async def storage_report(org_id: str) -> dict[str, Any]:
    """``org_id``'s footprint and ceiling, for the admin surface and upgrade prompts (REQ-1049)."""
    from provisa.api.app import state
    from provisa.storage.byo import org_has_byo_store

    if org_has_byo_store(org_id):
        # A BYO org's bytes are in a bucket the platform holds no credentials to enumerate on its
        # own account, and by REQ-1048 they are none of its business. Reported as such, not as 0.
        return {"org_id": org_id, "byo": True, "used_bytes": None, "ceiling_bytes": None}

    used = await org_storage_bytes(state.federation_engine.materialize_store_dsn(), org_id)
    resolved = await storage_ceiling(org_id)
    return {
        "org_id": org_id,
        "byo": False,
        "used_bytes": used,
        "ceiling_bytes": None if resolved is None else resolved[0],
        "plan": None if resolved is None else resolved[1],
    }
