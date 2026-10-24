# Copyright (c) 2026 Kenneth Stott
# Canary: 5403b40c-4973-4c63-bfac-8fffab80205a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a curator should look at in the data products (REQ-2146): findings, by product, each
``{kind, code, message, params}``, the same shape as the glossary's (glossary_findings). One
function per kind, in :data:`FINDERS`; a kind is added by adding one. A retired product is looked
at only for its succession: its members have moved, so the rest no longer applies to it."""

# Requirements: REQ-2146, REQ-1634

from __future__ import annotations

import datetime
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.repositories import data_product as data_product_repo
from provisa.core.schema_org import data_products, domains, registered_tables, tag_assignments

if TYPE_CHECKING:
    from provisa.core.database import Connection

Finding = dict[str, Any]


@dataclass(frozen=True)
class Context:
    """What a finder reads beside the model store: today, and whether a configured export
    target needs a support contact on every product it lists (Horizon's listing manifest)."""

    today: datetime.date
    support_contact_required: bool


#: A finder: the findings of its kind, by product id.
Finder = Callable[["Connection", Context], Awaitable[dict[str, list[Finding]]]]


def _finding(kind: str, message: str, **params: Any) -> Finding:
    """A finding; ``code`` names its message in the UI catalogs (serverErrors)."""
    return {
        "kind": kind,
        "code": f"data_product.finding.{kind}",
        "message": message,
        "params": params,
    }


async def _in_service(conn: "Connection") -> list[Any]:
    return (
        await conn.execute_core(select(data_products).where(data_products.c.status != "retired"))
    ).fetchall()


async def no_members(conn: "Connection", _ctx: Context) -> dict[str, list[Finding]]:
    """A product no table, view or command belongs to."""
    out: dict[str, list[Finding]] = {}
    for p in await _in_service(conn):
        if await data_product_repo.member_count(conn, p.id) == 0:
            out[p.id] = [_finding("no_members", "no table, view or command belongs to it")]
    return out


async def member_deprecated(conn: "Connection", _ctx: Context) -> dict[str, list[Finding]]:
    """A member table or view carrying the deprecated tag: the product serves something its
    owner plans to remove."""
    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.product_id,
                registered_tables.c.id,
                registered_tables.c.source_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                tag_assignments.c.expires_on,
            )
            .join(tag_assignments, tag_assignments.c.table_id == registered_tables.c.id)
            .join(data_products, data_products.c.id == registered_tables.c.product_id)
            .where(tag_assignments.c.object_type == "table")
            .where(tag_assignments.c.base_tag_id == "deprecated")
            .where(data_products.c.status != "retired")
            .order_by(registered_tables.c.id)
        )
    ).fetchall()
    out: dict[str, list[Finding]] = {}
    for r in rows:
        out.setdefault(r.product_id, []).append(
            _finding(
                "member_deprecated",
                "a member table is deprecated",
                table_id=r.id,
                table=f"{r.source_id}.{r.schema_name}.{r.table_name}",
                expires_on=r.expires_on,
            )
        )
    return out


async def no_owner(conn: "Connection", _ctx: Context) -> dict[str, list[Finding]]:
    """A product no role is accountable for."""
    return {
        p.id: [_finding("no_owner", "no owner role is accountable for it")]
        for p in await _in_service(conn)
        if not p.owner_role
    }


async def no_support_contact(conn: "Connection", ctx: Context) -> dict[str, list[Finding]]:
    """A product with no support contact while an export target lists products only with one
    (REQ-1635: Horizon's organization listing manifest)."""
    if not ctx.support_contact_required:
        return {}
    return {
        p.id: [
            _finding(
                "no_support_contact",
                "it has no support contact, which a configured export target requires",
            )
        ]
        for p in await _in_service(conn)
        if not p.support_contact
    }


async def retirement_blocked(conn: "Connection", ctx: Context) -> dict[str, list[Finding]]:
    """A deprecated product past its planned retirement that still has members, which need a
    successor no date can name."""
    rows = (
        await conn.execute_core(
            select(data_products.c.id, data_products.c.retire_on)
            .where(data_products.c.status == "deprecated")
            .where(data_products.c.retire_on <= ctx.today)
        )
    ).fetchall()
    return {
        r.id: [
            _finding(
                "retirement_blocked",
                "it was due for retirement but still has members; retire it with a successor "
                "to take them",
                retire_on=r.retire_on.isoformat(),
            )
        ]
        for r in rows
        if await data_product_repo.member_count(conn, r.id)
    }


async def domain_steward_unset(conn: "Connection", _ctx: Context) -> dict[str, list[Finding]]:
    """A product whose domain has no steward yet (REQ-609: pending)."""
    unset = {
        r.id
        for r in (
            await conn.execute_core(select(domains.c.id).where(domains.c.steward.is_(None)))
        ).fetchall()
    }
    return {
        p.id: [_finding("domain_steward_unset", "its domain has no steward", domain=p.domain_id)]
        for p in await _in_service(conn)
        if p.domain_id in unset
    }


async def no_description(conn: "Connection", _ctx: Context) -> dict[str, list[Finding]]:
    """A product with neither a purpose nor a usage: the description the exports build from
    them is empty."""
    return {
        p.id: [_finding("no_description", "it states neither its purpose nor its usage")]
        for p in await _in_service(conn)
        if not p.purpose.strip() and not p.usage.strip()
    }


async def successor_retired(conn: "Connection", _ctx: Context) -> dict[str, list[Finding]]:
    """A retired product whose successor has been retired too: its members are no longer served
    by the product that took them."""
    successor = data_products.alias("successor")
    rows = (
        await conn.execute_core(
            select(data_products.c.id, data_products.c.successor_id)
            .join(successor, successor.c.id == data_products.c.successor_id)
            .where(data_products.c.status == "retired")
            .where(successor.c.status == "retired")
        )
    ).fetchall()
    return {
        r.id: [
            _finding(
                "successor_retired",
                "the product that took its members has been retired too",
                successor=r.successor_id,
            )
        ]
        for r in rows
    }


FINDERS: dict[str, Finder] = {
    "no_members": no_members,
    "member_deprecated": member_deprecated,
    "no_owner": no_owner,
    "no_support_contact": no_support_contact,
    "retirement_blocked": retirement_blocked,
    "domain_steward_unset": domain_steward_unset,
    "no_description": no_description,
    "successor_retired": successor_retired,
}


async def findings(conn: "Connection", ctx: Context) -> dict[str, list[Finding]]:
    """Every finding, by product id, in the order of :data:`FINDERS`."""
    out: dict[str, list[Finding]] = {}
    for finder in FINDERS.values():
        for product_id, found in (await finder(conn, ctx)).items():
            out.setdefault(product_id, []).extend(found)
    return out


async def context(today: datetime.date) -> Context:
    """The finders' context for the current org: a support contact is required when its metadata
    export is on and lists products in Horizon."""
    from provisa.api.app import state
    from provisa.core.models import MetadataExportConfig
    from provisa.core.org_settings import resolve_org_config

    assert state.model_db is not None, "the org's model store is bound with its runtime"
    resolved = await resolve_org_config(state.model_db)
    config = MetadataExportConfig(**dict(resolved.get("metadata_export") or {}))
    required = config.enabled and any(t.provider == "snowflake_horizon" for t in config.targets)
    return Context(today=today, support_contact_required=required)
