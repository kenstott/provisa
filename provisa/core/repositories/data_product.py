# Copyright (c) 2026 Kenneth Stott
# Canary: 4fceec2b-2f69-43db-b7a6-27ff29e7a9cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DataProduct repository — CRUD, domain-scoped, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-1634

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, func, select, update

from provisa.core import model_change
from provisa.core.models import DataProduct
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.schema_org import data_products, registered_tables, tracked_functions

if TYPE_CHECKING:
    from datetime import date

    from provisa.core.database import Connection

log = logging.getLogger(__name__)


async def upsert(  # REQ-1634, REQ-1919
    conn: "Connection", product: DataProduct
) -> None:
    """Create the data product, or replace its definition."""
    model_change.name("upsert", "data product", product.id)  # REQ-1524
    await conn.upsert(
        data_products,
        {
            "id": product.id,
            "domain_id": product.domain_id,
            "name": product.name,
            "owner_role": product.owner_role,
            "team_role": product.team_role,
            "purpose": product.purpose,
            "limitations": product.limitations,
            "usage": product.usage,
            "version": product.version,
            "status": product.status,
            "status_reason": product.status_reason,
            "retire_on": product.retire_on,
            "successor_id": product.successor,
            "sla": product.sla,
            "support": product.support,
            "support_contact": product.support_contact,
            "custom_properties": product.custom_properties,
        },
        index_elements=["id"],
        update_columns=[
            "domain_id",
            "name",
            "owner_role",
            "team_role",
            "purpose",
            "limitations",
            "usage",
            "version",
            "status",
            "status_reason",
            "retire_on",
            "successor_id",
            "sla",
            "support",
            "support_contact",
            "custom_properties",
        ],
    )


async def get(conn: "Connection", product_id: str) -> dict | None:  # REQ-1634
    result = await conn.execute_core(select(data_products).where(data_products.c.id == product_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-1634
    result = await conn.execute_core(select(data_products).order_by(data_products.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def list_by_domain(conn: "Connection", domain_id: str) -> list[dict]:  # REQ-1634
    result = await conn.execute_core(
        select(data_products)
        .where(data_products.c.domain_id == domain_id)
        .order_by(data_products.c.id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


class DataProductDeleteRefused(Exception):
    """A data product that may not be deleted because it has members; ``dependents`` lists
    them."""

    def __init__(self, product_id: str, dependents: list[Dependent]) -> None:
        self.product_id = product_id
        self.dependents = dependents
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = f"Data product {product_id!r} still has members"
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"Data product {product_id!r} still has members: {named}")


async def delete(conn: "Connection", product_id: str) -> bool:  # REQ-1634, REQ-1918
    """Delete one data product: THE delete, for every surface. False when there is none.

    A data product is blocked by its members (REQ-1918): refused
    (:class:`DataProductDeleteRefused`), naming each, while a table or a command belongs to
    it — the operator takes them out of the product first. (PostgreSQL used to detach them
    silently; SQLite left them naming a product that was gone.) Its tag assignments go with it.
    One transaction."""
    model_change.name("delete", "data product", product_id)  # REQ-1524
    ref = ObjectRef("data_product", product_id)
    async with conn.transaction():
        if await get(conn, product_id) is None:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise DataProductDeleteRefused(product_id, blocking)
        await remove_parts(conn, ref)
        await conn.execute_core(_delete(data_products).where(data_products.c.id == product_id))
    return True


# REQ-1634 STATUS: a data product's lifecycle. Every state is asserted by an action: publish takes
# a draft live; deprecate keeps a live product in service, with a planned retirement if given;
# retire moves its members to a successor; restore undoes a deprecation, or brings a retired
# product back a draft.

#: Who holds a product's members: the tables and views, and the commands, that name it.
_MEMBER_TABLES = (registered_tables, tracked_functions)


class StatusRefused(ValueError):
    """A status change refused by name: ``code`` (data_product.status.<code>) and ``params``."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


def _reason(reason: "str | None") -> str:
    text = (reason or "").strip()
    if not text:
        raise StatusRefused("reason_required", "a reason is required")
    return text


async def _status(conn: "Connection", product_id: str) -> str:
    product = await get(conn, product_id)
    if product is None:
        raise StatusRefused(
            "not_found", f"data product {product_id!r} not found", data_product=product_id
        )
    return product["status"]


async def member_count(conn: "Connection", product_id: str) -> int:
    """How many tables, views and commands name the product."""
    total = 0
    for table in _MEMBER_TABLES:
        total += (
            await conn.execute_core(
                select(func.count()).select_from(table).where(table.c.product_id == product_id)
            )
        ).scalar_one()
    return total


async def _set_status(conn: "Connection", product_id: str, by: "str | None", **values: Any) -> None:
    await conn.execute_core(
        update(data_products)
        .where(data_products.c.id == product_id)
        .values(status_by=by, status_at=func.now(), **values)
    )


async def publish(conn: "Connection", product_id: str, *, by: "str | None") -> None:
    """A draft product goes live."""
    status = await _status(conn, product_id)
    if status != "draft":
        raise StatusRefused(
            "not_draft",
            f"only a draft data product is published; this one is {status}",
            status=status,
        )
    await _set_status(conn, product_id, by, status="live")
    model_change.name("publish", "data product", product_id)


async def deprecate(
    conn: "Connection",
    product_id: str,
    *,
    reason: "str | None",
    retire_on: "date | None",
    by: "str | None",
    today: "date",
) -> None:
    """A live product is deprecated: it stays in service until ``retire_on``, when the scheduler
    retires it."""
    text = _reason(reason)
    if retire_on is not None and retire_on < today:
        raise StatusRefused(
            "retire_on_past",
            f"the planned retirement date {retire_on.isoformat()} is in the past",
            retire_on=retire_on.isoformat(),
        )
    status = await _status(conn, product_id)
    if status != "live":
        raise StatusRefused(
            "not_live",
            f"only a live data product is deprecated; this one is {status}",
            status=status,
        )
    await _set_status(
        conn, product_id, by, status="deprecated", status_reason=text, retire_on=retire_on
    )
    model_change.name("deprecate", "data product", product_id)


async def retire(
    conn: "Connection",
    product_id: str,
    *,
    reason: "str | None",
    successor: "str | None",
    by: "str | None",
) -> None:
    """A live or deprecated product is retired. Its members -- tables, views and commands -- move
    to ``successor``, required when it has any, of the same domain, not itself and not retired.
    Its own tags stay with it."""
    text = _reason(reason)
    product = await get(conn, product_id)
    if product is None:
        raise StatusRefused(
            "not_found", f"data product {product_id!r} not found", data_product=product_id
        )
    if product["status"] not in ("live", "deprecated"):
        raise StatusRefused(
            "not_live",
            f"only a live or deprecated data product is retired; this one is {product['status']}",
            status=product["status"],
        )
    members = await member_count(conn, product_id)
    if successor is None:
        if members:
            raise StatusRefused(
                "successor_required",
                f"the data product has {members} member(s); retiring it moves them to a "
                "successor, which is required",
                members=members,
            )
    else:
        await _check_successor(conn, product, successor)
        for table in _MEMBER_TABLES:
            await conn.execute_core(
                update(table).where(table.c.product_id == product_id).values(product_id=successor)
            )
    await _set_status(
        conn,
        product_id,
        by,
        status="retired",
        status_reason=text,
        retire_on=None,
        successor_id=successor,
    )
    model_change.name("retire", "data product", product_id)


async def _check_successor(conn: "Connection", product: dict, successor: str) -> None:
    if successor == product["id"]:
        raise StatusRefused("successor_is_self", "a data product cannot succeed itself")
    taker = await get(conn, successor)
    if taker is None:
        raise StatusRefused(
            "not_found", f"data product {successor!r} not found", data_product=successor
        )
    if taker["status"] == "retired":
        raise StatusRefused(
            "successor_retired",
            f"the successor {successor!r} is retired",
            successor=successor,
        )
    if taker["domain_id"] != product["domain_id"]:
        # REQ-1634: a product's members share its domain.
        raise StatusRefused(
            "successor_other_domain",
            f"the successor {successor!r} is in domain {taker['domain_id']!r}, not "
            f"{product['domain_id']!r}; a product's members share its domain",
            successor=successor,
            domain=taker["domain_id"],
        )


async def restore(conn: "Connection", product_id: str, *, by: "str | None") -> str:
    """Undo a deprecation (back to live) or a retirement (back to a draft; the members that moved
    stay with the successor). Returns the status it comes back to."""
    status = await _status(conn, product_id)
    if status not in ("deprecated", "retired"):
        raise StatusRefused(
            "not_restorable",
            f"only a deprecated or retired data product is restored; this one is {status}",
            status=status,
        )
    back = "live" if status == "deprecated" else "draft"
    await _set_status(
        conn,
        product_id,
        by,
        status=back,
        status_reason=None,
        retire_on=None,
        successor_id=None,
    )
    model_change.name("restore", "data product", product_id)
    return back


async def retire_due(conn: "Connection", today: "date") -> list[str]:
    """Retire every deprecated product whose planned retirement date has come, keeping the
    deprecation's reason. One that still has members needs a successor, which a date cannot
    name: it stays deprecated, a retirement_blocked finding. Returns the ids retired."""
    due = (
        await conn.execute_core(
            select(data_products.c.id)
            .where(data_products.c.status == "deprecated")
            .where(data_products.c.retire_on <= today)
        )
    ).fetchall()
    retired = []
    for r in due:
        if await member_count(conn, r.id):
            log.warning(
                "data product %r is due for retirement but still has members; it stays "
                "deprecated, a retirement_blocked finding",
                r.id,
            )
            continue
        await conn.execute_core(
            update(data_products)
            .where(data_products.c.id == r.id)
            .values(status="retired", retire_on=None, status_at=func.now())
        )
        retired.append(r.id)
    return retired
