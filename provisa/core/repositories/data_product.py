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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, func, insert, select, update

from provisa.core import model_change
from provisa.core.models import ContractCheck, DataContract, DataProduct
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.schema_org import (
    checker_targets,
    data_product_commands,
    data_product_tables,
    data_products,
    registered_tables,
    sources,
    table_columns,
    tracked_functions,
)

if TYPE_CHECKING:
    from datetime import date

    from provisa.core.database import Connection

log = logging.getLogger(__name__)


async def refs(conn: "Connection") -> tuple[tuple[str, str], ...]:  # REQ-2219
    """(id, domain id) of every data product: what an entitlement list can name."""
    result = await conn.execute_core(select(data_products.c.id, data_products.c.domain_id))
    return tuple((r.id, r.domain_id) for r in result.fetchall())


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
            "support": product.support,
            "support_contact": product.support_contact,
            "custom_properties": product.custom_properties,
            "created_by": product.created_by,  # REQ-2219: on insert; update_columns leaves it
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


async def owner_roles(conn: "Connection") -> set[str]:  # REQ-2188
    """The roles named in an Owner field: what the runtime build gives RW on data products."""
    result = await conn.execute_core(
        select(data_products.c.owner_role).where(data_products.c.owner_role.is_not(None)).distinct()
    )
    return {r[0] for r in result.fetchall()}


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


async def member_contracts(conn: "Connection", product_id: str | None = None) -> list[dict]:
    """REQ-2158: each product member's contract, with its table's id and address and the product it
    is a member of: one row per product and member, every product's, or ``product_id``'s."""
    query = (
        select(
            registered_tables.c.id,
            data_product_tables.c.product_id,
            registered_tables.c.source_id,
            registered_tables.c.schema_name,
            registered_tables.c.table_name,
            registered_tables.c.contract,
        )
        .join(data_product_tables, data_product_tables.c.table_id == registered_tables.c.id)
        .where(registered_tables.c.contract.is_not(None))
    )
    if product_id is not None:
        query = query.where(data_product_tables.c.product_id == product_id)
    result = await conn.execute_core(
        query.order_by(registered_tables.c.id, data_product_tables.c.product_id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def promised_checks(  # REQ-2158
    conn: "Connection", table_id: int
) -> list[ContractCheck]:
    """What the member's products promise of it: the checks of the checker tables that scan it
    (REQ-2157) and are members of a product it is a member of, each as a contract version keeps it.
    A check only another product owns, or none does, is no promise of these."""
    from provisa.dq.contract import check_severity

    its_products = select(data_product_tables.c.product_id).where(
        data_product_tables.c.table_id == table_id
    )
    owned = select(data_product_tables.c.table_id).where(
        data_product_tables.c.product_id.in_(its_products)
    )
    rows = (
        await conn.execute_core(
            select(registered_tables.c.dq_rules, sources.c.type)
            .join(sources, sources.c.id == registered_tables.c.source_id)
            .join(checker_targets, checker_targets.c.table_id == registered_tables.c.id)
            .where(
                checker_targets.c.checked_table_id == table_id,
                registered_tables.c.id.in_(owned),
                registered_tables.c.dq_rules.is_not(None),
            )
            .order_by(registered_tables.c.id)
        )
    ).fetchall()
    return [
        ContractCheck(
            name=rule["check_type"],
            column=rule["column_name"] or None,
            severity=check_severity(rule, row.type),
            definition=rule["definition"],
        )
        for row in rows
        for rule in row.dq_rules
    ]


async def save_contract(  # REQ-2158
    conn: "Connection", table_id: int, contract: DataContract
) -> dict:
    """A member's contract replaced, the version saved recorded on it: refused
    (data_contracts.ContractRefused) when the table is no product's output port. The member's row,
    as it is now."""
    from provisa.core import contract_versions, data_contracts
    from provisa.core.repositories.table import load_columns

    async with conn.transaction():
        row = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.id == table_id)
            )
        ).fetchone()
        products = await products_of_table(conn, table_id)
        if row is None or not products or row.contract is None:
            raise data_contracts.ContractRefused(
                f"table {table_id} is no data product's output port: it has no contract"
            )
        # REQ-2188: a contract belongs to its products: each one's U rule holds its save.
        await conn.require_admitted(update(data_products).where(data_products.c.id.in_(products)))
        cid = data_contracts.contract_id(products[0], row.schema_name, row.table_name)
        model_change.name("upsert", "data contract", cid)  # REQ-1524
        previous = DataContract.model_validate(row.contract)
        contract = data_contracts.created_as(previous, contract)
        contract = contract_versions.recorded(
            previous,
            contract,
            await promised_checks(conn, table_id),
            contract_versions.snapshot(await load_columns(conn, table_id)),
        )
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == table_id)
            .values(contract=contract.model_dump(mode="json"))
        )
    return {**dict(row._mapping), "contract": contract.model_dump(mode="json")}


# REQ-1634 STATUS: a data product's lifecycle. Every state is asserted by an action: publish takes
# a draft live; deprecate keeps a live product in service, with a planned retirement if given;
# retire moves its members to a successor; restore undoes a deprecation, or brings a retired
# product back a draft.

#: Who holds a product's members: the tables and views, and the commands, that belong to it.
_MEMBER_TABLES = (data_product_tables, data_product_commands)


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
    """How many tables, views and commands belong to the product."""
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
    # REQ-2188: one transaction: a retirement refused by a row rule moves no member.
    async with conn.transaction():
        await _retire(conn, product, text, successor, by)
    model_change.name("retire", "data product", product_id)


async def _retire(
    conn: "Connection", product: dict, text: str, successor: "str | None", by: "str | None"
) -> None:
    product_id = product["id"]
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
        await _move_members(conn, product_id, successor)
    await _set_status(
        conn,
        product_id,
        by,
        status="retired",
        status_reason=text,
        retire_on=None,
        successor_id=successor,
    )


async def _move_members(conn: "Connection", product_id: str, successor: str) -> None:
    """Every member of ``product_id`` becomes a member of ``successor`` instead; one already a
    member of the successor stays one, once."""
    tables = await member_table_ids(conn, product_id)
    commands = await member_command_names(conn, product_id)
    for member in _MEMBER_TABLES:
        await conn.execute_core(_delete(member).where(member.c.product_id == product_id))
    await _add_members(conn, successor, tables, commands)


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


# REQ-1634: membership is many-to-many, a relation of its own (data_product_tables,
# data_product_commands), and the product owns it: members are added to and removed from a product
# only through set_members, the one write, for every surface.


class MembershipRefused(ValueError):
    """A member set refused by name: ``code`` (data_product.members.<code>) and ``params``."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


async def member_table_ids(conn: "Connection", product_id: str) -> list[int]:
    """The ids of the product's member tables and views, in id order."""
    rows = await conn.execute_core(
        select(data_product_tables.c.table_id)
        .where(data_product_tables.c.product_id == product_id)
        .order_by(data_product_tables.c.table_id)
    )
    return [int(r.table_id) for r in rows.fetchall()]


async def member_command_names(conn: "Connection", product_id: str) -> list[str]:
    """The names of the product's member commands, in name order."""
    rows = await conn.execute_core(
        select(data_product_commands.c.command_name)
        .where(data_product_commands.c.product_id == product_id)
        .order_by(data_product_commands.c.command_name)
    )
    return [r.command_name for r in rows.fetchall()]


async def products_of_table(conn: "Connection", table_id: int) -> list[str]:
    """The ids of the products the table or view is a member of, in id order."""
    rows = await conn.execute_core(
        select(data_product_tables.c.product_id)
        .where(data_product_tables.c.table_id == table_id)
        .order_by(data_product_tables.c.product_id)
    )
    return [r.product_id for r in rows.fetchall()]


async def products_of_command(conn: "Connection", command_name: str) -> list[str]:
    """The ids of the products the command is a member of, in id order."""
    rows = await conn.execute_core(
        select(data_product_commands.c.product_id)
        .where(data_product_commands.c.command_name == command_name)
        .order_by(data_product_commands.c.product_id)
    )
    return [r.product_id for r in rows.fetchall()]


async def table_memberships(conn: "Connection") -> dict[int, list[str]]:
    """Every member table's products: table id -> the product ids, in id order."""
    rows = await conn.execute_core(
        select(data_product_tables.c.table_id, data_product_tables.c.product_id).order_by(
            data_product_tables.c.table_id, data_product_tables.c.product_id
        )
    )
    out: dict[int, list[str]] = {}
    for r in rows.fetchall():
        out.setdefault(int(r.table_id), []).append(r.product_id)
    return out


async def command_memberships(conn: "Connection") -> dict[str, list[str]]:
    """Every member command's products: command name -> the product ids, in id order."""
    rows = await conn.execute_core(
        select(data_product_commands.c.command_name, data_product_commands.c.product_id).order_by(
            data_product_commands.c.command_name, data_product_commands.c.product_id
        )
    )
    out: dict[str, list[str]] = {}
    for r in rows.fetchall():
        out.setdefault(r.command_name, []).append(r.product_id)
    return out


async def _require_member_tables(conn: "Connection", product: dict, table_ids: list[int]) -> None:
    """Each table exists, is in the product's domain, and can be a member: a parameterized table
    (a native-filter column) is a function f(args) -> rows that never lands, so it can back no
    product's export."""
    if not table_ids:
        return
    rows = {
        int(r.id): r
        for r in (
            await conn.execute_core(
                select(
                    registered_tables.c.id,
                    registered_tables.c.table_name,
                    registered_tables.c.domain_id,
                ).where(registered_tables.c.id.in_(table_ids))
            )
        ).fetchall()
    }
    parameterized = {
        int(r.table_id)
        for r in (
            await conn.execute_core(
                select(table_columns.c.table_id)
                .where(table_columns.c.table_id.in_(table_ids))
                .where(table_columns.c.native_filter_type.is_not(None))
            )
        ).fetchall()
    }
    for table_id in table_ids:
        row = rows.get(table_id)
        if row is None:
            raise MembershipRefused(
                "table_not_found", f"table {table_id} does not exist", table=table_id
            )
        if row.domain_id != product["domain_id"]:
            raise MembershipRefused(
                "other_domain",
                f"table {row.table_name} is in domain {row.domain_id!r} but data product "
                f"{product['id']!r} belongs to domain {product['domain_id']!r}",
                table=row.table_name,
                domain=row.domain_id,
                product_domain=product["domain_id"],
            )
        if table_id in parameterized:
            raise MembershipRefused(
                "parameterized",
                f"table {row.table_name} has a native-filter (parameterized) column and cannot "
                f"be a member of data product {product['id']!r} — it is never landed",
                table=row.table_name,
            )


async def _require_member_commands(conn: "Connection", product: dict, names: list[str]) -> None:
    """Each command exists and is in the product's domain."""
    if not names:
        return
    domains = {
        r.name: r.domain_id
        for r in (
            await conn.execute_core(
                select(tracked_functions.c.name, tracked_functions.c.domain_id).where(
                    tracked_functions.c.name.in_(names)
                )
            )
        ).fetchall()
    }
    for name in names:
        if name not in domains:
            raise MembershipRefused(
                "command_not_found", f"command {name!r} does not exist", command=name
            )
        if domains[name] != product["domain_id"]:
            raise MembershipRefused(
                "other_domain",
                f"command {name} is in domain {domains[name]!r} but data product "
                f"{product['id']!r} belongs to domain {product['domain_id']!r}",
                command=name,
                domain=domains[name],
                product_domain=product["domain_id"],
            )


async def _add_members(
    conn: "Connection", product_id: str, table_ids: Iterable[int], commands: Iterable[str]
) -> None:
    """``product_id`` gains each table and command it does not hold yet; a table joining its first
    product is given a draft contract (REQ-2158) when it has none."""
    held_tables = set(await member_table_ids(conn, product_id))
    held_commands = set(await member_command_names(conn, product_id))
    for table_id in table_ids:
        if table_id in held_tables:
            continue
        await conn.execute_core(
            insert(data_product_tables).values(product_id=product_id, table_id=table_id)
        )
        await _give_draft_contract(conn, table_id)
    for name in commands:
        if name not in held_commands:
            await conn.execute_core(
                insert(data_product_commands).values(product_id=product_id, command_name=name)
            )


async def _give_draft_contract(conn: "Connection", table_id: int) -> None:
    """REQ-2158: a member with no contract is given a draft, its first version snapshotting the
    member's columns as they are stored. A checker table (a data-quality contract's scan results)
    has none."""
    from provisa.core import contract_versions, data_contracts
    from provisa.core.repositories.table import load_columns

    row = (
        await conn.execute_core(
            select(registered_tables.c.contract, registered_tables.c.dq_contract).where(
                registered_tables.c.id == table_id
            )
        )
    ).fetchone()
    if row is None or row.contract is not None or row.dq_contract is not None:
        return
    contract = contract_versions.recorded(
        None,
        data_contracts.draft(),
        await promised_checks(conn, table_id),
        contract_versions.snapshot(await load_columns(conn, table_id)),
    )
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(contract=contract.model_dump(mode="json"))
    )


async def _drop_orphan_contracts(conn: "Connection", table_ids: Iterable[int]) -> None:
    """REQ-2158: a table that is no product's member any more has no contract: it is no output
    port."""
    still = select(data_product_tables.c.table_id)
    gone = list(table_ids)
    if gone:
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id.in_(gone))
            .where(registered_tables.c.id.not_in(still))
            .values(contract=None)
        )


async def set_members(  # REQ-1634
    conn: "Connection",
    product_id: str,
    *,
    table_ids: Iterable[int],
    commands: Iterable[str],
) -> None:
    """The product's members become exactly ``table_ids`` and ``commands``: THE membership write.
    Refused (:class:`MembershipRefused`) when the product or a member does not exist, when a member
    is in another domain, or when a table is parameterized; nothing changes then. A table may be a
    member of any number of products; one leaving its last product loses its contract. One
    transaction."""
    wanted_tables = sorted(set(table_ids))
    wanted_commands = sorted(set(commands))
    model_change.name("upsert", "data product members", product_id)  # REQ-1524
    async with conn.transaction():
        product = await get(conn, product_id)
        if product is None:
            raise MembershipRefused(
                "not_found", f"data product {product_id!r} not found", data_product=product_id
            )
        # REQ-2188: a product's membership is the product's: its U rule holds the write.
        await conn.require_admitted(update(data_products).where(data_products.c.id == product_id))
        await _require_member_tables(conn, product, wanted_tables)
        await _require_member_commands(conn, product, wanted_commands)
        held = await member_table_ids(conn, product_id)
        dropped = [t for t in held if t not in set(wanted_tables)]
        await conn.execute_core(
            _delete(data_product_tables)
            .where(data_product_tables.c.product_id == product_id)
            .where(data_product_tables.c.table_id.not_in(wanted_tables))
        )
        await conn.execute_core(
            _delete(data_product_commands)
            .where(data_product_commands.c.product_id == product_id)
            .where(data_product_commands.c.command_name.not_in(wanted_commands))
        )
        await _add_members(conn, product_id, wanted_tables, wanted_commands)
        await _drop_orphan_contracts(conn, dropped)


async def require_domain_of_products(  # REQ-1634
    conn: "Connection", products: list[str], domain_id: str, member: str
) -> None:
    """A member saved in ``domain_id`` keeps to the domain of every product it belongs to: one moved
    to another domain is refused by name while it is a member of a product there is not, as one
    joining a product of another domain is (set_members). ``member`` names it ("table orders")."""
    for product_id in products:
        product = await get(conn, product_id)
        if product is not None and product["domain_id"] != domain_id:
            raise MembershipRefused(
                "other_domain",
                f"{member} is in domain {domain_id!r} but data product {product_id!r} belongs to "
                f"domain {product['domain_id']!r}; take it out of the product first",
                member=member,
                domain=domain_id,
                product_domain=product["domain_id"],
            )


async def require_members_in_domain(  # REQ-1634
    conn: "Connection", product_id: str, domain_id: str
) -> None:
    """A product moved to ``domain_id`` holds no member of another domain: the move is refused by
    name while it does, as a member moved out of its products' domain is
    (require_domain_of_products), since a member joins only products of its own domain."""
    tables = (
        await conn.execute_core(
            select(registered_tables.c.table_name)
            .join(data_product_tables, data_product_tables.c.table_id == registered_tables.c.id)
            .where(data_product_tables.c.product_id == product_id)
            .where(registered_tables.c.domain_id != domain_id)
            .order_by(registered_tables.c.table_name)
        )
    ).fetchall()
    commands = (
        await conn.execute_core(
            select(tracked_functions.c.name)
            .join(
                data_product_commands,
                data_product_commands.c.command_name == tracked_functions.c.name,
            )
            .where(data_product_commands.c.product_id == product_id)
            .where(tracked_functions.c.domain_id != domain_id)
            .order_by(tracked_functions.c.name)
        )
    ).fetchall()
    members = [*(r.table_name for r in tables), *(r.name for r in commands)]
    if members:
        raise MembershipRefused(
            "domain_has_members",
            f"data product {product_id!r} cannot move to domain {domain_id!r} while it has members "
            f"of another domain ({', '.join(members)}); take them out of the product first",
            data_product=product_id,
            domain=domain_id,
            members=", ".join(members),
        )
