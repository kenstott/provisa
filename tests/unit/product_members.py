# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1f4a27-5e93-4b0d-a6c2-7d1e9b3f5a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634: a data product's members, declared on the product, for a test building a config."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from provisa.core.models import DataProduct, ProvisaConfig, Table


def member_name(table: Table) -> str:
    """How a product names a member table where a bare name might not be unique."""
    return f"{table.source_id}.{table.schema_name}.{table.table_name}"


def with_members(
    declared: Iterable[DataProduct], members: Mapping[str, Iterable[Table]]
) -> list[DataProduct]:
    """``declared`` with each product listing its ``members`` (by source.schema.table). A product
    ``members`` names that is not declared is declared, by its id, in its first member's domain;
    its members in any other domain go to a product of that domain, ``<id>-<domain>``, since a
    product's members share its domain (REQ-1634)."""
    out = {p.id: p for p in declared}
    for product_id, tables in members.items():
        tables = list(tables)
        if not tables:
            continue
        if product_id in out:
            names = [*out[product_id].tables, *(member_name(t) for t in tables)]
            out[product_id] = out[product_id].model_copy(update={"tables": names})
            continue
        by_domain: dict[str, list[str]] = {}
        for t in tables:
            by_domain.setdefault(t.domain_id, []).append(member_name(t))
        for i, (domain, names) in enumerate(by_domain.items()):
            pid = product_id if i == 0 else f"{product_id}-{domain}"
            out[pid] = DataProduct(id=pid, domain_id=domain, name=pid, tables=names)
    return list(out.values())


def all_members_of(config: ProvisaConfig, product_id: str) -> ProvisaConfig:
    """``config`` with every one of its tables a member of ``product_id`` (declared when it is not),
    so that each publishes: only a data product's members enter a catalog snapshot."""
    return config.model_copy(
        update={"data_products": with_members(config.data_products, {product_id: config.tables})}
    )


#: The product each table a test built was assigned to (``assigned``), by the table object.
_ASSIGNED: dict[int, tuple[Table, str | None]] = {}


def assigned(table: Table, product_id: str | None) -> Table:
    """``table``, recorded as a member of ``product_id`` (None: of no product), for
    :func:`assigned_members` to declare on the product when the test builds its config."""
    _ASSIGNED[id(table)] = (table, product_id)
    return table


def assigned_members(tables: Iterable[Table]) -> dict[str, list[Table]]:
    """Each product ``tables`` were assigned to (``assigned``), with its member tables."""
    out: dict[str, list[Table]] = {}
    for table in tables:
        held = _ASSIGNED.get(id(table))
        if held is not None and held[0] is table and held[1] is not None:
            out.setdefault(held[1], []).append(table)
    return out
