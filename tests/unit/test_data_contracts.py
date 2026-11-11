# Copyright (c) 2026 Kenneth Stott
# Canary: 104080d0-a391-4fac-a630-446f858584e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's contracts are derived from its offers (REQ-2180, amended 2026-11-11,
CONTRACTS ARE DERIVED FROM THE OFFERS), superseding the member contract declared and versioned by
hand (REQ-2158).

No table holds a contract of its own and nothing saves one: a configuration declaring one on a
table is refused by name, and the admin API has no save for one. For every offer each dataset of
the product has the contract that is the offer's terms applied to it, derived again after every
build the process announces, so a contract follows its product's members, their columns, and the
masks and row rules its offer's role reads under: a change takes the contract's next version, a
column removed or masked being a breaking one; a build that changes none of it keeps the version.
"""

# Requirements: REQ-2180, REQ-2158

from __future__ import annotations

import dataclasses
import importlib
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

from provisa.api.offer_derivation import REVIEW_NOT_BUILT, rederive_all
from provisa.core.database import Database
from provisa.core.models import Column, DataProduct, Table
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, rls_rules, roles, sources
from tests.unit.offer_build import Built, installed

# -- nothing holds or saves a contract by hand ---------------------------------------------------


def test_no_table_holds_a_contract_of_its_own():
    assert "contract" not in registered_tables.c
    assert "contract" not in Table.model_fields


def test_a_configuration_declaring_a_tables_contract_is_refused_by_name():
    with pytest.raises(ValidationError, match="derived from its offers"):
        Table.model_validate(
            {
                "source_id": "pg",
                "domain_id": "sales",
                "table": "orders",
                "columns": [{"name": "id", "visible_to": ["analyst"]}],
                "contract": {"version": "1.0.0"},
            }
        )


def test_nothing_saves_a_contract_by_hand():
    assert not hasattr(data_product_repo, "save_contract")
    assert not hasattr(data_product_repo, "member_contracts")
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("provisa.core.data_contracts")
    from provisa.api.admin import types
    from provisa.api.admin.schema_mutation import Mutation

    assert not hasattr(Mutation, "save_data_contract")
    assert not hasattr(types, "DataContractInput")
    # A product's contracts are read per offer (the offers API), never as one fixed list.
    assert "contracts" not in {f.name for f in dataclasses.fields(types.DataProductType)}


# -- derived again from each build ---------------------------------------------------------------

_COLUMNS = {"orders": ["id", "amount", "email"], "customers": ["id", "name"]}


def _table(name: str, *columns: str) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[Column(name=c, visible_to=["viewer"], data_type="integer") for c in columns],
    )


@pytest.fixture
async def plane(control_plane) -> Database:
    """Product p in sales with its default offer (viewer), its member orders; customers is
    registered and no member yet."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        if (
            await conn.execute_core(select(roles.c.id).where(roles.c.id == "viewer"))
        ).fetchone() is None:
            await conn.execute_core(insert(roles).values(id="viewer", description="Reads."))
        await data_product_repo.upsert(conn, DataProduct(id="p", domain_id="sales", name="P"))
        for name, columns in _COLUMNS.items():
            await table_repo.upsert(conn, _table(name, *columns))
        await data_product_repo.set_members(
            conn, "p", table_ids=[await _id(conn, "orders")], commands=[]
        )
    return db


async def _id(conn, name: str) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


@pytest.fixture
def built(monkeypatch) -> Built:
    return installed(monkeypatch)


async def _build(db: Database, built: Built) -> dict[str, Any]:
    """What a build the process announces does to the contracts: each offer's derived again.
    The default offer's contracts afterwards, by table name."""
    async with db.acquire() as conn:
        await rederive_all(conn, await built.state(conn))
        (offer,) = await offers.list_offers(conn, "p")
        contracts = await offers.contracts(conn, offer["id"])
        names = await offers.table_names(conn, contracts)
    return {names[t]: c for t, c in contracts.items()}


def _fields(contract) -> list[tuple[str, str | None]]:
    return [(f.name, f.masked) for f in contract.terms.fields]


async def test_a_build_derives_the_default_offers_contract_on_each_member(plane, built):
    contracts = await _build(plane, built)
    assert list(contracts) == ["orders"]
    assert contracts["orders"].version == "1.0.0"
    assert _fields(contracts["orders"]) == [("id", None), ("amount", None), ("email", None)]


async def test_a_build_that_changes_nothing_keeps_the_version(plane, built):
    first = await _build(plane, built)
    again = await _build(plane, built)
    assert again["orders"] == first["orders"]


async def test_a_member_added_is_given_its_contract_and_one_taken_out_loses_it(plane, built):
    await _build(plane, built)
    async with plane.acquire() as conn:
        both = [await _id(conn, "orders"), await _id(conn, "customers")]
        await data_product_repo.set_members(conn, "p", table_ids=both, commands=[])
    contracts = await _build(plane, built)
    assert sorted(contracts) == ["customers", "orders"]
    assert contracts["customers"].version == "1.0.0"
    async with plane.acquire() as conn:
        await data_product_repo.set_members(
            conn, "p", table_ids=[await _id(conn, "customers")], commands=[]
        )
    assert list(await _build(plane, built)) == ["customers"]


async def test_a_column_removed_is_a_breaking_change_and_one_added_is_not(plane, built):
    await _build(plane, built)
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders", "id", "amount", "email", "placed_at"))
    added = (await _build(plane, built))["orders"]
    assert added.version == "1.1.0"
    assert [c.kind for c in added.versions[-1].changes] == ["field_added"]
    built.unread.add("email")  # the viewer no longer reads it
    removed = (await _build(plane, built))["orders"]
    assert removed.version == "2.0.0"
    assert [(c.kind, c.subject, c.breaking) for c in removed.versions[-1].changes] == [
        ("field_removed", "email", True)
    ]


async def test_a_column_masked_is_a_breaking_change(plane, built):
    await _build(plane, built)
    built.masked["email"] = "regex"
    contract = (await _build(plane, built))["orders"]
    assert contract.version == "2.0.0"
    assert ("email", "regex") in _fields(contract)
    assert [(c.kind, c.breaking) for c in contract.versions[-1].changes] == [("field_masked", True)]


async def test_a_row_rule_on_the_offers_role_changes_the_contract(plane, built):
    await _build(plane, built)
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(rls_rules).values(
                table_id=await _id(conn, "orders"),
                role_id="viewer",
                filter_expr=b"region = 'eu'",
                description="only the orders placed in the EU",
                rights="R",
            )
        )
    contract = (await _build(plane, built))["orders"]
    assert contract.version == "2.0.0"
    assert contract.terms.row_rules == ["only the orders placed in the EU"]


async def test_an_offer_the_build_cannot_derive_is_listed_for_review_and_keeps_its_contracts(
    plane, built
):
    first = await _build(plane, built)
    built.draft.add("orders")  # out of service: not in the build
    assert await _build(plane, built) == first
    async with plane.acquire() as conn:
        (offer,) = await offers.list_offers(conn, "p")
    assert offer["review"] == REVIEW_NOT_BUILT
    built.draft.clear()
    await _build(plane, built)
    async with plane.acquire() as conn:
        (offer,) = await offers.list_offers(conn, "p")
    assert offer["review"] is None
