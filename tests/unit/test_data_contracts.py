# Copyright (c) 2026 Kenneth Stott
# Canary: 104080d0-a391-4fac-a630-446f858584e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product member's contract, its output port, declared and versioned on its own (REQ-2158).

A member is given a draft contract at 0.1.0. A contract's schema is its member table's columns: it
holds no column selection of its own (no promised columns), so a member's columns changing refuses
nothing and re-versions nothing. A table that leaves its product, and a checker table, has none.
"""

# Requirements: REQ-2158

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

from provisa.core import data_contracts
from provisa.core.data_contracts import ContractRefused
from provisa.core.database import Database
from provisa.core.models import (
    FIRST_CONTRACT_VERSION,
    Column,
    DataContract,
    DataProduct,
    Table,
)
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources


def _contract(version: str) -> DataContract:
    return DataContract(version=version)


# -- the rules ------------------------------------------------------------------------------------


def test_a_new_member_is_given_a_draft_selecting_no_columns():
    contract = data_contracts.draft()
    assert (contract.version, contract.status) == (FIRST_CONTRACT_VERSION, "draft")
    assert "columns" not in contract.model_dump()


def test_a_contract_holds_no_column_selection_of_its_own():
    with pytest.raises(ValidationError, match="columns"):
        DataContract.model_validate(
            {"version": "1.0.0", "columns": [{"name": "id", "type": "INTEGER"}]}
        )


def test_the_api_carries_no_columns_on_a_contract_or_its_input():
    import dataclasses

    from provisa.api.admin.types import DataContractInput, DataContractType

    for kind in (DataContractType, DataContractInput):
        assert "columns" not in {f.name for f in dataclasses.fields(kind)}
    from provisa.core import models

    assert not hasattr(models, "ContractColumn")


def test_no_rule_holds_a_member_to_columns_its_contract_selected():
    # The checks are the promise (REQ-2157); there is no column promise to keep or to break.
    assert not hasattr(data_contracts, "require_new_version")
    assert not hasattr(data_contracts, "require_provided")


def _for_save(**kw):
    args = {
        "columns": [],
        "products": ["p"],
        "is_checker": False,
        "declared": None,
        "stored": None,
    }
    return data_contracts.for_save(**{**args, **kw})


def test_a_table_in_no_product_or_a_checker_has_no_contract():
    assert _for_save(products=[]) is None
    assert _for_save(is_checker=True) is None
    # A contract declared before the table joins a product is kept for it.
    assert _for_save(products=[], declared=_contract("1.0.0")).version == "1.0.0"


def test_a_save_keeps_the_one_stored_contract_whichever_products_the_table_is_in():
    # REQ-1634: a table in several products has one contract, which each of them lists.
    stored = _contract("3.0.0").model_dump()
    assert _for_save(stored=stored).version == "3.0.0"
    assert _for_save(stored=stored, products=["other", "p"]).version == "3.0.0"
    assert _for_save().version == FIRST_CONTRACT_VERSION


def test_a_contract_keeps_when_it_was_created_through_every_version():
    """REQ-2118: contractCreatedTs is when the contract was first created, never a later save."""
    first = _for_save()
    assert first.created_at is not None
    stored = first.model_dump(mode="json")
    released = _for_save(stored=stored, declared=_contract("2.0.0"))
    assert (released.version, released.created_at) == ("2.0.0", first.created_at)
    # A contract declared for a new member is created by that save.
    assert _for_save(declared=_contract("1.0.0")).created_at is not None


# -- the table's save -----------------------------------------------------------------------------


def _table(*columns: str, contract: DataContract | None = None):
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        contract=contract,
        columns=[Column(name=c, visible_to=["analyst"], data_type="integer") for c in columns],
    )


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await data_product_repo.upsert(conn, DataProduct(id="p", domain_id="sales", name="P"))
        await table_repo.upsert(conn, _table("id", "amount"))
        # REQ-1634: the table joins the product on the product.
        table_id = (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == "orders")
            )
        ).scalar_one()
        await data_product_repo.set_members(conn, "p", table_ids=[table_id], commands=[])
    return db


async def _stored(db: Database) -> tuple[int, DataContract | None]:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(registered_tables.c.id, registered_tables.c.contract).where(
                    registered_tables.c.table_name == "orders"
                )
            )
        ).one()
    return row.id, None if row.contract is None else DataContract.model_validate(row.contract)


async def test_joining_a_product_gives_the_member_a_draft(plane):
    _, contract = await _stored(plane)
    assert contract is not None
    assert contract.version == FIRST_CONTRACT_VERSION


async def test_a_save_that_drops_a_column_keeps_the_contract_as_it_is(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id"))
    _, contract = await _stored(plane)
    assert contract is not None and contract.version == FIRST_CONTRACT_VERSION


async def test_a_new_version_declared_in_the_same_save_is_stored(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id", contract=_contract("1.0.0")))
    _, contract = await _stored(plane)
    assert contract is not None and contract.version == "1.0.0"


async def test_the_same_version_declared_with_a_column_dropped_is_stored(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id", contract=_contract("0.1.0")))
    _, contract = await _stored(plane)
    assert contract is not None and contract.version == "0.1.0"


async def test_leaving_the_product_takes_the_contract_away(plane):
    async with plane.acquire() as conn:
        await data_product_repo.set_members(conn, "p", table_ids=[], commands=[])
    _, contract = await _stored(plane)
    assert contract is None


# -- the contract's own save ----------------------------------------------------------------------


async def test_a_contract_saved_at_a_new_version_is_stored(plane):
    table_id, _ = await _stored(plane)
    async with plane.acquire() as conn:
        await data_product_repo.save_contract(conn, table_id, _contract("1.0.0"))
    _, contract = await _stored(plane)
    assert contract is not None and contract.version == "1.0.0"
    # A column no contract selects may go.
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id"))


async def test_a_contract_saved_at_its_own_version_is_written_in_place(plane):
    table_id, _ = await _stored(plane)
    async with plane.acquire() as conn:
        await data_product_repo.save_contract(
            conn, table_id, DataContract(version="0.1.0", description="edited")
        )
    _, contract = await _stored(plane)
    assert contract is not None and (contract.version, contract.description) == (
        "0.1.0",
        "edited",
    )


async def test_a_table_in_no_product_has_no_contract_to_save(plane):
    async with plane.acquire() as conn:
        await data_product_repo.set_members(conn, "p", table_ids=[], commands=[])
    table_id, _ = await _stored(plane)
    async with plane.acquire() as conn:
        with pytest.raises(ContractRefused, match="is no data product's output port"):
            await data_product_repo.save_contract(conn, table_id, _contract("1.0.0"))


async def test_the_products_contracts_are_listed_with_their_members(plane):
    table_id, _ = await _stored(plane)
    async with plane.acquire() as conn:
        (member,) = await data_product_repo.member_contracts(conn, "p")
    assert (member["id"], member["schema_name"], member["table_name"]) == (
        table_id,
        "public",
        "orders",
    )
