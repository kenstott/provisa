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

A member is given a draft contract at 0.1.0 promising its columns. A contract that stops promising
a column, or promises it as another type, is a new version: the same version is refused, naming
the contract. A member keeps every column its contract promises: a save that drops or retypes one
is refused, unless the contract's new version stops promising it in the same save. A table that
leaves its product, and a checker table, has none.
"""

# Requirements: REQ-2158

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.core import data_contracts
from provisa.core.data_contracts import ContractRefused
from provisa.core.database import Database
from provisa.core.models import (
    FIRST_CONTRACT_VERSION,
    Column,
    ContractColumn,
    DataContract,
    DataProduct,
    Table,
)
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources


def _contract(version: str, **columns: str) -> DataContract:
    return DataContract(
        version=version, columns=[ContractColumn(name=n, type=t) for n, t in columns.items()]
    )


# -- the rules ------------------------------------------------------------------------------------


def test_a_new_member_is_given_a_draft_promising_its_columns():
    contract = data_contracts.draft({"id": "INTEGER", "amount": "NUMERIC(10, 2)"})
    assert (contract.version, contract.status) == (FIRST_CONTRACT_VERSION, "draft")
    assert [(c.name, c.type) for c in contract.columns] == [
        ("id", "INTEGER"),
        ("amount", "NUMERIC(10, 2)"),
    ]


def test_a_contract_that_stops_promising_a_column_is_a_new_version():
    old = _contract("1.0.0", id="INTEGER", amount="INTEGER")
    with pytest.raises(ContractRefused, match=r"p/s\.t version 1\.0\.0 stops promising amount"):
        data_contracts.require_new_version("p/s.t", old, _contract("1.0.0", id="INTEGER"))
    with pytest.raises(ContractRefused, match=r"amount \(INTEGER -> TEXT\)"):
        data_contracts.require_new_version(
            "p/s.t", old, _contract("1.0.0", id="INTEGER", amount="TEXT")
        )
    data_contracts.require_new_version("p/s.t", old, _contract("2.0.0", id="INTEGER"))
    # Promising more is no break.
    data_contracts.require_new_version(
        "p/s.t", old, _contract("1.0.0", id="INTEGER", amount="INTEGER", note="TEXT")
    )


def test_a_member_provides_what_its_contract_promises():
    promised = _contract("1.0.0", id="INTEGER", amount="INTEGER")
    with pytest.raises(ContractRefused, match=r"promises amount \(INTEGER\)"):
        data_contracts.require_provided("p/s.t", promised, {"id": "INTEGER"})
    with pytest.raises(ContractRefused, match=r"amount \(INTEGER, now TEXT\)"):
        data_contracts.require_provided("p/s.t", promised, {"id": "INTEGER", "amount": "TEXT"})
    data_contracts.require_provided(
        "p/s.t", promised, {"id": "INTEGER", "amount": "INTEGER", "note": "TEXT"}
    )


def test_a_promised_type_is_spelled_as_a_column_type_is_stored():
    contract = data_contracts.as_stored("p/s.t", _contract("1.0.0", amount="numeric(10,2)"))
    assert contract.columns[0].type == data_contracts.stored_type_of("numeric(10,2)")


def _for_save(**kw):
    args = {
        "product_id": "p",
        "is_checker": False,
        "schema_name": "s",
        "table_name": "t",
        "declared": None,
        "stored": None,
        "stored_product_id": None,
        "column_types": {"id": "INTEGER"},
    }
    return data_contracts.for_save(**{**args, **kw})


def test_a_table_in_no_product_or_a_checker_has_no_contract():
    assert _for_save(product_id=None) is None
    assert _for_save(is_checker=True) is None


def test_a_save_keeps_the_stored_contract_and_a_new_product_starts_a_draft():
    stored = _contract("3.0.0", id="INTEGER").model_dump()
    assert _for_save(stored=stored, stored_product_id="p").version == "3.0.0"
    assert _for_save(stored=stored, stored_product_id="other").version == FIRST_CONTRACT_VERSION


# -- the table's save -----------------------------------------------------------------------------


def _table(*columns: str, product: str | None = "p", contract: DataContract | None = None):
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        product_id=product,
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


async def test_joining_a_product_gives_the_member_a_draft_of_its_columns(plane):
    _, contract = await _stored(plane)
    assert contract is not None
    assert contract.version == FIRST_CONTRACT_VERSION
    assert [c.name for c in contract.columns] == ["id", "amount"]


async def test_a_save_that_drops_a_promised_column_is_refused_and_changes_nothing(plane):
    with pytest.raises(ContractRefused, match=r"p/public\.orders version 0\.1\.0 promises amount"):
        async with plane.acquire() as conn:
            await table_repo.upsert(conn, _table("id"))
    _, contract = await _stored(plane)
    assert contract is not None and [c.name for c in contract.columns] == ["id", "amount"]


async def test_a_new_version_without_the_column_in_the_same_save_lets_it_go(plane):
    declared = _contract("1.0.0", id="INTEGER")
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id", contract=declared))
    _, contract = await _stored(plane)
    assert contract is not None and contract.version == "1.0.0"


async def test_the_same_version_without_the_column_is_refused(plane):
    with pytest.raises(ContractRefused, match="a change that breaks a contract is a new version"):
        async with plane.acquire() as conn:
            await table_repo.upsert(conn, _table("id", contract=_contract("0.1.0", id="INTEGER")))


async def test_leaving_the_product_takes_the_contract_away(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id", "amount", product=None))
    _, contract = await _stored(plane)
    assert contract is None


# -- the contract's own save ----------------------------------------------------------------------


async def test_a_contract_saved_at_a_new_version_may_stop_promising_a_column(plane):
    table_id, _ = await _stored(plane)
    async with plane.acquire() as conn:
        await data_product_repo.save_contract(conn, table_id, _contract("1.0.0", id="integer"))
    _, contract = await _stored(plane)
    assert contract is not None and (contract.version, len(contract.columns)) == ("1.0.0", 1)
    # The column, now promised by no contract, may go.
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id"))


async def test_a_contract_breaking_its_version_or_promising_what_is_not_there_is_refused(plane):
    table_id, _ = await _stored(plane)
    async with plane.acquire() as conn:
        with pytest.raises(ContractRefused, match="stops promising amount"):
            await data_product_repo.save_contract(conn, table_id, _contract("0.1.0", id="integer"))
        with pytest.raises(ContractRefused, match=r"promises note \(text\)"):
            await data_product_repo.save_contract(
                conn, table_id, _contract("0.2.0", id="integer", amount="integer", note="text")
            )


async def test_a_table_in_no_product_has_no_contract_to_save(plane):
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("id", "amount", product=None))
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
