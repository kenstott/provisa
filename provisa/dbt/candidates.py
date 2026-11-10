# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7e90c4-6b1d-4f38-8c52-e1d07a4b93f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dbt model offered as a data product, and a steward's acceptance of it (REQ-2228).

A model the manifest marks public with an enforced contract is a candidate. The offer is read from
the held manifest and holds nothing of its own: a steward decides by accepting, or by leaving it.

A contract belongs to a product's member table (REQ-2158), so a candidate is accepted once the
steward has registered its model; the product takes the table's domain. The contract's schema stays
the member's columns: what the manifest promises of them is written there, so each later version
snapshots it. dbt's tests are carried as a property of the product and not as the contract's checks,
which are read live from the checker tables that own them; their results are the dbt checker's.
"""

# Requirements: REQ-2228

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from sqlalchemy import select, update

from provisa.core import contract_versions, data_contracts
from provisa.core.models import DataProduct
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories.table import load_columns
from provisa.core.schema_org import registered_tables, roles, table_columns
from provisa.dbt.manifest import DbtManifest, DbtModel, DbtNode
from provisa.dbt.store import model_tables

#: The product's custom properties that say which model it came from and what dbt holds of it.
PROPERTY_MODEL = "dbt_model"
PROPERTY_OWNER = "dbt_owner"
PROPERTY_TESTS = "dbt_tests"


class CandidateRefused(ValueError):
    """An acceptance refused by name: ``code`` (dbt.candidate.<code>) and ``params``."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


def _suggested_id(model: DbtModel) -> str:
    return model.name if model.version is None else f"{model.name}_v{model.version}"


def _owner_role(model: DbtModel, role_ids: set[str]) -> str | None:
    """The role the manifest's owner names, when one does: a suggestion the steward confirms."""
    names = (model.group, None if model.owner is None else model.owner.name)
    return next((name for name in names if name in role_ids), None)


def _test_label(test: DbtNode) -> str:
    return f"{test.test_name}({test.column})" if test.column else str(test.test_name)


def _offer(
    manifest: DbtManifest, model: DbtModel, table: Any, products: list[str], role_ids: set[str]
) -> dict:
    return {
        "unique_id": model.unique_id,
        "name": model.name,
        "description": model.description,
        "version": model.version,
        "suggested_id": _suggested_id(model),
        "owner": None if model.owner is None else asdict(model.owner),
        "owner_role": _owner_role(model, role_ids),
        "columns": [
            {
                "name": c.name,
                "data_type": c.data_type,
                "description": c.description,
                "required": c.required,
            }
            for c in model.columns
        ],
        "tests": [
            {"name": t.test_name, "column": t.column} for t in manifest.tests_of(model.unique_id)
        ],
        "table_id": None if table is None else table.id,
        "products": products,
    }


async def _tables_of(conn: Any, manifest: DbtManifest) -> dict[str, Any]:
    """Each model's registered table, by the model's unique id."""
    return {model.unique_id: row for model, row in await model_tables(conn, manifest)}


async def candidates(conn: Any, manifest: DbtManifest) -> list[dict]:
    """What ``manifest`` offers as data products. Nothing is created by asking."""
    tables = await _tables_of(conn, manifest)
    role_ids = {r.id for r in (await conn.execute_core(select(roles.c.id))).fetchall()}
    out = []
    for model in manifest.models:
        if not model.is_candidate:
            continue
        table = tables.get(model.unique_id)
        products = (
            [] if table is None else await data_product_repo.products_of_table(conn, table.id)
        )
        out.append(_offer(manifest, model, table, products, role_ids))
    return out


async def candidate_table(conn: Any, manifest: DbtManifest, unique_id: str) -> tuple[DbtModel, Any]:
    """The candidate ``unique_id`` and the table it is registered as; refused when it is neither."""
    model = next((m for m in manifest.models if m.unique_id == unique_id), None)
    if model is None or not model.is_candidate:
        raise CandidateRefused(
            "not_offered",
            f"{unique_id} is not offered as a data product: the manifest does not mark it public "
            "with an enforced contract",
            model=unique_id,
        )
    table = (await _tables_of(conn, manifest)).get(unique_id)
    if table is None:
        raise CandidateRefused(
            "not_registered",
            f"Register {model.name} as a table before accepting it as a data product",
            model=model.name,
        )
    return model, table


def _product(
    manifest: DbtManifest, model: DbtModel, table: Any, product_id: str, owner_role: str | None
) -> DataProduct:
    tests = "; ".join(sorted(_test_label(t) for t in manifest.tests_of(model.unique_id)))
    properties = {PROPERTY_MODEL: model.unique_id}
    if model.owner is not None and model.owner.name:
        properties[PROPERTY_OWNER] = model.owner.name
    if tests:
        properties[PROPERTY_TESTS] = tests
    return DataProduct(
        id=product_id,
        domain_id=table.domain_id,
        name=model.name,
        owner_role=owner_role,
        purpose=model.description or "",
        version=model.version,
        support_contact=None if model.owner is None else model.owner.email,
        custom_properties=properties,
    )


async def _promise_columns(conn: Any, model: DbtModel, table_id: int) -> None:
    """The member's columns as the contract promises them; one the table lacks is refused."""
    held = {
        c["column_name"].casefold(): c["column_name"] for c in await load_columns(conn, table_id)
    }
    missing = sorted(c.name for c in model.columns if c.name.casefold() not in held)
    if missing:
        raise CandidateRefused(
            "columns_missing",
            f"The table does not hold the contract's columns: {', '.join(missing)}",
            columns=", ".join(missing),
        )
    required = [held[c.name.casefold()] for c in model.columns if c.required]
    if required:
        await conn.execute_core(
            update(table_columns)
            .where(table_columns.c.table_id == table_id, table_columns.c.column_name.in_(required))
            .values(not_null=True)
        )


async def _write_contract(conn: Any, model: DbtModel, table_id: int) -> None:
    """The member's contract: dbt's version where the model has one, its first version recorded."""
    declared = data_contracts.draft()
    contract = contract_versions.recorded(
        None,
        declared.model_copy(
            update={
                "version": model.version or declared.version,
                "description": model.description,
            }
        ),
        [],
        contract_versions.snapshot(await load_columns(conn, table_id)),
    )
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(contract=contract.model_dump(mode="json"))
    )


async def accept_candidate(
    conn: Any,
    manifest: DbtManifest,
    model: DbtModel,
    table: Any,
    *,
    product_id: str,
    owner_role: str | None,
) -> None:
    """A steward's yes: the product, its member, and the member's contract, in one transaction."""
    if await data_product_repo.get(conn, product_id) is not None:
        raise CandidateRefused(
            "id_taken", f"Data product {product_id!r} already exists", data_product=product_id
        )
    products = await data_product_repo.products_of_table(conn, table.id)
    if products:
        raise CandidateRefused(
            "already_a_product",
            f"{model.name} is already a member of data product {products[0]!r}",
            model=model.name,
            data_product=products[0],
        )
    async with conn.transaction():
        await _promise_columns(conn, model, table.id)
        await data_product_repo.upsert(
            conn, _product(manifest, model, table, product_id, owner_role)
        )
        await _write_contract(conn, model, table.id)
        await data_product_repo.set_members(conn, product_id, table_ids=[table.id], commands=[])
