# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7e90c4-6b1d-4f38-8c52-e1d07a4b93f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The data products a dbt project's public models make (REQ-2228).

A model the manifest marks public with an enforced contract is offered. Register Table registers
them all at once, after a preview: one draft product per dbt group, and one for the project when
it has no groups. The plan is read from the held manifest and holds nothing of its own; what
registers the tables and creates the products is the admin surface's one governed path.

A contract belongs to a product's member table (REQ-2158). The contract's schema stays the
member's columns: what the manifest promises of them is written there, so each later version
snapshots it. dbt's tests are carried as a property of the product and not as the contract's checks,
which are read live from the checker tables that own them; their results are the dbt checker's.
"""

# Requirements: REQ-2228

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import update

from provisa.core import contract_versions, data_contracts
from provisa.core.repositories.table import load_columns
from provisa.core.schema_org import registered_tables, table_columns
from provisa.dbt.manifest import DbtManifest, DbtModel, DbtNode, DbtOwner

#: The product's custom properties that say which models it came from and what dbt holds of them.
PROPERTY_MODELS = "dbt_models"
PROPERTY_OWNER = "dbt_owner"
PROPERTY_TESTS = "dbt_tests"


class ProductsRefused(ValueError):
    """A bulk registration refused by name: its ``code`` (dbt.products.<name>, or the code of the
    governed mutation that refused it) and ``params``."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


@dataclass(frozen=True)
class PlannedProduct:
    """A data product the project's public models make: a dbt group's, or the project's."""

    id: str
    #: The dbt group it is; None for the project's one product.
    group: str | None
    owner: DbtOwner | None
    models: tuple[DbtModel, ...]


def offered_models(manifest: DbtManifest) -> list[DbtModel]:
    """The models the manifest marks public with an enforced contract."""
    return [m for m in manifest.models if m.is_candidate]


def planned_products(manifest: DbtManifest) -> list[PlannedProduct]:
    """One product per dbt group the offered models are in; a project with no groups, one."""
    offered = offered_models(manifest)
    if not offered:
        return []
    if not manifest.groups:
        return [PlannedProduct(id=manifest.project, group=None, owner=None, models=tuple(offered))]
    return [
        PlannedProduct(
            id=group.name,
            group=group.name,
            owner=group.owner,
            models=tuple(m for m in offered if m.group == group.name),
        )
        for group in manifest.groups
        if any(m.group == group.name for m in offered)
    ]


def ungrouped(manifest: DbtManifest) -> list[DbtModel]:
    """Offered models no product takes: in a project with groups, those in none."""
    if not manifest.groups:
        return []
    return [m for m in offered_models(manifest) if m.group is None]


def owner_role(product: PlannedProduct, role_ids: set[str]) -> str | None:
    """The role the manifest's group or owner names, when one does."""
    names = (product.group, None if product.owner is None else product.owner.name)
    return next((name for name in names if name in role_ids), None)


def _test_label(model: DbtModel, test: DbtNode) -> str:
    call = f"{test.test_name}({test.column})" if test.column else str(test.test_name)
    return f"{model.name}.{call}"


def tests_of(manifest: DbtManifest, product: PlannedProduct) -> list[str]:
    """The dbt tests on the product's models, each named with its model."""
    return sorted(
        _test_label(model, test)
        for model in product.models
        for test in manifest.tests_of(model.unique_id)
    )


def custom_properties(manifest: DbtManifest, product: PlannedProduct) -> dict[str, str]:
    """What the product carries of dbt: its models, its owner and its tests."""
    properties = {PROPERTY_MODELS: ", ".join(m.unique_id for m in product.models)}
    if product.owner is not None and product.owner.name:
        properties[PROPERTY_OWNER] = product.owner.name
    tests = tests_of(manifest, product)
    if tests:
        properties[PROPERTY_TESTS] = "; ".join(tests)
    return properties


def preview(
    manifest: DbtManifest,
    registered: dict[str, int],
    existing: set[str],
    role_ids: set[str],
) -> dict:
    """What registering the public models would do. ``registered`` maps a model's unique id to the
    table it is already registered as; ``existing`` is the product ids already in use."""
    return {
        "products": [
            {
                "id": p.id,
                "group": p.group,
                "owner": None if p.owner is None else asdict(p.owner),
                "owner_role": owner_role(p, role_ids),
                "exists": p.id in existing,
                "tests": tests_of(manifest, p),
                "models": [
                    {
                        "unique_id": m.unique_id,
                        "name": m.name,
                        "schema": m.schema,
                        "relation": m.relation,
                        "version": m.version,
                        "description": m.description,
                        "table_id": registered.get(m.unique_id),
                        "columns": [
                            {"name": c.name, "data_type": c.data_type, "required": c.required}
                            for c in m.columns
                        ],
                    }
                    for m in p.models
                ],
            }
            for p in planned_products(manifest)
        ],
        "ungrouped": [m.name for m in ungrouped(manifest)],
    }


async def check_promise(conn: Any, model: DbtModel, table_id: int) -> None:
    """Refused when the member's table lacks a column the contract promises."""
    held = {c["column_name"].casefold() for c in await load_columns(conn, table_id)}
    missing = sorted(c.name for c in model.columns if c.name.casefold() not in held)
    if missing:
        raise ProductsRefused(
            "dbt.products.columns_missing",
            f"{model.name} does not hold the contract's columns: {', '.join(missing)}",
            model=model.name,
            columns=", ".join(missing),
        )


async def promise_columns(conn: Any, model: DbtModel, table_id: int) -> None:
    """The member's columns as the contract promises them."""
    await check_promise(conn, model, table_id)
    held = {
        c["column_name"].casefold(): c["column_name"] for c in await load_columns(conn, table_id)
    }
    required = [held[c.name.casefold()] for c in model.columns if c.required]
    if required:
        await conn.execute_core(
            update(table_columns)
            .where(table_columns.c.table_id == table_id, table_columns.c.column_name.in_(required))
            .values(not_null=True)
        )


async def write_contract(conn: Any, model: DbtModel, table_id: int) -> None:
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
