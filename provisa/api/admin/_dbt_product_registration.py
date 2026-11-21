# Copyright (c) 2026 Kenneth Stott
# Canary: 4c81e7a9-2d3f-4b60-9e15-a87c0d3f61b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a dbt project's public models as draft data products, from Register Table (REQ-2228).

Every table is registered by the registerTable mutation and every product is created and given its
members by the data product mutations, as the steward's own forms do, under the same gates: there is
no second way to register a table or create a product. A product is created a draft, as every new
product is, for a steward to review and publish.
"""

# Requirements: REQ-2228

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import select

from provisa.core.repositories import data_product as data_product_repo
from provisa.core.schema_org import roles
from provisa.dbt.products import (
    PlannedProduct,
    ProductsRefused,
    check_promise,
    custom_properties,
    owner_role,
    planned_products,
    preview,
    promise_columns,
)
from provisa.dbt.store import held_manifest, model_tables

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult
    from provisa.dbt.manifest import DbtManifest


def _not_held(source_id: str) -> ProductsRefused:
    return ProductsRefused(
        "dbt.source_not_registered", f"dbt source {source_id!r} not registered", source_id=source_id
    )


async def _registered(conn: Any, manifest: DbtManifest) -> dict[str, int]:
    return {model.unique_id: row.id for model, row in await model_tables(conn, manifest)}


async def _role_ids(conn: Any) -> set[str]:
    return {r.id for r in (await conn.execute_core(select(roles.c.id))).fetchall()}


async def _existing(conn: Any, plan: list[PlannedProduct]) -> set[str]:
    return {p.id for p in plan if await data_product_repo.get(conn, p.id) is not None}


async def products_preview(conn: Any, source_id: str) -> dict:
    """What registering ``source_id``'s public models would do. Nothing is created by asking."""
    manifest = await held_manifest(conn, source_id)
    if manifest is None:
        raise _not_held(source_id)
    return preview(
        manifest,
        await _registered(conn, manifest),
        await _existing(conn, planned_products(manifest)),
        await _role_ids(conn),
    )


def _refused(result: MutationResult) -> ProductsRefused:
    """A governed mutation's refusal, passed on as it was given."""
    return ProductsRefused(
        result.code or "dbt.products.refused", result.message, **(result.params or {})
    )


async def _register_models(info: Any, source_id: str, domain_id: str, models: list) -> None:
    """Each model registered as a table by the registerTable mutation, columns as the warehouse
    lists them, as Register Table proposes them."""
    from provisa.api.admin.schema_mutation import Mutation, default_table_input
    from provisa.api.admin.schema_query import resolve_available_columns_metadata

    for model in models:
        columns = await resolve_available_columns_metadata(source_id, model.schema, model.relation)
        table = default_table_input(source_id, domain_id, model.schema, model.relation, columns)
        result = await Mutation().register_table(info, table)  # pyright: ignore[reportCallIssue]
        if not result.success:
            raise _refused(result)


async def register_products(info: Any, source_id: str, domain_id: str) -> list[str]:
    """Every public model of ``source_id`` registered, and its products created as drafts in
    ``domain_id``. Refused before anything is written when there is nothing to register or a
    product's id is in use; the ids of the products created otherwise."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.admin.types import DataProductInput

    pool = await _get_pool()
    async with pool.acquire() as conn:
        manifest = await held_manifest(conn, source_id)
        if manifest is None:
            raise _not_held(source_id)
        plan = planned_products(manifest)
        if not plan:
            raise ProductsRefused(
                "dbt.products.none_offered",
                f"dbt source {source_id!r} has no public model with an enforced contract",
                source_id=source_id,
            )
        taken = sorted(await _existing(conn, plan))
        if taken:
            raise ProductsRefused(
                "dbt.products.id_taken",
                f"Data product {taken[0]!r} already exists",
                data_product=taken[0],
            )
        registered = await _registered(conn, manifest)
        role_ids = await _role_ids(conn)
    models = [m for p in plan for m in p.models]
    await _register_models(
        info, source_id, domain_id, [m for m in models if m.unique_id not in registered]
    )
    async with pool.acquire() as conn:
        tables = await _registered(conn, manifest)
        for model in models:
            await check_promise(conn, model, tables[model.unique_id])
    mutation = Mutation()
    for product in plan:
        draft = DataProductInput(
            id=product.id,
            domain_id=domain_id,
            name=product.id,
            owner_role=owner_role(product, role_ids),
            support_contact=None if product.owner is None else product.owner.email,
            custom_properties=cast("Any", custom_properties(manifest, product)),
        )
        # Same strawberry-decorator signature limitation as the other in-module mutation calls.
        created = await mutation.create_data_product(info, draft)  # pyright: ignore[reportCallIssue]
        if not created.success:
            raise _refused(created)
        async with pool.acquire() as conn:
            for model in product.models:
                await promise_columns(conn, model, tables[model.unique_id])
        table_ids = [tables[m.unique_id] for m in product.models]
        members = await mutation.set_data_product_members(info, product.id, table_ids, [])  # pyright: ignore[reportCallIssue]
        if not members.success:
            raise _refused(members)
    return [p.id for p in plan]
