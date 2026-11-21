# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5d7f2e-3b8a-4e61-9d2f-6a1c4b7e8f30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2131 (amended 8fa0d1c262): saving a constant mask with a value other than NULL on a
non-text column is refused by name, since it can only read as NULL -- on every path that saves a
mask list: the table store, the table mutation, a config load, and a command's or webhook's
contract through the store and the REST saves."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from provisa.api.admin import actions_router
from provisa.api.errors import ApiError
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.models import Column, DatasetColumn, Function, InlineType, Table, Webhook
from provisa.core.repositories import function as function_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import table_columns, tracked_functions
from provisa.security.masking import (
    NON_TEXT_CONSTANT,
    MaskConstantNotText,
    refuse_non_text_constant,
)
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_landing_ttl_admin import _db, _table_input, _table_patches
from tests.unit.test_mask_rules import _config, plane  # noqa: F401 -- the fixture

ZERO = [{"to": ["clerk"], "mask": {"type": "constant", "value": "0"}}, {"to": None, "mask": None}]
NULLED = [
    {"to": ["clerk"], "mask": {"type": "constant", "value": None}},
    {"to": None, "mask": None},
]
STARRED = [
    {"to": ["clerk"], "mask": {"type": "constant", "value": "***"}},
    {"to": None, "mask": None},
]


def _table(data_type: str, masks: list[dict]) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema="public",
        table="orders",
        columns=[Column(name="amount", data_type=data_type, visible_to=["clerk"], masks=masks)],
    )


async def _stored(db) -> list:
    async with db.acquire() as conn:
        return [r[0] for r in (await conn.execute_core(select(table_columns.c.masks))).fetchall()]


# --- the rule -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "data_type", ["integer", "decimal(10,2)", "boolean", "date", "timestamp", "bytea", "uuid"]
)
def test_a_constant_other_than_null_on_a_non_text_column_is_refused_by_name(data_type):
    with pytest.raises(MaskConstantNotText) as refused:
        refuse_non_text_constant("amount", ZERO, data_type)
    assert str(refused.value) == f"{NON_TEXT_CONSTANT}amount ({data_type})"
    assert (refused.value.column, refused.value.data_type) == ("amount", data_type)


@pytest.mark.parametrize("data_type", ["varchar(30)", "text", "character varying", "String"])
def test_a_text_column_keeps_its_constant(data_type):
    refuse_non_text_constant("amount", STARRED, data_type)


def test_null_is_saved_on_a_non_text_column():
    refuse_non_text_constant("amount", NULLED, "integer")


# --- every path that saves a list -----------------------------------------------------------


async def test_the_table_store_refuses_the_list(plane):  # noqa: F811
    async with plane.acquire() as conn:
        with pytest.raises(MaskConstantNotText) as refused:
            await table_repo.upsert(conn, _table("integer", ZERO))
    assert refused.value.column == "amount"
    assert await _stored(plane) == []


async def test_the_table_store_saves_null_on_a_non_text_column(plane):  # noqa: F811
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table("integer", NULLED))
    [stored] = await _stored(plane)
    assert (stored[0]["mask"]["type"], stored[0]["mask"]["value"]) == ("constant", None)


@pytest.mark.asyncio
async def test_the_table_mutation_answers_the_refusal_by_name(control_plane, monkeypatch):
    from provisa.api.admin.schema_mutation import Mutation

    serve_every_column(monkeypatch)
    column = Column(
        name="id",
        data_type="integer",
        visible_to=["analyst"],
        masks=[{"to": None, "mask": {"type": "constant", "value": "0"}}],
    )
    async with _db(control_plane, table_ttl=60) as db:
        patches = list(_table_patches(db))
        patches[3] = patch(
            "provisa.api.admin.schema_mutation._build_columns_for_input",
            new=AsyncMock(return_value=([column], None)),
        )
        for p in patches:
            p.start() if hasattr(p, "start") else p.__enter__()
        try:
            result = await Mutation().update_table(MagicMock(), _table_input())
        finally:
            for p in reversed(patches):
                p.stop() if hasattr(p, "stop") else p.__exit__(None, None, None)
    assert result.success is False
    assert result.code == "schema.mask_constant_not_text"
    assert result.message == f"{NON_TEXT_CONSTANT}id (integer)"
    assert result.params == {"table": "orders", "column": "id", "type": "integer"}


async def test_a_config_load_refuses_the_list(control_plane):
    db = control_plane.org()
    masks = [{"to": None, "mask": {"type": "constant", "value": 0}}]
    async with db.acquire() as conn:
        with pytest.raises(MaskConstantNotText, match=r"id \(integer\)"):
            await apply_config(parse_config_dict(_config(masks)), conn)


OUT = [{"name": "amount", "type": "integer", "masks": ZERO}]


async def test_the_store_refuses_a_command_or_webhook_list(plane):  # noqa: F811
    async with plane.acquire() as conn:
        with pytest.raises(MaskConstantNotText):
            await function_repo.upsert_function(
                conn,
                Function(
                    name="refund",
                    source_id="pg",
                    function_name="refund",
                    returns="",
                    domain_id="sales",
                    output_columns=[DatasetColumn(**OUT[0])],
                ),
            )
        with pytest.raises(MaskConstantNotText):
            await function_repo.upsert_webhook(
                conn,
                Webhook(
                    name="notify",
                    url="http://x",
                    domain_id="sales",
                    inline_return_type=[
                        InlineType(name="amount", type="Int", masks=ZERO)  # a GraphQL scalar
                    ],
                ),
            )


async def _refused(call) -> ApiError:
    with pytest.raises(ApiError) as refused:
        await call
    assert refused.value.status_code == 422
    assert refused.value.code == "actions.mask_constant_not_text"
    return refused.value


async def test_the_rest_saves_of_a_command_and_a_webhook_refuse_the_list(plane):  # noqa: F811
    request = MagicMock()
    function = actions_router.FunctionInput(
        name="refund",
        sourceId="pg",
        functionName="refund",
        returns="",
        domainId="sales",
        outputColumns=OUT,
    )
    webhook = actions_router.WebhookInput(
        name="notify", url="http://x", domainId="sales", inlineReturnType=OUT
    )
    for call in (
        actions_router.create_function(request, function),
        actions_router.update_function(request, "refund", function),
        actions_router.create_webhook(request, webhook),
        actions_router.update_webhook(request, "notify", webhook),
    ):
        refused = await _refused(call)
        assert str(refused.detail) == f"{NON_TEXT_CONSTANT}amount (integer)"
        assert refused.params == {"column": "amount", "type": "integer"}
    async with plane.acquire() as conn:
        assert (await conn.execute_core(select(tracked_functions))).fetchall() == []
