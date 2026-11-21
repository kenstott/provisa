# Copyright (c) 2026 Kenneth Stott
# Canary: 3b0b1c52-5f0e-4a64-9c1e-6a1f5b7d2e91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table named ``commands`` is refused by name on every registration path (REQ-1929)."""

# Requirements: REQ-1929

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from provisa.api.admin import schema_mutation
from provisa.api.admin.schema_helpers import table_domain_refusal
from provisa.api.admin.types import ColumnInput, TableInput
from provisa.core import domain_policy
from provisa.core.models import Column, Table

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

RESERVED = "schema.table_name_reserved"
SPELLINGS = ["commands", "Commands", "COMMANDS"]


@pytest.fixture(autouse=True)
def _inert_policy():
    domain_policy.reset()
    yield
    domain_policy.reset()


def _input(**kw: Any) -> TableInput:
    base: dict[str, Any] = dict(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="commands",
        columns=[ColumnInput(name="id", visible_to=["analyst"])],
    )
    base.update(kw)
    return TableInput(**base)


def _table_fields(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="commands",
        columns=[Column(name="id", visible_to=["analyst"])],
    )
    base.update(kw)
    return base


def _table(**kw: Any) -> Table:
    return Table(**_table_fields(**kw))


@pytest.mark.parametrize("name", SPELLINGS)
async def test_the_policy_refuses_the_reserved_name(name):
    with pytest.raises(domain_policy.ReservedTableName, match="commands"):
        domain_policy.require_table_name(name, None)


async def test_the_policy_refuses_a_reserved_alias():
    with pytest.raises(domain_policy.ReservedTableName):
        domain_policy.require_table_name("beer", "Commands")


async def test_the_policy_accepts_other_names():
    domain_policy.require_table_name("beer", None)
    domain_policy.require_table_name("commands_log", "my_commands")


@pytest.mark.parametrize("name", SPELLINGS)
async def test_the_early_api_check_refuses_it_with_its_code(name):
    result = table_domain_refusal("d", name)
    assert result is not None
    assert (result.success, result.code, result.params) == (False, RESERVED, {"table": name})


async def test_the_early_api_check_refuses_an_alias():
    result = table_domain_refusal("d", "beer", "commands")
    assert result is not None and result.code == RESERVED


async def test_register_table_refuses_it():
    result = await schema_mutation.Mutation().register_table(MagicMock(), _input())  # pyright: ignore[reportCallIssue]
    assert (result.success, result.code) == (False, RESERVED)


async def test_register_tables_bulk_refuses_it():
    from provisa.api.admin._batch_registration import register_tables

    outcomes = await register_tables(MagicMock(), [_input()])
    assert [(o.result.success, o.result.code) for o in outcomes] == [(False, RESERVED)]


async def test_the_repository_write_refuses_it():
    from provisa.core.repositories import table as table_repo

    with pytest.raises(domain_policy.ReservedTableName):
        await table_repo.upsert(MagicMock(), Table.model_construct(**_table_fields()))


async def test_the_repository_write_refuses_a_reserved_alias():
    from provisa.core.repositories import table as table_repo

    with pytest.raises(domain_policy.ReservedTableName):
        await table_repo.upsert(
            MagicMock(),
            Table.model_construct(**_table_fields(table_name="beer", alias="Commands")),
        )


@pytest.mark.parametrize("name", SPELLINGS)
async def test_a_config_declaring_it_is_refused_at_load(name):
    with pytest.raises(ValidationError, match="commands"):
        _table(table_name=name)


async def test_a_config_declaring_it_as_an_alias_is_refused_at_load():
    with pytest.raises(ValidationError, match="commands"):
        _table(table_name="beer", alias="commands")


async def test_a_config_declaring_another_name_loads():
    assert _table(table_name="beer").table_name == "beer"


async def test_the_server_error_code_is_translated_in_every_locale():
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "provisa-ui/src/i18n/locales"
    for locale in sorted(p for p in root.iterdir() if p.is_dir()):
        errors = json.loads((locale / "serverErrors.json").read_text(encoding="utf-8"))
        entry = errors["serverErrors"]["schema"]["table_name_reserved"]
        assert "{{table}}" in entry, locale.name
