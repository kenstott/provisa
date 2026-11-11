# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e7a52-1d84-4f6b-b2e0-8a5f1c7d9e36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Several tables registered in one action (REQ-2234): each goes through the same registerTable
a single registration runs, with its rights and checks, and a table one of them refuses is named
while the others are registered."""

# Requirements: REQ-2234

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import schema_mutation
from provisa.api.admin.types import ColumnInput, MutationResult, TableInput


def _table(name: str, domain: str = "sales") -> TableInput:
    return TableInput(
        source_id="pg",
        domain_id=domain,
        schema_name="public",
        table_name=name,
        columns=[ColumnInput(name="id", visible_to=["analyst"], data_type="integer")],
    )


@pytest.fixture
def single_path(monkeypatch):
    """The single registration's own steps: its rights check (refusing the domain ``hr``) and the
    registration it then runs, recorded in the order they ran."""
    calls: list[tuple[str, str]] = []

    def _require(_info, capability, domain_id=None):
        calls.append(("check", f"{capability}:{domain_id}"))
        if domain_id == "hr":
            raise PermissionError(f"Missing capability: {capability!r}")

    async def _register(_info, table):
        calls.append(("register", table.table_name))
        if table.table_name == "invoices":
            return MutationResult(success=False, message="column 'total' has no data type")
        return MutationResult(
            success=True, message=f"Registered {table.table_name}", params={"id": len(calls)}
        )

    monkeypatch.setattr(schema_mutation, "require_capability", _require)
    monkeypatch.setattr(schema_mutation._ops, "register_table", _register)
    return calls


async def test_each_table_is_registered_through_the_single_registration(single_path):
    from provisa.api.admin._batch_registration import register_tables

    outcomes = await register_tables(SimpleNamespace(), [_table("orders"), _table("customers")])

    assert single_path == [
        ("check", "table_registration:sales"),
        ("register", "orders"),
        ("check", "table_registration:sales"),
        ("register", "customers"),
    ]
    assert [(o.table_name, o.result.success) for o in outcomes] == [
        ("orders", True),
        ("customers", True),
    ]
    # What the single registration answered is what each table's outcome carries.
    assert outcomes[0].result.params == {"id": 2}


async def test_a_refused_table_is_named_and_the_others_are_registered(single_path):
    from provisa.api.admin._batch_registration import register_tables

    outcomes = await register_tables(
        SimpleNamespace(),
        [
            _table("orders"),
            _table("invoices"),
            _table("salaries", domain="hr"),
            _table("customers"),
        ],
    )

    assert [(o.table_name, o.result.success) for o in outcomes] == [
        ("orders", True),
        ("invoices", False),
        ("salaries", False),
        ("customers", True),
    ]
    assert outcomes[1].result.message == "column 'total' has no data type"
    assert outcomes[2].result.message == "Missing capability: 'table_registration'"
    # The table its rights refused is never registered; every other one is.
    assert [c[1] for c in single_path if c[0] == "register"] == ["orders", "invoices", "customers"]


async def test_the_mutation_registers_the_tables_it_is_given(single_path):
    outcomes = await schema_mutation.Mutation().register_tables(  # pyright: ignore[reportCallIssue]
        SimpleNamespace(), [_table("orders")]
    )

    assert [(o.table_name, o.result.success) for o in outcomes] == [("orders", True)]
