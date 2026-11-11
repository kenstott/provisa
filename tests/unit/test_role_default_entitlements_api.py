# Copyright (c) 2026 Kenneth Stott
# Canary: 3e1b7c52-9a64-4d0f-8f2e-b6a0c91d7e44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, THE ENTITLEMENT MODEL: a role's default entitlements (what a holder gets with no list)
are authored with the role. The admin GraphQL role carries them as defaultEntitlements, read and
written as {kind: [identifier, ...]}."""

from __future__ import annotations

from typing import Any

import pytest
from strawberry.printer import print_schema

from provisa.api.admin._row_mappers import _role_from_row
from provisa.api.admin.schema import admin_schema
from provisa.api.admin.types import RoleInput
from tests.unit.test_role_session_constants import _info, _stored
from tests.unit.test_role_session_constants import plane as plane  # noqa: F401  (fixture)

_EFFECTIVE = {"capabilities": [], "domains": [], "regions": []}
_ROW = {
    "description": "Test role.",
    "id": "seller",
    "capabilities": [],
    "domain_access": ["sales"],
    "demonstrated": [],
    "residency_values": [],
    "session_vars": {},
    "inactivity_days": None,
    "parents": [],
}


def _block(sdl: str, header: str) -> str:
    return sdl.split(header, 1)[1].split("}", 1)[0]


def test_the_role_type_and_input_carry_default_entitlements_by_its_graphql_name():
    sdl = print_schema(admin_schema)
    assert "defaultEntitlements: JSON\n" in _block(sdl, "type RoleType {")
    assert "defaultEntitlements: JSON = null" in _block(sdl, "input RoleInput {")


def test_the_role_type_reads_the_stored_defaults():
    stored = {"tables": ["sales.*"], "relationships": []}
    out = _role_from_row(
        {**_ROW, "default_entitlements": stored}, detail=True, effective=_EFFECTIVE
    )
    assert out.default_entitlements == stored


def test_a_role_shown_without_its_definition_shows_no_defaults():
    out = _role_from_row(
        {"id": "seller", "description": "Test role.", "default_entitlements": {"tables": []}},
        detail=False,
        effective=_EFFECTIVE,
    )
    assert out.default_entitlements is None


async def _save(**over: Any) -> Any:
    from provisa.api.admin import schema_mutation

    return await schema_mutation.Mutation().create_role(
        _info("admin"),  # type: ignore[arg-type]
        RoleInput(
            id="seller",
            description="Test role.",
            capabilities=["usage"],
            domain_access=["sales"],
            **over,
        ),  # pyright: ignore[reportCallIssue]
    )


@pytest.mark.asyncio
async def test_a_save_writes_the_defaults_it_states(plane):  # noqa: F811
    result = await _save(default_entitlements={"tables": ["sales.orders"], "relationships": []})
    assert result.success is True, result.message
    stored = await _stored(plane)
    assert stored is not None
    assert stored["default_entitlements"] == {"tables": ["sales.orders"], "relationships": []}


@pytest.mark.asyncio
async def test_a_save_without_defaults_writes_them_from_the_roles_domains(plane):  # noqa: F811
    result = await _save()
    assert result.success is True, result.message
    stored = await _stored(plane)
    assert stored is not None
    assert stored["default_entitlements"] == {"domains": ["sales"]}
