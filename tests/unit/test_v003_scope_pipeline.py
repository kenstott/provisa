# Copyright (c) 2026 Kenneth Stott
# Canary: 067af2db-dab2-4a68-b1cd-f152825b0f3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_catalog.unnest(...) is the client table function it is in the pgwire pipeline's relation
check too (REQ-2204; J595 triage cause 8): on a catalog statement a role without every domain
is not refused it as an inaccessible table; on any other statement it is refused."""

# Requirements: REQ-2204

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.pgwire.catalog_route import current_request
from tests.unit.test_view_inputs_in_service import _OVER_A_SERVED_TABLE, _build, _view

pytestmark = pytest.mark.unit

_SQL = 'SELECT o.id, x FROM "sales"."orders" o, pg_catalog.unnest(ARRAY[1]) x'


async def _govern(sql: str = _SQL) -> None:
    from provisa.pgwire import _pipeline

    await _pipeline.govern_statement(sql, "r")


async def test_pg_catalog_unnest_is_admitted_on_a_catalog_statement(monkeypatch):
    _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    token = current_request.set(SimpleNamespace(role_id="r"))  # type: ignore[arg-type]
    try:
        await _govern()
    finally:
        current_request.reset(token)


async def test_pg_catalog_unnest_is_refused_on_any_other_statement(monkeypatch):
    _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    with pytest.raises(PermissionError, match="UNNEST|unnest"):
        await _govern()


@pytest.mark.parametrize(
    "fn",
    [
        "evil.unnest(ARRAY[1]) x",
        "public.unnest(ARRAY[1]) x",
        "cat.evil.unnest(ARRAY[1]) x",
        "explode(ARRAY[1]) x",
        "pg_catalog.explode(ARRAY[1]) x",
        "pg_catalog.posexplode(ARRAY[1]) x",
    ],
)
async def test_a_function_that_is_not_postgresql_unnest_is_refused_on_a_catalog_statement(
    monkeypatch, fn
):
    # RV597 H3: only pg_catalog's own unnest is the client table function, never another schema's
    # function of that name or sqlglot's other Explode functions.
    _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), drafts=[])
    token = current_request.set(SimpleNamespace(role_id="r"))  # type: ignore[arg-type]
    try:
        with pytest.raises(PermissionError, match="(?i)unnest|explode"):
            await _govern(f'SELECT o.id FROM "sales"."orders" o, {fn}')
    finally:
        current_request.reset(token)
