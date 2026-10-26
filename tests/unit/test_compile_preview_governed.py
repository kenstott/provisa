# Copyright (c) 2026 Kenneth Stott
# Canary: 2bbdf903-8921-4b60-886c-135c16d8e555
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The compile preview ("Show SQL") shows the statement the compiled pipeline runs: governed by
the one stage-2 rewrite, so a relationship join on a masked key reads the mask (REQ-740) there too,
and the row filter and the row cap are those the run applies."""

# Requirements: REQ-740, REQ-262

from __future__ import annotations

import pytest

from provisa.api.admin.dev_queries import _apply_pipeline_transforms
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, CompiledQuery, TableMeta
from provisa.federation.replica_address import ReplicaRoutes
from provisa.security.masking import MaskingRule, MaskType
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio

_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["*"], "max_rows": 50}
_SQL = (
    'SELECT "t0"."id", "t1"."name" FROM "sales"."orders" "t0" '
    'LEFT JOIN "sales"."customers" "t1" ON "t0"."email" = "t1"."name"'
)


def _meta(table_id: int, name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="sales",
        table_name=name,
        domain_id="sales",
    )


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "sales",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": ["analyst"]} for c in columns],
    }


def _state():
    ctx = CompilationContext()
    ctx.tables = {"orders": _meta(1, "orders"), "customers": _meta(2, "customers")}
    return ctx, RoleState(
        contexts={"analyst": ctx},
        rls_contexts={"analyst": RLSContext(rules={1: "region = 'east'"})},
        roles={"analyst": _ROLE},
        masking_rules={
            (1, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
            }
        },
        tables=[
            _table(1, "orders", ["id", "email", "region"]),
            _table(2, "customers", ["id", "name"]),
        ],
        relationships=[],
        source_types={"pg": "postgresql"},
        source_catalogs={},
        metrics={},
        federation_engine=None,
        view_sql_map=None,
        kafka_table_configs=None,
        security_high=False,
        admin_db=None,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        schema_boot_id="test-boot",
        schema_version=1,
        model_stamp=1,
    )


def _compiled() -> CompiledQuery:
    return CompiledQuery(sql=_SQL, params=[], root_field="orders", columns=[], sources={"pg"})


async def _shown(monkeypatch) -> str:
    import provisa.api.app as app_mod

    ctx, state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    shown, _mv = await _apply_pipeline_transforms(_compiled(), ctx, "analyst", [], state)
    return shown.sql


async def test_the_preview_shows_a_join_on_a_masked_key_reading_the_mask(monkeypatch):
    assert 'ON \'***\' = "t1"."name"' in await _shown(monkeypatch)


async def test_the_preview_carries_the_row_filter_and_the_row_cap_the_run_applies(monkeypatch):
    shown = await _shown(monkeypatch)
    assert '"t0"."region" = \'east\'' in shown and "LIMIT 50" in shown
