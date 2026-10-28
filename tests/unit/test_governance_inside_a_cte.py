# Copyright (c) 2026 Kenneth Stott
# Canary: db701772-ffc2-4fcc-a400-efb7f9978e12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table read inside a CTE is governed as anywhere else (REQ-2176: a label's union
is one CTE, its model arm the registered table): the role's row filter and its column masks
apply to the table where the CTE reads it, so every reference to the CTE reads governed rows."""

# Requirements: REQ-2176, REQ-087, REQ-041

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.security.masking import MaskType, MaskingRule
from provisa.transpiler.transpile import transpile
from tests.helpers import governed_for_role

pytestmark = pytest.mark.unit

_CUSTOMERS = TableMeta(
    table_id=2,
    field_name="customers",
    type_name="Customers",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="customers",
)
_MASK = {
    (2, "clerk"): {
        "email": (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar"),
    }
}
_SQL = (
    'WITH "__session_node__Customer" AS ('
    'SELECT "c"."id", "c"."email", "c"."region" FROM "public"."customers" AS "c") '
    'SELECT a."id", a."email" FROM "__session_node__Customer" AS a '
    'JOIN "__session_node__Customer" AS b ON a."id" = b."id"'
)


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {"customers": _CUSTOMERS}
    ctx.joins = {}
    return ctx


def test_the_row_filter_and_the_mask_apply_to_the_table_inside_the_cte():
    governed = governed_for_role(
        _SQL, _ctx(), "clerk", rls={2: "region = 'east'"}, masking_rules=_MASK
    )
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.customers (id INTEGER, email VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO public.customers VALUES (1, 'a@x.io', 'east'), (2, 'b@x.io', 'west')")
    rows = con.execute(transpile(governed.replace('"pg".', ""), "duckdb")).fetchall()
    assert rows == [(1, "***")]
