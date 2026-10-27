# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f0d0e-2f4b-4c86-9d1b-5a3e7f2c8b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A parameter's WHERE condition survives governance for a role served the table (REQ-1986).

A parameter column is an argument, not data: it carries no grant of its own (``visible_to: []``,
which names no role, REQ-2154), and a role served one of the table's data columns may pass it
(REQ-1614). Governance must leave the condition as written, or the argument is never extracted
and the read is refused for want of it.
"""

# Requirements: REQ-1986, REQ-1614, REQ-2154

from __future__ import annotations

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.nf_extractor import extract_nf_args
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.pgwire._pipeline import context_parameters

ROLE = {"id": "analyst", "capabilities": ["query_development"], "domain_access": ["*"]}


def _table(data_grant: list[str]) -> dict:
    return {
        "id": 1,
        "source_id": "atlas",
        "domain_id": "films",
        "schema_name": "public",
        "table_name": "movies_matching",
        "columns": [
            {"column_name": "title", "visible_to": data_grant},
            {
                "column_name": "_nf_q",
                "visible_to": [],
                "native_filter_type": "query_arg",
                "native_filter_required": True,
            },
        ],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _governed(table: dict) -> tuple[str, set[str]]:
    si = SchemaInput(
        tables=[table],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="title", data_type="varchar", is_nullable=True),
                ColumnMetadata(column_name="_nf_q", data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role=ROLE,
        domains=[{"id": "films", "description": "Films"}],
    )
    ctx = build_context(si)
    gov = build_governance_context("analyst", RLSContext(rules={}), {}, ctx, [table], ROLE)
    sql = "SELECT title FROM movies_matching WHERE _nf_q = 'dogs'"
    return apply_governance(sql, gov), context_parameters(ctx)


def test_a_parameter_with_no_grant_of_its_own_is_passed_by_a_role_served_the_table():
    governed, parameters = _governed(_table(["analyst"]))
    _sql, _params, args = extract_nf_args(governed, [], parameters)
    assert args == {"q": "dogs"}
