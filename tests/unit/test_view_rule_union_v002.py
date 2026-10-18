# Copyright (c) 2026 Kenneth Stott
# Canary: 2d036f5f-9bf9-4bce-9208-b26ef0198ce0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#271: a row rule on a view whose body is a UNION ALL, each branch joining the same table along
a relationship, is not refused V002. The rule's column comes from that table in every branch; the
branches are alternatives, so the condition relates no instance of one to the other's. A condition
that does relate two tables of one branch by something other than an equality is still refused."""

# Requirements: REQ-603

from types import SimpleNamespace

import pytest

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.view_read import semantic_statement

_ROLE = {
    "id": "reviewer_ben",
    "description": "Test role.",
    "capabilities": [],
    "domain_access": ["*"],
}
_COLUMNS = {
    1: ["employee_id", "desk"],  # employee
    2: ["desk", "reviewer"],  # desk_reviewer
    3: ["employee_id", "reviewer"],  # view_queue
}
_ONE = (
    "SELECT e.employee_id, r.reviewer FROM employee AS e JOIN desk_reviewer AS r ON r.desk = e.desk"
)
_VIEWS = {"one": _ONE, "union": f"{_ONE} UNION ALL {_ONE}"}


def _meta(table_id, name):
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="bank",
        table_name=name,
        domain_id="bank",
        source_type="postgresql",
    )


def _v002(view_sql, rule):
    employee, desk, queue = _meta(1, "employee"), _meta(2, "desk_reviewer"), _meta(3, "view_queue")
    ctx = CompilationContext()
    ctx.tables = {"employee": employee, "desk_reviewer": desk, "view_queue": queue}
    ctx.joins = {
        ("Employee", "reviewed_by"): JoinMeta(
            source_column="desk",
            target_column="desk",
            source_column_type="varchar",
            target_column_type="varchar",
            target=desk,
            cardinality="many-to-one",
        ),
    }
    gov = GovernanceContext()
    gov.table_map = {"employee": 1, "desk_reviewer": 2, "view_queue": 3}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in _COLUMNS.items()}
    if rule:
        gov.rls_rules = {3: "reviewer = current_setting('provisa.role')"}
    state = SimpleNamespace(mv_registry={}, tables=[], role_build_inputs={"draft_tables": []})
    _governed, validated = semantic_statement(
        "SELECT employee_id FROM view_queue", {"view_queue": view_sql}, state, gov
    )
    return [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]


@pytest.mark.parametrize("view", ["one", "union"])
@pytest.mark.parametrize("rule", [False, True], ids=["no rule", "row rule on the view"])
def test_a_view_joined_along_its_relationship_is_allowed(view, rule):
    assert _v002(_VIEWS[view], rule) == []


def test_a_non_equality_between_two_tables_of_a_branch_is_still_refused():
    view = (
        "SELECT e.employee_id, r.reviewer FROM employee AS e "
        "JOIN desk_reviewer AS r ON r.desk > e.desk"
    )
    assert _v002(f"{view} UNION ALL {view}", False)
