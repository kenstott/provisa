# Copyright (c) 2026 Kenneth Stott
# Canary: 3736b664-d9e3-4f78-b651-ba84ea65eea8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A join on a column a view passes through relates the tables along the view's registered
relationship (REQ-603).

The view ``countries`` passes each member's ``code`` through from the member tables it reads, and
``countries.code = alias.fifa_code`` is registered. The statement governance validates has every
view expanded, so before this the join resolved ``c.code`` to the member tables' ``code`` alone,
and the reader was refused V002 for a relationship no member table has, though the view has it.
A pass-through column is now the view's column as well as its table's: a pairing is related when
either is registered. A pairing neither has is refused as before."""

# Requirements: REQ-603

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.compiler.view_expand import expand_view_refs

pytestmark = pytest.mark.unit

_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["*"]}
_IDS = {
    "afc": 1,
    "uefa": 2,
    "alias": 3,
    "countries": 4,
    "names": 5,
    "finals": 6,
    "goalscorers": 7,
    "positional": 8,
}
_COLUMNS = {
    1: ["code", "name"],
    2: ["code", "name"],
    3: ["name", "fifa_code"],
    4: ["code", "country", "confederation"],
    5: ["name"],
    6: ["year", "winners"],
    7: ["team", "scorer"],
    8: ["name"],
}
_VIEWS = {
    "countries": (
        "SELECT a.code, a.name AS country, 'AFC' AS confederation FROM afc a "
        "UNION ALL SELECT u.code, u.name, 'UEFA' FROM uefa u"
    ),
    "names": (
        "SELECT DISTINCT name FROM (SELECT f.winners AS name FROM finals f "
        "UNION ALL SELECT a.name FROM afc a) AS n WHERE name IS NOT NULL"
    ),
}


def _meta(name: str) -> TableMeta:
    return TableMeta(
        table_id=_IDS[name],
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id="worldcup",
        source_type="postgresql",
    )


def _join(target: str, source_column: str, target_column: str) -> JoinMeta:
    return JoinMeta(
        source_column=source_column,
        target_column=target_column,
        source_column_type="varchar",
        target_column_type="varchar",
        target=_meta(target),
        cardinality="one-to-many",
    )


def _v002(sql: str, joins: dict) -> list[str]:
    ctx = CompilationContext()
    ctx.tables = {name: _meta(name) for name in _IDS}
    ctx.joins = joins
    gov = GovernanceContext()
    gov.table_map = dict(_IDS)
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in _COLUMNS.items()}
    validated = expand_view_refs(sql, _VIEWS, mark=True)
    return [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]


_BY_CODE = {("Countries", "alias"): _join("alias", "code", "fifa_code")}
_COUNTRY_ALIAS = "SELECT c.country, a.name FROM countries c JOIN alias a ON a.fifa_code = c.code"


def test_a_union_view_s_column_relates_along_the_view_s_relationship():
    assert _v002(_COUNTRY_ALIAS, _BY_CODE) == []


def test_a_view_over_a_derived_union_relates_along_the_view_s_relationship():
    sql = "SELECT n.name, a.fifa_code FROM names n JOIN alias a ON a.name = n.name"
    assert _v002(sql, {("Names", "alias"): _join("alias", "name", "name")}) == []


def test_the_view_s_tables_own_relationships_still_relate_it():
    # Registered on the member table, not the view: the pass-through column is the table's too.
    joins = {("Afc", "alias"): _join("alias", "code", "fifa_code")}
    sql = "SELECT c.country FROM countries c JOIN alias a ON a.fifa_code = c.code"
    assert all("afc" not in m for m in _v002(sql, joins))


def test_a_pairing_neither_the_view_nor_its_tables_have_is_refused():
    assert any(
        "no approved relationship" in m
        for m in _v002(
            "SELECT c.country FROM countries c JOIN finals f ON f.winners = c.country", _BY_CODE
        )
    )


def test_a_union_branch_fills_the_view_s_column_by_position():
    # The second branch's column is ``team``: it is the view's ``name`` by position, so the
    # goalscorers it reads are related through the view like the finals.
    _VIEWS["positional"] = (
        "SELECT name FROM (SELECT f.winners AS name FROM finals f "
        "UNION ALL SELECT g.team FROM goalscorers g) AS n"
    )
    try:
        assert (
            _v002(
                "SELECT p.name, a.fifa_code FROM positional p JOIN alias a ON a.name = p.name",
                {("Positional", "alias"): _join("alias", "name", "name")},
            )
            == []
        )
    finally:
        del _VIEWS["positional"]


def test_a_union_a_caller_writes_still_needs_each_branch_s_relationship():
    # No view: each branch's table is paired with alias on its own column, by position, and
    # goalscorers.team has no relationship to alias.
    written = (
        "SELECT n.name FROM (SELECT f.winners AS name FROM finals f "
        "UNION ALL SELECT g.team FROM goalscorers g) AS n JOIN alias a ON a.name = n.name"
    )
    refused = _v002(written, {("Finals", "alias"): _join("alias", "winners", "name")})
    assert any("goalscorers.team" in m and "no approved relationship" in m for m in refused)
