# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4b0a2e-7c15-4f39-a6de-2b91c07e5f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a statement names beside a table's registered columns reaches its governed relation.

The governed relation (REQ-039) exposes the columns the role is served, so a name it does not
hold cannot be read through it. Two kinds of column are read without being data columns of the
relation: a parameter a statement names outside its WHERE conjuncts, as a relationship's join on
the words a search was run for (REQ-2099) or a read of an attached code table (REQ-2100); and the
times every read of a bitemporal view answers, which a view inheriting its versions never declared
(REQ-2064, REQ-2109). Named demos sonnets, banking and the code-source reads failed in the engine
with "Binder Error" until the relation carried them."""

# Requirements: REQ-039, REQ-2064, REQ-2099, REQ-2100, REQ-2109

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.governed_relation import relations
from provisa.compiler.nf_extractor import extract_nf_args
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.mv.bitemporal import READ_COLUMNS, BitemporalSpec
from provisa.mv.bitemporal_inherit import with_inherited_read_columns


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"line": 1, "lines_like": 2}
    gov.all_columns = {
        1: [("id", "integer"), ("text", "varchar")],
        2: [("id", "bigint"), ("score", "double")],
    }
    gov.visible_columns = {1: None, 2: None}
    gov.parameter_columns = {2: [("_nf_query", "varchar"), ("_nf_k", "integer")]}
    return gov


def _exposed(governed: str, alias: str) -> set[str]:
    tree = sqlglot.parse_one(governed, read="postgres")
    return {
        p.alias_or_name
        for r in relations(tree)
        if r.alias == alias
        for p in r.relation.this.expressions
    }


def test_a_join_on_a_parameter_reads_it_through_the_relation():
    governed = apply_governance(
        "SELECT l.id, s.id FROM line AS l JOIN lines_like AS s ON s._nf_query = l.text "
        "WHERE s._nf_k = 3",
        _gov(),
    )
    assert "_nf_query" in _exposed(governed, "s")


def test_a_parameter_the_statement_does_not_name_is_not_in_the_relation():
    governed = apply_governance("SELECT s.score FROM lines_like AS s", _gov())
    assert not {"_nf_query", "_nf_k"} & _exposed(governed, "s")


def test_a_where_argument_is_still_extracted_and_leaves_the_relation():
    governed = apply_governance("SELECT s.score FROM lines_like AS s WHERE s._nf_k = 3", _gov())
    clean, _params, args = extract_nf_args(governed, [], {"_nf_query", "_nf_k"})
    assert args == {"k": 3}
    assert "_nf_k" not in clean


def test_a_parameter_the_role_is_not_served_is_refused_by_name():
    gov = _gov()
    gov.visible_columns[2] = frozenset({"score"})
    with pytest.raises(PermissionError, match="_nf_k"):
        apply_governance("SELECT s.score FROM lines_like AS s WHERE s._nf_k = 3", gov)


_VIEW = {
    "id": 7,
    "columns": [
        {
            "column_name": "business_date",
            "data_type": "date",
            "visible_to": ["ops"],
            "is_primary_key": False,
            "alias": None,
            "description": "The day",
        },
        {"column_name": "net", "data_type": "decimal", "visible_to": []},
    ],
}
_SPEC = BitemporalSpec(key=("business_date",), mode="delta", valid_column="business_date")


def _with_read_columns(row: dict) -> dict:
    return with_inherited_read_columns(row, _SPEC)


def test_an_inherited_view_answers_the_read_columns_granted_as_its_key():
    columns = _with_read_columns({**_VIEW, "columns": list(_VIEW["columns"])})["columns"]
    added = {c["column_name"]: c for c in columns[2:]}
    assert tuple(added) == READ_COLUMNS
    assert all(c["visible_to"] == ["ops"] for c in added.values())
    assert all(c["data_type"] == "timestamp" for c in added.values())
    assert added["recorded_at"]["description"] != "The day"


def test_a_view_already_holding_a_read_column_is_given_only_the_others():
    held = {"column_name": "recorded_at", "data_type": "timestamp", "visible_to": ["x"]}
    row = {"id": 7, "columns": [*_VIEW["columns"], held]}
    names = [c["column_name"] for c in _with_read_columns(row)["columns"]]
    assert names.count("recorded_at") == 1
    assert {"source_from", "source_to"} <= set(names)


def test_a_view_with_the_read_columns_exposes_them_through_its_relation():
    columns = _with_read_columns({**_VIEW, "columns": list(_VIEW["columns"])})["columns"]
    gov = GovernanceContext()
    gov.table_map = {"view_totals": 7}
    gov.all_columns = {7: [(c["column_name"], c["data_type"]) for c in columns]}
    gov.visible_columns = {7: None}
    governed = apply_governance("SELECT MIN(e.recorded_at) AS first FROM view_totals AS e", gov)
    tree = sqlglot.parse_one(governed, read="postgres")
    assert isinstance(tree, exp.Select)
    assert "recorded_at" in _exposed(governed, "e")
