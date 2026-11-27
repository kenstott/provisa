# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d3a92-8c47-4e15-b0a9-c2e7d5f3b841
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2278: a column rule keyed on a tag or a classification hides or masks, per role, every
column carrying the key -- a table's and a command's output column alike, through one evaluator --
and where the column's own grant and masks apply too, the stricter result holds."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.api.app_loaders import _with_ruled_masks
from provisa.api.data.action_governance import govern_action_rows
from provisa.core.models import ColumnRuleModel
from provisa.security.column_rules import (
    CLASSIFICATION,
    FLOOR,
    TAG,
    ColumnRule,
    govern_column,
    hide_ruled_tables,
    ruled_columns,
    tag_keys,
)
from provisa.security.masking import MaskType, masked_for, resolve
from provisa.security.rights import column_served
from tests.unit.test_action_governance import FN, ROWS, _state

CHAINS = {
    "analyst": ["analyst"],
    "junior": ["junior", "analyst"],
    "org_admin": ["org_admin"],
}
PARTIAL = {"type": "regex", "pattern": "^(.{2}).*$", "replace": "\\1**"}


def _hide(role: str, key=(TAG, "pii"), rid: int = 1) -> ColumnRule:
    return ColumnRule(id=rid, key=key, role_id=role, action="hide")


def _mask(role: str, mask: dict | None = None, key=(TAG, "pii"), rid: int = 2) -> ColumnRule:
    return ColumnRule(id=rid, key=key, role_id=role, action="mask", mask=mask)


def _served(column: dict, role: str) -> bool:
    return column_served({"id": role}, "sales", column, reaches=True)


class TestEvaluator:
    def test_hide_takes_the_role_and_its_descendants_out_of_the_grant(self):
        column = {"visible_to": ["analyst", "junior", "org_admin"], "masks": []}
        governed = govern_column(column, [_hide("analyst")], CHAINS)
        assert not _served(governed, "analyst")
        assert not _served(governed, "junior")  # descends from analyst
        assert _served(governed, "org_admin")
        assert column["visible_to"] == ["analyst", "junior", "org_admin"]  # a copy is governed

    def test_mask_applies_to_the_role_and_its_descendants_only(self):
        governed = govern_column(
            {"visible_to": ["analyst", "junior", "org_admin"], "masks": []},
            [_mask("analyst", PARTIAL)],
            CHAINS,
        )
        for role in ("analyst", "junior"):
            rule = resolve(governed["masks"], CHAINS[role], "varchar")
            assert rule is not None and rule.mask_type == MaskType.regex
            assert masked_for(governed["masks"], CHAINS[role])
        assert resolve(governed["masks"], CHAINS["org_admin"], "varchar") is None
        assert not masked_for(governed["masks"], CHAINS["org_admin"])

    def test_a_mask_rule_with_no_mask_masks_completely(self):
        governed = govern_column({"visible_to": ["analyst"], "masks": []}, [_mask("analyst")], {})
        rule = resolve(governed["masks"], ["analyst"], "varchar")
        assert (rule.mask_type, rule.value) == (MaskType.constant, "****")
        rule = resolve(governed["masks"], ["analyst"], "integer")
        assert (rule.mask_type, rule.value) == (MaskType.constant, None)

    def test_a_mask_that_does_not_fit_the_type_masks_completely(self):
        """A rule on a tag reaches columns of any type: a regex on a number fails closed."""
        governed = govern_column(
            {"visible_to": ["analyst"], "masks": []}, [_mask("analyst", PARTIAL)], {}
        )
        rule = resolve(governed["masks"], ["analyst"], "integer")
        assert (rule.mask_type, rule.value) == (MaskType.constant, None)

    def test_no_rule_leaves_the_column_as_it_is(self):
        column = {"visible_to": ["analyst"], "masks": []}
        assert govern_column(column, [], CHAINS) is column


class TestStricterWins:
    def test_hidden_over_masked(self):
        """The column's own list masks analyst; a hide rule hides it."""
        own = [{"to": ["analyst"], "mask": {"type": "constant", "value": "x"}}]
        governed = govern_column(
            {"visible_to": ["analyst"], "masks": own}, [_hide("analyst")], CHAINS
        )
        assert not _served(governed, "analyst")

    def test_hidden_by_the_column_stays_hidden_under_a_mask_rule(self):
        governed = govern_column(
            {"visible_to": ["org_admin"], "masks": []}, [_mask("analyst", PARTIAL)], CHAINS
        )
        assert not _served(governed, "analyst")

    def test_masked_over_plain(self):
        """The column's own list leaves analyst unmasked; the rule masks it."""
        own = [{"to": ["analyst"], "mask": None}, {"mask": {"type": "constant", "value": "x"}}]
        governed = govern_column(
            {"visible_to": ["analyst"], "masks": own}, [_mask("analyst", PARTIAL)], CHAINS
        )
        assert resolve(governed["masks"], ["analyst"], "varchar").mask_type == MaskType.regex

    def test_the_column_own_mask_holds_over_the_rule(self):
        own = [{"to": ["analyst"], "mask": {"type": "constant", "value": "x"}}]
        governed = govern_column(
            {"visible_to": ["analyst"], "masks": own}, [_mask("analyst", PARTIAL)], CHAINS
        )
        rule = resolve(governed["masks"], ["analyst"], "varchar")
        assert (rule.mask_type, rule.value) == (MaskType.constant, "x")

    def test_a_floor_never_unmasks_a_reader_no_entry_of_the_column_holds(self):
        """REQ-2059: a reader no own entry holds reads the column completely masked, rule or no."""
        own = [{"to": ["org_admin"], "mask": None}]
        governed = govern_column(
            {"visible_to": ["analyst", "org_admin"], "masks": own},
            [_mask("analyst", PARTIAL)],
            CHAINS,
        )
        rule = resolve(governed["masks"], ["analyst"], "varchar")
        assert (rule.mask_type, rule.value) == (MaskType.constant, "****")
        assert resolve(governed["masks"], ["org_admin"], "varchar") is None

    def test_a_floor_masks_no_reader_the_rule_does_not_name(self):
        """Floor entries alone are no list of the column's own: other readers read in clear."""
        governed = govern_column(
            {"visible_to": ["analyst", "org_admin"], "masks": []},
            [_mask("analyst", PARTIAL)],
            CHAINS,
        )
        assert all(e[FLOOR] for e in governed["masks"])
        assert resolve(governed["masks"], ["org_admin"], "varchar") is None


class TestReach:
    def test_a_column_comes_under_the_rules_of_its_keys(self):
        rules = [
            _hide("analyst", (TAG, "pii"), 1),
            _mask("junior", None, (CLASSIFICATION, "email"), 2),
            _hide("analyst", (TAG, "finance"), 3),
        ]
        keys = {
            ("column", 7, "email"): {(TAG, "pii"), (CLASSIFICATION, "email")},
            ("command_column", "lookup", "ssn"): {(TAG, "pii")},
            ("column", 7, "id"): {(TAG, "other")},
        }
        reached = ruled_columns(keys, rules)
        assert [r.id for r in reached[("column", 7, "email")]] == [1, 2]
        assert [r.id for r in reached[("command_column", "lookup", "ssn")]] == [1]
        assert ("column", 7, "id") not in reached  # untagged: released

    def test_a_rule_on_a_base_tag_reaches_its_parameterized_assignments(self):
        assert set(tag_keys("entity:customer")) == {(TAG, "entity:customer"), (TAG, "entity")}
        assert tag_keys("pii") == ((TAG, "pii"),)

    def test_a_rule_is_keyed_on_exactly_one_of_a_tag_and_a_classification(self):
        with pytest.raises(ValidationError):
            ColumnRuleModel(role_id="analyst", action="hide")
        with pytest.raises(ValidationError):
            ColumnRuleModel(tag_id="pii", classification="email", role_id="a", action="hide")
        with pytest.raises(ValidationError):
            ColumnRuleModel(
                tag_id="pii", role_id="a", action="hide", mask={"type": "constant", "value": "x"}
            )
        assert ColumnRuleModel(classification="email", role_id="a", action="mask").mask is None


class TestTableColumns:
    def test_hide_narrows_a_table_column_grant(self):
        tables = [
            {
                "id": 7,
                "columns": [
                    {"column_name": "email", "visible_to": ["analyst", "junior", "org_admin"]},
                    {"column_name": "id", "visible_to": ["analyst", "junior", "org_admin"]},
                ],
            }
        ]
        hide_ruled_tables(tables, {("column", 7, "email"): (_hide("analyst"),)}, CHAINS)
        email, other = tables[0]["columns"]
        assert email["visible_to"] == ["org_admin"]
        assert other["visible_to"] == ["analyst", "junior", "org_admin"]

    def test_mask_floors_a_table_column_list(self):
        class _Col:
            def __init__(self, name: str, data_type: str) -> None:
                self.column_name, self.data_type = name, data_type

        own = [{"to": ["org_admin"], "mask": None}, {"mask": {"type": "constant", "value": "x"}}]
        masked = [(7, "ssn", own, "varchar")]
        ruled = {
            ("column", 7, "ssn"): (_mask("org_admin", PARTIAL),),
            ("column", 7, "email"): (_mask("analyst"),),
            ("command_column", "lookup", "ssn"): (_mask("analyst"),),
        }
        types = {7: [_Col("ssn", "varchar"), _Col("email", "varchar")]}
        out = {(t, c): (m, d) for t, c, m, d in _with_ruled_masks(masked, ruled, types, {})}
        assert set(out) == {(7, "ssn"), (7, "email")}  # a command column is not a table's
        ssn, _ = out[(7, "ssn")]
        assert resolve(ssn, ["org_admin"], "varchar").mask_type == MaskType.regex
        email, _ = out[(7, "email")]
        assert resolve(email, ["analyst"], "varchar").value == "****"
        assert resolve(email, ["org_admin"], "varchar") is None

    def test_a_column_faked_for_every_role_is_left_as_it_is(self):
        faked = {(7, "analyst"): {"ssn": object()}}
        out = _with_ruled_masks([], {("column", 7, "ssn"): (_mask("analyst"),)}, {7: []}, faked)
        assert out == []


@pytest.mark.asyncio
class TestCommandColumns:
    async def test_a_hide_rule_hides_a_command_output_column(self):
        st = _state()
        st.ruled_columns = {("command_column", "customer_lookup", "region"): (_hide("analyst"),)}
        rows, enf = await govern_action_rows(ROWS, FN, "junior", st)
        assert "region" not in rows[0]
        assert "region" in enf.columns_excluded
        rows, _ = await govern_action_rows(ROWS, FN, "org_admin", st)
        assert rows[0]["region"] == "east"

    async def test_a_mask_rule_masks_a_command_output_column(self):
        st = _state()
        st.ruled_columns = {("command_column", "customer_lookup", "region"): (_mask("analyst"),)}
        rows, enf = await govern_action_rows(ROWS, FN, "analyst", st)
        assert {r["region"] for r in rows} == {"****"}
        assert "region -> constant" in enf.masking_applied
        rows, _ = await govern_action_rows(ROWS, FN, "org_admin", st)
        assert rows[0]["region"] == "east"

    async def test_the_command_column_own_mask_holds_over_the_rule(self):
        """ssn is masked '***' for analyst by its own list; the rule's mask does not replace it."""
        st = _state()
        st.ruled_columns = {
            ("command_column", "customer_lookup", "ssn"): (
                _mask("analyst", {"type": "constant", "value": "RULE"}),
            )
        }
        rows, _ = await govern_action_rows(ROWS, FN, "analyst", st)
        assert {r["ssn"] for r in rows} == {"***"}
