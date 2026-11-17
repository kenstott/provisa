# Copyright (c) 2026 Kenneth Stott
# Canary: c61279d4-b0e3-41f6-830d-78325dd90635
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view applying a column transform (REQ-2131) in the one semantic statement: its outputs exist
only in its stored table, landed by its build. So, as a bitemporal view, it keeps its reference
through governance for every read (its own rules apply to it as the registered table it is), the
V-rules judge its definition for the reader, and at optimization it is answered from its stored
table -- whether or not its build is fresh. Its stored table put in before governance was refused
V006, a relation no reader names.

Amended 2026-11-16: the view is built once and stored, never recomputed per reader. The reader's
row rules on its inputs filter the stored rows, over the input columns the build keeps; an output
computed from a column masked for the reader is masked as that column is; a reader whose rules
narrow nothing reads the stored rows as they are; no transform is called on a read."""

# Requirements: REQ-2131
from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot
from sqlglot import exp

from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry
from provisa.mv.view_read import semantic_statement, unnarrowed_view_bodies

pytestmark = pytest.mark.unit

STORED = '"mat_store"."org_default_mv_cache"."mv_clean_suppliers"'
READ = f"SELECT * FROM {STORED}"
DEFINITION = "SELECT id, clean_address(raw_address) FROM fx.suppliers"
STATEMENT = "SELECT id, city FROM clean_suppliers WHERE city = 'Springfield 3'"


CLEAN_ADDRESS = {
    "name": "clean_address",
    "arguments": [{"name": "raw_address"}],
    "output_columns": [
        {"name": "street", "type": "text"},
        {"name": "city", "type": "text"},
        {"name": "country", "type": "text"},
    ],
}


def _state(
    status: MVStatus, rules: dict | None = None, definition: str = DEFINITION
) -> SimpleNamespace:
    registry = MVRegistry()
    registry.register(
        MVDefinition(
            id="view-clean_suppliers",
            source_tables=[],
            target_catalog="mat_store",
            target_schema="org_default_mv_cache",
            target_table="mv_clean_suppliers",
            sql=definition,
            semantic_sql=definition,
            status=status,
            view_table="clean_suppliers",
        )
    )
    return SimpleNamespace(
        mv_registry=registry,
        stored_view_reads={"clean_suppliers"},
        view_sql_map={"clean_suppliers": READ},
        column_transforms={"clean_address": CLEAN_ADDRESS},
        role_build_inputs={
            "withdrawn_tables": [],  # no table is out of service
            # Every role's row rules, as stored: what the build keeps the inputs of.
            "rls_rules": [
                {"role_id": "east_reader", "table_id": table_id, "filter_expr": rule}
                for table_id, rule in (rules or {}).items()
            ],
            "tables": [
                {"id": 1, "source_id": "fx", "domain_id": "fx"},
                {"id": 2, "source_id": "fx", "domain_id": "fx"},
            ],
        },
    )


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"fx.suppliers": 1, "suppliers": 1, "clean_suppliers": 2}
    return gov


def _relations(sql: str) -> set[str]:
    return {
        ".".join(p for p in (t.catalog, t.db, t.name) if p)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    }


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_transform_view_keeps_its_reference_through_governance(status):
    governed, validated = semantic_statement(
        STATEMENT, {"clean_suppliers": READ}, _state(status), _gov()
    )
    # Governance judges the view as the registered table it is, its own rules on top.
    assert governed == STATEMENT
    # The V-rules judge what the view reads -- its definition -- never its stored table.
    assert _relations(validated) == {"fx.suppliers"}


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_transform_view_is_answered_from_its_stored_table(status):
    bodies = unnarrowed_view_bodies(STATEMENT, {"clean_suppliers": READ}, _state(status))
    assert bodies == {"clean_suppliers": READ}


# --- REQ-2131 (amended 2026-11-16): the stored rows read under the reader's rules ----------------

HELD = '"clean_suppliers"."__rule_input_suppliers__region"'


def _east():
    from provisa.compiler.rls import read_rule

    return read_rule("region = 'east'")


def _answered(statement: str, state: SimpleNamespace, gov: GovernanceContext) -> str:
    """The statement as it runs for the reader: governed, read under the reader's rules on the
    transform view, then each view it still references put in at optimization."""
    from provisa.compiler.view_expand import expand_view_refs
    from provisa.mv.view_read import read_stored_views

    governed, _ = semantic_statement(statement, {"clean_suppliers": READ}, state, gov)
    governed = read_stored_views(governed, state, gov)
    bodies = unnarrowed_view_bodies(governed, {"clean_suppliers": READ}, state)
    return expand_view_refs(governed, bodies)


def _reader(*, row_rule: bool = False, masked: tuple[str, ...] = ()) -> GovernanceContext:
    from provisa.security.masking import MaskingRule, MaskType

    gov = _gov()
    gov.all_columns = {
        1: [("id", "integer"), ("raw_address", "varchar"), ("region", "varchar")],
        2: [("id", "integer"), ("street", "text"), ("city", "text"), ("country", "text")],
    }
    if row_rule:
        gov.rls_rules = {1: _east()}
    for column in masked:
        gov.masking_rules[(1, column)] = (MaskingRule(MaskType.constant, value="***"), "varchar")
    return gov


def _items(sql: str) -> dict[str, str]:
    """The reader's read of the stored rows: its projection, by output name."""

    def reads_stored(select: exp.Select) -> bool:
        inner = select.args["from_"].this
        return (
            isinstance(inner, exp.Subquery)
            and isinstance(inner.this, exp.Select)
            and inner.this.args["from_"].this.name == "mv_clean_suppliers"
        )

    tree = sqlglot.parse_one(sql, read="postgres")
    (read,) = [s for s in tree.find_all(exp.Select) if s.args.get("from_") and reads_stored(s)]
    return {e.alias_or_name: e.this.sql(dialect="postgres") for e in read.expressions}


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_row_rule_on_an_input_that_is_no_output_column_filters_the_stored_rows(status):
    """region is no column of the view; the reader's rule on it filters the stored rows over the
    input value the build kept."""
    answered = _answered(STATEMENT, _state(status, {1: _east()}), _reader(row_rule=True))
    assert _relations(answered) == {"mat_store.org_default_mv_cache.mv_clean_suppliers"}
    assert f"WHERE {HELD} = 'east'" in answered
    assert "clean_address" not in answered  # no transform is called on a read


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_masked_input_masks_the_outputs_derived_from_it_and_no_other(status):
    answered = _answered(STATEMENT, _state(status), _reader(masked=("raw_address",)))
    items = _items(answered)
    assert {items[c] for c in ("street", "city", "country")} == {"'***'"}
    assert items["id"] == '"clean_suppliers"."id"'
    assert "__rule_input" not in answered  # no row rule: nothing filters the stored rows
    assert "clean_address" not in answered


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_mask_on_a_plain_input_masks_its_own_output_only(status):
    items = _items(_answered(STATEMENT, _state(status), _reader(masked=("id",))))
    assert items["id"] == "'***'"
    assert {items[c] for c in ("street", "city", "country")} == {
        f'"clean_suppliers"."{c}"' for c in ("street", "city", "country")
    }


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_reader_whose_rules_narrow_nothing_reads_the_stored_rows_as_they_are(status):
    """Another role's rule makes the build keep region; this reader has none, so it reads the
    stored rows as they are."""
    answered = _answered(STATEMENT, _state(status, {1: _east()}), _reader())
    # The statement only has the view put in at optimization, as for an administrator.
    assert answered == f"SELECT id, city FROM ({READ}) clean_suppliers WHERE city = 'Springfield 3'"


def test_rows_and_masks_hold_together():
    answered = _answered(
        STATEMENT,
        _state(MVStatus.FRESH, {1: _east()}),
        _reader(row_rule=True, masked=("raw_address",)),
    )
    assert f"WHERE {HELD} = 'east'" in answered
    assert _items(answered)["city"] == "'***'"


def test_the_build_keeps_the_inputs_every_roles_row_rules_read():
    from provisa.compiler.rls import read_rule
    from provisa.mv.transform_view_rules import kept_inputs, roles_rules, with_rule_inputs

    state = _state(MVStatus.FRESH, {1: _east()})
    state.role_build_inputs["rls_rules"].append(
        {
            "role_id": "west_reader",
            "table_id": 1,
            "filter_expr": read_rule(
                "id > current_setting('provisa.min_id') AND region IN (SELECT r FROM allowed)"
            ),
        }
    )
    kept = kept_inputs(DEFINITION, _gov().table_map, roles_rules(state))
    # A column of a subquery in a rule is the subquery's, not the input's.
    assert kept == [("suppliers", 1, "id"), ("suppliers", 1, "region")]
    built = with_rule_inputs(DEFINITION, kept)
    assert built == (
        'SELECT id, clean_address(raw_address), "suppliers"."id" AS "__rule_input_suppliers__id", '
        '"suppliers"."region" AS "__rule_input_suppliers__region" FROM fx.suppliers'
    )


def test_the_build_keeps_the_inputs_a_domain_rule_on_an_input_reads():
    """A meta-role's rule on a table folds in a member's domain rule (security.meta_role), so
    the build keeps the columns a domain rule on an input's domain reads."""
    from provisa.compiler.rls import read_rule
    from provisa.mv.transform_view_rules import kept_inputs, roles_rules

    state = _state(MVStatus.FRESH)
    state.role_build_inputs["rls_rules"].append(
        {"role_id": "fx_member", "domain_id": "fx", "filter_expr": read_rule("raw_address <> ''")}
    )
    kept = kept_inputs(DEFINITION, _gov().table_map, roles_rules(state))
    assert kept == [("suppliers", 1, "raw_address")]


def test_a_session_variable_in_the_readers_rule_is_cast_to_the_kept_columns_type():
    from provisa.compiler.rls import read_rule

    rule = read_rule("id = current_setting('provisa.supplier_id')")
    gov = _reader()
    gov.rls_rules = {1: rule}
    answered = _answered(STATEMENT, _state(MVStatus.FRESH, {1: rule}), gov)
    assert (
        'WHERE "clean_suppliers"."__rule_input_suppliers__id" = '
        "CAST(CURRENT_SETTING('provisa.supplier_id') AS INT)"
    ) in answered


def test_a_build_folding_its_inputs_together_keeps_none_and_a_row_rule_reader_is_refused():
    """Grouped rows carry no input row's values, so a reader's row rule cannot filter them: it is
    refused naming the view, never served the stored rows."""
    grouped = "SELECT clean_address(raw_address) FROM fx.suppliers GROUP BY raw_address"
    from provisa.mv.transform_view_rules import kept_inputs, roles_rules

    state = _state(MVStatus.FRESH, {1: _east()}, definition=grouped)
    assert kept_inputs(grouped, _gov().table_map, roles_rules(state)) == []
    with pytest.raises(PermissionError, match="fx.suppliers"):
        _answered(STATEMENT, state, _reader(row_rule=True))


def test_a_row_rule_on_a_table_read_inside_a_derived_table_is_refused():
    nested = "SELECT s.id, clean_address(s.raw_address) FROM (SELECT * FROM fx.suppliers) AS s"
    state = _state(MVStatus.FRESH, {1: _east()}, definition=nested)
    with pytest.raises(PermissionError, match="cannot filter view 'clean_suppliers'"):
        _answered(STATEMENT, state, _reader(row_rule=True))


def test_lineage_follows_a_derived_table_to_the_masked_input():
    nested = "SELECT s.id, clean_address(s.raw) FROM (SELECT id, raw_address AS raw FROM fx.suppliers) AS s"
    items = _items(
        _answered(
            STATEMENT, _state(MVStatus.FRESH, definition=nested), _reader(masked=("raw_address",))
        )
    )
    assert items["city"] == "'***'" and items["id"] == '"clean_suppliers"."id"'


def test_a_rebuild_reads_the_view_with_its_kept_inputs(monkeypatch):
    """The build's statement is the definition with the kept inputs: governed_build hands the one
    pipeline that, as the region's administrator."""
    from provisa.mv.governed_build import _with_kept_inputs

    state = _state(MVStatus.FRESH, {1: _east()})
    state.view_context = SimpleNamespace(tables={})
    mv = state.mv_registry.get("view-clean_suppliers")
    monkeypatch.setattr("provisa.compiler.stage2.context_table_map", lambda _ctx: _gov().table_map)
    built = _with_kept_inputs(state, mv, DEFINITION)
    assert built.endswith(
        '"suppliers"."region" AS "__rule_input_suppliers__region" FROM fx.suppliers'
    )
