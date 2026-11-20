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
narrow nothing reads the stored rows as they are; no transform is called on a read.

Amended 2026-11-16: a view calling a code command (REQ-2249) is read under the same rule; the
kept columns a read filters over are those its stored table holds as built, so a row rule changed
since the build is refused by name until the build that follows the change has published, and
that change builds the view again."""

# Requirements: REQ-2131, REQ-2249
from __future__ import annotations

import asyncio
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


class _Store:
    """The engine a stored view's table is read through: it answers a probe of the stored table
    with the columns it was built with, and counts the probes."""

    def __init__(self, columns: list[str] | None) -> None:
        self.columns = columns
        self.probes: list[str] = []

    async def execute_engine(self, sql: str, authorization=None):
        del authorization
        self.probes.append(sql)
        if self.columns is None:
            raise RuntimeError("no such table")
        return SimpleNamespace(column_names=list(self.columns))


#: Each column a view below gives, as its registered table (id 2) has it.
_OUTPUTS = ["id", "street", "city", "country"]


def _state(
    status: MVStatus,
    rules: dict | None = None,
    definition: str = DEFINITION,
    stored: list[str] | None = None,
) -> SimpleNamespace:
    """The stored view, built (``stored`` its kept input columns; by default those the
    ``rules`` name, as a build under them keeps)."""
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
    state = SimpleNamespace(
        mv_registry=registry,
        source_catalogs={},  # the default org: its views lowered under __derived__
        stored_view_reads={"fx.clean_suppliers"},
        view_sql_map={"fx.clean_suppliers": READ},
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
        tracked_functions={"rank_suppliers": RANK_SUPPLIERS},
        relationships=[],
    )
    if stored is None:
        from provisa.mv.transform_view_rules import kept_inputs, roles_rules, rule_input

        kept = kept_inputs(definition, _gov().table_map, roles_rules(state))
        stored = [rule_input(alias, column) for alias, _, column in kept]
    state.federation_engine = _Store([*_OUTPUTS, *stored])
    return state


#: A code command (REQ-2249) a view may call: it ranks the suppliers it is given.
RANK_SUPPLIERS = {
    "name": "rank_suppliers",
    "impl_kind": "code_command",
    "kind": "query",
    "arguments": [{"name": "suppliers", "arg_kind": "table_ref"}],
    "output_columns": [
        {"name": "supplier_id", "type": "integer"},
        {"name": "score", "type": "double"},
    ],
}


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {
        "fx.suppliers": 1,
        "suppliers": 1,
        "fx.clean_suppliers": 2,
        "clean_suppliers": 2,
    }
    return gov


def _relations(sql: str) -> set[str]:
    return {
        ".".join(p for p in (t.catalog, t.db, t.name) if p)
        for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table)
    }


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_transform_view_keeps_its_reference_through_governance(status):
    governed, validated = semantic_statement(
        STATEMENT, {"fx.clean_suppliers": READ}, _state(status), _gov()
    )
    # Governance judges the view as the registered table it is, its own rules on top.
    assert governed == STATEMENT
    # The V-rules judge what the view reads -- its definition -- never its stored table.
    assert _relations(validated) == {"fx.suppliers"}


@pytest.mark.parametrize("status", [MVStatus.FRESH, MVStatus.STALE])
def test_a_transform_view_is_answered_from_its_stored_table(status):
    bodies = unnarrowed_view_bodies(STATEMENT, {"fx.clean_suppliers": READ}, _state(status))
    assert bodies == {"fx.clean_suppliers": READ}


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

    governed, _ = semantic_statement(statement, {"fx.clean_suppliers": READ}, state, gov)
    governed = asyncio.run(read_stored_views(governed, state, gov))
    bodies = unnarrowed_view_bodies(governed, {"fx.clean_suppliers": READ}, state)
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
    assert (
        answered
        == f"SELECT id, city FROM ({READ}) \"clean_suppliers\" WHERE city = 'Springfield 3'"
    )


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


# --- REQ-2249: a view calling a code command is read under the same rule ---------------------------

RANKED = (
    "SELECT s.id, r.score FROM rank_suppliers('fx.suppliers') AS r "
    "JOIN fx.suppliers AS s ON s.id = r.supplier_id"
)
RANKED_STATEMENT = "SELECT id, score FROM clean_suppliers"


def _ranked_reader(*, row_rule: bool = False, masked: tuple[str, ...] = ()) -> GovernanceContext:
    gov = _reader(row_rule=row_rule, masked=masked)
    gov.all_columns[2] = [("id", "integer"), ("score", "double")]
    return gov


def test_a_code_command_view_read_by_a_restricted_role_gets_only_its_rows_masked():
    """The reader's row rule on the input the view joins filters the stored rows over the kept
    column; the command's output is computed from every column of the table it is given, so a
    mask on any of them masks it, while id (from the joined input) stays clear."""
    state = _state(MVStatus.FRESH, {1: _east()}, definition=RANKED)
    answered = _answered(
        RANKED_STATEMENT, state, _ranked_reader(row_rule=True, masked=("raw_address",))
    )
    assert f"WHERE {HELD.replace('suppliers__', 's__')} = 'east'" in answered
    items = _items(answered)
    assert items["score"] == "'***'"
    assert items["id"] == '"clean_suppliers"."id"'
    assert "rank_suppliers" not in answered  # no command is called on a read


def test_the_build_of_a_code_command_view_keeps_its_joined_inputs_rule_columns(monkeypatch):
    from provisa.mv.governed_build import _with_kept_inputs

    state = _state(MVStatus.FRESH, {1: _east()}, definition=RANKED)
    state.view_context = SimpleNamespace(tables={})
    mv = state.mv_registry.get("view-clean_suppliers")
    monkeypatch.setattr("provisa.compiler.stage2.context_table_map", lambda _ctx: _gov().table_map)
    built = _with_kept_inputs(state, mv, RANKED)
    assert '"s"."region" AS "__rule_input_s__region"' in built


def test_a_row_rule_on_a_table_only_a_code_command_is_given_is_refused():
    """The command's rows carry no row of the table it is given: a reader's row rule on it
    cannot filter them, so the read is refused naming the table, never served unfiltered."""
    called_only = "SELECT r.supplier_id AS id, r.score FROM rank_suppliers('fx.suppliers') AS r"
    state = _state(MVStatus.FRESH, {1: _east()}, definition=called_only)
    with pytest.raises(PermissionError, match="fx.suppliers"):
        _answered(RANKED_STATEMENT, state, _ranked_reader(row_rule=True))


def test_a_mask_on_a_table_a_code_command_is_given_masks_its_outputs():
    called_only = "SELECT r.supplier_id AS id, r.score FROM rank_suppliers('fx.suppliers') AS r"
    state = _state(MVStatus.FRESH, definition=called_only)
    items = _items(_answered(RANKED_STATEMENT, state, _ranked_reader(masked=("region",))))
    assert items == {"id": "'***'", "score": "'***'"}


# --- REQ-2131 (amended 2026-11-16): a rule change builds the view again --------------------------

RAW_RULE = "raw_address LIKE '%springfield 3,%'"


def _changed(state: SimpleNamespace) -> GovernanceContext:
    """east_reader's rule changed to one on raw_address, a column the stored rows do not keep."""
    from provisa.compiler.rls import read_rule

    state.role_build_inputs = {
        **state.role_build_inputs,
        "rls_rules": [
            {"role_id": "east_reader", "table_id": 1, "filter_expr": read_rule(RAW_RULE)}
        ],
    }
    gov = _reader()
    gov.rls_rules = {1: read_rule(RAW_RULE)}
    return gov


def test_a_rule_on_a_column_the_stored_rows_do_not_keep_is_refused_until_the_rebuild():
    from provisa.mv.transform_view_rules import ViewAwaitsRuleRebuild

    state = _state(MVStatus.FRESH, {1: _east()})  # built keeping region
    gov = _changed(state)
    with pytest.raises(ViewAwaitsRuleRebuild) as refused:
        _answered(STATEMENT, state, gov)
    assert refused.value.code == "data.view_awaits_rule_rebuild"
    assert refused.value.params == {"view": "clean_suppliers"}

    # The build that follows the change publishes, keeping raw_address.
    state.federation_engine.columns = [*_OUTPUTS, "__rule_input_suppliers__raw_address"]
    state.mv_registry.mark_refreshed("view-clean_suppliers", 70)
    answered = _answered(STATEMENT, state, gov)
    assert (
        'WHERE "clean_suppliers"."__rule_input_suppliers__raw_address" LIKE \'%springfield 3,%\''
        in answered
    )


def test_a_reader_the_change_does_not_touch_keeps_reading_and_nothing_is_probed():
    state = _state(MVStatus.FRESH, {1: _east()})
    _changed(state)
    answered = _answered(STATEMENT, state, _reader())
    assert (
        answered
        == f"SELECT id, city FROM ({READ}) \"clean_suppliers\" WHERE city = 'Springfield 3'"
    )
    assert state.federation_engine.probes == []


def test_a_reader_whose_rule_the_stored_rows_keep_keeps_reading_after_another_rule_changed():
    from provisa.compiler.rls import read_rule

    state = _state(MVStatus.FRESH, {1: _east()})  # built keeping region
    state.role_build_inputs = {
        **state.role_build_inputs,
        "rls_rules": [
            *state.role_build_inputs["rls_rules"],
            {"role_id": "west_reader", "table_id": 1, "filter_expr": read_rule(RAW_RULE)},
        ],
    }
    answered = _answered(STATEMENT, state, _reader(row_rule=True))
    assert f"WHERE {HELD} = 'east'" in answered


def test_a_view_not_built_yet_refuses_a_row_rule_reader_by_name():
    from provisa.mv.transform_view_rules import ViewAwaitsRuleRebuild

    state = _state(MVStatus.STALE, {1: _east()})
    state.federation_engine.columns = None
    with pytest.raises(ViewAwaitsRuleRebuild):
        _answered(STATEMENT, state, _reader(row_rule=True))


def test_a_rule_change_builds_each_stored_view_whose_kept_inputs_it_changed(monkeypatch):
    from provisa.mv import transform_view_rules

    state = _state(MVStatus.FRESH, {1: _east()})
    state.view_context = SimpleNamespace(tables={})
    state.model_db = state.tenant_db = None
    monkeypatch.setattr("provisa.compiler.stage2.context_table_map", lambda _ctx: _gov().table_map)
    started: list[str] = []

    def spawn(coro, *, name):
        coro.close()
        started.append(name)

    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", spawn)
    # Built under the rules in force: nothing to build again.
    assert asyncio.run(transform_view_rules.rebuild_for_rules(state)) == []
    _changed(state)
    assert asyncio.run(transform_view_rules.rebuild_for_rules(state)) == ["view-clean_suppliers"]
    assert started == ["rule-change build of view-clean_suppliers"]
    # Published: the stored table keeps what the rules now name.
    state.federation_engine.columns = [*_OUTPUTS, "__rule_input_suppliers__raw_address"]
    state.mv_registry.mark_refreshed("view-clean_suppliers", 70)
    assert asyncio.run(transform_view_rules.rebuild_for_rules(state)) == []


# --- REQ-2131 (amended b0823d303a): a bridge is filtered through its relationship to each input ---

CALLED_ONLY = "SELECT r.supplier_id AS id, r.score FROM rank_suppliers('fx.suppliers') AS r"

#: suppliers.id to the view's id: the relationship a code resolver's set registers.
SUPPLIER_TO_VIEW = {
    "source_table_id": 1,
    "target_table_id": 2,
    "source_column": "id",
    "target_column": "id",
    "via_table_id": None,
    "target_function_name": None,
}


def test_a_code_command_views_rows_are_filtered_through_its_relationship_to_the_input():
    """The rows the command's call returns carry no supplier's values, but the view's id is
    suppliers.id by a registered relationship: the reader's rule on suppliers holds through it."""
    state = _state(MVStatus.FRESH, {1: _east()}, definition=CALLED_ONLY)
    state.relationships = [SUPPLIER_TO_VIEW]
    answered = _answered(RANKED_STATEMENT, state, _ranked_reader(row_rule=True))
    assert (
        'WHERE EXISTS(SELECT 1 FROM fx.suppliers AS __rule_1 WHERE "__rule_1"."id" = '
        '"clean_suppliers"."id" AND "__rule_1"."region" = \'east\')'
    ) in answered
    assert "rank_suppliers" not in answered
    assert state.federation_engine.probes == []  # no kept column is read


def test_a_key_output_is_masked_only_by_a_mask_on_its_key_column():
    """id is the view's key to suppliers.id: a mask on another suppliers column masks score,
    which the command computed from the whole table, and leaves id in clear."""
    state = _state(MVStatus.FRESH, definition=CALLED_ONLY)
    state.relationships = [SUPPLIER_TO_VIEW]
    items = _items(_answered(RANKED_STATEMENT, state, _ranked_reader(masked=("region",))))
    assert items == {"id": '"clean_suppliers"."id"', "score": "'***'"}
    items = _items(_answered(RANKED_STATEMENT, state, _ranked_reader(masked=("id",))))
    assert items == {"id": "'***'", "score": "'***'"}


def test_a_junction_edge_through_the_view_is_no_relationship_to_filter_by():
    state = _state(MVStatus.FRESH, {1: _east()}, definition=CALLED_ONLY)
    state.relationships = [{**SUPPLIER_TO_VIEW, "target_table_id": 1, "via_table_id": 2}]
    with pytest.raises(PermissionError, match="fx.suppliers"):
        _answered(RANKED_STATEMENT, state, _ranked_reader(row_rule=True))


# --- a build lands its transformed rows in a table of its own shape -------------------------------


def test_a_build_of_another_shape_lands_beside_the_last_never_into_it(monkeypatch):
    """The landing creates a table only where none is, and the last build's stays until its drop:
    a rule that keeps another input changes the build's columns, so its rows go to a table of
    their own (a fixed name failed the rebuild: the old table lacked the new kept column)."""
    import contextlib

    from provisa.api_source import engine_cache
    from provisa.mv import governed_build

    landed: list[tuple[str, list]] = []

    class _Engine:
        dialect = "duckdb"

        @contextlib.contextmanager
        def isolated_sync(self):
            yield None

        async def land_source_table(self, *, schema, table, columns, rows):
            del schema, rows
            landed.append((table, columns))

    async def _execute(_plan):
        return SimpleNamespace(column_names=["id"], rows=[(1,)])

    shapes = iter([[("id", "integer")], [("id", "integer"), ("__rule_input_s__id", "integer")]])

    async def _typed(_mv, _plan):
        return next(shapes)

    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    monkeypatch.setattr(governed_build, "_typed_columns", _typed)
    monkeypatch.setattr(engine_cache, "resolved_cache_catalog", lambda _e: "mat_store")
    monkeypatch.setattr(engine_cache, "org_cache_schema", lambda _s: "org_default_api_cache")
    monkeypatch.setattr(engine_cache, "ensure_cache_schema", lambda _c, _l: None)
    monkeypatch.setattr(engine_cache, "schedule_drop", lambda *_a: None)
    mv = _state(MVStatus.FRESH).mv_registry.get("view-clean_suppliers")
    for _ in range(2):
        asyncio.run(governed_build._transformed_rows_sql(SimpleNamespace(), mv, None, _Engine()))
    (first, _), (second, _) = landed
    assert first.startswith("mv_columns_view-clean_suppliers_") and first != second
