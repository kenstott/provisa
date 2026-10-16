# Copyright (c) 2026 Kenneth Stott
# Canary: 70567724-20b4-4514-8eb2-59bd1efefb46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A join on a view's column relates the tables along the view's registered relationship, also
where the view's SQL gives that column as a constant (REQ-603).

The view ``evokes`` gives each line it reads with the theme it evokes, as a constant
(``'time' AS theme_id``); ``evokes.theme_id = theme.id`` is registered. The statement governance
validates has every view expanded, so before this the constant left ``theme`` joined to nothing
and the reader was refused V002 for a cross join. A derived table a caller writes is no view, and
gains nothing from this."""

# Requirements: REQ-603

from __future__ import annotations

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.compiler.view_expand import VIEW_MARK, expand_view_refs

_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["*"]}
_COLUMNS = {
    1: ["id", "text"],  # line
    2: ["id", "name"],  # theme
    3: ["line_id", "theme_id"],  # evokes, a view
}
_EVOKES = "SELECT l.id AS line_id, 'time' AS theme_id FROM line l"
_READ = "SELECT t.name, e.line_id FROM evokes e JOIN theme t ON t.id = e.theme_id"


def _meta(table_id: int, name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=name,
        type_name=name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=name,
        domain_id="sonnets",
        source_type="postgresql",
    )


def _join(target: TableMeta, source_column: str, target_column: str) -> JoinMeta:
    return JoinMeta(
        source_column=source_column,
        target_column=target_column,
        source_column_type="varchar",
        target_column_type="varchar",
        target=target,
        cardinality="many-to-one",
    )


def _v002(sql: str, *, related: bool = True) -> list[str]:
    """V002 for ``sql``, its views expanded as governance validates them. ``related``: whether
    ``evokes.theme_id = theme.id`` and ``evokes.line_id = line.id`` are registered."""
    line, theme, evokes = _meta(1, "line"), _meta(2, "theme"), _meta(3, "evokes")
    ctx = CompilationContext()
    ctx.tables = {"line": line, "theme": theme, "evokes": evokes}
    if related:
        ctx.joins = {
            ("Evokes", "theme"): _join(theme, "theme_id", "id"),
            ("Evokes", "line"): _join(line, "line_id", "id"),
        }
    gov = GovernanceContext()
    gov.table_map = {"line": 1, "theme": 2, "evokes": 3}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in _COLUMNS.items()}
    validated = expand_view_refs(sql, {"evokes": _EVOKES}, mark=True)
    return [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]


def test_a_join_on_a_constant_a_view_gives_relates_along_the_view_s_relationship():
    assert _v002(_READ) == []


def test_it_still_needs_the_view_s_relationship():
    # Refused for the join it makes; which further unrelated pair is also named depends on the
    # order the statement's table instances are compared in.
    assert (
        "Invalid JOIN: theme.id = evokes.theme_id — no approved relationship exists between "
        "these tables on these columns"
    ) in _v002(_READ, related=False)


def test_a_derived_table_a_caller_writes_is_no_view():
    written = (
        "SELECT t.name FROM (SELECT l.id AS line_id, 'time' AS theme_id FROM line l) e "
        "JOIN theme t ON t.id = e.theme_id"
    )
    assert any("no condition relating them" in m for m in _v002(written))


def test_a_mark_the_process_did_not_make_is_no_view():
    forged = (
        "SELECT t.name FROM (SELECT * FROM (SELECT l.id AS line_id, 'time' AS theme_id "
        'FROM line l) AS "__view_0000000000000000__evokes") e JOIN theme t ON t.id = e.theme_id'
    )
    assert not forged.count(VIEW_MARK)
    assert any("no condition relating them" in m for m in _v002(forged))


def test_a_cross_join_inside_the_view_is_still_refused():
    inside = "SELECT l.id AS line_id, 'time' AS theme_id FROM line l, theme t2"
    line, theme, evokes = _meta(1, "line"), _meta(2, "theme"), _meta(3, "evokes")
    ctx = CompilationContext()
    ctx.tables = {"line": line, "theme": theme, "evokes": evokes}
    ctx.joins = {("Evokes", "theme"): _join(theme, "theme_id", "id")}
    gov = GovernanceContext()
    gov.table_map = {"line": 1, "theme": 2, "evokes": 3}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in _COLUMNS.items()}
    validated = expand_view_refs(_READ, {"evokes": inside}, mark=True)
    messages = [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]
    assert any("line and theme are combined with no condition" in m for m in messages), messages


def test_a_constant_a_view_reads_from_the_views_it_reads_is_its_own():
    """``evokes`` is the union of two theme views, each giving its theme as a constant: the
    join on ``evokes.theme_id`` is along ``evokes``' relationship."""
    line, theme, evokes = _meta(1, "line"), _meta(2, "theme"), _meta(3, "evokes")
    time_view, beauty_view = _meta(4, "theme_time"), _meta(5, "theme_beauty")
    ctx = CompilationContext()
    ctx.tables = {
        "line": line,
        "theme": theme,
        "evokes": evokes,
        "theme_time": time_view,
        "theme_beauty": beauty_view,
    }
    ctx.joins = {
        ("Evokes", "theme"): _join(theme, "theme_id", "id"),
        ("Evokes", "line"): _join(line, "line_id", "id"),
    }
    gov = GovernanceContext()
    gov.table_map = {"line": 1, "theme": 2, "evokes": 3, "theme_time": 4, "theme_beauty": 5}
    columns = {**_COLUMNS, 4: _COLUMNS[3], 5: _COLUMNS[3]}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in columns.items()}
    views = {
        # Three branches: sqlglot nests them as a union of a union.
        "evokes": "SELECT line_id, theme_id FROM theme_time "
        "UNION ALL SELECT line_id, theme_id FROM theme_beauty "
        "UNION ALL SELECT line_id, theme_id FROM theme_time",
        "theme_time": "SELECT l.id AS line_id, 'time' AS theme_id FROM line l",
        "theme_beauty": "SELECT l.id AS line_id, 'beauty' AS theme_id FROM line l",
    }
    validated = expand_view_refs(_READ, views, mark=True)
    messages = [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]
    assert messages == []


def test_the_statement_governance_validates_reads_each_view_as_itself():
    """Through view_read.semantic_statement, as the pipeline calls it: views the statement reads
    are expanded twice there (governed, then validated), and both expansions are marked."""
    from types import SimpleNamespace

    from provisa.mv.registry import MVRegistry
    from provisa.mv.view_read import semantic_statement

    line, theme, evokes = _meta(1, "line"), _meta(2, "theme"), _meta(3, "evokes")
    time_view = _meta(4, "theme_time")
    ctx = CompilationContext()
    ctx.tables = {"line": line, "theme": theme, "evokes": evokes, "theme_time": time_view}
    ctx.joins = {
        ("Evokes", "theme"): _join(theme, "theme_id", "id"),
        ("Evokes", "line"): _join(line, "line_id", "id"),
    }
    gov = GovernanceContext()
    gov.table_map = {"line": 1, "theme": 2, "evokes": 3, "theme_time": 4}
    columns = {**_COLUMNS, 4: _COLUMNS[3]}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in columns.items()}
    views = {
        "evokes": "SELECT line_id, theme_id FROM theme_time",
        "theme_time": "SELECT l.id AS line_id, 'time' AS theme_id FROM line l",
    }
    state = SimpleNamespace(mv_registry=MVRegistry(), role_build_inputs={"draft_tables": []})
    _governed, validated = semantic_statement(_READ, views, state, gov)
    messages = [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]
    assert messages == []


def test_a_filter_on_a_value_computed_over_a_union_view_relates_nothing():
    """``known`` is the union of two tables. Its latest row per key, picked by a filter on a
    window computed over it, reads one row of the view at a time: the filter touches the view and
    what its SQL reads, which are the same row, and relates no two tables."""
    line, theme, known = _meta(1, "line"), _meta(2, "theme"), _meta(3, "known")
    ctx = CompilationContext()
    ctx.tables = {"line": line, "theme": theme, "known": known}
    gov = GovernanceContext()
    gov.table_map = {"line": 1, "theme": 2, "known": 3}
    columns = {1: ["id", "text"], 2: ["id", "name"], 3: ["id", "text"]}
    gov.all_columns = {tid: [(c, "varchar") for c in cols] for tid, cols in columns.items()}
    views = {"known": "SELECT id, text FROM line UNION ALL SELECT id, name AS text FROM theme"}
    read = (
        "SELECT id, text FROM (SELECT k.*, ROW_NUMBER() OVER (PARTITION BY id ORDER BY text DESC) "
        "AS latest FROM known AS k) AS x WHERE latest = 1"
    )
    validated = expand_view_refs(read, views, mark=True)
    messages = [v.message for v in validate_sql(validated, ctx, gov, _ROLE, []) if v.code == "V002"]
    assert messages == []
