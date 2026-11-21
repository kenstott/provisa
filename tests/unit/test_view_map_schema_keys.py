# Copyright (c) 2026 Kenneth Stott
# Canary: 7c41e2a9-0d3b-4f6e-9a58-b2e1c4d7f830
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view reference is the view only under the view's own schema: the view map keys every view
by ``schema.table``, and a reference under another schema is whatever that schema holds -- never
the view's SQL. An unqualified name is the view's where no other view has it, as the statement's
own qualification reads it. The same on the marked (parsed) and the unmarked (text) expansion."""

# Requirements: REQ-135, REQ-603

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.compiler.view_expand import VIEW_MARK, expand_view_refs, unqualified_keys
from provisa.compiler.stage2 import GovernanceContext
from provisa.mv.view_read import statement_as_read, view_names, views_read

pytestmark = pytest.mark.unit

_BODY = "SELECT secret FROM vault.keys"
_VIEWS = {"views.v1": _BODY}


@pytest.mark.parametrize("mark", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        'SELECT * FROM "__derived__"."otherschema"."v1"',
        'SELECT * FROM "otherschema"."v1"',
        "SELECT * FROM otherschema.v1",
        "SELECT * FROM otherschema.v1 AS x",
        'SELECT * FROM "views"."V1"',
    ],
)
def test_a_view_under_another_schema_is_not_expanded(sql: str, mark: bool) -> None:
    out = expand_view_refs(sql, _VIEWS, mark=mark)
    assert "vault" not in out
    assert VIEW_MARK not in out


@pytest.mark.parametrize("mark", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        'SELECT * FROM "__derived__"."views"."v1"',
        'SELECT * FROM "views"."v1"',
        "SELECT * FROM views.v1",
        "SELECT * FROM v1",
        "SELECT * FROM v1 AS x",
    ],
)
def test_a_view_under_its_own_schema_or_unqualified_is_expanded(sql: str, mark: bool) -> None:
    assert "vault.keys" in expand_view_refs(sql, _VIEWS, mark=mark)


@pytest.mark.parametrize("mark", [False, True])
def test_an_unqualified_name_two_views_share_is_neither(mark: bool) -> None:
    views = {"views.v1": _BODY, "other.v1": "SELECT 2 AS n"}
    out = expand_view_refs("SELECT * FROM v1", views, mark=mark)
    assert out.count("vault") == 0 and "SELECT 2" not in out
    # Qualified, each is its own.
    assert "vault" in expand_view_refs("SELECT * FROM views.v1", views, mark=mark)
    assert "SELECT 2" in expand_view_refs("SELECT * FROM other.v1", views, mark=mark)


@pytest.mark.parametrize("mark", [False, True])
def test_a_subset_expanded_judges_unqualified_names_against_every_view(mark: bool) -> None:
    every = {"views.v1": _BODY, "other.v1": "SELECT 2 AS n"}
    out = expand_view_refs("SELECT * FROM v1", {"views.v1": _BODY}, mark=mark, views=every)
    assert "vault" not in out


def test_an_ontology_class_is_never_named_unqualified() -> None:
    assert unqualified_keys(["ontology.customer", "views.v1"]) == {"views.v1"}
    assert "vault" not in expand_view_refs("SELECT * FROM customer", {"ontology.customer": _BODY})


@pytest.mark.parametrize("mark", [False, True])
def test_a_cte_of_the_name_is_the_cte(mark: bool) -> None:
    sql = "WITH v1 AS (SELECT 1 AS n) SELECT * FROM v1"
    assert "vault" not in expand_view_refs(sql, _VIEWS, mark=mark)


def test_views_read_and_statement_as_read_take_the_same_key() -> None:
    assert views_read("SELECT * FROM otherschema.v1", _VIEWS) == {}
    assert views_read('SELECT * FROM "__derived__"."views"."v1"', _VIEWS) == _VIEWS
    assert views_read("SELECT * FROM v1", _VIEWS) == _VIEWS
    state = SimpleNamespace(mv_registry=SimpleNamespace(get=lambda _id: None))
    gov = GovernanceContext(table_map={})
    assert "vault" not in statement_as_read("SELECT * FROM otherschema.v1", _VIEWS, state, gov)
    assert "vault" in statement_as_read("SELECT * FROM views.v1", _VIEWS, state, gov)


#: The reader's governance names the view's registered table by its stored schema and by its
#: domain (``sales``), as a statement not yet lowered to stored names calls it; ``otherschema.v1``
#: is another registered table.
_GOV = GovernanceContext(table_map={"views.v1": 7, "sales.v1": 7, "otherschema.v1": 8, "v1": 7})


@pytest.mark.parametrize("mark", [False, True])
def test_a_name_governance_gives_the_view_is_the_view(mark: bool) -> None:
    names = view_names(_VIEWS, _GOV.table_map)
    assert names == {"sales.v1": "views.v1"}
    for sql in ('SELECT * FROM "sales"."v1" AS t', "SELECT * FROM sales.v1"):
        assert "vault" in expand_view_refs(sql, _VIEWS, mark=mark, names=names), sql
    other = "SELECT * FROM otherschema.v1"
    assert "vault" not in expand_view_refs(other, _VIEWS, mark=mark, names=names)


def test_views_read_takes_the_names_governance_gives_the_view() -> None:
    names = view_names(_VIEWS, _GOV.table_map)
    assert views_read('SELECT * FROM "sales"."v1"', _VIEWS, names) == _VIEWS
    assert views_read("SELECT * FROM otherschema.v1", _VIEWS, names) == {}
    state = SimpleNamespace(mv_registry=SimpleNamespace(get=lambda _id: None))
    assert "vault" in statement_as_read('SELECT * FROM "sales"."v1"', _VIEWS, state, _GOV)
    assert "vault" not in statement_as_read("SELECT * FROM otherschema.v1", _VIEWS, state, _GOV)


def test_a_nested_view_is_read_only_under_its_schema() -> None:
    views = {"views.outer_v": "SELECT * FROM otherschema.v1", "views.v1": _BODY}
    assert set(views_read("SELECT * FROM views.outer_v", views)) == {"views.outer_v"}
    out = expand_view_refs("SELECT * FROM views.outer_v", views)
    assert "otherschema.v1" in out and "vault" not in out


# A view is a table of the derived source, read under its catalog (``__derived__``). A three-part
# name under another catalog is that catalog's table -- governance reads it by its catalog-physical
# name first (naming.table_id_of) -- never the view, though its schema and name are the view's.
_PG_TABLE = 'SELECT * FROM "pg"."views"."v1"'


@pytest.mark.parametrize("mark", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        _PG_TABLE,
        'SELECT * FROM "pg"."views"."v1" AS t',
        "SELECT * FROM pg.views.v1",
        'SELECT * FROM pg."views"."v1"',
        'SELECT * FROM "pg".views.v1 t',
    ],
)
def test_a_three_part_name_in_another_catalog_is_not_the_view(sql: str, mark: bool) -> None:
    out = expand_view_refs(sql, _VIEWS, mark=mark)
    assert "vault" not in out
    assert VIEW_MARK not in out


@pytest.mark.parametrize("mark", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        'SELECT * FROM "__derived__"."views"."v1"',
        'SELECT * FROM "__derived__"."views"."v1" AS t',
        "SELECT * FROM __derived__.views.v1",
        "SELECT * FROM views.v1",
        "SELECT * FROM v1",
    ],
)
def test_the_views_own_catalog_still_names_it(sql: str, mark: bool) -> None:
    assert "vault.keys" in expand_view_refs(sql, _VIEWS, mark=mark)


#: The reader's table map: ``pg.views.v1`` is another source's table of the view's schema and name.
_GOV_PG = GovernanceContext(
    table_map={"views.v1": 7, "__derived__.views.v1": 7, "pg.views.v1": 9, "v1": 7}
)


@pytest.mark.parametrize("mark", [False, True])
def test_read_against_the_readers_names_another_catalogs_table_is_that_table(mark: bool) -> None:
    names = view_names(_VIEWS, _GOV_PG.table_map)
    assert "vault" not in expand_view_refs(_PG_TABLE, _VIEWS, mark=mark, names=names)
    assert views_read(_PG_TABLE, _VIEWS, names) == {}
    state = SimpleNamespace(mv_registry=SimpleNamespace(get=lambda _id: None))
    assert "vault" not in statement_as_read(_PG_TABLE, _VIEWS, state, _GOV_PG)
    own = 'SELECT * FROM "__derived__"."views"."v1"'
    assert "vault" in expand_view_refs(own, _VIEWS, mark=mark, names=names)
    assert views_read(own, _VIEWS, names) == _VIEWS


@pytest.mark.parametrize("mark", [False, True])
def test_a_virtual_database_name_reads_the_view_as_governance_does(mark: bool) -> None:
    """A client qualifying with the database it connected to (no catalog of the reader's) names
    the table by its schema and name, as governance reads it (naming.table_id_of)."""
    names = view_names(_VIEWS, _GOV_PG.table_map)
    sql = 'SELECT * FROM "provisa"."views"."v1"'
    assert "vault" in expand_view_refs(sql, _VIEWS, mark=mark, names=names)
    assert views_read(sql, _VIEWS, names) == _VIEWS


def test_the_write_guard_and_time_travel_take_the_catalog() -> None:
    import sqlglot

    from provisa.compiler.view_expand import view_of

    names = view_names(_VIEWS, _GOV_PG.table_map)
    pg = sqlglot.parse_one(_PG_TABLE, read="postgres").find(sqlglot.exp.Table)
    own = sqlglot.parse_one('SELECT * FROM "__derived__"."views"."v1"', read="postgres").find(
        sqlglot.exp.Table
    )
    assert pg is not None and own is not None
    assert view_of(pg, _VIEWS, names) is None
    assert view_of(pg, _VIEWS) is None
    assert view_of(own, _VIEWS, names) == "views.v1"
    assert view_of(own, _VIEWS) == "views.v1"


def _two_catalogs_context():  # noqa: ANN202
    from provisa.compiler.sql_gen import CompilationContext, TableMeta

    ctx = CompilationContext()
    # The view first: the base table of its schema and name in another source comes after it.
    ctx.tables = {
        "v1": TableMeta(
            table_id=7,
            field_name="v1",
            type_name="V1",
            source_id="__derived__",
            catalog_name="__derived__",
            schema_name="views",
            table_name="v1",
            domain_id="sales",
        ),
        "pg_v1": TableMeta(
            table_id=9,
            field_name="pg_v1",
            type_name="PgV1",
            source_id="pg",
            catalog_name="pg",
            schema_name="views",
            table_name="v1",
            domain_id="ops",
        ),
    }
    return ctx


def test_lowering_keeps_the_catalog_a_reference_names() -> None:
    """Lowered, a reference naming its catalog stays that catalog's table: rewritten to the view's
    catalog, the expansion after governance read it as the view."""
    from provisa.compiler.sql_rewrite import normalize_table_refs

    ctx = _two_catalogs_context()
    assert '"pg"."views"."v1"' in normalize_table_refs(_PG_TABLE, ctx)
    own = normalize_table_refs('SELECT * FROM "__derived__"."views"."v1"', ctx)
    assert '"__derived__"."views"."v1"' in own


#: A non-default org's derived catalog: its views are lowered under it (app_loaders.
#: catalog_name_for_source, naming.org_prefixed_catalog), never under the bare ``__derived__``.
_ORG_DERIVED = "org_sbx06d8e0d47972____derived__"


def _org_state(**extra: object) -> SimpleNamespace:
    return SimpleNamespace(
        mv_registry=SimpleNamespace(get=lambda _id: None),
        source_catalogs={"__derived__": _ORG_DERIVED},
        **extra,
    )


@pytest.mark.parametrize("mark", [False, True])
def test_a_lowered_reference_in_a_non_default_org_is_the_view(mark: bool) -> None:
    """The sandbox case: a statement lowered for an org other than the default names its views
    under that org's derived catalog, and is expanded there after governance."""
    from provisa.mv.view_read import lowered_view_catalog

    catalog = lowered_view_catalog(_org_state())
    sql = f'SELECT e.a FROM "{_ORG_DERIVED}"."views"."v1" AS e'
    assert "vault.keys" in expand_view_refs(sql, _VIEWS, mark=mark, view_catalog=catalog)
    # The bare derived catalog is another org's under this one: not the view.
    bare = 'SELECT * FROM "__derived__"."views"."v1"'
    assert "vault" not in expand_view_refs(bare, _VIEWS, mark=mark, view_catalog=catalog)
    assert "vault" not in expand_view_refs(_PG_TABLE, _VIEWS, mark=mark, view_catalog=catalog)


def test_optimization_answers_a_bitemporal_view_lowered_in_a_non_default_org() -> None:
    """unnarrowed_view_bodies finds a bitemporal view a non-default org's lowered statement reads,
    and replaces it with the read over its append log (the sandbox reproduce demo's report)."""
    from provisa.mv.view_read import lowered_view_catalog, unnarrowed_view_bodies

    log_read = "SELECT * FROM mat_store.main.mv_v1"
    state = _org_state(bitemporal_view_reads={"views.v1": ("mat_store.main.mv_v1", None)})
    views = {"views.v1": log_read}
    sql = f'SELECT e.desk FROM "{_ORG_DERIVED}"."views"."v1" AS e ORDER BY e.desk'
    bodies = unnarrowed_view_bodies(sql, views, state)
    assert bodies == {"views.v1": log_read}
    out = expand_view_refs(sql, bodies, view_catalog=lowered_view_catalog(state))
    assert out == f"SELECT e.desk FROM ({log_read}) AS e ORDER BY e.desk"
    other = 'SELECT * FROM "pg"."views"."v1"'
    assert unnarrowed_view_bodies(other, views, state) == {}


_AT = "TIMESTAMP '2026-01-01 00:00:00.000000'"


def test_read_views_at_reads_a_non_default_orgs_bitemporal_view_at_its_time() -> None:
    from provisa.mv.bitemporal import declared_spec
    from provisa.mv.time_travel import Travel
    from provisa.mv.view_read import read_views_at

    spec = declared_spec(
        ["desk"], "valid_from", "recorded_at", ["desk", "valid_from", "recorded_at"]
    )
    state = _org_state(bitemporal_view_reads={"views.v1": ("mat_store.main.mv_v1", spec)})
    sql = f'SELECT e.desk FROM "{_ORG_DERIVED}"."views"."v1" AS e'
    out = read_views_at(sql, state, Travel(default=None, by_alias={"e": _AT}))
    assert "mv_v1" in out
