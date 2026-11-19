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

import pytest

from provisa.compiler.view_expand import VIEW_MARK, expand_view_refs, unqualified_keys
from provisa.mv.view_read import statement_as_read, views_read

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
    state = object()
    assert "vault" not in statement_as_read("SELECT * FROM otherschema.v1", _VIEWS, state)
    assert "vault" in statement_as_read("SELECT * FROM views.v1", _VIEWS, state)


def test_a_nested_view_is_read_only_under_its_schema() -> None:
    views = {"views.outer_v": "SELECT * FROM otherschema.v1", "views.v1": _BODY}
    assert set(views_read("SELECT * FROM views.outer_v", views)) == {"views.outer_v"}
    out = expand_view_refs("SELECT * FROM views.outer_v", views)
    assert "otherschema.v1" in out and "vault" not in out
