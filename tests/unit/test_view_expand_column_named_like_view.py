# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e3a95-4b18-4d62-a9f0-2e8b5d1c7a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view is expanded where a statement reads it as a table, never where a column shares its name
(REQ-135).

A shared dimension (REQ-413) is a view named for the concept its column holds: the view
``neighborhood`` holds each ``neighborhood`` value of the tables that join it. A read of one of
those tables names its column ``t.neighborhood``, which must stay that column; replaced by the
view's SQL, it became a scalar subquery returning every neighborhood, and the read failed.
"""

# Requirements: REQ-135, REQ-413

from __future__ import annotations

from provisa.compiler.view_expand import expand_view_refs

_VIEWS = {"city_files.neighborhood": "SELECT n FROM d"}


def test_a_quoted_column_named_like_a_view_stays_the_column():
    sql = 'SELECT "t"."neighborhood" AS "c0" FROM "city_files"."budget" "t"'
    assert expand_view_refs(sql, _VIEWS) == sql


def test_an_unquoted_column_named_like_a_view_stays_the_column():
    sql = "SELECT t.neighborhood, t.amount FROM city_files.budget t WHERE t.neighborhood = 'x'"
    assert expand_view_refs(sql, _VIEWS) == sql


def test_a_column_after_a_comma_in_the_select_list_stays_the_column():
    sql = 'SELECT "t"."amount", "t"."neighborhood" FROM "city_files"."budget" "t"'
    assert expand_view_refs(sql, _VIEWS) == sql


def test_the_view_read_as_a_table_is_expanded_and_its_columns_stay():
    sql = (
        'SELECT "d"."neighborhood", "p"."neighborhood" FROM "city_files"."neighborhood" "d" '
        'JOIN "city_files"."permits" "p" ON "p"."neighborhood" = "d"."neighborhood"'
    )
    assert expand_view_refs(sql, _VIEWS) == (
        'SELECT "d"."neighborhood", "p"."neighborhood" FROM (SELECT n FROM d) "d" '
        'JOIN "city_files"."permits" "p" ON "p"."neighborhood" = "d"."neighborhood"'
    )


def test_the_view_joined_unquoted_is_expanded_and_its_columns_stay():
    sql = (
        "SELECT p.neighborhood FROM city_files.permits p "
        "JOIN city_files.neighborhood d ON p.neighborhood = d.neighborhood"
    )
    assert expand_view_refs(sql, _VIEWS) == (
        "SELECT p.neighborhood FROM city_files.permits p "
        "JOIN (SELECT n FROM d) d ON p.neighborhood = d.neighborhood"
    )


def test_the_view_in_a_from_list_after_a_comma_is_expanded():
    sql = (
        'SELECT "p"."neighborhood" FROM "city_files"."permits" "p", '
        '"city_files"."neighborhood" "d" WHERE "p"."neighborhood" = "d"."neighborhood"'
    )
    assert expand_view_refs(sql, _VIEWS) == (
        'SELECT "p"."neighborhood" FROM "city_files"."permits" "p", '
        '(SELECT n FROM d) "d" WHERE "p"."neighborhood" = "d"."neighborhood"'
    )


def test_the_view_catalog_qualified_is_expanded():
    sql = 'SELECT "d"."neighborhood" FROM "cat"."city_files"."neighborhood" "d"'
    assert expand_view_refs(sql, _VIEWS) == 'SELECT "d"."neighborhood" FROM (SELECT n FROM d) "d"'


def test_the_view_in_a_subquery_from_is_expanded_and_an_outer_column_stays():
    sql = (
        'SELECT "t"."neighborhood" FROM "city_files"."budget" "t" WHERE "t"."neighborhood" IN '
        '(SELECT "d"."neighborhood" FROM "city_files"."neighborhood" "d")'
    )
    assert expand_view_refs(sql, _VIEWS) == (
        'SELECT "t"."neighborhood" FROM "city_files"."budget" "t" WHERE "t"."neighborhood" IN '
        '(SELECT "d"."neighborhood" FROM (SELECT n FROM d) "d")'
    )


def test_a_statement_reading_only_a_column_named_like_a_view_reads_no_view():
    from provisa.mv.view_read import views_read

    sql = 'SELECT "t"."neighborhood" FROM "city_files"."budget" "t"'
    assert views_read(sql, _VIEWS) == {}
    joined = 'SELECT "d"."neighborhood" FROM "city_files"."neighborhood" "d"'
    assert views_read(joined, _VIEWS) == _VIEWS
