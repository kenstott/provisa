# Copyright (c) 2026 Kenneth Stott
# Canary: bf08f54b-3ee0-4089-8c9e-e0686168ee91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A view's name inside a string literal is a value, not a reference to it (REQ-135).

A compiled read -- a REST or GraphQL read of a view -- names its table in a literal, ``'city.
view_x' AS "_name_"``. View expansion put the view's SQL into that literal too, and the statement
no longer parsed (HTTP 500 on a REST read of any view)."""

# Requirements: REQ-135

from __future__ import annotations

from provisa.compiler.view_expand import expand_view_refs

_VIEWS = {"city.view_x": "SELECT 1 AS a"}


def test_a_view_named_in_a_string_literal_is_left_as_written():
    sql = "SELECT 'city.view_x' AS \"_name_\", t.a FROM city.view_x AS t"
    assert expand_view_refs(sql, _VIEWS) == (
        "SELECT 'city.view_x' AS \"_name_\", t.a FROM (SELECT 1 AS a) AS t"
    )


def test_the_same_reference_outside_a_literal_is_still_expanded():
    assert expand_view_refs("SELECT a FROM city.view_x", _VIEWS) == (
        'SELECT a FROM (SELECT 1 AS a) "view_x"'
    )
