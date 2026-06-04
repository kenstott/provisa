# Copyright (c) 2026 Kenneth Stott
# Canary: 4da8d727-e0ba-4a8d-bbfe-8cf1c6b3b44f
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for nf_extractor — _nf_ and _np_ native param extraction."""

import pytest

from provisa.compiler.nf_extractor import extract_nf_args, find_api_table_names


def test_find_api_tables_surfaces_inlined_view_tables():
    """Regression: a view inlines API/graphql_remote tables in its body. The Cypher
    path must detect them from the EXPANDED SQL (not the outer view-name-only SQL) so
    it routes through the remote-materialization path instead of plain Trino."""
    expanded = (
        'SELECT n FROM ('
        '  SELECT a.id FROM "inquiries_sqlite"."default"."inquiries" a'
        '  JOIN "graphql_demo"."graphql_remote"."shelter__animalBreeds" b ON a.x = b.y'
        ') AS n'
    )
    names = find_api_table_names(expanded)
    assert "shelter__animalBreeds" in names
    assert "inquiries" in names


def test_nf_prefix_extracted():
    sql = 'SELECT * FROM "t" WHERE "_nf_id" = $1'
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [42])
    assert nf_args == {"id": 42}
    assert clean_params == []
    assert "_nf_id" not in clean_sql


def test_non_native_where_kept():
    sql = 'SELECT * FROM "t" WHERE "name" = $1'
    clean_sql, clean_params, nf_args = extract_nf_args(sql, ["Rex"])
    assert nf_args == {}
    assert clean_params == ["Rex"]
    assert "name" in clean_sql


def test_mixed_nf_and_regular():
    sql = 'SELECT * FROM "t" WHERE "_nf_id" = $1 AND "status" = $2'
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [9, "sold"])
    assert nf_args == {"id": 9}
    assert clean_params == ["sold"]
    assert "status" in clean_sql
