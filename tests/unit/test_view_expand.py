# Copyright (c) 2026 Kenneth Stott
# Canary: a2b3c4d5-e6f7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Inline view expansion and NL→SQL qualifier-binding checks."""

from __future__ import annotations

import sqlglot

from provisa.api.data.endpoint_dev import _check_qualifier_binding
from provisa.compiler.view_expand import expand_view_refs

_VMAP = {"shelter__animalBreeds": 'SELECT name, "careLevel" FROM gql'}


def test_unaliased_view_ref_gets_table_name_alias():
    # Regression: an unaliased view ref became an anonymous subquery, so a
    # column qualified by the view-table name could no longer resolve.
    sql = (
        'JOIN "__provisa__"."shelter"."shelter__animalBreeds" '
        'ON "pets"."breed_name" = shelter__animalBreeds.name'
    )
    out = expand_view_refs(sql, _VMAP)
    assert out == (
        'JOIN (SELECT name, "careLevel" FROM gql) shelter__animalBreeds '
        'ON "pets"."breed_name" = shelter__animalBreeds.name'
    )


def test_provisa_generated_alias_preserved():
    sql = 'FROM "c"."shelter"."shelter__animalBreeds" "t0" WHERE 1=1'
    out = expand_view_refs(sql, _VMAP)
    assert out == 'FROM (SELECT name, "careLevel" FROM gql) "t0" WHERE 1=1'


def test_user_alias_preserved():
    sql = 'JOIN "c"."shelter"."shelter__animalBreeds" ab ON ab.name = x'
    out = expand_view_refs(sql, _VMAP)
    assert out == 'JOIN (SELECT name, "careLevel" FROM gql) ab ON ab.name = x'


def test_as_alias_preserved():
    sql = 'FROM "c"."shelter"."shelter__animalBreeds" AS ab'
    out = expand_view_refs(sql, _VMAP)
    assert out == 'FROM (SELECT name, "careLevel" FROM gql) AS ab'


def test_schema_qualified_unaliased():
    sql = 'FROM "shelter"."shelter__animalBreeds"'
    out = expand_view_refs(sql, _VMAP)
    assert out == 'FROM (SELECT name, "careLevel" FROM gql) shelter__animalBreeds'


def test_binding_check_flags_undefined_aliases():
    bad = (
        "SELECT i.id, u.name, ab.careLevel "
        "FROM pet_store.inquiries i "
        "JOIN pet_store.users ON i.user_id = users.id"
    )
    err = _check_qualifier_binding(sqlglot.parse_one(bad, read="postgres"))
    assert err is not None
    assert "u" in err and "ab" in err


def test_binding_check_passes_fully_qualified():
    good = (
        "SELECT inquiries.id, users.name "
        "FROM pet_store.inquiries "
        "JOIN pet_store.users ON inquiries.user_id = users.id"
    )
    assert _check_qualifier_binding(sqlglot.parse_one(good, read="postgres")) is None


def test_binding_check_flags_schema_qualified_columns():
    # schema.table.column embeds the semantic domain prefix, which is not
    # rewritten in column position and fails against the physical relation.
    bad = (
        "SELECT pet_store.inquiries.id "
        "FROM pet_store.inquiries "
        "JOIN shelter.shelter__animalBreeds "
        "ON pet_store.inquiries.user_id = shelter.shelter__animalBreeds.name"
    )
    err = _check_qualifier_binding(sqlglot.parse_one(bad, read="postgres"))
    assert err is not None
    assert "Schema-qualified" in err
