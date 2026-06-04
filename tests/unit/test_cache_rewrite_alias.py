# Copyright (c) 2026 Kenneth Stott
# Canary: a2b3c4d5-e6f7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cache rewrites preserve the original table name as an alias.

Regression: an unaliased API/GraphQL-remote table reference was rewritten to its
cache table / VALUES CTE without an alias, so column qualifiers using the original
table name (e.g. shelter__animalBreeds.name) could no longer resolve.
"""

from __future__ import annotations

from provisa.api_source.trino_cache import CacheLocation, rewrite_all_from_cache
from provisa.cache.hot_tables import HotTableEntry, build_values_cte_sql

_SQL = (
    'SELECT shelter__animalBreeds.name '
    'FROM "pg"."pet_store"."pets" '
    'JOIN "c"."shelter"."shelter__animalBreeds" '
    "ON pets.breed_name = shelter__animalBreeds.name"
)


def test_rewrite_all_from_cache_aliases_unaliased_ref():
    loc = CacheLocation("iceberg", "cache", "iceberg")
    out = rewrite_all_from_cache(_SQL, {"shelter__animalBreeds": (loc, "r_abc123")})
    # cache table is renamed but the original name survives as an alias
    assert "AS shelter__animalBreeds" in out
    assert 'iceberg.cache."r_abc123"' in out
    # the column qualifier is untouched and now binds to the alias
    assert "shelter__animalBreeds.name" in out


def test_rewrite_all_from_cache_keeps_existing_alias():
    sql = (
        'SELECT t0.name FROM "c"."shelter"."shelter__animalBreeds" "t0" '
        "ON x = t0.name"
    )
    loc = CacheLocation("iceberg", "cache", "iceberg")
    out = rewrite_all_from_cache(sql, {"shelter__animalBreeds": (loc, "r_abc123")})
    assert '"t0"' in out
    assert "AS shelter__animalBreeds" not in out


def test_build_values_cte_aliases_unaliased_ref():
    entry = HotTableEntry(
        table_name="shelter__animalBreeds",
        catalog="c",
        schema="shelter",
        pk_column="name",
        rows=[{"name": "Beagle"}],
        column_names=["name"],
    )
    out = build_values_cte_sql(_SQL, "shelter__animalBreeds", entry)
    assert "_hot_shelter__animalBreeds AS shelter__animalBreeds" in out
    assert "shelter__animalBreeds.name" in out
