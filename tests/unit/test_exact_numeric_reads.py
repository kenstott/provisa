# Copyright (c) 2026 Kenneth Stott
# Canary: 72b9b9a3-e1ef-4d9c-bf34-b100d3db4772
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On DuckDB, a PostgreSQL table holding a numeric registered from an unsized one is read through
postgres_query, the column cast to numeric(38, s) at its source (REQ-2167)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.exact_numeric import (
    NotAttached,
    WideTable,
    exact_numeric_reads,
    wide_numeric_tables,
)

_WIDE = {("src", "public", "t"): WideTable(("id", "amount", "name"), {"amount": "numeric(38,6)"})}
# The view src.public.t stands for remote.t on the raw attach "src_raw".
_REMOTE = {("src", "public", "t"): ("src_raw", "remote", "t")}.get


def test_only_the_columns_read_are_read_and_the_wide_one_is_cast_at_its_source():
    sql = 'SELECT "t"."amount" FROM "src"."public"."t" AS "t" WHERE "t"."id" = 1'
    assert exact_numeric_reads(sql, _WIDE, _REMOTE) == (
        'SELECT "t"."amount" FROM (SELECT * FROM POSTGRES_QUERY(\'src_raw\', \'SELECT "id", '
        '"amount"::numeric(38,6) AS "amount" FROM "remote"."t"\')) AS "t" WHERE "t"."id" = 1'
    )


def test_a_star_reads_every_registered_column():
    out = exact_numeric_reads('SELECT * FROM "src"."public"."t"', _WIDE, _REMOTE)
    assert '"id", "amount"::numeric(38,6) AS "amount", "name" FROM "remote"."t"' in out


def test_another_table_is_read_as_it_was():
    sql = 'SELECT "u"."x" FROM "src"."public"."u" AS "u"'
    assert exact_numeric_reads(sql, _WIDE, _REMOTE) == sql


def test_a_wide_table_not_attached_is_refused_by_name():
    with pytest.raises(NotAttached, match="src.public.t"):
        exact_numeric_reads('SELECT * FROM "src"."public"."t"', _WIDE, lambda view: None)


def test_the_wide_tables_are_the_postgres_tables_registration_sized_to_38():
    def column(name, data_type):
        return {"column_name": name, "data_type": data_type}

    state = SimpleNamespace(
        source_types={"pg": "postgresql", "my": "mysql"},
        catalog_for=lambda source_id: f"cat_{source_id}",
        schema_build_cache={
            "tables": [
                {
                    "source_id": "pg",
                    "schema_name": "public",
                    "table_name": "t",
                    "columns": [column("id", "integer"), column("amount", "numeric(38,6)")],
                },
                {
                    "source_id": "pg",
                    "schema_name": "public",
                    "table_name": "sized",
                    "columns": [column("price", "numeric(10,2)")],
                },
                {
                    "source_id": "my",
                    "schema_name": "shop",
                    "table_name": "m",
                    "columns": [column("amount", "numeric(38,6)")],
                },
            ]
        },
    )
    assert wide_numeric_tables(state) == {
        ("cat_pg", "public", "t"): WideTable(("id", "amount"), {"amount": "numeric(38,6)"})
    }
