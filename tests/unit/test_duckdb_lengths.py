# Copyright (c) 2026 Kenneth Stott
# Canary: caa9b748-e4f9-4e2b-82ca-cd5821bb6070
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""length() and octet_length() of a bytea or a text answer on DuckDB as PostgreSQL answers them
(pgspec TestTypes/LargeValues): DuckDB's length takes no BLOB, its octet_length no VARCHAR."""

from __future__ import annotations

import duckdb

from provisa.transpiler.transpile import transpile_to_duckdb


def test_lengths_of_a_bytea_and_a_text():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT 'héllo'::VARCHAR AS s, '\\xDE\\xAD'::BLOB AS b")
    sql = transpile_to_duckdb(
        "SELECT length(s), length(b), octet_length(s), octet_length(b) FROM t"
    )
    assert con.execute(sql).fetchall() == [(5, 2, 6, 2)]


def test_a_statement_without_a_length_is_left_as_it_is():
    assert transpile_to_duckdb("SELECT upper(s) FROM t") == "SELECT UPPER(s) FROM t"


def test_the_length_of_a_list_is_its_elements():
    con = duckdb.connect()
    sql = transpile_to_duckdb("SELECT length(ARRAY[1, 2, 3]), length('héllo'), length(42)")
    assert con.execute(sql).fetchall() == [(3, 5, 2)]
