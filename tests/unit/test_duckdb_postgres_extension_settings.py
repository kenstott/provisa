# Copyright (c) 2026 Kenneth Stott
# Canary: 9d535293-129f-42f8-b677-da02e1f62aed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#241: on DuckDB 1.5 the postgres extension was loaded with filter pushdown off, everywhere it
was loaded: with pushdown on, the Top-N dynamic filter of `ORDER BY <col> … NULLS FIRST LIMIT n`
reached Postgres as `WHERE <col> IS NULL` (upstream duckdb/duckdb-postgres#564), so a sorted page
held only NULL-key rows. postgres_scanner 2.0.0 answers that page correctly with pushdown on
(tests/integration/test_duckdb_postgres_sorted_pages_e2e.py) and renamed the setting
pg_enable_filter_pushdown, so the extension is loaded at its default, filters reaching Postgres,
on the connection and on the cursors the runtime queries on.
"""

import duckdb

from provisa.federation.duckdb_extensions import install_and_load
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime


def _pushdown(con: duckdb.DuckDBPyConnection) -> bool:
    return con.execute("SELECT current_setting('pg_enable_filter_pushdown')").fetchone()[0]


def test_the_extension_default_pushes_filters_down():
    # The default this guards against: loaded bare, pushdown is on.
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    assert _pushdown(con) is True


def test_the_shared_loader_keeps_pushdown_on_for_the_connection_and_its_cursors():
    con = duckdb.connect()
    install_and_load(con, "postgres", from_community=False)
    assert _pushdown(con) is True
    assert _pushdown(con.cursor()) is True


def test_the_runtime_loads_postgres_with_pushdown_on_on_the_cursors_it_queries_on():
    rt = DuckDBFederationRuntime()
    rt._ensure_postgres_loaded()
    assert _pushdown(rt._con.cursor()) is True


def test_the_1_5_setting_is_gone_from_postgres_scanner_2_0():
    # What made the old SET fail on 2.0: the setting was renamed.
    con = duckdb.connect()
    install_and_load(con, "postgres", from_community=False)
    names = {
        name
        for (name,) in con.execute(
            "SELECT name FROM duckdb_settings() WHERE name LIKE 'pg_%filter_pushdown'"
        ).fetchall()
    }
    assert names == {"pg_enable_filter_pushdown"}


def test_another_extension_is_loaded_with_no_postgres_setting():
    con = duckdb.connect()
    install_and_load(con, "sqlite", from_community=False)
    assert (
        con.execute(
            "SELECT count(*) FROM duckdb_extensions() WHERE loaded AND extension_name = 'sqlite_scanner'"
        ).fetchone()[0]
        == 1
    )


def test_a_postgres_store_s_catalog_cache_is_cleared_with_postgres_scanner_2_0_s_call(monkeypatch):
    # postgres_scanner 2.0 names it postgres_clear_cache; pg_clear_cache is gone, and every read of
    # a PostgreSQL store's replica or view failed on it.
    rt = DuckDBFederationRuntime()
    rt._ensure_postgres_loaded()
    monkeypatch.setattr(rt, "_store_scheme", lambda: "postgresql")
    rt._see_store_writes()
