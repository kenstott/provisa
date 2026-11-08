# Copyright (c) 2026 Kenneth Stott
# Canary: 9d535293-129f-42f8-b677-da02e1f62aed
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#241: the postgres extension is loaded with filter pushdown off, everywhere it is loaded.

With pushdown on (the extension's default), the Top-N dynamic filter of
`ORDER BY <col> … NULLS FIRST LIMIT n` reaches Postgres as `WHERE <col> IS NULL` (upstream
duckdb/duckdb-postgres#564), so a sorted page holds only NULL-key rows. The setting must be
GLOBAL: the runtime runs queries on cursors, which a plain SET never reaches.
"""

import duckdb

from provisa.federation.duckdb_extensions import load_extension
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime


def _pushdown(con: duckdb.DuckDBPyConnection) -> bool:
    return con.execute("SELECT current_setting('pg_experimental_filter_pushdown')").fetchone()[0]


def test_the_extension_default_pushes_filters_down():
    # The default this guards against: loaded bare, pushdown is on.
    con = duckdb.connect()
    con.execute("LOAD postgres")
    assert _pushdown(con) is True


def test_the_shared_loader_turns_pushdown_off_for_the_connection_and_its_cursors():
    con = duckdb.connect()
    load_extension(con, "postgres")
    assert _pushdown(con) is False
    assert _pushdown(con.cursor()) is False


def test_the_runtime_loads_postgres_with_pushdown_off_on_the_cursors_it_queries_on():
    rt = DuckDBFederationRuntime()
    rt._ensure_postgres_loaded()
    assert _pushdown(rt._con.cursor()) is False


def test_another_extension_is_loaded_with_no_postgres_setting():
    con = duckdb.connect()
    load_extension(con, "sqlite")
    assert (
        con.execute(
            "SELECT count(*) FROM duckdb_extensions() WHERE loaded AND extension_name = 'sqlite_scanner'"
        ).fetchone()[0]
        == 1
    )
