# Copyright (c) 2026 Kenneth Stott
# Canary: 5ce3f8b0-e393-4e1b-b696-5a504f03e709
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine kinds a federation engine is built for, by name (REQ-1922).

The model names an engine kind on a region's engine store and is validated where the model is
(``provisa/core/regions.py``), which may not import the engine layer. The engine layer's builder
map (``provisa/federation/engine.py``) is the authority; a test holds the two equal.
"""

# Requirements: REQ-1922

from __future__ import annotations

ENGINE_KINDS: frozenset[str] = frozenset(
    {
        "bigquery",
        "clickhouse",
        "clickhouse-server",
        "cockroachdb",
        "databricks",
        "db2",
        "duckdb",
        "exasol",
        "fabric",
        "firebird",
        "greenplum",
        "mariadb",
        "monetdb",
        "mssql",
        "mysql",
        "opengauss",
        "oracle",
        "pg",
        "redshift",
        "sapase",
        "saphana",
        "singlestore",
        "snowflake",
        "sqlalchemy",
        "sqlanywhere",
        "synapse",
        "teradata",
        "tidb",
        "trino",
        "trino-byo",
        "vertica",
        "yugabytedb",
    }
)
