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

import duckdb

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
        "motherduck",
        "mssql",
        "mysql",
        "opengauss",
        "oracle",
        "pg",
        "redshift",
        "saphana",
        "singlestore",
        "snowflake",
        "sqlalchemy",
        "synapse",
        "tidb",
        "trino",
        "trino-byo",
        "vertica",
        "yugabytedb",
    }
)

#: The engine kinds that read another region's replicas in place (REQ-1922): DuckDB ATTACHes that
#: region's store (the MotherDuck-stored DuckDB engine too), Trino registers a catalog of it, PostgreSQL imports its tables through
#: postgres_fdw — each a PostgreSQL server store. Any other engine kind cannot, so a region on one
#: may not serve an org that keeps data in another region. The engine layer's backends are the
#: authority (``EngineBackend.reads_other_regions``); a test holds the two equal.
REGION_READERS: frozenset[str] = frozenset({"duckdb", "motherduck", "pg", "trino", "trino-byo"})


#: The newest DuckDB MotherDuck's extension loads on (its own refusal on 2.0.0, 2026-11-06:
#: "Your DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version
#: is v1.5.6."). Raised when MotherDuck supports a later one.
MOTHERDUCK_LATEST_DUCKDB = (1, 5, 6)


def motherduck_extension_limit() -> str | None:
    """Why MotherDuck's extension does not load on the DuckDB this install runs, or None where it
    does."""
    running = tuple(int(p) for p in duckdb.__version__.split(".")[:3])
    if running <= MOTHERDUCK_LATEST_DUCKDB:
        return None
    latest = ".".join(map(str, MOTHERDUCK_LATEST_DUCKDB))
    return f"MotherDuck's extension supports DuckDB up to {latest} and this install runs DuckDB {duckdb.__version__}"


def unavailable_engine_kind(kind: str) -> str | None:
    """REQ-1999 (Amended 2026-11-06, UNAVAILABLE ON DUCKDB 2.0): why engine ``kind`` cannot be
    configured on this install, by name, or None where it can."""
    if kind != "motherduck":
        return None
    limit = motherduck_extension_limit()
    return None if limit is None else f"The MotherDuck engine is not available: {limit}"
