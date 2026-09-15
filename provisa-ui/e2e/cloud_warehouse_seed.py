# Copyright (c) 2026 Kenneth Stott
# Canary: 8b7d6c1a-3e42-4f8b-9d5e-2c6a1b4f9e07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Seed / teardown a small live table in each cloud warehouse for source-to-query-cloud-warehouse.
spec.ts (REQ-1747 lane): the Playwright spec drives the Sources/Register Table/SQL-page UI against
whatever this script puts there, then calls it again with "down" to remove it. Mirrors the exact
connection code the existing python integration suites already use for each warehouse (
tests/integration/test_snowflake_source_e2e.py, test_databricks_source_e2e.py,
test_bigquery_federation_engine_e2e.py, test_fabric_federation_engine_e2e.py) so this is not a new,
unverified way of reaching them.

Usage: python cloud_warehouse_seed.py <snowflake|databricks|bigquery|fabric> <up|down>
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    # Running as a standalone script (not `python -m`), so the repo root — home to both the
    # `provisa` package and the `tests.integration` helpers this reuses — isn't on sys.path yet.
    sys.path.insert(0, str(_ROOT))

_SCHEMA = "provisa_ui_e2e"
_TABLE = "widgets"


def _load_root_env() -> None:
    """Mirror playwright.config.ts's root .env load: this script also runs standalone (this
    file's own __main__, from a shell) where nothing has sourced it yet."""
    root_env = _ROOT / ".env"
    if not root_env.exists():
        return
    for line in root_env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


def _snowflake(action: str) -> None:
    import snowflake.connector as sf

    conn = sf.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    # A DEDICATED database, not SNOWFLAKE_DATABASE ("_landing"): that name was created with a
    # lowercase quoted identifier, and SnowflakeDriver.connect() (executor/drivers/snowflake.py)
    # passes the Sources form's Database field to snowflake-connector's `database=` kwarg
    # UNQUOTED — Snowflake upper-cases it to "_LANDING", which does not exist, so the connection
    # silently lands with no current database at all (CURRENT_DATABASE() reads NULL) rather than
    # erroring. Reproduced live; filed as a bug (see spec header). A plain uppercase-safe database
    # name sidesteps it for this e2e without masking the finding.
    db = _SCHEMA.upper()  # "PROVISA_UI_E2E" — also used as the schema name's database here
    table = _TABLE.upper()
    cur = conn.cursor()
    try:
        if action == "up":
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
            cur.execute(f"CREATE OR REPLACE TABLE {db}.PUBLIC.{table} (id NUMBER, name STRING)")
            cur.execute(
                f"INSERT INTO {db}.PUBLIC.{table} VALUES (1,'sprocket'),(2,'cog'),(3,'gear')"
            )
        else:
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    finally:
        cur.close()
        conn.close()


def _databricks(action: str) -> None:
    from databricks import sql as dbsql

    from provisa.federation.databricks_tls import databricks_tls_kwargs
    from tests.integration.databricks_warehouse import ensure_warehouse_running

    # A suspended serverless warehouse rejects OpenSession outright, so it must be woken BEFORE
    # dbsql.connect() is ever attempted (mirrors databricks_warehouse.py's own autouse fixture).
    ensure_warehouse_running()
    conn = dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        _socket_timeout=60,
        _retry_stop_after_attempts_count=2,
        **databricks_tls_kwargs(),
    )
    fq = f"`workspace`.`{_SCHEMA}`.`{_TABLE}`"
    cur = conn.cursor()
    try:
        if action == "up":
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `workspace`.`{_SCHEMA}`")
            cur.execute(f"DROP TABLE IF EXISTS {fq}")
            cur.execute(f"CREATE TABLE {fq} (id BIGINT, name STRING) USING DELTA")
            cur.execute(f"INSERT INTO {fq} VALUES (1,'sprocket'),(2,'cog'),(3,'gear')")
        else:
            cur.execute(f"DROP TABLE IF EXISTS {fq}")
            cur.execute(f"DROP SCHEMA IF EXISTS `workspace`.`{_SCHEMA}`")
    finally:
        cur.close()
        conn.close()


def _bigquery(action: str) -> None:
    from google.cloud import bigquery

    project = os.environ["GOOGLE_CLOUD_PROJECT"]
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    client = bigquery.Client(project=project, location=location)
    dataset_ref = f"{project}.{_SCHEMA}"
    table_ref = f"{dataset_ref}.{_TABLE}"
    if action == "up":
        ds = bigquery.Dataset(dataset_ref)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)
        schema = [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("name", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.delete_table(table_ref, not_found_ok=True)
        client.create_table(table)
        client.insert_rows_json(
            table_ref,
            [{"id": 1, "name": "sprocket"}, {"id": 2, "name": "cog"}, {"id": 3, "name": "gear"}],
        )
    else:
        client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)


def _fabric(action: str) -> None:
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime
    from tests.integration.fabric_capacity import ensure_capacity_resumed, suspend_capacity

    if action == "up":
        # A paused/suspended Fabric capacity rejects the SQL warehouse connection outright
        # (18456 "system update"), so it must be resumed BEFORE MssqlWarehouseRuntime ever
        # connects — mirrors _databricks()'s ensure_warehouse_running() call above.
        ensure_capacity_resumed()

    rt = MssqlWarehouseRuntime(
        server=os.environ["FABRIC_SQL_SERVER"],
        database=os.environ["FABRIC_DATABASE"],
        engine_name="fabric",
    )
    cur = rt.connection.cursor()
    try:
        if action == "up":
            cur.execute(f"IF SCHEMA_ID('{_SCHEMA}') IS NULL EXEC('CREATE SCHEMA [{_SCHEMA}]')")
            cur.execute(f"DROP TABLE IF EXISTS [{_SCHEMA}].[{_TABLE}]")
            cur.execute(f"CREATE TABLE [{_SCHEMA}].[{_TABLE}] (id BIGINT, name VARCHAR(50))")
            cur.execute(
                f"INSERT INTO [{_SCHEMA}].[{_TABLE}] VALUES (1,'sprocket'),(2,'cog'),(3,'gear')"
            )
            rt.connection.commit()
        else:
            cur.execute(f"DROP TABLE IF EXISTS [{_SCHEMA}].[{_TABLE}]")
            cur.execute(f"DROP SCHEMA IF EXISTS [{_SCHEMA}]")
            rt.connection.commit()
    finally:
        cur.close()
        rt.close()
        if action == "down":
            # Best-effort — leaving the capacity briefly running is a cost concern, not a
            # correctness one, so a suspend failure must never fail the seed teardown.
            suspend_capacity()


_ENGINES = {
    "snowflake": _snowflake,
    "databricks": _databricks,
    "bigquery": _bigquery,
    "fabric": _fabric,
}


def main() -> None:
    _load_root_env()
    engine, action = sys.argv[1], sys.argv[2]
    if engine not in _ENGINES or action not in ("up", "down"):
        raise SystemExit(f"usage: python cloud_warehouse_seed.py <{'|'.join(_ENGINES)}> <up|down>")
    _ENGINES[engine](action)


if __name__ == "__main__":
    main()
