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

Usage: python cloud_warehouse_seed.py <snowflake|databricks|bigquery|fabric> <up|down> <schema>
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

# The run's own schema (e2e/cloud-warehouse.ts WAREHOUSE_SCHEMA), set from argv in main(): the
# warehouses are shared by concurrent CI runs, which must not seed or drop each other's.
_SCHEMA = ""
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
    db = _SCHEMA.upper()  # the run's schema, upper-cased — also used as its database here
    table = _TABLE.upper()
    if action == "down":
        from tests.integration.live_teardown import run_every
        from tests.integration.snowflake_warehouse import drop_database

        # Dropped and read back gone, then the connection closed: every step attempted (REQ-254).
        run_every([(f"drop database {db}", lambda: drop_database(conn, db)), ("close", conn.close)])
        return
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cur.execute(f"CREATE OR REPLACE TABLE {db}.PUBLIC.{table} (id NUMBER, name STRING)")
        cur.execute(f"INSERT INTO {db}.PUBLIC.{table} VALUES (1,'sprocket'),(2,'cog'),(3,'gear')")
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
    if action == "down":
        from tests.integration.databricks_uc import drop_schema
        from tests.integration.live_teardown import run_every

        # The schema (its table with it) dropped and read back gone, then the connection closed.
        # The warehouse "up" woke is stopped by the lane's after-script (ui-core-after.sh): other
        # workers of the run may still be reading it here (REQ-254).
        run_every(
            [
                (
                    f"drop schema workspace.{_SCHEMA}",
                    lambda: drop_schema(conn, "workspace", _SCHEMA),
                ),
                ("close", conn.close),
            ]
        )
        return
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS `workspace`.`{_SCHEMA}`")
        cur.execute(f"DROP TABLE IF EXISTS {fq}")
        cur.execute(f"CREATE TABLE {fq} (id BIGINT, name STRING) USING DELTA")
        cur.execute(f"INSERT INTO {fq} VALUES (1,'sprocket'),(2,'cog'),(3,'gear')")
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
        # A DML insert, not a streaming one: insertAll into a table just dropped and recreated
        # under the same name answers 404 "Table ... not found" until BigQuery's streaming
        # backend sees the new table, which can take minutes.
        client.query(
            f"INSERT INTO `{table_ref}` (id, name) VALUES (1, 'sprocket'), (2, 'cog'), (3, 'gear')"
        ).result()
    else:
        from google.api_core.exceptions import NotFound

        client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            return
        raise RuntimeError(f"BigQuery dataset {dataset_ref} still exists after its delete")


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
    if action == "down":
        from tests.integration.live_teardown import none_remain, run_every

        def drop() -> None:
            cur = rt.connection.cursor()
            try:
                cur.execute(f"DROP TABLE IF EXISTS [{_SCHEMA}].[{_TABLE}]")
                cur.execute(f"DROP SCHEMA IF EXISTS [{_SCHEMA}]")
                rt.connection.commit()
                cur.execute("SELECT name FROM sys.schemas WHERE name = ?", [_SCHEMA])
                none_remain("Fabric schemas", [r[0] for r in cur.fetchall()])
            finally:
                cur.close()

        # The schema dropped and read back gone, the connection closed, the capacity paused: every
        # step attempted, and a pause that fails fails the teardown -- a capacity left running
        # bills (REQ-254).
        run_every(
            [
                (f"drop schema {_SCHEMA}", drop),
                ("close", rt.close),
                ("pause the Fabric capacity", suspend_capacity),
            ]
        )
        return
    cur = rt.connection.cursor()
    try:
        cur.execute(f"IF SCHEMA_ID('{_SCHEMA}') IS NULL EXEC('CREATE SCHEMA [{_SCHEMA}]')")
        cur.execute(f"DROP TABLE IF EXISTS [{_SCHEMA}].[{_TABLE}]")
        cur.execute(f"CREATE TABLE [{_SCHEMA}].[{_TABLE}] (id BIGINT, name VARCHAR(50))")
        cur.execute(
            f"INSERT INTO [{_SCHEMA}].[{_TABLE}] VALUES (1,'sprocket'),(2,'cog'),(3,'gear')"
        )
        rt.connection.commit()
    finally:
        cur.close()
        rt.close()


_ENGINES = {
    "snowflake": _snowflake,
    "databricks": _databricks,
    "bigquery": _bigquery,
    "fabric": _fabric,
}


def main() -> None:
    _load_root_env()
    global _SCHEMA
    if len(sys.argv) != 4:
        raise SystemExit(
            f"usage: python cloud_warehouse_seed.py <{'|'.join(_ENGINES)}> <up|down> <schema>"
        )
    engine, action, _SCHEMA = sys.argv[1], sys.argv[2], sys.argv[3]
    if engine not in _ENGINES or action not in ("up", "down"):
        raise SystemExit(f"usage: python cloud_warehouse_seed.py <{'|'.join(_ENGINES)}> <up|down>")
    _ENGINES[engine](action)


if __name__ == "__main__":
    main()
