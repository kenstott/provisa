# Federation Engines

A source type says where data lives. A federation **engine** is the runtime Provisa compiles that
data's queries into — the thing that holds the connectors, executes the plan, and owns the
materialization store. The two are separate registries: `SourceType` in `provisa/core/models.py`
lists what you can register, `_ENGINE_BUILDERS` in `provisa/federation/engine.py` lists what can run
it. `trino` and several warehouses appear in both, meaning Provisa can read a Trino cluster as a
source, run on one as an engine, or do both at once against different clusters.

Provisa ships **32** engine keys. [tool-verified: `provisa/federation/engine.py` `_ENGINE_BUILDERS`]

## Federating engines

These reach sources in place through connectors, so a cross-source JOIN can run without landing
every input first.

| Key | Engine | Notes |
| --- | --- | --- |
| `trino` | Trino (embedded MPP) | Provisa-managed Trino cluster; JVM heap and cluster config are editable in Admin and regenerate on restart |
| `trino-byo` | Trino (bring your own) | External coordinator — same runtime, connection only, no cluster tuning |
| `pg` | PostgreSQL | BYO or embedded; federates via FDWs or pg_duckdb |
| `duckdb` | DuckDB (in-process) | Native partial federator; the desktop and demo default |
| `clickhouse` | ClickHouse (embedded chdb) | OLAP federator (REQ-909); the only engine with a Hudi path (REQ-1178) |
| `clickhouse-server` | ClickHouse (server or cloud) | Same runtime as above, URL-driven |

## Warehouse engines

Partial federators: the warehouse executes, and sources it cannot reach natively land into it
first. Each reads Arrow-natively where the vendor's client supports it.

| Key | Engine | Notes |
| --- | --- | --- |
| `snowflake` | Snowflake | Self-only MPP warehouse, Arrow-native (REQ-988) |
| `databricks` | Databricks | Partial federator, Arrow-native (REQ-987) |
| `bigquery` | BigQuery | Partial federator; GCS external links |
| `fabric` | Microsoft Fabric Warehouse | T-SQL; OneLake `OPENROWSET` links |
| `synapse` | Azure Synapse serverless SQL | T-SQL; ADLS `OPENROWSET` links |

## SQLAlchemy engines

One runtime with zero federation connectors: every source LANDs into the target store and is then
federated with plain SQL. `sqlalchemy` takes any URL from `$PROVISA_ENGINE_URL`; the 20 keys below
are the same runtime pre-named per database so a picker can name the product rather than the
library (REQ-905, REQ-1421). [tool-verified: `provisa/federation/engine.py` `_RDB_KINDS`,
`build_sqlalchemy_engine`]

| Key | Engine |
| --- | --- |
| `sqlalchemy` | Any SQLAlchemy URL |
| `mysql`, `mariadb`, `tidb`, `singlestore` | MySQL and its wire-compatible relatives |
| `greenplum`, `cockroachdb`, `yugabytedb`, `opengauss` | PostgreSQL-wire relatives |
| `oracle`, `mssql`, `db2`, `teradata` | Enterprise RDBMS |
| `saphana`, `sapase`, `sqlanywhere` | SAP |
| `redshift`, `vertica`, `exasol`, `monetdb` | Analytic stores |
| `firebird` | Firebird |

File-embedded stores are absent on purpose: the engine's store must be reachable over the network
from wherever Provisa runs, which a local file on someone else's disk is not.

## Two narrower lists

The 32 keys are the complete set, but two other places name a subset on purpose, and they are not
the same list:

- **The install wizard** offers 5 options — `duckdb`, `pg_duckdb`, `postgres_fdw`, `trino`,
  `sqlalchemy`. These are provisioning choices, not builder keys: `pg_duckdb` and `postgres_fdw`
  are two ways to provision the one `pg` engine. A wizard option must declare its platforms,
  provisioning route and startup cost, and the demo preset may only use bundled instant ones.
  [tool-verified: `config/capabilities.yaml` `roles.federation_engine.options`]
- **The Admin engine picker** renders the selectable-engine registry, which pairs each key with the
  config fields that engine needs. A selection persists to the platform config and binds on the
  next service restart, because the engine is chosen once at boot (REQ-916). [tool-verified:
  `provisa/federation/engine.py`, the registry following `_ENGINE_BUILDERS`]

## What an engine changes

Engine choice decides which sources are reachable in place versus landed as a replica. Ask the
running system rather than inferring it: `reachable_source_types(engine_key)` returns what an
engine can federate, and `live_source_types(engine_key)` returns the subset it queries without
materializing. [tool-verified: `provisa/federation/engine.py:1215`, `:1234`]
