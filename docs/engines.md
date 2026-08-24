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

## Engine lifecycle (hosted deployment only)

On the hosted cloud deployment, engine shards scale to zero replicas when idle — no pod means no
bill. A desktop or self-hosted install runs the engine as an always-on process; none of the
behavior below applies to it. [tool-verified: `provisa/federation/engine_wake.py` module docstring]

**Cold starts.** When the first query arrives after an idle period, Provisa wakes the shard before
dispatching. Autopilot needs 2–4 minutes to provision a node and start Trino. The query waits
rather than failing: the wake happens at the top of `_execute_plan`, so the statement runs once
with its full retry budget intact once the engine is ready. (REQ-1448) [tool-verified:
`engine_wake.py:ensure_engine_awake`, `ensure_shard_awake`]

**UI status.** A query waiting behind a cold start is not the same as a hung server. The UI
polls `GET /data/engine/state` — which never wakes the engine — and shows a timed banner while
the state is `starting`. The banner clears when the shard reports `ready`. Possible states:
`always-on` (desktop/self-hosted or BYO coordinator), `ready`, `starting`, `stopped`. (REQ-1516)
[tool-verified: `engine_wake.py:engine_state`]

**Prewarm on sign-in.** Signing in triggers a background wake for the org's shard, so the engine
starts provisioning while the operator reads schemas and composes a query. The sign-in endpoint
returns immediately — it does not block on the wake. If the first query arrives while the prewarm
is still running, the query path waits on it rather than starting a second wake. (REQ-1471)
[tool-verified: `engine_wake.py:prewarm_engine`]

**Coordinator re-resolution.** A coordinator pod can move between the cached address check and a
query dispatching to it — eviction, node repair, or a deploy rolling the pod. When a dial reaches
nothing, Provisa re-resolves the shard's address, detects whether the coordinator moved, and if it
did, redispatches the statement once at the new address. A statement error from an engine the query
actually reached is not retried this way. (REQ-1448) [tool-verified:
`engine_wake.py:readdress_lost_coordinator`, `_is_lost_coordinator`]

**Idle reaper.** A shard that has not served traffic for 15 minutes (default; configurable via
`PROVISA_ENGINE_IDLE_SECONDS`) is scaled to zero. The check runs every 60 seconds
(`PROVISA_ENGINE_IDLE_CHECK_SECONDS`). An in-flight query cancels a drain in progress — the pod
may already be gone by then, but the wake that follows treats the shard as cold and brings a new
one up. (REQ-1448, REQ-1464) [tool-verified: `engine_wake.py:idle_reaper`]

## Query plans and statistics

The Explore and SQL surfaces can show execution statistics and a plan diagram for any statement.
Turn on **Query stats** in the surface before running the query.

**Plan diagram.** The diagram is a Mermaid flowchart built from the governed plan — after RLS,
masking, and post-governance optimization. Sources appear on the left, the route node in the
centre, and the row count on the right. Each optimization that fired (hot-table cache serve,
API-cache rewrite, dropped UNION branch) appears as its own node. The diagram reflects the actual
route: if a cross-source query collapsed to a single live source after inlining, it shows `direct`,
not `engine`. (REQ-1517) [tool-verified: `provisa/executor/plan_stats.py:build_plan_mermaid`]

**EXPLAIN and ANALYZE.** The `POST /data/sql/explain` endpoint wraps the governed SQL in the
dialect's EXPLAIN syntax. Passing `analyze: true` runs EXPLAIN ANALYZE — the query actually
executes and the plan carries real row counts and timings. ANALYZE requires connector support;
the dialects that collect statistics are:

| Dialect | EXPLAIN | EXPLAIN ANALYZE |
| --- | --- | --- |
| `postgres` / `postgresql` | `EXPLAIN (FORMAT JSON)` | `EXPLAIN (ANALYZE, FORMAT JSON)` |
| `duckdb` | `EXPLAIN (FORMAT json)` | `EXPLAIN (ANALYZE, FORMAT json)` |
| `trino` | `EXPLAIN (FORMAT JSON)` | `EXPLAIN ANALYZE` (text format) |
| `mysql` | `EXPLAIN FORMAT=JSON` | `EXPLAIN ANALYZE` (text format) |
| `sqlite` | `EXPLAIN QUERY PLAN` | not supported |

Other dialects are not supported for EXPLAIN. Requesting ANALYZE on an unsupported dialect returns
`400`. The plan the endpoint explains is the governed plan — the SQL that actually ran under the
caller's role, after RLS and masking. [tool-verified: `provisa/executor/explain.py:_SYNTAX`,
`wrap_explain`, `analyze_sql`]
