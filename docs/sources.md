# Source Types

## Execution Model

Every query ends up running through the federation engine, which is what lets any source join any other. How a source gets there depends on how Provisa can reach it, and that puts each source in one of five groups.

| Category | Has Direct Driver | Has Federated Connector | Examples |
| --- | --- | --- | --- |
| **Direct-capable** | Yes | Yes | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Federation only** | No | Yes | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-backed) |
| **Direct-read (replica)** | Yes | Yes | Snowflake, Databricks, ClickHouse — driver reads data and lands a replica; queries run against the replica in the active engine |
| **Direct (no cross-source connector)** | Yes | No | Athena — single-source statements run whole in Athena; cross-source joins land a full replica |
| **Materialize → Federation** | No | No | REST/OpenAPI, remote GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (push receiver), GovData, SharePoint, Salesforce, ServiceNow, Cloud inventory, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

**Direct-capable** sources answer a query that touches only them through their own driver, in under 100 ms, without going through the federation engine. They keep their full connector, so they still join other sources through the engine.

**Federation only** sources have no direct driver. Every query reaches them through the federation layer.

**Direct-read (replica)** sources have a DirectDriver that reads from the warehouse natively (Arrow-native where it can) and lands a replica in the active engine's materialization store. Queries then run against that replica. See [Warehouses as Named Sources](#warehouses-as-named-sources).

**Direct (no cross-source connector)** sources have a DirectDriver that runs a single-source statement whole in the source, with every filter, column and limit pushed down. There's no Trino connector for them, so when a statement joins one with another source, the driver reads the data and lands a replica, and the join runs against that replica in the active engine.

**Materialize** sources have no federated connector either. Provisa fetches their data, on startup or at query time, and caches it as Parquet in S3 or in PostgreSQL. From there the federation engine can reach it for cross-source queries.

---

## All Sources

Provisa registers **73** source types, the ones the Sources form offers. The tables below cover all 73; the index is the count.

| # | Group | Source types |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Cloud data warehouses](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data lake / open table formats](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Code sources](code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Push receiver](#push-receiver) | `ingest` |
| 45–46 | [Graph & semantic](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [File-based](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observability & other](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [Enterprise SaaS](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [API sources](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [Data quality checkers](#data-quality-checkers) | `soda`, `great_expectations` |
| 71 | [Data profiler](#data-profiler) | `data_profiler` |
| 72 | [Kaggle datasets](#kaggle-datasets) | Kaggle (staged via Sources form; registers as a `files` source — see [Kaggle datasets](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (registers as a `files` source that crawls the named pages — see [Wikipedia](#wikipedia)) |

Reference for every source type Provisa supports. "Direct driver" means single-source queries execute against the source natively (sub-100ms). "Connector Name" is the federated connector used when the source participates in multi-source JOINs.

### RDBMS

| Source Type | Direct Driver | Connector Name | Dialect | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Yes |
| `mysql` | aiomysql | mysql | mysql | Yes |
| `mariadb` | aiomysql | mariadb | mysql | Yes |
| `singlestore` | — | singlestore | singlestore | Federated |
| `sqlserver` | aioodbc | sqlserver | tsql | Yes |
| `oracle` | oracledb | oracle | oracle | Yes |
| `duckdb` | duckdb | memory | duckdb | Yes |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Yes |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Yes |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Yes |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Yes |
| `firebird` | — | — (DuckDB extension) | — | No |
| `airport` | — | — (DuckDB extension) | — | No |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | No |
| `hiveserver2` | impyla (HiveServer2 over Thrift) | — | hive | No |

Wire-compatible databases reuse a base wire's JDBC driver, native async driver and dialect. CockroachDB, YugabyteDB and Greenplum ride the PostgreSQL wire, and TiDB rides the MySQL wire. They need only registry entries and no new connector code.

`firebird` (Firebird 3/4/5) and `airport` (Arrow Flight server) are registered source types that DuckDB community extensions reach in place when DuckDB is the active engine. They have no direct driver and no federated connector.
#### `saphana`

SAP HANA is read through its own driver and replicated into the active engine's store, so it is reachable on every engine. The form takes host, port (default 39015), username, password and database; on a multi-tenant system the database names the tenant. Register Table lists the schemas the user can read, with HANA's system schemas left out.

#### `hiveserver2`

A HiveServer2 endpoint read directly over Thrift, for a Hive deployment where the endpoint is the only thing reachable. It is distinct from `hive` and `hive_s3`, which read the warehouse's files and metastore and never go through HiveServer2. Rows are read through the endpoint and replicated into the active engine's store. The form takes host, port (default 10000), username, password and database; `federation_hints.auth_mechanism` selects the authentication (default `PLAIN`, for example `GSSAPI` for Kerberos).

### Cloud Data Warehouses

| Source Type | Direct Driver | Connector Name | Dialect | Mutations | Notes |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federated | Reads via snowflake-connector-python; lands replica; `account`/`warehouse`/`role` in `federation_hints` |
| `bigquery` | — | bigquery | bigquery | Federated | No DirectDriver; reaches via federation engine or BigQuery engine ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federated | Reads via databricks-sql-connector (Cloud Fetch, Arrow); lands replica; `http_path` required in `federation_hints` |
| `redshift` | — | redshift | redshift | Federated | — |
| `athena` | AthenaDriver | — | athena | Partial (INSERT always; UPDATE/DELETE/MERGE on Iceberg only) | Serverless S3 query; single-source runs whole in Athena; cross-source lands a replica; `region` required in `federation_hints` |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federated | Microsoft Fabric Warehouse; T-SQL over TDS, Azure AD auth; lands replica |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federated | Azure Synapse SQL; T-SQL over TDS, Azure AD auth; lands replica |
| `trino` | SQLAlchemyDriver | — | — | Federated | Remote Trino/Presto coordinator read via the SQLAlchemy trino dialect; lands replica on any engine |

#### `athena`

Amazon Athena is a serverless query service over data in S3, billed per byte scanned. A statement on Athena's tables alone goes straight to Athena, with filters, column lists and `LIMIT` pushed down, so Athena scans and bills only the bytes that statement needs. A statement that joins an Athena table with any other source first lands a full replica from Athena. That reads the whole table, and Athena bills for it; the join then runs against the replica. The source's Replicate and load-protection settings decide how often a landing runs, and this note is the only warning about that cost.

**Discovery.** Register Table calls Athena's metadata API: `ListDatabases`, `ListTableMetadata` and `GetTableMetadata`. These calls run no query, so listing databases and tables and reading a table's columns cost nothing per byte. A table's columns are fetched only when you select that table, with partition keys after the regular columns. `array<…>`, `map<…>` and `struct<…>` become `text`, and an unknown type is refused by name.

**Source fields.**

| Field | Where | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `database` | source | Yes | — | The Glue database queried by default |
| `username` | source | No | — | AWS access key ID; may be `${secret:…}` or `${env:…}` |
| `password` | source | No | — | AWS secret access key; may be `${secret:…}` or `${env:…}` |
| `region` | `federation_hints` | Yes | — | AWS region, e.g. `us-east-1` |
| `workgroup` | `federation_hints` | No | `primary` | Athena workgroup |
| `s3_staging_dir` | `federation_hints` | No | workgroup default | Where Athena writes query results; required when the workgroup does not set one |
| `catalog` | `federation_hints` | No | `AwsDataCatalog` | Athena data catalog |
| `role_arn` | `federation_hints` | No | — | IAM role ARN assumed for every call |

Both `username` and `password` empty means boto3's own credential chain: instance role, environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), or a named profile. One without the other is refused.

**Writes.** Every registered table accepts `INSERT`. `UPDATE`, `DELETE` and `MERGE` work only on an Iceberg table, and the format is recorded at registration (`registered_tables.table_format`). On a Hive table, a write that asks for `UPDATE`, `DELETE` or `MERGE` is refused, naming the table and the operation. Athena returns no written rows, so a `RETURNING` clause is refused and GraphQL offers no mutation fields for these tables. The write paths are `INSERT`, `UPDATE`, `DELETE` and `COPY`. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` and `TRUNCATE` are refused, naming the form.

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

With a role assumed per call (credential chain for the base session):

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```

### MotherDuck

`motherduck` is a MotherDuck-hosted DuckDB database. On the DuckDB engine it is attached in place; on the Postgres engine it stays the `pg_duckdb` foreign data wrapper connector.

**Connection.** `database` names the MotherDuck database, or a share by its share address. `password` is the MotherDuck token, resolved like any source secret.

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

The DuckDB engine issues `ATTACH 'md:<database>'`; for a share, `database` is the share address (`_share/...`), so the attach is `md:_share/...`.

- **Token handling.** The engine signs in with `SET GLOBAL motherduck_token = ?`, the token passed as a bound value. It is in no statement's text, so no log, trace or error carries it.
- **No token.** A source with no `password` is refused by name before anything is attached (`a MotherDuck source signs in with its token (the password field); none is set`). Without the check, the extension would start an interactive sign-in and wait about a minute. A source with no `database` is refused too.
- **One account per engine.** One engine signs in to one MotherDuck account. A second MotherDuck source with a different token is refused by name: "one DuckDB engine reads one MotherDuck account".
- **Writes** go through the engine, as on the Postgres engine.

**Hybrid (dual) execution** needs no setting. On the DuckDB engine every source is attached on one connection, so a statement joining a DuckDB file source's table with a MotherDuck table runs as one DuckDB statement, and MotherDuck decides where each part runs.

> **Data movement.** Under dual execution MotherDuck may move local rows to its side for the parts it runs. Such a statement can therefore send data from the local file into the organization's MotherDuck account. Do not join a file holding data that must stay on the node with a MotherDuck source unless sending it to that account is acceptable.

A MotherDuck source is served by the DuckDB and Postgres engines only. No other engine has a connector for it, and nothing lands its tables as replicas there.

### Analytics / OLAP

| Source Type | Direct Driver | Connector Name | Dialect | Mutations | Notes |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federated | Reads via clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` for TLS |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | HTTP (native engines) | elasticsearch (Trino) | — | No | On Trino the connector reads it, its properties from the type's mapping DSL; on every other engine Provisa reads the index over HTTP (indices and mapping for Register Table, a scroll read for landing) and lands the rows |
| `pinot` | — | pinot | — | No | Trino `pinot` connector; `pinot.controller-urls` = host:port of the Pinot controller |

### Data Lake / Open Table Formats

These source types are federation-only. They have no direct driver and no dialect.

| Source Type | Connector Name | Time Travel | Notes |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Yes (`as_of` argument) | — |
| `delta_lake` | delta_lake | Yes (`as_of` argument) | — |
| `hive` | hive | No | — |
| `hive_s3` | hive | No | S3-backed Hive |

### NoSQL

`mongodb`, `cassandra`, and `redis` have Trino connectors (`redis` builds its properties from the type's mapping DSL).

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra (Trino); CQL read over cassandra-driver on every other engine | No | Keyspaces are schemas; Register Table lists a keyspace's tables and types columns from the cluster's schema metadata (partition keys as primary keys); the `cassandra` extra installs the driver |
| `redis` | redis (Trino); HTTP-free redis-py read on every other engine | No | A key prefix `<table>:*` is a table and a hash is a row; Register Table lists the prefixes present and types a prefix's columns from its hashes (a `mapping.tables` entry overrides pattern, key column, value type and columns) |

**Redis on the hosted engines.** Redis is not available where the organization's engine is one of the hosted platform's provisioned engines. Trino reads a Redis catalog's tables from description files in a directory of the catalog's own, and a hosted engine cannot be given one while it runs. Saving or applying a Redis source there is refused by name (HTTP 422, `sources.redis_needs_engine_files`); a self-hosted deployment reads Redis as described above.

### Qdrant

`qdrant` is read and written by Qdrant's own client, `qdrant-client`. Qdrant is **live-only**: a property of the source type, not a setting. Its vector index and Query API are what it answers with, and no replica can answer the same, so its tables are never replicated. There is no Replicate choice for a Qdrant source or its tables; a `replicate` value or `row_materialize` on one is refused.

**Connection.** `host` is the instance's URL with its port (`https://<cluster>.cloud.qdrant.io:6333`, or a self-hosted `http://qdrant:6333`); `password` is its API key, a secret reference resolved when the source is read, empty for an instance without one. `federation_hints.prefer_grpc: true` reads over Qdrant's gRPC port instead of REST. The source has one schema, `default`.

**Collection tables.** A collection is a table. Register Table offers the collection's columns: `id` (the point id, its key, path `@id`), the payload's fields (an indexed field typed by its index, any other as the sampled points suggest; nested fields flattened with their path), and one `json` column per vector (path `@vector`, or `@vector.<name>` for a named vector), offered unselected. A vector column names the registered embedding model it holds embeddings of, chosen from the models whose dimensions are the vector's size (in Register Table, and on the editor's Type and keys tab); choosing a model registers the vector.

**Statements on one Qdrant source.** A statement that reads only one collection runs in Qdrant when Qdrant's API states it exactly:

- payload conditions become a Qdrant filter (`=`, `<>`, `IN`, ranges on numbers and dates, `IS NULL`, `AND`, `OR`, `NOT`); SQL's NULL semantics are kept, so `lang <> 'en'` skips points with no `lang`;
- a condition on `id` alone (`=` or `IN`) is one `retrieve`; any other read is a filtered `scroll`;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` is one `query_points` over a vector whose distance is Cosine, the filter applied before the top-k. A text query is embedded by the vector's model. The answer carries a warning that the top-k is approximate, chosen through Qdrant's index.

Anything else (an aggregate, a `GROUP BY`, a computed or masked column, an ordering other than most similar first) is answered by the engine, as below.

**Statements the engine answers.** When a statement joins a collection to another source, or computes over its columns, the collection's part of it is read live from Qdrant for that statement and handed to the engine as rows. The part is the collection's columns, the statement's conditions on the collection alone (its row rules among them), and, when the collection is read alone in its `SELECT`, that `SELECT`'s similarity ordering and `LIMIT`. A part Qdrant cannot state exactly is refused, naming the table and why. A part with no condition, no key and no top-k would read the whole collection and is refused too. A condition on a parameter (`$1`) is left to the engine.

**Writes.** `INSERT`, `UPDATE` and `DELETE` on a collection run in Qdrant:

- `INSERT … VALUES` names the `id` column and writes new points. An `id` the collection already holds is refused by name. The check reads the ids first, so it is not atomic with the write.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` is an upsert. Columns the statement does not set keep their values.
- `UPDATE` and `DELETE` act on the points their `WHERE` selects, as a Qdrant filter; a condition with no filter form is refused.
- A text written to a vector column is embedded by the column's model. A vector written to it must have the model's dimensions.

**Search tables.** Vector search is a search table: its `query_template` holds a Query API request.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` is always an argument; `using`, `filter`, `limit`, `score_threshold` and `model` are optional. Any value in `using`, `filter`, `limit` or `score_threshold` that is exactly `"@name"` is an argument too.
- Each argument is a parameter of the table (`query_arg`), converted strictly to its declared type and placed into the request as a JSON value, never into its text. The query's argument is `array(double)` and required.
- A caller passes arguments the way it passes any parameter: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` in SQL, field arguments in GraphQL.
- An optional argument not given filters nothing: its condition is left out of the filter. With no `limit` a search returns Qdrant's default, 10 points.
- The filter is applied by Qdrant before its nearest points are chosen, so it is exact. Any other condition in the statement narrows the points the search returned afterwards; it does not fetch more.
- `score` is the collection's own metric for the vector searched: for a Cosine collection, the cosine similarity. Register Table lists the collection's vectors with their size and distance.
- `model` names a registered embedding model (`vector_models`, admin → AI Models). A text given for the query is then embedded by that model, and a vector given for it must have that model's dimensions. With no `model`, the query must be a vector.

A search is read per statement under its arguments, its rows kept under those arguments for the table's TTL. A search table is read-only.

### Streaming

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Federated Kafka connector; schema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), manual definition, or sample inference | No; a table's live output publishes to Kafka |
| `websocket` | External WebSocket feed — connect, subscribe, receive events; results materialized | No |
| `rss` | RSS 2.0 / Atom feed — poll, watermark by pubDate/updated; results materialized | No |

**Kafka on the hosted engines.** On the hosted platform's provisioned engines a Kafka source must name a schema registry (`schema_registry_url`). One whose topics' columns only the model declares is refused by name when saved or applied (HTTP 422, `sources.kafka_needs_schema_registry`), since its table descriptions would need a directory the running engine cannot be given. SASL credentials reach those engines in memory (see [Security](security.md#the-federation-engine-trino)).

### Push Receiver

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | External services POST JSON events; results materialized | No |

### Graph & Semantic

| Source Type | Mechanism | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via HTTP API, results cached in PostgreSQL | No |
| `sparql` | SPARQL 1.1 POST, results cached in PostgreSQL | No |

### File-Based

Three mechanisms cover files. Single-file sources and the file crawler use the `path` field instead of `host`/`port`. The third, [Logs](#logs), names its files by a glob in its `mapping` and reads only files on the Provisa server.

**File-database and single-file sources.** A `sqlite`, `duckdb`, `csv` or `parquet` source points `path` at one file. (A `duckdb` source keeps its location in the form's `database` field.)

| Source Type | Locations | Mutations |
| --- | --- | --- |
| `sqlite` | local path, `https://`, `s3://`, `gs://`, `az://` | Yes on a local file; none on a remote one |
| `duckdb` | local path, `https://`, `s3://`, `gs://`, `az://` | Yes on a local file; none on a remote one |
| `csv` | local path, `https://`, `s3://`, `gs://`, `az://` (DuckDB engine) | No |
| `parquet` | local path, `https://`, `s3://`, `gs://`, `az://` (DuckDB engine) | No |

A path with any other scheme (`ftp://`, say) is refused: "DuckDB reads no file at a `ftp://` location".

**Credentials for a remote location.** Put them in the source's `federation_hints`. Each is scoped to the path's own bucket or container, so a source's credentials are never used for another source's location. A source that gives none is read as a public location.

| Location | `federation_hints` keys |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; optional `region`, `endpoint` (an S3-compatible store such as R2 or MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (an HMAC key) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Remote SQLite and DuckDB files are read-only.** The DuckDB engine attaches them in place with `READ_ONLY`, and a table on one offers no insert, update or delete. On an engine that does not attach SQLite, a remote SQLite file is read through a scratch DuckDB connection that attaches it read-only, and its rows land as usual.

**Uploading a file.** On the source form of a `sqlite`, `duckdb`, `csv` or `parquet` source, upload the file instead of typing a path. The file streams to disk, never held in memory, into the organization's folder under `files/<source id>/<file name>`, and the source's path is set to it.

- **Replace.** Uploading again takes the earlier file's place, whatever its name, and the source is provisioned again so the next read sees the new file.
- **Remove.** Removing the source removes its uploaded file. Nothing else removes it.
- **Who.** Only roles with the `source_registration` capability may upload.
- **Size limit.** The setting `sources.max_upload_bytes` (environment `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) caps the file, 1 GiB (1073741824 bytes) by default, minimum 1, read when an upload begins. A larger file is refused with HTTP 413 naming the limit, and none of it is kept.
- **Path.** While the path is the source's upload, the form's path field is read-only.
- **Names.** The source id and file name may use letters, digits, `.`, `_` and `-` only.
- **Storage.** An upload is stored like any org file: under the org's folder, encrypted when the org's folder is, and seen by every node on a shared mount.

**Writes to a local SQLite or DuckDB file, and locks.** A write takes the file's lock and never waits for it. When another process or node holds the file, the write is refused, naming the source and that its file is locked.

- **SQLite.** The write sets `PRAGMA busy_timeout = 0`, so a locked file fails at once.
- **DuckDB.** DuckDB shares a file among read-only opens only. So the engine's attach and the source's own connection hold the file read-only, and every node reads it at once. A write pauses this process's holders, opens the file read-write for that statement alone, and closes it. If the open cannot take the lock, the write is refused.

**Shared mounts.** On an encrypted org folder whose mount is shared by several nodes (setting `org_files.shared_mount`), a write to a SQLite or DuckDB file in it is refused outright, naming the source. The folder-encryption layer (gocryptfs) does not carry file locks to the other nodes, so nothing would keep two nodes from writing at once. Reads still work.

A folder shared without encryption must be mounted with locks that reach every node: an NFS mount with locking on, not `nolock` and not `local_lock`. No probe checks this; it is a deployment requirement.

**File Crawler (`files` source).** It points `path` at a glob, crawls it recursively, and registers the directory as a federated catalog of tables. It reads many formats over many transports:

| Formats | Transports |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, and documents converted to tables — HTML, Markdown, DOCX, PPTX | Local filesystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST and Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Any `files` source can also declare a `crawl` in its mapping, with the same settings listed under [Wikipedia](#wikipedia): the source then reads web pages first and lands each HTML data table and each linked data file in its directory as a table.

On the DuckDB engine, `files` is read natively, with a `read_csv_auto` scanner view per `<table>.csv` under the resolved directory. On an engine with no `files` connector of its own, rows land through the same Calcite pgwire server bundled with the connector (`pgwire-file`) that sharepoint and splunk use. See [Enterprise SaaS Connectors](#enterprise-saas-connectors) below.
#### Logs {: #logs }

A `logs` source turns log files on the Provisa server into one table. You name the files by a full path or a glob, and pick the format their records are written in. Each record becomes a row. A record the format cannot read is kept, with its raw text and the reason, never dropped.

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

The mapping takes four keys and refuses any other by name: `glob` and `format` (both required), and `pattern` and `record_start`, which only the `regex` format takes.

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

In the Sources form, a Logs source is "Log files" under File. The **Preview** button shows the files the glob matches, the columns the format gives, and the first 20 rows from the newest file, before anything is saved.

**The glob.** It must be a full path; `~` is expanded, and a relative path is refused ("name the files by their full path"). It is matched with Python's `glob` in non-recursive mode, so `**` behaves like `*` and matches within one directory level. Only regular files are taken, in name order. An empty file is skipped until it has bytes. The glob may name an environment variable, as in `${env:LOG_DIR}/*.log`. The source keeps the reference as written and resolves it each time the files are read; a variable that is not set is refused by name.

**Every row's own columns.** These come before the format's columns:

| Column | Type | Holds |
| --- | --- | --- |
| `_file_id` | text | The file's identity: a checksum of its first 4096 bytes, decompressed |
| `_file_name` | text | The path the record was read under |
| `_line_offset` | bigint | Byte offset of the record's first line in the decompressed text |
| `_entry` | bigint | Which entry of the record the row is: 0, except for a batch record such as a CloudTrail file's `Records` |
| `_raw` | text | The record as written |
| `_parse_error` | text | Why the record, or one of its values, could not be read; NULL when it was |
| `_landed_at` | timestamp | When the row landed |

`_file_id`, `_line_offset` and `_entry` are the row's key, so a record read twice is one row.

**Formats and presets.** The format select groups the formats by family; the last choice, `regex`, is your own pattern.

| Family (form label) | Format names |
| --- | --- |
| Common formats | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Databases and runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Containers and Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Network security monitoring | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Web servers and proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Your own | `regex` |

That is 56 built-in formats and `regex`.

**How a format makes columns.** Formats get their columns in one of four ways.

- **Fixed columns.** Most formats state their columns and types. `rfc3164`, for example, gives `timestamp`, `facility`, `severity`, `host`, `process`, `pid` and `message`; `facility` and `severity` are computed from the `<PRI>` value. A BSD syslog line carries no year, so the year is the file's last-write year, or the year before when the line's month is later than that write.
- **Fields named by the record.** `jsonl` and `logfmt` take their columns from the fields seen in the files' first lines (setting `logs.preview_lines`, default 1000). Each column gets the narrowest type every value seen fits: boolean, then integer, float, timestamp and json, else text. A field a later record carries that no column has goes into an `extra` json column.
- **Fields named by a header.** `w3c` and the presets built on it read each file's `#Fields:` line; `date` and `time` join into one `timestamp` column. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` and `zeek_tsv` read `#` header lines the same way, and `aws_vpc_flow` reads a header line that starts with `version`. Where a header line was read stays with the file, so a file read on later keeps its fields.
- **`regex`.** Each named group, such as `(?P<level>…)`, is a column of type text.

The Register Table screen lists these columns with their types and marks the key. When the rows are read, each value is cast to the type its column has in the registered table, so a type changed there is the one applied. A value its column cannot hold becomes NULL and its reason goes in `_parse_error`.

A record longer than `logs.max_line_bytes` (default 65536) keeps its first bytes in `_raw` and is not parsed; `_parse_error` gives its length. A file that starts with a UTF-8 or UTF-16 byte-order mark is read in that encoding, and any other file as UTF-8.

**Records that span lines.** A format with a record-start pattern makes a record from a line that matches it plus every following line that does not. A stack trace stays with its exception, and a slow-query entry stays whole. Lines before the first match are a record of their own; the format usually refuses it, and the row is kept. The built-in formats with a record start are `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` and `azure_monitor`. With `regex`, set `record_start` yourself.

Two formats read their records themselves instead of by line. `kubernetes_cri` joins the partial (`P`) lines the runtime split into one record, and `windows_evtx` reads the binary `.evtx` file.

A file that is still being written is read to its last complete line. For a multi-line format it is read to the start of its last record, which the next line may continue. Once the file has gone 2 seconds without a write, that last record is taken as whole.

**Rotation.** A file is known by a checksum of its first 4096 bytes, decompressed, not by its name. When `system.log` is rotated to `system.log.0` and then compressed to `system.log.0.gz`, every copy is the same file. Reading continues from the byte the last read reached, so no line is read twice.

- Compression is recognised by the file's first bytes, never its name: gzip and bzip2. A compressed file is treated as complete.
- A rotated copy and its compressed twin, both matched, are read once, from the compressed one.
- A file found shorter than the offset already read was truncated: it is a new file, read from its start. The rows of the old one stay.
- Files shorter than 4096 bytes that start alike are told apart by inode, so they stay two files.
- A file the glob no longer matches is forgotten; its rows stay as history.

**Refresh and live delivery.** A Logs table is a replica. Nothing watches the files. Instead, when the source's change signal is `probe` or `ttl_probe`, its probe digests each matched file's path, inode, size and last write; when the digest differs, a refresh is asked for. The probe types a Logs source takes are `hash` and `none`.

A refresh reads only what is new. Each file is read from where the last pass reached, and rows are written by their key, 5000 at a time, before the new position is kept. A pass that stops between the two steps reads those records again, which leaves the same rows. Each pass stamps its rows in `_landed_at`, the column live delivery uses as its watermark. A changed table definition is a whole rebuild that reads every file from its start.

**Allowed roots.** The operator setting `logs.allowed_roots` lists the directories a glob may resolve into. Its default is `["/var/log"]`; it is a list on the Security card of Admin → Settings, also set by `PROVISA_LOGS_ALLOWED_ROOTS`, and a change asks for confirmation. Each root is resolved after `~` expansion and symlinks.

Two checks apply. The fixed part of the glob, up to its first wildcard, must resolve inside a root; this is checked when the source is saved and each time it is previewed or read. Each matched file must also resolve inside a root once its symlinks are followed, so a symlink out of `/var/log` is refused. The refusal names the path, what it resolved to and the allowed roots:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

On save the code is `schema.logs_path_refused`; on preview it is a 403 with `logs.path_refused`.

**Hosted (SaaS) refusal.** A deployment that holds many organisations reads no log file of its own. Every path check refuses first, on save, preview and read, with "`<path>`: a server that holds many organizations reads no log file of its own". The files are the server's, not any one organisation's, so no tenant may read them. The deployment says whether it holds many organisations from its multitenancy state.

#### Kaggle datasets {: #kaggle-datasets }

Kaggle is a place to download data files. A Kaggle dataset you add registers as a `files` source, and you query it the same way as any other `files` source.

**Adding a dataset.** Open Sources → Subscriptions → Kaggle. The form works in two steps, because Kaggle's own dataset search needs a token, so the picker can't appear until yours checks out.

1. **Token.** Enter a Kaggle API token and click "Validate". Validation calls `POST https://www.kaggle.com/api/v1/datasets/create/new` with an empty body. A `401` means the token is invalid, and any other response means it's valid. Kaggle checks the empty body before it creates anything, so nothing is saved.
2. **Picker.** A search field queries `GET /api/v1/datasets/list` as you type, and each result shows the dataset's title and description. Select one, then click "Add Dataset".

"Add Dataset" downloads the bundle from `GET /api/v1/datasets/download/{owner}/{ref}`, unzips its CSV and Parquet files into `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`, and creates **one** `files` source whose `path` is that root directory. The server never stores the token.

Then register the tables you want on the normal Register Table screen. Each file is listed as its own table, as in any other `files` source.

**Only CSV and Parquet.** A bundle that contains a `.sqlite` or `.db` file is refused with an error that says so. Only CSV and Parquet files are staged.

**Refreshing.** When Kaggle publishes a new version of a dataset, call the `refreshKaggleSource` GraphQL mutation with the source ID and a valid token. It stages the files again in place and clears the file endpoint's cache, so the next query picks up any schema change. The `kaggle_owner` and `kaggle_ref` kept in `federation_hints` when the source was created say which dataset to fetch.

**No YAML of its own.** You create a Kaggle source only through the Sources form. Exported to YAML, it appears as `type: files` with `kaggle_owner` and `kaggle_ref` in `federation_hints`. Fetching from Kaggle again takes the refresh in the UI or the `refreshKaggleSource` mutation. In an air-gapped environment, point the YAML `path` at a directory you staged yourself.

#### Wikipedia {: #wikipedia }

Wikipedia crawls the pages you name. Each data table on a page, and each linked CSV, TSV, Excel, JSON or Parquet file, becomes a table of the source. Like Kaggle, it registers as a `files` source with a `crawl` in its mapping. Adding it registers nothing: a steward registers the tables wanted through the normal Register Table screen.

**Setup fields.**

| Field | Default | Meaning |
| --- | --- | --- |
| Pages to start from | none (required) | One per line: a page title, or the address of a page in the chosen edition |
| Language edition | `en` | The edition's code, for example `en`, `de` or `ja` |
| Link depth | `1` | How many links away from a starting page to read; `0` reads only the starting pages |
| Page limit | `25` | The most pages one crawl reads |
| Contact | empty | Sent with each request so Wikipedia can reach you, as its policy asks |
| Follow "See also" links | off | Off leaves that section out of the article |
| What this edition calls its "See also" section | empty | Asked only when the box above is off and Provisa cannot name the section for the chosen edition. The section's title as a page shows it, for example `Voir aussi` |
| Directory for the crawl's files | empty | Where tables and data files are written. Empty: `<PROVISA_DATA_DIR>/crawl/<source id>` |

Provisa knows the "See also" heading of these editions: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. For any other edition with "See also" off, the form asks what the edition calls that section; type its title as a page shows it (for example `Voir aussi`). A source saved without it is refused (`wikipedia.see_also_heading_needed`).

**Which links are followed.** A link is chosen by what the page's own markup says it is, never by a name in its address: such names differ in every language edition and also occur in ordinary titles.

- Followed: links to other articles that sit in the article's own text, lists and data tables. The markup mark is `a[rel='mw:WikiLink']`.
- Not followed: pages not yet written, the page itself, book-source lookups, file pages, other wikis and external sites.
- Not read at all (removed before links and tables are collected): navigation boxes, information boxes, notes and citations, categories, the table of contents, images and maintenance messages.
- Not read unless you turn the option on: the "See also" section.

Only tables that editors mark as data tables (`table.wikitable`) become tables, and a table with fewer than 2 rows is not offered.

**Advanced crawl settings.** The form keeps these folded under "Crawl settings" and shows each with its default; leave a field empty to keep it. In a `files` mapping the same names go under `crawl:`, and a name outside this list is refused.

| Setting | Wikipedia default | Meaning |
| --- | --- | --- |
| `start_urls` | the pages you named | Pages the crawl starts from |
| `max_depth` | `1` | Link depth |
| `max_pages` | `25` | Page limit |
| `request_delay` | `1 seconds` | Delay between requests |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, with your contact appended | The name the crawl gives itself |
| `content_selector` | `#mw-content-text .mw-parser-output` | CSS selector: links and tables are read only inside it |
| `remove_selectors` | the list of parts described above | CSS selectors for parts left out. Stating it replaces the whole list, including the "See also" rule |
| `link_selector` | `a[rel='mw:WikiLink']` with exclusions | CSS selector a link must match to be followed |
| `link_exclude_patterns` | empty | Regular expressions; a link whose address one finds is not followed |
| `follow_external_links` | `false` | Follow links to other sites |
| `allowed_domains` | not set | Domains the crawl may visit |
| `table_selector` | `table.wikitable` | CSS selector a table must match |
| `generate_tables_from_html` | not set | Whether HTML tables become tables |
| `html_table_min_rows` | `2` | Fewest rows in a table |
| `html_table_max_rows` | not set | Most rows in a table |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Linked file types that become tables |
| `html_cache_ttl` | `1 days` | How long a crawl is kept |
| `max_html_size` | not set | Largest page read, for example `10MB` |
| `max_data_file_size` | not set | Largest data file read, for example `100MB` |

**Table naming.** A crawled table is named `<page>__<name>`. The name is the table's caption or, when it has none, the last heading above it; a table with neither is `table_1`, `table_2` and so on. Both parts are lowercased, with every run of other characters turned into `_`. For example, a table under the heading "Tallest buildings" on the page `List_of_tallest_buildings` becomes `list_of_tallest_buildings__tallest_buildings`. A linked data file keeps its own name.

**Refreshing.** A crawl is kept for the cache period, `1 days` by default (`html_cache_ttl`). Pages are fetched again only after that time has passed.

### Observability & Other

`prometheus` has a Trino connector (properties built from the type's mapping DSL). `google_sheets` is a registered source type with no Trino connector and materializes through the API cache pipeline.

| Source Type | Connector Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialized) | No |
| `prometheus` | prometheus | No | A metric is a table and a sample is a row (`timestamp`, `value`, one column per label); on every engine without a live connector Provisa reads the HTTP API — metric names and labels for Register Table, `query_range` over the table's range for landing

### Enterprise SaaS Connectors

Google Workspace and Microsoft 365 are read by loaders of their own; see [Google Workspace and Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, cloud inventory and Splunk are read through drivers that run as their own process and speak the PostgreSQL wire protocol (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`), so none of them has a direct driver. On the DuckDB engine the driver's endpoint is attached live through the postgres extension: Register Table lists the connector's tables from the attached catalog, queries read the connector in place, and filters and projections are pushed down to it. Every other engine lands the rows in the materialize store for federation. The driver bundles are fetched per OS and architecture from a pinned release (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64). An operator can instead name a bundle folder built for this host, per connector, under Admin → Settings → Connector servers (`connectors.<connector>.bundle_dir`); see [Admin](admin.md). All five Trino connectors (Jira has none) always match names case-insensitively, as each product itself does.
#### `sharepoint`

SharePoint lists are enumerated as schemas and exposed as queryable tables. Two auth methods: `CLIENT_CREDENTIALS` (default) and certificate-based via a PFX certificate. Secret values in `mapping` are resolved through the secrets engine before reaching the connector.

| Source field | Connector property | Notes |
| --- | --- | --- |
| `base_url` or `host` | `site-url` | SharePoint site URL |
| `username` | `client-id` | Azure app client ID |
| `password` | `client-secret` | Azure app client secret |
| `database` | `tenant-id` | Azure tenant UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (default) or `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | PFX path when `auth_type: CERTIFICATE` — must be ABSOLUTE |
| `mapping.certificate_password` | `certificate-password` | PFX password — the key must be present, empty string for a password-less PFX |

Certificate auth on the non-Trino engines carries two extra rules, both enforced when the Calcite pgwire server's `model.json` operand is built. `certificate_path` must be absolute: the server runs with its bundle directory as the working directory, so a relative path resolves inside the runtime-deps cache and the PFX is not found. `certificate_password` must be present in `mapping` even when the PFX has no password, in which case it is the empty string. The Calcite adapter rejects a null password outright, and an absent key is treated as a config error rather than silently read as an empty password. A missing or relative value raises `MissingConnectorConfig` naming the field.

When the connector does not expose `information_schema.columns`, register the table with explicit column definitions (obtained from the Microsoft Graph API) via the `registerTable` mutation.

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

Certificate auth, with the absolute path and the always-present password:

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

Every sObject the credential can query is a table the steward may register (`Account`, `Opportunity`, custom `*__c` objects); nothing is registered automatically. Columns and types come from the sObject's describe. Filters, projections, sorts and limits are pushed down as SOQL.

A Salesforce source names the org's My Domain login URL and one complete credential set, chosen by `mapping.auth_type`. The login URL is the full `https://…` URL (for example `https://acme.my.salesforce.com`); a bare host or any other scheme is refused, naming the field. An incomplete set is refused when the source is used, naming the missing value. Each secret may be a `${secret:…}` or `${env:…}` reference. A secret typed as a literal is stored in the org vault and the source keeps its reference.

| Source field | Connector property | Notes |
| --- | --- | --- |
| `base_url` or `host` | `login-url` | The org's My Domain URL (Setup, then My Domain). `login.salesforce.com` does not work for the client-credentials flow |
| `username` | `client-id` | The connected app's consumer key |
| `password` | `client-secret` | The connected app's consumer secret |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (default), `USERNAME_PASSWORD` or `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | `USERNAME_PASSWORD` only, alongside the consumer key and secret |
| `mapping.security_token` | `security-token` | `USERNAME_PASSWORD` only; optional |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | `ACCESS_TOKEN` only; no consumer key or secret |
| `mapping.api_version` | `api-version` | Optional, for example `v61.0` |

For `CLIENT_CREDENTIALS` the connected app needs *Enable Client Credentials Flow* and a *Run As* user; every read and write runs with that user's permissions. Orgs created since Summer '23 block the username-password flow by default.

**Reads.** On Trino the source is read through the `salesforce` catalog, which reads only. On every other engine the engine attaches the source's pgwire server and reads it in place.

**Writes.** `INSERT`, `UPDATE` and `DELETE` on a registered sObject are mutations: governed, audited and handled per environment as any other mutation is. On every engine, Trino included, a write runs on the source's pgwire server; on Trino that server runs for writes alone. Each statement is sent to Salesforce and committed when it runs, so it cannot be rolled back. The server does not return the rows it writes: a statement with a `RETURNING` clause is refused, and GraphQL offers no mutation fields for these tables; SQL `INSERT`, `UPDATE`, `DELETE` and `COPY` are the write paths. A write is one plain statement on one table: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` and `TRUNCATE` are refused, naming the form. A SharePoint source is written the same way.

**Startup.** Before it listens, the source's server reads the columns of every sObject, about four minutes for an org with 1,200 of them. That is paid once: the describe results are kept in the source's own state directory under the instance's data directory. The server is started when the source is registered or loaded. On Trino the `salesforce` catalog keeps its own describe results, one directory per source under `/data/trino/cache/salesforce-describe` in the coordinator, so the cost is paid once there too when that path is a volume that outlives the container: the compose stack's `trino_cache` volume, or the Helm chart's coordinator cache volume (`trino.coordinator.cache.persistence`). Until it listens, Register Table shows the connector as starting and a write is answered with the same message instead of waiting.

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Username-password flow:

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

Every table of the instance is available for registration, and nothing is registered automatically. A busy instance has thousands of tables, so Register Table groups them by theme. Each theme is a ServiceNow package's display name, or "(no package)" for a table that belongs to none. Each table is listed as `name — label`. Selecting a table reads that table's columns from the instance; listing the tables does not.

A ServiceNow source names the instance's full `https://…` URL and a username and password. The URL is taken from `base_url` or `host`; a bare host or any other scheme is refused, naming the field (`instanceUrl`). A missing username or password is refused, naming it. Each value may be a `${secret:…}` or `${env:…}` reference. A secret typed as a literal is stored in the org vault and the source keeps its reference.

| Source field | Adapter property | Notes |
| --- | --- | --- |
| `base_url` or `host` | `instanceUrl` | The instance's full `https://…` URL (for example `https://acme.service-now.com`). A bare host or other scheme is refused |
| `username` | `username` | Basic auth username |
| `password` | `password` | Basic auth password |

**Reads.** On Trino the source is read through the `servicenow` catalog, which reads only. On every other engine the engine attaches the source's pgwire server and reads it in place. Only the selected columns are requested from the instance (`sysparm_fields`). A filter is sent to the instance (`sysparm_query`) only in a form the adapter has verified against a live instance; every other filter, and every sort and limit, is applied to the rows read.

A reference field appears as two columns: `<field>` holds the referenced record's sys_id, and `<field>__display` holds its display value. An empty field value is NULL. A field whose type the adapter cannot map is an error that names the table, field and type.

Table search (natural language) matches each table's name, label and theme. Once a table is registered, its columns join the search.

**Server lifecycle.** The server serves only the tables registered from it. Registering or removing a table restarts the server and re-provisions the source on the engine; a statement that arrives during that restart fails.

**Writes.** `INSERT`, `UPDATE` and `DELETE` on a registered table are mutations: governed, audited and handled per environment as any other mutation is. On every engine, Trino included, a write runs on the source's pgwire server. The server returns the row the instance holds after each write, so a `RETURNING` clause is accepted and GraphQL offers mutation fields for these tables. A write is one plain statement on one table: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` and `TRUNCATE` are refused, naming the form.

**Import sets.** When a table has an active transform map into it, Register Table offers its import sets, and the steward may bind the table to one (a table with several maps names each). A bound table's inserts go through the instance's Import Set API and that map's transform; an unbound table's inserts, and every update and delete, are written to the table directly. A row the transform rejects is refused, naming the row; rows already written stay written. The binding is checked when the table is saved: it must be the import set of an active transform map into the table, on a ServiceNow source.

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

A Jira Cloud site is read and written through its own Calcite adapter. A table of a Jira source is a **form**: one project and one issue type, and the fields its columns read. Register Table lists every form the account can create an issue of, grouped by project; selecting one reads that form's fields from Jira, and listing the forms does not. Every form has the columns `key` (its primary key), `id`, `summary`, `status`, `created` and `updated`, then one column per chosen field, named as the adapter names it: the field's name in snake case (`Story Points` is `story_points`). A field whose name is already a column's is named by its field id (`customfield_10099`), so every field can be chosen. A user field holds the account id; its display name is a field of its own (`assignee.displayName`, column `assignee_name`). Columns Jira writes itself are read only: `key`, `id`, `created`, `updated`, a user field's display name, and a field of a shape the adapter does not know, which it serves as JSON text. Registering one marks it "Read only: Jira sets it" in its description where it has none, and a write that sets it is refused by the server, naming it.

A form's table names its project and issue type in its `arguments` (`project`, `issue_type`), and each column reads the field its `path` names (a field id or a field's name), or the field of its own name. A table registered from the admin gets both from Jira; a configuration states them. A Jira table fixing anything else, or missing either, is refused by name.

| Source field | Adapter property | Notes |
| --- | --- | --- |
| `base_url` or `host` | `baseUrl` | The site's full `https://…` address (for example `https://acme.atlassian.net`). A bare host or other scheme is refused |
| `username` | `email` | The Atlassian account's email |
| `password` | `apiToken` | An API token of that account, made at id.atlassian.com under Security, API tokens |

**Links.** The source's `issue_links` table holds the links between the issues of its forms: `link_id`, `link_type`, `outward_key` and `inward_key`, each link once. A relationship between two forms is declared through it as a junction, its `link_type` the discriminator, so GraphQL and Cypher walk issue to linked issue by link type.

**Reads.** A lookup by key (`key =`, `key IN`) is fetched by key, so a key that names no issue is no row. A date or number comparison is sent to Jira as JQL. Every other filter, and every sort, is applied to the rows read: an equality on text, an option, a status or a user is not sent, since JQL refuses a value that names nothing where SQL returns no rows. A multi-value field (labels, a multi-select, components, versions) is text holding a JSON array, and is written the same way.

On DuckDB, PostgreSQL and ClickHouse the engine attaches the source's pgwire server and reads it in place. On Trino the source has no catalog of its own and is landed through its server.

**Server lifecycle.** The server serves only the forms registered from it, beside its form list, its field list and the links. Registering, changing or removing a form restarts the server and re-provisions the source on the engine.

**Writes.** `INSERT`, `UPDATE` and `DELETE` on a form are mutations, written on the source's pgwire server on every engine. Rows are inserted in batches of 50; an update or delete is made issue by issue, by key. Setting `status` performs the workflow transition to that status, and is refused, naming the reachable statuses, where the issue's workflow has none. An issue with subtasks is not deleted. The server returns each written row by its key, so `RETURNING` is accepted and GraphQL offers mutation fields. On `issue_links`, `INSERT` creates a link and `DELETE` removes one; `UPDATE` is refused.

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

Cloud inventory lays out the resources of Azure, AWS and GCP as one set of tables: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries`, and `compute_security_groups`, which ties a compute resource to its security groups. Each table has one set of columns across the three clouds, and its `cloud_provider` column names the cloud a row came from. The source reads only, and its tables are registered by a steward like any other.

A source names one or more clouds. Each cloud is all or nothing: a cloud with some of its required values and not the rest is refused, naming what is missing, and so is a source naming no cloud. The source reads exactly the clouds it names. Every value lives in `mapping`, and each secret may be a `${secret:…}` or `${env:…}` reference. A secret typed as a literal is stored in the org vault and the source keeps its reference.

| Cloud | `mapping` key | Connector property | Notes |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Required |
| Azure | `azure_client_id` | `azure.client-id` | Required; the app registration's client ID |
| Azure | `azure_client_secret` | `azure.client-secret` | Required; secret |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Required; comma-separated |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Required |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Required; secret |
| AWS | `aws_region` | `aws.region` | Required, for example `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Required; comma-separated |
| AWS | `aws_role_arn` | `aws.role-arn` | Optional; a role to assume for cross-account access |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Required; ABSOLUTE path to a service-account JSON key file |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Required; comma-separated |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Optional; how long a result is reused before the clouds are asked again (adapter default 5) |

The GCP key file is opened by whatever reads the source. On Trino that is the Trino server, so the path must exist there. On every other engine it is the source's pgwire server on the Provisa host, whose working directory is its own state directory, so a relative path is refused.

On Trino the source is read through the `cloudops` catalog. On every other engine the engine attaches the source's pgwire server and reads it in place, as a SharePoint source is read.

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

Splunk search results are queryable as tables (e.g. `internal_server`). The connector URL comes from `base_url`, or is constructed as `https://{host}:{port}` with a default port of `8089`. Auth: when `mapping.use_token` is `true` (the default), `password` is passed as the API token; when `false`, `username` and `password` are passed as separate credentials.

| Source field | Connector property | Notes |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, else `https://host:port` (port default 8089) |
| `password` | `token` or `password` | token when `use_token: true` |
| `username` | `user` | only when `use_token: false` |
| `database` | `app` | restrict to a Splunk app |
| `mapping.datamodel_filter` | `datamodel-filter` | filter to a data model |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | for self-signed certs |

On the pgwire-replica path (every engine but Trino) the same four optional settings become the Calcite `model.json` operand keys `app`, `token`/`username`+`password`, `datamodelFilter` and `disableSslValidation`. The last two take the types `SplunkSchemaFactory` casts them to, a string and a boolean.

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

#### Google Workspace and Microsoft 365 {: #google-workspace-and-microsoft-365 }

`google_workspace` and `microsoft_365` read the mail, calendars and tasks of the accounts a source names, and land them as replicas. They are not Calcite connectors: each has a loader of its own that calls the provider's API, Gmail, Google Calendar and Google Tasks on one side and Microsoft Graph on the other. The form lists them under Enterprise as "Google Workspace (Gmail)" and "Microsoft 365 (Outlook, Exchange)".

**Tables.** Both types produce the same canonical tables: the same names, column types and order. A union of the two needs no cast or rename; a column one provider has no value for is NULL. The tables are listed under the schema `default`, and every row leads with the account it was read from. A source offers the tables of the resources it reads:

| Resource | Tables | Written by statements |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | No |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

The columns are listed in [the canonical mail schema](arch/canonical-mail-schema.md). `events` holds single events, series masters with their recurrence as RFC 5545 lines, and exceptions; it never holds the expanded occurrences of a series.

**Registration.** An organisation's administrator first enters the organisation's client for the platform under Admin → Email: a client ID and secret, plus the directory (tenant) ID or domain for Microsoft 365. Nobody adding a source sees or types it. A source then takes one of two shapes:

- **One mailbox.** The source names one address in `accounts`. Its owner clicks **Connect** on the form and approves the organisation's client. The resulting approval is kept in the organisation's state store, not in the source's settings. On Google Workspace this shape also states `sign_in: google_account`.
- **The organisation's mailboxes.** The source states `mailboxes` as exactly one of `everyone: true`, `group: <address or id>`, or `list: [addresses]`. It holds no credential and nobody connects it. The organisation reads with its own access, and only once the administrator has turned on "Read the organization's mailboxes". On Google that access is a service-account key with domain-wide delegation, plus an administrator address for directory calls. On Microsoft it is the client-credentials grant of the app registration.

With the second shape, one table holds every mailbox's rows. A role that may read the table reads every mailbox, unless a row rule on the `account` column limits it.

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**Settings.** Each module refuses a key it does not read, by name.

| `mapping` key | Type | Meaning |
| --- | --- | --- |
| `resources` | both | Which of `mail`, `calendar`, `tasks` the source reads; at least one |
| `accounts` | both | The one mailbox's address (one-mailbox shape) |
| `mailboxes` | both | `everyone`, `group` or `list` (organisation shape) |
| `sign_in` | `google_workspace` | `google_account` (one-mailbox shape) |
| `mail_since` | both | `YYYY-MM-DD`; nothing received earlier is held |
| `mail_content` | `google_workspace` | `full`, or `headers` for headers and labels only (Gmail's `gmail.metadata` scope). Required when mail is read |
| `mail_search` | `google_workspace` | A search as typed in Gmail's search box |
| `mail_labels` | `google_workspace` | Label names; only mail carrying every one is read |
| `mail_include_spam_trash` | `google_workspace` | `false` by default |
| `calendar_send_updates` | both | Whether a write tells an event's attendees; off by default |
| `calendar_check_days` | `microsoft_365` | Days either side of today the change check looks at; a whole number, 1 or more, required when calendar is read |

Google refuses a search under `gmail.metadata`, so `mail_content: headers` with `mail_search` or `mail_since` is refused.

**Scopes.** A sign-in asks only for the scopes of the resources ticked:

| Resource | Google Workspace | Microsoft 365 (delegated) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, or `gmail.metadata` for headers only | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| every source | `userinfo.email` | `offline_access`, `User.Read` |

For the organisation shape, Microsoft needs the application permissions `Mail.Read` and `User.Read.All`, plus `Calendars.ReadWrite` and `Tasks.ReadWrite.All` for calendars and tasks. Google's service account is authorised for the scopes in `DELEGATED_SCOPES`, which also include directory user and group-member read.

**Refresh and freshness.** The probe types these sources take are `hash` and `none`, and their loader supplies the change token:

- **Gmail:** a digest of each mailbox's current history id, one `users.getProfile` call per mailbox.
- **Microsoft mail:** each kept delta link, followed for one item.
- **Google calendars and tasks:** each calendar's tag and last change time; each task list's tag, and each task's id and update time.
- **Microsoft calendars and tasks:** the change lines of every mailbox, over `calendar_check_days` either side of today.

A check that cannot be made asks for a read; it never reports "unchanged". Calendar and task tables are read whole at each refresh. The mail replicas of a Microsoft 365 or Google Workspace source are kept in place: each part of a mailbox is written as it arrives and can be queried at once. A Google Workspace source that narrows by `mail_search` is read again whole on every refresh, because only Google can say what a search matches.

**Writes.** `INSERT`, `UPDATE` and `DELETE` on `events`, `event_attendees` and `tasks` go to the provider through one write path for both types. An `UPDATE` or `DELETE` names its rows by `provider`, `account` and `id`; an attendee is named by `event_id` and `address`. One occurrence of a series is named by `series_id` and `original_start_at`. A write that sets a column outside the table's writable set is refused by name. Google writes with `sendUpdates=all` when `calendar_send_updates` is on and `sendUpdates=none` when it is off. Microsoft Graph cannot make such a change silently, so a change to a meeting's attendees is refused, naming the setting.

**Exclusions.** Mail is read only; no statement changes a mailbox. `calendars`, `task_lists` and every mail table take no writes. Gmail's Spam and Trash are left out unless `mail_include_spam_trash` is on. Google's Tasks API keeps only the day a task is due, and has no reminder.

### API Sources

Register any HTTP endpoint as a queryable table.

| API Type | Discovery | Column Inference |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI spec parsing | Primitives → native, objects → JSONB |
| `graphql_remote` | Schema introspection | Primitives → native, objects → JSONB |
| `grpc_remote` | Server reflection | Primitives → native, objects → JSONB |

API responses are fetched, cached in PostgreSQL (configurable TTL), and exposed as GraphQL types. Cached tables participate in federated queries like any other source.

**JSONB rules**: Complex columns (objects, arrays) stored as JSONB are not filterable. Sub-field access uses `->>` extraction in SQL. Relationships are declared between tables using scalar FK columns. JSONB blob columns are not join targets. Use JSONB promotion to convert nested fields into native scalar columns when filtering or joining on them is needed.

#### GitHub and GitLab

GitHub and GitLab are branded sources carried by `graphql_remote`: each appears in the Sources form under its own name and registers as a remote GraphQL source that records its brand. The form takes a personal access token and nothing else. The endpoint (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) and the schema ship with Provisa, so the schema is not introspected at registration. The token is checked when the source is added. Adding the source registers no tables: every table the schema offers is available under the source's namespace (`gh`, `gl`), and the steward registers the ones wanted. A field the token may not read is left out of a table when it is registered.

#### Stripe {: #stripe }

Stripe is a branded source carried by `openapi`, built the way GitHub and GitLab are built over `graphql_remote`. It appears in the Sources form as Stripe, under Enterprise. It registers as an OpenAPI source whose row records the brand in `federation_hints`, and whose spec path is `brand:stripe`.

**What it reaches.** The Stripe REST API, through Stripe's published OpenAPI specification (`stripe/openapi`, `spec3.json`). The specification ships with Provisa, pinned and gzipped, and is not fetched at registration. The shipped one is API version `2026-09-30.endive` with 425 paths. Every call carries `Stripe-Version` set to that version, so each answer has the shape the specification describes, whatever version the account defaults to. Fields Stripe returns but the specification leaves out are added from `stripe.additions.json`.

As with any OpenAPI source, each `GET` operation is a table on offer and each other operation is a command on offer. Adding the source registers none of them: the steward registers the ones wanted.

**Auth.** The form asks for one thing, an access token (`BRAND_AUTH`: `token`). Use a Stripe secret or restricted API key. The token is sent as a bearer token, and is checked once with `GET /v1/balance` before the source is added. A rejected token fails registration with `openapi.credential_rejected`; a missing one fails with `openapi.credential_required`.

**Refresh and freshness.** A Stripe table is a replica, like every OpenAPI table. Its cached answers last for the source's `cache_ttl`, which a registration request sets to 300 seconds unless it says otherwise. The OpenAPI probe types apply. A preset can name the parameter a refresh uses to fetch only what changed. The Stripe brand names none, so each refresh reads its tables whole.

**Exclusions.** Stripe's deprecated token and Sources payment integration is left out of the shipped specification. The list is under [Exclusions on a specification-built source](#exclusions-on-a-specification-built-source). An operator's own `exclusions` are added to the brand's.

`stripe` also appears in `SourceType` among the stubbed Supabase `wrappers` types. A Stripe source added through the form is an `openapi` source, not that stub.

#### Exclusions on a specification-built source

A published OpenAPI specification describes everything the remote API accepts, including what its vendor has deprecated and what a deployment must never send. An OpenAPI source can carry exclusions: things in the specification the source does not offer.

| Excluded | Named by | Effect |
|---|---|---|
| An operation | `method` and `path` | Not listed for registration as a table or a command |
| A request argument | `name`, on one operation (`method` and `path`) or, with neither, on every operation that has it | Not among the operation's arguments, not shown, never sent |

Exclusions are set under `exclusions` in the source's `mapping`:

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

They are applied to the specification when it is loaded, so the registration listing, the generated commands and the request that is sent all see a specification in which the excluded things do not exist. A body property and a query, header or cookie parameter can be excluded; a path parameter cannot, because it is part of the operation's address.

An exclusion that names an operation or argument the specification does not have fails the load of that source's specification and names each stale entry, so a list that no longer matches a newer specification is noticed.

A branded source ships its own exclusions as part of its curation, and an operator's are added to them. The Stripe source excludes Stripe's deprecated token and source payment integration: creating a charge, the Tokens and Sources APIs, adding a card or bank account to a customer from a token, and the `source`, `card`, `bank_account` and `default_card` arguments of customer and invoice-payment writes. Payments are taken through Payment Intents, Payment Methods and Setup Intents, which the source offers. Reading and deleting payment sources an account already has stays on offer.

### GovData

U.S. government open data. Access is partitioned by subject grouping.

A source is given one or more subjects, and each subject brings the adapter schemas listed below. A schema may belong to more than one subject (`census`). The `ref` and `geo` schemas are linker schemas: reference data and geography that every subject's tables join through, so every source serves them whatever subjects it is given. The list is stated once on the server and is held, by a test, to the schemas the pinned adapter bundle serves.

| Subject | Schemas Exposed |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| every source | `ref`, `geo` |

`ALL` is a subscription's shorthand for every subject; it is not a choice on the Sources form.

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Yes | — | Unique identifier |
| `subject` | Yes | — | One of the subject values above |
| `domain_id` | Yes | — | Domain this source belongs to |
| `description` | No | `""` | Human-readable description |

An AskAmerica source created in the admin takes one credential, the API key. Provisa presents
the key to AskAmerica's API and receives the short-lived, read-only storage credentials the data
is read with; the key is refused there by name if it is unknown or revoked.

The source is then a Postgres-wire source like the SharePoint, Salesforce and cloud inventory
sources: Provisa starts the bundled `pgwire-govdata` server for it, with the key and those
credentials in the server's environment, and each engine reaches that server the way it reaches a
sibling's. DuckDB, Postgres and ClickHouse attach it live and read its tables in place, so an
AskAmerica table joins tables of other sources, sits under views and is delivered like any other.
Trino reads it live, as a catalog of its own PostgreSQL connector pointed at the same server,
once that route's integration test passes in CI; until then Trino reads a replica landed through
the server: a copy of the table kept in the materialization store, as fresh as its last refresh
under the table's cache and replication settings rather than read at query time. A table is read from
the adapter schema it names (`sec`, `econ`, and so on).

The server mounts its schemas from object storage before it accepts a connection, which takes
minutes on a first start. It is started when the source is saved; until it is ready the Register
Table form reports that the source is still starting.

### Where a source's password lives

A source's password is never stored beside the rest of its connection settings. The
control-plane `sources` row carries a `password_ref` column holding what was entered for the
password: the value itself, or a reference such as `${env:PG_PASSWORD}` or
`${secret:SNOWFLAKE_KEY}`. It is resolved at the moment the source is dialled, inside the
organization the request is running as.

A value with no `${…}` in it is used as it is. `${env:VAR}` reads the deployment's process
environment and needs nothing bound. `${secret:NAME}` names a secret an organization owns, so it
resolves only inside that organization's own operations: the admin introspection seams and the
query terminal every surface reaches establish that binding.

The field holds what you put in it, however the source was registered:

- **From config.** The file is the record: Provisa copies the value or the reference into
  `password_ref` verbatim.
- **From the Sources form.** What is typed into the password field is stored verbatim, a value
  or a reference alike. Provisa writes nothing into the organization's vault for it and makes up
  no secret name. To keep a credential out of the control-plane row, create the secret yourself
  (see [Secrets](secrets.md)) and type `${secret:NAME}` into the field.

A value typed as a value is stored in the control-plane database as typed. Retyping the password
replaces the field's text. Deleting the source removes its row and nothing from the vault: a
secret you created is yours.

`password_ref` does not travel between environments. A branch or a copied environment
supplies its own connection values, and the vault a reference names belongs to whichever
environment supplied it.

### Data Profiler

A data profiler is a source that holds a name, a schedule and run defaults, and nothing else. It is never introspected. Tables of other sources are added to it, and each run measures them and writes its rows into the org's control plane, where they are read like any other source's tables. See [Test data](test-data.md) for creating a profiler and adding tables to it.

### Data Quality Checkers

A data-quality checker is just another source type. Its scan output is data: each check result is an observation, so it lands through the ordinary source path and gets what every other source gets, from cadence, freshness, events and lineage to governance, RLS, the grid and export.

Two checkers are supported, and choosing between them is as much about the licence as about features.

| Source Type | Contract Dialect | Extra | Licence | Hosted cloud plane |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Refused — see below |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Allowed |

Elastic License 2.0 forbids providing the software to third parties as a hosted or managed service, and running Soda inside the SaaS plane on a tenant's behalf would be exactly that. `config/capabilities.yaml` records this as `cloud_eligible: false` on the `soda` option, and the hosted plane reads that flag. A hosted deployment that wants Soda points at a Soda endpoint the operator runs themselves.

Provisa vendors and links nothing. The scan runs in a child interpreter (`python -m provisa.dq.worker`), the only place `soda_core` or `great_expectations` is imported. A source-available checker therefore never reaches the server process, and if a checker crashes, it takes down a subprocess and leaves the event loop alone.

**The source points at Provisa's own pgwire endpoint.** That's how one postgres driver can check a table backed by Snowflake or Iceberg: the checker scans the federated view, never the system underneath. Policy applies to that connection, so you declare the scan identity explicitly. If it were inherited, a filtered set of rows could make a check pass without anyone noticing.

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**One results table per contract, and the contract is the whole registration.** The table carries `dq_contract`, the contract text word for word, and nothing else about its shape. Columns, watermark and promotions are all derived from it.

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

The registration derives four things from that text:

- **Lineage.** The contract already names its target dataset, so the registration parses it the way `extract_inputs` parses SQL and resolves it to the governed table. There's one definition and no second copy to drift. A contract that names an ungoverned dataset fails loudly at registration, so it never lands rows nobody asked for.
- **Columns.** The result envelope belongs to the checker: 16 shipped columns, from `scan_id` through `diagnostics`. Declared columns are read only for their `visible_to`, which must be the same on all of them, and are then replaced.
- **Watermark.** `scan_time` becomes the watermark, which makes the landing an append. Scan history builds up with no history subsystem.
- **Promotions.** `freshness_max_timestamp` and `dataset_rows_tested` are promoted out of the `diagnostics` jsonb as typed columns. You can add more the way you would on any other jsonb column.

Timing needs no new fields. `change_signal` plus `cache_ttl` set how often it polls, `mv_debounce_quiet` and `mv_debounce_max_delay` fold a burst of upstream changes into one scan, a calendar grain makes it periodic, and `expected_events` holds the scan until its inputs are fresh through the window. The poll loop is the scan scheduler.

`outcome` is one of `pass`, `fail`, `warn`, `error` or `skipped`. None of them enforces anything by itself. If you want enforcement, declare it separately: a preflight, or an MV over the landed results. A landed observation doesn't have to be deterministic, so checks that could never gate a preflight are fine here, such as an anomaly score, a change over a trailing window, or freshness measured against now.

You write the contract in the UI, in the data-quality panel of the table's edit page, and the raw contract text there is always the source of truth. A dry run executes the contract against the live table and shows the outcomes without landing them. Use it to catch a contract whose dataset name resolved somewhere unexpected, which would otherwise land nothing but passing rows.

---

## Custom Connectors

An operator can give the native federation engines (Postgres, DuckDB and ClickHouse) reach to a new source type by declaring a connector for it in `config/custom_connectors.yaml`. No code is needed.

This gives the native engines, which run without a cluster, the same config-driven extensibility the Trino engine has at its own layer. There, one generic JDBC connector is parametrized per source type, each type gets a catalog `.properties` body, and Provisa adds its own Trino connector plugins (Splunk, SharePoint, Calcite).

The file ships empty. The built-in connectors cover the sources that work with no extra setup, and everything in this file is written by the operator. To use a different path, for example in tests, set `PROVISA_CUSTOM_CONNECTORS`.

### Descriptor kinds

| Engine | Kind | Mechanism | What the descriptor supplies |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (ISO standard) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + scanner view | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (auto-exposes every remote table) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | per-table `CREATE TABLE ENGINE=…` (columns from the registry) | `ch_engine`, `engine_template` (may carry `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse infers the schema | `ch_engine`, `engine_template` |

**Postgres is generic.** SQL/MED is an ISO standard, so every conforming FDW shares the same DDL shape: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, optional `CREATE USER MAPPING`, then either `IMPORT FOREIGN SCHEMA` (when `supports_import: true`) or an explicit `CREATE FOREIGN TABLE` per table (when `false`). A `pg_fdw` descriptor supplies only the per-FDW variance: extension name, server option keys, user-mapping keys, import flag, table options. Any standard-conforming FDW is therefore drivable from config alone.

**DuckDB supports two mechanisms.** An extension exposing a catalog via ATTACH uses `duckdb_attach`; one exposing a read table-function uses `duckdb_scan`. An extension fitting neither pattern is not supported.

**ClickHouse supports three mechanisms**, one per integration-engine shape: a relational DATABASE engine that auto-exposes every remote table (`clickhouse_database`, e.g. Redis/MySQL), a per-table engine whose columns the registry supplies (`clickhouse_table`, e.g. the JDBC/ODBC bridge; the `engine_template` may carry a `{table}` placeholder the runtime binds), and a file/lake/URL engine whose schema ClickHouse infers (`clickhouse_scan`, e.g. HDFS/URL). SQLite (DATABASE engine, file, no server) and Hudi (lakehouse, zero-copy) ship OOTB.

An unknown `kind` value fails loudly at startup, so a typo in a descriptor never quietly leaves a source type unreachable.
### Probe gating

Availability is verified at attach time against each engine's standard discovery catalog:

- **Postgres** — checks `pg_extension`, then `pg_available_extensions`.
- **DuckDB** — runs `INSTALL`/`LOAD` and checks `duckdb_functions()` for the declared `probe_symbol`.
- **ClickHouse** — checks `system.table_engines` for the declared `ch_engine`; absent from the build fails loud.

A declared extension that is not installable fails loud. No silent skip, no fallback. A connector whose probe fails is simply not active for that deployment.

### Template variables

Every `server_options` value, `user_mapping` value, `attach_template`, and `scan_template` may use `{field}` placeholders. Available fields:

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus any key from `federation_hints`. DuckDB attach templates also receive `{alias}`, the internal catalog alias Provisa gives the attached database.

A template referencing an unknown field fails loud at attach time, surfacing a descriptor/source mismatch before broken DDL reaches the engine.

### Examples

**Postgres — MongoDB via `mongo_fdw` (no schema import; columns supplied per table)**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — Excel files via `read_xlsx` (scan table-function)**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

With either descriptor in place, registering a source with the declared `source_type` routes through the custom connector, subject to a successful probe. No other configuration change is needed.

---

## Warehouses as Named Sources

Snowflake, Databricks, and ClickHouse can be registered as named sources independently of which federation engine is active.

Once registered, the warehouse is read through the source's DirectDriver, which lands a replica in the active engine's materialization store, and the query runs against that replica. On the usual direct path (asyncpg, aiomysql) the engine is skipped entirely. Here the engine still runs the query, but against a local replica instead of going over the wire to the warehouse on every request.

Reads are Arrow-native where the warehouse supports it: Databricks uses Cloud Fetch, Snowflake uses `fetch_arrow_table`, and ClickHouse uses the native columnar HTTP interface.

Extended connection parameters that the standard `host`/`port`/`username`/`password` fields cannot carry go in `federation_hints`:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

Registration as a named source is independent of selecting the same warehouse as the federation engine. A Snowflake source on a DuckDB engine lands a replica into DuckDB, not into Snowflake.

Cloud object and lake data (parquet, csv, iceberg and delta_lake files on S3, GCS or R2) is a separate source type. It attaches in place when the active engine has an ATTACH connector for that type, so no replica is landed and the engine scans the object storage directly. Credentials for these sources also go in `federation_hints`:

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## Source Configuration Fields

All sources share a common set of fields.

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Yes | — | Unique identifier; alphanumeric with hyphens/underscores |
| `type` | Yes | — | Source type (see tables above) |
| `host` | No | `""` | Hostname or IP |
| `port` | No | `0` | Port number |
| `database` | No | `""` | Database name |
| `username` | No | `""` | Username |
| `password` | No | `""` | Password; use `${env:VAR}` or `${secret:NAME}` rather than a literal (see below) |
| `path` | No | `null` | File path or cloud URI for file-based and object/lake sources |
| `base_url` | No | `null` | Base URL for OpenAPI sources |
| `pool_min` | No | `1` | Minimum connection pool size |
| `pool_max` | No | `5` | Maximum connection pool size |
| `use_pgbouncer` | No | `false` | Route connections through PgBouncer |
| `pgbouncer_port` | No | `6432` | PgBouncer port |
| `cache_enabled` | No | `true` | Enable API response caching |
| `cache_ttl` | No | `null` | Cache TTL in seconds; inherits global default when null |
| `cache_catalog` | No | `null` | Federated catalog for API cache; defaults to source's own catalog |
| `cache_schema` | No | `api_cache` | Schema within the cache catalog |
| `naming_convention` | No | `null` | Override global naming convention for this source |
| `federation_hints` | No | `{}` | Session properties passed to the federation engine, and extended connection params for warehouse sources |
| `mapping` | No | `{}` | Type-specific connector settings for NoSQL and SaaS sources (e.g. SharePoint `auth_type`, Salesforce `auth_type`, Splunk `use_token`) |
| `allowed_domains` | No | `[]` | Restrict source to specific domains; empty = unrestricted |
| `description` | No | `""` | Human-readable description |

---

## Kafka Sources

Kafka topics are configured separately under `kafka_sources`, keyed by the source `id` of a registered `kafka` source.

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Field | Description |
| ------- | ------------- |
| `id` | Must match the `id` of a source with `type: kafka` |
| `topics[].id` | Logical name for this topic within Provisa |
| `topics[].topic` | Kafka topic name |
| `topics[].domain_id` | Domain this topic belongs to |
| `topics[].description` | Human-readable description |
| `topics[].default_window` | Default time window for windowed queries (e.g. `1h`) |
| `topics[].columns` | Column definitions for the topic schema |

---

## Column Visibility

The `visible_to` field on each column is a list of role IDs that can see that column.

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Columns omitted from a role's `visible_to` list do not appear in that role's GraphQL schema and cannot be queried or referenced in filters.

---

## Relationships

Relationships connect two registered tables and appear as nested fields in GraphQL.

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `id` | Yes | Unique identifier for this relationship |
| `source_table_id` | Yes | Table that holds the foreign key |
| `target_table_id` | Yes | Table being referenced; empty for computed relationships |
| `source_column` | Yes | Column on the source table |
| `target_column` | Yes | Column on the target table; empty for computed relationships |
| `cardinality` | Yes | `many-to-one` or `one-to-many` |
| `materialize` | No | Auto-create a materialized view for cross-source joins. On a junction-backed edge the view covers the two-hop traversal, not a direct join |
| `refresh_interval` | No | MV refresh interval in seconds (default: 300) |
| `target_function_name` | No | DB function name for computed relationships |
| `function_arg` | No | Which function argument receives the source column value |
| `alias` | No | Human-readable relationship type (e.g. `WORKS_FOR`) |
| `graphql_alias` | No | Names the SDL field this relationship exposes on the parent type. When absent, the name is derived from the target table's `field_name` and relationship cardinality. |
| `disable_cypher` | No | When `true`, exclude this relationship from Cypher graph edges |
| `source_json_key` | No | Extract this key from source column as a JSON object before JOIN |
| `via_table` | No | Registered table name of the junction this edge traverses. Setting it makes the edge junction-backed; leaving it empty makes it a foreign-key edge |
| `via_source_column` | No | Junction column pairing with `source_column`. Comma-separated and positional for a composite key |
| `via_target_column` | No | Junction column pairing with `target_column` |
| `via_type_column` | No | Discriminator column, when one junction carries several relationship types |
| `via_type_value` | No | The discriminator value this edge is pinned to |
| `via_label_source` | No | Which nomination names the Cypher type: `column` (the discriminator value), `table` (the junction's table name), or `fixed` (the declared alias). All are upper-snake-cased |

### Junction-backed relationships

You can declare an associative table as a Cypher relationship in its own right, instead of a node. Its columns then become the relationship's attributes:

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

The junction is a registered table like any other, and it must be registered before a relationship can name it. Declare one relationship per discriminator value. Three rows over `pet_companions` give `BONDED_PAIR`, `LITTERMATE` and `SHARES_ENCLOSURE` as three separate Cypher types, each carrying the junction row's other columns as edge properties. The shipped demo config declares exactly this.

A junction edge exists only in Cypher. The GraphQL join emitter builds its `ON` clause for a single pair of columns and has nowhere to put the second hop, so junction edges are left out of the generated SDL and out of `pg_constraint`. The junction table itself stays queryable as its own root field. It is dropped from the node side of the Cypher graph schema, so it never shows up as a node label.

`materialize: true` works on a junction edge too. What it materializes is the whole traversal: the view holds the source hop, the junction hop, the discriminator, and the junction's own columns next to the target's. The junction is a third leg of the join, so whether the edge crosses sources is judged across all three tables. A junction that lives in a different source from the two tables it links is materialized even when those two share a source. Each declaration materializes one edge type, so a view built for `bonded pair` never answers a `littermate` traversal.

Cardinality values:

- `many-to-one`: each source row maps to one target row (FK to PK)
- `one-to-many`: each source row maps to several target rows (the reverse)

---

## Row-Level Security Rules

RLS rules inject `WHERE` clauses at query time, scoped to a role and optionally to a table or domain.

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

When both a domain-level and a table-level rule exist for the same role, the table-level rule takes precedence.

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `table_id` | Conditional | Table to apply the rule to; mutually exclusive with `domain_id` |
| `domain_id` | Conditional | Domain to apply the rule to; applies to all tables in the domain |
| `role_id` | Yes | Role this rule applies to |
| `filter` | Yes | SQL predicate injected into `WHERE`; may reference session variables |
| `description` | Yes | What the rule is for, as an access review reads it |

---

## Functions and Webhooks

### DB Functions

Track a database function and expose it as a GraphQL query or mutation.

Database sources can also auto-discover their stored procedures and functions from the vendor catalog (`pg_proc`, `information_schema.routines`, or vendor equivalents), removing the need to hand-register each one. Discovery reads `prokind` and `provolatile`: immutable/stable functions register as parameterized relations (proc arguments become query parameters, the same shape as OpenAPI GET tables), and volatile procedures register as mutations/tracked functions. Discovered routines flow through Stage-2 governance identically to hand-registered ones.

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Yes | — | GraphQL field name |
| `source_id` | Yes | — | Source containing the function |
| `schema` | No | `public` | Database schema |
| `function_name` | Yes | — | Actual database function name |
| `returns` | Yes | — | Registered table ID the function returns |
| `arguments` | No | `[]` | List of `{name, type}` argument definitions |
| `visible_to` | No | `[]` | Roles the function is assigned to (empty: every role). A `mutation` also needs the `write` right |
| `domain_id` | No | `""` | Domain this function belongs to |
| `description` | No | `null` | GraphQL field description |
| `kind` | No | `mutation` | `"query"` or `"mutation"` |

### Webhooks

Expose an external HTTP endpoint as a GraphQL query or mutation.

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Field | Required | Default | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Yes | — | GraphQL field name |
| `url` | Yes | — | Webhook endpoint URL |
| `method` | No | `POST` | HTTP method |
| `timeout_ms` | No | `5000` | Request timeout in milliseconds |
| `returns` | No | `null` | Registered table ID, or null for inline type |
| `inline_return_type` | No | `[]` | List of `{name, type}` fields for custom return shapes |
| `arguments` | No | `[]` | List of `{name, type}` argument definitions |
| `visible_to` | No | `[]` | Roles that can call this webhook |
| `domain_id` | No | `""` | Domain this webhook belongs to |
| `description` | No | `null` | GraphQL field description |
| `kind` | No | `mutation` | `"query"` or `"mutation"` |

---

## Authentication

Auth is configured under the `auth` key.

| Provider | Description |
| ---------- | ------------- |
| `none` | No authentication; all requests treated as the `default_role` |
| `firebase` | Firebase Authentication; requires `project_id` and `service_account_key` |
| `keycloak` | Keycloak OIDC |
| `oauth` | Generic OAuth 2.0 |
| `simple` | Username/password without an external provider |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` reads role assignments from JWT claims. `assignments_source: provisa` reads them from Provisa's own assignment store.

---

## Execution Routing

**Direct execution.** A query that touches one relational source goes straight to that source's own driver, in under 100 ms. A source type takes this path when Provisa knows both its SQL dialect and its connector.

**Federated execution.** A query across sources, or on a source with no direct driver, goes through the federation engine. Provisa ships an embedded one; for large deployments, point it at your own compatible cluster.

**Statistics.** When a table is registered, Provisa runs `ANALYZE` on each published table so the cost-based optimizer has row counts, null fractions, distinct values and min/max. If that fails, the failure is logged and the registration goes ahead.

---

## Graph & Semantic Sources

### Neo4j

Register a Neo4j graph database as a queryable source. Stewards author Cypher queries that project scalar values; Provisa caches results and exposes them as GraphQL types.

Cypher queries must use property accessors in the `RETURN` clause (`RETURN n.id AS id, n.name AS name`). Registration refuses a query that returns node objects.
#### Config-file registration

Declare a `neo4j` source and its tables in YAML. Each table requires a `query_template` (the Cypher that produces its rows) and typed columns. The `query_template` key is invalid under any other source type.

A source requires `host`, `port`, and `database`. Rows are fetched by POSTing `{"statements": [{"statement": <cypher>}]}` to `/db/<database>/tx/commit` (the Neo4j HTTP transaction API). A response with a non-empty `errors` list is treated as a failed query, not an empty result.

Each column requires `data_type`. The loader maps config types to the API column type used at query time:

| Config `data_type` | API type |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` and `decimal(10,2)` are accepted, and the base type before the parenthesis is used.

Registration persists an `api_sources` row and one `api_endpoints` row per table, so the tables survive a restart without re-reading the file. The admin REST endpoints under `/admin/sources/neo4j` write the same rows.

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### Register Table in the UI

A neo4j source has no tables to list, so the Register Table form asks for the table instead of offering one.

1. Pick the neo4j source and a domain. The schema and table pickers, the discover checkbox and the watermark picker do not appear; the source is never introspected.
2. Type a table name and the Cypher. The Cypher must project scalars (`RETURN a.name AS name`); a projection that returns a node or a list is reported as an error.
3. Press Preview. The form runs the Cypher with `LIMIT 5` through the `neo4jPreview` GraphQL query and fills the column list from the rows that come back, typed as `text`, `integer`, `double`, `boolean` or `json`. A failed preview keeps the Cypher in the editor and shows the message.
4. Adjust visibility, aliases or masking as for any table, then register. The form refuses to submit until a preview has typed the columns, and the server refuses a neo4j table that carries no Cypher (`schema.neo4j_query_required`).

The Cypher is stored with the table as `queryTemplate`, shows on the table's read view, and persists exactly as a config-file registration does: an `api_sources` row and an `api_endpoints` row the next start hydrates. Editing the table re-persists an edited Cypher.

#### Admin REST registration

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

The preview endpoint (`POST /admin/sources/neo4j/{id}/preview`) returns sample rows and blocks registration if the Cypher returns node objects.

### SPARQL

Register any SPARQL 1.1 compliant triplestore (Apache Jena Fuseki, Virtuoso, Stardog, etc.) as a queryable source.

Queries must be `SELECT` queries. Variable names in the `SELECT` clause become column names automatically.

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

Both connectors use the API source cache pipeline. Results are stored in PostgreSQL with configurable TTL, making them available for cross-source federated JOINs.

---

#### Config-file and UI registration

A `sparql` source's `host` is its SPARQL endpoint URL (the Sources form stores it the same way). Each table under it carries `query_template`, a SELECT whose variables are the columns; every binding is `text`.

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table works the same way as for Neo4j: pick the source, type a table name and the SELECT, press Preview (the `sparqlPreview` query runs it with `LIMIT 5` and fills the column list), then register. The registration persists an `api_sources` row and an `api_endpoints` row (form-encoded POST to the endpoint path, `sparql_bindings` normalizer), the same rows a config-file registration writes, and the native engine lands the rows through the same fetch chain as Neo4j.

## Connection Examples

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Cross-Source Query

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

Single-source portions route directly. Cross-source JOINs federate with automatic type coercion.
