# SingleStore bulk/ingest probe (Phase 1)

Probed against the live `.env` instance (`SINGLESTORE_*`): memsql_version 10.4.0, MySQL-compat
5.7.32, shared-tier workspace `db_kenneth_27404`, user `kenneth-ddc39`. Connection: pymysql over
TLS (`ssl.create_default_context()`), matching `provisa/executor/drivers/mysql.py` `_make_singlestore`.
No product code written in this phase. Method: probe each DDL; a syntax error (1064) means the form
is unsupported, a table-not-exist (1146) or feature-specific error means the form parsed (supported).

## In-place reads (query S3 data without ingest)

Scope of this test: what the `.env` SHARED-TIER workspace exposes. Not a statement about the product.

| Probe on this tier | Result | Evidence |
|---|---|---|
| External table over S3 (Trino-style `CREATE EXTERNAL TABLE`) | not that syntax | `CREATE EXTERNAL TABLE` → 1064 syntax error — so this is not the Iceberg external-table form |
| Iceberg external table / Iceberg ingest | not available on this tier | gated behind `enable_iceberg_ingest`, which this shared tier refuses (`SET global …` → Access denied 1227); the external-table DDL is not in the public docs I could reach |
| Parquet in place (no ingest) | not available on this tier | no external-table/TVF scan of S3 Parquet reached here; it is pipelined in |

Qualified finding (per the maintainer): Iceberg external tables DO exist in the product — SingleStore
Helios release notes (August 2026) list *"Hive and REST catalog support for Iceberg external tables"* —
but `CREATE EXTERNAL TABLE` is not its syntax, the exact DDL is undocumented in the public docs I
reached, and this shared tier refuses the Iceberg globals (1227). So: **not available on this
shared-tier instance; unverified on a dedicated workspace.** Do not conclude the product lacks it.

For the bulk path Provisa builds now, the reachable mechanism on this tier is ingest (PIPELINE / LOAD
DATA) — the "land into the store" reachability case. Whether an Iceberg external table could later
serve an in-place/direct-attach read is open, pending a dedicated workspace that allows the globals.

## Ingest (PIPELINE) capability matrix

| Source / format | Works | DDL shape | Limits / notes |
|---|---|---|---|
| S3 | Yes | `CREATE PIPELINE p AS LOAD DATA S3 's3://…' CONFIG '{"region":…}' CREDENTIALS '{…}' INTO TABLE t …` | — |
| GCS | Yes | `LOAD DATA GCS 'gs://…' CONFIG '{…}'` | parses/supported |
| Azure | Yes | `LOAD DATA AZURE 'container/blob' CONFIG '{…}'` | parses/supported |
| Kafka | Yes | `LOAD DATA KAFKA 'host/topic'` | continuous pipeline; offsets are the pipeline's own state |
| FORMAT CSV (default) | Yes | default | — |
| FORMAT JSON | Yes | `… FORMAT JSON (col <- ::field …)` | — |
| FORMAT PARQUET | Yes | `… FORMAT PARQUET (col <- src …)` | reached table resolution (fully supported) |
| FORMAT AVRO | Yes* | `… FORMAT AVRO (col <- %::field …)` | *requires an explicit field list — "Missing field list with FORMAT AVRO" is refused (1706); a schema registry supplies the schema |
| FORMAT ICEBERG | Gated | `… FORMAT ICEBERG` (CONFIG names the catalog) | Recognized but behind a global flag: `Feature 'Pipelines for the Iceberg format' is disabled. Enable with 'SET global enable_iceberg_ingest=1'.` Setting it here is **Access denied (1227)** — no SUPER/global on the shared tier; an operator/cloud admin must enable it per workspace. |
| `CREATE LINK` (named reusable credentials) | Yes | `CREATE LINK l AS S3 CONFIG '{…}' CREDENTIALS '{…}'` | lets a pipeline reference stored creds by name instead of inline (keeps secrets out of logged DDL) |
| `START PIPELINE … FOREGROUND` | Yes | `START PIPELINE p FOREGROUND` | runs to completion synchronously (1944 = pipeline-not-found, so the form parses) — the shape for a one-shot REPLACE land into a fresh table + swap |

## Bulk load from the client (LOAD DATA phase foundation)

| Feature | Works | Notes |
|---|---|---|
| `local_infile` | ON | `LOAD DATA LOCAL INFILE` is available — the basis for streaming client rows without a server-side file or a temp file |

## Privileges held by the `.env` user (own DB only)

`CREATE/DROP/START/ALTER/SHOW PIPELINE`, `CREATE/DROP/SHOW LINK`, `CREATE/DROP/ALTER/SHOW EXTENSION`,
`SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER`, and `GRANT OUTBOUND` (pipelines reach out). No
global/SUPER — so `enable_iceberg_ingest` cannot be toggled from this account.

## Region / RLS load-filter constraint (design note, carried into PIPELINE phase)

A homed table's data must not cross its region at load. The pipeline applies the region filter as it
lands — a `SET`/`WHERE` transform in the `LOAD DATA … INTO TABLE` body that restricts rows to the
region, or the pipeline is refused by name when it cannot. (SingleStore `LOAD DATA` supports `SET
col = expr` and `WHERE` transforms per docs; exact clause position to confirm in the PIPELINE
implementation — my first probe's `WHERE`/`SET` placement was malformed, 1064, not a feature gap.)

## Docs

- Pipelines overview and `CREATE PIPELINE`: https://docs.singlestore.com/cloud/load-data/about-loading-data-with-pipelines/
- Parquet pipelines: https://docs.singlestore.com/cloud/load-data/load-data-from-parquet-files/
- Iceberg ingest: https://docs.singlestore.com/cloud/load-data/load-data-from-iceberg/ (needs `enable_iceberg_ingest`)
- `LOAD DATA` (local infile + transforms): https://docs.singlestore.com/cloud/reference/sql-reference/data-manipulation-language-dml/load-data/
- Iceberg external tables (exist in the product; unverified on this tier): SingleStore Helios release notes, August 2026 — "Hive and REST catalog support for Iceberg external tables" (https://docs.singlestore.com/cloud/release-notes/)
